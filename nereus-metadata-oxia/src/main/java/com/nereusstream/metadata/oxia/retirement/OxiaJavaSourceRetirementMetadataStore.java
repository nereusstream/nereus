/* Licensed under the Apache License, Version 2.0 */
package com.nereusstream.metadata.oxia.retirement;

import com.nereusstream.api.Checksum;
import com.nereusstream.api.ChecksumType;
import com.nereusstream.api.EntryIndexLocation;
import com.nereusstream.api.EntryIndexRef;
import com.nereusstream.api.ObjectId;
import com.nereusstream.api.ObjectKey;
import com.nereusstream.api.ObjectType;
import com.nereusstream.api.StreamId;
import com.nereusstream.api.keys.KeyComponentCodec;
import com.nereusstream.api.target.ObjectSliceReadTarget;
import com.nereusstream.metadata.oxia.AppendRecoveryCommitEncoding;
import com.nereusstream.metadata.oxia.OxiaClientConfiguration;
import com.nereusstream.metadata.oxia.OxiaKeyspace;
import com.nereusstream.metadata.oxia.PartitionKey;
import com.nereusstream.metadata.oxia.SharedOxiaClientRuntime;
import com.nereusstream.metadata.oxia.codec.MetadataRecordCodecFactory;
import com.nereusstream.metadata.oxia.codec.ReadTargetCodecRegistry;
import com.nereusstream.metadata.oxia.records.CommittedAppendRecord;
import com.nereusstream.metadata.oxia.records.CommittedSliceRecord;
import com.nereusstream.metadata.oxia.records.OffsetIndexRecord;
import com.nereusstream.metadata.oxia.records.OffsetIndexTargetRecord;
import com.nereusstream.metadata.oxia.records.StreamCommitRecord;
import com.nereusstream.metadata.oxia.records.StreamCommitTargetRecord;
import java.util.Objects;
import java.util.Optional;
import java.util.concurrent.CompletableFuture;

/** Production exact source-retirement adapter over a borrowed shared Oxia runtime. */
public final class OxiaJavaSourceRetirementMetadataStore
        implements SourceRetirementMetadataStore {
    private final RetirementMetadataSupport support;

    public static OxiaJavaSourceRetirementMetadataStore usingSharedRuntime(
            OxiaClientConfiguration clientConfig,
            SharedOxiaClientRuntime runtime) {
        Objects.requireNonNull(runtime, "runtime");
        return new OxiaJavaSourceRetirementMetadataStore(
                runtime.retirementMetadataClient(Objects.requireNonNull(clientConfig, "clientConfig")));
    }

    OxiaJavaSourceRetirementMetadataStore(RetirementMetadataClient client) {
        this.support = new RetirementMetadataSupport(client);
    }

    @Override
    public CompletableFuture<Optional<VersionedGenerationZeroMarker>> getCommittedMarkerByKey(
            String cluster,
            String exactKey) {
        OxiaKeyspace keys = new OxiaKeyspace(cluster);
        MarkerKeyRoute route = markerRoute(keys, exactKey);
        return support.get(route.key(), keys.streamPartitionKey(route.streamId()))
                .thenApply(optional -> optional.map(value -> decodeMarker(keys, route, value)));
    }

    @Override
    public CompletableFuture<Optional<VersionedGenerationZeroMarker>> getCommittedMarker(
            String cluster,
            StreamId streamId,
            GenerationZeroMarkerIdentity marker) {
        OxiaKeyspace keys = new OxiaKeyspace(cluster);
        StreamId stream = Objects.requireNonNull(streamId, "streamId");
        GenerationZeroMarkerIdentity identity = Objects.requireNonNull(marker, "marker");
        String key = markerKey(keys, stream, identity);
        return getCommittedMarkerByKey(cluster, key).thenApply(optional -> optional.map(value -> {
            if (!value.streamId().equals(stream) || !value.identity().equals(identity)) {
                throw RetirementMetadataSupport.invariant(
                        "committed marker lookup returned another identity");
            }
            return value;
        }));
    }

    @Override
    public CompletableFuture<Optional<VersionedGenerationZeroCommit>> getCommitNodeByKey(
            String cluster,
            String exactKey) {
        OxiaKeyspace keys = new OxiaKeyspace(cluster);
        CommitKeyRoute route = commitRoute(keys, exactKey);
        return support.get(route.key(), keys.streamPartitionKey(route.streamId()))
                .thenApply(optional -> optional.map(value -> decodeCommit(keys, route, value)));
    }

    @Override
    public CompletableFuture<Void> deleteGenerationZeroIndex(
            String cluster,
            StreamId streamId,
            long offsetEnd,
            long expectedVersion,
            Checksum expectedDurableValueSha256) {
        if (offsetEnd <= 0) {
            throw new IllegalArgumentException("offsetEnd must be positive");
        }
        OxiaKeyspace keys = new OxiaKeyspace(cluster);
        StreamId stream = Objects.requireNonNull(streamId, "streamId");
        String key = keys.offsetIndexKey(stream, offsetEnd, 0);
        PartitionKey partition = keys.streamPartitionKey(stream);
        return support.get(key, partition).thenCompose(optional -> {
            RetirementMetadataValue value = optional.orElseThrow(() ->
                    RetirementMetadataSupport.missing("generation-zero index"));
            validateGenerationZeroIndex(stream, offsetEnd, value);
            support.requireExpected(value, expectedVersion, expectedDurableValueSha256);
            return support.delete(key, expectedVersion, partition);
        });
    }

    @Override
    public CompletableFuture<Void> deleteCommittedMarker(
            String cluster,
            StreamId streamId,
            GenerationZeroMarkerIdentity marker,
            long expectedVersion,
            Checksum expectedDurableValueSha256) {
        OxiaKeyspace keys = new OxiaKeyspace(cluster);
        StreamId stream = Objects.requireNonNull(streamId, "streamId");
        GenerationZeroMarkerIdentity identity = Objects.requireNonNull(marker, "marker");
        String key = markerKey(keys, stream, identity);
        return deleteCommittedMarkerByKey(
                cluster, key, expectedVersion, expectedDurableValueSha256);
    }

    @Override
    public CompletableFuture<Void> deleteCommittedMarkerByKey(
            String cluster,
            String exactKey,
            long expectedVersion,
            Checksum expectedDurableValueSha256) {
        OxiaKeyspace keys = new OxiaKeyspace(cluster);
        MarkerKeyRoute route = markerRoute(keys, exactKey);
        PartitionKey partition = keys.streamPartitionKey(route.streamId());
        return support.get(route.key(), partition).thenCompose(optional -> {
            RetirementMetadataValue value = optional.orElseThrow(() ->
                    RetirementMetadataSupport.missing("generation-zero committed marker"));
            decodeMarker(keys, route, value);
            support.requireExpected(value, expectedVersion, expectedDurableValueSha256);
            return support.delete(route.key(), expectedVersion, partition);
        });
    }

    @Override
    public CompletableFuture<Void> deleteCommitNode(
            String cluster,
            StreamId streamId,
            String commitId,
            long expectedVersion,
            Checksum expectedDurableValueSha256) {
        OxiaKeyspace keys = new OxiaKeyspace(cluster);
        StreamId stream = Objects.requireNonNull(streamId, "streamId");
        String exactCommitId = text(commitId, "commitId");
        String key = keys.streamCommitKey(stream, exactCommitId);
        return deleteCommitNodeByKey(
                cluster, key, expectedVersion, expectedDurableValueSha256);
    }

    @Override
    public CompletableFuture<Void> deleteCommitNodeByKey(
            String cluster,
            String exactKey,
            long expectedVersion,
            Checksum expectedDurableValueSha256) {
        OxiaKeyspace keys = new OxiaKeyspace(cluster);
        CommitKeyRoute route = commitRoute(keys, exactKey);
        PartitionKey partition = keys.streamPartitionKey(route.streamId());
        return support.get(route.key(), partition).thenCompose(optional -> {
            RetirementMetadataValue value = optional.orElseThrow(() ->
                    RetirementMetadataSupport.missing("checkpoint-replaced commit node"));
            decodeCommit(keys, route, value);
            support.requireExpected(value, expectedVersion, expectedDurableValueSha256);
            return support.delete(route.key(), expectedVersion, partition);
        });
    }

    @Override
    public void close() {
        support.close();
    }

    private VersionedGenerationZeroMarker decodeMarker(
            OxiaKeyspace keys,
            MarkerKeyRoute route,
            RetirementMetadataValue value) {
        GenerationZeroMarkerIdentity identity;
        long offsetStart;
        long offsetEnd;
        long commitVersion;
        Optional<Checksum> readTargetIdentitySha256;
        if (route.family() == MarkerKeyFamily.LEGACY_COMMITTED_SLICE) {
            CommittedSliceRecord record = support.decode(value, CommittedSliceRecord.class);
            identity = new LegacyCommittedSliceIdentity(
                    new ObjectId(record.objectId()), record.sliceId());
            if (!record.streamId().equals(route.streamId().value())
                    || record.generation() != 0
                    || record.metadataVersion() != 0) {
                throw RetirementMetadataSupport.invariant(
                        "legacy committed marker key/value identity mismatch");
            }
            offsetStart = record.offsetStart();
            offsetEnd = record.offsetEnd();
            commitVersion = record.commitVersion();
            readTargetIdentitySha256 = Optional.empty();
        } else {
            CommittedAppendRecord record = support.decode(value, CommittedAppendRecord.class);
            identity = new GenericCommittedAppendIdentity(record.commitId());
            if (!record.streamId().equals(route.streamId().value())
                    || record.generation() != 0
                    || record.metadataVersion() != 0) {
                throw RetirementMetadataSupport.invariant(
                        "generic committed marker key/value identity mismatch");
            }
            offsetStart = record.offsetStart();
            offsetEnd = record.offsetEnd();
            commitVersion = record.commitVersion();
            try {
                readTargetIdentitySha256 = Optional.of(new Checksum(
                        ChecksumType.SHA256, record.readTargetIdentitySha256()));
            } catch (IllegalArgumentException failure) {
                throw RetirementMetadataSupport.invariant(
                        "generic committed marker has an invalid read-target identity",
                        failure);
            }
        }
        if (!markerKey(keys, route.streamId(), identity).equals(route.key())) {
            throw RetirementMetadataSupport.invariant(
                    "committed marker key is not canonical for its stored identity");
        }
        return new VersionedGenerationZeroMarker(
                route.key(),
                route.streamId(),
                identity,
                offsetStart,
                offsetEnd,
                commitVersion,
                readTargetIdentitySha256,
                value.version(),
                support.digest(value));
    }

    private void validateGenerationZeroIndex(
            StreamId stream,
            long offsetEnd,
            RetirementMetadataValue value) {
        String type = support.recordType(value);
        if (type.equals(OffsetIndexRecord.class.getSimpleName())) {
            OffsetIndexRecord record = support.decode(value, OffsetIndexRecord.class);
            if (!record.streamId().equals(stream.value())
                    || record.offsetEnd() != offsetEnd
                    || record.generation() != 0
                    || record.metadataVersion() != 0) {
                throw RetirementMetadataSupport.invariant(
                        "legacy generation-zero index key/value identity mismatch");
            }
            return;
        }
        if (type.equals(OffsetIndexTargetRecord.class.getSimpleName())) {
            OffsetIndexTargetRecord record = support.decode(value, OffsetIndexTargetRecord.class);
            if (!record.streamId().equals(stream.value())
                    || record.offsetEnd() != offsetEnd
                    || record.generation() != 0
                    || record.metadataVersion() != 0) {
                throw RetirementMetadataSupport.invariant(
                        "generic generation-zero index key/value identity mismatch");
            }
            return;
        }
        throw RetirementMetadataSupport.invariant(
                "generation-zero index key contains an unsupported record type");
    }

    private VersionedGenerationZeroCommit decodeCommit(
            OxiaKeyspace keys,
            CommitKeyRoute route,
            RetirementMetadataValue value) {
        String type = support.recordType(value);
        AppendRecoveryCommitEncoding encoding;
        GenerationZeroMarkerIdentity markerIdentity;
        StreamCommitTargetRecord canonical;
        if (type.equals(StreamCommitRecord.class.getSimpleName())) {
            StreamCommitRecord record = support.decode(value, StreamCommitRecord.class);
            if (!record.streamId().equals(route.streamId().value())
                    || !record.commitId().equals(route.commitId())
                    || record.generation() != 0
                    || record.metadataVersion() != 0) {
                throw RetirementMetadataSupport.invariant(
                        "legacy commit-node key/value identity mismatch");
            }
            encoding = AppendRecoveryCommitEncoding.LEGACY_STREAM_COMMIT_V1;
            markerIdentity = new LegacyCommittedSliceIdentity(
                    new ObjectId(record.objectId()), record.sliceId());
            canonical = canonicalRecoveryCommit(record);
        } else if (type.equals(StreamCommitTargetRecord.class.getSimpleName())) {
            StreamCommitTargetRecord record = support.decode(value, StreamCommitTargetRecord.class);
            if (!record.streamId().equals(route.streamId().value())
                    || !record.commitId().equals(route.commitId())
                    || record.generation() != 0
                    || record.metadataVersion() != 0) {
                throw RetirementMetadataSupport.invariant(
                        "generic commit-node key/value identity mismatch");
            }
            encoding = AppendRecoveryCommitEncoding.GENERIC_STREAM_COMMIT_TARGET_V1;
            markerIdentity = new GenericCommittedAppendIdentity(record.commitId());
            canonical = record;
        } else {
            throw RetirementMetadataSupport.invariant(
                    "commit-node key contains an unsupported record type");
        }
        if (!keys.streamCommitKey(route.streamId(), canonical.commitId()).equals(route.key())) {
            throw RetirementMetadataSupport.invariant(
                    "commit-node key is not canonical for its stored identity");
        }
        byte[] canonicalBytes = MetadataRecordCodecFactory.encodeEnvelope(
                canonical, StreamCommitTargetRecord.class);
        return new VersionedGenerationZeroCommit(
                route.key(),
                route.streamId(),
                canonical.commitId(),
                encoding,
                markerIdentity,
                canonical,
                canonical.offsetStart(),
                canonical.offsetEnd(),
                canonical.commitVersion(),
                RetirementMetadataSupport.sha256(canonicalBytes),
                value.version(),
                support.digest(value));
    }

    private static StreamCommitTargetRecord canonicalRecoveryCommit(StreamCommitRecord value) {
        var rawIndex = value.entryIndexRef();
        EntryIndexRef index = new EntryIndexRef(
                EntryIndexLocation.valueOf(rawIndex.location()),
                rawIndex.objectId().isEmpty()
                        ? Optional.empty()
                        : Optional.of(new ObjectId(rawIndex.objectId())),
                rawIndex.objectKey().isEmpty()
                        ? Optional.empty()
                        : Optional.of(new ObjectKey(rawIndex.objectKey())),
                rawIndex.inlineData().length == 0
                        ? Optional.empty()
                        : Optional.of(rawIndex.inlineData()),
                rawIndex.offset(),
                rawIndex.length(),
                new Checksum(
                        ChecksumType.valueOf(rawIndex.checksumType()),
                        rawIndex.checksumValue()));
        ObjectSliceReadTarget target = new ObjectSliceReadTarget(
                1,
                new ObjectId(value.objectId()),
                new ObjectKey(value.objectKey()),
                ObjectType.valueOf(value.objectType()),
                value.physicalFormat(),
                value.logicalFormat(),
                value.sliceId(),
                value.objectOffset(),
                value.objectLength(),
                new Checksum(
                        ChecksumType.valueOf(value.sliceChecksumType()),
                        value.sliceChecksumValue()),
                index);
        return new StreamCommitTargetRecord(
                value.streamId(),
                value.commitId(),
                value.previousCommitId(),
                value.offsetStart(),
                value.offsetEnd(),
                0,
                value.cumulativeSize(),
                value.commitVersion(),
                value.writerId(),
                value.writerRunIdHash(),
                value.writerEpoch(),
                value.fencingTokenHash(),
                ReadTargetCodecRegistry.phase15().encode(target),
                value.payloadFormat(),
                value.recordCount(),
                value.entryCount(),
                value.logicalBytes(),
                value.schemaRefs(),
                value.projectionRef(),
                value.minEventTimeMillis(),
                value.maxEventTimeMillis(),
                value.preparedAtMillis(),
                0);
    }

    private static MarkerKeyRoute markerRoute(OxiaKeyspace keys, String suppliedKey) {
        SourceKeySuffix source = sourceSuffix(keys, suppliedKey);
        String[] segments = source.suffix().split("/", -1);
        if (segments.length == 2 && segments[0].equals("committed-appends")) {
            String commitId = KeyComponentCodec.decodeComponent(segments[1]);
            String canonical = keys.committedAppendKey(source.streamId(), commitId);
            if (!canonical.equals(source.key())) {
                throw RetirementMetadataSupport.invariant(
                        "generic committed-marker key is not canonical");
            }
            return new MarkerKeyRoute(
                    source.key(), source.streamId(), MarkerKeyFamily.GENERIC_COMMITTED_APPEND);
        }
        if (segments.length == 3 && segments[0].equals("committed-slices")) {
            ObjectId objectId = new ObjectId(KeyComponentCodec.decodeComponent(segments[1]));
            if (!segments[2].matches("[a-z2-7]{52}")) {
                throw RetirementMetadataSupport.invariant(
                        "legacy committed-marker hash component is not canonical");
            }
            String familyPrefix = keys.prefix()
                    + "/streams/"
                    + KeyComponentCodec.encodeComponent(source.streamId().value())
                    + "/committed-slices/"
                    + KeyComponentCodec.encodeComponent(objectId.value())
                    + "/";
            if (!source.key().startsWith(familyPrefix)) {
                throw RetirementMetadataSupport.invariant(
                        "legacy committed-marker key is not canonical");
            }
            return new MarkerKeyRoute(
                    source.key(), source.streamId(), MarkerKeyFamily.LEGACY_COMMITTED_SLICE);
        }
        throw RetirementMetadataSupport.invariant(
                "journaled key is not a supported generation-zero marker key");
    }

    private static CommitKeyRoute commitRoute(OxiaKeyspace keys, String suppliedKey) {
        SourceKeySuffix source = sourceSuffix(keys, suppliedKey);
        String[] segments = source.suffix().split("/", -1);
        if (segments.length != 2 || !segments[0].equals("commit-log")) {
            throw RetirementMetadataSupport.invariant(
                    "journaled key is not a generation-zero commit-node key");
        }
        String commitId = KeyComponentCodec.decodeComponent(segments[1]);
        if (!keys.streamCommitKey(source.streamId(), commitId).equals(source.key())) {
            throw RetirementMetadataSupport.invariant(
                    "commit-node key is not canonical");
        }
        return new CommitKeyRoute(source.key(), source.streamId(), commitId);
    }

    private static SourceKeySuffix sourceSuffix(OxiaKeyspace keys, String suppliedKey) {
        String key = text(suppliedKey, "exactKey");
        String streamsPrefix = keys.prefix() + "/streams/";
        if (!key.startsWith(streamsPrefix)) {
            throw RetirementMetadataSupport.invariant(
                    "source-retirement key belongs to another cluster namespace");
        }
        String remainder = key.substring(streamsPrefix.length());
        int streamEnd = remainder.indexOf('/');
        if (streamEnd <= 0 || streamEnd == remainder.length() - 1) {
            throw RetirementMetadataSupport.invariant(
                    "source-retirement key is missing its canonical stream scope");
        }
        StreamId stream = new StreamId(KeyComponentCodec.decodeComponent(
                remainder.substring(0, streamEnd)));
        String canonicalStreamPrefix = streamsPrefix
                + KeyComponentCodec.encodeComponent(stream.value())
                + "/";
        if (!key.startsWith(canonicalStreamPrefix)) {
            throw RetirementMetadataSupport.invariant(
                    "source-retirement stream component is not canonical");
        }
        return new SourceKeySuffix(
                key, stream, remainder.substring(streamEnd + 1));
    }

    private static String markerKey(
            OxiaKeyspace keys,
            StreamId stream,
            GenerationZeroMarkerIdentity marker) {
        if (marker instanceof LegacyCommittedSliceIdentity legacy) {
            return keys.committedSliceKey(stream, legacy.objectId(), legacy.sliceId());
        }
        if (marker instanceof GenericCommittedAppendIdentity generic) {
            return keys.committedAppendKey(stream, generic.commitId());
        }
        throw RetirementMetadataSupport.invariant(
                "unknown generation-zero committed marker identity");
    }

    private static String text(String value, String field) {
        Objects.requireNonNull(value, field);
        if (value.isBlank()) {
            throw new IllegalArgumentException(field + " cannot be blank");
        }
        return value;
    }

    private enum MarkerKeyFamily {
        LEGACY_COMMITTED_SLICE,
        GENERIC_COMMITTED_APPEND
    }

    private record MarkerKeyRoute(
            String key, StreamId streamId, MarkerKeyFamily family) {
    }

    private record CommitKeyRoute(String key, StreamId streamId, String commitId) {
    }

    private record SourceKeySuffix(String key, StreamId streamId, String suffix) {
    }
}
