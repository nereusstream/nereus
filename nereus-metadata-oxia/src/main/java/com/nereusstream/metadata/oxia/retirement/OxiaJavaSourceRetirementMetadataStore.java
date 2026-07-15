/* Licensed under the Apache License, Version 2.0 */
package com.nereusstream.metadata.oxia.retirement;

import com.nereusstream.api.Checksum;
import com.nereusstream.api.StreamId;
import com.nereusstream.metadata.oxia.OxiaClientConfiguration;
import com.nereusstream.metadata.oxia.OxiaKeyspace;
import com.nereusstream.metadata.oxia.PartitionKey;
import com.nereusstream.metadata.oxia.SharedOxiaClientRuntime;
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
    public CompletableFuture<Optional<VersionedGenerationZeroMarker>> getCommittedMarker(
            String cluster,
            StreamId streamId,
            GenerationZeroMarkerIdentity marker) {
        OxiaKeyspace keys = new OxiaKeyspace(cluster);
        StreamId stream = Objects.requireNonNull(streamId, "streamId");
        GenerationZeroMarkerIdentity identity = Objects.requireNonNull(marker, "marker");
        String key = markerKey(keys, stream, identity);
        return support.get(key, keys.streamPartitionKey(stream)).thenApply(optional ->
                optional.map(value -> decodeMarker(key, stream, identity, value)));
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
        PartitionKey partition = keys.streamPartitionKey(stream);
        return support.get(key, partition).thenCompose(optional -> {
            RetirementMetadataValue value = optional.orElseThrow(() ->
                    RetirementMetadataSupport.missing("generation-zero committed marker"));
            decodeMarker(key, stream, identity, value);
            support.requireExpected(value, expectedVersion, expectedDurableValueSha256);
            return support.delete(key, expectedVersion, partition);
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
        PartitionKey partition = keys.streamPartitionKey(stream);
        return support.get(key, partition).thenCompose(optional -> {
            RetirementMetadataValue value = optional.orElseThrow(() ->
                    RetirementMetadataSupport.missing("checkpoint-replaced commit node"));
            validateCommitNode(stream, exactCommitId, value);
            support.requireExpected(value, expectedVersion, expectedDurableValueSha256);
            return support.delete(key, expectedVersion, partition);
        });
    }

    @Override
    public void close() {
        support.close();
    }

    private VersionedGenerationZeroMarker decodeMarker(
            String key,
            StreamId stream,
            GenerationZeroMarkerIdentity identity,
            RetirementMetadataValue value) {
        if (identity instanceof LegacyCommittedSliceIdentity legacy) {
            CommittedSliceRecord record = support.decode(value, CommittedSliceRecord.class);
            if (!record.streamId().equals(stream.value())
                    || !record.objectId().equals(legacy.objectId().value())
                    || !record.sliceId().equals(legacy.sliceId())
                    || record.generation() != 0
                    || record.metadataVersion() != 0) {
                throw RetirementMetadataSupport.invariant(
                        "legacy committed marker key/value identity mismatch");
            }
            return new VersionedGenerationZeroMarker(
                    key,
                    stream,
                    identity,
                    record.offsetStart(),
                    record.offsetEnd(),
                    record.commitVersion(),
                    value.version(),
                    support.digest(value));
        }
        if (identity instanceof GenericCommittedAppendIdentity generic) {
            CommittedAppendRecord record = support.decode(value, CommittedAppendRecord.class);
            if (!record.streamId().equals(stream.value())
                    || !record.commitId().equals(generic.commitId())
                    || record.generation() != 0
                    || record.metadataVersion() != 0) {
                throw RetirementMetadataSupport.invariant(
                        "generic committed marker key/value identity mismatch");
            }
            return new VersionedGenerationZeroMarker(
                    key,
                    stream,
                    identity,
                    record.offsetStart(),
                    record.offsetEnd(),
                    record.commitVersion(),
                    value.version(),
                    support.digest(value));
        }
        throw RetirementMetadataSupport.invariant(
                "unknown generation-zero committed marker identity");
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

    private void validateCommitNode(
            StreamId stream,
            String commitId,
            RetirementMetadataValue value) {
        String type = support.recordType(value);
        if (type.equals(StreamCommitRecord.class.getSimpleName())) {
            StreamCommitRecord record = support.decode(value, StreamCommitRecord.class);
            if (!record.streamId().equals(stream.value())
                    || !record.commitId().equals(commitId)
                    || record.generation() != 0
                    || record.metadataVersion() != 0) {
                throw RetirementMetadataSupport.invariant(
                        "legacy commit-node key/value identity mismatch");
            }
            return;
        }
        if (type.equals(StreamCommitTargetRecord.class.getSimpleName())) {
            StreamCommitTargetRecord record = support.decode(value, StreamCommitTargetRecord.class);
            if (!record.streamId().equals(stream.value())
                    || !record.commitId().equals(commitId)
                    || record.generation() != 0
                    || record.metadataVersion() != 0) {
                throw RetirementMetadataSupport.invariant(
                        "generic commit-node key/value identity mismatch");
            }
            return;
        }
        throw RetirementMetadataSupport.invariant(
                "commit-node key contains an unsupported record type");
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
}
