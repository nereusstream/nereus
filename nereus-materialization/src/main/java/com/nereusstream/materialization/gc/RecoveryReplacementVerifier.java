/* Licensed under the Apache License, Version 2.0 */
package com.nereusstream.materialization.gc;

import com.nereusstream.api.Checksum;
import com.nereusstream.api.ErrorCode;
import com.nereusstream.api.NereusException;
import com.nereusstream.api.ObjectId;
import com.nereusstream.api.ObjectKey;
import com.nereusstream.api.ObjectKeyHash;
import com.nereusstream.api.ReadView;
import com.nereusstream.api.StreamId;
import com.nereusstream.api.target.ObjectSliceReadTarget;
import com.nereusstream.api.target.ReadTarget;
import com.nereusstream.core.physical.GcReferenceQuery;
import com.nereusstream.core.physical.PhysicalObjectIdentity;
import com.nereusstream.core.physical.PhysicalObjectKind;
import com.nereusstream.metadata.oxia.F4Keyspace;
import com.nereusstream.metadata.oxia.GenerationIndexDigests;
import com.nereusstream.metadata.oxia.GenerationIndexIdentity;
import com.nereusstream.metadata.oxia.GenerationMetadataStore;
import com.nereusstream.metadata.oxia.OffsetIndexEntry;
import com.nereusstream.metadata.oxia.PhysicalObjectMetadataStore;
import com.nereusstream.metadata.oxia.VersionedGenerationIndex;
import com.nereusstream.metadata.oxia.VersionedPhysicalObjectRoot;
import com.nereusstream.metadata.oxia.codec.GenerationIndexRecordCodecV1;
import com.nereusstream.metadata.oxia.codec.ReadTargetCodecRegistry;
import com.nereusstream.metadata.oxia.records.GenerationIndexRecord;
import com.nereusstream.metadata.oxia.records.GenerationLifecycle;
import com.nereusstream.metadata.oxia.records.PhysicalObjectLifecycle;
import com.nereusstream.metadata.oxia.records.RecoveryCheckpointReferenceRecord;
import com.nereusstream.objectstore.checkpoint.RecoveryCheckpointCodecV1;
import com.nereusstream.objectstore.checkpoint.RecoveryCheckpointEntry;
import com.nereusstream.objectstore.checkpoint.RecoveryCheckpointObject;
import com.nereusstream.objectstore.checkpoint.RecoveryCheckpointPublication;
import com.nereusstream.objectstore.checkpoint.RecoveryCheckpointWriteRequest;
import java.nio.ByteBuffer;
import java.util.Arrays;
import java.util.Objects;
import java.util.Optional;
import java.util.OptionalInt;
import java.util.concurrent.CompletableFuture;

/** Shared NRC1-to-current-index/root proof used before source lifecycle retirement. */
final class RecoveryReplacementVerifier {
    private static final ReadTargetCodecRegistry TARGET_CODECS =
            ReadTargetCodecRegistry.phase15();

    private final String cluster;
    private final GenerationMetadataStore generations;
    private final PhysicalObjectMetadataStore physicalObjects;
    private final RecoveryCheckpointCodecV1 checkpoints;
    private final PhysicalGcConfig config;
    private final F4Keyspace keys;
    private final GenerationIndexRecordCodecV1 generationCodec =
            new GenerationIndexRecordCodecV1();

    RecoveryReplacementVerifier(
            String cluster,
            GenerationMetadataStore generations,
            PhysicalObjectMetadataStore physicalObjects,
            RecoveryCheckpointCodecV1 checkpoints,
            PhysicalGcConfig config) {
        this.cluster = requireText(cluster, "cluster");
        this.generations = Objects.requireNonNull(generations, "generations");
        this.physicalObjects = Objects.requireNonNull(physicalObjects, "physicalObjects");
        this.checkpoints = Objects.requireNonNull(checkpoints, "checkpoints");
        this.config = Objects.requireNonNull(config, "config");
        this.keys = new F4Keyspace(cluster);
    }

    CompletableFuture<HealthyReplacement> select(
            GcReferenceQuery query,
            StreamId stream,
            RecoveryCheckpointObject checkpoint,
            RecoveryCheckpointEntry entry,
            ReplacementRequirement requirement) {
        Objects.requireNonNull(query, "query");
        Objects.requireNonNull(stream, "stream");
        Objects.requireNonNull(checkpoint, "checkpoint");
        Objects.requireNonNull(entry, "entry");
        Objects.requireNonNull(requirement, "requirement");
        if (entry.range().startOffset() != requirement.offsetStart()
                || entry.range().endOffset() != requirement.offsetEnd()
                || entry.commitVersion() != requirement.commitVersion()
                || entry.cumulativeSizeAtEnd() != requirement.cumulativeSizeAtEnd()) {
            return CompletableFuture.failedFuture(invariant(
                    "NRC1 entry does not match the replacement requirement"));
        }
        return select(query, stream, checkpoint, entry, requirement, 0);
    }

    private CompletableFuture<HealthyReplacement> select(
            GcReferenceQuery query,
            StreamId stream,
            RecoveryCheckpointObject checkpoint,
            RecoveryCheckpointEntry entry,
            ReplacementRequirement requirement,
            int cursor) {
        if (cursor == entry.coveringPublicationIndexes().size()) {
            String source = requirement.minimumGenerationExclusive() == 0
                    ? "generation-zero"
                    : "higher-generation";
            return CompletableFuture.failedFuture(condition(
                    source + " source has no current healthy NRC1 replacement"));
        }
        int publicationIndex = entry.coveringPublicationIndexes().get(cursor);
        return checkpoints.scanPublications(
                        checkpoint,
                        OptionalInt.of(publicationIndex),
                        1,
                        config.operationTimeout())
                .thenCompose(page -> {
                    if (page.values().size() != 1) {
                        return CompletableFuture.failedFuture(invariant(
                                "NRC1 publication index did not resolve exactly one row"));
                    }
                    EmbeddedReplacement embedded = decode(
                            stream, requirement, page.values().get(0));
                    return loadHealthy(query, embedded).thenCompose(optional ->
                            optional.<CompletableFuture<HealthyReplacement>>map(
                                            CompletableFuture::completedFuture)
                                    .orElseGet(() -> select(
                                            query,
                                            stream,
                                            checkpoint,
                                            entry,
                                            requirement,
                                            cursor + 1)));
                });
    }

    private EmbeddedReplacement decode(
            StreamId stream,
            ReplacementRequirement requirement,
            RecoveryCheckpointPublication publication) {
        byte[] canonical = bytes(publication.canonicalGenerationIndexRecord());
        GenerationIndexRecord record;
        try {
            record = generationCodec.decode(canonical);
        } catch (RuntimeException failure) {
            throw invariant("cannot decode NRC1 source-retirement replacement index");
        }
        if (!Arrays.equals(canonical, generationCodec.encode(record))
                || !GenerationIndexDigests.canonicalRecordSha256(record)
                        .equals(publication.generationIndexRecordSha256())
                || record.metadataVersion() != 0
                || record.lifecycle() != GenerationLifecycle.COMMITTED
                || record.readViewId() != ReadView.COMMITTED.wireId()
                || !record.streamId().equals(stream.value())
                || record.generation() != publication.generation()
                || record.generation() <= requirement.minimumGenerationExclusive()
                || !record.publicationId().equals(publication.publicationId().value())
                || record.offsetStart() != publication.coverage().startOffset()
                || record.offsetEnd() != publication.coverage().endOffset()
                || record.offsetStart() > requirement.offsetStart()
                || record.offsetEnd() < requirement.offsetEnd()
                || record.firstCommitVersion() > requirement.commitVersion()
                || record.lastCommitVersion() < requirement.commitVersion()
                || record.cumulativeSizeAtStart()
                        > requirement.cumulativeSizeAtStart()
                || record.cumulativeSizeAtEnd() < requirement.cumulativeSizeAtEnd()
                || !record.targetIdentitySha256().equals(
                        record.readTarget().identityChecksumValue())) {
            throw invariant(
                    "NRC1 replacement index is non-canonical or does not cover the source commit");
        }
        ReadTarget decoded;
        try {
            decoded = TARGET_CODECS.decode(record.readTarget());
        } catch (RuntimeException failure) {
            throw invariant("cannot decode NRC1 source-retirement replacement target");
        }
        if (!(decoded instanceof ObjectSliceReadTarget target)) {
            throw invariant("NRC1 source-retirement replacement is not an object slice");
        }
        GenerationIndexIdentity identity = new GenerationIndexIdentity(
                stream,
                ReadView.COMMITTED,
                record.offsetEnd(),
                record.generation());
        String key = keys.generationIndexKey(
                stream,
                ReadView.COMMITTED,
                record.offsetEnd(),
                record.generation());
        return new EmbeddedReplacement(record, identity, key, target);
    }

    private CompletableFuture<Optional<HealthyReplacement>> loadHealthy(
            GcReferenceQuery query,
            EmbeddedReplacement embedded) {
        return generations.getIndex(cluster, embedded.identity()).thenCompose(optionalIndex -> {
            if (optionalIndex.isEmpty()) {
                return CompletableFuture.completedFuture(Optional.empty());
            }
            VersionedGenerationIndex index = optionalIndex.orElseThrow();
            GenerationIndexRecord expected = embedded.record().withMetadataVersion(
                    index.metadataVersion());
            Checksum expectedDigest = GenerationIndexDigests.durableValueSha256(
                    embedded.record());
            if (!index.key().equals(embedded.key())
                    || !index.value().equals(expected)
                    || !index.durableValueSha256().equals(expectedDigest)) {
                return CompletableFuture.completedFuture(Optional.empty());
            }
            ObjectSliceReadTarget target = embedded.target();
            ObjectKeyHash object = ObjectKeyHash.from(target.objectKey());
            if (object.equals(query.object().objectKeyHash())
                    || query.object().objectId()
                            .map(target.objectId()::equals)
                            .orElse(false)) {
                return CompletableFuture.completedFuture(Optional.empty());
            }
            return physicalObjects.getRoot(cluster, object).thenApply(optionalRoot -> {
                if (optionalRoot.isEmpty()) {
                    return Optional.empty();
                }
                VersionedPhysicalObjectRoot root = optionalRoot.orElseThrow();
                PhysicalObjectIdentity physical;
                try {
                    physical = PhysicalObjectIdentity.from(root.value());
                } catch (RuntimeException failure) {
                    throw invariant("NRC1 replacement physical root is malformed");
                }
                long requiredEnd = Math.addExact(
                        target.objectOffset(), target.objectLength());
                PhysicalObjectKind expectedKind = switch (target.objectType()) {
                    case MULTI_STREAM_WAL_OBJECT -> PhysicalObjectKind.OBJECT_WAL;
                    case STREAM_COMPACTED_OBJECT -> PhysicalObjectKind.COMMITTED_COMPACTED;
                    default -> throw invariant(
                            "NRC1 source-retirement replacement object type is unsupported");
                };
                if (!root.key().equals(keys.physicalRootKey(object))
                        || root.value().lifecycle() != PhysicalObjectLifecycle.ACTIVE
                        || !physical.objectKey().equals(target.objectKey())
                        || physical.objectId().isEmpty()
                        || !physical.objectId().orElseThrow().equals(target.objectId())
                        || physical.kind() != expectedKind
                        || requiredEnd > physical.objectLength()) {
                    return Optional.empty();
                }
                return Optional.of(new HealthyReplacement(index, root));
            });
        });
    }

    CompletableFuture<Void> revalidate(HealthyReplacement expected) {
        Objects.requireNonNull(expected, "expected");
        GenerationIndexRecord value = expected.index().value();
        GenerationIndexIdentity identity = new GenerationIndexIdentity(
                new StreamId(value.streamId()),
                ReadView.fromWireId(value.readViewId()),
                value.offsetEnd(),
                value.generation());
        return generations.getIndex(cluster, identity).thenCompose(index -> {
            if (!index.equals(Optional.of(expected.index()))) {
                return CompletableFuture.failedFuture(condition(
                        "healthy NRC1 replacement index changed while source facts were frozen"));
            }
            ObjectKeyHash object = new ObjectKeyHash(
                    expected.root().value().objectKeyHash());
            return physicalObjects.getRoot(cluster, object).thenAccept(root -> {
                if (!root.equals(Optional.of(expected.root()))) {
                    throw condition(
                            "healthy NRC1 replacement root changed while source facts were frozen");
                }
            });
        });
    }

    void requireCheckpointIdentity(
            StreamId stream,
            RecoveryCheckpointReferenceRecord reference,
            RecoveryCheckpointObject checkpoint) {
        RecoveryCheckpointWriteRequest header = checkpoint.header();
        if (!checkpoint.objectId().equals(new ObjectId(reference.objectId()))
                || !checkpoint.objectKey().equals(new ObjectKey(reference.objectKey()))
                || checkpoint.objectLength() != reference.objectLength()
                || !checkpoint.contentSha256().value().equals(reference.contentSha256())
                || !header.cluster().equals(cluster)
                || !header.streamId().equals(stream)
                || header.checkpointSequence() != reference.checkpointSequence()
                || !header.checkpointAttemptId().equals(reference.checkpointAttemptId())
                || header.coverage().startOffset() != reference.coveredStartOffset()
                || header.coverage().endOffset() != reference.coveredEndOffset()
                || header.firstCommitVersion() != reference.firstCommitVersion()
                || header.lastCommitVersion() != reference.lastCommitVersion()
                || header.cumulativeSizeAtStart() != reference.cumulativeSizeAtStart()
                || header.cumulativeSizeAtEnd() != reference.cumulativeSizeAtEnd()
                || !header.firstCommitId().equals(reference.firstCommitId())
                || !header.lastCommitId().equals(reference.lastCommitId())
                || !header.sourceHeadCommitId().equals(reference.sourceHeadCommitId())
                || header.sourceHeadCommitVersion() != reference.sourceHeadCommitVersion()
                || !header.projectionIdentitySha256().value().equals(
                        reference.projectionIdentitySha256())
                || header.expectedEntryCount() != reference.commitEntryCount()
                || header.expectedPublicationCount() != reference.publicationCount()) {
            throw invariant("verified recovery checkpoint does not match its root reference");
        }
    }

    private static byte[] bytes(ByteBuffer value) {
        ByteBuffer copy = value.asReadOnlyBuffer();
        byte[] result = new byte[copy.remaining()];
        copy.get(result);
        return result;
    }

    private static String requireText(String value, String name) {
        Objects.requireNonNull(value, name);
        if (value.isBlank()) {
            throw new IllegalArgumentException(name + " must be non-blank");
        }
        return value;
    }

    private static NereusException invariant(String message) {
        return new NereusException(
                ErrorCode.METADATA_INVARIANT_VIOLATION, false, message);
    }

    private static NereusException condition(String message) {
        return new NereusException(ErrorCode.METADATA_CONDITION_FAILED, true, message);
    }

    record ReplacementRequirement(
            long offsetStart,
            long offsetEnd,
            long commitVersion,
            long cumulativeSizeAtStart,
            long cumulativeSizeAtEnd,
            long minimumGenerationExclusive) {
        ReplacementRequirement {
            if (offsetStart < 0
                    || offsetEnd <= offsetStart
                    || commitVersion <= 0
                    || cumulativeSizeAtStart < 0
                    || cumulativeSizeAtEnd < cumulativeSizeAtStart
                    || minimumGenerationExclusive < 0) {
                throw new IllegalArgumentException(
                        "recovery replacement requirement is invalid");
            }
        }

        static ReplacementRequirement generationZero(OffsetIndexEntry index) {
            Objects.requireNonNull(index, "index");
            return new ReplacementRequirement(
                    index.offsetStart(),
                    index.offsetEnd(),
                    index.commitVersion(),
                    Math.subtractExact(
                            index.cumulativeSize(), index.logicalBytes()),
                    index.cumulativeSize(),
                    0);
        }
    }

    record HealthyReplacement(
            VersionedGenerationIndex index,
            VersionedPhysicalObjectRoot root) {
        HealthyReplacement {
            Objects.requireNonNull(index, "index");
            Objects.requireNonNull(root, "root");
        }
    }

    private record EmbeddedReplacement(
            GenerationIndexRecord record,
            GenerationIndexIdentity identity,
            String key,
            ObjectSliceReadTarget target) {
        private EmbeddedReplacement {
            Objects.requireNonNull(record, "record");
            Objects.requireNonNull(identity, "identity");
            requireText(key, "key");
            Objects.requireNonNull(target, "target");
        }
    }
}
