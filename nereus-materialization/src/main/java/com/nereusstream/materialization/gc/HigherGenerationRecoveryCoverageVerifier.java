/* Licensed under the Apache License, Version 2.0 */
package com.nereusstream.materialization.gc;

import com.nereusstream.api.Checksum;
import com.nereusstream.api.ChecksumType;
import com.nereusstream.api.ErrorCode;
import com.nereusstream.api.MetadataCanonicalizer;
import com.nereusstream.api.NereusException;
import com.nereusstream.api.ObjectKey;
import com.nereusstream.api.ReadView;
import com.nereusstream.api.SchemaRef;
import com.nereusstream.api.StreamId;
import com.nereusstream.core.physical.GcReferenceQuery;
import com.nereusstream.metadata.oxia.F4Keyspace;
import com.nereusstream.metadata.oxia.GenerationIndexIdentity;
import com.nereusstream.metadata.oxia.GenerationMetadataStore;
import com.nereusstream.metadata.oxia.VersionedGenerationIndex;
import com.nereusstream.metadata.oxia.VersionedRecoveryCheckpointRoot;
import com.nereusstream.metadata.oxia.codec.MetadataRecordCodecFactory;
import com.nereusstream.metadata.oxia.records.GenerationIndexRecord;
import com.nereusstream.metadata.oxia.records.GenerationLifecycle;
import com.nereusstream.metadata.oxia.records.RecoveryCheckpointReferenceRecord;
import com.nereusstream.metadata.oxia.records.StreamCommitTargetRecord;
import com.nereusstream.objectstore.checkpoint.RecoveryCheckpointCodecV1;
import com.nereusstream.objectstore.checkpoint.RecoveryCheckpointEntry;
import com.nereusstream.objectstore.checkpoint.RecoveryCheckpointObject;
import java.nio.ByteBuffer;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.LinkedHashMap;
import java.util.List;
import java.util.Map;
import java.util.Objects;
import java.util.Optional;
import java.util.concurrent.CompletableFuture;

/** Proves an entire higher-generation range is checkpointed onto healthier newer targets. */
final class HigherGenerationRecoveryCoverageVerifier {
    private final String cluster;
    private final GenerationMetadataStore generations;
    private final RecoveryCheckpointCodecV1 checkpoints;
    private final RecoveryReplacementVerifier replacements;
    private final PhysicalGcConfig config;
    private final F4Keyspace keys;

    HigherGenerationRecoveryCoverageVerifier(
            String cluster,
            GenerationMetadataStore generations,
            RecoveryCheckpointCodecV1 checkpoints,
            RecoveryReplacementVerifier replacements,
            PhysicalGcConfig config) {
        this.cluster = requireText(cluster, "cluster");
        this.generations = Objects.requireNonNull(generations, "generations");
        this.checkpoints = Objects.requireNonNull(checkpoints, "checkpoints");
        this.replacements = Objects.requireNonNull(replacements, "replacements");
        this.config = Objects.requireNonNull(config, "config");
        this.keys = new F4Keyspace(cluster);
    }

    CompletableFuture<CoverageProof> prove(
            GcReferenceQuery query,
            VersionedGenerationIndex source) {
        Objects.requireNonNull(query, "query");
        Objects.requireNonNull(source, "source");
        GenerationIndexRecord value = source.value();
        if (value.readViewId() != ReadView.COMMITTED.wireId()) {
            return CompletableFuture.failedFuture(condition(
                    "TOPIC_COMPACTED higher-generation retirement requires a view-specific replacement proof"));
        }
        if (value.lifecycle() != GenerationLifecycle.COMMITTED
                && value.lifecycle() != GenerationLifecycle.QUARANTINED
                && value.lifecycle() != GenerationLifecycle.DRAINING) {
            return CompletableFuture.failedFuture(condition(
                    "higher-generation source is not in a pre-drain or DRAINING lifecycle"));
        }
        StreamId stream = new StreamId(value.streamId());
        return generations.getRecoveryRoot(cluster, stream).thenCompose(optionalRoot -> {
            VersionedRecoveryCheckpointRoot root = optionalRoot.orElseThrow(() -> condition(
                    "higher-generation source has no recovery root"));
            requireRootCovers(root, source);
            CoverageState state = new CoverageState(value);
            return proveNext(query, stream, root, source, state)
                    .thenCompose(ignored -> revalidateReplacements(
                            List.copyOf(state.replacements.values()), 0))
                    .thenCompose(ignored -> generations.getRecoveryRoot(cluster, stream))
                    .thenCompose(reloadedRoot -> {
                        if (!reloadedRoot.equals(Optional.of(root))) {
                            return CompletableFuture.failedFuture(condition(
                                    "recovery root changed while higher-generation coverage was frozen"));
                        }
                        GenerationIndexIdentity identity = identity(source.value());
                        return generations.getIndex(cluster, identity).thenApply(reloadedSource -> {
                            if (!reloadedSource.equals(Optional.of(source))) {
                                throw condition(
                                        "higher-generation source changed while coverage was frozen");
                            }
                            return new CoverageProof(
                                    root,
                                    source,
                                    List.copyOf(state.replacements.values()),
                                    state.entryCount);
                        });
                    });
        });
    }

    private CompletableFuture<Void> proveNext(
            GcReferenceQuery query,
            StreamId stream,
            VersionedRecoveryCheckpointRoot root,
            VersionedGenerationIndex source,
            CoverageState state) {
        if (state.offset == source.value().offsetEnd()) {
            validateComplete(source.value(), state);
            return CompletableFuture.completedFuture(null);
        }
        if (state.entryCount >= config.maxAuthoritiesPerDomainSnapshot()) {
            return CompletableFuture.failedFuture(invariant(
                    "higher-generation recovery coverage exceeded its entry bound"));
        }
        RecoveryCheckpointReferenceRecord reference = coveringReference(
                root, state.offset, state.commitVersion);
        Checksum content = new Checksum(
                ChecksumType.SHA256, reference.contentSha256());
        return checkpoints.openAndVerify(
                        new ObjectKey(reference.objectKey()),
                        reference.objectLength(),
                        content,
                        config.operationTimeout())
                .thenCompose(checkpoint -> {
                    replacements.requireCheckpointIdentity(
                            stream, reference, checkpoint);
                    return checkpoints.findCommitCoveringOffset(
                                    checkpoint,
                                    state.offset,
                                    config.operationTimeout())
                            .thenCompose(optionalEntry -> proveEntry(
                                    query,
                                    stream,
                                    root,
                                    source,
                                    state,
                                    checkpoint,
                                    optionalEntry.orElseThrow(() -> condition(
                                            "recovery checkpoint has a gap in higher-generation coverage"))));
                });
    }

    private CompletableFuture<Void> proveEntry(
            GcReferenceQuery query,
            StreamId stream,
            VersionedRecoveryCheckpointRoot root,
            VersionedGenerationIndex source,
            CoverageState state,
            RecoveryCheckpointObject checkpoint,
            RecoveryCheckpointEntry entry) {
        StreamCommitTargetRecord commit = decodeCommit(stream, entry);
        requireNextEntry(source.value(), state, entry, commit);
        long cumulativeStart = Math.subtractExact(
                entry.cumulativeSizeAtEnd(), commit.logicalBytes());
        RecoveryReplacementVerifier.ReplacementRequirement requirement =
                new RecoveryReplacementVerifier.ReplacementRequirement(
                        entry.range().startOffset(),
                        entry.range().endOffset(),
                        entry.commitVersion(),
                        cumulativeStart,
                        entry.cumulativeSizeAtEnd(),
                        source.value().generation());
        return replacements.select(
                        query,
                        stream,
                        checkpoint,
                        entry,
                        requirement)
                .thenCompose(replacement -> {
                    RecoveryReplacementVerifier.HealthyReplacement previous =
                            state.replacements.putIfAbsent(
                                    replacement.index().key(), replacement);
                    if (previous != null && !previous.equals(replacement)) {
                        return CompletableFuture.failedFuture(invariant(
                                "one NRC1 replacement key resolved to different current facts"));
                    }
                    if (state.replacements.size()
                            > config.maxReferencesPerDomainSnapshot()) {
                        return CompletableFuture.failedFuture(invariant(
                                "higher-generation recovery replacements exceeded their bound"));
                    }
                    state.advance(commit);
                    return proveNext(query, stream, root, source, state);
                });
    }

    private CompletableFuture<Void> revalidateReplacements(
            List<RecoveryReplacementVerifier.HealthyReplacement> values,
            int index) {
        if (index == values.size()) {
            return CompletableFuture.completedFuture(null);
        }
        return replacements.revalidate(values.get(index))
                .thenCompose(ignored -> revalidateReplacements(values, index + 1));
    }

    private void requireRootCovers(
            VersionedRecoveryCheckpointRoot root,
            VersionedGenerationIndex source) {
        GenerationIndexRecord value = source.value();
        StreamId stream = new StreamId(value.streamId());
        String expectedSourceKey = keys.generationIndexKey(
                stream,
                ReadView.fromWireId(value.readViewId()),
                value.offsetEnd(),
                value.generation());
        if (!source.key().equals(expectedSourceKey)
                || !root.key().equals(keys.recoveryRootKey(stream))
                || !root.value().streamId().equals(value.streamId())
                || root.value().coveredStartOffset() > value.offsetStart()
                || root.value().coveredEndOffset() < value.offsetEnd()
                || root.value().firstCommitVersion() > value.firstCommitVersion()
                || root.value().lastCommitVersion() < value.lastCommitVersion()
                || root.value().cumulativeSizeAtStart()
                        > value.cumulativeSizeAtStart()
                || root.value().cumulativeSizeAtEnd()
                        < value.cumulativeSizeAtEnd()) {
            throw condition(
                    "recovery root does not fully cover the higher-generation source");
        }
    }

    private static RecoveryCheckpointReferenceRecord coveringReference(
            VersionedRecoveryCheckpointRoot root,
            long offset,
            long commitVersion) {
        List<RecoveryCheckpointReferenceRecord> matching = root.value().checkpoints().stream()
                .filter(reference -> reference.coveredStartOffset() <= offset
                        && offset < reference.coveredEndOffset()
                        && reference.firstCommitVersion() <= commitVersion
                        && reference.lastCommitVersion() >= commitVersion)
                .toList();
        if (matching.size() != 1) {
            throw condition(
                    "higher-generation range is not covered by one exact recovery checkpoint at the cursor");
        }
        return matching.get(0);
    }

    private static StreamCommitTargetRecord decodeCommit(
            StreamId stream,
            RecoveryCheckpointEntry entry) {
        byte[] canonical = bytes(entry.canonicalCommitRecord());
        StreamCommitTargetRecord commit;
        try {
            commit = MetadataRecordCodecFactory.decodeEnvelope(
                    canonical, StreamCommitTargetRecord.class);
        } catch (RuntimeException failure) {
            throw invariant(
                    "cannot decode higher-generation NRC1 commit evidence");
        }
        if (!Arrays.equals(
                        canonical,
                        MetadataRecordCodecFactory.encodeEnvelope(
                                commit, StreamCommitTargetRecord.class))
                || commit.metadataVersion() != 0
                || commit.generation() != 0
                || !commit.streamId().equals(stream.value())
                || !commit.commitId().equals(entry.commitId())
                || !commit.previousCommitId().equals(entry.previousCommitId())
                || commit.commitVersion() != entry.commitVersion()
                || commit.offsetStart() != entry.range().startOffset()
                || commit.offsetEnd() != entry.range().endOffset()
                || commit.cumulativeSize() != entry.cumulativeSizeAtEnd()) {
            throw invariant(
                    "higher-generation NRC1 commit is non-canonical or contradicts its entry");
        }
        return commit;
    }

    private static void requireNextEntry(
            GenerationIndexRecord source,
            CoverageState state,
            RecoveryCheckpointEntry entry,
            StreamCommitTargetRecord commit) {
        long cumulativeStart = Math.subtractExact(
                commit.cumulativeSize(), commit.logicalBytes());
        if (entry.range().startOffset() != state.offset
                || entry.range().endOffset() > source.offsetEnd()
                || entry.commitVersion() != state.commitVersion
                || cumulativeStart != state.cumulativeSize
                || (!state.previousCommitId.isEmpty()
                        && !entry.previousCommitId().equals(state.previousCommitId))
                || !commit.payloadFormat().equals(source.payloadFormat())
                || !commit.projectionRef().equals(source.projectionRef())) {
            throw invariant(
                    "higher-generation source is not an exact tiling of NRC1 commit entries");
        }
    }

    private static void validateComplete(
            GenerationIndexRecord source,
            CoverageState state) {
        List<SchemaRef> schemas = MetadataCanonicalizer.canonicalSchemaRefs(
                state.schemaRefs);
        if (state.commitVersion != Math.addExact(source.lastCommitVersion(), 1)
                || state.cumulativeSize != source.cumulativeSizeAtEnd()
                || state.recordCount != source.sourceRecordCount()
                || state.entryTotal != source.entryCount()
                || state.logicalBytes != source.logicalBytes()
                || !schemas.equals(source.schemaRefs())) {
            throw invariant(
                    "higher-generation NRC1 tiling does not reproduce source counts or schemas");
        }
    }

    private static GenerationIndexIdentity identity(GenerationIndexRecord value) {
        return new GenerationIndexIdentity(
                new StreamId(value.streamId()),
                ReadView.fromWireId(value.readViewId()),
                value.offsetEnd(),
                value.generation());
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

    record CoverageProof(
            VersionedRecoveryCheckpointRoot root,
            VersionedGenerationIndex source,
            List<RecoveryReplacementVerifier.HealthyReplacement> replacements,
            int entryCount) {
        CoverageProof {
            Objects.requireNonNull(root, "root");
            Objects.requireNonNull(source, "source");
            replacements = List.copyOf(Objects.requireNonNull(
                    replacements, "replacements"));
            if (replacements.isEmpty() || entryCount <= 0) {
                throw new IllegalArgumentException(
                        "higher-generation coverage proof is empty");
            }
        }
    }

    private static final class CoverageState {
        private long offset;
        private long commitVersion;
        private long cumulativeSize;
        private int entryCount;
        private int recordCount;
        private int entryTotal;
        private long logicalBytes;
        private String previousCommitId = "";
        private final List<SchemaRef> schemaRefs = new ArrayList<>();
        private final Map<String, RecoveryReplacementVerifier.HealthyReplacement>
                replacements = new LinkedHashMap<>();

        private CoverageState(GenerationIndexRecord source) {
            offset = source.offsetStart();
            commitVersion = source.firstCommitVersion();
            cumulativeSize = source.cumulativeSizeAtStart();
        }

        private void advance(StreamCommitTargetRecord commit) {
            offset = commit.offsetEnd();
            commitVersion = Math.addExact(commitVersion, 1);
            cumulativeSize = commit.cumulativeSize();
            entryCount = Math.addExact(entryCount, 1);
            recordCount = Math.addExact(recordCount, commit.recordCount());
            entryTotal = Math.addExact(entryTotal, commit.entryCount());
            logicalBytes = Math.addExact(logicalBytes, commit.logicalBytes());
            previousCommitId = commit.commitId();
            schemaRefs.addAll(commit.schemaRefs());
        }
    }
}
