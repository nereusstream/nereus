/* Licensed under the Apache License, Version 2.0 */
package com.nereusstream.core.read;

import com.nereusstream.api.ErrorCode;
import com.nereusstream.api.NereusException;
import com.nereusstream.api.StreamId;
import com.nereusstream.api.target.ObjectSliceReadTarget;
import com.nereusstream.metadata.oxia.F4MetadataConditionFailedException;
import com.nereusstream.metadata.oxia.GenerationIndexIdentity;
import com.nereusstream.metadata.oxia.GenerationMetadataStore;
import com.nereusstream.metadata.oxia.PhysicalObjectMetadataStore;
import com.nereusstream.metadata.oxia.VersionedGenerationIndex;
import com.nereusstream.metadata.oxia.VersionedPhysicalObjectRoot;
import com.nereusstream.metadata.oxia.records.GenerationIndexRecord;
import com.nereusstream.metadata.oxia.records.GenerationLifecycle;
import com.nereusstream.metadata.oxia.records.PhysicalObjectLifecycle;
import com.nereusstream.metadata.oxia.records.PhysicalObjectRootRecord;
import java.time.Clock;
import java.util.Objects;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.CompletionException;

/** Propagates immutable-object read corruption into physical-root and generation health metadata. */
public final class MetadataGenerationReadFailureHandler implements GenerationReadFailureHandler {
    private static final int MAX_CAS_ATTEMPTS = 8;

    private final String cluster;
    private final GenerationMetadataStore generationStore;
    private final PhysicalObjectMetadataStore physicalStore;
    private final Clock clock;

    public MetadataGenerationReadFailureHandler(
            String cluster,
            GenerationMetadataStore generationStore,
            PhysicalObjectMetadataStore physicalStore,
            Clock clock) {
        this.cluster = requireText(cluster, "cluster");
        this.generationStore = Objects.requireNonNull(generationStore, "generationStore");
        this.physicalStore = Objects.requireNonNull(physicalStore, "physicalStore");
        this.clock = Objects.requireNonNull(clock, "clock");
    }

    @Override
    public CompletableFuture<Void> handle(
            StreamId streamId,
            GenerationReadCandidate candidate,
            Throwable failure) {
        StreamId exactStream = Objects.requireNonNull(streamId, "streamId");
        GenerationReadCandidate exactCandidate = Objects.requireNonNull(candidate, "candidate");
        Throwable cause = unwrap(Objects.requireNonNull(failure, "failure"));
        if (!isImmutableObjectCorruption(cause)) {
            return CompletableFuture.completedFuture(null);
        }
        if (!(exactCandidate.resolvedRange().readTarget() instanceof ObjectSliceReadTarget target)) {
            return CompletableFuture.failedFuture(new NereusException(
                    ErrorCode.METADATA_INVARIANT_VIOLATION,
                    false,
                    "generation corruption target is not an object slice"));
        }
        String reason = cause instanceof NereusException nereus
                ? "read-" + nereus.code().name().toLowerCase(java.util.Locale.ROOT)
                : "immutable-object-read-corruption";
        return quarantineRoot(target, reason, 0)
                .handle((ignored, rootFailure) -> rootFailure)
                .thenCompose(rootFailure -> quarantineIndex(exactStream, exactCandidate, reason, 0)
                        .handle((ignored, indexFailure) -> {
                            if (rootFailure == null && indexFailure == null) {
                                return null;
                            }
                            Throwable failureToReport = rootFailure == null
                                    ? unwrap(indexFailure)
                                    : unwrap(rootFailure);
                            if (rootFailure != null && indexFailure != null) {
                                failureToReport.addSuppressed(unwrap(indexFailure));
                            }
                            throw new CompletionException(failureToReport);
                        }));
    }

    private CompletableFuture<Void> quarantineRoot(
            ObjectSliceReadTarget target,
            String reason,
            int attempt) {
        return physicalStore.getRoot(cluster, com.nereusstream.api.ObjectKeyHash.from(target.objectKey()))
                .thenCompose(optional -> {
                    VersionedPhysicalObjectRoot current = optional.orElseThrow(() -> new NereusException(
                            ErrorCode.OBJECT_NOT_FOUND,
                            false,
                            "physical object root disappeared while propagating corruption"));
                    requireSameTargetIdentity(target, current.value());
                    if (current.value().lifecycle() == PhysicalObjectLifecycle.QUARANTINED) {
                        return CompletableFuture.completedFuture(null);
                    }
                    if (current.value().lifecycle() != PhysicalObjectLifecycle.ACTIVE
                            && current.value().lifecycle() != PhysicalObjectLifecycle.MARKED) {
                        return CompletableFuture.completedFuture(null);
                    }
                    PhysicalObjectRootRecord replacement = quarantinedRoot(current.value(), reason);
                    return physicalStore.compareAndSetRoot(
                                    cluster, replacement, current.metadataVersion())
                            .thenApply(ignored -> (Void) null)
                            .exceptionallyCompose(error -> retryRoot(target, reason, attempt, error));
                });
    }

    private CompletableFuture<Void> retryRoot(
            ObjectSliceReadTarget target,
            String reason,
            int attempt,
            Throwable failure) {
        if (attempt + 1 >= MAX_CAS_ATTEMPTS || !isConditionFailure(failure)) {
            return CompletableFuture.failedFuture(unwrap(failure));
        }
        return quarantineRoot(target, reason, attempt + 1);
    }

    private CompletableFuture<Void> quarantineIndex(
            StreamId streamId,
            GenerationReadCandidate candidate,
            String reason,
            int attempt) {
        if (candidate.generationZero()) {
            return CompletableFuture.completedFuture(null);
        }
        GenerationIndexIdentity identity = new GenerationIndexIdentity(
                streamId,
                candidate.view(),
                candidate.resolvedRange().offsetRange().endOffset(),
                candidate.resolvedRange().generation());
        return generationStore.getIndex(cluster, identity).thenCompose(optional -> {
            VersionedGenerationIndex current = optional.orElseThrow(() -> new NereusException(
                    ErrorCode.METADATA_CONDITION_FAILED,
                    true,
                    "generation index disappeared while propagating corruption"));
            if (!current.key().equals(candidate.indexKey())) {
                return CompletableFuture.failedFuture(new NereusException(
                        ErrorCode.METADATA_INVARIANT_VIOLATION,
                        false,
                        "generation quarantine reloaded a different index key"));
            }
            if (current.value().lifecycle() == GenerationLifecycle.QUARANTINED
                    || current.value().lifecycle() == GenerationLifecycle.DRAINING
                    || current.value().lifecycle() == GenerationLifecycle.RETIRED) {
                return CompletableFuture.completedFuture(null);
            }
            if (current.value().lifecycle() != GenerationLifecycle.COMMITTED) {
                return CompletableFuture.failedFuture(new NereusException(
                        ErrorCode.METADATA_INVARIANT_VIOLATION,
                        false,
                        "only a committed generation can be quarantined after read corruption"));
            }
            GenerationIndexRecord replacement = quarantinedIndex(current.value(), reason);
            return generationStore.compareAndSetIndex(
                            cluster, replacement, current.metadataVersion())
                    .thenApply(ignored -> (Void) null)
                    .exceptionallyCompose(error -> retryIndex(streamId, candidate, reason, attempt, error));
        });
    }

    private CompletableFuture<Void> retryIndex(
            StreamId streamId,
            GenerationReadCandidate candidate,
            String reason,
            int attempt,
            Throwable failure) {
        if (attempt + 1 >= MAX_CAS_ATTEMPTS || !isConditionFailure(failure)) {
            return CompletableFuture.failedFuture(unwrap(failure));
        }
        return quarantineIndex(streamId, candidate, reason, attempt + 1);
    }

    private PhysicalObjectRootRecord quarantinedRoot(
            PhysicalObjectRootRecord current,
            String reason) {
        return new PhysicalObjectRootRecord(
                current.schemaVersion(),
                current.objectKeyHash(),
                current.objectKey(),
                current.objectId(),
                current.objectKindId(),
                current.objectLength(),
                current.storageChecksumType(),
                current.storageChecksumValue(),
                current.contentSha256(),
                current.etag(),
                PhysicalObjectLifecycle.QUARANTINED,
                Math.addExact(current.lifecycleEpoch(), 1),
                current.createdAtMillis(),
                current.orphanNotBeforeMillis(),
                current.gcAttemptId(),
                current.referenceSetSha256(),
                current.markedAtMillis(),
                current.deleteNotBeforeMillis(),
                current.deleteStartedAtMillis(),
                current.deletedAtMillis(),
                current.tombstoneFirstAbsentAtMillis(),
                current.tombstoneProofSha256(),
                reason,
                0);
    }

    private GenerationIndexRecord quarantinedIndex(
            GenerationIndexRecord current,
            String reason) {
        long now = Math.max(clock.millis(), current.stateChangedAtMillis());
        return new GenerationIndexRecord(
                current.schemaVersion(),
                current.streamId(),
                current.readViewId(),
                current.offsetStart(),
                current.offsetEnd(),
                current.generation(),
                current.publicationId(),
                current.taskId(),
                GenerationLifecycle.QUARANTINED,
                current.sourceSetSha256(),
                current.policySha256(),
                current.readTarget(),
                current.targetIdentitySha256(),
                current.materializationPolicySha256(),
                current.payloadFormat(),
                current.sourceRecordCount(),
                current.outputRecordCount(),
                current.entryCount(),
                current.logicalBytes(),
                current.cumulativeSizeAtStart(),
                current.cumulativeSizeAtEnd(),
                current.firstCommitVersion(),
                current.lastCommitVersion(),
                current.schemaRefs(),
                current.projectionRef(),
                current.createdAtMillis(),
                current.committedAtMillis(),
                reason,
                now,
                0);
    }

    private static void requireSameTargetIdentity(
            ObjectSliceReadTarget target,
            PhysicalObjectRootRecord root) {
        if (!root.objectKey().equals(target.objectKey().value())
                || (!root.objectId().isEmpty()
                        && !root.objectId().equals(target.objectId().value()))) {
            throw new NereusException(
                    ErrorCode.METADATA_INVARIANT_VIOLATION,
                    false,
                    "physical root identity does not match the failed read target");
        }
    }

    private static boolean isImmutableObjectCorruption(Throwable failure) {
        return failure instanceof NereusException nereus
                && (nereus.code() == ErrorCode.OBJECT_NOT_FOUND
                        || nereus.code() == ErrorCode.OBJECT_CHECKSUM_MISMATCH);
    }

    private static boolean isConditionFailure(Throwable failure) {
        Throwable cause = unwrap(failure);
        return cause instanceof F4MetadataConditionFailedException
                || cause instanceof NereusException nereus
                        && nereus.code() == ErrorCode.METADATA_CONDITION_FAILED;
    }

    private static Throwable unwrap(Throwable failure) {
        Throwable current = failure;
        while (current instanceof CompletionException && current.getCause() != null) {
            current = current.getCause();
        }
        return current;
    }

    private static String requireText(String value, String field) {
        Objects.requireNonNull(value, field);
        if (value.isBlank()) {
            throw new IllegalArgumentException(field + " cannot be blank");
        }
        return value;
    }
}
