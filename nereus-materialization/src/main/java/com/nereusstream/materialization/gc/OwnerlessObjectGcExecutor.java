/* Licensed under the Apache License, Version 2.0 */
package com.nereusstream.materialization.gc;

import com.nereusstream.api.ObjectKeyHash;
import com.nereusstream.core.physical.GcReferenceQuery;
import com.nereusstream.core.physical.GcReferenceQueryKind;
import com.nereusstream.core.physical.PhysicalObjectIdentity;
import com.nereusstream.materialization.MaterializationDeadline;
import com.nereusstream.metadata.oxia.PhysicalObjectMetadataStore;
import com.nereusstream.metadata.oxia.VersionedPhysicalObjectRoot;
import com.nereusstream.metadata.oxia.records.PhysicalObjectLifecycle;
import java.time.Clock;
import java.util.List;
import java.util.Objects;
import java.util.Optional;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.ScheduledExecutorService;

/** Restart-reconstructable execution path for ACTIVE roots with no durable owner metadata. */
public final class OwnerlessObjectGcExecutor {
    private final PhysicalGcConfig config;
    private final String cluster;
    private final PhysicalObjectMetadataStore metadataStore;
    private final GcReferenceDomainRegistry referenceDomains;
    private final PhysicalObjectGarbageCollector garbageCollector;
    private final SourceRetirementCoordinator sourceRetirement;
    private final GcIdGenerator candidateIds;
    private final Clock clock;
    private final ScheduledExecutorService scheduler;

    public OwnerlessObjectGcExecutor(
            String cluster,
            PhysicalGcConfig config,
            PhysicalObjectMetadataStore metadataStore,
            GcReferenceDomainRegistry referenceDomains,
            PhysicalObjectGarbageCollector garbageCollector,
            SourceRetirementCoordinator sourceRetirement,
            GcIdGenerator candidateIds,
            Clock clock,
            ScheduledExecutorService scheduler) {
        this.cluster = requireText(cluster, "cluster");
        this.config = Objects.requireNonNull(config, "config");
        this.metadataStore = Objects.requireNonNull(metadataStore, "metadataStore");
        this.referenceDomains = Objects.requireNonNull(
                referenceDomains, "referenceDomains");
        this.garbageCollector = Objects.requireNonNull(
                garbageCollector, "garbageCollector");
        this.sourceRetirement = Objects.requireNonNull(
                sourceRetirement, "sourceRetirement");
        this.candidateIds = Objects.requireNonNull(candidateIds, "candidateIds");
        this.clock = Objects.requireNonNull(clock, "clock");
        this.scheduler = Objects.requireNonNull(scheduler, "scheduler");
    }

    /** Evaluates one exact ACTIVE root through the complete ownerless global-domain proof. */
    public CompletableFuture<ExecutionResult> executeActive(
            VersionedPhysicalObjectRoot activeRoot) {
        VersionedPhysicalObjectRoot active = Objects.requireNonNull(
                activeRoot, "activeRoot");
        if (active.value().lifecycle() != PhysicalObjectLifecycle.ACTIVE) {
            return CompletableFuture.failedFuture(new IllegalArgumentException(
                    "ownerless discovery requires an exact ACTIVE root"));
        }
        long now = nonNegativeNow();
        ObjectKeyHash object = object(active);
        if (now < active.value().createdAtMillis()) {
            return CompletableFuture.completedFuture(ExecutionResult.skipped(object));
        }
        return hasProtection(object).thenCompose(protectedRoot -> {
            if (protectedRoot) {
                return CompletableFuture.completedFuture(ExecutionResult.skipped(object));
            }
            GcCandidate candidate = candidate(active, now);
            return garbageCollector.mark(candidate, List.of(), List.of())
                    .thenCompose(mark -> {
                        if (mark.status() != PhysicalGcMarkStatus.MARKED) {
                            return CompletableFuture.completedFuture(
                                    ExecutionResult.markOnly(object, mark));
                        }
                        return advance(mark.plan().orElseThrow(), Optional.of(mark));
                    });
        });
    }

    /** Reconstructs an ownerless plan from the exact MARKED root after restart. */
    public CompletableFuture<ExecutionResult> recoverMarked(
            VersionedPhysicalObjectRoot markedRoot) {
        VersionedPhysicalObjectRoot marked = Objects.requireNonNull(
                markedRoot, "markedRoot");
        if (marked.value().lifecycle() != PhysicalObjectLifecycle.MARKED) {
            return CompletableFuture.failedFuture(new IllegalArgumentException(
                    "ownerless recovery requires an exact MARKED root"));
        }
        long now = nonNegativeNow();
        if (now < marked.value().createdAtMillis()) {
            return CompletableFuture.completedFuture(
                    ExecutionResult.skipped(object(marked)));
        }
        GcCandidate candidate = candidate(marked, now);
        return referenceDomains.snapshotForDeletion(candidate.referenceQuery())
                .thenCompose(collection -> {
                    if (!collection.clear()) {
                        if (collection.status() == GcReferenceCollectionStatus.VETOED) {
                            return unmark(marked);
                        }
                        return CompletableFuture.completedFuture(
                                ExecutionResult.skipped(object(marked)));
                    }
                    final GcPlan plan;
                    try {
                        plan = GcPlan.fromMarkedRoot(
                                config,
                                marked.value().gcAttemptId(),
                                candidate,
                                collection.snapshots(),
                                List.of(),
                                List.of(),
                                marked);
                    } catch (IllegalArgumentException drift) {
                        return unmark(marked);
                    }
                    return advance(plan, Optional.empty());
                });
    }

    private CompletableFuture<ExecutionResult> advance(
            GcPlan plan, Optional<PhysicalGcMarkResult> mark) {
        return garbageCollector.advanceToDeleteIntent(
                        plan,
                        ignored -> CompletableFuture.completedFuture(true))
                .thenCompose(advance -> {
                    if (advance.status() != PhysicalGcAdvanceStatus.DELETE_INTENT) {
                        return CompletableFuture.completedFuture(
                                ExecutionResult.advanced(
                                        plan.candidate().object().objectKeyHash(),
                                        mark,
                                        advance));
                    }
                    return sourceRetirement.resume(advance.root().orElseThrow())
                            .thenApply(deletion -> ExecutionResult.deleted(
                                    plan.candidate().object().objectKeyHash(),
                                    mark,
                                    advance,
                                    deletion));
                });
    }

    private CompletableFuture<ExecutionResult> unmark(
            VersionedPhysicalObjectRoot marked) {
        ObjectKeyHash object = object(marked);
        return garbageCollector.unmarkDrifted(marked)
                .thenApply(advance -> ExecutionResult.advanced(
                        object, Optional.empty(), advance));
    }

    private CompletableFuture<Boolean> hasProtection(ObjectKeyHash object) {
        MaterializationDeadline deadline = new MaterializationDeadline(
                config.operationTimeout(), scheduler);
        CompletableFuture<Boolean> result = deadline.bound(
                        () -> metadataStore.scanProtections(
                                cluster, object, Optional.empty(), 1),
                        "preflight ownerless physical-root protections")
                .thenApply(page -> !page.values().isEmpty());
        result.whenComplete((ignored, failure) -> deadline.close());
        return result;
    }

    private GcCandidate candidate(VersionedPhysicalObjectRoot root, long now) {
        PhysicalObjectIdentity object = PhysicalObjectIdentity.from(root.value());
        GcReferenceQuery query = GcReferenceQuery.create(
                GcReferenceQueryKind.OWNERLESS_ORPHAN_CANDIDATE,
                object,
                List.of(),
                object.identitySha256());
        if (root.value().lifecycle() == PhysicalObjectLifecycle.ACTIVE) {
            return GcCandidate.fromActiveRoot(
                    config,
                    candidateIds.next(),
                    root,
                    query,
                    object.identitySha256(),
                    now,
                    root.value().orphanNotBeforeMillis());
        }
        return GcCandidate.fromMarkedRoot(
                config,
                candidateIds.next(),
                root,
                query,
                object.identitySha256(),
                now);
    }

    private long nonNegativeNow() {
        long now = clock.millis();
        if (now < 0) {
            throw new IllegalStateException("clock returned a negative epoch millisecond");
        }
        return now;
    }

    private static ObjectKeyHash object(VersionedPhysicalObjectRoot root) {
        return new ObjectKeyHash(root.value().objectKeyHash());
    }

    private static String requireText(String value, String field) {
        Objects.requireNonNull(value, field);
        if (value.isBlank()) {
            throw new IllegalArgumentException(field + " cannot be blank");
        }
        return value;
    }

    /** Exact progress from one ACTIVE or MARKED ownerless lifecycle visit. */
    public record ExecutionResult(
            ObjectKeyHash object,
            Optional<PhysicalGcMarkResult> mark,
            Optional<PhysicalGcAdvanceResult> advance,
            Optional<PhysicalGcDeletionResult> deletion) {
        public ExecutionResult {
            Objects.requireNonNull(object, "object");
            mark = Objects.requireNonNull(mark, "mark");
            advance = Objects.requireNonNull(advance, "advance");
            deletion = Objects.requireNonNull(deletion, "deletion");
            if (deletion.isPresent()
                    && (advance.isEmpty()
                            || advance.orElseThrow().status()
                                    != PhysicalGcAdvanceStatus.DELETE_INTENT)) {
                throw new IllegalArgumentException(
                        "ownerless deletion requires durable delete intent");
            }
        }

        private static ExecutionResult skipped(ObjectKeyHash object) {
            return new ExecutionResult(
                    object, Optional.empty(), Optional.empty(), Optional.empty());
        }

        private static ExecutionResult markOnly(
                ObjectKeyHash object, PhysicalGcMarkResult mark) {
            return new ExecutionResult(
                    object, Optional.of(mark), Optional.empty(), Optional.empty());
        }

        private static ExecutionResult advanced(
                ObjectKeyHash object,
                Optional<PhysicalGcMarkResult> mark,
                PhysicalGcAdvanceResult advance) {
            return new ExecutionResult(
                    object, mark, Optional.of(advance), Optional.empty());
        }

        private static ExecutionResult deleted(
                ObjectKeyHash object,
                Optional<PhysicalGcMarkResult> mark,
                PhysicalGcAdvanceResult advance,
                PhysicalGcDeletionResult deletion) {
            return new ExecutionResult(
                    object,
                    mark,
                    Optional.of(advance),
                    Optional.of(deletion));
        }
    }
}
