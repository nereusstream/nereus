/* Licensed under the Apache License, Version 2.0 */
package com.nereusstream.materialization.gc;

import com.nereusstream.api.Checksum;
import com.nereusstream.api.ErrorCode;
import com.nereusstream.api.NereusException;
import com.nereusstream.api.ObjectKeyHash;
import com.nereusstream.core.capability.DomainValidatedDeletionSubject;
import com.nereusstream.core.capability.GenerationActivationProof;
import com.nereusstream.core.capability.GenerationOperation;
import com.nereusstream.core.capability.GenerationProtocolActivationGuard;
import com.nereusstream.core.physical.GcReferenceSnapshot;
import com.nereusstream.core.physical.PhysicalObjectIdentity;
import com.nereusstream.materialization.MaterializationDeadline;
import com.nereusstream.metadata.oxia.F4ScanToken;
import com.nereusstream.metadata.oxia.ObjectProtectionScanPage;
import com.nereusstream.metadata.oxia.PhysicalObjectMetadataStore;
import com.nereusstream.metadata.oxia.ReaderLeaseScanPage;
import com.nereusstream.metadata.oxia.VersionedObjectProtection;
import com.nereusstream.metadata.oxia.VersionedPhysicalObjectRoot;
import com.nereusstream.metadata.oxia.VersionedReaderLease;
import com.nereusstream.metadata.oxia.records.PhysicalObjectLifecycle;
import com.nereusstream.metadata.oxia.records.PhysicalObjectRootRecord;
import java.time.Clock;
import java.util.ArrayList;
import java.util.List;
import java.util.Objects;
import java.util.Optional;
import java.util.OptionalLong;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.CompletionException;
import java.util.concurrent.ScheduledExecutorService;
import java.util.function.Function;

/**
 * Establishes and drains the physical-root deletion fence.
 *
 * <p>A root can enter MARKED only after its exact retirement journal is sealed, and the same journal is reloaded at
 * drain admission and immediately before DELETING. This checkpoint still stops at the durable DELETING intent;
 * metadata retirement and object deletion are later coordinator stages and cannot be invoked through this class.
 */
public final class PhysicalObjectGarbageCollector {
    public static final String PROJECTION_REFERENCE_DOMAIN = "projection-generation-v1";
    public static final int PROJECTION_REFERENCE_DOMAIN_VERSION = 1;

    private final String cluster;
    private final PhysicalGcConfig config;
    private final PhysicalObjectMetadataStore metadataStore;
    private final GcReferenceDomainRegistry referenceDomains;
    private final GenerationProtocolActivationGuard activationGuard;
    private final GcPlanMetadataRevalidator metadataRevalidator;
    private final GcRetirementJournal retirementJournal;
    private final GcIdGenerator attemptIds;
    private final Clock clock;
    private final ScheduledExecutorService scheduler;

    public PhysicalObjectGarbageCollector(
            String cluster,
            PhysicalGcConfig config,
            PhysicalObjectMetadataStore metadataStore,
            GcReferenceDomainRegistry referenceDomains,
            GenerationProtocolActivationGuard activationGuard,
            GcPlanMetadataRevalidator metadataRevalidator,
            GcRetirementJournal retirementJournal,
            GcIdGenerator attemptIds,
            Clock clock,
            ScheduledExecutorService scheduler) {
        this.cluster = requireText(cluster, "cluster");
        this.config = Objects.requireNonNull(config, "config");
        this.metadataStore = Objects.requireNonNull(metadataStore, "metadataStore");
        this.referenceDomains = Objects.requireNonNull(referenceDomains, "referenceDomains");
        this.activationGuard = Objects.requireNonNull(activationGuard, "activationGuard");
        this.metadataRevalidator = Objects.requireNonNull(
                metadataRevalidator, "metadataRevalidator");
        this.retirementJournal = Objects.requireNonNull(
                retirementJournal, "retirementJournal");
        this.attemptIds = Objects.requireNonNull(attemptIds, "attemptIds");
        this.clock = Objects.requireNonNull(clock, "clock");
        this.scheduler = Objects.requireNonNull(scheduler, "scheduler");
        if (!referenceDomains.contains(
                PROJECTION_REFERENCE_DOMAIN, PROJECTION_REFERENCE_DOMAIN_VERSION)) {
            throw new IllegalArgumentException(
                    "physical GC requires the exact projection-generation-v1 reference domain");
        }
    }

    public CompletableFuture<PhysicalGcMarkResult> mark(
            GcCandidate candidate,
            List<GcPlannedProtectionRemoval> plannedProtectionRemovals,
            List<GcPlannedMetadataRemoval> plannedMetadataRemovals) {
        Objects.requireNonNull(candidate, "candidate");
        List<GcPlannedProtectionRemoval> protections = List.copyOf(
                Objects.requireNonNull(plannedProtectionRemovals, "plannedProtectionRemovals"));
        List<GcPlannedMetadataRemoval> metadataRemovals = List.copyOf(
                Objects.requireNonNull(plannedMetadataRemovals, "plannedMetadataRemovals"));
        if (candidate.rootState() != GcCandidateRootState.ACTIVE_DISCOVERY) {
            return CompletableFuture.failedFuture(new IllegalArgumentException(
                    "mark requires an ACTIVE_DISCOVERY candidate"));
        }
        if (!config.enabled()) {
            return CompletableFuture.completedFuture(
                    PhysicalGcMarkResult.simple(PhysicalGcMarkStatus.DISABLED));
        }
        if (config.dryRun()) {
            return CompletableFuture.completedFuture(
                    PhysicalGcMarkResult.simple(PhysicalGcMarkStatus.DRY_RUN));
        }
        long now = nonNegativeNow();
        if (now < candidate.notBeforeMillis()) {
            return CompletableFuture.completedFuture(
                    PhysicalGcMarkResult.notYetEligible(candidate.notBeforeMillis()));
        }
        OptionalLong deleteNotBefore = config.deadline(now, config.drainGrace());
        if (deleteNotBefore.isEmpty()) {
            return CompletableFuture.completedFuture(
                    PhysicalGcMarkResult.simple(PhysicalGcMarkStatus.DEADLINE_OVERFLOW));
        }
        MaterializationDeadline deadline = new MaterializationDeadline(
                config.operationTimeout(), scheduler);
        CompletableFuture<PhysicalGcMarkResult> result = referenceDomains
                .snapshotForDeletion(candidate.referenceQuery(), deadline)
                .thenCompose(collection -> {
                    if (!collection.clear()) {
                        return CompletableFuture.completedFuture(
                                PhysicalGcMarkResult.domainBlocked(collection.status()));
                    }
                    return scanProtections(candidate.object().objectKeyHash(), deadline)
                            .thenCompose(currentProtections -> {
                                if (currentProtections.overLimit()
                                        || !currentProtections.values().equals(
                                                protections)) {
                                    return CompletableFuture.completedFuture(
                                            PhysicalGcMarkResult.simple(
                                                    PhysicalGcMarkStatus.PLAN_CHANGED));
                                }
                                return metadataStillMatches(
                                                candidate, metadataRemovals, deadline)
                                        .thenCompose(matches -> matches
                                                ? markWithStableFacts(
                                                        candidate,
                                                        collection,
                                                        protections,
                                                        metadataRemovals,
                                                        now,
                                                        deleteNotBefore.orElseThrow(),
                                                        deadline)
                                                : CompletableFuture.completedFuture(
                                                        PhysicalGcMarkResult.simple(
                                                                PhysicalGcMarkStatus.PLAN_CHANGED)));
                            });
                });
        result.whenComplete((ignored, failure) -> deadline.close());
        return result;
    }

    public CompletableFuture<PhysicalGcAdvanceResult> advanceToDeleteIntent(GcPlan plan) {
        return advanceToDeleteIntent(
                plan,
                ignored -> CompletableFuture.completedFuture(true));
    }

    /**
     * Releases an exact restart-discovered MARKED root when its original plan can no longer be reconstructed.
     *
     * <p>This is a fail-safe rollback only: it never enters delete intent and conditionally changes only the exact
     * supplied MARKED wrapper back to ACTIVE. An uncertain CAS response is resolved by reloading that same wrapper.
     */
    public CompletableFuture<PhysicalGcAdvanceResult> unmarkDrifted(
            VersionedPhysicalObjectRoot discoveredRoot) {
        VersionedPhysicalObjectRoot discovered = Objects.requireNonNull(
                discoveredRoot, "discoveredRoot");
        if (discovered.value().lifecycle() != PhysicalObjectLifecycle.MARKED) {
            return CompletableFuture.failedFuture(new IllegalArgumentException(
                    "drift rollback requires an exact MARKED root"));
        }
        if (!config.enabled()) {
            return CompletableFuture.completedFuture(
                    PhysicalGcAdvanceResult.simple(PhysicalGcAdvanceStatus.DISABLED));
        }
        if (config.dryRun()) {
            return CompletableFuture.completedFuture(
                    PhysicalGcAdvanceResult.simple(PhysicalGcAdvanceStatus.DRY_RUN));
        }
        MaterializationDeadline deadline = new MaterializationDeadline(
                config.operationTimeout(), scheduler);
        ObjectKeyHash object = new ObjectKeyHash(discovered.value().objectKeyHash());
        CompletableFuture<PhysicalGcAdvanceResult> result = deadline.bound(
                        () -> metadataStore.getRoot(cluster, object),
                        "reload drifted MARKED physical root")
                .thenCompose(optional -> {
                    if (optional.isEmpty()
                            || !optional.orElseThrow().equals(discovered)) {
                        return CompletableFuture.completedFuture(
                                PhysicalGcAdvanceResult.simple(
                                        PhysicalGcAdvanceStatus.ROOT_CHANGED));
                    }
                    PhysicalObjectRootRecord replacement = active(discovered.value());
                    CompletableFuture<VersionedPhysicalObjectRoot> cas = deadline.bound(
                            () -> metadataStore.compareAndSetRoot(
                                    cluster, replacement, discovered.metadataVersion()),
                            "CAS unreconstructable MARKED physical root back to ACTIVE");
                    return cas.handle((active, failure) -> {
                        if (failure == null) {
                            return CompletableFuture.completedFuture(
                                    PhysicalGcAdvanceResult.withRoot(
                                            PhysicalGcAdvanceStatus.PLAN_DRIFT_UNMARKED,
                                            active));
                        }
                        Throwable original = unwrap(failure);
                        return deadline.bound(
                                        () -> metadataStore.getRoot(cluster, object),
                                        "reload physical root after uncertain recovery unmark CAS")
                                .handle((reloaded, reloadFailure) -> {
                                    if (reloadFailure != null) {
                                        original.addSuppressed(unwrap(reloadFailure));
                                        throw propagate(original);
                                    }
                                    if (reloaded.isPresent()
                                            && exactReplacement(
                                                    reloaded.orElseThrow(), replacement)) {
                                        return PhysicalGcAdvanceResult.withRoot(
                                                PhysicalGcAdvanceStatus.PLAN_DRIFT_UNMARKED,
                                                reloaded.orElseThrow());
                                    }
                                    if (reloaded.isPresent()
                                            && reloaded.orElseThrow().equals(discovered)) {
                                        throw propagate(original);
                                    }
                                    return PhysicalGcAdvanceResult.simple(
                                            PhysicalGcAdvanceStatus.ROOT_CHANGED);
                                });
                    }).thenCompose(Function.identity());
                });
        result.whenComplete((ignored, failure) -> deadline.close());
        return result;
    }

    /**
     * Advances one sealed plan only after a candidate-kind-specific final inventory revalidation.
     *
     * <p>The callback runs after both reader/protection/metadata drain passes and immediately before
     * the final MARKED-root/journal/activation fence. A normal authority drift returns {@code false}
     * and rolls the root back to ACTIVE; a callback failure leaves the root MARKED for retry.
     */
    public CompletableFuture<PhysicalGcAdvanceResult> advanceToDeleteIntent(
            GcPlan plan,
            FinalCandidateRevalidator finalCandidateRevalidator) {
        Objects.requireNonNull(plan, "plan");
        Objects.requireNonNull(finalCandidateRevalidator, "finalCandidateRevalidator");
        if (!config.enabled()) {
            return CompletableFuture.completedFuture(
                    PhysicalGcAdvanceResult.simple(PhysicalGcAdvanceStatus.DISABLED));
        }
        if (config.dryRun()) {
            return CompletableFuture.completedFuture(
                    PhysicalGcAdvanceResult.simple(PhysicalGcAdvanceStatus.DRY_RUN));
        }
        long now = nonNegativeNow();
        if (now < plan.deleteNotBeforeMillis()) {
            return CompletableFuture.completedFuture(PhysicalGcAdvanceResult.waiting(
                    PhysicalGcAdvanceStatus.WAITING_FOR_GRACE,
                    plan.deleteNotBeforeMillis()));
        }
        MaterializationDeadline deadline = new MaterializationDeadline(
                config.operationTimeout(), scheduler);
        CompletableFuture<PhysicalGcAdvanceResult> result = loadExactMarked(plan, deadline)
                .thenCompose(optionalRoot -> optionalRoot.<CompletableFuture<PhysicalGcAdvanceResult>>map(root ->
                                loadExactRetirementJournal(plan, deadline)
                                        .thenCompose(ignored -> acquireActivationProof(plan, deadline))
                                        .thenCompose(proof -> drainAndEnterIntent(
                                                plan,
                                                root,
                                                proof,
                                                now,
                                                finalCandidateRevalidator,
                                                deadline)))
                        .orElseGet(() -> CompletableFuture.completedFuture(
                                PhysicalGcAdvanceResult.simple(
                                        PhysicalGcAdvanceStatus.ROOT_CHANGED))));
        result.whenComplete((ignored, failure) -> deadline.close());
        return result;
    }

    private CompletableFuture<PhysicalGcMarkResult> markWithStableFacts(
            GcCandidate candidate,
            GcReferenceCollection collection,
            List<GcPlannedProtectionRemoval> plannedProtectionRemovals,
            List<GcPlannedMetadataRemoval> plannedMetadataRemovals,
            long markedAtMillis,
            long deleteNotBeforeMillis,
            MaterializationDeadline deadline) {
        String attemptId = GcPlanValidation.requireBase32Id(attemptIds.next(), "gcAttemptId");
        var digest = GcPlan.computeReferenceSetSha256(
                config,
                candidate,
                collection.snapshots(),
                plannedProtectionRemovals,
                plannedMetadataRemovals);
        return deadline.bound(
                        () -> retirementJournal.prepare(
                                attemptId,
                                candidate,
                                collection.snapshots(),
                                plannedProtectionRemovals,
                                plannedMetadataRemovals,
                                digest,
                                markedAtMillis,
                                deadline),
                        "prepare sealed GC retirement journal before mark")
                .thenApply(journal -> {
                    requireExactRetirementJournal(
                            journal,
                            candidate,
                            attemptId,
                            collection.snapshots(),
                            plannedProtectionRemovals,
                            plannedMetadataRemovals,
                            digest);
                    return journal;
                })
                .thenCompose(ignored -> acquireActivationProof(collection, deadline))
                .thenCompose(proof -> deadline.bound(
                                () -> metadataStore.getRoot(
                                        cluster, candidate.object().objectKeyHash()),
                                "reload ACTIVE physical root before mark")
                        .thenCompose(optional -> {
                            if (optional.isEmpty()
                                    || !exactActiveCandidate(candidate, optional.orElseThrow())) {
                                return CompletableFuture.completedFuture(
                                        PhysicalGcMarkResult.simple(
                                                PhysicalGcMarkStatus.ROOT_CHANGED));
                            }
                            PhysicalObjectRootRecord replacement = marked(
                                    optional.orElseThrow().value(),
                                    attemptId,
                                    digest.value(),
                                    markedAtMillis,
                                    deleteNotBeforeMillis);
                            return deadline.bound(
                                            () -> activationGuard.revalidate(proof),
                                            "revalidate physical-delete activation before mark")
                                    .thenCompose(ignored -> markCas(
                                            candidate,
                                            collection.snapshots(),
                                            plannedProtectionRemovals,
                                            plannedMetadataRemovals,
                                            attemptId,
                                            digest.value(),
                                            replacement,
                                            deadline));
                        }));
    }

    private CompletableFuture<PhysicalGcMarkResult> markCas(
            GcCandidate candidate,
            List<GcReferenceSnapshot> snapshots,
            List<GcPlannedProtectionRemoval> protections,
            List<GcPlannedMetadataRemoval> metadataRemovals,
            String attemptId,
            String referenceSetSha256,
            PhysicalObjectRootRecord replacement,
            MaterializationDeadline deadline) {
        CompletableFuture<VersionedPhysicalObjectRoot> cas = deadline.bound(
                () -> metadataStore.compareAndSetRoot(
                        cluster, replacement, candidate.rootMetadataVersion()),
                "CAS ACTIVE physical root to MARKED");
        return cas.handle((marked, failure) -> {
            if (failure == null) {
                return CompletableFuture.completedFuture(PhysicalGcMarkResult.marked(
                        GcPlan.fromMarkedRoot(
                                config,
                                attemptId,
                                candidate,
                                snapshots,
                                protections,
                                metadataRemovals,
                                marked)));
            }
            Throwable original = unwrap(failure);
            return deadline.bound(
                            () -> metadataStore.getRoot(cluster, candidate.object().objectKeyHash()),
                            "reload physical root after uncertain mark CAS")
                    .handle((reloaded, reloadFailure) -> {
                        if (reloadFailure != null) {
                            original.addSuppressed(unwrap(reloadFailure));
                            throw propagate(original);
                        }
                        Optional<VersionedPhysicalObjectRoot> exact = reloaded.filter(root ->
                                exactMarked(
                                        root,
                                        candidate.object(),
                                        attemptId,
                                        referenceSetSha256,
                                        Math.addExact(candidate.rootLifecycleEpoch(), 1),
                                        replacement));
                        if (exact.isPresent()) {
                            return PhysicalGcMarkResult.marked(GcPlan.fromMarkedRoot(
                                    config,
                                    attemptId,
                                    candidate,
                                    snapshots,
                                    protections,
                                    metadataRemovals,
                                    exact.orElseThrow()));
                        }
                        if (reloaded.isPresent()
                                && exactActiveCandidate(candidate, reloaded.orElseThrow())) {
                            throw propagate(original);
                        }
                        return PhysicalGcMarkResult.simple(PhysicalGcMarkStatus.ROOT_CHANGED);
                    });
        }).thenCompose(Function.identity());
    }

    private CompletableFuture<PhysicalGcAdvanceResult> drainAndEnterIntent(
            GcPlan plan,
            VersionedPhysicalObjectRoot markedRoot,
            GenerationActivationProof proof,
            long now,
            FinalCandidateRevalidator finalCandidateRevalidator,
            MaterializationDeadline deadline) {
        return activeLeaseRetryAt(plan, now, deadline)
                .thenCompose(retryAt -> {
                    if (retryAt.isPresent()) {
                        return CompletableFuture.completedFuture(PhysicalGcAdvanceResult.waiting(
                                PhysicalGcAdvanceStatus.WAITING_FOR_READERS,
                                retryAt.orElseThrow()));
                    }
                    return scanProtections(plan.candidate().object().objectKeyHash(), deadline)
                            .thenCompose(currentProtections -> {
                                if (currentProtections.overLimit()
                                        || !currentProtections.values().equals(
                                                plan.plannedProtectionRemovals())) {
                                    return unmark(plan, markedRoot, deadline);
                                }
                                return metadataStillMatches(
                                                plan.candidate(),
                                                plan.plannedMetadataRemovals(),
                                                deadline)
                                        .thenCompose(metadataMatches -> {
                                            if (!metadataMatches) {
                                                return unmark(plan, markedRoot, deadline);
                                            }
                                            GcReferenceCollection collection =
                                                    new GcReferenceCollection(
                                                            plan.candidate().referenceQuery(),
                                                            plan.domainSnapshots(),
                                                            GcReferenceCollectionStatus.CLEAR,
                                                            Optional.empty());
                                            return referenceDomains
                                                    .stillMatches(collection, deadline)
                                                    .thenCompose(matches -> matches
                                                            ? finalDrainFence(
                                                                    plan,
                                                                    proof,
                                                                    now,
                                                                    finalCandidateRevalidator,
                                                                    deadline)
                                                            : unmark(
                                                                    plan,
                                                                    markedRoot,
                                                                    deadline));
                                        });
                            });
                });
    }

    private CompletableFuture<PhysicalGcAdvanceResult> finalDrainFence(
            GcPlan plan,
            GenerationActivationProof proof,
            long now,
            FinalCandidateRevalidator finalCandidateRevalidator,
            MaterializationDeadline deadline) {
        return activeLeaseRetryAt(plan, now, deadline)
                .thenCompose(retryAt -> {
                    if (retryAt.isPresent()) {
                        return CompletableFuture.completedFuture(PhysicalGcAdvanceResult.waiting(
                                PhysicalGcAdvanceStatus.WAITING_FOR_READERS,
                                retryAt.orElseThrow()));
                    }
                    return scanProtections(
                                    plan.candidate().object().objectKeyHash(), deadline)
                            .thenCompose(protections -> {
                                if (protections.overLimit()
                                        || !protections.values().equals(
                                                plan.plannedProtectionRemovals())) {
                                    return unmarkCurrent(plan, deadline);
                                }
                                return metadataStillMatches(
                                                plan.candidate(),
                                                plan.plannedMetadataRemovals(),
                                                deadline)
                                        .thenCompose(matches -> {
                                            if (!matches) {
                                                return unmarkCurrent(plan, deadline);
                                            }
                                            return deadline.bound(
                                                            () -> finalCandidateRevalidator.revalidate(plan),
                                                            "revalidate candidate-specific inventory before delete intent")
                                                    .thenCompose(candidateMatches -> candidateMatches
                                                            ? enterDeleteIntent(
                                                                    plan,
                                                                    proof,
                                                                    now,
                                                                    deadline)
                                                            : unmarkCurrent(plan, deadline));
                                        });
                            });
                });
    }

    private CompletableFuture<PhysicalGcAdvanceResult> enterDeleteIntent(
            GcPlan plan,
            GenerationActivationProof proof,
            long now,
            MaterializationDeadline deadline) {
        return loadExactMarked(plan, deadline).thenCompose(optional -> {
            if (optional.isEmpty()) {
                return CompletableFuture.completedFuture(
                        PhysicalGcAdvanceResult.simple(
                                PhysicalGcAdvanceStatus.ROOT_CHANGED));
            }
            return loadExactRetirementJournal(plan, deadline)
                    .thenCompose(ignored -> deadline.bound(
                            () -> activationGuard.revalidate(proof),
                            "revalidate physical-delete activation before delete intent"))
                    .thenCompose(ignored -> deletingCas(
                            plan,
                            optional.orElseThrow(),
                            now,
                            deadline));
        });
    }

    @FunctionalInterface
    public interface FinalCandidateRevalidator {
        CompletableFuture<Boolean> revalidate(GcPlan plan);
    }

    private CompletableFuture<PhysicalGcAdvanceResult> unmarkCurrent(
            GcPlan plan, MaterializationDeadline deadline) {
        return loadExactMarked(plan, deadline).thenCompose(optional -> optional
                .<CompletableFuture<PhysicalGcAdvanceResult>>map(root ->
                        unmark(plan, root, deadline))
                .orElseGet(() -> CompletableFuture.completedFuture(
                        PhysicalGcAdvanceResult.simple(
                                PhysicalGcAdvanceStatus.ROOT_CHANGED))));
    }

    private CompletableFuture<PhysicalGcAdvanceResult> deletingCas(
            GcPlan plan,
            VersionedPhysicalObjectRoot markedRoot,
            long deleteStartedAtMillis,
            MaterializationDeadline deadline) {
        PhysicalObjectRootRecord replacement = deleting(
                markedRoot.value(), deleteStartedAtMillis);
        CompletableFuture<VersionedPhysicalObjectRoot> cas = deadline.bound(
                () -> metadataStore.compareAndSetRoot(
                        cluster, replacement, markedRoot.metadataVersion()),
                "CAS MARKED physical root to DELETING");
        return cas.handle((deleting, failure) -> {
            if (failure == null) {
                return CompletableFuture.completedFuture(PhysicalGcAdvanceResult.withRoot(
                        PhysicalGcAdvanceStatus.DELETE_INTENT, deleting));
            }
            Throwable original = unwrap(failure);
            return deadline.bound(
                            () -> metadataStore.getRoot(
                                    cluster, plan.candidate().object().objectKeyHash()),
                            "reload physical root after uncertain delete-intent CAS")
                    .handle((reloaded, reloadFailure) -> {
                        if (reloadFailure != null) {
                            original.addSuppressed(unwrap(reloadFailure));
                            throw propagate(original);
                        }
                        if (reloaded.isPresent()
                                && exactReplacement(reloaded.orElseThrow(), replacement)) {
                            return PhysicalGcAdvanceResult.withRoot(
                                    PhysicalGcAdvanceStatus.DELETE_INTENT,
                                    reloaded.orElseThrow());
                        }
                        if (reloaded.isPresent() && exactMarkedPlan(plan, reloaded.orElseThrow())) {
                            throw propagate(original);
                        }
                        return PhysicalGcAdvanceResult.simple(
                                PhysicalGcAdvanceStatus.ROOT_CHANGED);
                    });
        }).thenCompose(Function.identity());
    }

    private CompletableFuture<PhysicalGcAdvanceResult> unmark(
            GcPlan plan,
            VersionedPhysicalObjectRoot markedRoot,
            MaterializationDeadline deadline) {
        PhysicalObjectRootRecord replacement = active(markedRoot.value());
        CompletableFuture<VersionedPhysicalObjectRoot> cas = deadline.bound(
                () -> metadataStore.compareAndSetRoot(
                        cluster, replacement, markedRoot.metadataVersion()),
                "CAS drifted MARKED physical root back to ACTIVE");
        return cas.handle((active, failure) -> {
            if (failure == null) {
                return CompletableFuture.completedFuture(PhysicalGcAdvanceResult.withRoot(
                        PhysicalGcAdvanceStatus.PLAN_DRIFT_UNMARKED, active));
            }
            Throwable original = unwrap(failure);
            return deadline.bound(
                            () -> metadataStore.getRoot(
                                    cluster, plan.candidate().object().objectKeyHash()),
                            "reload physical root after uncertain unmark CAS")
                    .handle((reloaded, reloadFailure) -> {
                        if (reloadFailure != null) {
                            original.addSuppressed(unwrap(reloadFailure));
                            throw propagate(original);
                        }
                        if (reloaded.isPresent()
                                && exactReplacement(reloaded.orElseThrow(), replacement)) {
                            return PhysicalGcAdvanceResult.withRoot(
                                    PhysicalGcAdvanceStatus.PLAN_DRIFT_UNMARKED,
                                    reloaded.orElseThrow());
                        }
                        if (reloaded.isPresent() && exactMarkedPlan(plan, reloaded.orElseThrow())) {
                            throw propagate(original);
                        }
                        return PhysicalGcAdvanceResult.simple(
                                PhysicalGcAdvanceStatus.ROOT_CHANGED);
                    });
        }).thenCompose(Function.identity());
    }

    private CompletableFuture<Optional<VersionedPhysicalObjectRoot>> loadExactMarked(
            GcPlan plan, MaterializationDeadline deadline) {
        return deadline.bound(
                        () -> metadataStore.getRoot(
                                cluster, plan.candidate().object().objectKeyHash()),
                        "reload MARKED physical root")
                .thenApply(optional -> optional.filter(root -> exactMarkedPlan(plan, root)));
    }

    private CompletableFuture<GcRetirementJournalSnapshot> loadExactRetirementJournal(
            GcPlan plan, MaterializationDeadline deadline) {
        return deadline.bound(
                        () -> retirementJournal.load(
                                plan.candidate().object().objectKeyHash(),
                                plan.gcAttemptId(),
                                deadline),
                        "load exact sealed GC retirement journal")
                .thenApply(optional -> {
                    GcRetirementJournalSnapshot journal = optional.orElseThrow(() -> invariant(
                            "sealed GC retirement journal is missing for the MARKED root"));
                    requireExactRetirementJournal(
                            journal,
                            plan.candidate(),
                            plan.gcAttemptId(),
                            plan.domainSnapshots(),
                            plan.plannedProtectionRemovals(),
                            plan.plannedMetadataRemovals(),
                            plan.referenceSetSha256());
                    return journal;
                });
    }

    private static void requireExactRetirementJournal(
            GcRetirementJournalSnapshot journal,
            GcCandidate candidate,
            String attemptId,
            List<GcReferenceSnapshot> snapshots,
            List<GcPlannedProtectionRemoval> protections,
            List<GcPlannedMetadataRemoval> metadataRemovals,
            Checksum referenceSetSha256) {
        List<GcDomainSnapshotProof> proofs = snapshots.stream()
                .map(GcDomainSnapshotProof::from)
                .toList();
        if (!journal.object().equals(candidate.object().objectKeyHash())
                || !journal.gcAttemptId().equals(attemptId)
                || !journal.queryIdentitySha256().equals(
                        candidate.referenceQuery().queryIdentitySha256())
                || !journal.domainProofs().equals(proofs)
                || !journal.plannedProtectionRemovals().equals(protections)
                || !journal.plannedMetadataRemovals().equals(metadataRemovals)
                || !journal.referenceSetSha256().equals(referenceSetSha256)) {
            throw invariant(
                    "sealed GC retirement journal does not match the root-authenticated plan");
        }
    }

    private CompletableFuture<Boolean> metadataStillMatches(
            GcCandidate candidate,
            List<GcPlannedMetadataRemoval> expected,
            MaterializationDeadline deadline) {
        return deadline.bound(
                        () -> metadataRevalidator.reload(candidate, expected),
                        "reload exact GC metadata-removal facts")
                .thenApply(reloaded -> GcPlanValidation.canonicalAllowEmpty(
                                reloaded,
                                GcPlanValidation.METADATA_ORDER,
                                config.maxReferencesPerDomainSnapshot(),
                                "reloadedMetadataRemovals")
                        .equals(expected));
    }

    private CompletableFuture<GenerationActivationProof> acquireActivationProof(
            GcReferenceCollection collection, MaterializationDeadline deadline) {
        GcReferenceSnapshot projection = collection.snapshot(PROJECTION_REFERENCE_DOMAIN)
                .orElseThrow(() -> invariant(
                        "reference collection lacks projection-generation-v1"));
        return acquireActivationProof(
                collection.query(), projection, deadline);
    }

    private CompletableFuture<GenerationActivationProof> acquireActivationProof(
            GcPlan plan, MaterializationDeadline deadline) {
        GcReferenceSnapshot projection = plan.domainSnapshots().stream()
                .filter(snapshot -> snapshot.domainId().equals(PROJECTION_REFERENCE_DOMAIN))
                .findFirst()
                .orElseThrow(() -> invariant(
                        "GC plan lacks projection-generation-v1"));
        return acquireActivationProof(
                plan.candidate().referenceQuery(), projection, deadline);
    }

    private CompletableFuture<GenerationActivationProof> acquireActivationProof(
            com.nereusstream.core.physical.GcReferenceQuery query,
            GcReferenceSnapshot projection,
            MaterializationDeadline deadline) {
        DomainValidatedDeletionSubject subject = new DomainValidatedDeletionSubject(
                query, projection.snapshotSha256());
        return deadline.bound(
                () -> activationGuard.requireReady(
                        GenerationOperation.PHYSICAL_DELETE, subject, false),
                "require physical-delete activation");
    }

    private CompletableFuture<ProtectionScan> scanProtections(
            ObjectKeyHash object, MaterializationDeadline deadline) {
        return scanProtections(
                object, Optional.empty(), new ArrayList<>(), null, deadline);
    }

    private CompletableFuture<ProtectionScan> scanProtections(
            ObjectKeyHash object,
            Optional<F4ScanToken> continuation,
            ArrayList<GcPlannedProtectionRemoval> values,
            String lastKey,
            MaterializationDeadline deadline) {
        return deadline.bound(
                        () -> metadataStore.scanProtections(
                                cluster,
                                object,
                                continuation,
                                config.metadataScanPageSize()),
                        "scan physical-object protections")
                .thenCompose(page -> {
                    requireIncreasingPage(page, lastKey);
                    for (VersionedObjectProtection protection : page.values()) {
                        values.add(new GcPlannedProtectionRemoval(protection));
                        if (values.size() > config.maxReferencesPerDomainSnapshot()) {
                            return CompletableFuture.completedFuture(
                                    new ProtectionScan(List.copyOf(values), true));
                        }
                    }
                    if (page.continuation().isEmpty()) {
                        return CompletableFuture.completedFuture(
                                new ProtectionScan(List.copyOf(values), false));
                    }
                    String nextLast = page.values().get(page.values().size() - 1).key();
                    return scanProtections(
                            object, page.continuation(), values, nextLast, deadline);
                });
    }

    private CompletableFuture<OptionalLong> activeLeaseRetryAt(
            GcPlan plan, long now, MaterializationDeadline deadline) {
        return scanReaderLeases(
                plan.candidate().object().objectKeyHash(),
                Optional.empty(),
                null,
                now,
                OptionalLong.empty(),
                deadline);
    }

    private CompletableFuture<OptionalLong> scanReaderLeases(
            ObjectKeyHash object,
            Optional<F4ScanToken> continuation,
            String lastKey,
            long now,
            OptionalLong retryAt,
            MaterializationDeadline deadline) {
        return deadline.bound(
                        () -> metadataStore.scanReaderLeases(
                                cluster,
                                object,
                                continuation,
                                config.metadataScanPageSize()),
                        "scan physical-object reader leases")
                .thenCompose(page -> {
                    requireIncreasingPage(page, lastKey);
                    long maximumRetry = retryAt.orElse(Long.MIN_VALUE);
                    for (VersionedReaderLease lease : page.values()) {
                        if (!lease.value().objectKeyHash().equals(object.value())) {
                            return CompletableFuture.failedFuture(invariant(
                                    "reader-lease scan escaped its object identity"));
                        }
                        long safeExpiry;
                        try {
                            safeExpiry = Math.addExact(
                                    lease.value().expiresAtMillis(),
                                    config.maximumClockSkew().toMillis());
                        } catch (ArithmeticException overflow) {
                            safeExpiry = Long.MAX_VALUE;
                        }
                        if (safeExpiry >= now) {
                            long candidateRetry = safeExpiry == Long.MAX_VALUE
                                    ? Long.MAX_VALUE
                                    : safeExpiry + 1;
                            maximumRetry = Math.max(maximumRetry, candidateRetry);
                        }
                    }
                    OptionalLong updated = maximumRetry == Long.MIN_VALUE
                            ? OptionalLong.empty()
                            : OptionalLong.of(maximumRetry);
                    if (page.continuation().isEmpty()) {
                        return CompletableFuture.completedFuture(updated);
                    }
                    String nextLast = page.values().get(page.values().size() - 1).key();
                    return scanReaderLeases(
                            object,
                            page.continuation(),
                            nextLast,
                            now,
                            updated,
                            deadline);
                });
    }

    private static void requireIncreasingPage(ObjectProtectionScanPage page, String lastKey) {
        if (lastKey != null && !page.values().isEmpty()
                && page.values().get(0).key().compareTo(lastKey) <= 0) {
            throw invariant("protection scan did not advance monotonically");
        }
    }

    private static void requireIncreasingPage(ReaderLeaseScanPage page, String lastKey) {
        if (lastKey != null && !page.values().isEmpty()
                && page.values().get(0).key().compareTo(lastKey) <= 0) {
            throw invariant("reader-lease scan did not advance monotonically");
        }
    }

    private static PhysicalObjectRootRecord marked(
            PhysicalObjectRootRecord current,
            String attemptId,
            String referenceSetSha256,
            long markedAtMillis,
            long deleteNotBeforeMillis) {
        return copy(
                current,
                PhysicalObjectLifecycle.MARKED,
                Math.addExact(current.lifecycleEpoch(), 1),
                attemptId,
                referenceSetSha256,
                markedAtMillis,
                deleteNotBeforeMillis,
                0,
                0);
    }

    private static PhysicalObjectRootRecord deleting(
            PhysicalObjectRootRecord current, long deleteStartedAtMillis) {
        return copy(
                current,
                PhysicalObjectLifecycle.DELETING,
                Math.addExact(current.lifecycleEpoch(), 1),
                current.gcAttemptId(),
                current.referenceSetSha256(),
                current.markedAtMillis(),
                current.deleteNotBeforeMillis(),
                deleteStartedAtMillis,
                0);
    }

    private static PhysicalObjectRootRecord active(PhysicalObjectRootRecord current) {
        return copy(
                current,
                PhysicalObjectLifecycle.ACTIVE,
                Math.addExact(current.lifecycleEpoch(), 1),
                "",
                "",
                0,
                0,
                0,
                0);
    }

    private static PhysicalObjectRootRecord copy(
            PhysicalObjectRootRecord current,
            PhysicalObjectLifecycle lifecycle,
            long lifecycleEpoch,
            String attemptId,
            String referenceSetSha256,
            long markedAtMillis,
            long deleteNotBeforeMillis,
            long deleteStartedAtMillis,
            long deletedAtMillis) {
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
                lifecycle,
                lifecycleEpoch,
                current.createdAtMillis(),
                current.orphanNotBeforeMillis(),
                attemptId,
                referenceSetSha256,
                markedAtMillis,
                deleteNotBeforeMillis,
                deleteStartedAtMillis,
                deletedAtMillis,
                0,
                "",
                "",
                0);
    }

    private static boolean exactActiveCandidate(
            GcCandidate candidate, VersionedPhysicalObjectRoot root) {
        return root.value().lifecycle() == PhysicalObjectLifecycle.ACTIVE
                && root.metadataVersion() == candidate.rootMetadataVersion()
                && root.value().lifecycleEpoch() == candidate.rootLifecycleEpoch()
                && PhysicalObjectIdentity.from(root.value()).equals(candidate.object());
    }

    private static boolean exactMarkedPlan(
            GcPlan plan, VersionedPhysicalObjectRoot root) {
        return root.value().lifecycle() == PhysicalObjectLifecycle.MARKED
                && root.metadataVersion() == plan.markedRootMetadataVersion()
                && root.value().lifecycleEpoch() == plan.markedRootLifecycleEpoch()
                && root.value().gcAttemptId().equals(plan.gcAttemptId())
                && root.value().referenceSetSha256().equals(plan.referenceSetSha256().value())
                && root.value().deleteNotBeforeMillis() == plan.deleteNotBeforeMillis()
                && PhysicalObjectIdentity.from(root.value()).equals(plan.candidate().object());
    }

    private static boolean exactMarked(
            VersionedPhysicalObjectRoot root,
            PhysicalObjectIdentity object,
            String attemptId,
            String referenceSetSha256,
            long lifecycleEpoch,
            PhysicalObjectRootRecord replacement) {
        return root.value().lifecycle() == PhysicalObjectLifecycle.MARKED
                && root.value().lifecycleEpoch() == lifecycleEpoch
                && root.value().gcAttemptId().equals(attemptId)
                && root.value().referenceSetSha256().equals(referenceSetSha256)
                && PhysicalObjectIdentity.from(root.value()).equals(object)
                && root.value().withMetadataVersion(0).equals(replacement);
    }

    private static boolean exactReplacement(
            VersionedPhysicalObjectRoot root, PhysicalObjectRootRecord replacement) {
        return root.value().withMetadataVersion(0).equals(replacement);
    }

    private long nonNegativeNow() {
        long now = clock.millis();
        if (now < 0) {
            throw new IllegalStateException("physical GC clock cannot be negative");
        }
        return now;
    }

    private static String requireText(String value, String name) {
        Objects.requireNonNull(value, name);
        if (value.isBlank()) {
            throw new IllegalArgumentException(name + " must be non-blank");
        }
        return value;
    }

    private static NereusException invariant(String message) {
        return new NereusException(ErrorCode.METADATA_INVARIANT_VIOLATION, false, message);
    }

    private static Throwable unwrap(Throwable failure) {
        Throwable current = failure;
        while ((current instanceof CompletionException
                        || current instanceof java.util.concurrent.ExecutionException)
                && current.getCause() != null) {
            current = current.getCause();
        }
        return current;
    }

    private static CompletionException propagate(Throwable failure) {
        return failure instanceof CompletionException completion
                ? completion
                : new CompletionException(failure);
    }

    private record ProtectionScan(
            List<GcPlannedProtectionRemoval> values,
            boolean overLimit) {
        private ProtectionScan {
            values = List.copyOf(values);
        }
    }
}
