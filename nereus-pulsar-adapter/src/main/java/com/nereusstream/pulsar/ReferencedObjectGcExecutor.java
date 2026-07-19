/* Licensed under the Apache License, Version 2.0 */
package com.nereusstream.pulsar;

import com.nereusstream.api.Checksum;
import com.nereusstream.api.ErrorCode;
import com.nereusstream.api.NereusException;
import com.nereusstream.api.ObjectKeyHash;
import com.nereusstream.api.StreamId;
import com.nereusstream.core.physical.GcReferenceQuery;
import com.nereusstream.core.physical.GcReferenceQueryKind;
import com.nereusstream.core.physical.ObjectProtectionManager;
import com.nereusstream.core.physical.ObjectProtectionOwner;
import com.nereusstream.core.physical.ObjectProtectionRequest;
import com.nereusstream.core.physical.PhysicalObjectIdentity;
import com.nereusstream.materialization.MaterializationConfig;
import com.nereusstream.materialization.MaterializationDeadline;
import com.nereusstream.materialization.gc.GcCandidate;
import com.nereusstream.materialization.gc.GcIdGenerator;
import com.nereusstream.materialization.gc.GcPlan;
import com.nereusstream.materialization.gc.GcPlannedMetadataRemoval;
import com.nereusstream.materialization.gc.GcPlannedProtectionRemoval;
import com.nereusstream.materialization.gc.GcReferenceCollection;
import com.nereusstream.materialization.gc.GcReferenceDomainRegistry;
import com.nereusstream.materialization.gc.GcRetirementJournal;
import com.nereusstream.materialization.gc.GcRetirementJournalSnapshot;
import com.nereusstream.materialization.gc.HigherGenerationPreDrainCoordinator;
import com.nereusstream.materialization.gc.HigherGenerationPreDrainStatus;
import com.nereusstream.materialization.gc.PhysicalGcAdvanceResult;
import com.nereusstream.materialization.gc.PhysicalGcAdvanceStatus;
import com.nereusstream.materialization.gc.PhysicalGcConfig;
import com.nereusstream.materialization.gc.PhysicalGcDeletionResult;
import com.nereusstream.materialization.gc.PhysicalGcMarkResult;
import com.nereusstream.materialization.gc.PhysicalGcMarkStatus;
import com.nereusstream.materialization.gc.PhysicalObjectGarbageCollector;
import com.nereusstream.materialization.gc.SourceRetirementCoordinator;
import com.nereusstream.materialization.gc.SourceRetirementPlanBuilder;
import com.nereusstream.metadata.oxia.F4Keyspace;
import com.nereusstream.metadata.oxia.F4ScanToken;
import com.nereusstream.metadata.oxia.ObjectProtectionScanPage;
import com.nereusstream.metadata.oxia.PhysicalObjectMetadataStore;
import com.nereusstream.metadata.oxia.VersionedObjectProtection;
import com.nereusstream.metadata.oxia.VersionedPhysicalObjectRoot;
import com.nereusstream.metadata.oxia.records.ObjectProtectionType;
import com.nereusstream.metadata.oxia.records.PhysicalObjectLifecycle;
import java.time.Clock;
import java.time.Duration;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.Objects;
import java.util.Optional;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.CompletionException;
import java.util.concurrent.ExecutionException;
import java.util.concurrent.ScheduledExecutorService;

/**
 * Discovers generation-owned physical roots from their exact durable protections and drives the shared GC protocol.
 *
 * <p>The protection scan is only candidate discovery. Generation/reference domains, the source-retirement planner,
 * the root-authenticated journal and the central collector remain the correctness owners. A root without a current
 * {@link ObjectProtectionType#VISIBLE_GENERATION} protection is deliberately left to the ownerless-global path.
 */
public final class ReferencedObjectGcExecutor {
    private final String cluster;
    private final PhysicalGcConfig config;
    private final PhysicalObjectMetadataStore physicalMetadata;
    private final ObjectProtectionManager objectProtections;
    private final HigherGenerationPreDrainCoordinator preDrain;
    private final SourceRetirementPlanBuilder sourcePlans;
    private final GcReferenceDomainRegistry referenceDomains;
    private final PhysicalObjectGarbageCollector garbageCollector;
    private final SourceRetirementCoordinator sourceRetirement;
    private final GcRetirementJournal retirementJournal;
    private final GcIdGenerator candidateIds;
    private final Clock clock;
    private final ScheduledExecutorService scheduler;
    private final F4Keyspace keys;
    private final long sourceGraceMillis;
    private final long maximumClockSkewMillis;

    public ReferencedObjectGcExecutor(
            String cluster,
            PhysicalGcConfig config,
            MaterializationConfig materializationConfig,
            PhysicalObjectMetadataStore physicalMetadata,
            ObjectProtectionManager objectProtections,
            HigherGenerationPreDrainCoordinator preDrain,
            SourceRetirementPlanBuilder sourcePlans,
            GcReferenceDomainRegistry referenceDomains,
            PhysicalObjectGarbageCollector garbageCollector,
            SourceRetirementCoordinator sourceRetirement,
            GcRetirementJournal retirementJournal,
            GcIdGenerator candidateIds,
            Clock clock,
            ScheduledExecutorService scheduler) {
        this.cluster = requireText(cluster, "cluster");
        this.config = Objects.requireNonNull(config, "config");
        MaterializationConfig exactMaterializationConfig = Objects.requireNonNull(
                materializationConfig, "materializationConfig");
        this.physicalMetadata = Objects.requireNonNull(
                physicalMetadata, "physicalMetadata");
        this.objectProtections = Objects.requireNonNull(
                objectProtections, "objectProtections");
        this.preDrain = Objects.requireNonNull(preDrain, "preDrain");
        this.sourcePlans = Objects.requireNonNull(sourcePlans, "sourcePlans");
        this.referenceDomains = Objects.requireNonNull(
                referenceDomains, "referenceDomains");
        this.garbageCollector = Objects.requireNonNull(
                garbageCollector, "garbageCollector");
        this.sourceRetirement = Objects.requireNonNull(
                sourceRetirement, "sourceRetirement");
        this.retirementJournal = Objects.requireNonNull(
                retirementJournal, "retirementJournal");
        this.candidateIds = Objects.requireNonNull(candidateIds, "candidateIds");
        this.clock = Objects.requireNonNull(clock, "clock");
        this.scheduler = Objects.requireNonNull(scheduler, "scheduler");
        this.keys = new F4Keyspace(this.cluster);
        this.sourceGraceMillis = requireMillis(
                exactMaterializationConfig.sourceRetirementGrace(),
                "sourceRetirementGrace");
        this.maximumClockSkewMillis = requireMillis(
                this.config.maximumClockSkew(), "maximumClockSkew");
    }

    /** Returns empty only when no exact visible-generation owner identifies this ACTIVE root as a source. */
    public CompletableFuture<Optional<ExecutionResult>> executeActive(
            VersionedPhysicalObjectRoot activeRoot) {
        VersionedPhysicalObjectRoot active = Objects.requireNonNull(
                activeRoot, "activeRoot");
        if (active.value().lifecycle() != PhysicalObjectLifecycle.ACTIVE) {
            return CompletableFuture.failedFuture(new IllegalArgumentException(
                    "referenced discovery requires an exact ACTIVE root"));
        }
        return discover(active).thenCompose(optional -> {
            if (optional.isEmpty()) {
                return CompletableFuture.completedFuture(Optional.empty());
            }
            Discovery discovery = optional.orElseThrow();
            long now = nonNegativeNow();
            long notBefore = sourceNotBefore(active);
            if (now < active.value().createdAtMillis() || now < notBefore) {
                return handled(ExecutionResult.notEligible(
                        discovery.object().objectKeyHash(),
                        "source-retirement grace is pending"));
            }
            GcCandidate candidate = GcCandidate.fromActiveRoot(
                    config,
                    candidateIds.next(),
                    active,
                    discovery.query(),
                    discovery.evidence(),
                    now,
                    notBefore);
            return recoverConditionAsNotEligible(
                    preDrain.preDrain(candidate).thenCompose(result -> {
                        if (result.status() == HigherGenerationPreDrainStatus.NOT_ELIGIBLE_YET
                                || result.status() == HigherGenerationPreDrainStatus.MUTATION_DISABLED) {
                            return CompletableFuture.completedFuture(
                                    ExecutionResult.notEligible(
                                            candidate.object().objectKeyHash(),
                                            "higher-generation pre-drain status is "
                                                    + result.status()));
                        }
                        return sourcePlans.build(candidate).thenCompose(removals -> {
                            if (removals.isEmpty()) {
                                return CompletableFuture.completedFuture(
                                        ExecutionResult.notEligible(
                                                candidate.object().objectKeyHash(),
                                                "source-retirement plan is empty"));
                            }
                            Optional<String> ownerMismatch = protectionOwnerMismatch(
                                    discovery.protections(), removals);
                            if (ownerMismatch.isPresent()) {
                                return CompletableFuture.completedFuture(
                                        ExecutionResult.notEligible(
                                                candidate.object().objectKeyHash(),
                                                ownerMismatch.orElseThrow()));
                            }
                            return rebindStaleProtectionEpochs(
                                            discovery, removals, candidate)
                                    .thenCompose(rebound -> {
                                        if (rebound > 0) {
                                            return CompletableFuture.completedFuture(
                                                    ExecutionResult.notEligible(
                                                            candidate.object().objectKeyHash(),
                                                            rebound
                                                                    + " physical protection(s) rebound to the current ACTIVE root epoch; retry"));
                                        }
                                        Optional<String> protectionMismatch =
                                                protectionMismatch(
                                                        discovery.protections(),
                                                        removals,
                                                        candidate);
                                        if (protectionMismatch.isPresent()) {
                                            return CompletableFuture.completedFuture(
                                                    ExecutionResult.notEligible(
                                                            candidate.object().objectKeyHash(),
                                                            protectionMismatch.orElseThrow()));
                                        }
                                        return garbageCollector.mark(
                                                        candidate,
                                                        discovery.protections(),
                                                        removals)
                                                .thenCompose(mark -> mark.status()
                                                                == PhysicalGcMarkStatus.MARKED
                                                        ? advance(
                                                                mark.plan().orElseThrow(),
                                                                Optional.of(mark))
                                                        : CompletableFuture.completedFuture(
                                                                ExecutionResult.markOnly(
                                                                        candidate.object()
                                                                                .objectKeyHash(),
                                                                        mark)));
                                    });
                        });
                    }), discovery.object().objectKeyHash()).thenApply(Optional::of);
        });
    }

    /**
     * Returns empty when the current referenced discovery identity cannot reproduce this MARKED journal query. The
     * caller must then use the ownerless-global recovery path, which either reconstructs its own exact plan or safely
     * rolls the pre-delete root back to ACTIVE.
     */
    public CompletableFuture<Optional<ExecutionResult>> recoverMarked(
            VersionedPhysicalObjectRoot markedRoot) {
        VersionedPhysicalObjectRoot marked = Objects.requireNonNull(
                markedRoot, "markedRoot");
        if (marked.value().lifecycle() != PhysicalObjectLifecycle.MARKED) {
            return CompletableFuture.failedFuture(new IllegalArgumentException(
                    "referenced recovery requires an exact MARKED root"));
        }
        return discover(marked).thenCompose(optional -> {
            if (optional.isEmpty()) {
                return CompletableFuture.completedFuture(Optional.empty());
            }
            Discovery discovery = optional.orElseThrow();
            MaterializationDeadline deadline = new MaterializationDeadline(
                    config.operationTimeout(), scheduler);
            CompletableFuture<Optional<ExecutionResult>> result = deadline.bound(
                            () -> retirementJournal.load(
                                    discovery.object().objectKeyHash(),
                                    marked.value().gcAttemptId(),
                                    deadline),
                            "load referenced-object GC retirement journal")
                    .thenCompose(journal -> recoverMarked(
                            marked, discovery, requireJournal(journal, marked)));
            result.whenComplete((ignored, failure) -> deadline.close());
            return result;
        });
    }

    private CompletableFuture<Optional<ExecutionResult>> recoverMarked(
            VersionedPhysicalObjectRoot marked,
            Discovery discovery,
            GcRetirementJournalSnapshot journal) {
        if (!journal.queryIdentitySha256().equals(
                discovery.query().queryIdentitySha256())) {
            return CompletableFuture.completedFuture(Optional.empty());
        }
        GcCandidate candidate = GcCandidate.fromMarkedRoot(
                config,
                candidateIds.next(),
                marked,
                discovery.query(),
                discovery.evidence(),
                nonNegativeNow());
        return sourcePlans.reload(candidate, journal.plannedMetadataRemovals())
                .thenCompose(removals -> {
                    if (!removals.equals(journal.plannedMetadataRemovals())
                            || !discovery.protections().equals(
                                    journal.plannedProtectionRemovals())) {
                        return unmark(marked);
                    }
                    return referenceDomains.snapshotForDeletion(discovery.query())
                            .thenCompose(collection -> reconstructAndAdvance(
                                    marked,
                                    candidate,
                                    journal,
                                    collection));
                })
                .handle((execution, failure) -> {
                    if (failure == null) {
                        return CompletableFuture.completedFuture(Optional.of(execution));
                    }
                    Throwable exact = unwrap(failure);
                    if (!isConditionFailure(exact)) {
                        return CompletableFuture.<Optional<ExecutionResult>>failedFuture(exact);
                    }
                    return unmark(marked).thenApply(Optional::of);
                })
                .thenCompose(value -> value);
    }

    private CompletableFuture<ExecutionResult> reconstructAndAdvance(
            VersionedPhysicalObjectRoot marked,
            GcCandidate candidate,
            GcRetirementJournalSnapshot journal,
            GcReferenceCollection collection) {
        if (!collection.clear()) {
            return unmark(marked);
        }
        final GcPlan plan;
        try {
            plan = GcPlan.fromMarkedRoot(
                    config,
                    marked.value().gcAttemptId(),
                    candidate,
                    collection.snapshots(),
                    journal.plannedProtectionRemovals(),
                    journal.plannedMetadataRemovals(),
                    marked);
        } catch (IllegalArgumentException drift) {
            return unmark(marked);
        }
        return advance(plan, Optional.empty());
    }

    private CompletableFuture<ExecutionResult> advance(
            GcPlan plan, Optional<PhysicalGcMarkResult> mark) {
        return garbageCollector.advanceToDeleteIntent(
                        plan,
                        ignored -> sourcePlans.reload(
                                        plan.candidate(),
                                        plan.plannedMetadataRemovals())
                                .thenApply(plan.plannedMetadataRemovals()::equals))
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
        ObjectKeyHash object = new ObjectKeyHash(marked.value().objectKeyHash());
        return garbageCollector.unmarkDrifted(marked)
                .thenApply(advance -> ExecutionResult.advanced(
                        object, Optional.empty(), advance));
    }

    private CompletableFuture<Optional<Discovery>> discover(
            VersionedPhysicalObjectRoot root) {
        PhysicalObjectIdentity object = PhysicalObjectIdentity.from(root.value());
        MaterializationDeadline deadline = new MaterializationDeadline(
                config.operationTimeout(), scheduler);
        CompletableFuture<Optional<Discovery>> result = scanProtections(
                        object.objectKeyHash(),
                        Optional.empty(),
                        new ArrayList<>(),
                        null,
                        deadline)
                .thenApply(protections -> discovery(object, protections));
        result.whenComplete((ignored, failure) -> deadline.close());
        return result;
    }

    private CompletableFuture<List<GcPlannedProtectionRemoval>> scanProtections(
            ObjectKeyHash object,
            Optional<F4ScanToken> continuation,
            ArrayList<GcPlannedProtectionRemoval> values,
            String previousKey,
            MaterializationDeadline deadline) {
        return deadline.bound(
                        () -> physicalMetadata.scanProtections(
                                cluster,
                                object,
                                continuation,
                                config.metadataScanPageSize()),
                        "scan referenced-object physical protections")
                .thenCompose(page -> {
                    requireProgress(page, previousKey);
                    for (VersionedObjectProtection protection : page.values()) {
                        values.add(new GcPlannedProtectionRemoval(protection));
                        if (values.size() > config.maxReferencesPerDomainSnapshot()) {
                            return CompletableFuture.failedFuture(new NereusException(
                                    ErrorCode.METADATA_INVARIANT_VIOLATION,
                                    false,
                                    "referenced-object protection scan exceeded its configured bound"));
                        }
                    }
                    if (page.continuation().isEmpty()) {
                        return CompletableFuture.completedFuture(List.copyOf(values));
                    }
                    String lastKey = page.values().get(page.values().size() - 1).key();
                    return scanProtections(
                            object,
                            page.continuation(),
                            values,
                            lastKey,
                            deadline);
                });
    }

    private Optional<Discovery> discovery(
            PhysicalObjectIdentity object,
            List<GcPlannedProtectionRemoval> protections) {
        ArrayList<StreamId> streams = new ArrayList<>();
        for (GcPlannedProtectionRemoval removal : protections) {
            VersionedObjectProtection protection = removal.protection();
            if (!protection.value().objectKeyHash().equals(
                    object.objectKeyHash().value())) {
                throw invariant("protection scan escaped its physical object identity");
            }
            if (ObjectProtectionType.fromWireId(
                            protection.value().protectionTypeId())
                    == ObjectProtectionType.VISIBLE_GENERATION) {
                StreamId stream = keys.parseGenerationIndexKey(
                                protection.value().ownerKey())
                        .streamId();
                streams.add(stream);
            }
        }
        List<StreamId> exactStreams = streams.stream()
                .distinct()
                .sorted(java.util.Comparator.comparing(StreamId::value))
                .toList();
        if (exactStreams.isEmpty()) {
            return Optional.empty();
        }
        if (exactStreams.size() > config.maxStreamsPerCandidate()) {
            throw invariant("referenced-object affected-stream set exceeded its configured bound");
        }
        Checksum evidence = object.identitySha256();
        GcReferenceQuery query = GcReferenceQuery.create(
                GcReferenceQueryKind.REFERENCED_OBJECT,
                object,
                exactStreams,
                evidence);
        return Optional.of(new Discovery(object, query, evidence, protections));
    }

    private Optional<String> protectionMismatch(
            List<GcPlannedProtectionRemoval> protections,
            List<GcPlannedMetadataRemoval> removals,
            GcCandidate candidate) {
        for (GcPlannedProtectionRemoval protection : protections) {
            VersionedObjectProtection exact = protection.protection();
            ObjectProtectionType type = ObjectProtectionType.fromWireId(
                    exact.value().protectionTypeId());
            if (exact.value().rootLifecycleEpoch() != candidate.activeRootLifecycleEpoch()) {
                return Optional.of(
                        "physical protection " + type
                                + " has root lifecycle epoch "
                                + exact.value().rootLifecycleEpoch()
                                + ", expected "
                                + candidate.activeRootLifecycleEpoch());
            }
        }
        return protectionOwnerMismatch(protections, removals);
    }

    private Optional<String> protectionOwnerMismatch(
            List<GcPlannedProtectionRemoval> protections,
            List<GcPlannedMetadataRemoval> removals) {
        Map<String, GcPlannedMetadataRemoval> byKey = metadataRemovalsByKey(removals);
        for (GcPlannedProtectionRemoval protection : protections) {
            VersionedObjectProtection exact = protection.protection();
            ObjectProtectionType type = ObjectProtectionType.fromWireId(
                    exact.value().protectionTypeId());
            GcPlannedMetadataRemoval owner = byKey.get(exact.value().ownerKey());
            if (owner == null) {
                return Optional.of(
                        "physical protection " + type
                                + " has no source-retirement owner removal: "
                                + exact.value().ownerKey());
            }
            if (owner.metadataVersion() != exact.value().ownerMetadataVersion()) {
                return Optional.of(
                        "physical protection " + type
                                + " owner metadata version is "
                                + exact.value().ownerMetadataVersion()
                                + ", expected "
                                + owner.metadataVersion());
            }
            if (!owner.durableValueSha256().value().equals(
                    exact.value().ownerIdentitySha256())) {
                return Optional.of(
                        "physical protection " + type
                                + " owner identity differs from source-retirement removal");
            }
        }
        return protections.isEmpty()
                ? Optional.of("referenced physical root has no current protections")
                : Optional.empty();
    }

    private CompletableFuture<Integer> rebindStaleProtectionEpochs(
            Discovery discovery,
            List<GcPlannedMetadataRemoval> removals,
            GcCandidate candidate) {
        Map<String, GcPlannedMetadataRemoval> byKey = metadataRemovalsByKey(removals);
        CompletableFuture<Integer> rebound = CompletableFuture.completedFuture(0);
        for (GcPlannedProtectionRemoval plannedProtection : discovery.protections()) {
            VersionedObjectProtection protection = plannedProtection.protection();
            if (protection.value().rootLifecycleEpoch()
                    >= candidate.activeRootLifecycleEpoch()) {
                continue;
            }
            GcPlannedMetadataRemoval removal = byKey.get(
                    protection.value().ownerKey());
            if (removal == null) {
                throw invariant("protection epoch rebind lost its exact owner removal");
            }
            ObjectProtectionOwner owner = new ObjectProtectionOwner(
                    removal.key(),
                    removal.metadataVersion(),
                    removal.durableValueSha256());
            ObjectProtectionRequest request = new ObjectProtectionRequest(
                    discovery.object(),
                    ObjectProtectionType.fromWireId(
                            protection.value().protectionTypeId()),
                    protection.value().referenceId(),
                    owner,
                    protection.value().expiresAtMillis());
            rebound = rebound.thenCompose(count -> objectProtections.acquireOrTransfer(
                            request,
                            expected -> revalidateProtectionOwner(
                                    candidate, removals, owner, expected))
                    .thenApply(ignored -> count + 1));
        }
        return rebound;
    }

    private CompletableFuture<Void> revalidateProtectionOwner(
            GcCandidate candidate,
            List<GcPlannedMetadataRemoval> expectedRemovals,
            ObjectProtectionOwner plannedOwner,
            ObjectProtectionOwner requestedOwner) {
        if (!plannedOwner.equals(requestedOwner)) {
            return CompletableFuture.failedFuture(invariant(
                    "protection manager revalidated an unexpected source owner"));
        }
        return sourcePlans.reload(candidate, expectedRemovals)
                .thenCompose(current -> current.equals(expectedRemovals)
                        ? CompletableFuture.completedFuture(null)
                        : CompletableFuture.failedFuture(condition(
                                "source-retirement owner changed during protection epoch rebind")));
    }

    private Map<String, GcPlannedMetadataRemoval> metadataRemovalsByKey(
            List<GcPlannedMetadataRemoval> removals) {
        Map<String, GcPlannedMetadataRemoval> byKey = new HashMap<>();
        for (GcPlannedMetadataRemoval removal : removals) {
            if (byKey.put(removal.key(), removal) != null) {
                throw invariant("source-retirement removal keys are not unique");
            }
        }
        return byKey;
    }

    private long sourceNotBefore(VersionedPhysicalObjectRoot root) {
        long grace;
        try {
            grace = Math.addExact(sourceGraceMillis, maximumClockSkewMillis);
        } catch (ArithmeticException overflow) {
            return Long.MAX_VALUE;
        }
        long sourceBoundary;
        try {
            sourceBoundary = Math.addExact(root.value().createdAtMillis(), grace);
        } catch (ArithmeticException overflow) {
            sourceBoundary = Long.MAX_VALUE;
        }
        return Math.max(root.value().orphanNotBeforeMillis(), sourceBoundary);
    }

    private CompletableFuture<ExecutionResult> recoverConditionAsNotEligible(
            CompletableFuture<ExecutionResult> operation,
            ObjectKeyHash object) {
        return operation.handle((value, failure) -> {
                    if (failure == null) {
                        return CompletableFuture.completedFuture(value);
                    }
                    Throwable exact = unwrap(failure);
                    return isConditionFailure(exact)
                            ? CompletableFuture.completedFuture(
                                    ExecutionResult.notEligible(
                                            object,
                                            exact.getMessage() == null
                                                    ? "source-retirement condition failed"
                                                    : exact.getMessage()))
                            : CompletableFuture.<ExecutionResult>failedFuture(exact);
                })
                .thenCompose(value -> value);
    }

    private static GcRetirementJournalSnapshot requireJournal(
            Optional<GcRetirementJournalSnapshot> optional,
            VersionedPhysicalObjectRoot root) {
        GcRetirementJournalSnapshot journal = optional.orElseThrow(() -> invariant(
                "MARKED referenced-object root is missing its retirement journal"));
        if (!journal.object().value().equals(root.value().objectKeyHash())
                || !journal.gcAttemptId().equals(root.value().gcAttemptId())
                || !journal.referenceSetSha256().value().equals(
                        root.value().referenceSetSha256())) {
            throw invariant("MARKED referenced-object root differs from its retirement journal");
        }
        return journal;
    }

    private static void requireProgress(
            ObjectProtectionScanPage page,
            String previousKey) {
        if (previousKey != null
                && !page.values().isEmpty()
                && page.values().get(0).key().compareTo(previousKey) <= 0) {
            throw invariant("referenced-object protection scan did not advance");
        }
        if (page.continuation().isPresent() && page.values().isEmpty()) {
            throw invariant("referenced-object protection scan returned an empty continuation page");
        }
    }

    private static CompletableFuture<Optional<ExecutionResult>> handled(
            ExecutionResult result) {
        return CompletableFuture.completedFuture(Optional.of(result));
    }

    private long nonNegativeNow() {
        long now = clock.millis();
        if (now < 0) {
            throw new IllegalStateException("clock returned a negative epoch millisecond");
        }
        return now;
    }

    private static boolean isConditionFailure(Throwable failure) {
        return failure instanceof NereusException nereus
                && nereus.code() == ErrorCode.METADATA_CONDITION_FAILED;
    }

    private static Throwable unwrap(Throwable failure) {
        Throwable current = failure;
        while ((current instanceof CompletionException
                        || current instanceof ExecutionException)
                && current.getCause() != null) {
            current = current.getCause();
        }
        return current;
    }

    private static long requireMillis(Duration value, String field) {
        Objects.requireNonNull(value, field);
        try {
            long millis = value.toMillis();
            if (value.isNegative()) {
                throw new IllegalArgumentException(field + " must be non-negative");
            }
            return millis;
        } catch (ArithmeticException failure) {
            throw new IllegalArgumentException(field + " does not fit milliseconds", failure);
        }
    }

    private static String requireText(String value, String field) {
        Objects.requireNonNull(value, field);
        if (value.isBlank()) {
            throw new IllegalArgumentException(field + " cannot be blank");
        }
        return value;
    }

    private static NereusException invariant(String message) {
        return new NereusException(
                ErrorCode.METADATA_INVARIANT_VIOLATION, false, message);
    }

    private static NereusException condition(String message) {
        return new NereusException(
                ErrorCode.METADATA_CONDITION_FAILED, true, message);
    }

    private record Discovery(
            PhysicalObjectIdentity object,
            GcReferenceQuery query,
            Checksum evidence,
            List<GcPlannedProtectionRemoval> protections) {
        private Discovery {
            Objects.requireNonNull(object, "object");
            Objects.requireNonNull(query, "query");
            Objects.requireNonNull(evidence, "evidence");
            protections = List.copyOf(Objects.requireNonNull(
                    protections, "protections"));
        }
    }

    /** Exact progress for one referenced root visit; NOT_ELIGIBLE is a handled, fail-closed no-op. */
    public record ExecutionResult(
            ObjectKeyHash object,
            boolean eligible,
            Optional<String> eligibilityBlocker,
            Optional<PhysicalGcMarkResult> mark,
            Optional<PhysicalGcAdvanceResult> advance,
            Optional<PhysicalGcDeletionResult> deletion) {
        public ExecutionResult {
            Objects.requireNonNull(object, "object");
            eligibilityBlocker = Objects.requireNonNull(
                    eligibilityBlocker, "eligibilityBlocker");
            mark = Objects.requireNonNull(mark, "mark");
            advance = Objects.requireNonNull(advance, "advance");
            deletion = Objects.requireNonNull(deletion, "deletion");
            if (deletion.isPresent()
                    && (advance.isEmpty()
                            || advance.orElseThrow().status()
                                    != PhysicalGcAdvanceStatus.DELETE_INTENT)) {
                throw new IllegalArgumentException(
                        "referenced-object deletion requires durable delete intent");
            }
            if (eligible == eligibilityBlocker.isPresent()) {
                throw new IllegalArgumentException(
                        "only an ineligible referenced-object result carries a blocker");
            }
        }

        private static ExecutionResult notEligible(
                ObjectKeyHash object, String blocker) {
            return new ExecutionResult(
                    object,
                    false,
                    Optional.of(requireText(blocker, "eligibilityBlocker")),
                    Optional.empty(),
                    Optional.empty(),
                    Optional.empty());
        }

        private static ExecutionResult markOnly(
                ObjectKeyHash object, PhysicalGcMarkResult mark) {
            return new ExecutionResult(
                    object,
                    true,
                    Optional.empty(),
                    Optional.of(mark),
                    Optional.empty(),
                    Optional.empty());
        }

        private static ExecutionResult advanced(
                ObjectKeyHash object,
                Optional<PhysicalGcMarkResult> mark,
                PhysicalGcAdvanceResult advance) {
            return new ExecutionResult(
                    object,
                    true,
                    Optional.empty(),
                    mark,
                    Optional.of(advance),
                    Optional.empty());
        }

        private static ExecutionResult deleted(
                ObjectKeyHash object,
                Optional<PhysicalGcMarkResult> mark,
                PhysicalGcAdvanceResult advance,
                PhysicalGcDeletionResult deletion) {
            return new ExecutionResult(
                    object,
                    true,
                    Optional.empty(),
                    mark,
                    Optional.of(advance),
                    Optional.of(deletion));
        }
    }
}
