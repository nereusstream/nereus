/* Licensed under the Apache License, Version 2.0 */
package com.nereusstream.materialization.gc;

import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.assertThatThrownBy;

import com.nereusstream.api.Checksum;
import com.nereusstream.api.ChecksumType;
import com.nereusstream.api.ObjectKey;
import com.nereusstream.api.ObjectKeyHash;
import com.nereusstream.api.StreamId;
import com.nereusstream.core.capability.GenerationActivationProof;
import com.nereusstream.core.capability.GenerationActivationSubject;
import com.nereusstream.core.capability.GenerationOperation;
import com.nereusstream.core.capability.GenerationProtocolActivationGuard;
import com.nereusstream.core.physical.GcAuthorityToken;
import com.nereusstream.core.physical.GcReferenceDomain;
import com.nereusstream.core.physical.GcReferenceQuery;
import com.nereusstream.core.physical.GcReferenceQueryKind;
import com.nereusstream.core.physical.GcReferenceSnapshot;
import com.nereusstream.core.physical.PhysicalObjectIdentity;
import com.nereusstream.core.physical.PhysicalObjectKind;
import com.nereusstream.materialization.MaterializationDeadline;
import com.nereusstream.metadata.oxia.FakePhysicalObjectMetadataStore;
import com.nereusstream.metadata.oxia.VersionedObjectProtection;
import com.nereusstream.metadata.oxia.VersionedPhysicalObjectRoot;
import com.nereusstream.metadata.oxia.VersionedReaderLease;
import com.nereusstream.metadata.oxia.records.ObjectProtectionRecord;
import com.nereusstream.metadata.oxia.records.ObjectProtectionType;
import com.nereusstream.metadata.oxia.records.ObjectReaderLeaseRecord;
import com.nereusstream.metadata.oxia.records.PhysicalObjectLifecycle;
import com.nereusstream.metadata.oxia.records.PhysicalObjectRootRecord;
import java.time.Clock;
import java.time.Duration;
import java.time.Instant;
import java.time.ZoneId;
import java.time.ZoneOffset;
import java.util.List;
import java.util.Optional;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.Executors;
import java.util.concurrent.ScheduledExecutorService;
import java.util.concurrent.atomic.AtomicInteger;
import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.Test;

class PhysicalObjectGarbageCollectorTest {
    private static final String CLUSTER = "cluster-a";
    private static final String CANDIDATE_ID = "a".repeat(52);
    private static final String ATTEMPT_ID = "b".repeat(52);
    private static final Checksum SHA_A = sha('a');
    private static final Checksum SHA_B = sha('b');
    private static final Checksum CRC = new Checksum(ChecksumType.CRC32C, "01020304");

    private final ScheduledExecutorService scheduler = Executors.newSingleThreadScheduledExecutor();

    @AfterEach
    void closeScheduler() {
        scheduler.shutdownNow();
    }

    @Test
    void disabledAndDryRunModesNeverReadDomainsOrMutateRoots() {
        FakePhysicalObjectMetadataStore store = new FakePhysicalObjectMetadataStore();
        MutableClock clock = new MutableClock(1_000);
        VersionedPhysicalObjectRoot active = createActiveRoot(store);
        GcReferenceQuery query = query(active);
        TrackingDomain projection = new TrackingDomain("projection-generation-v1");
        TrackingDomain generation = new TrackingDomain("generation-v1");

        PhysicalGcMarkResult disabled = collector(
                        PhysicalGcConfig.defaults(), store, clock, projection, generation)
                .mark(candidate(PhysicalGcConfig.defaults(), active, query), List.of(), List.of())
                .join();
        PhysicalGcConfig dryRun = config(true, true);
        PhysicalGcMarkResult dry = collector(dryRun, store, clock, projection, generation)
                .mark(candidate(dryRun, active, query), List.of(), List.of())
                .join();

        assertThat(disabled.status()).isEqualTo(PhysicalGcMarkStatus.DISABLED);
        assertThat(dry.status()).isEqualTo(PhysicalGcMarkStatus.DRY_RUN);
        assertThat(projection.snapshotCalls.get()).isZero();
        assertThat(generation.snapshotCalls.get()).isZero();
        assertThat(store.getRoot(CLUSTER, activeIdentity(active).objectKeyHash()).join()
                        .orElseThrow().value().lifecycle())
                .isEqualTo(PhysicalObjectLifecycle.ACTIVE);
    }

    @Test
    void markBindsExactFactsAndRecoversALostCasResponse() {
        LostRootCasResponseStore store = new LostRootCasResponseStore(PhysicalObjectLifecycle.MARKED);
        PhysicalGcConfig config = config(true, false);
        MutableClock clock = new MutableClock(1_000);
        VersionedPhysicalObjectRoot active = createActiveRoot(store);
        GcReferenceQuery query = query(active);
        GcCandidate candidate = candidate(config, active, query);
        GcPlannedProtectionRemoval protection = createProtection(store, active, 1);
        TrackingDomain projection = new TrackingDomain("projection-generation-v1");
        TrackingDomain generation = new TrackingDomain("generation-v1");
        TrackingActivationGuard activation = new TrackingActivationGuard();
        PhysicalObjectGarbageCollector collector = collector(
                config, store, clock, activation, projection, generation);

        PhysicalGcMarkResult result = collector.mark(
                candidate,
                List.of(protection),
                List.of(new GcPlannedMetadataRemoval(
                        "generation-index", "/metadata/index-a", 7, SHA_A)))
                .join();

        assertThat(result.status()).isEqualTo(PhysicalGcMarkStatus.MARKED);
        GcPlan plan = result.plan().orElseThrow();
        assertThat(plan.gcAttemptId()).isEqualTo(ATTEMPT_ID);
        assertThat(plan.deleteNotBeforeMillis()).isEqualTo(12_000);
        assertThat(plan.plannedProtectionRemovals()).containsExactly(protection);
        assertThat(plan.plannedMetadataRemovals()).hasSize(1);
        assertThat(store.getRoot(CLUSTER, candidate.object().objectKeyHash()).join()
                        .orElseThrow().value().lifecycle())
                .isEqualTo(PhysicalObjectLifecycle.MARKED);
        assertThat(store.getRetirementManifest(
                        CLUSTER, candidate.object().objectKeyHash(), ATTEMPT_ID)
                .join())
                .isPresent();
        assertThat(activation.requireCalls.get()).isEqualTo(1);
        assertThat(activation.revalidateCalls.get()).isEqualTo(1);
        assertThat(projection.snapshotCalls.get()).isEqualTo(1);
        assertThat(generation.snapshotCalls.get()).isEqualTo(1);
    }

    @Test
    void journalPrepareFailureLeavesRootActiveBeforeActivationOrMark() {
        PhysicalGcConfig config = config(true, false);
        FakePhysicalObjectMetadataStore store = new FakePhysicalObjectMetadataStore();
        MutableClock clock = new MutableClock(1_000);
        VersionedPhysicalObjectRoot active = createActiveRoot(store);
        TrackingActivationGuard activation = new TrackingActivationGuard();
        GcReferenceDomainRegistry registry = new GcReferenceDomainRegistry(
                config,
                scheduler,
                List.of(
                        new TrackingDomain("generation-v1"),
                        new TrackingDomain("projection-generation-v1")));
        PhysicalObjectGarbageCollector collector = collector(
                config,
                store,
                clock,
                activation,
                (candidate, expected) -> CompletableFuture.completedFuture(expected),
                registry,
                new FailingRetirementJournal());

        assertThatThrownBy(() -> collector.mark(
                        candidate(config, active, query(active)), List.of(), List.of())
                .join()).hasRootCauseMessage("injected journal prepare failure");

        assertThat(activation.requireCalls.get()).isZero();
        assertThat(store.getRoot(CLUSTER, activeIdentity(active).objectKeyHash()).join()
                        .orElseThrow().value().lifecycle())
                .isEqualTo(PhysicalObjectLifecycle.ACTIVE);
    }

    @Test
    void markRejectsPrematureVetoedAndProtectionDriftedCandidatesWithoutRootMutation() {
        PhysicalGcConfig config = config(true, false);
        FakePhysicalObjectMetadataStore store = new FakePhysicalObjectMetadataStore();
        MutableClock clock = new MutableClock(1_000);
        VersionedPhysicalObjectRoot active = createActiveRoot(store);
        GcReferenceQuery query = query(active);
        TrackingDomain projection = new TrackingDomain("projection-generation-v1");
        TrackingDomain generation = new TrackingDomain("generation-v1");
        PhysicalObjectGarbageCollector collector = collector(
                config, store, clock, projection, generation);

        GcCandidate future = GcCandidate.fromActiveRoot(
                config, CANDIDATE_ID, active, query, SHA_A, 1_000, 2_000);
        PhysicalGcMarkResult early = collector.mark(future, List.of(), List.of()).join();
        assertThat(early.status()).isEqualTo(PhysicalGcMarkStatus.NOT_YET_ELIGIBLE);
        assertThat(early.retryAtMillis()).hasValue(2_000);
        assertThat(generation.snapshotCalls.get()).isZero();

        generation.veto = true;
        PhysicalGcMarkResult vetoed = collector.mark(
                candidate(config, active, query), List.of(), List.of()).join();
        assertThat(vetoed.status()).isEqualTo(PhysicalGcMarkStatus.DOMAIN_BLOCKED);
        assertThat(vetoed.domainStatus()).contains(GcReferenceCollectionStatus.VETOED);

        generation.veto = false;
        createProtection(store, active, 1);
        PhysicalGcMarkResult changed = collector.mark(
                candidate(config, active, query), List.of(), List.of()).join();
        assertThat(changed.status()).isEqualTo(PhysicalGcMarkStatus.PLAN_CHANGED);
        assertThat(store.getRoot(CLUSTER, activeIdentity(active).objectKeyHash()).join()
                        .orElseThrow().value().lifecycle())
                .isEqualTo(PhysicalObjectLifecycle.ACTIVE);
    }

    @Test
    void drainWaitsForGraceAndEveryReaderLeaseBeforeEnteringDeleteIntent() {
        PhysicalGcConfig config = config(true, false);
        FakePhysicalObjectMetadataStore store = new FakePhysicalObjectMetadataStore();
        MutableClock clock = new MutableClock(1_000);
        VersionedPhysicalObjectRoot active = createActiveRoot(store);
        GcReferenceQuery query = query(active);
        GcPlannedProtectionRemoval protection = createProtection(store, active, 1);
        TrackingDomain projection = new TrackingDomain("projection-generation-v1");
        TrackingDomain generation = new TrackingDomain("generation-v1");
        TrackingActivationGuard activation = new TrackingActivationGuard();
        PhysicalObjectGarbageCollector collector = collector(
                config, store, clock, activation, projection, generation);
        GcPlan plan = collector.mark(
                candidate(config, active, query), List.of(protection), List.of())
                .join().plan().orElseThrow();

        PhysicalGcAdvanceResult grace = collector.advanceToDeleteIntent(plan).join();
        assertThat(grace.status()).isEqualTo(PhysicalGcAdvanceStatus.WAITING_FOR_GRACE);
        assertThat(grace.retryAtMillis()).hasValue(12_000);

        clock.setMillis(12_001);
        VersionedReaderLease first = createLease(
                store, plan, "c".repeat(52), "d".repeat(52), 15_000);
        createLease(store, plan, "e".repeat(52), "f".repeat(52), 16_000);
        PhysicalGcAdvanceResult readers = collector.advanceToDeleteIntent(plan).join();
        assertThat(readers.status()).isEqualTo(PhysicalGcAdvanceStatus.WAITING_FOR_READERS);
        assertThat(readers.retryAtMillis()).hasValue(16_006);

        store.deleteReaderLease(
                CLUSTER,
                plan.candidate().object().objectKeyHash(),
                first.value().processRunId(),
                first.metadataVersion()).join();
        clock.setMillis(16_006);
        PhysicalGcAdvanceResult intent = collector.advanceToDeleteIntent(plan).join();

        assertThat(intent.status()).isEqualTo(PhysicalGcAdvanceStatus.DELETE_INTENT);
        VersionedPhysicalObjectRoot deleting = intent.root().orElseThrow();
        assertThat(deleting.value().lifecycle()).isEqualTo(PhysicalObjectLifecycle.DELETING);
        assertThat(deleting.value().lifecycleEpoch())
                .isEqualTo(plan.markedRootLifecycleEpoch() + 1);
        assertThat(deleting.value().gcAttemptId()).isEqualTo(plan.gcAttemptId());
        assertThat(deleting.value().referenceSetSha256())
                .isEqualTo(plan.referenceSetSha256().value());
        assertThat(activation.revalidateCalls.get()).isGreaterThanOrEqualTo(2);
    }

    @Test
    void deleteIntentReloadsExactJournalAtAdmissionAndFinalFence() {
        PhysicalGcConfig config = config(true, false);
        FakePhysicalObjectMetadataStore store = new FakePhysicalObjectMetadataStore();
        MutableClock clock = new MutableClock(1_000);
        VersionedPhysicalObjectRoot active = createActiveRoot(store);
        TrackingRetirementJournal journal = new TrackingRetirementJournal(
                new DefaultGcRetirementJournal(CLUSTER, store, config), 0);
        PhysicalObjectGarbageCollector collector = collector(
                config,
                store,
                clock,
                new TrackingActivationGuard(),
                (candidate, expected) -> CompletableFuture.completedFuture(expected),
                new GcReferenceDomainRegistry(
                        config,
                        scheduler,
                        List.of(
                                new TrackingDomain("generation-v1"),
                                new TrackingDomain("projection-generation-v1"))),
                journal);
        GcPlan plan = collector.mark(
                        candidate(config, active, query(active)), List.of(), List.of())
                .join().plan().orElseThrow();
        clock.setMillis(12_001);

        PhysicalGcAdvanceResult result = collector.advanceToDeleteIntent(plan).join();

        assertThat(result.status()).isEqualTo(PhysicalGcAdvanceStatus.DELETE_INTENT);
        assertThat(journal.prepareCalls.get()).isEqualTo(1);
        assertThat(journal.loadCalls.get()).isEqualTo(2);
    }

    @Test
    void journalDisappearanceAtFinalFenceLeavesRootMarked() {
        PhysicalGcConfig config = config(true, false);
        FakePhysicalObjectMetadataStore store = new FakePhysicalObjectMetadataStore();
        MutableClock clock = new MutableClock(1_000);
        VersionedPhysicalObjectRoot active = createActiveRoot(store);
        TrackingRetirementJournal journal = new TrackingRetirementJournal(
                new DefaultGcRetirementJournal(CLUSTER, store, config), 2);
        PhysicalObjectGarbageCollector collector = collector(
                config,
                store,
                clock,
                new TrackingActivationGuard(),
                (candidate, expected) -> CompletableFuture.completedFuture(expected),
                new GcReferenceDomainRegistry(
                        config,
                        scheduler,
                        List.of(
                                new TrackingDomain("generation-v1"),
                                new TrackingDomain("projection-generation-v1"))),
                journal);
        GcPlan plan = collector.mark(
                        candidate(config, active, query(active)), List.of(), List.of())
                .join().plan().orElseThrow();
        clock.setMillis(12_001);

        assertThatThrownBy(() -> collector.advanceToDeleteIntent(plan).join())
                .hasRootCauseMessage(
                        "sealed GC retirement journal is missing for the MARKED root");
        assertThat(journal.loadCalls.get()).isEqualTo(2);
        assertThat(store.getRoot(CLUSTER, plan.candidate().object().objectKeyHash()).join()
                        .orElseThrow().value().lifecycle())
                .isEqualTo(PhysicalObjectLifecycle.MARKED);
    }

    @Test
    void exactProtectionOrDomainDriftUnmarksBeforeAnyDestructiveSideEffect() {
        PhysicalGcConfig config = config(true, false);
        MutableClock clock = new MutableClock(1_000);

        FakePhysicalObjectMetadataStore protectionStore = new FakePhysicalObjectMetadataStore();
        VersionedPhysicalObjectRoot protectionActive = createActiveRoot(protectionStore);
        GcPlannedProtectionRemoval protection = createProtection(protectionStore, protectionActive, 1);
        TrackingDomain projection = new TrackingDomain("projection-generation-v1");
        TrackingDomain generation = new TrackingDomain("generation-v1");
        PhysicalObjectGarbageCollector protectionCollector = collector(
                config, protectionStore, clock, projection, generation);
        GcPlan protectionPlan = protectionCollector.mark(
                candidate(config, protectionActive, query(protectionActive)),
                List.of(protection),
                List.of()).join().plan().orElseThrow();
        ObjectProtectionRecord changedOwner = protection.protection().value();
        VersionedObjectProtection changed = protectionStore.compareAndSetProtection(
                CLUSTER,
                new ObjectProtectionRecord(
                        changedOwner.schemaVersion(),
                        changedOwner.objectKeyHash(),
                        changedOwner.protectionTypeId(),
                        changedOwner.referenceId(),
                        "/owner/changed",
                        changedOwner.ownerMetadataVersion() + 1,
                        SHA_B.value(),
                        changedOwner.rootLifecycleEpoch(),
                        changedOwner.createdAtMillis(),
                        changedOwner.expiresAtMillis(),
                        0),
                protection.protection().metadataVersion()).join();
        assertThat(changed.metadataVersion()).isGreaterThan(protection.protection().metadataVersion());
        clock.setMillis(12_001);

        PhysicalGcAdvanceResult protectionDrift = protectionCollector
                .advanceToDeleteIntent(protectionPlan).join();
        assertThat(protectionDrift.status()).isEqualTo(PhysicalGcAdvanceStatus.PLAN_DRIFT_UNMARKED);
        assertThat(protectionDrift.root().orElseThrow().value().lifecycle())
                .isEqualTo(PhysicalObjectLifecycle.ACTIVE);
        assertThat(protectionDrift.root().orElseThrow().value().gcAttemptId()).isEmpty();

        FakePhysicalObjectMetadataStore domainStore = new FakePhysicalObjectMetadataStore();
        clock.setMillis(1_000);
        VersionedPhysicalObjectRoot domainActive = createActiveRoot(domainStore);
        TrackingDomain domainProjection = new TrackingDomain("projection-generation-v1");
        TrackingDomain drifting = new TrackingDomain("generation-v1");
        PhysicalObjectGarbageCollector domainCollector = collector(
                config, domainStore, clock, domainProjection, drifting);
        GcPlan domainPlan = domainCollector.mark(
                candidate(config, domainActive, query(domainActive)), List.of(), List.of())
                .join().plan().orElseThrow();
        drifting.stillMatches = false;
        clock.setMillis(12_001);

        PhysicalGcAdvanceResult domainDrift = domainCollector
                .advanceToDeleteIntent(domainPlan).join();
        assertThat(domainDrift.status()).isEqualTo(PhysicalGcAdvanceStatus.PLAN_DRIFT_UNMARKED);
        assertThat(domainDrift.root().orElseThrow().value().lifecycle())
                .isEqualTo(PhysicalObjectLifecycle.ACTIVE);
    }

    @Test
    void exactMetadataVersionOrEnvelopeDriftBlocksMarkAndUnmarksDrain() {
        PhysicalGcConfig config = config(true, false);
        MutableClock clock = new MutableClock(1_000);
        GcPlannedMetadataRemoval removal = new GcPlannedMetadataRemoval(
                "generation-index", "/metadata/index-a", 7, SHA_A);

        FakePhysicalObjectMetadataStore markStore = new FakePhysicalObjectMetadataStore();
        VersionedPhysicalObjectRoot markActive = createActiveRoot(markStore);
        TrackingMetadataRevalidator markFacts = new TrackingMetadataRevalidator();
        markFacts.matches = false;
        PhysicalObjectGarbageCollector markCollector = collector(
                config,
                markStore,
                clock,
                new TrackingActivationGuard(),
                markFacts,
                new TrackingDomain("projection-generation-v1"),
                new TrackingDomain("generation-v1"));
        PhysicalGcMarkResult blocked = markCollector.mark(
                candidate(config, markActive, query(markActive)),
                List.of(),
                List.of(removal)).join();
        assertThat(blocked.status()).isEqualTo(PhysicalGcMarkStatus.PLAN_CHANGED);
        assertThat(markStore.getRoot(CLUSTER, activeIdentity(markActive).objectKeyHash()).join()
                        .orElseThrow().value().lifecycle())
                .isEqualTo(PhysicalObjectLifecycle.ACTIVE);

        FakePhysicalObjectMetadataStore drainStore = new FakePhysicalObjectMetadataStore();
        VersionedPhysicalObjectRoot drainActive = createActiveRoot(drainStore);
        TrackingMetadataRevalidator drainFacts = new TrackingMetadataRevalidator();
        PhysicalObjectGarbageCollector drainCollector = collector(
                config,
                drainStore,
                clock,
                new TrackingActivationGuard(),
                drainFacts,
                new TrackingDomain("projection-generation-v1"),
                new TrackingDomain("generation-v1"));
        GcPlan plan = drainCollector.mark(
                candidate(config, drainActive, query(drainActive)),
                List.of(),
                List.of(removal)).join().plan().orElseThrow();
        drainFacts.matches = false;
        clock.setMillis(12_001);

        PhysicalGcAdvanceResult drift = drainCollector.advanceToDeleteIntent(plan).join();
        assertThat(drift.status()).isEqualTo(PhysicalGcAdvanceStatus.PLAN_DRIFT_UNMARKED);
        assertThat(drift.root().orElseThrow().value().lifecycle())
                .isEqualTo(PhysicalObjectLifecycle.ACTIVE);
    }

    @Test
    void deleteIntentLostResponseAndMarkedRestartBothConvergeWithoutDeletingAnything() {
        LostRootCasResponseStore store = new LostRootCasResponseStore(PhysicalObjectLifecycle.DELETING);
        PhysicalGcConfig config = config(true, false);
        MutableClock clock = new MutableClock(1_000);
        VersionedPhysicalObjectRoot active = createActiveRoot(store);
        TrackingDomain projection = new TrackingDomain("projection-generation-v1");
        TrackingDomain generation = new TrackingDomain("generation-v1");
        GcReferenceQuery query = query(active);
        PhysicalObjectGarbageCollector firstCollector = collector(
                config, store, clock, projection, generation);
        GcPlan original = firstCollector.mark(
                candidate(config, active, query), List.of(), List.of())
                .join().plan().orElseThrow();
        VersionedPhysicalObjectRoot marked = store.getRoot(
                CLUSTER, original.candidate().object().objectKeyHash()).join().orElseThrow();

        GcCandidate recoveredCandidate = GcCandidate.fromMarkedRoot(
                config, "g".repeat(52), marked, query, SHA_A, 5_000);
        GcReferenceCollection recoveredFacts = new GcReferenceDomainRegistry(
                        config, scheduler, List.of(generation, projection))
                .snapshotForDeletion(query).join();
        GcPlan recovered = GcPlan.fromMarkedRoot(
                config,
                marked.value().gcAttemptId(),
                recoveredCandidate,
                recoveredFacts.snapshots(),
                List.of(),
                List.of(),
                marked);
        clock.setMillis(12_001);
        PhysicalObjectGarbageCollector restarted = collector(
                config, store, clock, projection, generation);

        PhysicalGcAdvanceResult result = restarted.advanceToDeleteIntent(recovered).join();

        assertThat(result.status()).isEqualTo(PhysicalGcAdvanceStatus.DELETE_INTENT);
        assertThat(result.root().orElseThrow().value().lifecycle())
                .isEqualTo(PhysicalObjectLifecycle.DELETING);
        assertThat(store.getRoot(CLUSTER, recovered.candidate().object().objectKeyHash()).join())
                .isPresent();
    }

    private PhysicalObjectGarbageCollector collector(
            PhysicalGcConfig config,
            FakePhysicalObjectMetadataStore store,
            MutableClock clock,
            TrackingDomain... domains) {
        return collector(config, store, clock, new TrackingActivationGuard(), domains);
    }

    private PhysicalObjectGarbageCollector collector(
            PhysicalGcConfig config,
            FakePhysicalObjectMetadataStore store,
            MutableClock clock,
            TrackingActivationGuard activation,
            TrackingDomain... domains) {
        GcReferenceDomainRegistry registry = new GcReferenceDomainRegistry(
                config, scheduler, List.of(domains));
        return collector(
                config,
                store,
                clock,
                activation,
                (candidate, expected) -> CompletableFuture.completedFuture(expected),
                registry);
    }

    private PhysicalObjectGarbageCollector collector(
            PhysicalGcConfig config,
            FakePhysicalObjectMetadataStore store,
            MutableClock clock,
            TrackingActivationGuard activation,
            GcPlanMetadataRevalidator metadataRevalidator,
            TrackingDomain... domains) {
        return collector(
                config,
                store,
                clock,
                activation,
                metadataRevalidator,
                new GcReferenceDomainRegistry(config, scheduler, List.of(domains)));
    }

    private PhysicalObjectGarbageCollector collector(
            PhysicalGcConfig config,
            FakePhysicalObjectMetadataStore store,
            MutableClock clock,
            TrackingActivationGuard activation,
            GcPlanMetadataRevalidator metadataRevalidator,
            GcReferenceDomainRegistry registry) {
        return collector(
                config,
                store,
                clock,
                activation,
                metadataRevalidator,
                registry,
                new DefaultGcRetirementJournal(CLUSTER, store, config));
    }

    private PhysicalObjectGarbageCollector collector(
            PhysicalGcConfig config,
            FakePhysicalObjectMetadataStore store,
            MutableClock clock,
            TrackingActivationGuard activation,
            GcPlanMetadataRevalidator metadataRevalidator,
            GcReferenceDomainRegistry registry,
            GcRetirementJournal retirementJournal) {
        return new PhysicalObjectGarbageCollector(
                CLUSTER,
                config,
                store,
                registry,
                activation,
                metadataRevalidator,
                retirementJournal,
                () -> ATTEMPT_ID,
                clock,
                scheduler);
    }

    private static VersionedPhysicalObjectRoot createActiveRoot(
            FakePhysicalObjectMetadataStore store) {
        PhysicalObjectIdentity object = PhysicalObjectIdentity.create(
                new ObjectKey("objects/gc-one"),
                Optional.empty(),
                PhysicalObjectKind.COMMITTED_COMPACTED,
                42,
                CRC,
                Optional.of(SHA_A),
                Optional.of("etag"));
        PhysicalObjectRootRecord root = new PhysicalObjectRootRecord(
                1,
                object.objectKeyHash().value(),
                object.objectKey().value(),
                "",
                object.kind().wireId(),
                object.objectLength(),
                object.storageChecksum().type().name(),
                object.storageChecksum().value(),
                object.contentSha256().orElseThrow().value(),
                object.etag().orElse(""),
                PhysicalObjectLifecycle.ACTIVE,
                1,
                100,
                200,
                "",
                "",
                0,
                0,
                0,
                0,
                0,
                "",
                "",
                0);
        return store.createRoot(CLUSTER, root).join();
    }

    private static GcPlannedProtectionRemoval createProtection(
            FakePhysicalObjectMetadataStore store,
            VersionedPhysicalObjectRoot active,
            long ownerVersion) {
        ObjectProtectionRecord protection = new ObjectProtectionRecord(
                1,
                active.value().objectKeyHash(),
                ObjectProtectionType.VISIBLE_GENERATION.wireId(),
                "generation-a",
                "/owner/generation-a",
                ownerVersion,
                SHA_A.value(),
                active.value().lifecycleEpoch(),
                500,
                0,
                0);
        return new GcPlannedProtectionRemoval(
                store.createProtection(CLUSTER, protection).join());
    }

    private static VersionedReaderLease createLease(
            FakePhysicalObjectMetadataStore store,
            GcPlan plan,
            String processRunId,
            String leaseId,
            long expiresAtMillis) {
        ObjectReaderLeaseRecord lease = new ObjectReaderLeaseRecord(
                1,
                plan.candidate().object().objectKeyHash().value(),
                processRunId,
                leaseId,
                plan.candidate().activeRootLifecycleEpoch(),
                10_000,
                expiresAtMillis,
                expiresAtMillis - 1,
                0,
                0);
        return store.createOrCompareReaderLease(CLUSTER, lease).join();
    }

    private static GcCandidate candidate(
            PhysicalGcConfig config,
            VersionedPhysicalObjectRoot active,
            GcReferenceQuery query) {
        return GcCandidate.fromActiveRoot(
                config, CANDIDATE_ID, active, query, SHA_A, 1_000, 1_000);
    }

    private static GcReferenceQuery query(VersionedPhysicalObjectRoot root) {
        return GcReferenceQuery.create(
                GcReferenceQueryKind.REFERENCED_OBJECT,
                activeIdentity(root),
                List.of(new StreamId("stream-a")),
                SHA_A);
    }

    private static PhysicalObjectIdentity activeIdentity(VersionedPhysicalObjectRoot root) {
        return PhysicalObjectIdentity.from(root.value());
    }

    private static PhysicalGcConfig config(boolean enabled, boolean dryRun) {
        return new PhysicalGcConfig(
                enabled,
                dryRun,
                1,
                1,
                2,
                4_096,
                10,
                10,
                Duration.ofSeconds(1),
                Duration.ofSeconds(10),
                Duration.ofSeconds(3),
                Duration.ofMillis(5),
                Duration.ofSeconds(11),
                Duration.ofHours(1),
                Duration.ofHours(25),
                Duration.ofDays(7),
                Duration.ofSeconds(2),
                Duration.ofSeconds(2));
    }

    private static Checksum sha(char value) {
        return new Checksum(ChecksumType.SHA256, Character.toString(value).repeat(64));
    }

    private static final class TrackingDomain implements GcReferenceDomain {
        private final String domainId;
        private final AtomicInteger snapshotCalls = new AtomicInteger();
        private volatile boolean veto;
        private volatile boolean stillMatches = true;

        private TrackingDomain(String domainId) {
            this.domainId = domainId;
        }

        @Override
        public String domainId() {
            return domainId;
        }

        @Override
        public int protocolVersion() {
            return 1;
        }

        @Override
        public CompletableFuture<GcReferenceSnapshot> snapshot(GcReferenceQuery query) {
            snapshotCalls.incrementAndGet();
            return CompletableFuture.completedFuture(GcReferenceSnapshot.create(
                    domainId,
                    1,
                    query.queryIdentitySha256(),
                    true,
                    veto,
                    1,
                    0,
                    List.of(new GcAuthorityToken("/authority/" + domainId, 1, SHA_A)),
                    List.of()));
        }

        @Override
        public CompletableFuture<Boolean> stillMatches(
                GcReferenceQuery query, GcReferenceSnapshot snapshot) {
            if (!query.queryIdentitySha256().equals(snapshot.queryIdentitySha256())) {
                return CompletableFuture.failedFuture(
                        new IllegalArgumentException("query does not match snapshot"));
            }
            return CompletableFuture.completedFuture(stillMatches);
        }
    }

    private static final class TrackingActivationGuard
            implements GenerationProtocolActivationGuard {
        private final AtomicInteger requireCalls = new AtomicInteger();
        private final AtomicInteger revalidateCalls = new AtomicInteger();

        @Override
        public CompletableFuture<GenerationActivationProof> requireReady(
                GenerationOperation operation,
                GenerationActivationSubject subject,
                boolean activateLiveProjectionIfAbsent) {
            requireCalls.incrementAndGet();
            return CompletableFuture.completedFuture(GenerationActivationProof.create(
                    operation, subject, 0, 1, 1, SHA_B, false, true, 1_000));
        }

        @Override
        public CompletableFuture<Void> revalidate(GenerationActivationProof proof) {
            revalidateCalls.incrementAndGet();
            return CompletableFuture.completedFuture(null);
        }
    }

    private static final class TrackingMetadataRevalidator
            implements GcPlanMetadataRevalidator {
        private volatile boolean matches = true;

        @Override
        public CompletableFuture<List<GcPlannedMetadataRemoval>> reload(
                GcCandidate candidate,
                List<GcPlannedMetadataRemoval> expectedRemovals) {
            return CompletableFuture.completedFuture(matches ? expectedRemovals : List.of());
        }
    }

    private static final class FailingRetirementJournal implements GcRetirementJournal {
        @Override
        public CompletableFuture<GcRetirementJournalSnapshot> prepare(
                String gcAttemptId,
                GcCandidate candidate,
                List<GcReferenceSnapshot> domainSnapshots,
                List<GcPlannedProtectionRemoval> plannedProtectionRemovals,
                List<GcPlannedMetadataRemoval> plannedMetadataRemovals,
                Checksum referenceSetSha256,
                long createdAtMillis,
                MaterializationDeadline deadline) {
            return CompletableFuture.failedFuture(
                    new IllegalStateException("injected journal prepare failure"));
        }

        @Override
        public CompletableFuture<Optional<GcRetirementJournalSnapshot>> load(
                ObjectKeyHash object,
                String gcAttemptId,
                MaterializationDeadline deadline) {
            return CompletableFuture.completedFuture(Optional.empty());
        }
    }

    private static final class TrackingRetirementJournal implements GcRetirementJournal {
        private final GcRetirementJournal delegate;
        private final int disappearAtLoad;
        private final AtomicInteger prepareCalls = new AtomicInteger();
        private final AtomicInteger loadCalls = new AtomicInteger();

        private TrackingRetirementJournal(
                GcRetirementJournal delegate, int disappearAtLoad) {
            this.delegate = delegate;
            this.disappearAtLoad = disappearAtLoad;
        }

        @Override
        public CompletableFuture<GcRetirementJournalSnapshot> prepare(
                String gcAttemptId,
                GcCandidate candidate,
                List<GcReferenceSnapshot> domainSnapshots,
                List<GcPlannedProtectionRemoval> plannedProtectionRemovals,
                List<GcPlannedMetadataRemoval> plannedMetadataRemovals,
                Checksum referenceSetSha256,
                long createdAtMillis,
                MaterializationDeadline deadline) {
            prepareCalls.incrementAndGet();
            return delegate.prepare(
                    gcAttemptId,
                    candidate,
                    domainSnapshots,
                    plannedProtectionRemovals,
                    plannedMetadataRemovals,
                    referenceSetSha256,
                    createdAtMillis,
                    deadline);
        }

        @Override
        public CompletableFuture<Optional<GcRetirementJournalSnapshot>> load(
                ObjectKeyHash object,
                String gcAttemptId,
                MaterializationDeadline deadline) {
            int call = loadCalls.incrementAndGet();
            if (call == disappearAtLoad) {
                return CompletableFuture.completedFuture(Optional.empty());
            }
            return delegate.load(object, gcAttemptId, deadline);
        }
    }

    private static final class LostRootCasResponseStore extends FakePhysicalObjectMetadataStore {
        private final PhysicalObjectLifecycle lostLifecycle;
        private boolean lost;

        private LostRootCasResponseStore(PhysicalObjectLifecycle lostLifecycle) {
            this.lostLifecycle = lostLifecycle;
        }

        @Override
        public synchronized CompletableFuture<VersionedPhysicalObjectRoot> compareAndSetRoot(
                String cluster, PhysicalObjectRootRecord root, long expectedVersion) {
            CompletableFuture<VersionedPhysicalObjectRoot> committed =
                    super.compareAndSetRoot(cluster, root, expectedVersion);
            if (!lost && root.lifecycle() == lostLifecycle) {
                lost = true;
                return committed.thenCompose(ignored -> CompletableFuture.failedFuture(
                        new RuntimeException("injected lost root CAS response")));
            }
            return committed;
        }
    }

    private static final class MutableClock extends Clock {
        private volatile long millis;

        private MutableClock(long millis) {
            this.millis = millis;
        }

        private void setMillis(long value) {
            millis = value;
        }

        @Override
        public ZoneId getZone() {
            return ZoneOffset.UTC;
        }

        @Override
        public Clock withZone(ZoneId zone) {
            return this;
        }

        @Override
        public Instant instant() {
            return Instant.ofEpochMilli(millis);
        }

        @Override
        public long millis() {
            return millis;
        }
    }
}
