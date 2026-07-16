/* Licensed under the Apache License, Version 2.0 */
package com.nereusstream.materialization.gc;

import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.assertThatThrownBy;

import com.nereusstream.api.Checksum;
import com.nereusstream.api.ChecksumType;
import com.nereusstream.api.ObjectKey;
import com.nereusstream.api.StreamId;
import com.nereusstream.core.physical.GcAuthorityToken;
import com.nereusstream.core.physical.GcReferenceQuery;
import com.nereusstream.core.physical.GcReferenceQueryKind;
import com.nereusstream.core.physical.GcReferenceSnapshot;
import com.nereusstream.core.physical.PhysicalObjectIdentity;
import com.nereusstream.core.physical.PhysicalObjectKind;
import com.nereusstream.materialization.MaterializationDeadline;
import com.nereusstream.metadata.oxia.FakePhysicalObjectMetadataStore;
import com.nereusstream.metadata.oxia.VersionedGcRetirementManifest;
import com.nereusstream.metadata.oxia.VersionedGcRetirementProtection;
import com.nereusstream.metadata.oxia.VersionedGcRetirementRemoval;
import com.nereusstream.metadata.oxia.VersionedObjectProtection;
import com.nereusstream.metadata.oxia.VersionedPhysicalObjectRoot;
import com.nereusstream.metadata.oxia.records.GcRetirementManifestRecord;
import com.nereusstream.metadata.oxia.records.GcRetirementProtectionRecord;
import com.nereusstream.metadata.oxia.records.GcRetirementRemovalRecord;
import com.nereusstream.metadata.oxia.records.ObjectProtectionRecord;
import com.nereusstream.metadata.oxia.records.ObjectProtectionType;
import com.nereusstream.metadata.oxia.records.PhysicalObjectLifecycle;
import com.nereusstream.metadata.oxia.records.PhysicalObjectRootRecord;
import java.time.Duration;
import java.util.List;
import java.util.Optional;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.Executors;
import java.util.concurrent.ScheduledExecutorService;
import java.util.concurrent.atomic.AtomicBoolean;
import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.Test;

class DefaultGcRetirementJournalTest {
    private static final String CLUSTER = "cluster-a";
    private static final String CANDIDATE_ID = "a".repeat(52);
    private static final String ATTEMPT_ID = "b".repeat(52);
    private static final Checksum SHA_A = sha('a');
    private static final Checksum SHA_B = sha('b');
    private static final Checksum SHA_C = sha('c');
    private static final Checksum CRC = new Checksum(ChecksumType.CRC32C, "01020304");

    private final ScheduledExecutorService scheduler = Executors.newSingleThreadScheduledExecutor();

    @AfterEach
    void closeScheduler() {
        scheduler.shutdownNow();
    }

    @Test
    void sealsManifestLastAndReloadsExactPlanAcrossRestartAndPagination() {
        PhysicalGcConfig config = configWithPageSize(1);
        FakePhysicalObjectMetadataStore store = new FakePhysicalObjectMetadataStore();
        PlanFacts facts = facts(config);
        DefaultGcRetirementJournal journal = new DefaultGcRetirementJournal(CLUSTER, store, config);

        GcRetirementJournalSnapshot prepared = prepare(journal, facts);

        assertThat(prepared.object()).isEqualTo(facts.candidate().object().objectKeyHash());
        assertThat(prepared.gcAttemptId()).isEqualTo(ATTEMPT_ID);
        assertThat(prepared.queryIdentitySha256())
                .isEqualTo(facts.candidate().referenceQuery().queryIdentitySha256());
        assertThat(prepared.domainProofs())
                .isEqualTo(facts.snapshots().stream().map(GcDomainSnapshotProof::from).toList());
        assertThat(prepared.plannedProtectionRemovals()).isEqualTo(facts.protections());
        assertThat(prepared.plannedMetadataRemovals()).isEqualTo(facts.removals());
        assertThat(prepared.referenceSetSha256()).isEqualTo(facts.digest());
        assertThat(prepared.protectionEntries()).hasSize(2);
        assertThat(prepared.removalEntries()).hasSize(2);

        DefaultGcRetirementJournal restarted = new DefaultGcRetirementJournal(CLUSTER, store, config);
        GcRetirementJournalSnapshot loaded;
        try (MaterializationDeadline deadline = deadline()) {
            loaded = restarted.load(
                            facts.candidate().object().objectKeyHash(), ATTEMPT_ID, deadline)
                    .join()
                    .orElseThrow();
        }
        assertThat(loaded.referenceSetSha256()).isEqualTo(prepared.referenceSetSha256());
        assertThat(loaded.plannedProtectionRemovals()).isEqualTo(prepared.plannedProtectionRemovals());
        assertThat(loaded.plannedMetadataRemovals()).isEqualTo(prepared.plannedMetadataRemovals());

        GcRetirementJournalSnapshot repeated = prepare(restarted, facts);
        assertThat(repeated.manifest().metadataVersion())
                .isEqualTo(prepared.manifest().metadataVersion());
    }

    @Test
    void convergesWhenEntryAndManifestCreateResponsesAreLostAfterCommit() {
        PhysicalGcConfig config = configWithPageSize(1);
        ResponseLossStore store = new ResponseLossStore();
        PlanFacts facts = facts(config);

        GcRetirementJournalSnapshot prepared = prepare(
                new DefaultGcRetirementJournal(CLUSTER, store, config), facts);

        assertThat(store.lostProtectionResponse()).isTrue();
        assertThat(store.lostManifestResponse()).isTrue();
        assertThat(prepared.referenceSetSha256()).isEqualTo(facts.digest());
        assertThat(store.getRetirementManifest(
                        CLUSTER, facts.candidate().object().objectKeyHash(), ATTEMPT_ID)
                .join())
                .isPresent();
    }

    @Test
    void neverSealsManifestWhenAnyJournalEntryIsMissing() {
        PhysicalGcConfig config = configWithPageSize(1);
        RejectingRemovalStore store = new RejectingRemovalStore();
        PlanFacts facts = facts(config);
        DefaultGcRetirementJournal journal = new DefaultGcRetirementJournal(CLUSTER, store, config);

        assertThatThrownBy(() -> prepare(journal, facts))
                .hasRootCauseMessage("GC retirement journal is missing removal entries");
        assertThat(store.getRetirementManifest(
                        CLUSTER, facts.candidate().object().objectKeyHash(), ATTEMPT_ID)
                .join())
                .isEmpty();
    }

    private GcRetirementJournalSnapshot prepare(
            DefaultGcRetirementJournal journal, PlanFacts facts) {
        try (MaterializationDeadline deadline = deadline()) {
            return journal.prepare(
                            ATTEMPT_ID,
                            facts.candidate(),
                            facts.snapshots(),
                            facts.protections(),
                            facts.removals(),
                            facts.digest(),
                            300,
                            deadline)
                    .join();
        }
    }

    private MaterializationDeadline deadline() {
        return new MaterializationDeadline(Duration.ofSeconds(5), scheduler);
    }

    private static PlanFacts facts(PhysicalGcConfig config) {
        VersionedPhysicalObjectRoot active = activeRoot();
        GcReferenceQuery query = GcReferenceQuery.create(
                GcReferenceQueryKind.REFERENCED_OBJECT,
                PhysicalObjectIdentity.from(active.value()),
                List.of(new StreamId("stream-a")),
                SHA_A);
        GcCandidate candidate = GcCandidate.fromActiveRoot(
                config, CANDIDATE_ID, active, query, SHA_A, 250, 250);
        List<GcReferenceSnapshot> snapshots = List.of(GcReferenceSnapshot.create(
                "generation-v1",
                1,
                query.queryIdentitySha256(),
                true,
                false,
                1,
                0,
                List.of(new GcAuthorityToken("/authority/a", 1, SHA_A)),
                List.of()));
        List<GcPlannedProtectionRemoval> protections = List.of(
                protection(candidate.object().objectKeyHash().value(), "generation-a", 7, SHA_A),
                protection(candidate.object().objectKeyHash().value(), "generation-b", 8, SHA_B));
        List<GcPlannedMetadataRemoval> removals = List.of(
                new GcPlannedMetadataRemoval("generation-index", "/metadata/a", 11, SHA_B),
                new GcPlannedMetadataRemoval("generation-index", "/metadata/b", 12, SHA_C));
        Checksum digest = GcPlan.computeReferenceSetSha256(
                config, candidate, snapshots, protections, removals);
        return new PlanFacts(candidate, snapshots, protections, removals, digest);
    }

    private static GcPlannedProtectionRemoval protection(
            String objectHash, String generationId, long metadataVersion, Checksum durableSha) {
        ObjectProtectionRecord value = new ObjectProtectionRecord(
                1,
                objectHash,
                ObjectProtectionType.VISIBLE_GENERATION.wireId(),
                generationId,
                "/owner/" + generationId,
                metadataVersion,
                durableSha.value(),
                4,
                100,
                0,
                metadataVersion);
        return new GcPlannedProtectionRemoval(new VersionedObjectProtection(
                "/protections/" + objectHash + "/" + generationId,
                value,
                metadataVersion,
                durableSha));
    }

    private static VersionedPhysicalObjectRoot activeRoot() {
        PhysicalObjectIdentity object = PhysicalObjectIdentity.create(
                new ObjectKey("objects/one"),
                Optional.empty(),
                PhysicalObjectKind.COMMITTED_COMPACTED,
                42,
                CRC,
                Optional.of(SHA_A),
                Optional.of("etag"));
        PhysicalObjectRootRecord record = new PhysicalObjectRootRecord(
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
                4,
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
                7);
        return new VersionedPhysicalObjectRoot("/root/one", record, 7, SHA_A);
    }

    private static PhysicalGcConfig configWithPageSize(int pageSize) {
        PhysicalGcConfig defaults = PhysicalGcConfig.defaults();
        return new PhysicalGcConfig(
                defaults.enabled(),
                defaults.dryRun(),
                pageSize,
                defaults.objectListPageSize(),
                defaults.maxConcurrentDeletes(),
                defaults.maxStreamsPerCandidate(),
                defaults.maxAuthoritiesPerDomainSnapshot(),
                defaults.maxReferencesPerDomainSnapshot(),
                defaults.scanInterval(),
                defaults.readerLeaseDuration(),
                defaults.readerLeaseRenewInterval(),
                defaults.maximumClockSkew(),
                defaults.drainGrace(),
                defaults.pendingProtectionDuration(),
                defaults.orphanGrace(),
                defaults.tombstoneAuditGrace(),
                defaults.operationTimeout(),
                defaults.closeTimeout());
    }

    private static Checksum sha(char value) {
        return new Checksum(ChecksumType.SHA256, Character.toString(value).repeat(64));
    }

    private record PlanFacts(
            GcCandidate candidate,
            List<GcReferenceSnapshot> snapshots,
            List<GcPlannedProtectionRemoval> protections,
            List<GcPlannedMetadataRemoval> removals,
            Checksum digest) { }

    private static final class ResponseLossStore extends FakePhysicalObjectMetadataStore {
        private final AtomicBoolean loseProtection = new AtomicBoolean(true);
        private final AtomicBoolean loseManifest = new AtomicBoolean(true);

        @Override
        public CompletableFuture<VersionedGcRetirementProtection> createRetirementProtection(
                String cluster, GcRetirementProtectionRecord protection) {
            return super.createRetirementProtection(cluster, protection).thenCompose(created ->
                    loseProtection.compareAndSet(true, false)
                            ? CompletableFuture.failedFuture(
                                    new IllegalStateException("lost protection response"))
                            : CompletableFuture.completedFuture(created));
        }

        @Override
        public CompletableFuture<VersionedGcRetirementManifest> createRetirementManifest(
                String cluster, GcRetirementManifestRecord manifest) {
            return super.createRetirementManifest(cluster, manifest).thenCompose(created ->
                    loseManifest.compareAndSet(true, false)
                            ? CompletableFuture.failedFuture(
                                    new IllegalStateException("lost manifest response"))
                            : CompletableFuture.completedFuture(created));
        }

        boolean lostProtectionResponse() {
            return !loseProtection.get();
        }

        boolean lostManifestResponse() {
            return !loseManifest.get();
        }
    }

    private static final class RejectingRemovalStore extends FakePhysicalObjectMetadataStore {
        @Override
        public CompletableFuture<VersionedGcRetirementRemoval> createRetirementRemoval(
                String cluster, GcRetirementRemovalRecord removal) {
            return CompletableFuture.failedFuture(
                    new IllegalStateException("injected removal write failure"));
        }
    }
}
