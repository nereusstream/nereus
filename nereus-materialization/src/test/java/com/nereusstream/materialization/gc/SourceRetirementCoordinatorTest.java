/* Licensed under the Apache License, Version 2.0 */
package com.nereusstream.materialization.gc;

import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.assertThatThrownBy;

import com.nereusstream.api.Checksum;
import com.nereusstream.api.ChecksumType;
import com.nereusstream.api.ErrorCode;
import com.nereusstream.api.NereusException;
import com.nereusstream.api.ObjectKey;
import com.nereusstream.api.ObjectKeyHash;
import com.nereusstream.core.physical.GcReferenceSnapshot;
import com.nereusstream.materialization.MaterializationDeadline;
import com.nereusstream.metadata.oxia.FakePhysicalObjectMetadataStore;
import com.nereusstream.metadata.oxia.VersionedGcRetirementManifest;
import com.nereusstream.metadata.oxia.VersionedGcRetirementRemoval;
import com.nereusstream.metadata.oxia.VersionedPhysicalObjectRoot;
import com.nereusstream.metadata.oxia.records.GcDomainSnapshotProofRecord;
import com.nereusstream.metadata.oxia.records.GcRetirementManifestRecord;
import com.nereusstream.metadata.oxia.records.GcRetirementRemovalRecord;
import com.nereusstream.metadata.oxia.records.PhysicalObjectLifecycle;
import com.nereusstream.metadata.oxia.records.PhysicalObjectRootRecord;
import com.nereusstream.objectstore.DeleteObjectOptions;
import com.nereusstream.objectstore.DeleteObjectResult;
import com.nereusstream.objectstore.HeadObjectOptions;
import com.nereusstream.objectstore.HeadObjectResult;
import com.nereusstream.objectstore.ObjectStore;
import com.nereusstream.objectstore.RangeReadOptions;
import com.nereusstream.objectstore.RangeReadResult;
import java.time.Clock;
import java.time.Instant;
import java.time.ZoneOffset;
import java.util.List;
import java.util.Map;
import java.util.Optional;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.Executors;
import java.util.concurrent.ScheduledExecutorService;
import java.util.concurrent.atomic.AtomicInteger;
import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.Test;

class SourceRetirementCoordinatorTest {
    private static final String CLUSTER = "cluster-a";
    private static final String ATTEMPT_ID = "b".repeat(52);
    private static final ObjectKey OBJECT_KEY = new ObjectKey("objects/wal-a");
    private static final Checksum STORAGE_CHECKSUM =
            new Checksum(ChecksumType.CRC32C, "01020304");

    private final ScheduledExecutorService scheduler = Executors.newSingleThreadScheduledExecutor();

    @AfterEach
    void closeScheduler() {
        scheduler.shutdownNow();
    }

    @Test
    void deletingRecoveryDeletesObjectCommitsRootAndReentersAsAlreadyDeleted() {
        Fixture fixture = fixture(true);
        SourceRetirementCoordinator coordinator = coordinator(
                fixture.store(), fixture.journal(), fixture.objectStore());

        PhysicalGcDeletionResult deleted = coordinator.resume(fixture.root()).join();

        assertThat(deleted.status()).isEqualTo(PhysicalGcDeletionStatus.DELETED);
        assertThat(deleted.objectStatus()).contains(DeleteObjectResult.Status.DELETED);
        assertThat(deleted.root().orElseThrow().value().lifecycle())
                .isEqualTo(PhysicalObjectLifecycle.DELETED);
        assertThat(fixture.objectStore().exists).isFalse();
        assertThat(fixture.objectStore().deleteCalls).hasValue(1);

        PhysicalGcDeletionResult restarted = coordinator.resume(deleted.root().orElseThrow()).join();

        assertThat(restarted.status()).isEqualTo(PhysicalGcDeletionStatus.ALREADY_DELETED);
        assertThat(restarted.root()).isEqualTo(deleted.root());
        assertThat(fixture.objectStore().deleteCalls).hasValue(1);
    }

    @Test
    void alreadyAbsentObjectConvergesOnlyUnderExactDeletingRootAndJournal() {
        Fixture fixture = fixture(false);

        PhysicalGcDeletionResult result = coordinator(
                        fixture.store(), fixture.journal(), fixture.objectStore())
                .resume(fixture.root())
                .join();

        assertThat(result.status()).isEqualTo(PhysicalGcDeletionStatus.DELETED);
        assertThat(result.objectStatus()).contains(DeleteObjectResult.Status.ALREADY_ABSENT);
        assertThat(result.root().orElseThrow().value().lifecycle())
                .isEqualTo(PhysicalObjectLifecycle.DELETED);
        assertThat(fixture.objectStore().deleteCalls).hasValue(0);
    }

    @Test
    void missingJournalFailsBeforeProtectionOrObjectMutation() {
        Fixture fixture = fixture(true);
        GcRetirementJournal missing = new StaticJournal(Optional.empty());

        assertThatThrownBy(() -> coordinator(
                        fixture.store(), missing, fixture.objectStore())
                .resume(fixture.root())
                .join())
                .hasRootCauseInstanceOf(NereusException.class)
                .hasRootCauseMessage("DELETING root is missing its sealed retirement journal");

        assertThat(fixture.objectStore().headCalls).hasValue(0);
        assertThat(fixture.objectStore().deleteCalls).hasValue(0);
        assertThat(fixture.store()
                        .getRoot(CLUSTER, ObjectKeyHash.from(OBJECT_KEY))
                        .join()
                        .orElseThrow()
                        .value()
                        .lifecycle())
                .isEqualTo(PhysicalObjectLifecycle.DELETING);
    }

    @Test
    void unregisteredMetadataRemovalFailsBeforeObjectAccess() {
        GcPlannedMetadataRemoval removal = new GcPlannedMetadataRemoval(
                "generation-index", "/indexes/one", 7, sha('e'));
        Fixture fixture = fixture(journalSnapshot(List.of(removal)), true);

        assertThatThrownBy(() -> coordinator(
                        fixture.store(), fixture.journal(), fixture.objectStore())
                .resume(fixture.root())
                .join())
                .hasRootCauseInstanceOf(NereusException.class)
                .hasRootCauseMessage(
                        "no metadata-retirement handler is registered for generation-index");

        assertThat(fixture.objectStore().headCalls).hasValue(0);
        assertThat(fixture.objectStore().deleteCalls).hasValue(0);
        assertThat(fixture.store()
                        .getRoot(CLUSTER, ObjectKeyHash.from(OBJECT_KEY))
                        .join()
                        .orElseThrow()
                        .value()
                        .lifecycle())
                .isEqualTo(PhysicalObjectLifecycle.DELETING);
    }

    @Test
    void journalIsReauthenticatedBeforeEveryMetadataBatch() {
        GcPlannedMetadataRemoval first = new GcPlannedMetadataRemoval(
                "test-removal", "/indexes/a", 7, sha('e'));
        GcPlannedMetadataRemoval second = new GcPlannedMetadataRemoval(
                "test-removal", "/indexes/b", 8, sha('f'));
        GcRetirementJournalSnapshot snapshot = journalSnapshot(List.of(first, second));
        Fixture fixture = fixture(snapshot, true);
        DisappearingJournal journal = new DisappearingJournal(snapshot, 2);
        AtomicInteger retireCalls = new AtomicInteger();
        GcMetadataRetirementHandler handler = new GcMetadataRetirementHandler() {
            @Override
            public String removalType() {
                return "test-removal";
            }

            @Override
            public CompletableFuture<GcMetadataRetirementOutcome> retire(
                    GcMetadataRetirementContext context,
                    GcPlannedMetadataRemoval removal,
                    MaterializationDeadline deadline) {
                retireCalls.incrementAndGet();
                return CompletableFuture.completedFuture(
                        GcMetadataRetirementOutcome.RETIRED);
            }
        };

        assertThatThrownBy(() -> coordinator(
                        enabledConfig(1),
                        fixture.store(),
                        journal,
                        new GcMetadataRetirementRegistry(List.of(handler)),
                        fixture.objectStore())
                .resume(fixture.root())
                .join())
                .hasRootCauseMessage(
                        "DELETING root is missing its sealed retirement journal");

        assertThat(journal.loadCalls).hasValue(3);
        assertThat(retireCalls).hasValue(1);
        assertThat(fixture.objectStore().headCalls).hasValue(0);
        assertThat(fixture.objectStore().deleteCalls).hasValue(0);
    }

    @Test
    void finalJournalReloadFencesPhysicalHeadAndDelete() {
        GcRetirementJournalSnapshot snapshot = journalSnapshot(List.of());
        Fixture fixture = fixture(snapshot, true);
        DisappearingJournal journal = new DisappearingJournal(snapshot, 3);

        assertThatThrownBy(() -> coordinator(
                        enabledConfig(),
                        fixture.store(),
                        journal,
                        new GcMetadataRetirementRegistry(List.of()),
                        fixture.objectStore())
                .resume(fixture.root())
                .join())
                .hasRootCauseMessage(
                        "DELETING root is missing its sealed retirement journal");

        assertThat(journal.loadCalls).hasValue(4);
        assertThat(fixture.objectStore().headCalls).hasValue(0);
        assertThat(fixture.objectStore().deleteCalls).hasValue(0);
    }

    private SourceRetirementCoordinator coordinator(
            FakePhysicalObjectMetadataStore store,
            GcRetirementJournal journal,
            ExactObjectStore objectStore) {
        return coordinator(
                enabledConfig(),
                store,
                journal,
                new GcMetadataRetirementRegistry(List.of()),
                objectStore);
    }

    private SourceRetirementCoordinator coordinator(
            PhysicalGcConfig config,
            FakePhysicalObjectMetadataStore store,
            GcRetirementJournal journal,
            GcMetadataRetirementRegistry retirements,
            ExactObjectStore objectStore) {
        return new SourceRetirementCoordinator(
                CLUSTER,
                config,
                store,
                journal,
                retirements,
                objectStore,
                Clock.fixed(Instant.ofEpochMilli(500), ZoneOffset.UTC),
                scheduler);
    }

    private static Fixture fixture(boolean objectExists) {
        return fixture(journalSnapshot(List.of()), objectExists);
    }

    private static Fixture fixture(
            GcRetirementJournalSnapshot snapshot,
            boolean objectExists) {
        FakePhysicalObjectMetadataStore store = new FakePhysicalObjectMetadataStore();
        VersionedPhysicalObjectRoot root = store.createRoot(
                        CLUSTER, deletingRoot(snapshot.referenceSetSha256()))
                .join();
        return new Fixture(
                store,
                root,
                new StaticJournal(Optional.of(snapshot)),
                new ExactObjectStore(objectExists));
    }

    private static GcRetirementJournalSnapshot journalSnapshot(
            List<GcPlannedMetadataRemoval> removals) {
        Checksum query = sha('a');
        GcDomainSnapshotProof proof = new GcDomainSnapshotProof(
                "generation-v1", 1, query, sha('c'));
        Checksum referenceSet = GcPlanValidation.referenceSetSha256(
                query, List.of(proof), List.of(), removals);
        List<VersionedGcRetirementRemoval> entries = removals.stream()
                .map(removal -> {
                    GcRetirementRemovalRecord value = new GcRetirementRemovalRecord(
                            1,
                            ObjectKeyHash.from(OBJECT_KEY).value(),
                            ATTEMPT_ID,
                            removal.removalType(),
                            removal.key(),
                            removal.metadataVersion(),
                            removal.durableValueSha256().value(),
                            1);
                    return new VersionedGcRetirementRemoval(
                            "/journal/removal/" + removal.key(), value, 1, sha('f'));
                })
                .toList();
        GcRetirementManifestRecord value = new GcRetirementManifestRecord(
                1,
                ObjectKeyHash.from(OBJECT_KEY).value(),
                ATTEMPT_ID,
                GcRetirementManifestRecord.REFERENCE_SET_PROTOCOL_VERSION,
                query.value(),
                List.of(new GcDomainSnapshotProofRecord(
                        proof.domainId(),
                        proof.protocolVersion(),
                        proof.queryIdentitySha256().value(),
                        proof.snapshotSha256().value())),
                0,
                entries.size(),
                referenceSet.value(),
                100,
                1);
        VersionedGcRetirementManifest manifest = new VersionedGcRetirementManifest(
                "/journal/manifest", value, 1, sha('d'));
        return new GcRetirementJournalSnapshot(manifest, List.of(), entries);
    }

    private static PhysicalObjectRootRecord deletingRoot(Checksum referenceSet) {
        return new PhysicalObjectRootRecord(
                1,
                ObjectKeyHash.from(OBJECT_KEY).value(),
                OBJECT_KEY.value(),
                "object-a",
                1,
                4,
                STORAGE_CHECKSUM.type().name(),
                STORAGE_CHECKSUM.value(),
                "",
                "",
                PhysicalObjectLifecycle.DELETING,
                3,
                1,
                2,
                ATTEMPT_ID,
                referenceSet.value(),
                100,
                200,
                201,
                0,
                0,
                "",
                "",
                0);
    }

    private static PhysicalGcConfig enabledConfig() {
        return enabledConfig(PhysicalGcConfig.defaults().maxConcurrentDeletes());
    }

    private static PhysicalGcConfig enabledConfig(int maxConcurrentDeletes) {
        PhysicalGcConfig defaults = PhysicalGcConfig.defaults();
        return new PhysicalGcConfig(
                true,
                false,
                defaults.metadataScanPageSize(),
                defaults.objectListPageSize(),
                maxConcurrentDeletes,
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
        return new Checksum(ChecksumType.SHA256, String.valueOf(value).repeat(64));
    }

    private record Fixture(
            FakePhysicalObjectMetadataStore store,
            VersionedPhysicalObjectRoot root,
            GcRetirementJournal journal,
            ExactObjectStore objectStore) {
    }

    private static final class StaticJournal implements GcRetirementJournal {
        private final Optional<GcRetirementJournalSnapshot> snapshot;

        private StaticJournal(Optional<GcRetirementJournalSnapshot> snapshot) {
            this.snapshot = snapshot;
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
            return CompletableFuture.failedFuture(new UnsupportedOperationException());
        }

        @Override
        public CompletableFuture<Optional<GcRetirementJournalSnapshot>> load(
                ObjectKeyHash object,
                String gcAttemptId,
                MaterializationDeadline deadline) {
            return CompletableFuture.completedFuture(snapshot);
        }
    }

    private static final class DisappearingJournal implements GcRetirementJournal {
        private final GcRetirementJournalSnapshot snapshot;
        private final int successfulLoads;
        private final AtomicInteger loadCalls = new AtomicInteger();

        private DisappearingJournal(
                GcRetirementJournalSnapshot snapshot,
                int successfulLoads) {
            this.snapshot = snapshot;
            this.successfulLoads = successfulLoads;
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
            return CompletableFuture.failedFuture(new UnsupportedOperationException());
        }

        @Override
        public CompletableFuture<Optional<GcRetirementJournalSnapshot>> load(
                ObjectKeyHash object,
                String gcAttemptId,
                MaterializationDeadline deadline) {
            return CompletableFuture.completedFuture(
                    loadCalls.incrementAndGet() <= successfulLoads
                            ? Optional.of(snapshot)
                            : Optional.empty());
        }
    }

    private static final class ExactObjectStore implements ObjectStore {
        private final AtomicInteger headCalls = new AtomicInteger();
        private final AtomicInteger deleteCalls = new AtomicInteger();
        private volatile boolean exists;

        private ExactObjectStore(boolean exists) {
            this.exists = exists;
        }

        @Override
        public CompletableFuture<RangeReadResult> readRange(
                ObjectKey key,
                long offset,
                long length,
                RangeReadOptions options) {
            return CompletableFuture.failedFuture(new UnsupportedOperationException());
        }

        @Override
        public CompletableFuture<HeadObjectResult> headObject(
                ObjectKey key,
                HeadObjectOptions options) {
            headCalls.incrementAndGet();
            if (!exists) {
                return CompletableFuture.failedFuture(new NereusException(
                        ErrorCode.OBJECT_NOT_FOUND, true, "object not found"));
            }
            return CompletableFuture.completedFuture(new HeadObjectResult(
                    OBJECT_KEY, 4, STORAGE_CHECKSUM, Optional.empty(), Map.of()));
        }

        @Override
        public CompletableFuture<DeleteObjectResult> deleteObject(
                ObjectKey key,
                DeleteObjectOptions options) {
            deleteCalls.incrementAndGet();
            if (!OBJECT_KEY.equals(key)
                    || options.expectedLength() != 4
                    || !STORAGE_CHECKSUM.equals(options.expectedStorageChecksum())
                    || options.expectedEtag().isPresent()) {
                return CompletableFuture.failedFuture(
                        new AssertionError("delete did not carry the exact root identity"));
            }
            DeleteObjectResult.Status status = exists
                    ? DeleteObjectResult.Status.DELETED
                    : DeleteObjectResult.Status.ALREADY_ABSENT;
            exists = false;
            return CompletableFuture.completedFuture(new DeleteObjectResult(key, status));
        }

        @Override
        public void close() {
        }
    }
}
