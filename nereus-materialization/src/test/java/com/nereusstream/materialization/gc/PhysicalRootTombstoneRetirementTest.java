/* Licensed under the Apache License, Version 2.0 */
package com.nereusstream.materialization.gc;

import static org.assertj.core.api.Assertions.assertThat;

import com.nereusstream.api.Checksum;
import com.nereusstream.api.ChecksumType;
import com.nereusstream.api.ErrorCode;
import com.nereusstream.api.NereusException;
import com.nereusstream.api.ObjectId;
import com.nereusstream.api.ObjectKey;
import com.nereusstream.api.ObjectKeyHash;
import com.nereusstream.core.physical.GcAuthorityToken;
import com.nereusstream.core.physical.GcReference;
import com.nereusstream.core.physical.GcReferenceDomain;
import com.nereusstream.core.physical.GcReferenceDomainConfig;
import com.nereusstream.core.physical.GcReferenceQuery;
import com.nereusstream.core.physical.GcReferenceSnapshot;
import com.nereusstream.core.physical.GcReferenceSnapshotBuilder;
import com.nereusstream.metadata.oxia.FakePhysicalObjectMetadataStore;
import com.nereusstream.metadata.oxia.VersionedPhysicalObjectRoot;
import com.nereusstream.metadata.oxia.records.ObjectManifestRecord;
import com.nereusstream.metadata.oxia.records.ObjectReaderLeaseRecord;
import com.nereusstream.metadata.oxia.records.ObjectReferenceRecord;
import com.nereusstream.metadata.oxia.records.PhysicalObjectLifecycle;
import com.nereusstream.metadata.oxia.records.PhysicalObjectRootRecord;
import com.nereusstream.metadata.oxia.retirement.ObjectAuditRetirementStore;
import com.nereusstream.metadata.oxia.retirement.VersionedObjectManifestAudit;
import com.nereusstream.metadata.oxia.retirement.VersionedObjectReferencesAudit;
import com.nereusstream.objectstore.DeleteObjectOptions;
import com.nereusstream.objectstore.DeleteObjectResult;
import com.nereusstream.objectstore.HeadObjectOptions;
import com.nereusstream.objectstore.HeadObjectResult;
import com.nereusstream.objectstore.ObjectStore;
import com.nereusstream.objectstore.RangeReadOptions;
import com.nereusstream.objectstore.RangeReadResult;
import java.time.Clock;
import java.time.Duration;
import java.time.Instant;
import java.time.ZoneId;
import java.time.ZoneOffset;
import java.util.ArrayList;
import java.util.List;
import java.util.Map;
import java.util.Optional;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.Executors;
import java.util.concurrent.ScheduledExecutorService;
import java.util.concurrent.atomic.AtomicInteger;
import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.Test;

class PhysicalRootTombstoneRetirementTest {
    private static final String CLUSTER = "cluster-a";
    private static final String ATTEMPT_ID = "a".repeat(52);
    private static final String REFERENCE_SHA = "b".repeat(64);
    private static final ObjectKey OBJECT_KEY = new ObjectKey("objects/wal-a");
    private static final ObjectKeyHash OBJECT = ObjectKeyHash.from(OBJECT_KEY);
    private static final Checksum STORAGE_CHECKSUM =
            new Checksum(ChecksumType.CRC32C, "01020304");

    private final ScheduledExecutorService scheduler = Executors.newSingleThreadScheduledExecutor();

    @AfterEach
    void closeScheduler() {
        scheduler.shutdownNow();
    }

    @Test
    void durableFirstAbsenceAndSecondWindowRetireRootLast() {
        Fixture fixture = fixture(false);

        TombstoneRetirementResult first = fixture.coordinator()
                .retire(fixture.deletedRoot())
                .join();

        assertThat(first.status()).isEqualTo(TombstoneRetirementStatus.NOT_OLD_ENOUGH);
        VersionedPhysicalObjectRoot observed = fixture.currentRoot();
        assertThat(observed.value().tombstoneFirstAbsentAtMillis()).isEqualTo(5_000);
        assertThat(observed.value().tombstoneProofSha256()).hasSize(64);

        fixture.clock().setMillis(7_000);
        TombstoneRetirementResult retired = fixture.coordinator().retire(observed).join();

        assertThat(retired.status()).isEqualTo(TombstoneRetirementStatus.RETIRED);
        assertThat(retired.rootRetired()).isTrue();
        assertThat(fixture.store().getRoot(CLUSTER, OBJECT).join()).isEmpty();
        assertThat(fixture.mutationOrder()).containsExactly("root");
    }

    @Test
    void anyPersistedReaderHandleClearsTheWindowAndBlocksRetirement() {
        Fixture fixture = fixture(false);
        fixture.coordinator().retire(fixture.deletedRoot()).join();
        VersionedPhysicalObjectRoot observed = fixture.currentRoot();
        fixture.store().createOrCompareReaderLease(CLUSTER, new ObjectReaderLeaseRecord(
                1,
                OBJECT.value(),
                "c".repeat(26),
                "d".repeat(26),
                observed.value().lifecycleEpoch(),
                5_100,
                5_200,
                5_150,
                0,
                0)).join();
        fixture.clock().setMillis(7_000);

        TombstoneRetirementResult result = fixture.coordinator().retire(observed).join();

        assertThat(result.status()).isEqualTo(TombstoneRetirementStatus.HANDLE_PRESENT);
        assertThat(fixture.currentRoot().value().tombstoneFirstAbsentAtMillis()).isZero();
        assertThat(fixture.objectStore().deleteCalls).hasValue(0);
    }

    @Test
    void ownerAppearanceClearsTheWindowWithoutTreatingRegistrationAsDeleteAuthority() {
        Fixture fixture = fixture(false);
        fixture.coordinator().retire(fixture.deletedRoot()).join();
        VersionedPhysicalObjectRoot observed = fixture.currentRoot();
        fixture.domain().mode = DomainMode.OWNER;
        fixture.clock().setMillis(7_000);

        TombstoneRetirementResult result = fixture.coordinator().retire(observed).join();

        assertThat(result.status()).isEqualTo(TombstoneRetirementStatus.OWNER_PRESENT);
        assertThat(fixture.currentRoot().value().tombstoneFirstAbsentAtMillis()).isZero();
        assertThat(fixture.store().getRoot(CLUSTER, OBJECT).join()).isPresent();
    }

    @Test
    void authorityDigestDriftRestartsTheSeparatedAbsenceWindow() {
        Fixture fixture = fixture(false);
        fixture.coordinator().retire(fixture.deletedRoot()).join();
        VersionedPhysicalObjectRoot first = fixture.currentRoot();
        String firstProof = first.value().tombstoneProofSha256();
        fixture.domain().authorityVersion++;
        fixture.clock().setMillis(7_000);

        TombstoneRetirementResult result = fixture.coordinator().retire(first).join();

        VersionedPhysicalObjectRoot restarted = fixture.currentRoot();
        assertThat(result.status()).isEqualTo(TombstoneRetirementStatus.NOT_OLD_ENOUGH);
        assertThat(restarted.value().tombstoneFirstAbsentAtMillis()).isEqualTo(7_000);
        assertThat(restarted.value().tombstoneProofSha256()).isNotEqualTo(firstProof);
    }

    @Test
    void lateExactPutIsDeletedUnderFreshOwnerlessProofAndRestartsTheWindow() {
        Fixture fixture = fixture(false);
        fixture.coordinator().retire(fixture.deletedRoot()).join();
        VersionedPhysicalObjectRoot observed = fixture.currentRoot();
        fixture.objectStore().exists = true;
        fixture.clock().setMillis(7_000);

        TombstoneRetirementResult result = fixture.coordinator().retire(observed).join();

        assertThat(result.status()).isEqualTo(TombstoneRetirementStatus.OBJECT_PRESENT);
        assertThat(fixture.objectStore().exists).isFalse();
        assertThat(fixture.objectStore().deleteCalls).hasValue(1);
        assertThat(fixture.currentRoot().value().tombstoneFirstAbsentAtMillis()).isZero();
    }

    @Test
    void mismatchedReappearingBytesAreQuarantinedWithoutDelete() {
        Fixture fixture = fixture(false);
        fixture.coordinator().retire(fixture.deletedRoot()).join();
        VersionedPhysicalObjectRoot observed = fixture.currentRoot();
        fixture.objectStore().exists = true;
        fixture.objectStore().mismatched = true;
        fixture.clock().setMillis(7_000);

        TombstoneRetirementResult result = fixture.coordinator().retire(observed).join();

        assertThat(result.status()).isEqualTo(TombstoneRetirementStatus.QUARANTINED);
        assertThat(fixture.objectStore().deleteCalls).hasValue(0);
        assertThat(fixture.store().getRoot(CLUSTER, OBJECT).join()).isPresent();
        assertThat(fixture.currentRoot().value().tombstoneFirstAbsentAtMillis()).isZero();
    }

    @Test
    void auditAndRootDeleteResponseLossConvergeReferencesThenManifestThenRoot() {
        Fixture fixture = fixture(true);
        fixture.coordinator().retire(fixture.deletedRoot()).join();
        VersionedPhysicalObjectRoot observed = fixture.currentRoot();
        fixture.auditStore().loseReferenceDeleteResponse = true;
        fixture.auditStore().loseManifestDeleteResponse = true;
        fixture.store().loseRootDeleteResponse = true;
        fixture.clock().setMillis(7_000);

        TombstoneRetirementResult result = fixture.coordinator().retire(observed).join();

        assertThat(result.status()).isEqualTo(TombstoneRetirementStatus.RETIRED);
        assertThat(result.referencesRetired()).isTrue();
        assertThat(result.manifestRetired()).isTrue();
        assertThat(result.rootRetired()).isTrue();
        assertThat(fixture.mutationOrder())
                .containsExactly("references", "manifest", "root");
        assertThat(fixture.store().getRoot(CLUSTER, OBJECT).join()).isEmpty();
        assertThat(fixture.auditStore().references).isEmpty();
        assertThat(fixture.auditStore().manifest).isEmpty();
    }

    Fixture fixture(boolean withAudits) {
        ArrayList<String> mutationOrder = new ArrayList<>();
        TrackingPhysicalStore store = new TrackingPhysicalStore(mutationOrder);
        VersionedPhysicalObjectRoot deleted = deletedRoot(
                store, withAudits ? "object-1" : "");
        MutableDomain domain = new MutableDomain(config().referenceDomainConfig());
        GcReferenceDomainRegistry registry = new GcReferenceDomainRegistry(
                config(), scheduler, List.of(domain));
        ExactObjectStore objectStore = new ExactObjectStore();
        FakeAuditStore auditStore = new FakeAuditStore(mutationOrder, withAudits);
        MutableClock clock = new MutableClock(5_000);
        var coordinator = new DefaultPhysicalRootTombstoneRetirementCoordinator(
                CLUSTER,
                config(),
                store,
                auditStore,
                registry,
                objectStore,
                clock,
                scheduler);
        return new Fixture(
                store,
                domain,
                objectStore,
                auditStore,
                clock,
                coordinator,
                deleted,
                mutationOrder);
    }

    private static VersionedPhysicalObjectRoot deletedRoot(
            FakePhysicalObjectMetadataStore store,
            String objectId) {
        VersionedPhysicalObjectRoot active = store.createRoot(
                CLUSTER,
                new PhysicalObjectRootRecord(
                        1,
                        OBJECT.value(),
                        OBJECT_KEY.value(),
                        objectId,
                        1,
                        42,
                        STORAGE_CHECKSUM.type().name(),
                        STORAGE_CHECKSUM.value(),
                        "",
                        "",
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
                        0))
                .join();
        VersionedPhysicalObjectRoot marked = store.compareAndSetRoot(
                CLUSTER,
                lifecycle(
                        active.value(),
                        PhysicalObjectLifecycle.MARKED,
                        2,
                        300,
                        400,
                        0,
                        0),
                active.metadataVersion())
                .join();
        VersionedPhysicalObjectRoot deleting = store.compareAndSetRoot(
                CLUSTER,
                lifecycle(
                        marked.value(),
                        PhysicalObjectLifecycle.DELETING,
                        3,
                        300,
                        400,
                        500,
                        0),
                marked.metadataVersion())
                .join();
        return store.compareAndSetRoot(
                        CLUSTER,
                        lifecycle(
                                deleting.value(),
                                PhysicalObjectLifecycle.DELETED,
                                4,
                                300,
                                400,
                                500,
                                1_000),
                        deleting.metadataVersion())
                .join();
    }

    private static PhysicalObjectRootRecord lifecycle(
            PhysicalObjectRootRecord root,
            PhysicalObjectLifecycle lifecycle,
            long epoch,
            long markedAt,
            long deleteNotBefore,
            long deleteStarted,
            long deletedAt) {
        return new PhysicalObjectRootRecord(
                root.schemaVersion(),
                root.objectKeyHash(),
                root.objectKey(),
                root.objectId(),
                root.objectKindId(),
                root.objectLength(),
                root.storageChecksumType(),
                root.storageChecksumValue(),
                root.contentSha256(),
                root.etag(),
                lifecycle,
                epoch,
                root.createdAtMillis(),
                root.orphanNotBeforeMillis(),
                ATTEMPT_ID,
                REFERENCE_SHA,
                markedAt,
                deleteNotBefore,
                deleteStarted,
                deletedAt,
                0,
                "",
                "",
                0);
    }

    private static PhysicalGcConfig config() {
        return new PhysicalGcConfig(
                true,
                false,
                2,
                2,
                1,
                4_096,
                10,
                10,
                Duration.ofSeconds(1),
                Duration.ofSeconds(10),
                Duration.ofSeconds(3),
                Duration.ofMillis(5),
                Duration.ofSeconds(11),
                Duration.ofSeconds(30),
                Duration.ofSeconds(1),
                Duration.ofSeconds(2),
                Duration.ofSeconds(2),
                Duration.ofSeconds(2));
    }

    private static Checksum sha(char value) {
        return new Checksum(ChecksumType.SHA256, String.valueOf(value).repeat(64));
    }

    record Fixture(
            TrackingPhysicalStore store,
            MutableDomain domain,
            ExactObjectStore objectStore,
            FakeAuditStore auditStore,
            MutableClock clock,
            DefaultPhysicalRootTombstoneRetirementCoordinator coordinator,
            VersionedPhysicalObjectRoot deletedRoot,
            List<String> mutationOrder) {
        VersionedPhysicalObjectRoot currentRoot() {
            return store.getRoot(CLUSTER, OBJECT).join().orElseThrow();
        }

        void reappearAfterReferenceRetirement() {
            auditStore.afterReferenceDelete = () -> objectStore.exists = true;
        }

        void reappearAfterManifestRetirement() {
            auditStore.afterManifestDelete = () -> objectStore.exists = true;
        }

        boolean auditsAbsent() {
            return auditStore.references.isEmpty() && auditStore.manifest.isEmpty();
        }
    }

    private enum DomainMode {
        CLEAR,
        OWNER,
        VETO
    }

    static final class MutableDomain implements GcReferenceDomain {
        private final GcReferenceDomainConfig config;
        private volatile DomainMode mode = DomainMode.CLEAR;
        private volatile long authorityVersion = 1;
        volatile boolean becomeOwnerBeforeRevalidation;

        private MutableDomain(GcReferenceDomainConfig config) {
            this.config = config;
        }

        @Override
        public String domainId() {
            return "test-owner-v1";
        }

        @Override
        public int protocolVersion() {
            return 1;
        }

        @Override
        public CompletableFuture<GcReferenceSnapshot> snapshot(GcReferenceQuery query) {
            GcReferenceSnapshotBuilder builder = new GcReferenceSnapshotBuilder(
                    domainId(), protocolVersion(), query, config);
            builder.addAuthority(new GcAuthorityToken(
                    "/authority",
                    authorityVersion,
                    authorityVersion % 2 == 0 ? sha('e') : sha('d')));
            if (mode == DomainMode.OWNER) {
                builder.addReference(new GcReference(
                        "test-owner",
                        "owner-1",
                        "/owner",
                        7,
                        sha('f')));
                builder.veto();
            } else if (mode == DomainMode.VETO) {
                builder.veto();
            }
            return CompletableFuture.completedFuture(builder.build());
        }

        @Override
        public CompletableFuture<Boolean> stillMatches(
                GcReferenceQuery query,
                GcReferenceSnapshot snapshot) {
            if (becomeOwnerBeforeRevalidation) {
                becomeOwnerBeforeRevalidation = false;
                mode = DomainMode.OWNER;
            }
            return snapshot(query).thenApply(snapshot::equals);
        }
    }

    private static final class TrackingPhysicalStore
            extends FakePhysicalObjectMetadataStore {
        private final List<String> mutationOrder;
        private volatile boolean loseRootDeleteResponse;

        private TrackingPhysicalStore(List<String> mutationOrder) {
            this.mutationOrder = mutationOrder;
        }

        @Override
        public CompletableFuture<Void> deleteRoot(
                String cluster,
                ObjectKeyHash object,
                long expectedVersion,
                Checksum expectedRootSha256) {
            return super.deleteRoot(
                            cluster, object, expectedVersion, expectedRootSha256)
                    .thenCompose(ignored -> {
                        mutationOrder.add("root");
                        if (loseRootDeleteResponse) {
                            loseRootDeleteResponse = false;
                            return CompletableFuture.failedFuture(
                                    new IllegalStateException(
                                            "lost root delete response"));
                        }
                        return CompletableFuture.completedFuture(null);
                    });
        }
    }

    static final class ExactObjectStore implements ObjectStore {
        final AtomicInteger deleteCalls = new AtomicInteger();
        volatile boolean exists;
        volatile boolean mismatched;
        volatile boolean loseDeleteResponse;

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
            if (!exists) {
                return CompletableFuture.failedFuture(new NereusException(
                        ErrorCode.OBJECT_NOT_FOUND, true, "object not found"));
            }
            return CompletableFuture.completedFuture(new HeadObjectResult(
                    key,
                    mismatched ? 43 : 42,
                    STORAGE_CHECKSUM,
                    Optional.empty(),
                    Map.of()));
        }

        @Override
        public CompletableFuture<DeleteObjectResult> deleteObject(
                ObjectKey key,
                DeleteObjectOptions options) {
            deleteCalls.incrementAndGet();
            assertThat(key).isEqualTo(OBJECT_KEY);
            assertThat(options.expectedLength()).isEqualTo(42);
            assertThat(options.expectedStorageChecksum()).isEqualTo(STORAGE_CHECKSUM);
            DeleteObjectResult.Status status = exists
                    ? DeleteObjectResult.Status.DELETED
                    : DeleteObjectResult.Status.ALREADY_ABSENT;
            exists = false;
            if (loseDeleteResponse) {
                loseDeleteResponse = false;
                return CompletableFuture.failedFuture(
                        new IllegalStateException("lost late object delete response"));
            }
            return CompletableFuture.completedFuture(
                    new DeleteObjectResult(key, status));
        }

        @Override
        public void close() {
        }
    }

    private static final class FakeAuditStore
            implements ObjectAuditRetirementStore {
        private final List<String> mutationOrder;
        private Optional<VersionedObjectReferencesAudit> references;
        private Optional<VersionedObjectManifestAudit> manifest;
        private volatile boolean loseReferenceDeleteResponse;
        private volatile boolean loseManifestDeleteResponse;
        private volatile Runnable afterReferenceDelete = () -> { };
        private volatile Runnable afterManifestDelete = () -> { };

        private FakeAuditStore(List<String> mutationOrder, boolean populated) {
            this.mutationOrder = mutationOrder;
            if (populated) {
                ObjectReferenceRecord referenceValue = new ObjectReferenceRecord(
                        "object-1", List.of(), 900, 11);
                references = Optional.of(new VersionedObjectReferencesAudit(
                        "/references/object-1",
                        referenceValue,
                        11,
                        sha('1')));
                ObjectManifestRecord manifestValue = new ObjectManifestRecord(
                        "object-1",
                        OBJECT_KEY.value(),
                        "MULTI_STREAM_WAL_OBJECT",
                        "DELETED",
                        1,
                        0,
                        "test",
                        "writer",
                        "run",
                        1,
                        100,
                        200,
                        42,
                        "CRC32C",
                        "01020304",
                        STORAGE_CHECKSUM.type().name(),
                        STORAGE_CHECKSUM.value(),
                        List.of(),
                        300,
                        12);
                manifest = Optional.of(new VersionedObjectManifestAudit(
                        "/manifests/object-1",
                        manifestValue,
                        12,
                        sha('2')));
            } else {
                references = Optional.empty();
                manifest = Optional.empty();
            }
        }

        @Override
        public CompletableFuture<Optional<VersionedObjectManifestAudit>> getManifest(
                String cluster,
                ObjectId objectId) {
            return CompletableFuture.completedFuture(manifest);
        }

        @Override
        public CompletableFuture<Optional<VersionedObjectReferencesAudit>> getReferences(
                String cluster,
                ObjectId objectId) {
            return CompletableFuture.completedFuture(references);
        }

        @Override
        public CompletableFuture<Void> deleteReferences(
                String cluster,
                ObjectId objectId,
                long expectedVersion,
                Checksum expectedDurableValueSha256) {
            VersionedObjectReferencesAudit current = references.orElseThrow();
            assertThat(current.metadataVersion()).isEqualTo(expectedVersion);
            assertThat(current.durableValueSha256())
                    .isEqualTo(expectedDurableValueSha256);
            references = Optional.empty();
            mutationOrder.add("references");
            afterReferenceDelete.run();
            if (loseReferenceDeleteResponse) {
                loseReferenceDeleteResponse = false;
                return CompletableFuture.failedFuture(
                        new IllegalStateException("lost reference delete response"));
            }
            return CompletableFuture.completedFuture(null);
        }

        @Override
        public CompletableFuture<Void> deleteManifest(
                String cluster,
                ObjectId objectId,
                long expectedVersion,
                Checksum expectedDurableValueSha256) {
            VersionedObjectManifestAudit current = manifest.orElseThrow();
            assertThat(current.metadataVersion()).isEqualTo(expectedVersion);
            assertThat(current.durableValueSha256())
                    .isEqualTo(expectedDurableValueSha256);
            manifest = Optional.empty();
            mutationOrder.add("manifest");
            afterManifestDelete.run();
            if (loseManifestDeleteResponse) {
                loseManifestDeleteResponse = false;
                return CompletableFuture.failedFuture(
                        new IllegalStateException("lost manifest delete response"));
            }
            return CompletableFuture.completedFuture(null);
        }

        @Override
        public void close() {
        }
    }

    static final class MutableClock extends Clock {
        private volatile long millis;

        private MutableClock(long millis) {
            this.millis = millis;
        }

        void setMillis(long millis) {
            this.millis = millis;
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
