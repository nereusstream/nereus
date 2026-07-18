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
import com.nereusstream.core.physical.GcReferenceDomain;
import com.nereusstream.core.physical.GcReferenceDomainConfig;
import com.nereusstream.core.physical.GcReferenceQuery;
import com.nereusstream.core.physical.GcReferenceSnapshot;
import com.nereusstream.core.physical.GcReferenceSnapshotBuilder;
import com.nereusstream.materialization.MaterializationSchedulers;
import com.nereusstream.metadata.oxia.F4ScanToken;
import com.nereusstream.metadata.oxia.FakePhysicalObjectMetadataStore;
import com.nereusstream.metadata.oxia.PhysicalObjectRootScanPage;
import com.nereusstream.metadata.oxia.VersionedPhysicalObjectRoot;
import com.nereusstream.metadata.oxia.records.ObjectManifestRecord;
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
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.Optional;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.ScheduledThreadPoolExecutor;
import java.util.concurrent.atomic.AtomicInteger;
import org.junit.jupiter.api.Test;

class PhysicalRootTombstoneRetirementScaleTest {
    private static final int ROOT_COUNT = 10_000;
    private static final int PAGE_SIZE = 32;
    private static final String CLUSTER = "cluster-tombstone-scale";
    private static final String ATTEMPT_ID = "a".repeat(52);
    private static final String REFERENCE_SHA = "b".repeat(64);
    private static final Checksum STORAGE_CHECKSUM =
            new Checksum(ChecksumType.CRC32C, "01020304");

    @Test
    void retiresTenThousandDeletedRootsThroughTwoBoundedWindowsWithoutAccumulation() {
        ScheduledThreadPoolExecutor scheduler =
                MaterializationSchedulers.newSingleThreadScheduler(Thread::new);
        PhysicalObjectRootScanner scanner = null;
        try {
            PhysicalGcConfig config = config();
            ScaleAuditStore audits = new ScaleAuditStore();
            TrackingPhysicalStore metadata = new TrackingPhysicalStore(audits);
            for (int index = 0; index < ROOT_COUNT; index++) {
                createDeletedRoot(metadata, audits, index);
            }

            ClearReferenceDomain domain = new ClearReferenceDomain(
                    config.referenceDomainConfig());
            GcReferenceDomainRegistry domains = new GcReferenceDomainRegistry(
                    config, scheduler, List.of(domain));
            AbsentObjectStore objects = new AbsentObjectStore();
            MutableClock clock = new MutableClock(5_000);
            DefaultPhysicalRootTombstoneRetirementCoordinator coordinator =
                    new DefaultPhysicalRootTombstoneRetirementCoordinator(
                            CLUSTER,
                            config,
                            metadata,
                            audits,
                            domains,
                            objects,
                            clock,
                            scheduler);
            scanner = new PhysicalObjectRootScanner(
                    CLUSTER, config, metadata, scheduler);

            RetirementPass firstPass = new RetirementPass(
                    TombstoneRetirementStatus.NOT_OLD_ENOUGH,
                    metadata,
                    audits);
            PhysicalObjectRootScanResult first = scanner.scan(
                            root -> firstPass.visit(coordinator, root))
                    .join();

            assertThat(first.deletedRoots()).isEqualTo(ROOT_COUNT);
            assertThat(first.totalRoots()).isEqualTo(ROOT_COUNT);
            assertThat(firstPass.completed()).isEqualTo(ROOT_COUNT);
            assertThat(firstPass.maximumConcurrentVisits()).isEqualTo(1);
            assertThat(audits.referenceCount()).isEqualTo(ROOT_COUNT);
            assertThat(audits.manifestCount()).isEqualTo(ROOT_COUNT);
            assertThat(audits.referencesDeleted()).isZero();
            assertThat(audits.manifestsDeleted()).isZero();
            assertThat(scheduler.getQueue()).isEmpty();

            clock.setMillis(7_000);
            RetirementPass secondPass = new RetirementPass(
                    TombstoneRetirementStatus.RETIRED,
                    metadata,
                    audits);
            PhysicalObjectRootScanResult second = scanner.scan(
                            root -> secondPass.visit(coordinator, root))
                    .join();

            assertThat(second.deletedRoots()).isEqualTo(ROOT_COUNT);
            assertThat(second.totalRoots()).isEqualTo(ROOT_COUNT);
            assertThat(secondPass.completed()).isEqualTo(ROOT_COUNT);
            assertThat(secondPass.maximumConcurrentVisits()).isEqualTo(1);
            assertThat(audits.referenceCount()).isZero();
            assertThat(audits.manifestCount()).isZero();
            assertThat(audits.referencesDeleted()).isEqualTo(ROOT_COUNT);
            assertThat(audits.manifestsDeleted()).isEqualTo(ROOT_COUNT);
            assertThat(metadata.rootsDeleted()).isEqualTo(ROOT_COUNT);
            assertThat(scheduler.getQueue()).isEmpty();

            PhysicalObjectRootScanResult empty = scanner.scan(root ->
                            CompletableFuture.failedFuture(
                                    new AssertionError("retired root was rediscovered")))
                    .join();

            assertThat(empty.totalRoots()).isZero();
            assertThat(metadata.maximumPageSize()).isLessThanOrEqualTo(PAGE_SIZE);
            assertThat(metadata.continuedScanCalls()).isPositive();
            assertThat(metadata.maximumConcurrentRootDeletes()).isEqualTo(1);
            assertThat(audits.maximumConcurrentCalls()).isEqualTo(1);
            assertThat(objects.maximumConcurrentHeads()).isEqualTo(1);
            assertThat(objects.headCalls()).isGreaterThanOrEqualTo(ROOT_COUNT * 2);
            assertThat(objects.deleteCalls()).isZero();
            assertThat(domain.maximumConcurrentCalls()).isEqualTo(1);
            assertThat(scheduler.getRemoveOnCancelPolicy()).isTrue();
            assertThat(scheduler.getQueue()).isEmpty();
        } finally {
            if (scanner != null) {
                scanner.close();
            }
            scheduler.shutdownNow();
        }
    }

    private static void createDeletedRoot(
            TrackingPhysicalStore metadata,
            ScaleAuditStore audits,
            int index) {
        ObjectKey key = new ObjectKey("objects/tombstone-scale/" + index);
        ObjectKeyHash object = ObjectKeyHash.from(key);
        String objectId = "object-tombstone-scale-" + index;
        VersionedPhysicalObjectRoot active = metadata.createRoot(
                        CLUSTER,
                        new PhysicalObjectRootRecord(
                                1,
                                object.value(),
                                key.value(),
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
        VersionedPhysicalObjectRoot marked = metadata.compareAndSetRoot(
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
        VersionedPhysicalObjectRoot deleting = metadata.compareAndSetRoot(
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
        metadata.compareAndSetRoot(
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
        audits.add(objectId, key);
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
                PAGE_SIZE,
                PAGE_SIZE,
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

    private static final class RetirementPass {
        private final TombstoneRetirementStatus expected;
        private final TrackingPhysicalStore metadata;
        private final ScaleAuditStore audits;
        private final AtomicInteger concurrent = new AtomicInteger();
        private final AtomicInteger maximumConcurrent = new AtomicInteger();
        private final AtomicInteger completed = new AtomicInteger();

        private RetirementPass(
                TombstoneRetirementStatus expected,
                TrackingPhysicalStore metadata,
                ScaleAuditStore audits) {
            this.expected = expected;
            this.metadata = metadata;
            this.audits = audits;
        }

        private CompletableFuture<Void> visit(
                DefaultPhysicalRootTombstoneRetirementCoordinator coordinator,
                VersionedPhysicalObjectRoot root) {
            int admitted = concurrent.incrementAndGet();
            maximumConcurrent.accumulateAndGet(admitted, Math::max);
            CompletableFuture<Void> result;
            try {
                result = coordinator.retire(root).thenAccept(retired -> {
                    assertThat(retired.status()).isEqualTo(expected);
                    ObjectKeyHash object = new ObjectKeyHash(root.value().objectKeyHash());
                    String objectId = root.value().objectId();
                    if (expected == TombstoneRetirementStatus.RETIRED) {
                        assertThat(retired.referencesRetired()).isTrue();
                        assertThat(retired.manifestRetired()).isTrue();
                        assertThat(retired.rootRetired()).isTrue();
                        assertThat(metadata.getRoot(CLUSTER, object).join()).isEmpty();
                        assertThat(audits.absent(objectId)).isTrue();
                    } else {
                        VersionedPhysicalObjectRoot current = metadata
                                .getRoot(CLUSTER, object)
                                .join()
                                .orElseThrow();
                        assertThat(current.value().tombstoneFirstAbsentAtMillis())
                                .isEqualTo(5_000);
                        assertThat(current.value().tombstoneProofSha256()).hasSize(64);
                        assertThat(audits.absent(objectId)).isFalse();
                    }
                    completed.incrementAndGet();
                });
            } catch (Throwable failure) {
                concurrent.decrementAndGet();
                return CompletableFuture.failedFuture(failure);
            }
            return result.whenComplete((ignored, failure) -> concurrent.decrementAndGet());
        }

        private int completed() {
            return completed.get();
        }

        private int maximumConcurrentVisits() {
            return maximumConcurrent.get();
        }
    }

    private static final class TrackingPhysicalStore
            extends FakePhysicalObjectMetadataStore {
        private final ScaleAuditStore audits;
        private final AtomicInteger maximumPageSize = new AtomicInteger();
        private final AtomicInteger continuedScanCalls = new AtomicInteger();
        private final AtomicInteger rootDeletes = new AtomicInteger();
        private final AtomicInteger concurrentRootDeletes = new AtomicInteger();
        private final AtomicInteger maximumConcurrentRootDeletes = new AtomicInteger();

        private TrackingPhysicalStore(ScaleAuditStore audits) {
            this.audits = audits;
        }

        @Override
        public CompletableFuture<PhysicalObjectRootScanPage> scanRoots(
                String cluster,
                int shard,
                Optional<F4ScanToken> continuation,
                int limit) {
            return super.scanRoots(cluster, shard, continuation, limit)
                    .thenApply(page -> {
                        maximumPageSize.accumulateAndGet(
                                page.values().size(), Math::max);
                        if (continuation.isPresent()) {
                            continuedScanCalls.incrementAndGet();
                        }
                        return page;
                    });
        }

        @Override
        public CompletableFuture<Void> deleteRoot(
                String cluster,
                ObjectKeyHash object,
                long expectedVersion,
                Checksum expectedRootSha256) {
            int admitted = concurrentRootDeletes.incrementAndGet();
            maximumConcurrentRootDeletes.accumulateAndGet(admitted, Math::max);
            CompletableFuture<Void> deletion;
            try {
                VersionedPhysicalObjectRoot current = getRoot(cluster, object)
                        .join()
                        .orElseThrow();
                assertThat(audits.absent(current.value().objectId())).isTrue();
                deletion = super.deleteRoot(
                        cluster, object, expectedVersion, expectedRootSha256);
            } catch (Throwable failure) {
                concurrentRootDeletes.decrementAndGet();
                return CompletableFuture.failedFuture(failure);
            }
            return deletion.thenRun(rootDeletes::incrementAndGet)
                    .whenComplete((ignored, failure) ->
                            concurrentRootDeletes.decrementAndGet());
        }

        private int maximumPageSize() {
            return maximumPageSize.get();
        }

        private int continuedScanCalls() {
            return continuedScanCalls.get();
        }

        private int rootsDeleted() {
            return rootDeletes.get();
        }

        private int maximumConcurrentRootDeletes() {
            return maximumConcurrentRootDeletes.get();
        }
    }

    private static final class ScaleAuditStore
            implements ObjectAuditRetirementStore {
        private final Map<String, VersionedObjectReferencesAudit> references =
                new HashMap<>();
        private final Map<String, VersionedObjectManifestAudit> manifests =
                new HashMap<>();
        private final AtomicInteger concurrent = new AtomicInteger();
        private final AtomicInteger maximumConcurrent = new AtomicInteger();
        private final AtomicInteger referencesDeleted = new AtomicInteger();
        private final AtomicInteger manifestsDeleted = new AtomicInteger();

        private synchronized void add(String objectId, ObjectKey key) {
            ObjectReferenceRecord referenceValue = new ObjectReferenceRecord(
                    objectId, List.of(), 900, 11);
            references.put(objectId, new VersionedObjectReferencesAudit(
                    "/references/" + objectId,
                    referenceValue,
                    11,
                    sha('1')));
            ObjectManifestRecord manifestValue = new ObjectManifestRecord(
                    objectId,
                    key.value(),
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
            manifests.put(objectId, new VersionedObjectManifestAudit(
                    "/manifests/" + objectId,
                    manifestValue,
                    12,
                    sha('2')));
        }

        @Override
        public CompletableFuture<Optional<VersionedObjectManifestAudit>> getManifest(
                String cluster,
                ObjectId objectId) {
            return call(() -> Optional.ofNullable(manifests.get(objectId.value())));
        }

        @Override
        public CompletableFuture<Optional<VersionedObjectReferencesAudit>> getReferences(
                String cluster,
                ObjectId objectId) {
            return call(() -> Optional.ofNullable(references.get(objectId.value())));
        }

        @Override
        public CompletableFuture<Void> deleteReferences(
                String cluster,
                ObjectId objectId,
                long expectedVersion,
                Checksum expectedDurableValueSha256) {
            return call(() -> {
                VersionedObjectReferencesAudit current =
                        references.get(objectId.value());
                assertThat(current).isNotNull();
                assertThat(current.metadataVersion()).isEqualTo(expectedVersion);
                assertThat(current.durableValueSha256())
                        .isEqualTo(expectedDurableValueSha256);
                references.remove(objectId.value());
                referencesDeleted.incrementAndGet();
                return null;
            });
        }

        @Override
        public CompletableFuture<Void> deleteManifest(
                String cluster,
                ObjectId objectId,
                long expectedVersion,
                Checksum expectedDurableValueSha256) {
            return call(() -> {
                assertThat(references).doesNotContainKey(objectId.value());
                VersionedObjectManifestAudit current = manifests.get(objectId.value());
                assertThat(current).isNotNull();
                assertThat(current.metadataVersion()).isEqualTo(expectedVersion);
                assertThat(current.durableValueSha256())
                        .isEqualTo(expectedDurableValueSha256);
                manifests.remove(objectId.value());
                manifestsDeleted.incrementAndGet();
                return null;
            });
        }

        private synchronized <T> CompletableFuture<T> call(
                java.util.function.Supplier<T> operation) {
            int admitted = concurrent.incrementAndGet();
            maximumConcurrent.accumulateAndGet(admitted, Math::max);
            try {
                return CompletableFuture.completedFuture(operation.get());
            } catch (Throwable failure) {
                return CompletableFuture.failedFuture(failure);
            } finally {
                concurrent.decrementAndGet();
            }
        }

        private synchronized boolean absent(String objectId) {
            return !references.containsKey(objectId) && !manifests.containsKey(objectId);
        }

        private synchronized int referenceCount() {
            return references.size();
        }

        private synchronized int manifestCount() {
            return manifests.size();
        }

        private int referencesDeleted() {
            return referencesDeleted.get();
        }

        private int manifestsDeleted() {
            return manifestsDeleted.get();
        }

        private int maximumConcurrentCalls() {
            return maximumConcurrent.get();
        }

        @Override
        public void close() {
        }
    }

    private static final class ClearReferenceDomain implements GcReferenceDomain {
        private final GcReferenceDomainConfig config;
        private final AtomicInteger concurrent = new AtomicInteger();
        private final AtomicInteger maximumConcurrent = new AtomicInteger();

        private ClearReferenceDomain(GcReferenceDomainConfig config) {
            this.config = config;
        }

        @Override
        public String domainId() {
            return "tombstone-scale-owner-v1";
        }

        @Override
        public int protocolVersion() {
            return 1;
        }

        @Override
        public CompletableFuture<GcReferenceSnapshot> snapshot(
                GcReferenceQuery query) {
            int admitted = concurrent.incrementAndGet();
            maximumConcurrent.accumulateAndGet(admitted, Math::max);
            try {
                GcReferenceSnapshotBuilder builder = new GcReferenceSnapshotBuilder(
                        domainId(), protocolVersion(), query, config);
                builder.addAuthority(new GcAuthorityToken(
                        "/authority/tombstone-scale", 1, sha('d')));
                return CompletableFuture.completedFuture(builder.build());
            } catch (Throwable failure) {
                return CompletableFuture.failedFuture(failure);
            } finally {
                concurrent.decrementAndGet();
            }
        }

        @Override
        public CompletableFuture<Boolean> stillMatches(
                GcReferenceQuery query,
                GcReferenceSnapshot snapshot) {
            return snapshot(query).thenApply(snapshot::equals);
        }

        private int maximumConcurrentCalls() {
            return maximumConcurrent.get();
        }
    }

    private static final class AbsentObjectStore implements ObjectStore {
        private final AtomicInteger headCalls = new AtomicInteger();
        private final AtomicInteger deleteCalls = new AtomicInteger();
        private final AtomicInteger concurrentHeads = new AtomicInteger();
        private final AtomicInteger maximumConcurrentHeads = new AtomicInteger();

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
            int admitted = concurrentHeads.incrementAndGet();
            maximumConcurrentHeads.accumulateAndGet(admitted, Math::max);
            headCalls.incrementAndGet();
            try {
                return CompletableFuture.failedFuture(new NereusException(
                        ErrorCode.OBJECT_NOT_FOUND,
                        true,
                        "object is durably absent"));
            } finally {
                concurrentHeads.decrementAndGet();
            }
        }

        @Override
        public CompletableFuture<DeleteObjectResult> deleteObject(
                ObjectKey key,
                DeleteObjectOptions options) {
            deleteCalls.incrementAndGet();
            return CompletableFuture.failedFuture(
                    new AssertionError("an absent tombstone object must not be deleted"));
        }

        private int headCalls() {
            return headCalls.get();
        }

        private int deleteCalls() {
            return deleteCalls.get();
        }

        private int maximumConcurrentHeads() {
            return maximumConcurrentHeads.get();
        }

        @Override
        public void close() {
        }
    }

    private static final class MutableClock extends Clock {
        private volatile long millis;

        private MutableClock(long millis) {
            this.millis = millis;
        }

        private void setMillis(long millis) {
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
