/* Licensed under the Apache License, Version 2.0 */
package com.nereusstream.core.physical;

import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.assertThatThrownBy;

import com.nereusstream.api.Checksum;
import com.nereusstream.api.ChecksumType;
import com.nereusstream.api.ErrorCode;
import com.nereusstream.api.NereusException;
import com.nereusstream.api.ObjectKey;
import com.nereusstream.metadata.oxia.FakePhysicalObjectMetadataStore;
import com.nereusstream.metadata.oxia.VersionedPhysicalObjectRoot;
import com.nereusstream.metadata.oxia.VersionedReaderLease;
import com.nereusstream.metadata.oxia.records.ObjectReaderLeaseRecord;
import java.time.Clock;
import java.time.Duration;
import java.time.Instant;
import java.time.ZoneId;
import java.time.ZoneOffset;
import java.util.Optional;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.CompletionException;
import java.util.concurrent.atomic.AtomicInteger;
import org.junit.jupiter.api.Test;

class ObjectReadPinManagerTest {
    private static final String CLUSTER = "cluster-a";
    private static final String PROCESS = "p".repeat(26);
    private static final long NOW = 1_000_000L;

    @Test
    void multiplexesOneDurableLeaseAndDeletesOnlyAfterLastRelease() {
        FakePhysicalObjectMetadataStore store = new FakePhysicalObjectMetadataStore();
        MutableClock clock = new MutableClock(NOW);
        AtomicInteger validations = new AtomicInteger();
        DefaultObjectReadPinManager manager = manager(store, clock);
        PhysicalObjectIdentity object = object();

        ObjectReadLease first = manager.acquire(
                object, NOW + 2_000, () -> validated(validations)).join();
        ObjectReadLease second = manager.acquire(
                object, NOW + 3_000, () -> validated(validations)).join();

        assertThat(first.leaseId()).isEqualTo(second.leaseId());
        assertThat(validations.get()).isEqualTo(2);
        VersionedReaderLease durable = store.readerLease(
                CLUSTER, object.objectKeyHash(), PROCESS).orElseThrow();
        assertThat(durable.value().maximumReadDeadlineMillis()).isEqualTo(NOW + 3_000);

        first.release().join();
        assertThat(store.readerLease(CLUSTER, object.objectKeyHash(), PROCESS)).isPresent();
        second.release().join();
        second.release().join();
        assertThat(store.readerLease(CLUSTER, object.objectKeyHash(), PROCESS)).isEmpty();
    }

    @Test
    void renewsBeforeNewAdmissionAndKeepsTheSameLeaseIdentity() {
        FakePhysicalObjectMetadataStore store = new FakePhysicalObjectMetadataStore();
        MutableClock clock = new MutableClock(NOW);
        DefaultObjectReadPinManager manager = manager(store, clock);
        PhysicalObjectIdentity object = object();
        ObjectReadLease first = manager.acquire(
                object, NOW + 2_000, () -> CompletableFuture.completedFuture(null)).join();
        VersionedReaderLease before = store.readerLease(
                CLUSTER, object.objectKeyHash(), PROCESS).orElseThrow();

        clock.advance(Duration.ofSeconds(8));
        ObjectReadLease second = manager.acquire(
                object, clock.millis() + 5_000, () -> CompletableFuture.completedFuture(null)).join();
        VersionedReaderLease after = store.readerLease(
                CLUSTER, object.objectKeyHash(), PROCESS).orElseThrow();

        assertThat(after.metadataVersion()).isGreaterThan(before.metadataVersion());
        assertThat(after.value().leaseId()).isEqualTo(before.value().leaseId());
        assertThat(after.value().renewalSequence()).isEqualTo(before.value().renewalSequence() + 1);
        assertThat(first.leaseId()).isEqualTo(second.leaseId());
        first.release().join();
        second.release().join();
    }

    @Test
    void failedSelectionPostCheckRemovesTheJustWrittenLease() {
        FakePhysicalObjectMetadataStore store = new FakePhysicalObjectMetadataStore();
        DefaultObjectReadPinManager manager = manager(store, new MutableClock(NOW));
        PhysicalObjectIdentity object = object();
        NereusException stale = new NereusException(
                ErrorCode.READ_RESOLUTION_FAILED, true, "selected generation changed");

        assertThatThrownBy(() -> manager.acquire(
                        object,
                        NOW + 1_000,
                        () -> CompletableFuture.failedFuture(stale)).join())
                .satisfies(error -> assertThat(unwrap(error)).isSameAs(stale));
        assertThat(store.readerLease(CLUSTER, object.objectKeyHash(), PROCESS)).isEmpty();
        assertThat(store.getRoot(CLUSTER, object.objectKeyHash()).join()).isPresent();
    }

    @Test
    void rootLossAfterLeaseWriteFailsBeforeSelectionAndCleansLease() {
        DisappearingRootStore store = new DisappearingRootStore();
        DefaultObjectReadPinManager manager = manager(store, new MutableClock(NOW));
        PhysicalObjectIdentity object = object();
        AtomicInteger validations = new AtomicInteger();

        assertThatThrownBy(() -> manager.acquire(
                        object,
                        NOW + 1_000,
                        () -> validated(validations)).join())
                .satisfies(error -> assertThat(unwrap(error))
                        .isInstanceOfSatisfying(NereusException.class, nereus ->
                                assertThat(nereus.code()).isEqualTo(ErrorCode.METADATA_CONDITION_FAILED)));
        assertThat(validations.get()).isZero();
        assertThat(store.readerLease(CLUSTER, object.objectKeyHash(), PROCESS)).isEmpty();
    }

    @Test
    void rejectsDeadlineOutsideSkewSafeLeaseWindowAndNewPinsAfterClose() {
        FakePhysicalObjectMetadataStore store = new FakePhysicalObjectMetadataStore();
        DefaultObjectReadPinManager manager = manager(store, new MutableClock(NOW));

        assertThatThrownBy(() -> manager.acquire(
                        object(), NOW + 9_001, () -> CompletableFuture.completedFuture(null)).join())
                .hasCauseInstanceOf(IllegalArgumentException.class);
        manager.close();
        assertThatThrownBy(() -> manager.acquire(
                        object(), NOW + 1_000, () -> CompletableFuture.completedFuture(null)).join())
                .satisfies(error -> assertThat(unwrap(error))
                        .isInstanceOfSatisfying(NereusException.class, nereus ->
                                assertThat(nereus.code()).isEqualTo(ErrorCode.STORAGE_CLOSED)));
    }

    private static DefaultObjectReadPinManager manager(
            FakePhysicalObjectMetadataStore store, Clock clock) {
        return new DefaultObjectReadPinManager(
                CLUSTER,
                PROCESS,
                store,
                Duration.ofSeconds(10),
                Duration.ofSeconds(1),
                Duration.ofMinutes(1),
                clock,
                () -> "l".repeat(26));
    }

    private static CompletableFuture<Void> validated(AtomicInteger count) {
        count.incrementAndGet();
        return CompletableFuture.completedFuture(null);
    }

    private static PhysicalObjectIdentity object() {
        return PhysicalObjectIdentity.create(
                new ObjectKey("objects/lease-target"),
                Optional.empty(),
                PhysicalObjectKind.COMMITTED_COMPACTED,
                100,
                new Checksum(ChecksumType.CRC32C, "01020304"),
                Optional.of(new Checksum(ChecksumType.SHA256, "a".repeat(64))),
                Optional.of("etag"));
    }

    private static Throwable unwrap(Throwable supplied) {
        Throwable failure = supplied;
        while (failure instanceof CompletionException && failure.getCause() != null) {
            failure = failure.getCause();
        }
        return failure;
    }

    private static final class MutableClock extends Clock {
        private long millis;

        private MutableClock(long millis) {
            this.millis = millis;
        }

        private void advance(Duration duration) {
            millis = Math.addExact(millis, duration.toMillis());
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

    private static final class DisappearingRootStore extends FakePhysicalObjectMetadataStore {
        private boolean hideNextRoot;

        @Override
        public synchronized CompletableFuture<VersionedReaderLease> createOrCompareReaderLease(
                String cluster, ObjectReaderLeaseRecord lease) {
            return super.createOrCompareReaderLease(cluster, lease).thenApply(value -> {
                hideNextRoot = true;
                return value;
            });
        }

        @Override
        public synchronized CompletableFuture<Optional<VersionedPhysicalObjectRoot>> getRoot(
                String cluster, com.nereusstream.api.ObjectKeyHash object) {
            if (hideNextRoot) {
                hideNextRoot = false;
                return CompletableFuture.completedFuture(Optional.empty());
            }
            return super.getRoot(cluster, object);
        }
    }
}
