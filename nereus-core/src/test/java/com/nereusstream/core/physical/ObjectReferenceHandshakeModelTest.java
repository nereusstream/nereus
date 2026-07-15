/* Licensed under the Apache License, Version 2.0 */
package com.nereusstream.core.physical;

import static com.nereusstream.core.physical.ObjectProtectionTestSupport.CLUSTER;
import static com.nereusstream.core.physical.ObjectProtectionTestSupport.NOW;
import static com.nereusstream.core.physical.ObjectProtectionTestSupport.manager;
import static com.nereusstream.core.physical.ObjectProtectionTestSupport.object;
import static com.nereusstream.core.physical.ObjectProtectionTestSupport.owner;
import static com.nereusstream.core.physical.ObjectProtectionTestSupport.permanent;
import static com.nereusstream.core.physical.ObjectProtectionTestSupport.unwrap;
import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.assertThatThrownBy;

import com.nereusstream.api.ErrorCode;
import com.nereusstream.api.NereusException;
import com.nereusstream.api.ObjectKeyHash;
import com.nereusstream.metadata.oxia.FakePhysicalObjectMetadataStore;
import com.nereusstream.metadata.oxia.ObjectProtectionIdentity;
import com.nereusstream.metadata.oxia.VersionedObjectProtection;
import com.nereusstream.metadata.oxia.VersionedPhysicalObjectRoot;
import com.nereusstream.metadata.oxia.records.ObjectProtectionRecord;
import java.util.Optional;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.atomic.AtomicInteger;
import org.junit.jupiter.api.Test;

class ObjectReferenceHandshakeModelTest {
    @Test
    void rootLossAfterCreatePreventsAdmissionAndCleansTheKnownNewProtection() {
        PostCreateRootLossStore store = new PostCreateRootLossStore();
        DefaultObjectProtectionManager manager = manager(
                store, new ObjectProtectionTestSupport.MutableClock(NOW));
        ObjectProtectionRequest request = permanent(owner("b", 20));
        AtomicInteger ownerChecks = new AtomicInteger();

        assertThatThrownBy(() -> manager.acquire(request, ignored -> {
                    ownerChecks.incrementAndGet();
                    return CompletableFuture.completedFuture(null);
                }).join())
                .satisfies(error -> assertThat(unwrap(error))
                        .isInstanceOfSatisfying(NereusException.class, nereus ->
                                assertThat(nereus.code()).isEqualTo(
                                        ErrorCode.METADATA_CONDITION_FAILED)));

        assertThat(ownerChecks.get()).isZero();
        assertThat(store.protection(CLUSTER, request.identity())).isEmpty();
    }

    @Test
    void lostCreateResponseNeverClaimsExclusiveCleanupOwnership() {
        LostCreateResponseStore store = new LostCreateResponseStore();
        DefaultObjectProtectionManager manager = manager(
                store, new ObjectProtectionTestSupport.MutableClock(NOW));
        ObjectProtectionRequest request = permanent(owner("c", 21));
        NereusException stale = new NereusException(
                ErrorCode.METADATA_CONDITION_FAILED,
                true,
                "owner post-check failed");

        assertThatThrownBy(() -> manager.acquire(
                        request,
                        ignored -> CompletableFuture.failedFuture(stale)).join())
                .satisfies(error -> assertThat(unwrap(error)).isSameAs(stale));

        assertThat(store.protection(CLUSTER, request.identity())).isPresent();

        ObjectProtection recovered = manager.acquire(
                request, ignored -> CompletableFuture.completedFuture(null)).join();
        manager.release(
                recovered, ignored -> CompletableFuture.completedFuture(null)).join();
    }

    @Test
    void lostDeleteResponseIsSuccessOnlyAfterExactAbsenceIsObserved() {
        LostDeleteResponseStore store = new LostDeleteResponseStore();
        DefaultObjectProtectionManager manager = manager(
                store, new ObjectProtectionTestSupport.MutableClock(NOW));
        ObjectProtection protection = manager.acquire(
                permanent(owner("d", 22)),
                ignored -> CompletableFuture.completedFuture(null)).join();

        manager.release(
                protection, ignored -> CompletableFuture.completedFuture(null)).join();

        assertThat(store.protection(CLUSTER, protection.identity())).isEmpty();
        assertThat(store.deleteAttempts.get()).isOne();
    }

    @Test
    void rootLossAfterTransferLeavesTheNewSameKeyProtectionAsVeto() {
        PostTransferRootLossStore store = new PostTransferRootLossStore();
        DefaultObjectProtectionManager manager = manager(
                store, new ObjectProtectionTestSupport.MutableClock(NOW));
        ObjectProtection original = manager.acquire(
                permanent(owner("e", 23)),
                ignored -> CompletableFuture.completedFuture(null)).join();
        ObjectProtectionOwner newOwner = owner("f", 24);

        assertThatThrownBy(() -> manager.transfer(
                        original,
                        newOwner,
                        ignored -> CompletableFuture.completedFuture(null)).join())
                .satisfies(error -> assertThat(unwrap(error))
                        .isInstanceOfSatisfying(NereusException.class, nereus ->
                                assertThat(nereus.code()).isEqualTo(
                                        ErrorCode.METADATA_CONDITION_FAILED)));

        assertThat(store.protection(CLUSTER, original.identity()))
                .get()
                .extracting(value -> value.value().ownerKey())
                .isEqualTo(newOwner.ownerKey());

        ObjectProtection recovered = manager.transfer(
                original,
                newOwner,
                ignored -> CompletableFuture.completedFuture(null)).join();
        manager.release(
                recovered, ignored -> CompletableFuture.completedFuture(null)).join();
    }

    private static final class PostCreateRootLossStore
            extends FakePhysicalObjectMetadataStore {
        private boolean hideNextRoot;

        @Override
        public synchronized CompletableFuture<VersionedObjectProtection> createProtection(
                String cluster,
                ObjectProtectionRecord protection) {
            return super.createProtection(cluster, protection).thenApply(value -> {
                hideNextRoot = true;
                return value;
            });
        }

        @Override
        public synchronized CompletableFuture<Optional<VersionedPhysicalObjectRoot>> getRoot(
                String cluster,
                ObjectKeyHash object) {
            if (hideNextRoot) {
                hideNextRoot = false;
                return CompletableFuture.completedFuture(Optional.empty());
            }
            return super.getRoot(cluster, object);
        }
    }

    private static final class LostCreateResponseStore
            extends FakePhysicalObjectMetadataStore {
        private boolean lose = true;

        @Override
        public synchronized CompletableFuture<VersionedObjectProtection> createProtection(
                String cluster,
                ObjectProtectionRecord protection) {
            CompletableFuture<VersionedObjectProtection> write =
                    super.createProtection(cluster, protection);
            if (lose) {
                lose = false;
                return write.thenCompose(ignored -> CompletableFuture.failedFuture(
                        new RuntimeException("lost create response")));
            }
            return write;
        }
    }

    private static final class LostDeleteResponseStore
            extends FakePhysicalObjectMetadataStore {
        private final AtomicInteger deleteAttempts = new AtomicInteger();

        @Override
        public synchronized CompletableFuture<Void> deleteProtection(
                String cluster,
                ObjectProtectionIdentity protection,
                long expectedVersion) {
            deleteAttempts.incrementAndGet();
            return super.deleteProtection(cluster, protection, expectedVersion)
                    .thenCompose(ignored -> CompletableFuture.failedFuture(
                            new RuntimeException("lost delete response")));
        }
    }

    private static final class PostTransferRootLossStore
            extends FakePhysicalObjectMetadataStore {
        private boolean hideNextRoot;

        @Override
        public synchronized CompletableFuture<VersionedObjectProtection> compareAndSetProtection(
                String cluster,
                ObjectProtectionRecord protection,
                long expectedVersion) {
            return super.compareAndSetProtection(cluster, protection, expectedVersion)
                    .thenApply(value -> {
                        hideNextRoot = true;
                        return value;
                    });
        }

        @Override
        public synchronized CompletableFuture<Optional<VersionedPhysicalObjectRoot>> getRoot(
                String cluster,
                ObjectKeyHash object) {
            if (hideNextRoot) {
                hideNextRoot = false;
                return CompletableFuture.completedFuture(Optional.empty());
            }
            return super.getRoot(cluster, object);
        }
    }
}
