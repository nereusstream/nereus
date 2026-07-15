/* Licensed under the Apache License, Version 2.0 */
package com.nereusstream.core.physical;

import static com.nereusstream.core.physical.ObjectProtectionTestSupport.CLUSTER;
import static com.nereusstream.core.physical.ObjectProtectionTestSupport.NOW;
import static com.nereusstream.core.physical.ObjectProtectionTestSupport.manager;
import static com.nereusstream.core.physical.ObjectProtectionTestSupport.owner;
import static com.nereusstream.core.physical.ObjectProtectionTestSupport.permanent;
import static com.nereusstream.core.physical.ObjectProtectionTestSupport.unwrap;
import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.assertThatThrownBy;

import com.nereusstream.api.Checksum;
import com.nereusstream.api.ChecksumType;
import com.nereusstream.api.ErrorCode;
import com.nereusstream.api.NereusException;
import com.nereusstream.metadata.oxia.FakePhysicalObjectMetadataStore;
import com.nereusstream.metadata.oxia.VersionedObjectProtection;
import com.nereusstream.metadata.oxia.records.ObjectProtectionRecord;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.atomic.AtomicInteger;
import org.junit.jupiter.api.Test;

class ObjectProtectionOwnerTransferTest {
    @Test
    void acquireOrTransferConvergesOnlyAForwardVersionOfTheSameLogicalOwner() {
        FakePhysicalObjectMetadataStore store = new FakePhysicalObjectMetadataStore();
        DefaultObjectProtectionManager manager = manager(
                store, new ObjectProtectionTestSupport.MutableClock(NOW));
        ObjectProtectionOwner oldOwner = owner("a", 11);
        ObjectProtectionOwner newOwner = new ObjectProtectionOwner(
                oldOwner.ownerKey(),
                12,
                new Checksum(ChecksumType.SHA256, "f".repeat(64)));
        ObjectProtection original = manager.acquire(
                permanent(oldOwner), ignored -> CompletableFuture.completedFuture(null)).join();
        AtomicInteger validations = new AtomicInteger();

        ObjectProtection reconciled = manager.acquireOrTransfer(
                permanent(newOwner), expected -> {
                    assertThat(expected).isEqualTo(newOwner);
                    validations.incrementAndGet();
                    return CompletableFuture.completedFuture(null);
                }).join();

        assertThat(reconciled.identity()).isEqualTo(original.identity());
        assertThat(reconciled.owner()).isEqualTo(newOwner);
        assertThat(reconciled.metadataVersion()).isGreaterThan(original.metadataVersion());
        assertThat(validations).hasValue(2);

        assertThatThrownBy(() -> manager.acquireOrTransfer(
                        permanent(oldOwner), ignored -> CompletableFuture.completedFuture(null)).join())
                .satisfies(error -> assertThat(unwrap(error))
                        .isInstanceOfSatisfying(NereusException.class, nereus ->
                                assertThat(nereus.code()).isEqualTo(
                                        ErrorCode.METADATA_CONDITION_FAILED)));
        assertThatThrownBy(() -> manager.acquireOrTransfer(
                        permanent(owner("b", 13)),
                        ignored -> CompletableFuture.completedFuture(null)).join())
                .satisfies(error -> assertThat(unwrap(error))
                        .isInstanceOfSatisfying(NereusException.class, nereus ->
                                assertThat(nereus.code()).isEqualTo(
                                        ErrorCode.METADATA_CONDITION_FAILED)));
        assertThat(store.protection(CLUSTER, original.identity()))
                .get()
                .extracting(value -> value.value().ownerIdentitySha256())
                .isEqualTo(newOwner.identitySha256().value());
    }

    @Test
    void acquireOrTransferRecoversAnExactLostCasResponse() {
        LostTransferResponseStore store = new LostTransferResponseStore();
        DefaultObjectProtectionManager manager = manager(
                store, new ObjectProtectionTestSupport.MutableClock(NOW));
        ObjectProtectionOwner oldOwner = owner("d", 21);
        ObjectProtectionOwner newOwner = new ObjectProtectionOwner(
                oldOwner.ownerKey(),
                22,
                new Checksum(ChecksumType.SHA256, "e".repeat(64)));
        ObjectProtection original = manager.acquire(
                permanent(oldOwner), ignored -> CompletableFuture.completedFuture(null)).join();
        store.loseNextTransferResponse = true;

        ObjectProtection recovered = manager.acquireOrTransfer(
                permanent(newOwner), ignored -> CompletableFuture.completedFuture(null)).join();

        assertThat(recovered.identity()).isEqualTo(original.identity());
        assertThat(recovered.owner()).isEqualTo(newOwner);
        assertThat(store.transferAttempts).hasValue(1);
        assertThat(manager.acquireOrTransfer(
                permanent(newOwner), ignored -> CompletableFuture.completedFuture(null)).join())
                .isEqualTo(recovered);
    }

    @Test
    void transfersWithOneSameKeyCasAndNeverLetsAStaleOwnerDeleteIt() {
        FakePhysicalObjectMetadataStore store = new FakePhysicalObjectMetadataStore();
        DefaultObjectProtectionManager manager = manager(
                store, new ObjectProtectionTestSupport.MutableClock(NOW));
        ObjectProtectionOwner oldOwner = owner("b", 11);
        ObjectProtectionOwner newOwner = owner("c", 12);
        ObjectProtection original = manager.acquire(
                permanent(oldOwner), ignored -> CompletableFuture.completedFuture(null)).join();
        AtomicInteger newOwnerValidations = new AtomicInteger();

        ObjectProtection transferred = manager.transfer(original, newOwner, expected -> {
            assertThat(expected).isEqualTo(newOwner);
            newOwnerValidations.incrementAndGet();
            return CompletableFuture.completedFuture(null);
        }).join();

        assertThat(transferred.identity()).isEqualTo(original.identity());
        assertThat(transferred.metadataVersion()).isGreaterThan(original.metadataVersion());
        assertThat(transferred.owner()).isEqualTo(newOwner);
        assertThat(newOwnerValidations.get()).isEqualTo(2);
        assertThat(store.protection(CLUSTER, original.identity()))
                .get()
                .extracting(value -> value.value().ownerKey())
                .isEqualTo(newOwner.ownerKey());

        assertThatThrownBy(() -> manager.release(
                        original, ignored -> CompletableFuture.completedFuture(null)).join())
                .satisfies(error -> assertThat(unwrap(error))
                        .isInstanceOfSatisfying(NereusException.class, nereus ->
                                assertThat(nereus.code()).isEqualTo(
                                        ErrorCode.METADATA_CONDITION_FAILED)));
        assertThat(store.protection(CLUSTER, original.identity())).isPresent();

        manager.release(
                transferred, ignored -> CompletableFuture.completedFuture(null)).join();
    }

    @Test
    void failedPostTransferOwnerCheckLeavesTheNewOwnerAsDeletionVeto() {
        FakePhysicalObjectMetadataStore store = new FakePhysicalObjectMetadataStore();
        DefaultObjectProtectionManager manager = manager(
                store, new ObjectProtectionTestSupport.MutableClock(NOW));
        ObjectProtection original = manager.acquire(
                permanent(owner("d", 13)),
                ignored -> CompletableFuture.completedFuture(null)).join();
        ObjectProtectionOwner newOwner = owner("e", 14);
        AtomicInteger validations = new AtomicInteger();
        NereusException stale = new NereusException(
                ErrorCode.METADATA_CONDITION_FAILED,
                true,
                "new owner changed after transfer");

        assertThatThrownBy(() -> manager.transfer(original, newOwner, ignored ->
                        validations.incrementAndGet() == 1
                                ? CompletableFuture.completedFuture(null)
                                : CompletableFuture.failedFuture(stale)).join())
                .satisfies(error -> assertThat(unwrap(error)).isSameAs(stale));

        assertThat(store.protection(CLUSTER, original.identity()))
                .get()
                .extracting(value -> value.value().ownerKey())
                .isEqualTo(newOwner.ownerKey());

        ObjectProtection reconciled = manager.transfer(
                original,
                newOwner,
                ignored -> CompletableFuture.completedFuture(null)).join();
        manager.release(
                reconciled, ignored -> CompletableFuture.completedFuture(null)).join();
    }

    private static final class LostTransferResponseStore
            extends FakePhysicalObjectMetadataStore {
        private final AtomicInteger transferAttempts = new AtomicInteger();
        private boolean loseNextTransferResponse;

        @Override
        public synchronized CompletableFuture<VersionedObjectProtection> compareAndSetProtection(
                String cluster,
                ObjectProtectionRecord protection,
                long expectedVersion) {
            transferAttempts.incrementAndGet();
            CompletableFuture<VersionedObjectProtection> write =
                    super.compareAndSetProtection(cluster, protection, expectedVersion);
            if (!loseNextTransferResponse) {
                return write;
            }
            loseNextTransferResponse = false;
            return write.thenCompose(ignored -> CompletableFuture.failedFuture(
                    new RuntimeException("lost transfer response")));
        }
    }
}
