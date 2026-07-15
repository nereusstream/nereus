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
import com.nereusstream.metadata.oxia.FakePhysicalObjectMetadataStore;
import com.nereusstream.metadata.oxia.records.ObjectProtectionType;
import java.time.Duration;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.atomic.AtomicInteger;
import org.junit.jupiter.api.Test;

class ObjectProtectionManagerTest {
    @Test
    void acquiresReusesRevalidatesAndOwnerAuthorizesRelease() {
        FakePhysicalObjectMetadataStore store = new FakePhysicalObjectMetadataStore();
        ObjectProtectionTestSupport.MutableClock clock =
                new ObjectProtectionTestSupport.MutableClock(NOW);
        DefaultObjectProtectionManager manager = manager(
                store, clock);
        ObjectProtectionOwner owner = owner("b", 7);
        ObjectProtectionRequest request = permanent(owner);
        AtomicInteger validations = new AtomicInteger();

        ObjectProtection first = manager.acquire(request, expected -> {
            assertThat(expected).isEqualTo(owner);
            validations.incrementAndGet();
            return CompletableFuture.completedFuture(null);
        }).join();
        ObjectProtection duplicate = manager.acquire(request, expected -> {
            validations.incrementAndGet();
            return CompletableFuture.completedFuture(null);
        }).join();
        ObjectProtection checked = manager.revalidate(first, expected -> {
            validations.incrementAndGet();
            return CompletableFuture.completedFuture(null);
        }).join();

        assertThat(duplicate).isEqualTo(first);
        assertThat(checked).isEqualTo(first);
        assertThat(validations.get()).isEqualTo(3);
        assertThat(store.getRoot(CLUSTER, object().objectKeyHash()).join()).isPresent();
        assertThat(store.protection(CLUSTER, request.identity())).isPresent();

        AtomicInteger authorizations = new AtomicInteger();
        manager.release(first, exact -> {
            assertThat(exact).isEqualTo(first);
            authorizations.incrementAndGet();
            return CompletableFuture.completedFuture(null);
        }).join();
        assertThat(authorizations.get()).isOne();
        assertThat(store.protection(CLUSTER, request.identity())).isEmpty();
    }

    @Test
    void failedOwnerPostCheckRemovesOnlyTheProtectionCreatedByThisCall() {
        FakePhysicalObjectMetadataStore store = new FakePhysicalObjectMetadataStore();
        DefaultObjectProtectionManager manager = manager(
                store, new ObjectProtectionTestSupport.MutableClock(NOW));
        ObjectProtectionRequest request = permanent(owner("c", 8));
        NereusException stale = new NereusException(
                ErrorCode.METADATA_CONDITION_FAILED,
                true,
                "owner changed");

        assertThatThrownBy(() -> manager.acquire(
                        request,
                        ignored -> CompletableFuture.failedFuture(stale)).join())
                .satisfies(error -> assertThat(unwrap(error)).isSameAs(stale));

        assertThat(store.protection(CLUSTER, request.identity())).isEmpty();
        assertThat(store.getRoot(CLUSTER, object().objectKeyHash()).join()).isPresent();
    }

    @Test
    void mismatchedDuplicateRequiresExplicitTransferAndKeepsTheExistingVeto() {
        FakePhysicalObjectMetadataStore store = new FakePhysicalObjectMetadataStore();
        DefaultObjectProtectionManager manager = manager(
                store, new ObjectProtectionTestSupport.MutableClock(NOW));
        ObjectProtection first = manager.acquire(
                permanent(owner("b", 31)),
                ignored -> CompletableFuture.completedFuture(null)).join();
        ObjectProtectionRequest conflicting = permanent(owner("c", 32));

        assertThatThrownBy(() -> manager.acquire(
                        conflicting, ignored -> CompletableFuture.completedFuture(null)).join())
                .satisfies(error -> assertThat(unwrap(error))
                        .isInstanceOfSatisfying(NereusException.class, nereus ->
                                assertThat(nereus.code()).isEqualTo(
                                        ErrorCode.METADATA_CONDITION_FAILED)));
        assertThat(store.protection(CLUSTER, first.identity()))
                .get()
                .extracting(value -> value.value().ownerKey())
                .isEqualTo(first.owner().ownerKey());

        manager.release(first, ignored -> CompletableFuture.completedFuture(null)).join();
    }

    @Test
    void enforcesPendingExpiryBoundsAndKeepsPermanentExpiryClosed() {
        FakePhysicalObjectMetadataStore store = new FakePhysicalObjectMetadataStore();
        ObjectProtectionTestSupport.MutableClock clock =
                new ObjectProtectionTestSupport.MutableClock(NOW);
        DefaultObjectProtectionManager manager = manager(
                store, clock);
        ObjectProtectionOwner owner = owner("d", 9);

        assertThatThrownBy(() -> new ObjectProtectionRequest(
                        object(),
                        ObjectProtectionType.VISIBLE_GENERATION,
                        "bad-permanent",
                        owner,
                        NOW + 1))
                .isInstanceOf(IllegalArgumentException.class);
        assertThatThrownBy(() -> manager.acquire(
                        new ObjectProtectionRequest(
                                object(),
                                ObjectProtectionType.CURSOR_SNAPSHOT_PENDING,
                                "too-near",
                                owner,
                                NOW + 1_000),
                        ignored -> CompletableFuture.completedFuture(null)).join())
                .hasCauseInstanceOf(IllegalArgumentException.class);
        assertThatThrownBy(() -> manager.acquire(
                        new ObjectProtectionRequest(
                                object(),
                                ObjectProtectionType.CURSOR_SNAPSHOT_PENDING,
                                "too-far",
                                owner,
                                NOW + 60_001),
                        ignored -> CompletableFuture.completedFuture(null)).join())
                .hasCauseInstanceOf(IllegalArgumentException.class);

        ObjectProtectionRequest valid = new ObjectProtectionRequest(
                object(),
                ObjectProtectionType.CURSOR_SNAPSHOT_PENDING,
                "bounded",
                owner,
                NOW + 60_000);
        ObjectProtection protection = manager.acquire(
                valid, ignored -> CompletableFuture.completedFuture(null)).join();
        assertThat(protection.isPending()).isTrue();
        clock.advance(Duration.ofSeconds(59));
        assertThatThrownBy(() -> manager.revalidate(
                        protection, ignored -> CompletableFuture.completedFuture(null)).join())
                .hasCauseInstanceOf(IllegalArgumentException.class);
        manager.release(
                protection, ignored -> CompletableFuture.completedFuture(null)).join();
    }

    @Test
    void closeRejectsNewAdmissionButStillAllowsDurableRelease() {
        FakePhysicalObjectMetadataStore store = new FakePhysicalObjectMetadataStore();
        DefaultObjectProtectionManager manager = manager(
                store, new ObjectProtectionTestSupport.MutableClock(NOW));
        ObjectProtectionRequest request = permanent(owner("e", 10));
        ObjectProtection protection = manager.acquire(
                request, ignored -> CompletableFuture.completedFuture(null)).join();

        manager.close();
        assertThatThrownBy(() -> manager.acquire(
                        request, ignored -> CompletableFuture.completedFuture(null)).join())
                .satisfies(error -> assertThat(unwrap(error))
                        .isInstanceOfSatisfying(NereusException.class, nereus ->
                                assertThat(nereus.code()).isEqualTo(ErrorCode.STORAGE_CLOSED)));

        manager.release(
                protection, ignored -> CompletableFuture.completedFuture(null)).join();
        assertThat(store.protection(CLUSTER, request.identity())).isEmpty();
    }
}
