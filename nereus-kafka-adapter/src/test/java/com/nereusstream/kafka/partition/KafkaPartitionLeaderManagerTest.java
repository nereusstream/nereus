/* Licensed under the Apache License, Version 2.0 */
package com.nereusstream.kafka.partition;

import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.assertThatThrownBy;

import com.nereusstream.api.AppendOutcome;
import com.nereusstream.api.ErrorCode;
import com.nereusstream.api.NereusException;
import com.nereusstream.api.StorageProfile;
import java.nio.ByteBuffer;
import java.util.HashMap;
import java.util.Map;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.CompletionException;
import java.util.concurrent.atomic.AtomicBoolean;
import java.util.concurrent.atomic.AtomicInteger;
import org.junit.jupiter.api.Test;

class KafkaPartitionLeaderManagerTest {
    @Test
    void exactConcurrentOpenIsDeduplicatedAndInstalledOnce() {
        ControlledOpener opener = new ControlledOpener();
        KafkaPartitionLeaderManager manager = new KafkaPartitionLeaderManager(opener);
        KafkaLeaderAuthority authority = authority(1, 5, 9);
        CompletableFuture<KafkaPartitionStorage> first = manager.open(plan(authority));
        CompletableFuture<KafkaPartitionStorage> duplicate = manager.open(plan(authority));

        assertThat(duplicate).isSameAs(first);
        assertThat(first.cancel(false)).isFalse();
        assertThat(first.complete(new FakeStorage(authority))).isFalse();
        assertThat(opener.openCalls()).isEqualTo(1);
        FakeStorage storage = new FakeStorage(authority);
        opener.complete(authority, storage);

        assertThat(first.join()).isSameAs(storage);
        assertThat(manager.current(authority.identity())).contains(storage);
        assertThat(manager.installedPartitions()).isEqualTo(1);
        assertThat(manager.open(plan(authority)).join()).isSameAs(storage);
        assertThat(opener.openCalls()).isEqualTo(1);
    }

    @Test
    void higherLeaderTermPreventsALateOldOpenFromEverInstalling() {
        ControlledOpener opener = new ControlledOpener();
        KafkaPartitionLeaderManager manager = new KafkaPartitionLeaderManager(opener);
        KafkaLeaderAuthority oldAuthority = authority(1, 5, 9);
        KafkaLeaderAuthority newAuthority = authority(1, 6, 1);

        CompletableFuture<KafkaPartitionStorage> oldOpen = manager.open(plan(oldAuthority));
        CompletableFuture<KafkaPartitionStorage> newOpen = manager.open(plan(newAuthority));
        FakeStorage oldStorage = new FakeStorage(oldAuthority);
        opener.complete(oldAuthority, oldStorage);

        assertFailureCode(oldOpen, ErrorCode.FENCED_APPEND);
        assertThat(oldStorage.state()).isEqualTo(KafkaPartitionState.CLOSED);
        assertThat(manager.current(oldAuthority.identity())).isEmpty();

        FakeStorage newStorage = new FakeStorage(newAuthority);
        opener.complete(newAuthority, newStorage);
        assertThat(newOpen.join()).isSameAs(newStorage);
        assertThat(manager.current(newAuthority.identity())).contains(newStorage);
    }

    @Test
    void brokerRestartDominatesOnlyForTheSameLeaderAndStaleResignCannotCloseIt() {
        ControlledOpener opener = new ControlledOpener();
        KafkaPartitionLeaderManager manager = new KafkaPartitionLeaderManager(opener);
        KafkaLeaderAuthority first = authority(2, 7, 10);
        FakeStorage firstStorage = new FakeStorage(first);
        CompletableFuture<KafkaPartitionStorage> firstOpen = manager.open(plan(first));
        opener.complete(first, firstStorage);
        firstOpen.join();

        KafkaLeaderAuthority restarted = authority(2, 7, 11);
        CompletableFuture<KafkaPartitionStorage> restartedOpen = manager.open(plan(restarted));
        assertThat(firstStorage.state()).isEqualTo(KafkaPartitionState.CLOSED);
        FakeStorage restartedStorage = new FakeStorage(restarted);
        opener.complete(restarted, restartedStorage);
        restartedOpen.join();

        KafkaLeaderAuthority conflictingOwner = authority(3, 7, 99);
        assertFailureCode(manager.open(plan(conflictingOwner)), ErrorCode.FENCED_APPEND);
        assertFailureCode(manager.open(plan(authority(2, 6, 100))), ErrorCode.FENCED_APPEND);
        manager.resign(first).join();
        assertThat(manager.current(restarted.identity())).contains(restartedStorage);
        assertThat(restartedStorage.state()).isEqualTo(KafkaPartitionState.LEADER_WRITABLE);

        manager.resign(restarted).join();
        assertThat(restartedStorage.state()).isEqualTo(KafkaPartitionState.CLOSED);
        assertThat(manager.current(restarted.identity())).isEmpty();
    }

    @Test
    void shutdownRejectsNewOpenAndFencesAnInFlightResultWhenItArrives() {
        ControlledOpener opener = new ControlledOpener();
        KafkaPartitionLeaderManager manager = new KafkaPartitionLeaderManager(opener);
        KafkaLeaderAuthority authority = authority(4, 8, 12);
        CompletableFuture<KafkaPartitionStorage> opening = manager.open(plan(authority));

        manager.shutdown().join();
        FakeStorage late = new FakeStorage(authority);
        opener.complete(authority, late);

        assertFailureCode(opening, ErrorCode.FENCED_APPEND);
        assertThat(late.state()).isEqualTo(KafkaPartitionState.CLOSED);
        assertFailureCode(manager.open(plan(authority(4, 9, 13))), ErrorCode.STORAGE_CLOSED);
    }

    @Test
    void rejectsAnOpenerResultWhoseIdentityOrLeaderEpochDoesNotMatch() {
        ControlledOpener opener = new ControlledOpener();
        KafkaPartitionLeaderManager manager = new KafkaPartitionLeaderManager(opener);
        KafkaLeaderAuthority requested = authority(5, 10, 20);
        CompletableFuture<KafkaPartitionStorage> opening = manager.open(plan(requested));
        FakeStorage invalid = new FakeStorage(authority(5, 11, 20));

        opener.complete(requested, invalid);

        assertFailureCode(opening, ErrorCode.METADATA_INVARIANT_VIOLATION);
        assertThat(invalid.state()).isEqualTo(KafkaPartitionState.CLOSED);
        assertThat(manager.current(requested.identity())).isEmpty();
    }

    private static KafkaLeaderAuthority authority(int leaderId, int leaderEpoch, long brokerEpoch) {
        return new KafkaLeaderAuthority(
                KafkaPartitionStorageTestSupport.identity(), leaderId, leaderEpoch, brokerEpoch);
    }

    private static KafkaPartitionOpenPlan plan(KafkaLeaderAuthority authority) {
        return KafkaPartitionStorageTestSupport.openPlan(authority);
    }

    private static void assertFailureCode(
            CompletableFuture<?> completion, ErrorCode expected) {
        assertThatThrownBy(completion::join)
                .isInstanceOf(CompletionException.class)
                .satisfies(failure -> {
                    assertThat(failure.getCause()).isInstanceOf(NereusException.class);
                    assertThat(((NereusException) failure.getCause()).code()).isEqualTo(expected);
                });
    }

    private static final class ControlledOpener implements KafkaPartitionOpener {
        private final Map<KafkaLeaderAuthority, CompletableFuture<KafkaPartitionStorage>> attempts =
                new HashMap<>();
        private final AtomicInteger calls = new AtomicInteger();

        @Override
        public synchronized CompletableFuture<KafkaPartitionStorage> open(
                KafkaPartitionOpenPlan plan) {
            calls.incrementAndGet();
            CompletableFuture<KafkaPartitionStorage> result = new CompletableFuture<>();
            if (attempts.putIfAbsent(plan.authority(), result) != null) {
                throw new AssertionError("manager invoked the opener twice for one exact authority");
            }
            return result;
        }

        private synchronized void complete(
                KafkaLeaderAuthority authority, KafkaPartitionStorage storage) {
            attempts.get(authority).complete(storage);
        }

        private int openCalls() {
            return calls.get();
        }
    }

    private static final class FakeStorage implements KafkaPartitionStorage {
        private final KafkaLeaderAuthority authority;
        private final AtomicBoolean closed = new AtomicBoolean();

        private FakeStorage(KafkaLeaderAuthority authority) {
            this.authority = authority;
        }

        @Override
        public KafkaPartitionIdentity identity() {
            return authority.identity();
        }

        @Override
        public int leaderEpoch() {
            return authority.leaderEpoch();
        }

        @Override
        public StorageProfile storageProfile() {
            return StorageProfile.BOOKKEEPER_WAL_ASYNC_OBJECT;
        }

        @Override
        public KafkaPartitionState state() {
            return closed.get() ? KafkaPartitionState.CLOSED : KafkaPartitionState.LEADER_WRITABLE;
        }

        @Override
        public KafkaStableSnapshot stableSnapshot() {
            return KafkaStableSnapshot.nonTransactional(0, 0, 1);
        }

        @Override
        public CompletableFuture<KafkaStableAppendResult> append(
                ByteBuffer validatedRecords, KafkaAppendContext context) {
            return CompletableFuture.failedFuture(new NereusException(
                    ErrorCode.STORAGE_CLOSED,
                    false,
                    "leader manager test storage does not append",
                    AppendOutcome.KNOWN_NOT_COMMITTED));
        }

        @Override
        public CompletableFuture<KafkaStorageReadResult> read(KafkaStorageReadRequest request) {
            return CompletableFuture.failedFuture(new NereusException(
                    ErrorCode.STORAGE_CLOSED, false, "leader manager test storage does not read"));
        }

        @Override
        public KafkaPartitionEventSubscription subscribe(KafkaPartitionEventListener listener) {
            return () -> {};
        }

        @Override
        public CompletableFuture<Void> resign() {
            closed.set(true);
            return CompletableFuture.completedFuture(null);
        }

        @Override
        public void close() {
            closed.set(true);
        }
    }
}
