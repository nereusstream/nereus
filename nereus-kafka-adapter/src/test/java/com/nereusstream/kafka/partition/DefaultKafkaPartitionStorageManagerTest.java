/* Licensed under the Apache License, Version 2.0 */
package com.nereusstream.kafka.partition;

import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.assertThatThrownBy;

import com.nereusstream.api.AppendOutcome;
import com.nereusstream.api.ErrorCode;
import com.nereusstream.api.NereusException;
import com.nereusstream.api.StorageProfile;
import com.nereusstream.kafka.metadata.KafkaBindingRequest;
import com.nereusstream.kafka.metadata.KafkaPartitionBinding;
import com.nereusstream.kafka.metadata.KafkaPartitionBindingLifecycle;
import com.nereusstream.kafka.metadata.KafkaPartitionDeleteRequest;
import com.nereusstream.kafka.metadata.KafkaPartitionLifecycleCoordinator;
import com.nereusstream.kafka.testing.TestStreamStorage;
import com.nereusstream.metadata.oxia.records.KafkaPartitionLifecycle;
import com.nereusstream.metadata.oxia.testing.FakeKafkaPartitionMetadataStore;
import java.nio.ByteBuffer;
import java.time.Clock;
import java.time.Duration;
import java.time.Instant;
import java.time.ZoneOffset;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.CompletionException;
import java.util.concurrent.atomic.AtomicBoolean;
import java.util.concurrent.atomic.AtomicInteger;
import java.util.concurrent.atomic.AtomicReference;
import org.junit.jupiter.api.Test;

class DefaultKafkaPartitionStorageManagerTest {
    private static final Clock CLOCK = Clock.fixed(Instant.ofEpochMilli(10_000), ZoneOffset.UTC);
    private static final StorageProfile PROFILE = StorageProfile.BOOKKEEPER_WAL_ASYNC_OBJECT;

    @Test
    void ensuresExactBindingThenDeduplicatesAuthorityAcquireAndRecovery() {
        FakeLifecycle lifecycle = new FakeLifecycle(KafkaPartitionStorageTestSupport.binding(PROFILE));
        ControlledOpener opener = new ControlledOpener();
        DefaultKafkaPartitionStorageManager manager = manager(lifecycle, opener);
        KafkaPartitionLeaderOpenRequest request = request(5, PROFILE);

        CompletableFuture<KafkaPartitionStorage> first = manager.openLeader(request);
        CompletableFuture<KafkaPartitionStorage> duplicate = manager.openLeader(request);

        assertThat(first).isSameAs(duplicate);
        assertThat(first.cancel(false)).isFalse();
        assertThat(lifecycle.ensureCalls()).isEqualTo(1);
        assertThat(opener.calls()).isEqualTo(1);
        assertThat(opener.plan().binding().streamId())
                .isEqualTo(KafkaPartitionStorageTestSupport.binding(PROFILE).streamId());
        assertThat(opener.plan().profilePolicy())
                .isEqualTo(KafkaStorageProfilePolicy.forProfile(PROFILE));
        FakeStorage storage = new FakeStorage(request.authority(), PROFILE);
        opener.complete(storage);

        assertThat(first.join()).isSameAs(storage);
        assertThat(duplicate.join()).isSameAs(storage);
        assertThat(manager.current(request.identity())).contains(storage);
    }

    @Test
    void composesTheRealDeterministicLifecycleIntoTheOpenPlan() {
        FakeKafkaPartitionMetadataStore metadata = new FakeKafkaPartitionMetadataStore("nereus", "kraft");
        TestStreamStorage streams = new TestStreamStorage();
        KafkaPartitionLifecycleCoordinator lifecycle = new KafkaPartitionLifecycleCoordinator(
                metadata, streams, metadata.keyspace(), CLOCK);
        ControlledOpener opener = new ControlledOpener();
        DefaultKafkaPartitionStorageManager manager = new DefaultKafkaPartitionStorageManager(
                lifecycle, opener, CLOCK, "broker-run", 7, Duration.ofSeconds(30));
        KafkaPartitionLeaderOpenRequest request = request(5, PROFILE);

        CompletableFuture<KafkaPartitionStorage> opening = manager.openLeader(request);

        assertThat(opener.calls()).isEqualTo(1);
        assertThat(opener.plan().binding().durableRoot().value().lifecycle())
                .isEqualTo(KafkaPartitionLifecycle.ACTIVE);
        assertThat(opener.plan().binding().durableRoot().value().storageProfile())
                .isEqualTo(PROFILE.name());
        assertThat(streams.streamCount()).isEqualTo(1);
        FakeStorage storage = new FakeStorage(request.authority(), PROFILE);
        opener.complete(storage);
        assertThat(opening.join()).isSameAs(storage);
    }

    @SuppressWarnings("removal")
    @Test
    void rejectsUnactivatedOrMismatchedProfileBeforeOpeningStorage() {
        FakeLifecycle lifecycle = new FakeLifecycle(KafkaPartitionStorageTestSupport.binding(PROFILE));
        ControlledOpener opener = new ControlledOpener();
        DefaultKafkaPartitionStorageManager manager = manager(lifecycle, opener);

        assertThatThrownBy(() -> manager.openLeader(request(5, StorageProfile.OBJECT_WAL)))
                .isInstanceOf(IllegalArgumentException.class)
                .hasMessageContaining("explicitly activated");
        assertThat(lifecycle.ensureCalls()).isZero();

        assertFailureCode(
                manager.openLeader(request(5, StorageProfile.OBJECT_WAL_ASYNC_OBJECT)),
                ErrorCode.METADATA_INVARIANT_VIOLATION);
        assertThat(opener.calls()).isZero();
    }

    @Test
    void staleResignCannotCloseCurrentTermAndDeleteDrainsBeforeDurableDeletion() {
        FakeLifecycle lifecycle = new FakeLifecycle(KafkaPartitionStorageTestSupport.binding(PROFILE));
        ControlledOpener opener = new ControlledOpener();
        DefaultKafkaPartitionStorageManager manager = manager(lifecycle, opener);
        KafkaPartitionLeaderOpenRequest request = request(5, PROFILE);
        FakeStorage storage = new FakeStorage(request.authority(), PROFILE);
        CompletableFuture<KafkaPartitionStorage> opened = manager.openLeader(request);
        opener.complete(storage);
        opened.join();

        manager.resign(request.identity(), 4, Duration.ofSeconds(5)).join();
        assertThat(storage.closed()).isFalse();
        assertThat(manager.current(request.identity())).contains(storage);

        lifecycle.deleteObserver.set(storage::closed);
        manager.delete(request.identity(), 12, Duration.ofSeconds(5)).join();
        assertThat(storage.closed()).isTrue();
        assertThat(lifecycle.deleteCalls()).isEqualTo(1);
        assertThat(lifecycle.storageWasClosedAtDelete()).isTrue();
        assertThat(manager.current(request.identity())).isEmpty();
    }

    @Test
    void shutdownDuringBindingPreventsLateAuthorityOpen() {
        FakeLifecycle lifecycle = new FakeLifecycle();
        ControlledOpener opener = new ControlledOpener();
        DefaultKafkaPartitionStorageManager manager = manager(lifecycle, opener);
        CompletableFuture<KafkaPartitionStorage> opening = manager.openLeader(request(5, PROFILE));

        manager.shutdown().join();
        lifecycle.completeBinding(KafkaPartitionStorageTestSupport.binding(PROFILE));

        assertFailureCode(opening, ErrorCode.STORAGE_CLOSED);
        assertThat(opener.calls()).isZero();
        assertFailureCode(manager.openLeader(request(6, PROFILE)), ErrorCode.STORAGE_CLOSED);
    }

    @Test
    void deleteDuringBindingPreventsLateAuthorityOpen() {
        FakeLifecycle lifecycle = new FakeLifecycle();
        ControlledOpener opener = new ControlledOpener();
        DefaultKafkaPartitionStorageManager manager = manager(lifecycle, opener);
        KafkaPartitionLeaderOpenRequest request = request(5, PROFILE);
        CompletableFuture<KafkaPartitionStorage> opening = manager.openLeader(request);

        manager.delete(request.identity(), 12, Duration.ofSeconds(5)).join();
        lifecycle.completeBinding(KafkaPartitionStorageTestSupport.binding(PROFILE));

        assertFailureCode(opening, ErrorCode.FENCED_APPEND);
        assertThat(opener.calls()).isZero();
        assertThat(lifecycle.deleteCalls()).isEqualTo(1);
    }

    private static DefaultKafkaPartitionStorageManager manager(
            FakeLifecycle lifecycle, ControlledOpener opener) {
        return new DefaultKafkaPartitionStorageManager(
                lifecycle, opener, CLOCK, "broker-run", 7, Duration.ofSeconds(30));
    }

    private static KafkaPartitionLeaderOpenRequest request(
            int leaderEpoch, StorageProfile profile) {
        return new KafkaPartitionLeaderOpenRequest(
                KafkaPartitionStorageTestSupport.identity(),
                1,
                leaderEpoch,
                9,
                profile,
                11,
                Duration.ofSeconds(5));
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

    private static final class FakeLifecycle implements KafkaPartitionBindingLifecycle {
        private final CompletableFuture<KafkaPartitionBinding> binding = new CompletableFuture<>();
        private final AtomicInteger ensures = new AtomicInteger();
        private final AtomicInteger deletes = new AtomicInteger();
        private final AtomicReference<java.util.function.BooleanSupplier> deleteObserver = new AtomicReference<>();
        private final AtomicBoolean storageClosedAtDelete = new AtomicBoolean();

        private FakeLifecycle() {}

        private FakeLifecycle(KafkaPartitionBinding value) {
            binding.complete(value);
        }

        @Override
        public CompletableFuture<KafkaPartitionBinding> ensureBinding(KafkaBindingRequest request) {
            ensures.incrementAndGet();
            return binding;
        }

        @Override
        public CompletableFuture<Void> delete(KafkaPartitionDeleteRequest request) {
            deletes.incrementAndGet();
            java.util.function.BooleanSupplier observer = deleteObserver.get();
            storageClosedAtDelete.set(observer != null && observer.getAsBoolean());
            return CompletableFuture.completedFuture(null);
        }

        private void completeBinding(KafkaPartitionBinding value) {
            binding.complete(value);
        }

        private int ensureCalls() {
            return ensures.get();
        }

        private int deleteCalls() {
            return deletes.get();
        }

        private boolean storageWasClosedAtDelete() {
            return storageClosedAtDelete.get();
        }
    }

    private static final class ControlledOpener implements KafkaPartitionOpener {
        private final AtomicInteger calls = new AtomicInteger();
        private final AtomicReference<KafkaPartitionOpenPlan> plan = new AtomicReference<>();
        private final CompletableFuture<KafkaPartitionStorage> result = new CompletableFuture<>();

        @Override
        public CompletableFuture<KafkaPartitionStorage> open(KafkaPartitionOpenPlan requested) {
            if (!plan.compareAndSet(null, requested)) {
                throw new AssertionError("opener was invoked more than once");
            }
            calls.incrementAndGet();
            return result;
        }

        private void complete(KafkaPartitionStorage storage) {
            result.complete(storage);
        }

        private int calls() {
            return calls.get();
        }

        private KafkaPartitionOpenPlan plan() {
            return plan.get();
        }
    }

    private static final class FakeStorage implements KafkaPartitionStorage {
        private final KafkaLeaderAuthority authority;
        private final StorageProfile profile;
        private final AtomicBoolean closed = new AtomicBoolean();

        private FakeStorage(KafkaLeaderAuthority authority, StorageProfile profile) {
            this.authority = authority;
            this.profile = profile;
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
            return profile;
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
                    "manager test storage does not append",
                    AppendOutcome.KNOWN_NOT_COMMITTED));
        }

        @Override
        public CompletableFuture<KafkaStorageReadResult> read(KafkaStorageReadRequest request) {
            return CompletableFuture.failedFuture(new NereusException(
                    ErrorCode.STORAGE_CLOSED, false, "manager test storage does not read"));
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

        private boolean closed() {
            return closed.get();
        }
    }
}
