/*
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 *   https://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

package com.nereusstream.kafka.compaction;

import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.assertThatThrownBy;
import com.nereusstream.api.ErrorCode;
import com.nereusstream.api.NereusException;
import com.nereusstream.api.StorageProfile;
import com.nereusstream.kafka.partition.KafkaAppendContext;
import com.nereusstream.kafka.partition.KafkaPartitionEventListener;
import com.nereusstream.kafka.partition.KafkaPartitionEventSubscription;
import com.nereusstream.kafka.partition.KafkaPartitionIdentity;
import com.nereusstream.kafka.partition.KafkaPartitionLeaderOpenRequest;
import com.nereusstream.kafka.partition.KafkaPartitionState;
import com.nereusstream.kafka.partition.KafkaPartitionStorage;
import com.nereusstream.kafka.partition.KafkaPartitionStorageManager;
import com.nereusstream.kafka.partition.KafkaStableAppendResult;
import com.nereusstream.kafka.partition.KafkaStableSnapshot;
import com.nereusstream.kafka.partition.KafkaStorageReadRequest;
import com.nereusstream.kafka.partition.KafkaStorageReadResult;
import java.nio.ByteBuffer;
import java.time.Duration;
import java.util.ArrayList;
import java.util.Base64;
import java.util.Collections;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.Optional;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.CompletionException;
import java.util.concurrent.ScheduledThreadPoolExecutor;
import java.util.concurrent.atomic.AtomicInteger;
import org.junit.jupiter.api.Test;

class KafkaCompactionRuntimeTest {
    @Test
    void ordersInternalFirstBoundsConcurrencySkipsStaleAuthorityAndDrainsAcceptedPass() {
        ScheduledThreadPoolExecutor timer = new ScheduledThreadPoolExecutor(1);
        try {
            KafkaPartitionIdentity internal = identity("a-internal", "__consumer_offsets");
            KafkaPartitionIdentity userOne = identity("b-user", "orders");
            KafkaPartitionIdentity stale = identity("c-stale", "old-leader");
            KafkaPartitionIdentity userTwo = identity("d-user", "payments");
            FakeManager manager = new FakeManager(Map.of(
                    internal, new FakeStorage(internal, 7),
                    userOne, new FakeStorage(userOne, 4),
                    stale, new FakeStorage(stale, 12),
                    userTwo, new FakeStorage(userTwo, 9)));
            List<KafkaCompactionRuntime.OwnedPartition> registrations = List.of(
                    owned(userTwo, 9, KafkaCompactionRuntime.WorkClass.USER),
                    owned(stale, 11, KafkaCompactionRuntime.WorkClass.USER),
                    owned(userOne, 4, KafkaCompactionRuntime.WorkClass.USER),
                    owned(internal, 7, KafkaCompactionRuntime.WorkClass.INTERNAL));
            Map<KafkaPartitionIdentity, CompletableFuture<KafkaCompactionPartitionPass.RunResult>> operations =
                    new HashMap<>();
            for (KafkaPartitionIdentity identity : List.of(internal, userOne, userTwo)) {
                operations.put(identity, new CompletableFuture<>());
            }
            List<KafkaPartitionIdentity> starts = Collections.synchronizedList(new ArrayList<>());
            AtomicInteger active = new AtomicInteger();
            AtomicInteger maximumActive = new AtomicInteger();
            CompletableFuture<Void> firstWave = new CompletableFuture<>();
            CompletableFuture<Void> secondWave = new CompletableFuture<>();

            KafkaCompactionRuntime runtime = new KafkaCompactionRuntime(
                    manager,
                    (triggers, maximum) -> CompletableFuture.completedFuture(registrations),
                    registration -> partition -> {
                        starts.add(registration.identity());
                        int count = active.incrementAndGet();
                        maximumActive.accumulateAndGet(count, Math::max);
                        if (starts.size() == 2) {
                            firstWave.complete(null);
                        } else if (starts.size() == 3) {
                            secondWave.complete(null);
                        }
                        CompletableFuture<KafkaCompactionPartitionPass.RunResult> operation =
                                operations.get(registration.identity());
                        operation.whenComplete((ignored, failure) -> active.decrementAndGet());
                        return operation;
                    },
                    Duration.ofDays(1),
                    2,
                    10,
                    timer,
                    Runnable::run);

            runtime.start().join();
            firstWave.join();
            assertThat(starts).containsExactly(internal, userOne);
            assertThat(maximumActive).hasValue(2);

            operations.get(internal).complete(KafkaCompactionPartitionPass.RunResult.noCandidate());
            secondWave.join();
            assertThat(starts).containsExactly(internal, userOne, userTwo);
            assertThat(starts).doesNotContain(stale);

            CompletableFuture<Void> close = runtime.closeAsync();
            assertThat(close).isNotDone();
            operations.get(userOne).complete(KafkaCompactionPartitionPass.RunResult.noCandidate());
            operations.get(userTwo).complete(KafkaCompactionPartitionPass.RunResult.noCandidate());

            close.orTimeout(5, java.util.concurrent.TimeUnit.SECONDS).join();
            assertThat(close).isCompletedWithValue(null);
            assertThat(active).hasValue(0);
            assertThat(timer.isShutdown()).isFalse();
        } finally {
            timer.shutdownNow();
        }
    }

    @Test
    void attemptsEveryPartitionAndReportsFailuresInStablePartitionOrder() {
        ScheduledThreadPoolExecutor timer = new ScheduledThreadPoolExecutor(1);
        try {
            KafkaPartitionIdentity first = identity("a", "first");
            KafkaPartitionIdentity middle = identity("b", "middle");
            KafkaPartitionIdentity last = identity("c", "last");
            FakeManager manager = new FakeManager(Map.of(
                    first, new FakeStorage(first, 1),
                    middle, new FakeStorage(middle, 1),
                    last, new FakeStorage(last, 1)));
            CompletableFuture<Void> startupSnapshot = new CompletableFuture<>();
            AtomicInteger attempts = new AtomicInteger();
            RuntimeException firstFailure = new RuntimeException("first failed");
            RuntimeException lastFailure = new RuntimeException("last failed");

            KafkaCompactionRuntime runtime = new KafkaCompactionRuntime(
                    manager,
                    (triggers, maximum) -> {
                        if (triggers.reasons().contains(KafkaCompactionScheduler.Trigger.STARTUP)) {
                            startupSnapshot.complete(null);
                            return CompletableFuture.completedFuture(List.of());
                        }
                        return CompletableFuture.completedFuture(List.of(
                                owned(last, 1, KafkaCompactionRuntime.WorkClass.USER),
                                owned(middle, 1, KafkaCompactionRuntime.WorkClass.USER),
                                owned(first, 1, KafkaCompactionRuntime.WorkClass.USER)));
                    },
                    registration -> partition -> {
                        attempts.incrementAndGet();
                        if (registration.identity().equals(first)) {
                            return CompletableFuture.failedFuture(firstFailure);
                        }
                        if (registration.identity().equals(last)) {
                            return CompletableFuture.failedFuture(lastFailure);
                        }
                        return CompletableFuture.completedFuture(KafkaCompactionPartitionPass.RunResult.noCandidate());
                    },
                    Duration.ofDays(1),
                    2,
                    10,
                    timer,
                    Runnable::run);

            runtime.start().join();
            startupSnapshot.join();
            CompletableFuture<Void> pass = runtime.trigger(KafkaCompactionScheduler.Trigger.ADMIN);

            assertThatThrownBy(pass::join)
                    .isInstanceOf(CompletionException.class)
                    .cause()
                    .isInstanceOfSatisfying(NereusException.class, failure -> {
                        assertThat(failure.code()).isEqualTo(ErrorCode.METADATA_UNAVAILABLE);
                        assertThat(failure.retriable()).isTrue();
                        assertThat(failure.getCause()).isSameAs(firstFailure);
                        assertThat(failure.getSuppressed()).containsExactly(lastFailure);
                    });
            assertThat(attempts).hasValue(3);
            runtime.closeAsync().join();
        } finally {
            timer.shutdownNow();
        }
    }

    @Test
    void rejectsDuplicateDurableIdentityBeforeCreatingAnyPartitionPass() {
        ScheduledThreadPoolExecutor timer = new ScheduledThreadPoolExecutor(1);
        try {
            KafkaPartitionIdentity original = identity("a", "orders");
            KafkaPartitionIdentity renamed = new KafkaPartitionIdentity(
                    original.kafkaClusterId(), original.topicId(), original.partition(), "renamed-orders");
            CompletableFuture<Void> startupSnapshot = new CompletableFuture<>();
            AtomicInteger factories = new AtomicInteger();
            KafkaCompactionRuntime runtime = new KafkaCompactionRuntime(
                    new FakeManager(Map.of()),
                    (triggers, maximum) -> {
                        assertThat(maximum).isEqualTo(10);
                        if (triggers.reasons().contains(KafkaCompactionScheduler.Trigger.STARTUP)) {
                            startupSnapshot.complete(null);
                            return CompletableFuture.completedFuture(List.of());
                        }
                        return CompletableFuture.completedFuture(List.of(
                                owned(original, 1, KafkaCompactionRuntime.WorkClass.USER),
                                owned(renamed, 1, KafkaCompactionRuntime.WorkClass.USER)));
                    },
                    registration -> {
                        factories.incrementAndGet();
                        throw new AssertionError("duplicate snapshot reached the pass factory");
                    },
                    Duration.ofDays(1),
                    1,
                    10,
                    timer,
                    Runnable::run);

            runtime.start().join();
            startupSnapshot.join();
            CompletableFuture<Void> pass = runtime.trigger(KafkaCompactionScheduler.Trigger.ADMIN);

            assertThatThrownBy(pass::join)
                    .isInstanceOf(CompletionException.class)
                    .cause()
                    .isInstanceOfSatisfying(NereusException.class, failure -> assertThat(failure.code())
                            .isEqualTo(ErrorCode.METADATA_INVARIANT_VIOLATION));
            assertThat(factories).hasValue(0);
            runtime.closeAsync().join();
        } finally {
            timer.shutdownNow();
        }
    }

    private static KafkaCompactionRuntime.OwnedPartition owned(
            KafkaPartitionIdentity identity, int leaderEpoch, KafkaCompactionRuntime.WorkClass workClass) {
        return new KafkaCompactionRuntime.OwnedPartition(
                identity,
                leaderEpoch,
                workClass,
                partition -> CompletableFuture.failedFuture(new UnsupportedOperationException()));
    }

    private static KafkaPartitionIdentity identity(String topicId, String topicName) {
        ByteBuffer bytes =
                ByteBuffer.allocate(16).putLong(0x1234_5678_9abc_def0L).putLong(topicId.charAt(0));
        return new KafkaPartitionIdentity(
                "cluster", Base64.getUrlEncoder().withoutPadding().encodeToString(bytes.array()), 0, topicName);
    }

    private static final class FakeManager implements KafkaPartitionStorageManager {
        private final Map<KafkaPartitionIdentity, KafkaPartitionStorage> current;

        private FakeManager(Map<KafkaPartitionIdentity, KafkaPartitionStorage> current) {
            this.current = Map.copyOf(current);
        }

        @Override
        public CompletableFuture<KafkaPartitionStorage> openLeader(KafkaPartitionLeaderOpenRequest request) {
            return CompletableFuture.failedFuture(new UnsupportedOperationException());
        }

        @Override
        public CompletableFuture<Void> resign(
                KafkaPartitionIdentity identity, int observedLeaderEpoch, Duration timeout) {
            return CompletableFuture.failedFuture(new UnsupportedOperationException());
        }

        @Override
        public CompletableFuture<Void> delete(KafkaPartitionIdentity identity, long metadataOffset, Duration timeout) {
            return CompletableFuture.failedFuture(new UnsupportedOperationException());
        }

        @Override
        public Optional<KafkaPartitionStorage> current(KafkaPartitionIdentity identity) {
            return Optional.ofNullable(current.get(identity));
        }

        @Override
        public CompletableFuture<Void> shutdown() {
            return CompletableFuture.completedFuture(null);
        }

        @Override
        public void close() {}
    }

    private record FakeStorage(KafkaPartitionIdentity identity, int leaderEpoch) implements KafkaPartitionStorage {
        @Override
        public StorageProfile storageProfile() {
            return StorageProfile.OBJECT_WAL_SYNC_OBJECT;
        }

        @Override
        public KafkaPartitionState state() {
            return KafkaPartitionState.LEADER_WRITABLE;
        }

        @Override
        public KafkaStableSnapshot stableSnapshot() {
            throw new UnsupportedOperationException();
        }

        @Override
        public CompletableFuture<KafkaStableAppendResult> append(
                ByteBuffer validatedRecords, KafkaAppendContext context) {
            return CompletableFuture.failedFuture(new UnsupportedOperationException());
        }

        @Override
        public CompletableFuture<KafkaStorageReadResult> read(KafkaStorageReadRequest request) {
            return CompletableFuture.failedFuture(new UnsupportedOperationException());
        }

        @Override
        public KafkaPartitionEventSubscription subscribe(KafkaPartitionEventListener listener) {
            throw new UnsupportedOperationException();
        }

        @Override
        public CompletableFuture<Void> resign() {
            return CompletableFuture.completedFuture(null);
        }

        @Override
        public void close() {}
    }
}
