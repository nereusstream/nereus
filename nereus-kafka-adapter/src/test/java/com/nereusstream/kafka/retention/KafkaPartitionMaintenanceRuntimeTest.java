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

package com.nereusstream.kafka.retention;

import static org.assertj.core.api.Assertions.assertThat;
import com.nereusstream.api.StorageProfile;
import com.nereusstream.kafka.checkpoint.KafkaCheckpointSourceState;
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
import com.nereusstream.metadata.oxia.VersionedKafkaPartitionBinding;
import java.nio.ByteBuffer;
import java.time.Duration;
import java.util.ArrayList;
import java.util.Base64;
import java.util.Collections;
import java.util.List;
import java.util.Map;
import java.util.Optional;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.ScheduledThreadPoolExecutor;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicInteger;
import org.junit.jupiter.api.Test;

class KafkaPartitionMaintenanceRuntimeTest {

    @Test
    void boundsConcurrencySkipsStaleAuthorityAndDrainsAcceptedRetention() {
        ScheduledThreadPoolExecutor timer = new ScheduledThreadPoolExecutor(1);
        try {
            KafkaPartitionIdentity first = identity("a", "orders");
            KafkaPartitionIdentity stale = identity("b", "stale");
            KafkaPartitionIdentity second = identity("c", "payments");
            KafkaPartitionIdentity third = identity("d", "audit");
            List<KafkaPartitionIdentity> starts = Collections.synchronizedList(new ArrayList<>());
            AtomicInteger active = new AtomicInteger();
            AtomicInteger maximumActive = new AtomicInteger();
            CompletableFuture<Void> firstWave = new CompletableFuture<>();
            CompletableFuture<Void> secondWave = new CompletableFuture<>();
            Map<KafkaPartitionIdentity, CompletableFuture<KafkaRetentionCoordinator.RunResult>> operations = Map.of(
                    first, new CompletableFuture<>(),
                    second, new CompletableFuture<>(),
                    third, new CompletableFuture<>());
            FakeManager manager = new FakeManager(Map.of(
                    first,
                    storage(first, 4, starts, active, maximumActive, firstWave, secondWave, operations),
                    stale,
                    storage(stale, 6, starts, active, maximumActive, firstWave, secondWave, operations),
                    second,
                    storage(second, 5, starts, active, maximumActive, firstWave, secondWave, operations),
                    third,
                    storage(third, 7, starts, active, maximumActive, firstWave, secondWave, operations)));
            KafkaPartitionMaintenanceRuntime runtime = new KafkaPartitionMaintenanceRuntime(
                    manager,
                    maximum -> CompletableFuture.completedFuture(
                            List.of(owned(third, 7), owned(stale, 5), owned(second, 5), owned(first, 4))),
                    Duration.ofDays(1),
                    2,
                    10,
                    timer,
                    Runnable::run);

            runtime.start().join();
            firstWave.join();
            assertThat(starts).containsExactly(first, second);
            assertThat(maximumActive).hasValue(2);

            operations.get(first).complete(null);
            secondWave.join();
            assertThat(starts).containsExactly(first, second, third);
            assertThat(starts).doesNotContain(stale);

            CompletableFuture<Void> close = runtime.closeAsync();
            assertThat(close).isNotDone();
            operations.get(second).complete(null);
            operations.get(third).complete(null);

            close.orTimeout(5, TimeUnit.SECONDS).join();
            assertThat(close).isCompletedWithValue(null);
            assertThat(active).hasValue(0);
            assertThat(timer.isShutdown()).isFalse();
        } finally {
            timer.shutdownNow();
        }
    }

    private static FakeStorage storage(
            KafkaPartitionIdentity identity,
            int leaderEpoch,
            List<KafkaPartitionIdentity> starts,
            AtomicInteger active,
            AtomicInteger maximumActive,
            CompletableFuture<Void> firstWave,
            CompletableFuture<Void> secondWave,
            Map<KafkaPartitionIdentity, CompletableFuture<KafkaRetentionCoordinator.RunResult>> operations) {
        KafkaPartitionMaintenance maintenance = new KafkaPartitionMaintenance() {
            @Override
            public CompletableFuture<KafkaRetentionCoordinator.RunResult> runRetention(Hooks hooks) {
                starts.add(identity);
                maximumActive.accumulateAndGet(active.incrementAndGet(), Math::max);
                if (starts.size() == 2) {
                    firstWave.complete(null);
                } else if (starts.size() == 3) {
                    secondWave.complete(null);
                }
                CompletableFuture<KafkaRetentionCoordinator.RunResult> operation = operations.get(identity);
                operation.whenComplete((ignored, failure) -> active.decrementAndGet());
                return operation;
            }

            @Override
            public CompletableFuture<KafkaDeleteRecordsCoordinator.Result> deleteRecords(
                    Hooks hooks, long normalizedRequestedOffset) {
                return CompletableFuture.failedFuture(new UnsupportedOperationException());
            }
        };
        return new FakeStorage(identity, leaderEpoch, maintenance);
    }

    private static KafkaPartitionMaintenanceRuntime.OwnedPartition owned(
            KafkaPartitionIdentity identity, int leaderEpoch) {
        return new KafkaPartitionMaintenanceRuntime.OwnedPartition(identity, leaderEpoch, new UnsupportedHooks());
    }

    private static KafkaPartitionIdentity identity(String topicId, String topicName) {
        ByteBuffer bytes =
                ByteBuffer.allocate(16).putLong(0x1234_5678_9abc_def0L).putLong(topicId.charAt(0));
        return new KafkaPartitionIdentity(
                "cluster", Base64.getUrlEncoder().withoutPadding().encodeToString(bytes.array()), 0, topicName);
    }

    private static final class UnsupportedHooks implements KafkaPartitionMaintenance.Hooks {
        @Override
        public CompletableFuture<KafkaPartitionMaintenance.Capture> capture(KafkaCheckpointSourceState currentSource) {
            return CompletableFuture.failedFuture(new UnsupportedOperationException());
        }

        @Override
        public CompletableFuture<Void> advanceLogStart(
                KafkaTrimBarrier.Snapshot revalidated,
                long durableTrimOffset,
                VersionedKafkaPartitionBinding publishedBinding) {
            return CompletableFuture.failedFuture(new UnsupportedOperationException());
        }
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

    private record FakeStorage(
            KafkaPartitionIdentity identity, int leaderEpoch, KafkaPartitionMaintenance partitionMaintenance)
            implements KafkaPartitionStorage {

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
        public Optional<KafkaPartitionMaintenance> maintenance() {
            return Optional.of(partitionMaintenance);
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
            return CompletableFuture.failedFuture(new UnsupportedOperationException());
        }

        @Override
        public void close() {}
    }
}
