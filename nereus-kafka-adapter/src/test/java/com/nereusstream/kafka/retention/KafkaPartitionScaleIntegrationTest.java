/* Licensed under the Apache License, Version 2.0 */

package com.nereusstream.kafka.retention;

import static org.assertj.core.api.Assertions.assertThat;
import com.nereusstream.api.Checksum;
import com.nereusstream.api.ChecksumType;
import com.nereusstream.api.StorageProfile;
import com.nereusstream.api.StreamId;
import com.nereusstream.api.StreamName;
import com.nereusstream.kafka.checkpoint.KafkaCheckpointSourceState;
import com.nereusstream.kafka.metadata.KafkaBindingRequest;
import com.nereusstream.kafka.metadata.KafkaPartitionBinding;
import com.nereusstream.kafka.metadata.KafkaPartitionBindingLifecycle;
import com.nereusstream.kafka.metadata.KafkaPartitionDeleteRequest;
import com.nereusstream.kafka.partition.DefaultKafkaPartitionStorageManager;
import com.nereusstream.kafka.partition.KafkaAppendContext;
import com.nereusstream.kafka.partition.KafkaPartitionEventListener;
import com.nereusstream.kafka.partition.KafkaPartitionEventSubscription;
import com.nereusstream.kafka.partition.KafkaPartitionIdentity;
import com.nereusstream.kafka.partition.KafkaPartitionLeaderOpenRequest;
import com.nereusstream.kafka.partition.KafkaPartitionState;
import com.nereusstream.kafka.partition.KafkaPartitionStorage;
import com.nereusstream.kafka.partition.KafkaStableAppendResult;
import com.nereusstream.kafka.partition.KafkaStableSnapshot;
import com.nereusstream.kafka.partition.KafkaStorageReadRequest;
import com.nereusstream.kafka.partition.KafkaStorageReadResult;
import com.nereusstream.metadata.oxia.KafkaPartitionMetadataTransitions;
import com.nereusstream.metadata.oxia.VersionedKafkaPartitionBinding;
import com.nereusstream.metadata.oxia.records.KafkaPartitionOperationType;
import com.nereusstream.metadata.oxia.records.KafkaPartitionPendingOperationRecord;
import java.nio.ByteBuffer;
import java.time.Clock;
import java.time.Duration;
import java.time.Instant;
import java.time.ZoneOffset;
import java.util.ArrayList;
import java.util.Base64;
import java.util.List;
import java.util.Optional;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.ConcurrentLinkedQueue;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.ScheduledThreadPoolExecutor;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicBoolean;
import java.util.concurrent.atomic.AtomicInteger;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.Timeout;

/**
 * F9-M7 exact 10,000-partition open, bounded-maintenance, and close boundary.
 */
class KafkaPartitionScaleIntegrationTest {
    private static final int PARTITION_COUNT = 10_000;
    private static final int MAX_CONCURRENT_MAINTENANCE = 64;
    private static final StorageProfile PROFILE = StorageProfile.OBJECT_WAL_SYNC_OBJECT;
    private static final Clock CLOCK = Clock.fixed(Instant.ofEpochMilli(10_000), ZoneOffset.UTC);

    @Test
    @Timeout(value = 3, unit = TimeUnit.MINUTES)
    void scenarioKfScl002() throws Exception {
        ScaleLifecycle lifecycle = new ScaleLifecycle();
        MaintenanceTracker tracker = new MaintenanceTracker();
        AtomicInteger resigned = new AtomicInteger();
        DefaultKafkaPartitionStorageManager manager = new DefaultKafkaPartitionStorageManager(
                lifecycle,
                plan -> CompletableFuture.completedFuture(new ScaleStorage(
                        plan.authority().identity(), plan.authority().leaderEpoch(), tracker, resigned)),
                CLOCK,
                "scale-broker-run",
                1,
                Duration.ofMinutes(5));
        ArrayList<KafkaPartitionIdentity> identities = new ArrayList<>(PARTITION_COUNT);
        ArrayList<KafkaPartitionMaintenanceRuntime.OwnedPartition> owned = new ArrayList<>(PARTITION_COUNT);
        for (int partition = 0; partition < PARTITION_COUNT; partition++) {
            KafkaPartitionIdentity identity = identity(partition);
            KafkaPartitionStorage opened = manager.openLeader(new KafkaPartitionLeaderOpenRequest(
                            identity, 1, 1, 1, PROFILE, partition, Duration.ofMinutes(1)))
                    .join();
            assertThat(opened.identity()).isEqualTo(identity);
            identities.add(identity);
            owned.add(new KafkaPartitionMaintenanceRuntime.OwnedPartition(identity, 1, UnsupportedHooks.INSTANCE));
        }
        assertThat(lifecycle.ensureCalls()).hasValue(PARTITION_COUNT);
        assertThat(identities)
                .allSatisfy(identity -> assertThat(manager.current(identity)).isPresent());

        ScheduledThreadPoolExecutor scheduler = new ScheduledThreadPoolExecutor(1);
        ExecutorService callbacks = Executors.newFixedThreadPool(8, runnable -> {
            Thread thread = new Thread(runnable, "f9-partition-scale-callback");
            thread.setDaemon(true);
            return thread;
        });
        KafkaPartitionMaintenanceRuntime maintenance = new KafkaPartitionMaintenanceRuntime(
                manager,
                maximum -> {
                    assertThat(maximum).isEqualTo(PARTITION_COUNT);
                    return CompletableFuture.completedFuture(List.copyOf(owned));
                },
                Duration.ofDays(1),
                MAX_CONCURRENT_MAINTENANCE,
                PARTITION_COUNT,
                scheduler,
                callbacks);
        try {
            maintenance.start().join();
            tracker.firstWave().get(30, TimeUnit.SECONDS);
            assertThat(tracker.active()).hasValue(MAX_CONCURRENT_MAINTENANCE);
            assertThat(tracker.maximumActive()).hasValue(MAX_CONCURRENT_MAINTENANCE);
            tracker.releaseFirstWave();
            tracker.allCompleted().get(60, TimeUnit.SECONDS);
            assertThat(tracker.started()).hasValue(PARTITION_COUNT);
            assertThat(tracker.completed()).hasValue(PARTITION_COUNT);
            assertThat(tracker.active()).hasValue(0);
            maintenance.closeAsync().get(30, TimeUnit.SECONDS);
        } finally {
            callbacks.shutdownNow();
            scheduler.shutdownNow();
        }

        manager.shutdown().get(30, TimeUnit.SECONDS);
        assertThat(resigned).hasValue(PARTITION_COUNT);
        assertThat(identities)
                .allSatisfy(identity -> assertThat(manager.current(identity)).isEmpty());
    }

    private static KafkaPartitionIdentity identity(int partition) {
        byte[] topicId =
                ByteBuffer.allocate(16).putLong(0x4e45524555534639L).putLong(1).array();
        return new KafkaPartitionIdentity(
                "kraft-scale",
                Base64.getUrlEncoder().withoutPadding().encodeToString(topicId),
                partition,
                "scale-topic");
    }

    private static KafkaPartitionBinding binding(KafkaBindingRequest request) {
        long now = CLOCK.millis();
        String attempt = KafkaPartitionMetadataTransitions.deterministicCreateAttemptId(
                request.identity().durableId(), request.metadataOffset());
        var creating = KafkaPartitionMetadataTransitions.creating(
                request.identity().durableId(),
                request.identity().observedTopicName(),
                request.storageProfile().name(),
                request.metadataOffset(),
                now,
                new KafkaPartitionPendingOperationRecord(
                        KafkaPartitionOperationType.CREATE.wireId(),
                        attempt,
                        request.operationOwnerId(),
                        request.operationOwnerEpoch(),
                        now + request.operationTtl().toMillis(),
                        request.metadataOffset(),
                        now,
                        ""));
        String streamToken = Integer.toString(request.identity().partition());
        StreamName streamName = new StreamName("kafka-scale-" + streamToken);
        StreamId streamId = new StreamId("stream-scale-" + streamToken);
        var active = KafkaPartitionMetadataTransitions.activate(
                creating, streamName.value(), streamId.value(), request.metadataOffset() + 1, now + 1);
        return new KafkaPartitionBinding(
                request.identity(),
                streamName,
                streamId,
                new VersionedKafkaPartitionBinding(
                        "/f9/scale/" + streamToken, active, 0, new Checksum(ChecksumType.SHA256, "a".repeat(64))));
    }

    private static final class ScaleLifecycle implements KafkaPartitionBindingLifecycle {
        private final AtomicInteger ensureCalls = new AtomicInteger();

        @Override
        public CompletableFuture<KafkaPartitionBinding> ensureBinding(KafkaBindingRequest request) {
            ensureCalls.incrementAndGet();
            return CompletableFuture.completedFuture(binding(request));
        }

        @Override
        public CompletableFuture<Void> delete(KafkaPartitionDeleteRequest request) {
            return CompletableFuture.completedFuture(null);
        }

        private AtomicInteger ensureCalls() {
            return ensureCalls;
        }
    }

    private static final class MaintenanceTracker {
        private final AtomicInteger started = new AtomicInteger();
        private final AtomicInteger active = new AtomicInteger();
        private final AtomicInteger maximumActive = new AtomicInteger();
        private final AtomicInteger completed = new AtomicInteger();
        private final CompletableFuture<Void> firstWave = new CompletableFuture<>();
        private final CompletableFuture<Void> allCompleted = new CompletableFuture<>();
        private final ConcurrentLinkedQueue<CompletableFuture<KafkaRetentionCoordinator.RunResult>> held =
                new ConcurrentLinkedQueue<>();

        private CompletableFuture<KafkaRetentionCoordinator.RunResult> begin() {
            int ordinal = started.incrementAndGet();
            int current = active.incrementAndGet();
            maximumActive.accumulateAndGet(current, Math::max);
            CompletableFuture<KafkaRetentionCoordinator.RunResult> result;
            if (ordinal <= MAX_CONCURRENT_MAINTENANCE) {
                result = new CompletableFuture<>();
                held.add(result);
                if (ordinal == MAX_CONCURRENT_MAINTENANCE) {
                    firstWave.complete(null);
                }
            } else {
                result = CompletableFuture.completedFuture(null);
            }
            result.whenComplete((ignored, failure) -> {
                active.decrementAndGet();
                if (completed.incrementAndGet() == PARTITION_COUNT) {
                    allCompleted.complete(null);
                }
            });
            return result;
        }

        private void releaseFirstWave() {
            CompletableFuture<KafkaRetentionCoordinator.RunResult> operation;
            while ((operation = held.poll()) != null) {
                operation.complete(null);
            }
        }

        private AtomicInteger started() {
            return started;
        }

        private AtomicInteger active() {
            return active;
        }

        private AtomicInteger maximumActive() {
            return maximumActive;
        }

        private AtomicInteger completed() {
            return completed;
        }

        private CompletableFuture<Void> firstWave() {
            return firstWave;
        }

        private CompletableFuture<Void> allCompleted() {
            return allCompleted;
        }
    }

    private static final class ScaleStorage implements KafkaPartitionStorage {
        private final KafkaPartitionIdentity identity;
        private final int leaderEpoch;
        private final MaintenanceTracker tracker;
        private final AtomicInteger resigned;
        private final AtomicBoolean closed = new AtomicBoolean();

        private ScaleStorage(
                KafkaPartitionIdentity identity, int leaderEpoch, MaintenanceTracker tracker, AtomicInteger resigned) {
            this.identity = identity;
            this.leaderEpoch = leaderEpoch;
            this.tracker = tracker;
            this.resigned = resigned;
        }

        @Override
        public KafkaPartitionIdentity identity() {
            return identity;
        }

        @Override
        public int leaderEpoch() {
            return leaderEpoch;
        }

        @Override
        public StorageProfile storageProfile() {
            return PROFILE;
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
        public Optional<KafkaPartitionMaintenance> maintenance() {
            return Optional.of(new KafkaPartitionMaintenance() {
                @Override
                public CompletableFuture<KafkaRetentionCoordinator.RunResult> runRetention(Hooks hooks) {
                    return tracker.begin();
                }

                @Override
                public CompletableFuture<KafkaDeleteRecordsCoordinator.Result> deleteRecords(
                        Hooks hooks, long normalizedRequestedOffset) {
                    return CompletableFuture.failedFuture(new UnsupportedOperationException());
                }
            });
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
            return () -> {};
        }

        @Override
        public CompletableFuture<Void> resign() {
            if (closed.compareAndSet(false, true)) {
                resigned.incrementAndGet();
            }
            return CompletableFuture.completedFuture(null);
        }

        @Override
        public void close() {
            resign();
        }
    }

    private enum UnsupportedHooks implements KafkaPartitionMaintenance.Hooks {
        INSTANCE;

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
}
