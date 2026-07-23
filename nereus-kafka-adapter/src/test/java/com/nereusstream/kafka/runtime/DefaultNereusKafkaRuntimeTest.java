/* Licensed under the Apache License, Version 2.0 */
package com.nereusstream.kafka.runtime;

import com.nereusstream.api.ErrorCode;
import com.nereusstream.api.NereusException;
import com.nereusstream.kafka.partition.KafkaPartitionIdentity;
import com.nereusstream.kafka.partition.KafkaPartitionLeaderOpenRequest;
import com.nereusstream.kafka.partition.KafkaPartitionStorage;
import com.nereusstream.kafka.partition.KafkaPartitionStorageManager;
import java.time.Duration;
import java.util.List;
import java.util.Optional;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.CompletionException;
import java.util.concurrent.TimeoutException;
import java.util.concurrent.atomic.AtomicInteger;
import org.junit.jupiter.api.Test;

import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.assertThatThrownBy;

class DefaultNereusKafkaRuntimeTest {
    @Test
    void startIsOperationOwnedDeduplicatedAndPublishesReadiness() {
        FakeManager manager = new FakeManager(CompletableFuture.completedFuture(null));
        CompletableFuture<Void> startup = new CompletableFuture<>();
        AtomicInteger starts = new AtomicInteger();
        DefaultNereusKafkaRuntime runtime = runtime(manager, () -> {
            starts.incrementAndGet();
            return startup;
        }, new KafkaRuntimeResources(List.of()));

        CompletableFuture<Void> first = runtime.start().toCompletableFuture();
        CompletableFuture<Void> second = runtime.start().toCompletableFuture();
        assertThat(starts).hasValue(1);
        assertThat(first.cancel(false)).isTrue();
        assertThat(second).isNotCancelled();

        startup.complete(null);

        assertThat(second).isCompletedWithValue(null);
        assertThat(runtime.health()).isEqualTo(new KafkaStorageHealth(
                KafkaStorageAdmissionState.READY, true, "runtime ready"));
        runtime.admission().requireReady("produce");
    }

    @Test
    void drainClosesAdmissionSynchronouslyTimesOutAsAViewAndClosesResourcesInOrder() {
        CompletableFuture<Void> managerDrain = new CompletableFuture<>();
        FakeManager manager = new FakeManager(managerDrain);
        List<String> closes = new java.util.ArrayList<>();
        KafkaRuntimeResources resources = new KafkaRuntimeResources(List.of(
                KafkaRuntimeResources.Resource.owned("provider", () -> closes.add("provider"))));
        DefaultNereusKafkaRuntime runtime = runtime(
                manager, () -> CompletableFuture.completedFuture(null), resources);
        runtime.start().toCompletableFuture().join();

        CompletableFuture<Void> drain = runtime.beginDrain(DrainReason.BROKER_SHUTDOWN).toCompletableFuture();
        assertThat(runtime.health().state()).isEqualTo(KafkaStorageAdmissionState.DRAINING);
        assertRejected(runtime, ErrorCode.METADATA_UNAVAILABLE, true);
        assertThatThrownBy(() -> runtime.awaitDrained(Duration.ofMillis(10)).toCompletableFuture().join())
                .isInstanceOf(CompletionException.class)
                .hasCauseInstanceOf(TimeoutException.class);
        assertThat(drain).isNotDone();

        managerDrain.complete(null);
        assertThat(drain).isCompletedWithValue(null);
        assertThat(runtime.awaitDrained(Duration.ofSeconds(1)).toCompletableFuture())
                .isCompletedWithValue(null);

        runtime.close();
        runtime.close();
        assertThat(runtime.health().state()).isEqualTo(KafkaStorageAdmissionState.CLOSED);
        assertThat(closes).containsExactly("provider");
        assertThat(manager.shutdowns).hasValue(1);
        assertThat(manager.closes).hasValue(1);
    }

    @Test
    void lateStartupCannotReopenDrainAndStartupFailureStaysNotReady() {
        FakeManager drainingManager = new FakeManager(CompletableFuture.completedFuture(null));
        CompletableFuture<Void> lateStartup = new CompletableFuture<>();
        DefaultNereusKafkaRuntime draining = runtime(
                drainingManager, () -> lateStartup, new KafkaRuntimeResources(List.of()));
        CompletableFuture<Void> start = draining.start().toCompletableFuture();
        draining.beginDrain(DrainReason.STARTUP_FAILURE);
        lateStartup.complete(null);

        assertThatThrownBy(start::join)
                .isInstanceOf(CompletionException.class)
                .hasCauseInstanceOf(NereusException.class);
        assertThat(draining.health().state()).isEqualTo(KafkaStorageAdmissionState.DRAINING);

        FakeManager failedManager = new FakeManager(CompletableFuture.completedFuture(null));
        RuntimeException startupFailure = new RuntimeException("activation failed");
        DefaultNereusKafkaRuntime failed = runtime(
                failedManager,
                () -> CompletableFuture.failedFuture(startupFailure),
                new KafkaRuntimeResources(List.of()));
        assertThatThrownBy(() -> failed.start().toCompletableFuture().join())
                .isInstanceOf(CompletionException.class)
                .hasCause(startupFailure);
        assertThat(failed.health().state()).isEqualTo(KafkaStorageAdmissionState.NOT_READY);
        assertThat(failed.health().detail()).contains("RuntimeException");
        assertThat(failedManager.shutdowns).hasValue(0);
    }

    private static DefaultNereusKafkaRuntime runtime(
            KafkaPartitionStorageManager manager,
            java.util.function.Supplier<CompletableFuture<Void>> startup,
            KafkaRuntimeResources resources) {
        return new DefaultNereusKafkaRuntime(
                new KafkaStorageAdmission(), manager, startup, resources);
    }

    private static void assertRejected(
            DefaultNereusKafkaRuntime runtime,
            ErrorCode expected,
            boolean retriable) {
        assertThatThrownBy(() -> runtime.admission().requireReady("fetch"))
                .isInstanceOfSatisfying(NereusException.class, failure -> {
                    assertThat(failure.code()).isEqualTo(expected);
                    assertThat(failure.retriable()).isEqualTo(retriable);
                });
    }

    private static final class FakeManager implements KafkaPartitionStorageManager {
        private final CompletableFuture<Void> drain;
        private final AtomicInteger shutdowns = new AtomicInteger();
        private final AtomicInteger closes = new AtomicInteger();

        private FakeManager(CompletableFuture<Void> drain) {
            this.drain = drain;
        }

        @Override
        public CompletableFuture<KafkaPartitionStorage> openLeader(KafkaPartitionLeaderOpenRequest request) {
            return CompletableFuture.failedFuture(new UnsupportedOperationException());
        }

        @Override
        public CompletableFuture<Void> resign(
                KafkaPartitionIdentity identity,
                int observedLeaderEpoch,
                Duration timeout) {
            return CompletableFuture.failedFuture(new UnsupportedOperationException());
        }

        @Override
        public CompletableFuture<Void> delete(
                KafkaPartitionIdentity identity,
                long metadataOffset,
                Duration timeout) {
            return CompletableFuture.failedFuture(new UnsupportedOperationException());
        }

        @Override
        public Optional<KafkaPartitionStorage> current(KafkaPartitionIdentity identity) {
            return Optional.empty();
        }

        @Override
        public CompletableFuture<Void> shutdown() {
            shutdowns.incrementAndGet();
            return drain;
        }

        @Override
        public void close() {
            closes.incrementAndGet();
        }
    }
}
