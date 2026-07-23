/* Licensed under the Apache License, Version 2.0 */
package com.nereusstream.kafka.activation;

import com.nereusstream.api.ErrorCode;
import com.nereusstream.api.NereusException;
import com.nereusstream.api.StorageProfile;
import com.nereusstream.metadata.oxia.VersionedKafkaStorageProtocolActivation;
import com.nereusstream.metadata.oxia.records.KafkaStorageActivationLifecycle;
import com.nereusstream.metadata.oxia.records.KafkaStorageProtocolActivationRecord;
import java.time.Clock;
import java.time.Duration;
import java.time.Instant;
import java.time.ZoneOffset;
import java.util.ArrayDeque;
import java.util.List;
import java.util.Set;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.CompletionException;
import org.junit.jupiter.api.Test;

import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.assertThatThrownBy;

class KafkaStorageFirstActivationCoordinatorTest {
    private static final long NOW = 10_000;
    private static final Clock CLOCK = Clock.fixed(Instant.ofEpochMilli(NOW), ZoneOffset.UTC);

    @Test
    void activatesAnEmptyClusterWithExactCapabilityAndBrokerDigests() {
        InMemoryKafkaStorageActivationStore store = capableStore();
        KafkaStorageFirstActivationCoordinator coordinator = coordinator(
                store, new SequenceSnapshots(empty(100), empty(101)));

        VersionedKafkaStorageProtocolActivation active =
                coordinator.activate().toCompletableFuture().join();

        assertThat(active.value().lifecycle()).isEqualTo(KafkaStorageActivationLifecycle.ACTIVE);
        assertThat(active.value().preparedAtMetadataOffset()).isEqualTo(100);
        assertThat(active.value().allowedStorageProfiles())
                .containsExactly(StorageProfile.OBJECT_WAL_SYNC_OBJECT.name());
        assertThat(store.getReadiness().join().orElseThrow().value().kraftMetadataOffset())
                .isEqualTo(100);
        assertThat(active.value().requiredBrokerSetSha256())
                .containsExactly(store.getReadiness().join().orElseThrow().value().brokerSetSha256());
        assertThat(active.value().requiredCapabilitySha256())
                .containsExactly(store.getReadiness().join().orElseThrow().value().capabilitySha256());
    }

    @Test
    void rejectsNonEmptyClusterWithoutWritingControlPlaneState() {
        InMemoryKafkaStorageActivationStore store = capableStore();
        KafkaStorageClusterSnapshot nonempty = new KafkaStorageClusterSnapshot(
                KafkaActivationTestSupport.CLUSTER,
                100,
                KafkaStorageProtocolActivationRecord.KAFKA_FEATURE_LEVEL,
                List.of(KafkaActivationTestSupport.BROKER),
                true,
                false,
                false);

        assertFailure(
                coordinator(store, new SequenceSnapshots(nonempty)).activate(),
                ErrorCode.METADATA_INVARIANT_VIOLATION,
                false,
                "first activation requires zero topics, authoritative local logs and bindings");
        assertThat(store.getActivation().join()).isEmpty();
        assertThat(store.getReadiness().join()).isEmpty();
    }

    @Test
    void resumesPreparedAfterControllerFailureWithoutChangingItsFacts() {
        InMemoryKafkaStorageActivationStore store = capableStore();
        store.failNextActivationCas();
        KafkaStorageFirstActivationCoordinator first = coordinator(
                store, new SequenceSnapshots(empty(100), empty(101)));

        assertThatThrownBy(() -> first.activate().toCompletableFuture().join())
                .hasRootCauseMessage("activation CAS interrupted");
        VersionedKafkaStorageProtocolActivation prepared = store.getActivation().join().orElseThrow();
        assertThat(prepared.value().lifecycle()).isEqualTo(KafkaStorageActivationLifecycle.PREPARED);

        VersionedKafkaStorageProtocolActivation active = coordinator(
                store, new SequenceSnapshots(empty(102), empty(103)))
                .activate().toCompletableFuture().join();
        assertThat(active.value().lifecycle()).isEqualTo(KafkaStorageActivationLifecycle.ACTIVE);
        assertThat(active.value().activationEpoch()).isEqualTo(prepared.value().activationEpoch());
        assertThat(active.value().preparedAtMillis()).isEqualTo(prepared.value().preparedAtMillis());
    }

    @Test
    void leavesPreparedWhenTheClusterStopsBeingEmptyAtTheSecondProof() {
        InMemoryKafkaStorageActivationStore store = capableStore();
        KafkaStorageClusterSnapshot topicsAppeared = new KafkaStorageClusterSnapshot(
                KafkaActivationTestSupport.CLUSTER,
                101,
                KafkaStorageProtocolActivationRecord.KAFKA_FEATURE_LEVEL,
                List.of(KafkaActivationTestSupport.BROKER),
                true,
                false,
                false);

        assertFailure(
                coordinator(store, new SequenceSnapshots(empty(100), topicsAppeared)).activate(),
                ErrorCode.METADATA_INVARIANT_VIOLATION,
                false,
                "first activation requires zero topics, authoritative local logs and bindings");
        assertThat(store.getActivation().join().orElseThrow().value().lifecycle())
                .isEqualTo(KafkaStorageActivationLifecycle.PREPARED);
    }

    @Test
    void activeRetryIsIdempotentEvenAfterTopicsExist() {
        InMemoryKafkaStorageActivationStore store = capableStore();
        VersionedKafkaStorageProtocolActivation first = coordinator(
                store, new SequenceSnapshots(empty(100), empty(101)))
                .activate().toCompletableFuture().join();
        KafkaStorageClusterSnapshot populated = new KafkaStorageClusterSnapshot(
                KafkaActivationTestSupport.CLUSTER,
                150,
                KafkaStorageProtocolActivationRecord.KAFKA_FEATURE_LEVEL,
                List.of(KafkaActivationTestSupport.BROKER),
                true,
                true,
                true);

        VersionedKafkaStorageProtocolActivation retried = coordinator(
                store, new SequenceSnapshots(populated)).activate().toCompletableFuture().join();

        assertThat(retried).isEqualTo(first);
    }

    private static InMemoryKafkaStorageActivationStore capableStore() {
        InMemoryKafkaStorageActivationStore store = new InMemoryKafkaStorageActivationStore();
        store.createCapability(KafkaActivationTestSupport.specification(3).initialRecord(NOW)).join();
        return store;
    }

    private static KafkaStorageFirstActivationCoordinator coordinator(
            InMemoryKafkaStorageActivationStore store,
            KafkaStorageClusterSnapshotProvider snapshots) {
        KafkaStorageActivationPolicy policy = new KafkaStorageActivationPolicy(
                KafkaActivationTestSupport.CLUSTER,
                Set.of(StorageProfile.OBJECT_WAL_SYNC_OBJECT),
                StorageProfile.OBJECT_WAL_SYNC_OBJECT,
                Duration.ofSeconds(30));
        return new KafkaStorageFirstActivationCoordinator(store, snapshots, policy, CLOCK);
    }

    private static KafkaStorageClusterSnapshot empty(long offset) {
        return new KafkaStorageClusterSnapshot(
                KafkaActivationTestSupport.CLUSTER,
                offset,
                KafkaStorageProtocolActivationRecord.KAFKA_FEATURE_LEVEL,
                List.of(KafkaActivationTestSupport.BROKER),
                false,
                false,
                false);
    }

    private static void assertFailure(
            java.util.concurrent.CompletionStage<?> operation,
            ErrorCode code,
            boolean retriable,
            String message) {
        assertThatThrownBy(() -> operation.toCompletableFuture().join()).satisfies(failure -> {
            Throwable exact = unwrap(failure);
            assertThat(exact).isInstanceOf(NereusException.class);
            NereusException nereus = (NereusException) exact;
            assertThat(nereus.code()).isEqualTo(code);
            assertThat(nereus.retriable()).isEqualTo(retriable);
            assertThat(nereus).hasMessage(message);
        });
    }

    private static Throwable unwrap(Throwable supplied) {
        Throwable current = supplied;
        while (current instanceof CompletionException && current.getCause() != null) {
            current = current.getCause();
        }
        return current;
    }

    private static final class SequenceSnapshots implements KafkaStorageClusterSnapshotProvider {
        private final ArrayDeque<KafkaStorageClusterSnapshot> snapshots;

        private SequenceSnapshots(KafkaStorageClusterSnapshot... snapshots) {
            this.snapshots = new ArrayDeque<>(List.of(snapshots));
        }

        @Override
        public CompletableFuture<KafkaStorageClusterSnapshot> currentSnapshot() {
            KafkaStorageClusterSnapshot next = snapshots.pollFirst();
            if (next == null) {
                return CompletableFuture.failedFuture(new AssertionError("unexpected snapshot read"));
            }
            return CompletableFuture.completedFuture(next);
        }
    }
}
