/* Licensed under the Apache License, Version 2.0 */
package com.nereusstream.kafka.metadata;

import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.assertThatThrownBy;

import com.nereusstream.kafka.partition.KafkaPartitionIdentity;
import com.nereusstream.kafka.testing.TestStreamStorage;
import com.nereusstream.metadata.oxia.records.KafkaPartitionLifecycle;
import com.nereusstream.metadata.oxia.testing.FakeKafkaPartitionMetadataStore;
import java.util.concurrent.CompletionException;
import org.junit.jupiter.api.Test;

class KafkaPartitionLifecycleIntegrationTest {
    @Test
    void restartConvergesAfterStreamCreateCommittedButResponseWasLost() {
        FakeKafkaPartitionMetadataStore metadata = new FakeKafkaPartitionMetadataStore("nereus", "kraft");
        TestStreamStorage streams = new TestStreamStorage();
        KafkaPartitionLifecycleCoordinator firstRuntime =
                KafkaPartitionLifecycleCoordinatorTest.coordinator(metadata, streams);
        KafkaPartitionIdentity identity = KafkaPartitionLifecycleCoordinatorTest.identity(10, 1, "payments");
        streams.loseNextCreateResponseAfterCommit();

        assertThatThrownBy(() -> firstRuntime.ensureBinding(
                KafkaPartitionLifecycleCoordinatorTest.request(identity, 50)).join())
                .isInstanceOf(CompletionException.class)
                .hasRootCauseMessage("simulated create response loss");
        assertThat(metadata.get(identity.durableId()).join().orElseThrow().value().lifecycle())
                .isEqualTo(KafkaPartitionLifecycle.CREATING);

        KafkaPartitionLifecycleCoordinator restarted =
                KafkaPartitionLifecycleCoordinatorTest.coordinator(metadata, streams);
        KafkaPartitionBinding recovered = restarted.ensureBinding(
                KafkaPartitionLifecycleCoordinatorTest.request(identity, 50)).join();

        assertThat(recovered.durableRoot().value().lifecycle()).isEqualTo(KafkaPartitionLifecycle.ACTIVE);
        assertThat(streams.creates()).isEqualTo(1);
    }

    @Test
    void twoReconcilersConvergeToTheSameDurableStream() {
        FakeKafkaPartitionMetadataStore metadata = new FakeKafkaPartitionMetadataStore("nereus", "kraft");
        TestStreamStorage streams = new TestStreamStorage();
        KafkaPartitionIdentity identity = KafkaPartitionLifecycleCoordinatorTest.identity(11, 9, "events");

        KafkaPartitionBinding first = KafkaPartitionLifecycleCoordinatorTest.coordinator(metadata, streams)
                .ensureBinding(KafkaPartitionLifecycleCoordinatorTest.request(identity, 60)).join();
        KafkaPartitionBinding second = KafkaPartitionLifecycleCoordinatorTest.coordinator(metadata, streams)
                .ensureBinding(KafkaPartitionLifecycleCoordinatorTest.request(identity, 60)).join();

        assertThat(second.streamId()).isEqualTo(first.streamId());
        assertThat(streams.streamCount()).isEqualTo(1);
    }
}
