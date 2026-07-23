/* Licensed under the Apache License, Version 2.0 */
package com.nereusstream.kafka.metadata;

import static org.assertj.core.api.Assertions.assertThat;

import com.nereusstream.api.StorageProfile;
import com.nereusstream.api.StreamState;
import com.nereusstream.kafka.partition.KafkaPartitionIdentity;
import com.nereusstream.kafka.testing.TestStreamStorage;
import com.nereusstream.metadata.oxia.KafkaPartitionKeyspace;
import com.nereusstream.metadata.oxia.records.KafkaPartitionLifecycle;
import com.nereusstream.metadata.oxia.testing.FakeKafkaPartitionMetadataStore;
import java.nio.ByteBuffer;
import java.time.Clock;
import java.time.Duration;
import java.time.Instant;
import java.time.ZoneOffset;
import java.util.Base64;
import org.junit.jupiter.api.Test;

class KafkaPartitionLifecycleCoordinatorTest {
    private static final Clock CLOCK = Clock.fixed(Instant.ofEpochMilli(10_000), ZoneOffset.UTC);

    @Test
    void createsOneDeterministicBindingAndDeletesItIdempotently() {
        FakeKafkaPartitionMetadataStore metadata = new FakeKafkaPartitionMetadataStore("nereus", "kraft");
        TestStreamStorage streams = new TestStreamStorage();
        KafkaPartitionLifecycleCoordinator coordinator = coordinator(metadata, streams);
        KafkaPartitionIdentity identity = identity(1, 3, "orders");
        KafkaBindingRequest request = request(identity, 20);

        KafkaPartitionBinding first = coordinator.ensureBinding(request).join();
        KafkaPartitionBinding second = coordinator.ensureBinding(request).join();

        assertThat(second.streamId()).isEqualTo(first.streamId());
        assertThat(second.streamName()).isEqualTo(first.streamName());
        assertThat(streams.creates()).isEqualTo(1);
        assertThat(first.durableRoot().value().lifecycle()).isEqualTo(KafkaPartitionLifecycle.ACTIVE);
        assertThat(first.durableRoot().value().bindingEpoch()).isEqualTo(2);
        assertThat(streams.getStreamMetadata(first.streamId()).join().attributes())
                .isEqualTo(KafkaPartitionLifecycleCoordinator.streamAttributes(identity.durableId()));

        coordinator.delete(identity, 30, "broker-run", 1, Duration.ofSeconds(30), Duration.ofSeconds(5)).join();
        coordinator.delete(identity, 30, "broker-run", 1, Duration.ofSeconds(30), Duration.ofSeconds(5)).join();

        assertThat(metadata.get(identity.durableId()).join().orElseThrow().value().lifecycle())
                .isEqualTo(KafkaPartitionLifecycle.DELETED);
        assertThat(streams.getStreamMetadata(first.streamId()).join().state()).isEqualTo(StreamState.DELETED);
    }

    @Test
    void topicNameReuseWithANewTopicIdCreatesAnotherStream() {
        FakeKafkaPartitionMetadataStore metadata = new FakeKafkaPartitionMetadataStore("nereus", "kraft");
        TestStreamStorage streams = new TestStreamStorage();
        KafkaPartitionLifecycleCoordinator coordinator = coordinator(metadata, streams);

        KafkaPartitionBinding oldTopic = coordinator.ensureBinding(request(identity(2, 0, "orders"), 40)).join();
        KafkaPartitionBinding newTopic = coordinator.ensureBinding(request(identity(3, 0, "orders"), 41)).join();

        assertThat(newTopic.streamId()).isNotEqualTo(oldTopic.streamId());
        assertThat(newTopic.streamName()).isNotEqualTo(oldTopic.streamName());
        assertThat(streams.streamCount()).isEqualTo(2);
    }

    static KafkaPartitionLifecycleCoordinator coordinator(
            FakeKafkaPartitionMetadataStore metadata, TestStreamStorage streams) {
        return new KafkaPartitionLifecycleCoordinator(metadata, streams, metadata.keyspace(), CLOCK);
    }

    static KafkaBindingRequest request(KafkaPartitionIdentity identity, long metadataOffset) {
        return new KafkaBindingRequest(
                identity, StorageProfile.BOOKKEEPER_WAL_ASYNC_OBJECT, metadataOffset,
                "broker-run", 1, Duration.ofSeconds(30));
    }

    static KafkaPartitionIdentity identity(long value, int partition, String name) {
        ByteBuffer bytes = ByteBuffer.allocate(16).putLong(0x1234_5678_9abc_def0L).putLong(value);
        String topicId = Base64.getUrlEncoder().withoutPadding().encodeToString(bytes.array());
        return new KafkaPartitionIdentity("kraft", topicId, partition, name);
    }
}
