/* Licensed under the Apache License, Version 2.0 */
package com.nereusstream.metadata.oxia;

import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.assertThatThrownBy;

import com.nereusstream.metadata.oxia.records.KafkaPartitionBindingRecord;
import com.nereusstream.metadata.oxia.records.KafkaPartitionLifecycle;
import com.nereusstream.metadata.oxia.records.KafkaPartitionOperationType;
import com.nereusstream.metadata.oxia.records.KafkaPartitionPendingOperationRecord;
import com.nereusstream.metadata.oxia.records.KafkaPartitionRegistryRecord;
import io.oxia.testcontainers.OxiaContainer;
import java.time.Clock;
import java.time.Duration;
import java.util.Base64;
import java.util.HexFormat;
import java.util.Optional;
import java.util.UUID;
import java.util.concurrent.CompletionException;
import org.junit.jupiter.api.Test;
import org.testcontainers.junit.jupiter.Container;
import org.testcontainers.junit.jupiter.Testcontainers;
import org.testcontainers.utility.DockerImageName;

@Testcontainers
class KafkaPartitionMetadataOxiaIntegrationTest {
    private static final String IMAGE = "oxia/oxia:0.16.3";

    @Container
    private static final OxiaContainer OXIA =
            new OxiaContainer(DockerImageName.parse(IMAGE)).withShards(4);

    @Test
    void bindingCasRegistryScanAndRestartUseRealOxia() {
        String nereusCluster = "f9/kafka/" + UUID.randomUUID();
        String kafkaCluster = "kraft-cluster";
        KafkaPartitionId id = new KafkaPartitionId(kafkaCluster, topicId(1), 3);
        OxiaClientConfiguration configuration = configuration();
        VersionedKafkaPartitionBinding active;

        try (SharedOxiaClientRuntime runtime = SharedOxiaClientRuntime.connect(configuration, Clock.systemUTC());
                OxiaJavaKafkaPartitionMetadataStore store =
                        OxiaJavaKafkaPartitionMetadataStore.usingSharedRuntime(
                                configuration, runtime, nereusCluster, kafkaCluster)) {
            KafkaPartitionBindingRecord creating = creating(id);
            VersionedKafkaPartitionBinding created = store.putCreatingIfAbsent(creating).join();
            assertThat(store.putCreatingIfAbsent(creating).join()).isEqualTo(created);
            KafkaPartitionBindingRecord activated = KafkaPartitionMetadataTransitions.activate(
                    created.value(), KafkaPartitionMetadataTransitions.deterministicStreamName(id, 1),
                    "stream-id", 11, 1_100);
            active = store.compareAndSet(created, activated).join();
            assertThatThrownBy(() -> store.compareAndSet(created, activated).join())
                    .satisfies(failure -> assertThat(unwrap(failure))
                            .isInstanceOf(KafkaMetadataConditionFailedException.class));

            KafkaPartitionKeyspace keys = new KafkaPartitionKeyspace(nereusCluster, kafkaCluster);
            store.putRegistryHint(new KafkaPartitionRegistryRecord(
                    1, id.kafkaClusterId(), id.topicId(), id.partitionId(), keys.bindingRootKey(id),
                    HexFormat.of().parseHex(active.durableValueSha256().value()),
                    active.value().lifecycleId(), active.value().bindingEpoch(), 1_200, 0)).join();
            assertThat(store.scanRegistry(keys.registryShard(id), Optional.empty(), 1).join().values())
                    .singleElement().satisfies(value -> assertThat(value.value().identity()).isEqualTo(id));
        }

        try (SharedOxiaClientRuntime runtime = SharedOxiaClientRuntime.connect(configuration, Clock.systemUTC());
                OxiaJavaKafkaPartitionMetadataStore store =
                        OxiaJavaKafkaPartitionMetadataStore.usingSharedRuntime(
                                configuration, runtime, nereusCluster, kafkaCluster)) {
            assertThat(store.get(id).join()).contains(active);
            KafkaPartitionKeyspace keys = new KafkaPartitionKeyspace(nereusCluster, kafkaCluster);
            assertThat(store.scanRegistry(keys.registryShard(id), Optional.empty(), 10).join().values())
                    .hasSize(1);
        }
    }

    private static KafkaPartitionBindingRecord creating(KafkaPartitionId id) {
        long metadataOffset = 10;
        String attempt = KafkaPartitionMetadataTransitions.deterministicCreateAttemptId(id, metadataOffset);
        return KafkaPartitionMetadataTransitions.creating(
                id, "orders", "BOOKKEEPER_WAL_ASYNC_OBJECT", metadataOffset, 1_000,
                new KafkaPartitionPendingOperationRecord(
                        KafkaPartitionOperationType.CREATE.wireId(), attempt, "broker-run", 1,
                        2_000, metadataOffset, 1_000, ""));
    }

    private static String topicId(int value) {
        byte[] bytes = new byte[16];
        bytes[15] = (byte) value;
        return Base64.getUrlEncoder().withoutPadding().encodeToString(bytes);
    }

    private static OxiaClientConfiguration configuration() {
        return new OxiaClientConfiguration(
                OXIA.getServiceAddress(), "default", Duration.ofSeconds(10),
                Duration.ofSeconds(30), 100, 1_024);
    }

    private static Throwable unwrap(Throwable supplied) {
        Throwable current = supplied;
        while (current instanceof CompletionException && current.getCause() != null) {
            current = current.getCause();
        }
        return current;
    }
}
