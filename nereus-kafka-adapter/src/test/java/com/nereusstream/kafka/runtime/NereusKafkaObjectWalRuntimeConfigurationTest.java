/* Licensed under the Apache License, Version 2.0 */
package com.nereusstream.kafka.runtime;

import com.nereusstream.api.StorageProfile;
import com.nereusstream.core.StreamStorageConfig;
import com.nereusstream.metadata.oxia.OxiaClientConfiguration;
import com.nereusstream.objectstore.ObjectPutRetryPolicy;
import com.nereusstream.objectstore.ObjectStoreConfiguration;
import java.net.URI;
import java.time.Duration;
import java.util.Optional;
import java.util.Set;
import org.junit.jupiter.api.Test;

import static org.assertj.core.api.Assertions.assertThatThrownBy;

class NereusKafkaObjectWalRuntimeConfigurationTest {
    @Test
    void acceptsOnlyExactFailClosedObjectWalComposition() {
        NereusKafkaRuntimeConfiguration runtime = runtime(
                Set.of(StorageProfile.OBJECT_WAL_SYNC_OBJECT));
        new NereusKafkaObjectWalRuntimeConfiguration(
                runtime,
                streams(false),
                oxia(),
                objects(),
                Duration.ofMinutes(10),
                Duration.ofSeconds(5),
                Duration.ofHours(24),
                2);

        assertThatThrownBy(() -> new NereusKafkaObjectWalRuntimeConfiguration(
                runtime,
                streams(true),
                oxia(),
                objects(),
                Duration.ofMinutes(10),
                Duration.ofSeconds(5),
                Duration.ofHours(24),
                2))
                .isInstanceOf(IllegalArgumentException.class)
                .hasMessageContaining("automatic append-session");
        assertThatThrownBy(() -> new NereusKafkaObjectWalRuntimeConfiguration(
                runtime(Set.of(
                        StorageProfile.OBJECT_WAL_SYNC_OBJECT,
                        StorageProfile.BOOKKEEPER_WAL_ONLY)),
                streams(false),
                oxia(),
                objects(),
                Duration.ofMinutes(10),
                Duration.ofSeconds(5),
                Duration.ofHours(24),
                2))
                .isInstanceOf(IllegalArgumentException.class)
                .hasMessageContaining("exactly OBJECT_WAL_SYNC_OBJECT");
    }

    private static NereusKafkaRuntimeConfiguration runtime(Set<StorageProfile> profiles) {
        return new NereusKafkaRuntimeConfiguration(
                "nereus",
                "kraft",
                "broker-run",
                Duration.ofSeconds(30),
                Duration.ofSeconds(5),
                "broker-run",
                7,
                Duration.ofSeconds(30),
                profiles);
    }

    private static StreamStorageConfig streams(boolean autoAcquire) {
        return new StreamStorageConfig(
                "nereus",
                "broker-run",
                Duration.ofSeconds(30),
                Duration.ofSeconds(10),
                Duration.ofSeconds(5),
                Duration.ofSeconds(30),
                Duration.ofSeconds(30),
                Duration.ofSeconds(30),
                64,
                10_000,
                256,
                10_000,
                1_024,
                64L * 1024 * 1024,
                64,
                128L * 1024 * 1024,
                16 * 1024 * 1024,
                100_000,
                Duration.ofSeconds(5),
                autoAcquire,
                false,
                true);
    }

    private static OxiaClientConfiguration oxia() {
        return OxiaClientConfiguration.defaults("oxia://localhost:6648");
    }

    private static ObjectStoreConfiguration objects() {
        return new ObjectStoreConfiguration(
                "provider",
                URI.create("http://localhost:9000"),
                "us-east-1",
                "bucket",
                "nereus/kafka",
                true,
                Duration.ofSeconds(30),
                new ObjectPutRetryPolicy(3, Duration.ofMillis(25), Duration.ofSeconds(1)),
                32,
                Optional.empty(),
                Optional.empty(),
                Optional.empty());
    }
}
