/* Licensed under the Apache License, Version 2.0 */
package com.nereusstream.kafka.runtime;

import com.nereusstream.api.StorageProfile;
import com.nereusstream.core.StreamStorageConfig;
import com.nereusstream.metadata.oxia.OxiaClientConfiguration;
import com.nereusstream.objectstore.ObjectPutRetryPolicy;
import com.nereusstream.objectstore.ObjectStore;
import com.nereusstream.objectstore.ObjectStoreConfiguration;
import com.nereusstream.objectstore.ObjectStoreProvider;
import com.nereusstream.objectstore.ObjectStoreSecretResolver;
import java.net.URI;
import java.time.Clock;
import java.time.Duration;
import java.util.Optional;
import java.util.Set;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.Executors;
import java.util.concurrent.ScheduledExecutorService;
import java.util.concurrent.atomic.AtomicBoolean;
import org.junit.jupiter.api.Test;

import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.assertThatThrownBy;

class NereusKafkaObjectWalRuntimeFactoryTest {
    @Test
    void closesAlreadyRegisteredProviderWhenBootstrapFails() {
        FailingObjectStoreProvider provider = new FailingObjectStoreProvider();
        ScheduledExecutorService scheduler = Executors.newSingleThreadScheduledExecutor();
        try {
            assertThatThrownBy(() -> NereusKafkaObjectWalRuntimeFactory.create(
                    configuration(provider),
                    new NereusKafkaObjectWalRuntimeContext(
                            provider,
                            reference -> Optional.empty(),
                            scheduler,
                            request -> CompletableFuture.failedFuture(
                                    new AssertionError("recovery not expected")),
                            Clock.systemUTC(),
                            () -> CompletableFuture.completedFuture(null))))
                    .isInstanceOf(IllegalStateException.class)
                    .hasRootCauseMessage("object store creation failed");
        } finally {
            scheduler.shutdownNow();
        }

        assertThat(provider.closed()).isTrue();
    }

    private static NereusKafkaObjectWalRuntimeConfiguration configuration(
            ObjectStoreProvider provider) {
        NereusKafkaRuntimeConfiguration runtime = new NereusKafkaRuntimeConfiguration(
                "nereus",
                "kraft",
                "broker-run",
                Duration.ofSeconds(30),
                Duration.ofSeconds(5),
                "broker-run",
                7,
                Duration.ofSeconds(30),
                Set.of(StorageProfile.OBJECT_WAL_SYNC_OBJECT));
        return new NereusKafkaObjectWalRuntimeConfiguration(
                runtime,
                streams(),
                OxiaClientConfiguration.defaults("oxia://localhost:6648"),
                objects(provider),
                Duration.ofMinutes(10),
                Duration.ofSeconds(5),
                Duration.ofHours(24),
                2);
    }

    private static StreamStorageConfig streams() {
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
                false,
                false,
                true);
    }

    private static ObjectStoreConfiguration objects(ObjectStoreProvider provider) {
        return new ObjectStoreConfiguration(
                provider.getClass().getName(),
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

    private static final class FailingObjectStoreProvider implements ObjectStoreProvider {
        private final AtomicBoolean closed = new AtomicBoolean();

        @Override
        public ObjectStore create(
                ObjectStoreConfiguration configuration,
                ObjectStoreSecretResolver secretResolver) throws Exception {
            throw new Exception("object store creation failed");
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
