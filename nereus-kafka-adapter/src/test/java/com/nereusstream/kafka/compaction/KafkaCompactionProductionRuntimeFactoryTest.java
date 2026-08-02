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
import com.nereusstream.api.ReadIsolation;
import com.nereusstream.api.ReadOptions;
import com.nereusstream.api.StorageProfile;
import com.nereusstream.core.physical.ObjectProtectionManager;
import com.nereusstream.core.physical.ObjectReadPinManager;
import com.nereusstream.core.read.ReadTargetReaderRegistry;
import com.nereusstream.kafka.activation.KafkaBrokerCapabilitySpecification;
import com.nereusstream.kafka.activation.KafkaStorageActivationVerifier;
import com.nereusstream.kafka.partition.KafkaPartitionStorageManager;
import com.nereusstream.kafka.runtime.NereusKafkaCompactionContext;
import com.nereusstream.kafka.runtime.NereusKafkaCompactionRuntimeConfiguration;
import com.nereusstream.metadata.oxia.GenerationMetadataStore;
import com.nereusstream.metadata.oxia.KafkaBrokerIdentity;
import com.nereusstream.metadata.oxia.KafkaCompactionPlanMetadataStore;
import com.nereusstream.metadata.oxia.KafkaPartitionMetadataStore;
import com.nereusstream.metadata.oxia.KafkaStorageActivationMetadataStore;
import com.nereusstream.metadata.oxia.OxiaMetadataStore;
import com.nereusstream.metadata.oxia.PhysicalObjectMetadataStore;
import com.nereusstream.objectstore.ObjectStore;
import com.nereusstream.objectstore.staging.StagingFileManager;
import java.lang.reflect.Proxy;
import java.nio.file.Path;
import java.time.Clock;
import java.time.Duration;
import java.util.List;
import java.util.Set;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.ScheduledThreadPoolExecutor;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.io.TempDir;

class KafkaCompactionProductionRuntimeFactoryTest {
    private static final Clock CLOCK = Clock.systemUTC();

    @Test
    void derivesTheDurableWorkerIdentityFromTheRuntimeProcessIdentity() {
        String workerId =
                KafkaCompactionProductionRuntimeFactory.workerProcessRunId("550e8400-e29b-41d4-a716-446655440000");

        assertThat(workerId).hasSize(52).matches("[a-z2-7]{52}");
        assertThat(workerId)
                .isEqualTo(KafkaCompactionProductionRuntimeFactory.workerProcessRunId(
                        "550e8400-e29b-41d4-a716-446655440000"));
    }

    @Test
    void assemblesAndStartsTheCompleteProductionGraph(@TempDir Path temporaryDirectory) {
        ScheduledThreadPoolExecutor scheduler = new ScheduledThreadPoolExecutor(1);
        CompletableFuture<Void> snapshotTaken = new CompletableFuture<>();
        Path stagingDirectory = temporaryDirectory.resolve("staging");
        StagingFileManager staging =
                new StagingFileManager(stagingDirectory, 128L << 20, 1 << 20, Duration.ofHours(1), Runnable::run);
        try {
            NereusKafkaCompactionContext context =
                    new NereusKafkaCompactionContext(configuration(stagingDirectory), (triggers, maximumPartitions) -> {
                        assertThat(maximumPartitions).isEqualTo(32);
                        snapshotTaken.complete(null);
                        return CompletableFuture.completedFuture(List.of());
                    });
            KafkaStorageActivationVerifier activationVerifier = new KafkaStorageActivationVerifier(
                    proxy(KafkaStorageActivationMetadataStore.class),
                    capability(),
                    () -> CompletableFuture.failedFuture(
                            new AssertionError("activation is not evaluated without owned work")),
                    CLOCK);

            KafkaCompactionRuntime runtime = KafkaCompactionProductionRuntimeFactory.create(
                    context,
                    "nereus",
                    "550e8400-e29b-41d4-a716-446655440000",
                    proxy(KafkaPartitionStorageManager.class),
                    proxy(OxiaMetadataStore.class),
                    proxy(GenerationMetadataStore.class),
                    proxy(PhysicalObjectMetadataStore.class),
                    proxy(KafkaPartitionMetadataStore.class),
                    proxy(KafkaCompactionPlanMetadataStore.class),
                    proxy(ObjectProtectionManager.class),
                    proxy(ObjectReadPinManager.class),
                    new ReadTargetReaderRegistry(List.of()),
                    List.of(),
                    proxy(ObjectStore.class),
                    staging,
                    activationVerifier,
                    scheduler,
                    Runnable::run,
                    CLOCK);

            runtime.start().join();
            snapshotTaken.join();
            assertThat(runtime.isRunning()).isTrue();

            runtime.closeAsync().join();
            assertThat(runtime.isRunning()).isFalse();
        } finally {
            staging.close();
            scheduler.shutdownNow();
        }
    }

    private static NereusKafkaCompactionRuntimeConfiguration configuration(Path stagingDirectory) {
        return new NereusKafkaCompactionRuntimeConfiguration(
                Duration.ofDays(1),
                4,
                32,
                128,
                new ReadOptions(1_024, 4 << 20, ReadIsolation.COMMITTED, Duration.ofSeconds(30)),
                1_024,
                4 << 20,
                new KafkaCompactionTwoPassExecutor.Limits(1_000_000, 250_000, 1L << 30),
                stagingDirectory,
                128L << 20,
                1 << 20,
                Duration.ofHours(1),
                Duration.ofSeconds(30),
                new KafkaCompactionPartitionPass.Configuration(
                        Duration.ofMinutes(2),
                        Duration.ofSeconds(30),
                        Duration.ofSeconds(5),
                        Duration.ofSeconds(10),
                        5,
                        128,
                        1_024));
    }

    private static KafkaBrokerCapabilitySpecification capability() {
        return new KafkaBrokerCapabilitySpecification(
                "kraft",
                new KafkaBrokerIdentity(1, 7),
                "runtime-1",
                "4.3.0",
                "nereus-test",
                "21",
                Set.of(StorageProfile.OBJECT_WAL_SYNC_OBJECT),
                StorageProfile.OBJECT_WAL_SYNC_OBJECT,
                bytes(1),
                bytes(2),
                bytes(3),
                Duration.ofSeconds(5),
                Duration.ofSeconds(30));
    }

    private static byte[] bytes(int seed) {
        byte[] value = new byte[32];
        for (int index = 0; index < value.length; index++) {
            value[index] = (byte) (seed + index);
        }
        return value;
    }

    @SuppressWarnings("unchecked")
    private static <T> T proxy(Class<T> type) {
        return (T) Proxy.newProxyInstance(
                type.getClassLoader(),
                new Class<?>[] {type},
                (instance, method, arguments) -> switch (method.getName()) {
                    case "close" -> null;
                    case "toString" -> type.getSimpleName();
                    case "hashCode" -> System.identityHashCode(instance);
                    case "equals" -> instance == arguments[0];
                    default ->
                        throw new AssertionError("unexpected " + type.getSimpleName() + " call: " + method.getName());
                });
    }
}
