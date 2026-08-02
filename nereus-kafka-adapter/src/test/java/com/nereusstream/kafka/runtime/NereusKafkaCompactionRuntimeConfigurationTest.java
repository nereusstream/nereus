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

package com.nereusstream.kafka.runtime;

import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.assertThatThrownBy;
import com.nereusstream.api.ReadIsolation;
import com.nereusstream.api.ReadOptions;
import com.nereusstream.kafka.compaction.KafkaCompactionPartitionPass;
import com.nereusstream.kafka.compaction.KafkaCompactionTwoPassExecutor;
import java.nio.file.Path;
import java.time.Duration;
import org.junit.jupiter.api.Test;

class NereusKafkaCompactionRuntimeConfigurationTest {
    @Test
    void preservesAValidBoundedProductionConfiguration() {
        NereusKafkaCompactionRuntimeConfiguration configuration =
                configuration(Path.of("/tmp/nereus-kafka-compaction"), 4, 32, 128L << 20, 1 << 20);

        assertThat(configuration.stagingDirectory()).isEqualTo(Path.of("/tmp/nereus-kafka-compaction"));
        assertThat(configuration.maxConcurrentPartitions()).isEqualTo(4);
        assertThat(configuration.maxPartitionsPerPass()).isEqualTo(32);
        assertThat(configuration.sourceReadOptions().isolation()).isEqualTo(ReadIsolation.COMMITTED);
    }

    @Test
    void rejectsRelativeStagingPathAndCrossLimitInconsistencies() {
        assertThatThrownBy(() -> configuration(Path.of("relative/staging"), 4, 32, 128L << 20, 1 << 20))
                .isInstanceOf(IllegalArgumentException.class)
                .hasMessageContaining("absolute");

        assertThatThrownBy(() -> configuration(Path.of("/tmp/nereus-kafka-compaction"), 5, 4, 128L << 20, 1 << 20))
                .isInstanceOf(IllegalArgumentException.class)
                .hasMessageContaining("maxPartitionsPerPass");

        assertThatThrownBy(() -> configuration(Path.of("/tmp/nereus-kafka-compaction"), 4, 32, 512L << 10, 1 << 20))
                .isInstanceOf(IllegalArgumentException.class)
                .hasMessageContaining("maxStagingBytes");
    }

    private static NereusKafkaCompactionRuntimeConfiguration configuration(
            Path stagingDirectory,
            int maxConcurrentPartitions,
            int maxPartitionsPerPass,
            long maxStagingBytes,
            int uploadChunkBytes) {
        return new NereusKafkaCompactionRuntimeConfiguration(
                Duration.ofSeconds(30),
                maxConcurrentPartitions,
                maxPartitionsPerPass,
                128,
                new ReadOptions(1_024, 4 << 20, ReadIsolation.COMMITTED, Duration.ofSeconds(30)),
                1_024,
                4 << 20,
                new KafkaCompactionTwoPassExecutor.Limits(1_000_000, 250_000, 1L << 30),
                stagingDirectory,
                maxStagingBytes,
                uploadChunkBytes,
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
}
