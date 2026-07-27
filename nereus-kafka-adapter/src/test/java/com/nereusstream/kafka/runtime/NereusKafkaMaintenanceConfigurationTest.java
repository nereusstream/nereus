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

import com.nereusstream.api.Checksum;
import com.nereusstream.api.ChecksumType;
import java.nio.file.Path;
import java.time.Duration;
import org.junit.jupiter.api.Test;

class NereusKafkaMaintenanceConfigurationTest {

    @Test
    void preservesAValidBoundedProductionConfiguration() {
        NereusKafkaMaintenanceConfiguration configuration =
                configuration(Path.of("/tmp/nereus-kafka-checkpoint"), 1L << 30, 8 << 20);

        assertThat(configuration.stagingDirectory())
                .isEqualTo(Path.of("/tmp/nereus-kafka-checkpoint"));
        assertThat(configuration.contentPolicySha256().type())
                .isEqualTo(ChecksumType.SHA256);
        assertThat(configuration.writerBuild()).isEqualTo("nereus-test");
    }

    @Test
    void rejectsUnsafeStagingAndContentIdentity() {
        assertThatThrownBy(
                        () -> configuration(
                                Path.of("relative/checkpoint"),
                                1L << 30,
                                8 << 20))
                .isInstanceOf(IllegalArgumentException.class)
                .hasMessageContaining("absolute");

        assertThatThrownBy(
                        () -> configuration(
                                Path.of("/tmp/nereus-kafka-checkpoint"),
                                64L << 10,
                                8 << 20))
                .isInstanceOf(IllegalArgumentException.class)
                .hasMessageContaining("maxStagingBytes");

        assertThatThrownBy(
                        () -> new NereusKafkaMaintenanceConfiguration(
                                Path.of("/tmp/nereus-kafka-checkpoint"),
                                1L << 30,
                                8 << 20,
                                Duration.ofHours(1),
                                Duration.ofSeconds(30),
                                Duration.ofSeconds(30),
                                Duration.ofSeconds(30),
                                Duration.ofMinutes(5),
                                new Checksum(ChecksumType.CRC32C, "00000000"),
                                "nereus-test"))
                .isInstanceOf(IllegalArgumentException.class)
                .hasMessageContaining("SHA256");
    }

    private static NereusKafkaMaintenanceConfiguration configuration(
            Path stagingDirectory, long maxStagingBytes, int uploadChunkBytes) {
        return new NereusKafkaMaintenanceConfiguration(
                stagingDirectory,
                maxStagingBytes,
                uploadChunkBytes,
                Duration.ofHours(1),
                Duration.ofSeconds(30),
                Duration.ofSeconds(30),
                Duration.ofSeconds(30),
                Duration.ofMinutes(5),
                new Checksum(ChecksumType.SHA256, "11".repeat(32)),
                "nereus-test");
    }
}
