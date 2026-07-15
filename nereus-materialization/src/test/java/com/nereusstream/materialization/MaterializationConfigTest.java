/* Licensed under the Apache License, Version 2.0 */
package com.nereusstream.materialization;

import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.assertThatThrownBy;

import com.nereusstream.api.ReadView;
import java.nio.file.Files;
import java.nio.file.Path;
import java.nio.file.attribute.PosixFilePermissions;
import java.time.Duration;
import java.util.Optional;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.io.TempDir;

class MaterializationConfigTest {
    @TempDir
    Path temporaryDirectory;

    @Test
    void defaultsFreezeACompleteBoundedLosslessRuntimeProfile() throws Exception {
        Path staging = privateDirectory("defaults");

        MaterializationConfig config = MaterializationConfig.defaults(staging);

        assertThat(config.committedPolicy().view()).isEqualTo(ReadView.COMMITTED);
        assertThat(config.committedPolicy().taskKind()).isEqualTo(TaskKind.LOSSLESS_REWRITE);
        assertThat(config.committedPolicy().targetPhysicalFormat())
                .isEqualTo(MaterializationPolicy.COMMITTED_FORMAT);
        assertThat(config.maxConcurrentWorkersPerStream())
                .isLessThanOrEqualTo(config.maxConcurrentWorkers());
        assertThat(config.operationTimeout().plus(config.maximumClockSkew()))
                .isLessThan(config.workerClaimDuration());
        assertThat(config.maxStagingBytes()).isGreaterThanOrEqualTo(Math.max(
                config.committedPolicy().targetObjectBytes(),
                config.recoveryCheckpointMaxBytes()));
        assertThat(config.stagingDirectory()).isEqualTo(staging.toAbsolutePath().normalize());
    }

    @Test
    void rejectsConcurrencyClaimAndLagRelationshipsThatCouldBreakRuntimeBounds() throws Exception {
        Fixture fixture = new Fixture(privateDirectory("relationships"));

        fixture.maxConcurrentWorkers = 2;
        fixture.maxConcurrentWorkersPerStream = 3;
        assertInvalid(fixture, "per-stream concurrency");

        fixture = new Fixture(privateDirectory("claim-renew"));
        fixture.claimRenew = Duration.ofSeconds(41);
        assertInvalid(fixture, "one third");

        fixture = new Fixture(privateDirectory("claim-window"));
        fixture.operationTimeout = Duration.ofSeconds(116);
        assertInvalid(fixture, "maximumClockSkew");

        fixture = new Fixture(privateDirectory("record-lag"));
        fixture.lagRejectRecords = fixture.lagThrottleRecords;
        assertInvalid(fixture, "record lag");

        fixture = new Fixture(privateDirectory("byte-lag"));
        fixture.lagRejectBytes = fixture.lagThrottleBytes;
        assertInvalid(fixture, "byte lag");
    }

    @Test
    void rejectsUnsafeStagingCheckpointAndGraceConfiguration() throws Exception {
        Fixture fixture = new Fixture(privateDirectory("staging-budget"));
        fixture.maxStagingBytes = 64L << 20;
        assertInvalid(fixture, "maxStagingBytes");

        fixture = new Fixture(privateDirectory("checkpoint-entries"));
        fixture.checkpointEntries = MaterializationConfig.MAX_RECOVERY_CHECKPOINT_ENTRIES + 1;
        assertInvalid(fixture, "recoveryCheckpointMaxEntries");

        fixture = new Fixture(privateDirectory("checkpoint-bytes"));
        fixture.checkpointBytes = MaterializationConfig.MAX_RECOVERY_CHECKPOINT_BYTES + 1;
        fixture.maxStagingBytes = fixture.checkpointBytes;
        assertInvalid(fixture, "recoveryCheckpointMaxBytes");

        fixture = new Fixture(privateDirectory("audit-grace"));
        fixture.metadataAuditGrace = Duration.ofMinutes(30);
        assertInvalid(fixture, "metadataAuditGrace");

        assertThatThrownBy(() -> new Fixture(Path.of("relative-staging")).build())
                .isInstanceOf(IllegalArgumentException.class)
                .hasMessageContaining("absolute");

        Path publicDirectory = Files.createDirectory(temporaryDirectory.resolve("public"));
        Files.setPosixFilePermissions(publicDirectory, PosixFilePermissions.fromString("rwxr-xr-x"));
        assertThatThrownBy(() -> new Fixture(publicDirectory).build())
                .isInstanceOf(IllegalArgumentException.class)
                .hasMessageContaining("0700");
    }

    @Test
    void rejectsTopicCompactionPolicyAtTheCommittedServiceBoundary() throws Exception {
        Fixture fixture = new Fixture(privateDirectory("topic-policy"));
        fixture.policy = new MaterializationPolicy(
                "topic-policy",
                1,
                ReadView.TOPIC_COMPACTED,
                TaskKind.TOPIC_KEY_COMPACTION,
                MaterializationPolicy.TOPIC_COMPACTED_FORMAT,
                2,
                16,
                1_000,
                1L << 20,
                128,
                "ZSTD",
                Optional.of(new TopicCompactionSpec("latest", 1, "pulsar-key-v1")));

        assertInvalid(fixture, "lossless COMMITTED");
    }

    private Path privateDirectory(String name) throws Exception {
        Path path = Files.createDirectory(temporaryDirectory.resolve(name));
        Files.setPosixFilePermissions(path, PosixFilePermissions.fromString("rwx------"));
        return path;
    }

    private static void assertInvalid(Fixture fixture, String message) {
        assertThatThrownBy(fixture::build)
                .isInstanceOf(IllegalArgumentException.class)
                .hasMessageContaining(message);
    }

    private static final class Fixture {
        private MaterializationPolicy policy = MaterializationPolicyFactory.losslessCommitted(
                2, 16, 10_000, 256L << 20, 1_024, "ZSTD");
        private final Path staging;
        private int maxConcurrentWorkers = 4;
        private int maxConcurrentWorkersPerStream = 1;
        private Duration claimRenew = Duration.ofSeconds(30);
        private Duration maximumClockSkew = Duration.ofSeconds(5);
        private Duration operationTimeout = Duration.ofSeconds(30);
        private long maxStagingBytes = 1L << 30;
        private long lagThrottleRecords = 100;
        private long lagRejectRecords = 200;
        private long lagThrottleBytes = 1_000;
        private long lagRejectBytes = 2_000;
        private Duration metadataAuditGrace = Duration.ofHours(2);
        private int checkpointEntries = 1_000;
        private long checkpointBytes = 512L << 20;

        private Fixture(Path staging) {
            this.staging = staging;
        }

        private MaterializationConfig build() {
            return new MaterializationConfig(
                    policy,
                    16,
                    Duration.ofSeconds(5),
                    16,
                    16,
                    16,
                    maxConcurrentWorkers,
                    maxConcurrentWorkersPerStream,
                    128,
                    1L << 20,
                    staging,
                    maxStagingBytes,
                    1 << 20,
                    Duration.ofMinutes(2),
                    claimRenew,
                    maximumClockSkew,
                    operationTimeout,
                    Duration.ofSeconds(5),
                    Duration.ofMillis(100),
                    Duration.ofSeconds(5),
                    8,
                    lagThrottleRecords,
                    lagRejectRecords,
                    lagThrottleBytes,
                    lagRejectBytes,
                    Duration.ofMinutes(5),
                    Duration.ofHours(1),
                    Duration.ofHours(1),
                    metadataAuditGrace,
                    checkpointEntries,
                    checkpointBytes);
        }
    }
}
