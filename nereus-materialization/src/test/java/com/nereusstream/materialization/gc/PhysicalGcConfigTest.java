/* Licensed under the Apache License, Version 2.0 */
package com.nereusstream.materialization.gc;

import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.assertThatThrownBy;

import com.nereusstream.core.StreamStorageConfig;
import com.nereusstream.materialization.MaterializationConfig;
import java.nio.file.Path;
import java.time.Duration;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.io.TempDir;

class PhysicalGcConfigTest {
    @Test
    void defaultsAreObservationOnlyAndCrossValidate(@TempDir Path stagingDirectory) {
        PhysicalGcConfig config = PhysicalGcConfig.defaults();

        assertThat(config.enabled()).isFalse();
        assertThat(config.dryRun()).isTrue();
        assertThat(config.mutationsAllowed()).isFalse();
        assertThat(config.validateAgainst(MaterializationConfig.defaults(stagingDirectory)))
                .isSameAs(config);
        assertThat(config.validateAgainst(
                        StreamStorageConfig.defaults("cluster-a", "writer-a"),
                        MaterializationConfig.defaults(stagingDirectory)))
                .isSameAs(config);
    }

    @Test
    void mutationRequiresBothEnablementAndNonDryRun() {
        assertThat(config(true, false, Duration.ofMinutes(1), Duration.ofHours(25), Duration.ofDays(7))
                        .mutationsAllowed())
                .isTrue();
        assertThat(config(true, true, Duration.ofMinutes(1), Duration.ofHours(25), Duration.ofDays(7))
                        .mutationsAllowed())
                .isFalse();
        assertThat(config(false, false, Duration.ofMinutes(1), Duration.ofHours(25), Duration.ofDays(7))
                        .mutationsAllowed())
                .isFalse();
    }

    @Test
    void boundsAndLeaseSafetyAreFailClosed() {
        assertThatThrownBy(() -> new PhysicalGcConfig(
                        false, true, 0, 256, 4, 4_096, 10, 10,
                        Duration.ofMinutes(1), Duration.ofMinutes(2), Duration.ofSeconds(30),
                        Duration.ofSeconds(5), Duration.ofMinutes(3), Duration.ofHours(1),
                        Duration.ofHours(25), Duration.ofDays(7), Duration.ofSeconds(30),
                        Duration.ofSeconds(30)))
                .isInstanceOf(IllegalArgumentException.class)
                .hasMessageContaining("metadataScanPageSize");
        assertThatThrownBy(() -> config(
                        false, true, Duration.ofMinutes(1), Duration.ofHours(25), Duration.ofDays(7),
                        Duration.ofMinutes(2), Duration.ofSeconds(41), Duration.ofMinutes(3),
                        Duration.ofSeconds(30), Duration.ofSeconds(5)))
                .isInstanceOf(IllegalArgumentException.class)
                .hasMessageContaining("one third");
        assertThatThrownBy(() -> config(
                        false, true, Duration.ofMinutes(1), Duration.ofHours(25), Duration.ofDays(7),
                        Duration.ofMinutes(2), Duration.ofSeconds(30), Duration.ofMinutes(2),
                        Duration.ofSeconds(30), Duration.ofSeconds(5)))
                .isInstanceOf(IllegalArgumentException.class)
                .hasMessageContaining("drainGrace");
        assertThatThrownBy(() -> config(
                        false, true, Duration.ofMinutes(1), Duration.ofHours(25), Duration.ofDays(7),
                        Duration.ofMinutes(2), Duration.ofSeconds(30), Duration.ofMinutes(3),
                        Duration.ofMinutes(2), Duration.ZERO))
                .isInstanceOf(IllegalArgumentException.class)
                .hasMessageContaining("operationTimeout");
    }

    @Test
    void durationsMustBeExactlyMillisecondRepresentable() {
        assertThatThrownBy(() -> config(
                        false, true, Duration.ofNanos(1_500_000), Duration.ofHours(25), Duration.ofDays(7)))
                .isInstanceOf(IllegalArgumentException.class)
                .hasMessageContaining("millisecond");
        assertThatThrownBy(() -> config(
                        false, true, Duration.ofMinutes(1), Duration.ofHours(25), Duration.ofDays(7),
                        Duration.ofMinutes(2), Duration.ofSeconds(30), Duration.ofMinutes(3),
                        Duration.ofSeconds(30), Duration.ofNanos(1)))
                .isInstanceOf(IllegalArgumentException.class)
                .hasMessageContaining("millisecond");
    }

    @Test
    void crossValidationRejectsShortOrphanAndTombstoneWindows(@TempDir Path stagingDirectory) {
        MaterializationConfig materialization = MaterializationConfig.defaults(stagingDirectory);

        assertThatThrownBy(() -> config(
                        false, true, Duration.ofMinutes(1), Duration.ofHours(1), Duration.ofDays(7))
                        .validateAgainst(materialization))
                .isInstanceOf(IllegalArgumentException.class)
                .hasMessageContaining("orphanGrace");
        assertThatThrownBy(() -> config(
                        false, true, Duration.ofMinutes(1), Duration.ofHours(25), Duration.ofHours(48))
                        .validateAgainst(materialization))
                .isInstanceOf(IllegalArgumentException.class)
                .hasMessageContaining("metadataAuditGrace");
    }

    @Test
    void deadlineOverflowDisablesCandidateInsteadOfWrapping() {
        PhysicalGcConfig config = PhysicalGcConfig.defaults();

        assertThat(config.deadline(10, Duration.ofMillis(20))).hasValue(30);
        assertThat(config.deadline(Long.MAX_VALUE - 1, Duration.ofMillis(2))).isEmpty();
        assertThatThrownBy(() -> config.deadline(0, Duration.ofMillis(-1)))
                .isInstanceOf(IllegalArgumentException.class);
    }

    private static PhysicalGcConfig config(
            boolean enabled,
            boolean dryRun,
            Duration scanInterval,
            Duration orphanGrace,
            Duration tombstoneAuditGrace) {
        return config(
                enabled,
                dryRun,
                scanInterval,
                orphanGrace,
                tombstoneAuditGrace,
                Duration.ofMinutes(2),
                Duration.ofSeconds(30),
                Duration.ofMinutes(3),
                Duration.ofSeconds(30),
                Duration.ofSeconds(5));
    }

    private static PhysicalGcConfig config(
            boolean enabled,
            boolean dryRun,
            Duration scanInterval,
            Duration orphanGrace,
            Duration tombstoneAuditGrace,
            Duration readerLeaseDuration,
            Duration readerLeaseRenewInterval,
            Duration drainGrace,
            Duration operationTimeout,
            Duration maximumClockSkew) {
        return new PhysicalGcConfig(
                enabled,
                dryRun,
                256,
                256,
                4,
                4_096,
                10_000,
                10_000,
                scanInterval,
                readerLeaseDuration,
                readerLeaseRenewInterval,
                maximumClockSkew,
                drainGrace,
                Duration.ofHours(1),
                orphanGrace,
                tombstoneAuditGrace,
                operationTimeout,
                Duration.ofSeconds(30));
    }
}
