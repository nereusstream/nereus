/* Licensed under the Apache License, Version 2.0 */
package com.nereusstream.managedledger.retention;

import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.assertThatThrownBy;

import java.time.Duration;
import org.junit.jupiter.api.Test;

class NereusRetentionConfigTest {
    @Test
    void validatesExactPolicyMatrixAndCheckedPulsarUnits() {
        assertThat(new RetentionPolicySnapshot(7, 0, 0)
                        .disablesPostConsumeRetention())
                .isTrue();
        assertThat(new RetentionPolicySnapshot(7, -1, -1)
                        .retainsIndefinitely())
                .isTrue();
        assertThat(new RetentionPolicySnapshot(7, 60_000, -1))
                .isNotNull();
        assertThat(new RetentionPolicySnapshot(7, -1, 1 << 20))
                .isNotNull();
        assertThat(RetentionPolicySnapshot.fromMinutesAndMebibytes(
                        9, 2, 3))
                .isEqualTo(new RetentionPolicySnapshot(
                        9, 120_000, 3L << 20));

        assertThatThrownBy(() -> new RetentionPolicySnapshot(1, 0, -1))
                .isInstanceOf(IllegalArgumentException.class);
        assertThatThrownBy(() -> new RetentionPolicySnapshot(1, 1, 0))
                .isInstanceOf(IllegalArgumentException.class);
        assertThatThrownBy(() -> new RetentionPolicySnapshot(1, -2, -1))
                .isInstanceOf(IllegalArgumentException.class);
        assertThatThrownBy(() ->
                        RetentionPolicySnapshot.fromMinutesAndMebibytes(
                                1, Long.MAX_VALUE, 1))
                .isInstanceOf(IllegalArgumentException.class)
                .hasMessageContaining("overflows");
    }

    @Test
    void validatesBoundedLaneConfiguration() {
        NereusRetentionConfig defaults = NereusRetentionConfig.defaults();
        assertThat(defaults.statsScanPageSize()).isBetween(1, 512);
        assertThat(defaults.maxConcurrentPlans()).isPositive();
        assertThat(defaults.maxQueuedPlans()).isPositive();

        assertThatThrownBy(() -> config(0, Duration.ofSeconds(1)))
                .isInstanceOf(IllegalArgumentException.class);
        assertThatThrownBy(() -> config(513, Duration.ofSeconds(1)))
                .isInstanceOf(IllegalArgumentException.class);
        assertThatThrownBy(() -> config(1, Duration.ofNanos(1)))
                .isInstanceOf(IllegalArgumentException.class)
                .hasMessageContaining("whole-millisecond");
        assertThatThrownBy(() -> new NereusRetentionConfig(
                        1,
                        0,
                        1,
                        Duration.ofSeconds(1),
                        Duration.ofSeconds(1)))
                .isInstanceOf(IllegalArgumentException.class);
    }

    private static NereusRetentionConfig config(
            int pageSize,
            Duration timeout) {
        return new NereusRetentionConfig(
                pageSize,
                1,
                1,
                timeout,
                Duration.ofSeconds(1));
    }
}
