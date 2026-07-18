/* Licensed under the Apache License, Version 2.0 */
package com.nereusstream.pulsar;

import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.assertThatThrownBy;

import com.nereusstream.core.StreamStorageConfig;
import com.nereusstream.managedledger.NereusManagedLedgerFactoryConfig;
import com.nereusstream.managedledger.retention.NereusRetentionConfig;
import com.nereusstream.metadata.oxia.OxiaClientConfiguration;
import com.nereusstream.metadata.oxia.ProjectionMetadataStoreConfig;
import com.nereusstream.objectstore.ObjectStoreConfiguration;
import java.net.URI;
import java.time.Duration;
import java.util.Optional;
import org.junit.jupiter.api.Test;

class NereusRuntimeConfigurationRetentionTest {
    @Test
    void acceptsRetentionBoundsInsideTheManagedLedgerBudget() {
        NereusRuntimeConfiguration base = base();
        NereusRetentionConfig retention = new NereusRetentionConfig(
                32,
                2,
                512,
                Duration.ofSeconds(20),
                Duration.ofSeconds(30));

        NereusRuntimeConfiguration checked = withRetention(base, retention);

        assertThat(checked.retention()).isEqualTo(retention);
    }

    @Test
    void rejectsOperationBeyondRetentionCloseBudget() {
        NereusRuntimeConfiguration base = base();
        assertThatThrownBy(() -> withRetention(
                        base,
                        new NereusRetentionConfig(
                                32,
                                2,
                                512,
                                Duration.ofSeconds(31),
                                Duration.ofSeconds(30))))
                .isInstanceOf(IllegalArgumentException.class)
                .hasMessageContaining("operation timeout");
    }

    @Test
    void rejectsCloseAndQueueBeyondManagedLedgerBudgets() {
        NereusRuntimeConfiguration base = base();
        assertThatThrownBy(() -> withRetention(
                        base,
                        new NereusRetentionConfig(
                                32,
                                2,
                                512,
                                Duration.ofSeconds(30),
                                Duration.ofSeconds(76))))
                .isInstanceOf(IllegalArgumentException.class)
                .hasMessageContaining("close timeout");
        assertThatThrownBy(() -> withRetention(
                        base,
                        new NereusRetentionConfig(
                                32,
                                2,
                                1_025,
                                Duration.ofSeconds(30),
                                Duration.ofSeconds(30))))
                .isInstanceOf(IllegalArgumentException.class)
                .hasMessageContaining("callback capacity");
    }

    private static NereusRuntimeConfiguration withRetention(
            NereusRuntimeConfiguration base,
            NereusRetentionConfig retention) {
        return new NereusRuntimeConfiguration(
                base.oxia(),
                base.objectStore(),
                base.streamStorage(),
                base.managedLedger(),
                base.projectionMetadata(),
                base.cursorMetadata(),
                base.cursorStorage(),
                base.materialization(),
                retention);
    }

    private static NereusRuntimeConfiguration base() {
        String processRunId = "AAAAAAAAAAAAAAAAAAAAAA";
        StreamStorageConfig stream = new StreamStorageConfig(
                "test",
                "pulsar-f2/" + processRunId,
                Duration.ofSeconds(30),
                Duration.ofSeconds(10),
                Duration.ofSeconds(5),
                Duration.ofSeconds(30),
                Duration.ofSeconds(30),
                Duration.ofSeconds(75),
                64,
                10_000,
                256,
                10_000,
                1_024,
                64L * 1024 * 1024,
                64,
                128L * 1024 * 1024,
                16 * 1024 * 1024,
                1,
                Duration.ofSeconds(5),
                true,
                false,
                true,
                processRunId,
                Duration.ofSeconds(5),
                Duration.ofMillis(100),
                Duration.ofSeconds(5),
                Duration.ofMinutes(10),
                1_024,
                2_048);
        NereusManagedLedgerFactoryConfig managed =
                new NereusManagedLedgerFactoryConfig(
                        "nereus",
                        Duration.ofSeconds(30),
                        Duration.ofSeconds(30),
                        Duration.ofSeconds(30),
                        Duration.ofSeconds(30),
                        Duration.ofSeconds(75),
                        Duration.ofSeconds(1),
                        5 * 1024 * 1024,
                        100,
                        10_000,
                        1_024,
                        1_024,
                        10_000);
        return new NereusRuntimeConfiguration(
                new OxiaClientConfiguration(
                        "oxia:6648",
                        "nereus/test",
                        Duration.ofSeconds(30),
                        Duration.ofSeconds(30),
                        10_000,
                        1_024),
                new ObjectStoreConfiguration(
                        "com.example.Provider",
                        URI.create("https://s3.example.com"),
                        "us-east-1",
                        "bucket",
                        "prefix",
                        false,
                        Duration.ofSeconds(30),
                        64,
                        Optional.empty(),
                        Optional.empty(),
                        Optional.empty()),
                stream,
                managed,
                new ProjectionMetadataStoreConfig(
                        Duration.ofSeconds(30),
                        1_024,
                        ProjectionMetadataStoreConfig.F2_MAX_VALUE_BYTES));
    }
}
