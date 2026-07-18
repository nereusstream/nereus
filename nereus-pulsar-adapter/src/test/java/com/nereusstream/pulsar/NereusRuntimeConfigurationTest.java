/* Licensed under the Apache License, Version 2.0 */
package com.nereusstream.pulsar;

import static org.assertj.core.api.Assertions.assertThatThrownBy;

import com.nereusstream.core.StreamStorageConfig;
import com.nereusstream.managedledger.NereusManagedLedgerFactoryConfig;
import com.nereusstream.materialization.gc.PhysicalGcConfig;
import com.nereusstream.metadata.oxia.OxiaClientConfiguration;
import com.nereusstream.metadata.oxia.ProjectionMetadataStoreConfig;
import com.nereusstream.objectstore.ObjectStoreConfiguration;
import java.net.URI;
import java.time.Duration;
import java.util.Optional;
import org.junit.jupiter.api.Test;

class NereusRuntimeConfigurationTest {
    @Test
    void acceptsExactlyAlignedRuntimeLimits() {
        new NereusRuntimeConfiguration(oxia(10_000, 1_024), objectStore(Duration.ofSeconds(30)),
                stream(10_000, 10_000, 1_024, 1_024), managed(10_000, 1_024, 1_024),
                projection(1_024));
    }

    @Test
    void rejectsCommitScanMismatch() {
        assertThatThrownBy(() -> new NereusRuntimeConfiguration(
                oxia(9_999, 1_024), objectStore(Duration.ofSeconds(30)),
                stream(10_000, 10_000, 1_024, 1_024), managed(10_000, 1_024, 1_024),
                projection(1_024)))
                .isInstanceOf(IllegalArgumentException.class)
                .hasMessageContaining("maxCommitChainScan");
    }

    @Test
    void rejectsObjectTimeoutOutsideReadDeadline() {
        assertThatThrownBy(() -> new NereusRuntimeConfiguration(
                oxia(10_000, 1_024), objectStore(Duration.ofSeconds(31)),
                stream(10_000, 10_000, 1_024, 1_024), managed(10_000, 1_024, 1_024),
                projection(1_024)))
                .isInstanceOf(IllegalArgumentException.class)
                .hasMessageContaining("object-store timeout");
    }

    @Test
    void rejectsPhysicalGcClockAndCloseBudgetDrift() {
        NereusRuntimeConfiguration base = new NereusRuntimeConfiguration(
                oxia(10_000, 1_024),
                objectStore(Duration.ofSeconds(30)),
                stream(10_000, 10_000, 1_024, 1_024),
                managed(10_000, 1_024, 1_024),
                projection(1_024));

        assertThatThrownBy(() -> withPhysical(
                        base,
                        physical(
                                Duration.ofSeconds(4),
                                Duration.ofSeconds(30),
                                Duration.ofSeconds(30))))
                .isInstanceOf(IllegalArgumentException.class)
                .hasMessageContaining("maximumClockSkew");
        assertThatThrownBy(() -> withPhysical(
                        base,
                        physical(
                                Duration.ofSeconds(5),
                                Duration.ofSeconds(31),
                                Duration.ofSeconds(30))))
                .isInstanceOf(IllegalArgumentException.class)
                .hasMessageContaining("operation timeout");
    }

    private static NereusRuntimeConfiguration withPhysical(
            NereusRuntimeConfiguration base,
            PhysicalGcConfig physicalGc) {
        return new NereusRuntimeConfiguration(
                base.oxia(),
                base.objectStore(),
                base.streamStorage(),
                base.managedLedger(),
                base.projectionMetadata(),
                base.cursorMetadata(),
                base.cursorStorage(),
                base.materialization(),
                base.retention(),
                physicalGc);
    }

    private static PhysicalGcConfig physical(
            Duration maximumClockSkew,
            Duration operationTimeout,
            Duration closeTimeout) {
        PhysicalGcConfig defaults = PhysicalGcConfig.defaults();
        return new PhysicalGcConfig(
                defaults.enabled(),
                defaults.dryRun(),
                defaults.metadataScanPageSize(),
                defaults.objectListPageSize(),
                defaults.maxConcurrentDeletes(),
                defaults.maxStreamsPerCandidate(),
                defaults.maxAuthoritiesPerDomainSnapshot(),
                defaults.maxReferencesPerDomainSnapshot(),
                defaults.scanInterval(),
                defaults.readerLeaseDuration(),
                defaults.readerLeaseRenewInterval(),
                maximumClockSkew,
                defaults.drainGrace(),
                defaults.pendingProtectionDuration(),
                defaults.orphanGrace(),
                defaults.tombstoneAuditGrace(),
                operationTimeout,
                closeTimeout);
    }

    private static OxiaClientConfiguration oxia(int scan, int pending) {
        return new OxiaClientConfiguration(
                "oxia:6648", "nereus/test", Duration.ofSeconds(30), Duration.ofSeconds(30), scan, pending);
    }

    private static ObjectStoreConfiguration objectStore(Duration timeout) {
        return new ObjectStoreConfiguration(
                "com.example.Provider", URI.create("https://s3.example.com"), "us-east-1", "bucket", "prefix",
                false, timeout, 64, Optional.empty(), Optional.empty(), Optional.empty());
    }

    private static StreamStorageConfig stream(int scan, int cached, int inFlight, int retained) {
        String processRunId = "AAAAAAAAAAAAAAAAAAAAAA";
        return new StreamStorageConfig(
                "test", "pulsar-f2/" + processRunId,
                Duration.ofSeconds(30), Duration.ofSeconds(10), Duration.ofSeconds(5),
                Duration.ofSeconds(30), Duration.ofSeconds(30), Duration.ofSeconds(75),
                64, scan, 256, cached, inFlight, 64L * 1024 * 1024,
                64, 128L * 1024 * 1024, 16 * 1024 * 1024, 1,
                Duration.ofSeconds(5), true, false, true, processRunId,
                Duration.ofSeconds(5), Duration.ofMillis(100), Duration.ofSeconds(5), Duration.ofMinutes(10),
                retained, retained * 2);
    }

    private static NereusManagedLedgerFactoryConfig managed(int open, int callbacks, int retained) {
        return new NereusManagedLedgerFactoryConfig(
                "nereus", Duration.ofSeconds(30), Duration.ofSeconds(30), Duration.ofSeconds(30),
                Duration.ofSeconds(30), Duration.ofSeconds(75), Duration.ofSeconds(1),
                5 * 1024 * 1024, 100, open, callbacks, retained, 10_000);
    }

    private static ProjectionMetadataStoreConfig projection(int pending) {
        return new ProjectionMetadataStoreConfig(
                Duration.ofSeconds(30), pending, ProjectionMetadataStoreConfig.F2_MAX_VALUE_BYTES);
    }
}
