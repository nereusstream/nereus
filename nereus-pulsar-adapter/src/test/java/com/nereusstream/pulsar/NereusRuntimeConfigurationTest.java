/* Licensed under the Apache License, Version 2.0 */
package com.nereusstream.pulsar;

import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.assertThatThrownBy;

import com.nereusstream.api.StorageProfile;
import com.nereusstream.bookkeeper.BookKeeperDigestType;
import com.nereusstream.bookkeeper.BookKeeperLedgerGcConfiguration;
import com.nereusstream.bookkeeper.BookKeeperSecretRef;
import com.nereusstream.bookkeeper.BookKeeperWalConfiguration;
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

    @Test
    void bookKeeperConfigurationIsExplicitBoundedAndRequiredByBookKeeperDefault() {
        NereusRuntimeConfiguration objectOnly = new NereusRuntimeConfiguration(
                oxia(10_000, 1_024),
                objectStore(Duration.ofSeconds(30)),
                stream(10_000, 10_000, 1_024, 1_024),
                managed(10_000, 1_024, 1_024),
                projection(1_024));
        NereusBookKeeperRuntimeConfiguration bookKeeper = bookKeeper("test");
        NereusManagedLedgerFactoryConfig bookKeeperDefault = managed(
                10_000,
                1_024,
                1_024,
                StorageProfile.BOOKKEEPER_WAL_SYNC_OBJECT);

        assertThatThrownBy(() -> new NereusRuntimeConfiguration(
                        objectOnly.oxia(),
                        objectOnly.objectStore(),
                        objectOnly.streamStorage(),
                        bookKeeperDefault,
                        objectOnly.projectionMetadata(),
                        objectOnly.cursorMetadata(),
                        objectOnly.cursorStorage(),
                        objectOnly.materialization(),
                        objectOnly.retention(),
                        objectOnly.physicalGc()))
                .isInstanceOf(IllegalArgumentException.class)
                .hasMessageContaining("requires BookKeeper runtime configuration");

        NereusRuntimeConfiguration configured = new NereusRuntimeConfiguration(
                objectOnly.oxia(),
                objectOnly.objectStore(),
                objectOnly.streamStorage(),
                bookKeeperDefault,
                objectOnly.projectionMetadata(),
                objectOnly.cursorMetadata(),
                objectOnly.cursorStorage(),
                objectOnly.materialization(),
                objectOnly.retention(),
                objectOnly.physicalGc(),
                Optional.of(bookKeeper));
        assertThat(configured.bookKeeper()).contains(bookKeeper);
        assertThat(configured.bookKeeper().orElseThrow().metadataStore().maxAppendRangesPerLedger())
                .isEqualTo(bookKeeper.wal().maxAppendRangesPerLedger());

        assertThatThrownBy(() -> new NereusRuntimeConfiguration(
                        objectOnly.oxia(),
                        objectOnly.objectStore(),
                        objectOnly.streamStorage(),
                        bookKeeperDefault,
                        objectOnly.projectionMetadata(),
                        objectOnly.cursorMetadata(),
                        objectOnly.cursorStorage(),
                        objectOnly.materialization(),
                        objectOnly.retention(),
                        objectOnly.physicalGc(),
                        Optional.of(bookKeeper("different-cluster"))))
                .isInstanceOf(IllegalArgumentException.class)
                .hasMessageContaining("clusterAlias");
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
        return managed(
                open,
                callbacks,
                retained,
                StorageProfile.OBJECT_WAL_SYNC_OBJECT);
    }

    private static NereusManagedLedgerFactoryConfig managed(
            int open,
            int callbacks,
            int retained,
            StorageProfile profile) {
        return new NereusManagedLedgerFactoryConfig(
                "nereus", Duration.ofSeconds(30), Duration.ofSeconds(30), Duration.ofSeconds(30),
                Duration.ofSeconds(30), Duration.ofSeconds(75), Duration.ofSeconds(1),
                5 * 1024 * 1024, 100, open, callbacks, retained, 10_000, profile);
    }

    private static ProjectionMetadataStoreConfig projection(int pending) {
        return new ProjectionMetadataStoreConfig(
                Duration.ofSeconds(30), pending, ProjectionMetadataStoreConfig.F2_MAX_VALUE_BYTES);
    }

    private static NereusBookKeeperRuntimeConfiguration bookKeeper(String cluster) {
        return new NereusBookKeeperRuntimeConfiguration(
                "deployment-1",
                new BookKeeperWalConfiguration(
                        cluster,
                        "11".repeat(32),
                        12,
                        0x801,
                        "reservation-1",
                        3,
                        3,
                        2,
                        BookKeeperDigestType.CRC32C,
                        new BookKeeperSecretRef("secret://bookkeeper/password", "v1"),
                        100_000,
                        256L * 1024 * 1024,
                        1_000,
                        8,
                        64,
                        32,
                        Duration.ofHours(1),
                        1,
                        8,
                        64L * 1024 * 1024,
                        Duration.ofSeconds(30),
                        Duration.ofSeconds(20),
                        Duration.ofSeconds(30),
                        Duration.ofSeconds(30),
                        Duration.ofMinutes(2),
                        Duration.ofSeconds(30),
                        Duration.ofMinutes(1),
                        256),
                BookKeeperLedgerGcConfiguration.safeDefault());
    }
}
