/* Licensed under the Apache License, Version 2.0 */
package com.nereusstream.pulsar;

import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.assertThatThrownBy;

import com.nereusstream.core.StreamStorageConfig;
import com.nereusstream.managedledger.NereusManagedLedgerFactoryConfig;
import com.nereusstream.managedledger.cursor.CursorStorageConfig;
import com.nereusstream.metadata.oxia.CursorMetadataStoreConfig;
import com.nereusstream.metadata.oxia.OxiaClientConfiguration;
import com.nereusstream.metadata.oxia.ProjectionMetadataStoreConfig;
import com.nereusstream.objectstore.ObjectStoreConfiguration;
import java.net.URI;
import java.time.Duration;
import java.util.Optional;
import org.junit.jupiter.api.Test;

class NereusRuntimeConfigurationCursorTest {
    @Test
    void fiveArgumentBridgeUsesFrozenCursorDefaults() {
        NereusRuntimeConfiguration configuration = new NereusRuntimeConfiguration(
                oxia(), objectStore(), stream(), managed(), projection());

        assertThat(configuration.cursorMetadata()).isEqualTo(CursorMetadataStoreConfig.defaults());
        assertThat(configuration.cursorStorage()).isEqualTo(CursorStorageConfig.defaults());
    }

    @Test
    void canonicalConfigurationAcceptsAlignedCursorLimits() {
        NereusRuntimeConfiguration configuration = new NereusRuntimeConfiguration(
                oxia(),
                objectStore(),
                stream(),
                managed(),
                projection(),
                CursorMetadataStoreConfig.defaults(),
                CursorStorageConfig.defaults());

        assertThat(configuration.cursorStorage().cursorScanPageSize()).isEqualTo(256);
    }

    @Test
    void rejectsCursorMetadataDeadlineThatDiffersFromSharedOxia() {
        CursorMetadataStoreConfig cursorMetadata = new CursorMetadataStoreConfig(
                Duration.ofSeconds(29), 1_024, CursorMetadataStoreConfig.F3_MAX_VALUE_BYTES, 256);

        assertThatThrownBy(() -> configuration(cursorMetadata, CursorStorageConfig.defaults()))
                .isInstanceOf(IllegalArgumentException.class)
                .hasMessageContaining("metadata operation deadlines");
    }

    @Test
    void rejectsCursorScanPageBeyondMetadataAdapterLimit() {
        CursorMetadataStoreConfig cursorMetadata = new CursorMetadataStoreConfig(
                Duration.ofSeconds(30), 1_024, CursorMetadataStoreConfig.F3_MAX_VALUE_BYTES, 128);

        assertThatThrownBy(() -> configuration(cursorMetadata, CursorStorageConfig.defaults()))
                .isInstanceOf(IllegalArgumentException.class)
                .hasMessageContaining("cursor scan page");
    }

    @Test
    void rejectsSnapshotDeadlineBeyondManagedLedgerCloseBudget() {
        CursorStorageConfig defaults = CursorStorageConfig.defaults();
        CursorStorageConfig tooSlow = copyWithSnapshotTimeout(defaults, Duration.ofSeconds(76));

        assertThatThrownBy(() -> configuration(CursorMetadataStoreConfig.defaults(), tooSlow))
                .isInstanceOf(IllegalArgumentException.class)
                .hasMessageContaining("snapshot timeout");
    }

    private static NereusRuntimeConfiguration configuration(
            CursorMetadataStoreConfig cursorMetadata,
            CursorStorageConfig cursorStorage) {
        return new NereusRuntimeConfiguration(
                oxia(), objectStore(), stream(), managed(), projection(), cursorMetadata, cursorStorage);
    }

    private static CursorStorageConfig copyWithSnapshotTimeout(
            CursorStorageConfig source,
            Duration snapshotTimeout) {
        return new CursorStorageConfig(
                source.cursorMetadataValueMaxBytes(),
                source.cursorMetadataSafetyMarginBytes(),
                source.cursorInlineAckMaxBytes(),
                source.cursorInlineDeltaMaxCount(),
                source.cursorNameMaxUtf8Bytes(),
                source.cursorPositionPropertiesMaxBytes(),
                source.cursorPropertiesMaxBytes(),
                source.cursorSnapshotMaxBytes(),
                source.cursorAckPositionsPerRequestMax(),
                source.cursorBatchIndexesMax(),
                source.cursorProtectionIntentMaxBytes(),
                source.cursorTrimReasonMaxUtf8Bytes(),
                source.cursorScanPageSize(),
                source.cursorRecordsPerStreamMax(),
                source.cursorOwnerClaimConcurrency(),
                source.cursorMutationQueueMax(),
                source.cursorMaxCasAttempts(),
                source.cursorHydrationMaxAttempts(),
                source.cursorSnapshotIdMaxAttempts(),
                source.cursorMetadataOperationTimeout(),
                snapshotTimeout);
    }

    private static OxiaClientConfiguration oxia() {
        return new OxiaClientConfiguration(
                "oxia:6648",
                "nereus/test",
                Duration.ofSeconds(30),
                Duration.ofSeconds(30),
                10_000,
                1_024);
    }

    private static ObjectStoreConfiguration objectStore() {
        return new ObjectStoreConfiguration(
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
                Optional.empty());
    }

    private static StreamStorageConfig stream() {
        String processRunId = "AAAAAAAAAAAAAAAAAAAAAA";
        return new StreamStorageConfig(
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
    }

    private static NereusManagedLedgerFactoryConfig managed() {
        return new NereusManagedLedgerFactoryConfig(
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
    }

    private static ProjectionMetadataStoreConfig projection() {
        return new ProjectionMetadataStoreConfig(
                Duration.ofSeconds(30),
                1_024,
                ProjectionMetadataStoreConfig.F2_MAX_VALUE_BYTES);
    }
}
