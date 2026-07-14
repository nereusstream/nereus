/* Licensed under the Apache License, Version 2.0 */
package com.nereusstream.managedledger.cursor;

import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.assertThatThrownBy;

import org.junit.jupiter.api.Test;

class CursorStorageConfigTest {
    @Test
    void defaultsAreTheFrozenF3Values() {
        CursorStorageConfig config = CursorStorageConfig.defaults();
        assertThat(config.cursorMetadataValueMaxBytes()).isEqualTo(65_536);
        assertThat(config.cursorMetadataSafetyMarginBytes()).isEqualTo(4_096);
        assertThat(config.cursorInlineAckMaxBytes()).isEqualTo(8_192);
        assertThat(config.cursorSnapshotMaxBytes()).isEqualTo(67_108_864);
        assertThat(config.cursorRecordsPerStreamMax()).isEqualTo(10_000);
        assertThat(config.cursorMetadataOperationTimeout()).isEqualTo(java.time.Duration.ofSeconds(30));
        assertThat(config.cursorSnapshotOperationTimeout()).isEqualTo(java.time.Duration.ofSeconds(60));
    }

    @Test
    void rejectsCrossFieldShapesThatCannotFitOneRootOrProtectionIntent() {
        CursorStorageConfig defaults = CursorStorageConfig.defaults();
        assertThatThrownBy(() -> copy(defaults, 65_535, defaults.cursorProtectionIntentMaxBytes()))
                .isInstanceOf(IllegalArgumentException.class)
                .hasMessageContaining("wire limit");
        assertThatThrownBy(() -> copy(defaults, 65_536, 16_000))
                .isInstanceOf(IllegalArgumentException.class)
                .hasMessageContaining("protection intent");
    }

    private static CursorStorageConfig copy(
            CursorStorageConfig value, int metadataMax, int protectionMax) {
        return new CursorStorageConfig(
                metadataMax,
                value.cursorMetadataSafetyMarginBytes(),
                value.cursorInlineAckMaxBytes(),
                value.cursorInlineDeltaMaxCount(),
                value.cursorNameMaxUtf8Bytes(),
                value.cursorPositionPropertiesMaxBytes(),
                value.cursorPropertiesMaxBytes(),
                value.cursorSnapshotMaxBytes(),
                value.cursorAckPositionsPerRequestMax(),
                value.cursorBatchIndexesMax(),
                protectionMax,
                value.cursorTrimReasonMaxUtf8Bytes(),
                value.cursorScanPageSize(),
                value.cursorRecordsPerStreamMax(),
                value.cursorOwnerClaimConcurrency(),
                value.cursorMutationQueueMax(),
                value.cursorMaxCasAttempts(),
                value.cursorHydrationMaxAttempts(),
                value.cursorSnapshotIdMaxAttempts(),
                value.cursorMetadataOperationTimeout(),
                value.cursorSnapshotOperationTimeout());
    }
}
