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

    @Test
    void admitsTheLargestBatchExactlyAtTheSnapshotPayloadBoundary() {
        CursorStorageConfig defaults = CursorStorageConfig.defaults();
        long largestPartialBytes = ((long) defaults.cursorBatchIndexesMax() + 63) / 64 * Long.BYTES;
        long exactSnapshotBound = largestPartialBytes + 512;

        assertThat(copy(defaults, exactSnapshotBound).cursorSnapshotMaxBytes())
                .isEqualTo(exactSnapshotBound);
        assertThatThrownBy(() -> copy(defaults, exactSnapshotBound - 1))
                .isInstanceOf(IllegalArgumentException.class)
                .hasMessageContaining("admitted batch");
    }

    private static CursorStorageConfig copy(
            CursorStorageConfig value, int metadataMax, int protectionMax) {
        return copy(value, metadataMax, protectionMax, value.cursorSnapshotMaxBytes());
    }

    private static CursorStorageConfig copy(
            CursorStorageConfig value, long snapshotMax) {
        return copy(
                value,
                value.cursorMetadataValueMaxBytes(),
                value.cursorProtectionIntentMaxBytes(),
                snapshotMax);
    }

    private static CursorStorageConfig copy(
            CursorStorageConfig value, int metadataMax, int protectionMax, long snapshotMax) {
        return new CursorStorageConfig(
                metadataMax,
                value.cursorMetadataSafetyMarginBytes(),
                value.cursorInlineAckMaxBytes(),
                value.cursorInlineDeltaMaxCount(),
                value.cursorNameMaxUtf8Bytes(),
                value.cursorPositionPropertiesMaxBytes(),
                value.cursorPropertiesMaxBytes(),
                snapshotMax,
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
