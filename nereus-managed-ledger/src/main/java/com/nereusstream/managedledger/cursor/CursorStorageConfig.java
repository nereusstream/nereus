/* Licensed under the Apache License, Version 2.0 */
package com.nereusstream.managedledger.cursor;

import com.nereusstream.metadata.oxia.CursorMetadataStoreConfig;
import com.nereusstream.metadata.oxia.CursorNames;
import java.time.Duration;
import java.util.Objects;

/** Frozen F3 cursor correctness, allocation, and deadline bounds. */
public record CursorStorageConfig(
        int cursorMetadataValueMaxBytes,
        int cursorMetadataSafetyMarginBytes,
        int cursorInlineAckMaxBytes,
        int cursorInlineDeltaMaxCount,
        int cursorNameMaxUtf8Bytes,
        int cursorPositionPropertiesMaxBytes,
        int cursorPropertiesMaxBytes,
        long cursorSnapshotMaxBytes,
        int cursorAckPositionsPerRequestMax,
        int cursorBatchIndexesMax,
        int cursorProtectionIntentMaxBytes,
        int cursorTrimReasonMaxUtf8Bytes,
        int cursorScanPageSize,
        int cursorRecordsPerStreamMax,
        int cursorOwnerClaimConcurrency,
        int cursorMutationQueueMax,
        int cursorMaxCasAttempts,
        int cursorHydrationMaxAttempts,
        int cursorSnapshotIdMaxAttempts,
        Duration cursorMetadataOperationTimeout,
        Duration cursorSnapshotOperationTimeout) {
    public CursorStorageConfig {
        requirePositive(cursorMetadataValueMaxBytes, "cursorMetadataValueMaxBytes");
        if (cursorMetadataValueMaxBytes != CursorMetadataStoreConfig.F3_MAX_VALUE_BYTES) {
            throw new IllegalArgumentException("cursorMetadataValueMaxBytes must equal the F3 wire limit");
        }
        requirePositive(cursorMetadataSafetyMarginBytes, "cursorMetadataSafetyMarginBytes");
        if (cursorMetadataSafetyMarginBytes >= cursorMetadataValueMaxBytes) {
            throw new IllegalArgumentException("cursorMetadataSafetyMarginBytes must be below the hard max");
        }
        int plannedRootMax = cursorMetadataValueMaxBytes - cursorMetadataSafetyMarginBytes;
        requirePositive(cursorInlineAckMaxBytes, "cursorInlineAckMaxBytes");
        if (cursorInlineAckMaxBytes > plannedRootMax) {
            throw new IllegalArgumentException("cursorInlineAckMaxBytes exceeds the margin-adjusted root max");
        }
        requirePositive(cursorInlineDeltaMaxCount, "cursorInlineDeltaMaxCount");
        requirePositive(cursorNameMaxUtf8Bytes, "cursorNameMaxUtf8Bytes");
        if (cursorNameMaxUtf8Bytes > CursorNames.MAX_CURSOR_NAME_BYTES) {
            throw new IllegalArgumentException("cursorNameMaxUtf8Bytes exceeds the F3 wire limit");
        }
        requirePositive(cursorPositionPropertiesMaxBytes, "cursorPositionPropertiesMaxBytes");
        requirePositive(cursorPropertiesMaxBytes, "cursorPropertiesMaxBytes");
        if ((long) cursorPositionPropertiesMaxBytes + cursorPropertiesMaxBytes + cursorNameMaxUtf8Bytes
                >= plannedRootMax) {
            throw new IllegalArgumentException("cursor name and property budgets cannot fit the planned root");
        }
        if (cursorSnapshotMaxBytes <= 0 || cursorSnapshotMaxBytes > 64L * 1024 * 1024) {
            throw new IllegalArgumentException("cursorSnapshotMaxBytes is outside the F3 wire limit");
        }
        requirePositive(cursorAckPositionsPerRequestMax, "cursorAckPositionsPerRequestMax");
        requirePositive(cursorBatchIndexesMax, "cursorBatchIndexesMax");
        if (cursorBatchIndexesMax > 131_072) {
            throw new IllegalArgumentException("cursorBatchIndexesMax exceeds the F3 wire limit");
        }
        requirePositive(cursorProtectionIntentMaxBytes, "cursorProtectionIntentMaxBytes");
        long largestPartialBytes = ((long) cursorBatchIndexesMax + 63) / 64 * Long.BYTES;
        if (largestPartialBytes + cursorNameMaxUtf8Bytes + 512 > cursorProtectionIntentMaxBytes) {
            throw new IllegalArgumentException("an admitted batch/name cannot fit the protection intent");
        }
        if (cursorProtectionIntentMaxBytes >= cursorMetadataValueMaxBytes) {
            throw new IllegalArgumentException("cursorProtectionIntentMaxBytes must fit one metadata value");
        }
        requirePositive(cursorTrimReasonMaxUtf8Bytes, "cursorTrimReasonMaxUtf8Bytes");
        if (cursorTrimReasonMaxUtf8Bytes + 512 >= cursorMetadataValueMaxBytes) {
            throw new IllegalArgumentException("cursor trim reason cannot fit TRIM_PENDING metadata");
        }
        requirePositive(cursorScanPageSize, "cursorScanPageSize");
        requirePositive(cursorRecordsPerStreamMax, "cursorRecordsPerStreamMax");
        if (cursorScanPageSize > cursorRecordsPerStreamMax) {
            throw new IllegalArgumentException("cursorScanPageSize exceeds cursorRecordsPerStreamMax");
        }
        requirePositive(cursorOwnerClaimConcurrency, "cursorOwnerClaimConcurrency");
        requirePositive(cursorMutationQueueMax, "cursorMutationQueueMax");
        requirePositive(cursorMaxCasAttempts, "cursorMaxCasAttempts");
        requirePositive(cursorHydrationMaxAttempts, "cursorHydrationMaxAttempts");
        requirePositive(cursorSnapshotIdMaxAttempts, "cursorSnapshotIdMaxAttempts");
        requirePositive(cursorMetadataOperationTimeout, "cursorMetadataOperationTimeout");
        requirePositive(cursorSnapshotOperationTimeout, "cursorSnapshotOperationTimeout");
    }

    public static CursorStorageConfig defaults() {
        return new CursorStorageConfig(
                65_536,
                4_096,
                8_192,
                256,
                16_384,
                8_192,
                16_384,
                67_108_864,
                1_000,
                131_072,
                49_152,
                1_024,
                256,
                10_000,
                32,
                1_024,
                32,
                8,
                8,
                Duration.ofSeconds(30),
                Duration.ofSeconds(60));
    }

    private static void requirePositive(int value, String name) {
        if (value <= 0) {
            throw new IllegalArgumentException(name + " must be positive");
        }
    }

    private static void requirePositive(Duration value, String name) {
        Objects.requireNonNull(value, name);
        if (value.isZero() || value.isNegative()) {
            throw new IllegalArgumentException(name + " must be positive");
        }
    }
}
