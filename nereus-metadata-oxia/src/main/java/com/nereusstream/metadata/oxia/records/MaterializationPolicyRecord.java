/* Licensed under the Apache License, Version 2.0 */
package com.nereusstream.metadata.oxia.records;

/** Complete immutable policy snapshot embedded in every durable materialization task. */
public record MaterializationPolicyRecord(
        String policyId,
        long policyVersion,
        int readViewId,
        int taskKindId,
        String targetPhysicalFormat,
        int minMergeSourceRanges,
        int maxSourceRanges,
        long maxRangeRecords,
        long targetObjectBytes,
        int targetRowGroupRecords,
        String compression,
        String topicStrategyId,
        long topicStrategyVersion,
        String topicKeyCodecId) {
    public static final int MAX_SOURCE_RANGES = 128;
    public static final long MAX_RANGE_RECORDS = 1_048_576L;
    public static final long MAX_TARGET_OBJECT_BYTES = 1L << 30;
    public static final int MAX_ROW_GROUP_RECORDS = 65_536;

    public MaterializationPolicyRecord {
        policyId = F4RecordValidation.requireText(policyId, "policyId");
        F4RecordValidation.requirePositive(policyVersion, "policyVersion");
        if (readViewId != 1 && readViewId != 2) {
            throw new IllegalArgumentException("readViewId must identify a known view");
        }
        if (taskKindId != 1 && taskKindId != 2) {
            throw new IllegalArgumentException("taskKindId is unknown");
        }
        targetPhysicalFormat = F4RecordValidation.requireText(
                targetPhysicalFormat, "targetPhysicalFormat");
        if (maxSourceRanges <= 0
                || maxSourceRanges > MAX_SOURCE_RANGES
                || minMergeSourceRanges < 2
                || minMergeSourceRanges > maxSourceRanges) {
            throw new IllegalArgumentException("source range limits are invalid");
        }
        if (maxRangeRecords <= 0
                || maxRangeRecords > MAX_RANGE_RECORDS
                || targetObjectBytes <= 0
                || targetObjectBytes > MAX_TARGET_OBJECT_BYTES
                || targetRowGroupRecords <= 0
                || targetRowGroupRecords > MAX_ROW_GROUP_RECORDS) {
            throw new IllegalArgumentException("materialization object/range limits are invalid");
        }
        compression = F4RecordValidation.requireText(compression, "compression");
        if (!compression.equals("ZSTD") && !compression.equals("UNCOMPRESSED")) {
            throw new IllegalArgumentException("compression must be ZSTD or UNCOMPRESSED");
        }
        topicStrategyId = F4RecordValidation.requireOptionalText(
                topicStrategyId, "topicStrategyId", 4096);
        F4RecordValidation.requireNonNegative(topicStrategyVersion, "topicStrategyVersion");
        topicKeyCodecId = F4RecordValidation.requireOptionalText(
                topicKeyCodecId, "topicKeyCodecId", 4096);
        if (taskKindId == 1) {
            if (readViewId != 1
                    || !targetPhysicalFormat.equals("NEREUS_COMPACTED_PARQUET_V1")
                    || !topicStrategyId.isEmpty()
                    || topicStrategyVersion != 0
                    || !topicKeyCodecId.isEmpty()) {
                throw new IllegalArgumentException("lossless policy snapshot is view/format inconsistent");
            }
        } else if (readViewId != 2
                || !targetPhysicalFormat.equals("NEREUS_TOPIC_COMPACTED_PARQUET_V1")
                || topicStrategyId.isEmpty()
                || topicStrategyVersion <= 0
                || topicKeyCodecId.isEmpty()) {
            throw new IllegalArgumentException("topic-compaction policy snapshot is incomplete");
        }
    }
}
