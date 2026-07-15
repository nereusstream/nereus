/* Licensed under the Apache License, Version 2.0 */
package com.nereusstream.materialization;

import com.nereusstream.api.Checksum;
import com.nereusstream.api.ReadView;
import java.util.Objects;
import java.util.Optional;

/** Immutable policy identity copied into every planned task and publication. */
public record MaterializationPolicy(
        String policyId,
        long policyVersion,
        ReadView view,
        TaskKind taskKind,
        String targetPhysicalFormat,
        int minMergeSourceRanges,
        int maxSourceRanges,
        long maxRangeRecords,
        long targetObjectBytes,
        int targetRowGroupRecords,
        String compression,
        Optional<TopicCompactionSpec> topicCompaction) {
    public static final int MAX_SOURCE_RANGES = 128;
    public static final long MAX_RANGE_RECORDS = 1_048_576L;
    public static final long MAX_TARGET_OBJECT_BYTES = 1L << 30;
    public static final int MAX_ROW_GROUP_RECORDS = 65_536;
    public static final String COMMITTED_FORMAT = "NEREUS_COMPACTED_PARQUET_V1";
    public static final String TOPIC_COMPACTED_FORMAT = "NEREUS_TOPIC_COMPACTED_PARQUET_V1";

    public MaterializationPolicy {
        policyId = requireText(policyId, "policyId");
        if (policyVersion <= 0) {
            throw new IllegalArgumentException("policyVersion must be positive");
        }
        Objects.requireNonNull(view, "view");
        Objects.requireNonNull(taskKind, "taskKind");
        targetPhysicalFormat = requireText(targetPhysicalFormat, "targetPhysicalFormat");
        compression = requireText(compression, "compression");
        topicCompaction = Objects.requireNonNull(topicCompaction, "topicCompaction");
        if (maxSourceRanges <= 0 || maxSourceRanges > MAX_SOURCE_RANGES
                || minMergeSourceRanges < 2 || minMergeSourceRanges > maxSourceRanges) {
            throw new IllegalArgumentException("source range limits are invalid");
        }
        if (maxRangeRecords <= 0 || maxRangeRecords > MAX_RANGE_RECORDS
                || targetObjectBytes <= 0 || targetObjectBytes > MAX_TARGET_OBJECT_BYTES
                || targetRowGroupRecords <= 0 || targetRowGroupRecords > MAX_ROW_GROUP_RECORDS) {
            throw new IllegalArgumentException("materialization object/range limits are invalid");
        }
        if (!compression.equals("ZSTD") && !compression.equals("UNCOMPRESSED")) {
            throw new IllegalArgumentException("compression must be ZSTD or UNCOMPRESSED");
        }
        if (taskKind == TaskKind.LOSSLESS_REWRITE) {
            if (view != ReadView.COMMITTED
                    || !targetPhysicalFormat.equals(COMMITTED_FORMAT)
                    || topicCompaction.isPresent()) {
                throw new IllegalArgumentException("lossless policy must target the committed compacted format");
            }
        } else if (view != ReadView.TOPIC_COMPACTED
                || !targetPhysicalFormat.equals(TOPIC_COMPACTED_FORMAT)
                || topicCompaction.isEmpty()) {
            throw new IllegalArgumentException("topic-compaction policy is incomplete or view-inconsistent");
        }
    }

    public Checksum digestSha256() {
        return MaterializationCanonical.policyDigest(this);
    }

    private static String requireText(String value, String field) {
        Objects.requireNonNull(value, field);
        if (value.isBlank()) {
            throw new IllegalArgumentException(field + " cannot be blank");
        }
        return value;
    }
}
