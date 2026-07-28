/* Licensed under the Apache License, Version 2.0 */
package com.nereusstream.materialization;

import com.nereusstream.api.ReadView;
import java.util.Optional;

/** Constructs versioned built-in policies from validated operator-controlled semantic fields. */
public final class MaterializationPolicyFactory {
    public static final String LOSSLESS_COMMITTED_POLICY_ID = "nereus-committed-default";
    public static final String KAFKA_LOSSLESS_COMMITTED_POLICY_ID =
            "nereus-kafka-committed-v2";
    public static final String TOPIC_COMPACTED_POLICY_ID = "nereus-topic-compacted-default";
    public static final String KAFKA_TOPIC_COMPACTED_POLICY_ID =
            "nereus-kafka-topic-compacted-v2";

    private MaterializationPolicyFactory() {
    }

    public static MaterializationPolicy losslessCommitted(
            int minMergeSourceRanges,
            int maxSourceRanges,
            long maxRangeRecords,
            long targetObjectBytes,
            int targetRowGroupRecords,
            String compression) {
        long policyVersion = MaterializationCanonical.operatorPolicyVersion(
                minMergeSourceRanges,
                maxSourceRanges,
                maxRangeRecords,
                targetObjectBytes,
                targetRowGroupRecords,
                compression);
        return new MaterializationPolicy(
                LOSSLESS_COMMITTED_POLICY_ID,
                policyVersion,
                ReadView.COMMITTED,
                TaskKind.LOSSLESS_REWRITE,
                MaterializationPolicy.COMMITTED_FORMAT,
                minMergeSourceRanges,
                maxSourceRanges,
                maxRangeRecords,
                targetObjectBytes,
                targetRowGroupRecords,
                compression,
                Optional.empty());
    }

    public static MaterializationPolicy kafkaLosslessCommitted(
            int minMergeSourceRanges,
            int maxSourceRanges,
            long maxRangeRecords,
            long targetObjectBytes,
            int targetRowGroupRecords,
            String compression) {
        long policyVersion = MaterializationCanonical.kafkaOperatorPolicyVersion(
                minMergeSourceRanges,
                maxSourceRanges,
                maxRangeRecords,
                targetObjectBytes,
                targetRowGroupRecords,
                compression);
        return new MaterializationPolicy(
                KAFKA_LOSSLESS_COMMITTED_POLICY_ID,
                policyVersion,
                ReadView.COMMITTED,
                TaskKind.LOSSLESS_REWRITE,
                MaterializationPolicy.KAFKA_COMMITTED_FORMAT,
                minMergeSourceRanges,
                maxSourceRanges,
                maxRangeRecords,
                targetObjectBytes,
                targetRowGroupRecords,
                compression,
                Optional.empty());
    }

    public static MaterializationPolicy topicCompacted(
            TopicCompactionSpec topicCompaction,
            int minMergeSourceRanges,
            int maxSourceRanges,
            long maxRangeRecords,
            long targetObjectBytes,
            int targetRowGroupRecords,
            String compression) {
        TopicCompactionSpec exact = java.util.Objects.requireNonNull(
                topicCompaction, "topicCompaction");
        long policyVersion = MaterializationCanonical.topicOperatorPolicyVersion(
                exact,
                minMergeSourceRanges,
                maxSourceRanges,
                maxRangeRecords,
                targetObjectBytes,
                targetRowGroupRecords,
                compression);
        return new MaterializationPolicy(
                TOPIC_COMPACTED_POLICY_ID,
                policyVersion,
                ReadView.TOPIC_COMPACTED,
                TaskKind.TOPIC_KEY_COMPACTION,
                MaterializationPolicy.TOPIC_COMPACTED_FORMAT,
                minMergeSourceRanges,
                maxSourceRanges,
                maxRangeRecords,
                targetObjectBytes,
                targetRowGroupRecords,
                compression,
                Optional.of(exact));
    }

    public static MaterializationPolicy kafkaTopicCompacted(
            TopicCompactionSpec topicCompaction,
            int minMergeSourceRanges,
            int maxSourceRanges,
            long maxRangeRecords,
            long targetObjectBytes,
            int targetRowGroupRecords,
            String compression) {
        TopicCompactionSpec exact = java.util.Objects.requireNonNull(
                topicCompaction, "topicCompaction");
        long policyVersion = MaterializationCanonical.kafkaTopicOperatorPolicyVersion(
                exact,
                minMergeSourceRanges,
                maxSourceRanges,
                maxRangeRecords,
                targetObjectBytes,
                targetRowGroupRecords,
                compression);
        return new MaterializationPolicy(
                KAFKA_TOPIC_COMPACTED_POLICY_ID,
                policyVersion,
                ReadView.TOPIC_COMPACTED,
                TaskKind.TOPIC_KEY_COMPACTION,
                MaterializationPolicy.KAFKA_TOPIC_COMPACTED_FORMAT,
                minMergeSourceRanges,
                maxSourceRanges,
                maxRangeRecords,
                targetObjectBytes,
                targetRowGroupRecords,
                compression,
                Optional.of(exact));
    }
}
