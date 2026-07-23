/* Licensed under the Apache License, Version 2.0 */
package com.nereusstream.objectstore.compacted;

import com.nereusstream.api.Checksum;
import com.nereusstream.api.ChecksumType;
import java.util.Objects;

/** Exact Kafka-specific identities frozen into NTC2 metadata and policy hashes. */
public record KafkaTopicCompactedFormatSpecV2(
        String strategyId,
        long strategyVersion,
        String keyCodecId,
        String rewriteCodecId,
        Checksum messageFormatSha256,
        long sourceBatchCount,
        long outputBatchCount) {
    public KafkaTopicCompactedFormatSpecV2 {
        strategyId = requireText(strategyId, "strategyId");
        if (strategyVersion <= 0) {
            throw new IllegalArgumentException("strategyVersion must be positive");
        }
        keyCodecId = requireText(keyCodecId, "keyCodecId");
        rewriteCodecId = requireText(rewriteCodecId, "rewriteCodecId");
        Objects.requireNonNull(messageFormatSha256, "messageFormatSha256");
        if (messageFormatSha256.type() != ChecksumType.SHA256) {
            throw new IllegalArgumentException("messageFormatSha256 must use SHA256");
        }
        if (sourceBatchCount <= 0 || outputBatchCount < 0 || outputBatchCount > sourceBatchCount) {
            throw new IllegalArgumentException("NTC2 source/output batch accounting is invalid");
        }
    }

    private static String requireText(String value, String field) {
        Objects.requireNonNull(value, field);
        if (value.isBlank()) {
            throw new IllegalArgumentException(field + " cannot be blank");
        }
        return value;
    }
}
