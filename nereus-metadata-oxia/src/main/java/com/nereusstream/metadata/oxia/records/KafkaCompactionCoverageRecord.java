/* Licensed under the Apache License, Version 2.0 */
package com.nereusstream.metadata.oxia.records;

import java.util.Arrays;

public record KafkaCompactionCoverageRecord(
        int coverageVersion,
        long startOffset,
        long endOffset,
        long activationEpoch,
        byte[] generationSetSha256,
        byte[] policySha256,
        long activatedAtMillis) {
    public static final KafkaCompactionCoverageRecord EMPTY =
            new KafkaCompactionCoverageRecord(0, 0, 0, 0, new byte[0], new byte[0], 0);

    public KafkaCompactionCoverageRecord {
        generationSetSha256 = KafkaMetadataValidation.sha256(
                generationSetSha256, "generationSetSha256", coverageVersion == 0);
        policySha256 = KafkaMetadataValidation.sha256(policySha256, "policySha256", coverageVersion == 0);
        if (coverageVersion == 0) {
            if (startOffset != 0 || endOffset != 0 || activationEpoch != 0
                    || generationSetSha256.length != 0 || policySha256.length != 0 || activatedAtMillis != 0) {
                throw new IllegalArgumentException("empty compaction coverage must contain zero fields");
            }
        } else if (coverageVersion != 1 || startOffset < 0 || endOffset <= startOffset
                || activationEpoch <= 0 || activatedAtMillis <= 0) {
            throw new IllegalArgumentException("invalid Kafka compaction coverage");
        }
    }

    @Override public byte[] generationSetSha256() { return generationSetSha256.clone(); }
    @Override public byte[] policySha256() { return policySha256.clone(); }

    @Override
    public boolean equals(Object other) {
        return this == other || other instanceof KafkaCompactionCoverageRecord that
                && coverageVersion == that.coverageVersion && startOffset == that.startOffset
                && endOffset == that.endOffset && activationEpoch == that.activationEpoch
                && activatedAtMillis == that.activatedAtMillis
                && Arrays.equals(generationSetSha256, that.generationSetSha256)
                && Arrays.equals(policySha256, that.policySha256);
    }

    @Override
    public int hashCode() {
        int result = java.util.Objects.hash(
                coverageVersion, startOffset, endOffset, activationEpoch, activatedAtMillis);
        result = 31 * result + Arrays.hashCode(generationSetSha256);
        return 31 * result + Arrays.hashCode(policySha256);
    }
}
