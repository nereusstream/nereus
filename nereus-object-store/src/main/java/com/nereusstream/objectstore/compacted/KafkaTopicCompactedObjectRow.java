/* Licensed under the Apache License, Version 2.0 */
package com.nereusstream.objectstore.compacted;

import com.nereusstream.api.Checksum;
import com.nereusstream.api.ChecksumType;
import java.nio.ByteBuffer;
import java.util.Objects;
import java.util.OptionalLong;

/** One retained Kafka semantic unit represented by one sparse NTC2 row. */
public record KafkaTopicCompactedObjectRow(
        long streamOffsetStart,
        int recordCount,
        KafkaCompactionDispositionV2 disposition,
        ByteBuffer compactionKey,
        ByteBuffer exactPayload,
        int payloadCrc32c,
        long sourceBatchBaseOffset,
        int sourceRecordIndex,
        Checksum sourceBatchSha256,
        OptionalLong eventTimeMillis) {
    public KafkaTopicCompactedObjectRow {
        if (streamOffsetStart < 0 || recordCount <= 0 || sourceBatchBaseOffset < 0 || sourceRecordIndex < 0) {
            throw new IllegalArgumentException("NTC2 row offset/count/source identity is invalid");
        }
        try {
            Math.addExact(streamOffsetStart, recordCount);
            Math.addExact(sourceBatchBaseOffset, sourceRecordIndex);
        } catch (ArithmeticException failure) {
            throw new IllegalArgumentException("NTC2 row range/source identity overflows", failure);
        }
        Objects.requireNonNull(disposition, "disposition");
        compactionKey = immutable(compactionKey, "compactionKey");
        exactPayload = immutable(exactPayload, "exactPayload");
        if (exactPayload.remaining() > CompactedObjectFormatV2.MAX_PAYLOAD_BYTES) {
            throw new IllegalArgumentException("NTC2 row payload exceeds 64 MiB");
        }
        Objects.requireNonNull(sourceBatchSha256, "sourceBatchSha256");
        if (sourceBatchSha256.type() != ChecksumType.SHA256) {
            throw new IllegalArgumentException("sourceBatchSha256 must use SHA256");
        }
        eventTimeMillis = Objects.requireNonNull(eventTimeMillis, "eventTimeMillis");
        if (eventTimeMillis.isPresent() && eventTimeMillis.getAsLong() < 0) {
            throw new IllegalArgumentException("eventTimeMillis must be non-negative");
        }
        KafkaCompactionKeyEncodingV2.validateForRow(compactionKey, streamOffsetStart, disposition);
        if (disposition == KafkaCompactionDispositionV2.RETAIN_TOMBSTONE && exactPayload.hasRemaining()) {
            throw new IllegalArgumentException("NTC2 tombstone payload must be empty");
        }
        if ((disposition == KafkaCompactionDispositionV2.RETAIN_VALUE
                        || disposition == KafkaCompactionDispositionV2.RETAIN_TOMBSTONE
                        || disposition == KafkaCompactionDispositionV2.RETAIN_UNKEYED)
                && recordCount != 1) {
            throw new IllegalArgumentException("ordinary NTC2 survivor rows must contain one record");
        }
    }

    @Override
    public ByteBuffer compactionKey() {
        return compactionKey.asReadOnlyBuffer();
    }

    @Override
    public ByteBuffer exactPayload() {
        return exactPayload.asReadOnlyBuffer();
    }

    public long endOffset() {
        return Math.addExact(streamOffsetStart, recordCount);
    }

    private static ByteBuffer immutable(ByteBuffer value, String field) {
        Objects.requireNonNull(value, field);
        return value.asReadOnlyBuffer().slice().asReadOnlyBuffer();
    }
}
