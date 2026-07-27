/* Licensed under the Apache License, Version 2.0 */
package com.nereusstream.materialization;

import com.nereusstream.api.Checksum;
import com.nereusstream.api.ChecksumType;
import java.nio.ByteBuffer;
import java.util.Objects;
import java.util.OptionalLong;
import java.util.zip.CRC32C;

/** One sparse survivor row before mapping to a protocol-specific compacted-object schema. */
public record RewrittenCompactionRecord(
        long absoluteOffset,
        Disposition disposition,
        ByteBuffer taggedCompactionKey,
        ByteBuffer exactPayload,
        int payloadCrc32c,
        long sourceBatchBaseOffset,
        int sourceRecordIndex,
        Checksum sourceBatchSha256,
        OptionalLong eventTimeMillis) {
    public RewrittenCompactionRecord {
        Objects.requireNonNull(disposition, "disposition");
        taggedCompactionKey = immutable(taggedCompactionKey, "taggedCompactionKey");
        exactPayload = immutable(exactPayload, "exactPayload");
        Objects.requireNonNull(sourceBatchSha256, "sourceBatchSha256");
        eventTimeMillis = Objects.requireNonNull(eventTimeMillis, "eventTimeMillis");
        if (absoluteOffset < 0
                || sourceBatchBaseOffset < 0
                || sourceRecordIndex < 0
                || sourceBatchSha256.type() != ChecksumType.SHA256
                || (eventTimeMillis.isPresent() && eventTimeMillis.getAsLong() < 0)) {
            throw new IllegalArgumentException("invalid rewritten compaction record identity");
        }
        try {
            if (Math.addExact(sourceBatchBaseOffset, sourceRecordIndex) != absoluteOffset) {
                throw new IllegalArgumentException(
                        "rewritten source record index does not identify the absolute offset");
            }
        } catch (ArithmeticException failure) {
            throw new IllegalArgumentException("rewritten source record identity overflows", failure);
        }
        CRC32C crc32c = new CRC32C();
        ByteBuffer payload = exactPayload.asReadOnlyBuffer();
        crc32c.update(payload);
        if ((int) crc32c.getValue() != payloadCrc32c) {
            throw new IllegalArgumentException(
                    "rewritten compaction payload CRC32C does not match exact bytes");
        }
    }

    @Override
    public ByteBuffer taggedCompactionKey() {
        return taggedCompactionKey.asReadOnlyBuffer();
    }

    @Override
    public ByteBuffer exactPayload() {
        return exactPayload.asReadOnlyBuffer();
    }

    private static ByteBuffer immutable(ByteBuffer value, String field) {
        ByteBuffer exact = Objects.requireNonNull(value, field).asReadOnlyBuffer();
        if (!exact.hasRemaining()) {
            throw new IllegalArgumentException(field + " cannot be empty");
        }
        byte[] bytes = new byte[exact.remaining()];
        exact.get(bytes);
        return ByteBuffer.wrap(bytes).asReadOnlyBuffer();
    }

    public enum Disposition {
        RETAIN_VALUE,
        RETAIN_TOMBSTONE,
        RETAIN_UNKEYED,
        RETAIN_CONTROL
    }
}
