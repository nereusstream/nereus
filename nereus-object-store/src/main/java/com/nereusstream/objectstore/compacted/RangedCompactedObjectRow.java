/* Licensed under the Apache License, Version 2.0 */
package com.nereusstream.objectstore.compacted;

import java.nio.ByteBuffer;
import java.util.Objects;
import java.util.OptionalLong;

/** One exact source entry represented by one dense NCP2 row. */
public record RangedCompactedObjectRow(
        long streamOffsetStart,
        int recordCount,
        int entryOrdinal,
        ByteBuffer exactPayload,
        int payloadCrc32c,
        OptionalLong eventTimeMillis) {
    public RangedCompactedObjectRow {
        if (streamOffsetStart < 0 || recordCount <= 0 || entryOrdinal < 0) {
            throw new IllegalArgumentException("NCP2 row offset/count/ordinal is invalid");
        }
        try {
            Math.addExact(streamOffsetStart, recordCount);
        } catch (ArithmeticException failure) {
            throw new IllegalArgumentException("NCP2 row range overflows", failure);
        }
        exactPayload = immutable(exactPayload, "exactPayload");
        if (exactPayload.remaining() > CompactedObjectFormatV2.MAX_PAYLOAD_BYTES) {
            throw new IllegalArgumentException("NCP2 row payload exceeds 64 MiB");
        }
        eventTimeMillis = Objects.requireNonNull(eventTimeMillis, "eventTimeMillis");
        if (eventTimeMillis.isPresent() && eventTimeMillis.getAsLong() < 0) {
            throw new IllegalArgumentException("eventTimeMillis must be non-negative");
        }
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
