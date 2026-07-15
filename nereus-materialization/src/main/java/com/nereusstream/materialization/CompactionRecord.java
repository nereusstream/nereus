/* Licensed under the Apache License, Version 2.0 */
package com.nereusstream.materialization;

import java.nio.ByteBuffer;
import java.util.Objects;
import java.util.OptionalLong;

/** Protocol-neutral key/tombstone facts decoded from one exact source record without carrying payload bytes. */
public record CompactionRecord(
        long streamOffset,
        ByteBuffer compactionKey,
        CompactionDisposition disposition,
        OptionalLong publishTimeMillis,
        OptionalLong eventTimeMillis) {
    public CompactionRecord {
        if (streamOffset < 0) {
            throw new IllegalArgumentException("streamOffset must be non-negative");
        }
        Objects.requireNonNull(compactionKey, "compactionKey");
        ByteBuffer supplied = compactionKey.asReadOnlyBuffer();
        if (!supplied.hasRemaining()) {
            throw new IllegalArgumentException("compactionKey cannot be empty");
        }
        byte[] exactKey = new byte[supplied.remaining()];
        supplied.get(exactKey);
        compactionKey = ByteBuffer.wrap(exactKey).asReadOnlyBuffer();
        Objects.requireNonNull(disposition, "disposition");
        publishTimeMillis = requireNonNegative(publishTimeMillis, "publishTimeMillis");
        eventTimeMillis = requireNonNegative(eventTimeMillis, "eventTimeMillis");
    }

    @Override
    public ByteBuffer compactionKey() {
        return compactionKey.asReadOnlyBuffer();
    }

    private static OptionalLong requireNonNegative(OptionalLong value, String field) {
        Objects.requireNonNull(value, field);
        if (value.isPresent() && value.getAsLong() < 0) {
            throw new IllegalArgumentException(field + " must be non-negative");
        }
        return value;
    }
}
