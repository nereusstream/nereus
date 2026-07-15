/* Licensed under the Apache License, Version 2.0 */
package com.nereusstream.objectstore.compacted;

import java.nio.ByteBuffer;
import java.util.Objects;
import java.util.Optional;
import java.util.OptionalInt;
import java.util.OptionalLong;

/** One protocol-neutral row supplied to the streaming compacted-object writer. */
public record CompactedObjectRow(
        long streamOffset,
        ByteBuffer exactPayload,
        int payloadCrc32c,
        OptionalLong publishTimeMillis,
        OptionalLong eventTimeMillis,
        Optional<ByteBuffer> messageKey,
        Optional<ByteBuffer> orderingKey,
        Optional<String> schemaIdentity,
        OptionalLong producerId,
        OptionalLong producerSequenceId,
        OptionalInt batchMessageCount,
        OptionalInt sparseDisposition,
        Optional<ByteBuffer> compactionKey) {
    public CompactedObjectRow {
        if (streamOffset < 0) {
            throw new IllegalArgumentException("streamOffset must be non-negative");
        }
        exactPayload = immutable(exactPayload, "exactPayload");
        publishTimeMillis = nonNegative(publishTimeMillis, "publishTimeMillis");
        eventTimeMillis = nonNegative(eventTimeMillis, "eventTimeMillis");
        messageKey = immutableOptional(messageKey, "messageKey");
        orderingKey = immutableOptional(orderingKey, "orderingKey");
        schemaIdentity = Objects.requireNonNull(schemaIdentity, "schemaIdentity")
                .map(value -> {
                    if (value.isBlank()) {
                        throw new IllegalArgumentException("schemaIdentity cannot be blank");
                    }
                    return value;
                });
        producerId = nonNegative(producerId, "producerId");
        producerSequenceId = nonNegative(producerSequenceId, "producerSequenceId");
        batchMessageCount = positive(batchMessageCount, "batchMessageCount");
        sparseDisposition = Objects.requireNonNull(sparseDisposition, "sparseDisposition");
        compactionKey = immutableOptional(compactionKey, "compactionKey");
    }

    @Override
    public ByteBuffer exactPayload() {
        return exactPayload.asReadOnlyBuffer();
    }

    @Override
    public Optional<ByteBuffer> messageKey() {
        return duplicate(messageKey);
    }

    @Override
    public Optional<ByteBuffer> orderingKey() {
        return duplicate(orderingKey);
    }

    @Override
    public Optional<ByteBuffer> compactionKey() {
        return duplicate(compactionKey);
    }

    private static ByteBuffer immutable(ByteBuffer value, String field) {
        Objects.requireNonNull(value, field);
        return value.asReadOnlyBuffer().slice().asReadOnlyBuffer();
    }

    private static Optional<ByteBuffer> immutableOptional(Optional<ByteBuffer> value, String field) {
        Objects.requireNonNull(value, field);
        return value.map(buffer -> immutable(buffer, field));
    }

    private static Optional<ByteBuffer> duplicate(Optional<ByteBuffer> value) {
        return value.map(ByteBuffer::asReadOnlyBuffer);
    }

    private static OptionalLong nonNegative(OptionalLong value, String field) {
        Objects.requireNonNull(value, field);
        if (value.isPresent() && value.getAsLong() < 0) {
            throw new IllegalArgumentException(field + " must be non-negative");
        }
        return value;
    }

    private static OptionalInt positive(OptionalInt value, String field) {
        Objects.requireNonNull(value, field);
        if (value.isPresent() && value.getAsInt() <= 0) {
            throw new IllegalArgumentException(field + " must be positive");
        }
        return value;
    }
}
