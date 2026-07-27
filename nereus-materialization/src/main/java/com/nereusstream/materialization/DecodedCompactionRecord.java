/* Licensed under the Apache License, Version 2.0 */
package com.nereusstream.materialization;

import com.nereusstream.api.Checksum;
import com.nereusstream.api.ChecksumType;
import java.nio.ByteBuffer;
import java.util.Objects;
import java.util.OptionalLong;

/**
 * Protocol-neutral compaction facts for one logical record decoded from a ranged source entry.
 */
public record DecodedCompactionRecord(
        long absoluteOffset,
        KeyKind keyKind,
        ControlKind controlKind,
        int coordinatorEpoch,
        ByteBuffer taggedCompactionKey,
        boolean tombstone,
        OptionalLong eventTimeMillis,
        OptionalLong deleteHorizonMillis,
        long sourceBatchBaseOffset,
        int sourceRecordIndex,
        Checksum sourceBatchSha256,
        boolean transactional,
        long producerId,
        short producerEpoch,
        int sequence,
        ByteBuffer rewriteToken) {
    public DecodedCompactionRecord {
        Objects.requireNonNull(keyKind, "keyKind");
        Objects.requireNonNull(controlKind, "controlKind");
        taggedCompactionKey = immutable(taggedCompactionKey, "taggedCompactionKey", false);
        eventTimeMillis = Objects.requireNonNull(eventTimeMillis, "eventTimeMillis");
        deleteHorizonMillis = Objects.requireNonNull(deleteHorizonMillis, "deleteHorizonMillis");
        Objects.requireNonNull(sourceBatchSha256, "sourceBatchSha256");
        rewriteToken = immutable(rewriteToken, "rewriteToken", false);
        if (absoluteOffset < 0
                || sourceBatchBaseOffset < 0
                || sourceRecordIndex < 0
                || sourceBatchSha256.type() != ChecksumType.SHA256
                || (eventTimeMillis.isPresent() && eventTimeMillis.getAsLong() < 0)
                || (deleteHorizonMillis.isPresent() && deleteHorizonMillis.getAsLong() < 0)) {
            throw new IllegalArgumentException("invalid ranged compaction record identity");
        }
        try {
            if (Math.addExact(sourceBatchBaseOffset, sourceRecordIndex) != absoluteOffset) {
                throw new IllegalArgumentException(
                        "source record index does not identify the absolute offset");
            }
        } catch (ArithmeticException failure) {
            throw new IllegalArgumentException("source record identity overflows", failure);
        }
        if (tombstone && keyKind != KeyKind.KEYED) {
            throw new IllegalArgumentException("only keyed data records can be tombstones");
        }
        if (keyKind == KeyKind.CONTROL && tombstone) {
            throw new IllegalArgumentException("control records cannot be tombstones");
        }
        if ((keyKind == KeyKind.CONTROL)
                != (controlKind != ControlKind.NONE && coordinatorEpoch >= 0)) {
            throw new IllegalArgumentException("invalid ranged compaction control-marker facts");
        }
        if (keyKind != KeyKind.CONTROL
                && (controlKind != ControlKind.NONE || coordinatorEpoch != -1)) {
            throw new IllegalArgumentException("data record cannot carry control-marker facts");
        }
        boolean hasProducer = producerId >= 0;
        boolean validSequence =
                keyKind == KeyKind.CONTROL ? sequence == -1 : sequence >= 0;
        if (hasProducer != (producerEpoch >= 0 && validSequence)
                || (!hasProducer && (producerId != -1 || producerEpoch != -1 || sequence != -1))
                || (transactional && !hasProducer)
                || (keyKind == KeyKind.CONTROL && (!transactional || !hasProducer))) {
            throw new IllegalArgumentException("invalid ranged compaction producer facts");
        }
    }

    @Override
    public ByteBuffer taggedCompactionKey() {
        return taggedCompactionKey.asReadOnlyBuffer();
    }

    @Override
    public ByteBuffer rewriteToken() {
        return rewriteToken.asReadOnlyBuffer();
    }

    private static ByteBuffer immutable(ByteBuffer value, String field, boolean allowEmpty) {
        ByteBuffer exact = Objects.requireNonNull(value, field).asReadOnlyBuffer();
        if (!allowEmpty && !exact.hasRemaining()) {
            throw new IllegalArgumentException(field + " cannot be empty");
        }
        byte[] bytes = new byte[exact.remaining()];
        exact.get(bytes);
        return ByteBuffer.wrap(bytes).asReadOnlyBuffer();
    }

    public enum KeyKind {
        KEYED,
        UNKEYED,
        CONTROL
    }

    public enum ControlKind {
        NONE,
        COMMIT,
        ABORT
    }
}
