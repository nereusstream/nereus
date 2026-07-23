/* Licensed under the Apache License, Version 2.0 */
package com.nereusstream.objectstore.compacted;

import java.nio.ByteBuffer;
import java.util.Objects;

/** Closed KCK2 key encoding that keeps empty keys, null keys, and control records distinct. */
public final class KafkaCompactionKeyEncodingV2 {
    public static final String ID = "KCK2";
    public static final int MAX_ENCODED_KEY_BYTES = 1 << 20;
    public static final byte KEYED_TAG = 0x01;
    public static final byte NULL_KEY_TAG = 0x02;
    public static final byte CONTROL_TAG = 0x03;

    private KafkaCompactionKeyEncodingV2() {
    }

    public static ByteBuffer keyed(ByteBuffer rawKey) {
        ByteBuffer key = Objects.requireNonNull(rawKey, "rawKey").asReadOnlyBuffer();
        if (key.remaining() >= MAX_ENCODED_KEY_BYTES) {
            throw new IllegalArgumentException("raw Kafka key is too large for KCK2");
        }
        ByteBuffer result = ByteBuffer.allocate(Math.addExact(1, key.remaining()));
        result.put(KEYED_TAG).put(key).flip();
        return result.asReadOnlyBuffer();
    }

    public static ByteBuffer nullKey(long absoluteOffset) {
        return offsetKey(NULL_KEY_TAG, absoluteOffset);
    }

    public static ByteBuffer control(long absoluteOffset) {
        return offsetKey(CONTROL_TAG, absoluteOffset);
    }

    public static void validateForRow(
            ByteBuffer encoded,
            long absoluteOffset,
            KafkaCompactionDispositionV2 disposition) {
        ByteBuffer key = Objects.requireNonNull(encoded, "encoded").asReadOnlyBuffer();
        Objects.requireNonNull(disposition, "disposition");
        if (!key.hasRemaining() || key.remaining() > MAX_ENCODED_KEY_BYTES) {
            throw new CompactedObjectFormatException("KCK2 key length is invalid");
        }
        int tag = Byte.toUnsignedInt(key.get());
        switch (disposition) {
            case RETAIN_VALUE, RETAIN_TOMBSTONE -> {
                if (tag != KEYED_TAG) {
                    throw new CompactedObjectFormatException("keyed NTC2 disposition requires KCK2 keyed tag");
                }
            }
            case RETAIN_UNKEYED -> requireOffsetIdentity(tag, NULL_KEY_TAG, key, absoluteOffset);
            case RETAIN_CONTROL -> requireOffsetIdentity(tag, CONTROL_TAG, key, absoluteOffset);
        }
    }

    private static ByteBuffer offsetKey(byte tag, long absoluteOffset) {
        if (absoluteOffset < 0) {
            throw new IllegalArgumentException("absoluteOffset must be non-negative");
        }
        return ByteBuffer.allocate(1 + Long.BYTES)
                .put(tag)
                .putLong(absoluteOffset)
                .flip()
                .asReadOnlyBuffer();
    }

    private static void requireOffsetIdentity(int actualTag, byte expectedTag, ByteBuffer key, long offset) {
        if (actualTag != Byte.toUnsignedInt(expectedTag)
                || key.remaining() != Long.BYTES
                || key.getLong() != offset) {
            throw new CompactedObjectFormatException("KCK2 offset identity does not match the NTC2 row");
        }
    }
}
