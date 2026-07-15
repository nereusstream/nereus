/* Licensed under the Apache License, Version 2.0 */
package com.nereusstream.objectstore.compacted;

import java.nio.ByteBuffer;
import java.util.Objects;

/** Collision-free durable NTC1 key namespace for decoded keys and retain-exact unkeyed rows. */
public final class TopicCompactionKeyEncodingV1 {
    public static final String ID = "TAGGED_V1";

    private static final byte KEYED_TAG = 0;
    private static final byte UNKEYED_TAG = 1;
    private static final int UNKEYED_LENGTH = 1 + Long.BYTES;

    private TopicCompactionKeyEncodingV1() {
    }

    public static ByteBuffer keyed(ByteBuffer decodedKey) {
        ByteBuffer source = Objects.requireNonNull(decodedKey, "decodedKey")
                .asReadOnlyBuffer();
        if (!source.hasRemaining()) {
            throw new IllegalArgumentException("decoded topic-compaction key cannot be empty");
        }
        ByteBuffer encoded = ByteBuffer.allocate(Math.addExact(1, source.remaining()));
        encoded.put(KEYED_TAG).put(source).flip();
        return encoded.asReadOnlyBuffer();
    }

    public static ByteBuffer unkeyed(long streamOffset) {
        if (streamOffset < 0) {
            throw new IllegalArgumentException("unkeyed stream offset must be non-negative");
        }
        ByteBuffer encoded = ByteBuffer.allocate(UNKEYED_LENGTH);
        encoded.put(UNKEYED_TAG).putLong(streamOffset).flip();
        return encoded.asReadOnlyBuffer();
    }

    public static DecodedKey decode(ByteBuffer encodedKey) {
        ByteBuffer source = Objects.requireNonNull(encodedKey, "encodedKey")
                .asReadOnlyBuffer();
        if (!source.hasRemaining()) {
            throw new CompactedObjectFormatException("NTC1 tagged compaction key cannot be empty");
        }
        byte tag = source.get();
        if (tag == KEYED_TAG) {
            if (!source.hasRemaining()) {
                throw new CompactedObjectFormatException("NTC1 keyed value omits the decoded key");
            }
            byte[] key = new byte[source.remaining()];
            source.get(key);
            return new DecodedKey.Keyed(ByteBuffer.wrap(key).asReadOnlyBuffer());
        }
        if (tag == UNKEYED_TAG) {
            if (source.remaining() != Long.BYTES) {
                throw new CompactedObjectFormatException("NTC1 unkeyed value has a malformed offset key");
            }
            long offset = source.getLong();
            if (offset < 0) {
                throw new CompactedObjectFormatException("NTC1 unkeyed value has a negative offset");
            }
            return new DecodedKey.Unkeyed(offset);
        }
        throw new CompactedObjectFormatException("NTC1 compaction key has an unknown namespace tag");
    }

    public static void validateForOffset(ByteBuffer encodedKey, long streamOffset) {
        DecodedKey decoded = decode(encodedKey);
        if (decoded instanceof DecodedKey.Unkeyed unkeyed
                && unkeyed.streamOffset() != streamOffset) {
            throw new CompactedObjectFormatException(
                    "NTC1 unkeyed key does not match its row offset");
        }
    }

    public sealed interface DecodedKey permits DecodedKey.Keyed, DecodedKey.Unkeyed {
        record Keyed(ByteBuffer decodedKey) implements DecodedKey {
            public Keyed {
                ByteBuffer source = Objects.requireNonNull(decodedKey, "decodedKey")
                        .asReadOnlyBuffer();
                if (!source.hasRemaining()) {
                    throw new IllegalArgumentException("decodedKey cannot be empty");
                }
                byte[] copy = new byte[source.remaining()];
                source.get(copy);
                decodedKey = ByteBuffer.wrap(copy).asReadOnlyBuffer();
            }

            @Override
            public ByteBuffer decodedKey() {
                return decodedKey.asReadOnlyBuffer();
            }
        }

        record Unkeyed(long streamOffset) implements DecodedKey {
            public Unkeyed {
                if (streamOffset < 0) {
                    throw new IllegalArgumentException("streamOffset must be non-negative");
                }
            }
        }
    }
}
