/*
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 *   https://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

package com.nereusstream.storage.bookkeeper;

/**
 * Exact normal-add payload allowance for BookKeeper v3, CRC32C, no flags, and no MDC request context.
 *
 * <p>The derivation mirrors release-4.18.0's length-prefixed light-protobuf request and 32-byte entry metadata plus
 * four-byte CRC32C. Ledger, entry, and transaction IDs use their maximum admitted non-negative varint width.
 */
public final class BookKeeperV3Crc32cAddPayloadLimitV1 {
    public static final int LENGTH_PREFIX_BYTES = 4;
    public static final int MASTER_KEY_BYTES = 20;
    public static final int ENTRY_METADATA_BYTES = 32;
    public static final int CRC32C_BYTES = 4;

    private static final int MAX_NON_NEGATIVE_INT64_VARINT_BYTES = 9;
    private static final int HEADER_SERIALIZED_BYTES =
            taggedVarintBytes(1) + taggedVarintBytes(1) + taggedVarintBytes(MAX_NON_NEGATIVE_INT64_VARINT_BYTES);
    private static final int HEADER_WRAPPER_BYTES = taggedMessageBytes(1, HEADER_SERIALIZED_BYTES);
    private static final int ADD_FIXED_BYTES = taggedVarintBytes(MAX_NON_NEGATIVE_INT64_VARINT_BYTES)
            + taggedVarintBytes(MAX_NON_NEGATIVE_INT64_VARINT_BYTES)
            + taggedBytesBytes(MASTER_KEY_BYTES);

    private BookKeeperV3Crc32cAddPayloadLimitV1() {}

    public static int maximumAddPayloadBytes(int clientFrameLimitBytes, int serverFrameLimitBytes) {
        if (clientFrameLimitBytes <= 0 || serverFrameLimitBytes <= 0) {
            throw new IllegalArgumentException("BookKeeper frame limits must be positive");
        }
        int frameLimit = Math.min(clientFrameLimitBytes, serverFrameLimitBytes);
        int lower = 0;
        int upper = frameLimit;
        while (lower < upper) {
            int candidate = lower + (upper - lower + 1) / 2;
            if (encodedWireFrameBytes(candidate) <= frameLimit) {
                lower = candidate;
            } else {
                upper = candidate - 1;
            }
        }
        if (lower <= 0) {
            throw new IllegalArgumentException("BookKeeper frame limit cannot carry one v3 CRC32C add payload byte");
        }
        return lower;
    }

    public static int encodedWireFrameBytes(int addPayloadBytes) {
        if (addPayloadBytes < 0) {
            throw new IllegalArgumentException("add payload bytes must be non-negative");
        }
        try {
            int digestBody = Math.addExact(addPayloadBytes, ENTRY_METADATA_BYTES + CRC32C_BYTES);
            int addSerialized = Math.addExact(ADD_FIXED_BYTES, taggedBytesBytes(digestBody));
            int requestSerialized = Math.addExact(HEADER_WRAPPER_BYTES, taggedMessageBytes(101, addSerialized));
            return Math.addExact(LENGTH_PREFIX_BYTES, requestSerialized);
        } catch (ArithmeticException failure) {
            throw new IllegalArgumentException("BookKeeper v3 add frame arithmetic overflows", failure);
        }
    }

    private static int taggedVarintBytes(int valueBytes) {
        return 1 + valueBytes;
    }

    private static int taggedBytesBytes(int valueBytes) {
        return Math.addExact(1 + unsignedVarintBytes(valueBytes), valueBytes);
    }

    private static int taggedMessageBytes(int fieldNumber, int valueBytes) {
        int tag = fieldNumber << 3 | 2;
        return Math.addExact(Math.addExact(unsignedVarintBytes(tag), unsignedVarintBytes(valueBytes)), valueBytes);
    }

    private static int unsignedVarintBytes(int value) {
        if (value < 0) {
            throw new IllegalArgumentException("varint domain must be non-negative");
        }
        int bytes = 1;
        int remaining = value;
        while ((remaining & ~0x7f) != 0) {
            bytes++;
            remaining >>>= 7;
        }
        return bytes;
    }
}
