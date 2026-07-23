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

package com.nereusstream.bookkeeper;

import com.nereusstream.api.ApiLimits;
import com.nereusstream.api.AppendEntry;
import com.nereusstream.api.ErrorCode;
import com.nereusstream.api.NereusException;
import java.nio.ByteBuffer;
import java.nio.ByteOrder;
import java.nio.charset.StandardCharsets;
import java.util.Arrays;
import java.util.Objects;
import java.util.zip.CRC32C;

/** Closed NBKE1 framing for one ranged logical entry stored in one BookKeeper entry. */
public final class BookKeeperRangedEntryCodecV1 {
    private static final byte[] MAGIC = "NBKE1".getBytes(StandardCharsets.US_ASCII);
    private static final int HEADER_BYTES = MAGIC.length + Integer.BYTES * 3;

    private BookKeeperRangedEntryCodecV1() {
    }

    public static byte[] encode(AppendEntry entry) {
        Objects.requireNonNull(entry, "entry");
        byte[] payload = entry.payload();
        ByteBuffer encoded = ByteBuffer.allocate(Math.addExact(HEADER_BYTES, payload.length))
                .order(ByteOrder.BIG_ENDIAN);
        encoded.put(MAGIC);
        encoded.putInt(entry.recordCount());
        encoded.putInt(payload.length);
        encoded.putInt(crc32c(payload));
        encoded.put(payload);
        return encoded.array();
    }

    public static long encodedRangeBytes(long logicalBytes, int entryCount) {
        if (logicalBytes < 0 || entryCount <= 0) {
            throw new IllegalArgumentException("logicalBytes and entryCount are invalid");
        }
        return Math.addExact(logicalBytes, Math.multiplyExact((long) HEADER_BYTES, entryCount));
    }

    public static DecodedEntry decode(byte[] encoded) {
        byte[] exact = Objects.requireNonNull(encoded, "encoded");
        if (exact.length < HEADER_BYTES) {
            throw malformed("NBKE1 entry is shorter than its header");
        }
        ByteBuffer input = ByteBuffer.wrap(exact).order(ByteOrder.BIG_ENDIAN);
        byte[] magic = new byte[MAGIC.length];
        input.get(magic);
        if (!Arrays.equals(magic, MAGIC)) {
            throw malformed("NBKE1 magic does not match");
        }
        int recordCount = input.getInt();
        int payloadLength = input.getInt();
        int expectedCrc32c = input.getInt();
        if (recordCount <= 0
                || payloadLength < 0
                || payloadLength > ApiLimits.MAX_ENTRY_PAYLOAD_BYTES
                || payloadLength != input.remaining()) {
            throw malformed("NBKE1 counts or payload length are invalid");
        }
        byte[] payload = new byte[payloadLength];
        input.get(payload);
        if (crc32c(payload) != expectedCrc32c) {
            throw new NereusException(
                    ErrorCode.PRIMARY_WAL_CHECKSUM_MISMATCH,
                    false,
                    "NBKE1 payload CRC32C mismatch");
        }
        return new DecodedEntry(recordCount, payload);
    }

    private static int crc32c(byte[] payload) {
        CRC32C crc32c = new CRC32C();
        crc32c.update(payload, 0, payload.length);
        return (int) crc32c.getValue();
    }

    private static NereusException malformed(String message) {
        return new NereusException(ErrorCode.UNSUPPORTED_FORMAT, false, message);
    }

    public record DecodedEntry(int recordCount, byte[] payload) {
        public DecodedEntry {
            if (recordCount <= 0) {
                throw new IllegalArgumentException("recordCount must be positive");
            }
            payload = Objects.requireNonNull(payload, "payload").clone();
        }

        @Override
        public byte[] payload() {
            return payload.clone();
        }
    }
}
