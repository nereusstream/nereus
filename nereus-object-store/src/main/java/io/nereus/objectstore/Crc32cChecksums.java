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

package io.nereus.objectstore;

import io.nereus.api.Checksum;
import io.nereus.api.ChecksumType;
import java.nio.ByteBuffer;
import java.util.HexFormat;
import java.util.Locale;
import java.util.Objects;
import java.util.zip.CRC32C;

/** CRC32C helper shared by local object IO and WAL format checksums. */
public final class Crc32cChecksums {
    private Crc32cChecksums() {
    }

    public static Checksum checksum(byte[] bytes) {
        Objects.requireNonNull(bytes, "bytes");
        CRC32C crc32c = new CRC32C();
        crc32c.update(bytes, 0, bytes.length);
        return checksum((int) crc32c.getValue());
    }

    public static Checksum checksum(ByteBuffer buffer) {
        Objects.requireNonNull(buffer, "buffer");
        ByteBuffer duplicate = buffer.asReadOnlyBuffer();
        CRC32C crc32c = new CRC32C();
        crc32c.update(duplicate);
        return checksum((int) crc32c.getValue());
    }

    public static Checksum checksum(byte[] first, byte[] second) {
        Objects.requireNonNull(first, "first");
        Objects.requireNonNull(second, "second");
        CRC32C crc32c = new CRC32C();
        crc32c.update(first, 0, first.length);
        crc32c.update(second, 0, second.length);
        return checksum((int) crc32c.getValue());
    }

    public static Checksum checksum(int unsignedValue) {
        return new Checksum(ChecksumType.CRC32C, String.format(Locale.ROOT, "%08x", unsignedValue));
    }

    public static int intValue(Checksum checksum) {
        Objects.requireNonNull(checksum, "checksum");
        if (checksum.type() != ChecksumType.CRC32C) {
            throw new IllegalArgumentException("checksum must be CRC32C");
        }
        return (int) Long.parseUnsignedLong(checksum.value(), 16);
    }

    public static byte[] bytes(Checksum checksum) {
        return HexFormat.of().parseHex(checksum.value());
    }
}
