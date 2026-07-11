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

package com.nereusstream.objectstore.wal;

import com.nereusstream.api.NereusException;
import com.nereusstream.api.ErrorCode;
import java.io.ByteArrayOutputStream;
import java.nio.ByteBuffer;
import java.nio.ByteOrder;
import java.nio.charset.CharacterCodingException;
import java.nio.charset.CodingErrorAction;
import java.nio.charset.StandardCharsets;
import java.util.Map;
import java.util.Objects;

final class WalBinary {
    private WalBinary() {
    }

    static final class Writer {
        private final ByteArrayOutputStream out = new ByteArrayOutputStream();

        void int32(int value) {
            out.write(value & 0xff);
            out.write((value >>> 8) & 0xff);
            out.write((value >>> 16) & 0xff);
            out.write((value >>> 24) & 0xff);
        }

        void int64(long value) {
            for (int shift = 0; shift < Long.SIZE; shift += Byte.SIZE) {
                out.write((int) (value >>> shift) & 0xff);
            }
        }

        void bytes(byte[] bytes) {
            Objects.requireNonNull(bytes, "bytes");
            int32(bytes.length);
            raw(bytes);
        }

        void raw(byte[] bytes) {
            out.writeBytes(Objects.requireNonNull(bytes, "bytes"));
        }

        void string(String value) {
            bytes(Objects.requireNonNull(value, "value").getBytes(StandardCharsets.UTF_8));
        }

        void stringMap(Map<String, String> value) {
            int32(value.size());
            for (Map.Entry<String, String> entry : value.entrySet()) {
                string(entry.getKey());
                string(entry.getValue());
            }
        }

        byte[] toByteArray() {
            return out.toByteArray();
        }
    }

    static final class Reader {
        private final ByteBuffer buffer;

        Reader(byte[] bytes) {
            buffer = ByteBuffer.wrap(Objects.requireNonNull(bytes, "bytes")).order(ByteOrder.LITTLE_ENDIAN);
        }

        Reader(ByteBuffer buffer) {
            this.buffer = Objects.requireNonNull(buffer, "buffer").slice().order(ByteOrder.LITTLE_ENDIAN);
        }

        int int32() {
            require(Integer.BYTES);
            return buffer.getInt();
        }

        long int64() {
            require(Long.BYTES);
            return buffer.getLong();
        }

        byte[] bytes() {
            int length = int32();
            if (length < 0) {
                throw corrupt("negative byte array length");
            }
            require(length);
            byte[] bytes = new byte[length];
            buffer.get(bytes);
            return bytes;
        }

        byte[] raw(int length) {
            if (length < 0) {
                throw corrupt("negative raw byte length");
            }
            require(length);
            byte[] bytes = new byte[length];
            buffer.get(bytes);
            return bytes;
        }

        String string() {
            byte[] bytes = bytes();
            try {
                return StandardCharsets.UTF_8.newDecoder()
                        .onMalformedInput(CodingErrorAction.REPORT)
                        .onUnmappableCharacter(CodingErrorAction.REPORT)
                        .decode(ByteBuffer.wrap(bytes))
                        .toString();
            } catch (CharacterCodingException e) {
                throw corrupt("malformed UTF-8 string", e);
            }
        }

        int position() {
            return buffer.position();
        }

        int remaining() {
            return buffer.remaining();
        }

        void requireFullyConsumed() {
            if (buffer.hasRemaining()) {
                throw corrupt("trailing bytes");
            }
        }

        private void require(int bytes) {
            if (buffer.remaining() < bytes) {
                throw corrupt("truncated WAL payload");
            }
        }
    }

    static NereusException corrupt(String message) {
        return new NereusException(ErrorCode.UNSUPPORTED_FORMAT, false, message);
    }

    static NereusException corrupt(String message, Throwable cause) {
        return new NereusException(ErrorCode.UNSUPPORTED_FORMAT, false, message, cause);
    }
}
