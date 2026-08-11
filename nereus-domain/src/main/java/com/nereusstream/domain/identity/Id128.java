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

package com.nereusstream.domain.identity;

import com.nereusstream.domain.bytes.CanonicalBytes;
import java.nio.ByteBuffer;
import java.util.Objects;

/** An exact 128-bit value encoded as two big-endian longs. */
public record Id128(long highBits, long lowBits) {
    public static final int LENGTH = 16;

    public static Id128 fromBytes(byte[] value) {
        Objects.requireNonNull(value, "value");
        if (value.length != LENGTH) {
            throw new IllegalArgumentException("128-bit identity must be exactly 16 bytes");
        }
        ByteBuffer buffer = ByteBuffer.wrap(value);
        return new Id128(buffer.getLong(), buffer.getLong());
    }

    public static Id128 zero() {
        return new Id128(0, 0);
    }

    public static Id128 one() {
        return new Id128(0, 1);
    }

    public boolean isZero() {
        return highBits == 0 && lowBits == 0;
    }

    public CanonicalBytes bytes() {
        return CanonicalBytes.copyOf(
                ByteBuffer.allocate(LENGTH).putLong(highBits).putLong(lowBits).array());
    }

    public String toHex() {
        return bytes().toHex();
    }

    @Override
    public String toString() {
        return toHex();
    }
}
