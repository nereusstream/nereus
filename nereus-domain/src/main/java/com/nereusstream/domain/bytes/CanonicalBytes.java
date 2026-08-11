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

package com.nereusstream.domain.bytes;

import java.util.Arrays;
import java.util.HexFormat;
import java.util.Objects;

/** An immutable, defensively copied canonical byte sequence. */
public final class CanonicalBytes {
    private static final CanonicalBytes EMPTY = new CanonicalBytes(new byte[0]);

    private final byte[] value;
    private final int hashCode;

    private CanonicalBytes(byte[] value) {
        this.value = value;
        this.hashCode = Arrays.hashCode(value);
    }

    public static CanonicalBytes copyOf(byte[] value) {
        Objects.requireNonNull(value, "value");
        return value.length == 0 ? EMPTY : new CanonicalBytes(value.clone());
    }

    public static CanonicalBytes empty() {
        return EMPTY;
    }

    public int length() {
        return value.length;
    }

    public boolean isEmpty() {
        return value.length == 0;
    }

    public byte[] toByteArray() {
        return value.clone();
    }

    public void copyTo(byte[] target, int offset) {
        Objects.requireNonNull(target, "target");
        Objects.checkFromIndexSize(offset, value.length, target.length);
        System.arraycopy(value, 0, target, offset, value.length);
    }

    public String toHex() {
        return HexFormat.of().formatHex(value);
    }

    @Override
    public boolean equals(Object other) {
        return this == other || other instanceof CanonicalBytes that && Arrays.equals(value, that.value);
    }

    @Override
    public int hashCode() {
        return hashCode;
    }

    @Override
    public String toString() {
        return toHex();
    }
}
