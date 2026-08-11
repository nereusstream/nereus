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

import java.nio.ByteBuffer;
import java.nio.CharBuffer;
import java.nio.charset.CharacterCodingException;
import java.nio.charset.CodingErrorAction;
import java.nio.charset.StandardCharsets;
import java.util.Arrays;
import java.util.Objects;

/** Strict UTF-8 bytes and their exact Java string, without normalization or replacement. */
public final class CanonicalUtf8 {
    private final String value;
    private final CanonicalBytes bytes;

    private CanonicalUtf8(String value, CanonicalBytes bytes) {
        this.value = value;
        this.bytes = bytes;
    }

    public static CanonicalUtf8 fromString(String value) {
        Objects.requireNonNull(value, "value");
        try {
            var encoder = StandardCharsets.UTF_8
                    .newEncoder()
                    .onMalformedInput(CodingErrorAction.REPORT)
                    .onUnmappableCharacter(CodingErrorAction.REPORT);
            ByteBuffer encoded = encoder.encode(CharBuffer.wrap(value));
            byte[] bytes = new byte[encoded.remaining()];
            encoded.get(bytes);
            return new CanonicalUtf8(value, CanonicalBytes.copyOf(bytes));
        } catch (CharacterCodingException e) {
            throw new IllegalArgumentException("value is not strict UTF-8 encodable", e);
        }
    }

    public static CanonicalUtf8 fromBytes(byte[] value) {
        Objects.requireNonNull(value, "value");
        byte[] copy = value.clone();
        try {
            var decoder = StandardCharsets.UTF_8
                    .newDecoder()
                    .onMalformedInput(CodingErrorAction.REPORT)
                    .onUnmappableCharacter(CodingErrorAction.REPORT);
            String decoded = decoder.decode(ByteBuffer.wrap(copy)).toString();
            CanonicalUtf8 canonical = fromString(decoded);
            if (!Arrays.equals(copy, canonical.bytes.toByteArray())) {
                throw new IllegalArgumentException("value is not canonical UTF-8");
            }
            return canonical;
        } catch (CharacterCodingException e) {
            throw new IllegalArgumentException("value is not strict UTF-8", e);
        }
    }

    public String value() {
        return value;
    }

    public CanonicalBytes bytes() {
        return bytes;
    }

    @Override
    public boolean equals(Object other) {
        return this == other || other instanceof CanonicalUtf8 that && bytes.equals(that.bytes);
    }

    @Override
    public int hashCode() {
        return bytes.hashCode();
    }

    @Override
    public String toString() {
        return value;
    }
}
