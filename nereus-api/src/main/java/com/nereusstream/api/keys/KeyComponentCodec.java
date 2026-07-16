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

package com.nereusstream.api.keys;

import java.nio.ByteBuffer;
import java.nio.charset.CharacterCodingException;
import java.nio.charset.CodingErrorAction;
import java.nio.charset.StandardCharsets;
import java.util.Locale;
import java.util.Objects;

/** Shared durable key component encoding for Oxia paths and object keys. */
public final class KeyComponentCodec {
    private static final String ENCODED_PREFIX = "b32-";

    private KeyComponentCodec() {
    }

    public static String encodeComponent(String value) {
        Objects.requireNonNull(value, "value");
        if (isAllowedRawComponent(value)) {
            return value;
        }
        return ENCODED_PREFIX + Base32LowerNoPad.encode(value.getBytes(StandardCharsets.UTF_8));
    }

    /** Strict inverse used only by type-owned durable-key routers. */
    public static String decodeComponent(String value) {
        Objects.requireNonNull(value, "value");
        if (!value.startsWith(ENCODED_PREFIX)) {
            if (!isAllowedRawComponent(value)) {
                throw new IllegalArgumentException("raw key component is not canonical");
            }
            return value;
        }
        byte[] bytes = Base32LowerNoPad.decode(value.substring(ENCODED_PREFIX.length()));
        final String decoded;
        try {
            decoded = StandardCharsets.UTF_8
                    .newDecoder()
                    .onMalformedInput(CodingErrorAction.REPORT)
                    .onUnmappableCharacter(CodingErrorAction.REPORT)
                    .decode(ByteBuffer.wrap(bytes))
                    .toString();
        } catch (CharacterCodingException failure) {
            throw new IllegalArgumentException("encoded key component is not valid UTF-8", failure);
        }
        if (!encodeComponent(decoded).equals(value)) {
            throw new IllegalArgumentException("encoded key component is not canonical");
        }
        return decoded;
    }

    public static String encodeNonNegativeLong(long value) {
        if (value < 0) {
            throw new IllegalArgumentException("value must be non-negative");
        }
        return String.format(Locale.ROOT, "%019d", value);
    }

    public static long decodeNonNegativeLong(String value) {
        Objects.requireNonNull(value, "value");
        if (value.length() != 19) {
            throw new IllegalArgumentException("encoded long must contain exactly 19 digits");
        }
        for (int index = 0; index < value.length(); index++) {
            if (value.charAt(index) < '0' || value.charAt(index) > '9') {
                throw new IllegalArgumentException("encoded long contains a non-decimal character");
            }
        }
        final long decoded;
        try {
            decoded = Long.parseLong(value);
        } catch (NumberFormatException failure) {
            throw new IllegalArgumentException("encoded long exceeds the signed long range", failure);
        }
        if (!encodeNonNegativeLong(decoded).equals(value)) {
            throw new IllegalArgumentException("encoded long is not canonical");
        }
        return decoded;
    }

    private static boolean isAllowedRawComponent(String value) {
        if (value.isEmpty() || value.equals(".") || value.equals("..") || value.startsWith(ENCODED_PREFIX)
                || isWindowsDriveDesignator(value)) {
            return false;
        }
        for (int i = 0; i < value.length(); i++) {
            char c = value.charAt(i);
            boolean allowed = (c >= 'a' && c <= 'z')
                    || (c >= 'A' && c <= 'Z')
                    || (c >= '0' && c <= '9')
                    || c == '_'
                    || c == '-'
                    || c == '.';
            if (!allowed) {
                return false;
            }
        }
        return true;
    }

    private static boolean isWindowsDriveDesignator(String value) {
        return value.length() == 2 && value.charAt(1) == ':' && Character.isLetter(value.charAt(0));
    }
}
