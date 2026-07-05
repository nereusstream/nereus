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

package io.nereus.api.keys;

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

    public static String encodeNonNegativeLong(long value) {
        if (value < 0) {
            throw new IllegalArgumentException("value must be non-negative");
        }
        return String.format(Locale.ROOT, "%019d", value);
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
