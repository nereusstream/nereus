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

package com.nereusstream.api;

import java.util.Locale;
import java.util.Objects;

/** Checksum type and canonical lowercase hexadecimal value. */
public record Checksum(
        ChecksumType type,
        String value) {
    public Checksum {
        Objects.requireNonNull(type, "type");
        Objects.requireNonNull(value, "value");
        value = value.toLowerCase(Locale.ROOT);
        int expectedLength = switch (type) {
            case CRC32C -> 8;
            case SHA256 -> 64;
        };
        if (value.length() != expectedLength || !isLowerHex(value)) {
            throw new IllegalArgumentException("invalid checksum value for " + type);
        }
    }

    private static boolean isLowerHex(String value) {
        for (int i = 0; i < value.length(); i++) {
            char c = value.charAt(i);
            if (!((c >= '0' && c <= '9') || (c >= 'a' && c <= 'f'))) {
                return false;
            }
        }
        return true;
    }
}
