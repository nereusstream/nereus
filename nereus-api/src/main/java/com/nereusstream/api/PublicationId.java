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

import java.util.Objects;

/** Stable base32 identity for one logical generation publication. */
public record PublicationId(String value) {
    private static final int MIN_BASE32_LENGTH_FOR_128_BITS = 26;
    private static final int MAX_LENGTH = 128;

    public PublicationId {
        Objects.requireNonNull(value, "value");
        if (value.length() < MIN_BASE32_LENGTH_FOR_128_BITS || value.length() > MAX_LENGTH) {
            throw new IllegalArgumentException("publication id must encode at least 128 bits and be at most 128 characters");
        }
        for (int index = 0; index < value.length(); index++) {
            char character = value.charAt(index);
            if (!((character >= 'a' && character <= 'z') || (character >= '2' && character <= '7'))) {
                throw new IllegalArgumentException("publication id must be lowercase base32 without padding");
            }
        }
    }
}
