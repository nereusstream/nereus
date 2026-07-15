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

import com.nereusstream.api.keys.DeterministicIds;
import java.util.Objects;

/** Base32lower-no-pad SHA-256 of the exact UTF-8 object key. */
public record ObjectKeyHash(String value) {
    private static final int SHA256_BASE32_LENGTH = 52;

    public ObjectKeyHash {
        Objects.requireNonNull(value, "value");
        if (value.length() != SHA256_BASE32_LENGTH) {
            throw new IllegalArgumentException("object key hash must be a base32 SHA-256 value");
        }
        for (int index = 0; index < value.length(); index++) {
            char character = value.charAt(index);
            if (!((character >= 'a' && character <= 'z') || (character >= '2' && character <= '7'))) {
                throw new IllegalArgumentException("object key hash must be lowercase base32 without padding");
            }
        }
    }

    public static ObjectKeyHash from(ObjectKey objectKey) {
        Objects.requireNonNull(objectKey, "objectKey");
        return new ObjectKeyHash(DeterministicIds.stableHashComponent(objectKey.value()));
    }
}
