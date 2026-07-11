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

package com.nereusstream.objectstore;

import com.nereusstream.api.Checksum;
import com.nereusstream.api.ObjectKey;
import java.nio.ByteBuffer;
import java.util.Objects;
import java.util.Optional;

public record RangeReadResult(
        ObjectKey key,
        long offset,
        long length,
        ByteBuffer payload,
        Optional<Checksum> checksum) {
    public RangeReadResult {
        Objects.requireNonNull(key, "key");
        RangeChecks.requireNonNegativeNonOverflowingRange(offset, length, "range read");
        payload = Objects.requireNonNull(payload, "payload").asReadOnlyBuffer();
        checksum = Objects.requireNonNull(checksum, "checksum");
        if (payload.remaining() != length) {
            throw new IllegalArgumentException("payload remaining bytes must equal length");
        }
    }
}
