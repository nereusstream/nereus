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

import com.nereusstream.api.ObjectId;
import java.util.Objects;

/** Exact byte accounting for one full slice whose checksum domain was read and verified. */
public record WalSliceReadStats(
        ObjectId objectId,
        long objectOffset,
        long fullSlicePayloadBytes,
        long entryIndexBytes,
        long returnedPayloadBytes) {
    public WalSliceReadStats {
        Objects.requireNonNull(objectId, "objectId");
        if (objectOffset < 0 || fullSlicePayloadBytes < 0 || entryIndexBytes < 0 || returnedPayloadBytes < 0) {
            throw new IllegalArgumentException("WAL slice read byte counts must be non-negative");
        }
        if (returnedPayloadBytes > fullSlicePayloadBytes) {
            throw new IllegalArgumentException("returned payload bytes cannot exceed the full slice payload");
        }
    }

    public long amplificationBytes() {
        try {
            return Math.subtractExact(
                    Math.addExact(fullSlicePayloadBytes, entryIndexBytes),
                    returnedPayloadBytes);
        } catch (ArithmeticException e) {
            throw new IllegalStateException("WAL slice read amplification overflows", e);
        }
    }
}
