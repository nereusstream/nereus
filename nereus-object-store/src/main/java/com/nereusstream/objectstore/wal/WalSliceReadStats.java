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

/** Resolved-target identity plus exact physical/logical byte accounting for one verified read. */
public record WalSliceReadStats(
        ObjectId objectId,
        long objectOffset,
        long fullSlicePayloadBytes,
        long entryIndexBytes,
        long downloadedPayloadBytes,
        long downloadedEntryIndexBytes,
        long returnedPayloadBytes) {
    public WalSliceReadStats(
            ObjectId objectId,
            long objectOffset,
            long fullSlicePayloadBytes,
            long entryIndexBytes,
            long returnedPayloadBytes) {
        this(
                objectId,
                objectOffset,
                fullSlicePayloadBytes,
                entryIndexBytes,
                fullSlicePayloadBytes,
                entryIndexBytes,
                returnedPayloadBytes);
    }

    public WalSliceReadStats {
        Objects.requireNonNull(objectId, "objectId");
        if (objectOffset < 0
                || fullSlicePayloadBytes < 0
                || entryIndexBytes < 0
                || downloadedPayloadBytes < 0
                || downloadedEntryIndexBytes < 0
                || returnedPayloadBytes < 0) {
            throw new IllegalArgumentException("read byte counts must be non-negative");
        }
    }

    public long physicalBytesRead() {
        try {
            return Math.addExact(downloadedPayloadBytes, downloadedEntryIndexBytes);
        } catch (ArithmeticException e) {
            throw new IllegalStateException("physical read byte accounting overflows", e);
        }
    }

    public long ioDeltaBytes() {
        try {
            return Math.subtractExact(physicalBytesRead(), returnedPayloadBytes);
        } catch (ArithmeticException e) {
            throw new IllegalStateException("read IO delta overflows", e);
        }
    }

    public long amplificationBytes() {
        return Math.max(0, ioDeltaBytes());
    }

    public long compressionSavingsBytes() {
        long delta = ioDeltaBytes();
        return delta < 0 ? Math.negateExact(delta) : 0;
    }
}
