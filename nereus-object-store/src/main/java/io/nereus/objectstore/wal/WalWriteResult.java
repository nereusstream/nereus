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

package io.nereus.objectstore.wal;

import io.nereus.api.Checksum;
import io.nereus.api.ObjectId;
import io.nereus.api.ObjectKey;
import java.util.List;
import java.util.Objects;

public record WalWriteResult(
        ObjectId objectId,
        ObjectKey objectKey,
        long objectLength,
        Checksum objectChecksum,
        Checksum storageChecksum,
        int formatMajorVersion,
        int formatMinorVersion,
        String writerVersion,
        long createdAtMillis,
        List<WrittenStreamSlice> slices) {
    public WalWriteResult {
        Objects.requireNonNull(objectId, "objectId");
        Objects.requireNonNull(objectKey, "objectKey");
        Objects.requireNonNull(objectChecksum, "objectChecksum");
        Objects.requireNonNull(storageChecksum, "storageChecksum");
        Objects.requireNonNull(writerVersion, "writerVersion");
        slices = List.copyOf(Objects.requireNonNull(slices, "slices"));
        if (objectLength <= 0) {
            throw new IllegalArgumentException("objectLength must be positive");
        }
        if (formatMajorVersion < 0 || formatMinorVersion < 0 || createdAtMillis < 0) {
            throw new IllegalArgumentException("WAL format versions and createdAtMillis must be non-negative");
        }
        if (writerVersion.isBlank()) {
            throw new IllegalArgumentException("writerVersion cannot be blank");
        }
        if (slices.isEmpty()) {
            throw new IllegalArgumentException("slices cannot be empty");
        }
    }
}
