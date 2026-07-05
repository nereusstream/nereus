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

package io.nereus.api;

import java.util.Objects;
import java.util.Optional;

/** Committed reference to entry-index bytes for a stream slice. */
public record EntryIndexRef(
        EntryIndexLocation location,
        Optional<ObjectId> objectId,
        Optional<ObjectKey> objectKey,
        Optional<byte[]> inlineData,
        long offset,
        long length,
        Checksum checksum) {
    public EntryIndexRef {
        Objects.requireNonNull(location, "location");
        objectId = Objects.requireNonNull(objectId, "objectId");
        objectKey = Objects.requireNonNull(objectKey, "objectKey");
        inlineData = copyOptionalBytes(inlineData);
        Objects.requireNonNull(checksum, "checksum");
        if (offset < 0 || length < 0) {
            throw new IllegalArgumentException("offset and length must be non-negative");
        }
    }

    @Override
    public Optional<byte[]> inlineData() {
        return copyOptionalBytes(inlineData);
    }

    private static Optional<byte[]> copyOptionalBytes(Optional<byte[]> bytes) {
        Objects.requireNonNull(bytes, "inlineData");
        return bytes.map(byte[]::clone);
    }
}
