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

package com.nereusstream.metadata.oxia.records;

import java.util.Objects;

public record StreamNameRecord(
        String streamName,
        String streamId,
        String streamNameHash,
        long createdAtMillis,
        long metadataVersion) {
    public StreamNameRecord {
        requireNonBlank(streamName, "streamName");
        requireNonBlank(streamId, "streamId");
        requireNonBlank(streamNameHash, "streamNameHash");
        if (createdAtMillis < 0 || metadataVersion < 0) {
            throw new IllegalArgumentException("stream name numeric fields must be non-negative");
        }
    }

    private static void requireNonBlank(String value, String fieldName) {
        Objects.requireNonNull(value, fieldName);
        if (value.isBlank()) {
            throw new IllegalArgumentException(fieldName + " cannot be blank");
        }
    }
}
