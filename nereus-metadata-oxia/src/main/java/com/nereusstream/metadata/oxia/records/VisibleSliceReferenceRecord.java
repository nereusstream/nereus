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

public record VisibleSliceReferenceRecord(
        String streamId,
        String sliceId,
        long offsetStart,
        long offsetEnd,
        long generation,
        long commitVersion) {
    public VisibleSliceReferenceRecord {
        requireNonBlank(streamId, "streamId");
        requireNonBlank(sliceId, "sliceId");
        if (offsetStart < 0 || offsetEnd <= offsetStart || generation < 0 || commitVersion <= 0) {
            throw new IllegalArgumentException("visible slice reference numeric fields are invalid");
        }
    }

    private static void requireNonBlank(String value, String fieldName) {
        Objects.requireNonNull(value, fieldName);
        if (value.isBlank()) {
            throw new IllegalArgumentException(fieldName + " cannot be blank");
        }
    }
}
