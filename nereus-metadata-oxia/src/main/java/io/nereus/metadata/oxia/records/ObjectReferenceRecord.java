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

package io.nereus.metadata.oxia.records;

import java.util.Comparator;
import java.util.HashSet;
import java.util.List;
import java.util.Objects;
import java.util.Set;

public record ObjectReferenceRecord(
        String objectId,
        List<VisibleSliceReferenceRecord> visibleSlices,
        long updatedAtMillis,
        long metadataVersion) {
    public ObjectReferenceRecord {
        objectId = requireNonBlank(objectId, "objectId");
        visibleSlices = Objects.requireNonNull(visibleSlices, "visibleSlices").stream()
                .sorted(Comparator.comparing(VisibleSliceReferenceRecord::streamId)
                        .thenComparingLong(VisibleSliceReferenceRecord::offsetStart)
                        .thenComparing(VisibleSliceReferenceRecord::sliceId))
                .toList();
        if (updatedAtMillis < 0 || metadataVersion < 0) {
            throw new IllegalArgumentException("object reference numeric fields must be non-negative");
        }
        Set<SliceIdentity> identities = new HashSet<>();
        for (VisibleSliceReferenceRecord slice : visibleSlices) {
            if (!identities.add(new SliceIdentity(slice.streamId(), slice.sliceId()))) {
                throw new IllegalArgumentException("object reference visible slice identities must be unique");
            }
        }
    }

    private static String requireNonBlank(String value, String fieldName) {
        Objects.requireNonNull(value, fieldName);
        if (value.isBlank()) {
            throw new IllegalArgumentException(fieldName + " cannot be blank");
        }
        return value;
    }

    private record SliceIdentity(String streamId, String sliceId) {
    }
}
