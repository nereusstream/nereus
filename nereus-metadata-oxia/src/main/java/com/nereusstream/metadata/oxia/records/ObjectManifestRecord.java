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

import java.util.List;
import java.util.Objects;

public record ObjectManifestRecord(
        String objectId,
        String objectKey,
        String objectType,
        String state,
        int formatMajorVersion,
        int formatMinorVersion,
        String writerVersion,
        String writerId,
        String writerRunIdHash,
        long writerEpoch,
        long createdAtMillis,
        long uploadedAtMillis,
        long objectLength,
        String objectChecksumType,
        String objectChecksumValue,
        String storageChecksumType,
        String storageChecksumValue,
        List<StreamSliceManifestRecord> slices,
        long orphanExpiresAtMillis,
        long metadataVersion) {
    public ObjectManifestRecord {
        objectId = requireNonBlank(objectId, "objectId");
        objectKey = requireNonBlank(objectKey, "objectKey");
        objectType = requireNonBlank(objectType, "objectType");
        state = requireNonBlank(state, "state");
        writerVersion = requireNonBlank(writerVersion, "writerVersion");
        writerId = requireNonBlank(writerId, "writerId");
        writerRunIdHash = requireNonBlank(writerRunIdHash, "writerRunIdHash");
        objectChecksumType = requireNonBlank(objectChecksumType, "objectChecksumType");
        objectChecksumValue = requireNonBlank(objectChecksumValue, "objectChecksumValue");
        storageChecksumType = requireNonBlank(storageChecksumType, "storageChecksumType");
        storageChecksumValue = requireNonBlank(storageChecksumValue, "storageChecksumValue");
        slices = List.copyOf(Objects.requireNonNull(slices, "slices"));
        if (formatMajorVersion < 0 || formatMinorVersion < 0 || writerEpoch < 0 || createdAtMillis < 0
                || uploadedAtMillis < 0 || objectLength < 0 || orphanExpiresAtMillis < 0
                || metadataVersion < 0) {
            throw new IllegalArgumentException("object manifest numeric fields must be non-negative");
        }
    }

    public ObjectManifestRecord withStateAndSlices(String newState, List<StreamSliceManifestRecord> newSlices, long newVersion) {
        return new ObjectManifestRecord(
                objectId,
                objectKey,
                objectType,
                newState,
                formatMajorVersion,
                formatMinorVersion,
                writerVersion,
                writerId,
                writerRunIdHash,
                writerEpoch,
                createdAtMillis,
                uploadedAtMillis,
                objectLength,
                objectChecksumType,
                objectChecksumValue,
                storageChecksumType,
                storageChecksumValue,
                newSlices,
                orphanExpiresAtMillis,
                newVersion);
    }

    private static String requireNonBlank(String value, String fieldName) {
        Objects.requireNonNull(value, fieldName);
        if (value.isBlank()) {
            throw new IllegalArgumentException(fieldName + " cannot be blank");
        }
        return value;
    }
}
