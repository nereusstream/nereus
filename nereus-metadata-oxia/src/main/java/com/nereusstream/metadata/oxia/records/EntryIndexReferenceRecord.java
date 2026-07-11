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

import com.nereusstream.api.EntryIndexLocation;
import com.nereusstream.api.EntryIndexRef;
import com.nereusstream.api.ObjectId;
import com.nereusstream.api.ObjectKey;
import java.util.Arrays;
import java.util.Objects;

public record EntryIndexReferenceRecord(
        String location,
        String objectId,
        String objectKey,
        byte[] inlineData,
        long offset,
        long length,
        String checksumType,
        String checksumValue) {
    public EntryIndexReferenceRecord {
        location = requireNonBlank(location, "location");
        objectId = Objects.requireNonNull(objectId, "objectId");
        objectKey = Objects.requireNonNull(objectKey, "objectKey");
        inlineData = Objects.requireNonNull(inlineData, "inlineData").clone();
        checksumType = requireNonBlank(checksumType, "checksumType");
        checksumValue = requireNonBlank(checksumValue, "checksumValue");
        MetadataRecordValidation.requireNonNegativeNonOverflowingRange(offset, length, "entry-index");
        validateLocation(location, objectId, objectKey, inlineData, offset, length);
    }

    public static EntryIndexReferenceRecord fromApi(EntryIndexRef ref) {
        Objects.requireNonNull(ref, "ref");
        return new EntryIndexReferenceRecord(
                ref.location().name(),
                ref.objectId().map(ObjectId::value).orElse(""),
                ref.objectKey().map(ObjectKey::value).orElse(""),
                ref.inlineData().orElseGet(() -> new byte[0]),
                ref.offset(),
                ref.length(),
                ref.checksum().type().name(),
                ref.checksum().value());
    }

    @Override
    public byte[] inlineData() {
        return inlineData.clone();
    }

    public EntryIndexLocation locationEnum() {
        return EntryIndexLocation.valueOf(location);
    }

    private static void validateLocation(
            String location,
            String objectId,
            String objectKey,
            byte[] inlineData,
            long offset,
            long length) {
        EntryIndexLocation parsedLocation;
        try {
            parsedLocation = EntryIndexLocation.valueOf(location);
        } catch (IllegalArgumentException e) {
            throw new IllegalArgumentException("unknown entry-index location: " + location, e);
        }

        boolean hasObjectId = !objectId.isEmpty();
        boolean hasObjectKey = !objectKey.isEmpty();
        boolean hasInlineData = inlineData.length > 0;
        if ((hasObjectId && objectId.isBlank()) || (hasObjectKey && objectKey.isBlank())) {
            throw new IllegalArgumentException("entry-index object id/key cannot be blank when present");
        }
        switch (parsedLocation) {
            case INLINE -> {
                if (!hasInlineData) {
                    throw new IllegalArgumentException("INLINE entry index requires inlineData");
                }
                if (hasObjectId || hasObjectKey) {
                    throw new IllegalArgumentException("INLINE entry index cannot reference an object");
                }
                if (offset != 0 || length != 0) {
                    throw new IllegalArgumentException("INLINE entry index requires zero offset and length");
                }
            }
            case OBJECT_FOOTER -> {
                if (hasInlineData) {
                    throw new IllegalArgumentException("OBJECT_FOOTER entry index cannot contain inlineData");
                }
                if (hasObjectId != hasObjectKey) {
                    throw new IllegalArgumentException("OBJECT_FOOTER entry index objectId/objectKey must both be present or both empty");
                }
                if (length <= 0) {
                    throw new IllegalArgumentException("OBJECT_FOOTER entry index length must be positive");
                }
            }
            case INDEX_OBJECT -> {
                if (!hasObjectId || !hasObjectKey) {
                    throw new IllegalArgumentException("INDEX_OBJECT entry index requires objectId and objectKey");
                }
                if (hasInlineData) {
                    throw new IllegalArgumentException("INDEX_OBJECT entry index cannot contain inlineData");
                }
                if (length <= 0) {
                    throw new IllegalArgumentException("INDEX_OBJECT entry index length must be positive");
                }
            }
        }
    }

    @Override
    public boolean equals(Object other) {
        if (this == other) {
            return true;
        }
        if (!(other instanceof EntryIndexReferenceRecord that)) {
            return false;
        }
        return offset == that.offset
                && length == that.length
                && location.equals(that.location)
                && objectId.equals(that.objectId)
                && objectKey.equals(that.objectKey)
                && Arrays.equals(inlineData, that.inlineData)
                && checksumType.equals(that.checksumType)
                && checksumValue.equals(that.checksumValue);
    }

    @Override
    public int hashCode() {
        int result = Objects.hash(location, objectId, objectKey, offset, length, checksumType, checksumValue);
        result = 31 * result + Arrays.hashCode(inlineData);
        return result;
    }

    private static String requireNonBlank(String value, String fieldName) {
        Objects.requireNonNull(value, fieldName);
        if (value.isBlank()) {
            throw new IllegalArgumentException(fieldName + " cannot be blank");
        }
        return value;
    }
}
