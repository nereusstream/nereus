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

import com.nereusstream.api.MetadataCanonicalizer;
import com.nereusstream.api.SchemaRef;
import java.util.List;
import java.util.Objects;

public record StreamSliceManifestRecord(
        int sliceOrdinal,
        String streamId,
        String sliceId,
        long writerEpoch,
        long objectOffset,
        long objectLength,
        int recordCount,
        int entryCount,
        long logicalBytes,
        List<SchemaRef> schemaRefs,
        EntryIndexReferenceRecord entryIndexRef,
        String sliceChecksumType,
        String sliceChecksumValue,
        String payloadFormat,
        String state) {
    public StreamSliceManifestRecord {
        streamId = requireNonBlank(streamId, "streamId");
        sliceId = requireNonBlank(sliceId, "sliceId");
        schemaRefs = MetadataCanonicalizer.canonicalSchemaRefs(schemaRefs);
        Objects.requireNonNull(entryIndexRef, "entryIndexRef");
        sliceChecksumType = requireNonBlank(sliceChecksumType, "sliceChecksumType");
        sliceChecksumValue = requireNonBlank(sliceChecksumValue, "sliceChecksumValue");
        payloadFormat = requireNonBlank(payloadFormat, "payloadFormat");
        state = requireNonBlank(state, "state");
        if (sliceOrdinal < 0 || writerEpoch < 0 || recordCount <= 0 || entryCount <= 0
                || logicalBytes < 0) {
            throw new IllegalArgumentException("slice manifest numeric fields are invalid");
        }
        MetadataRecordValidation.requirePositiveNonOverflowingRange(
                objectOffset,
                objectLength,
                "slice manifest object");
    }

    public StreamSliceManifestRecord withState(String newState) {
        return new StreamSliceManifestRecord(
                sliceOrdinal,
                streamId,
                sliceId,
                writerEpoch,
                objectOffset,
                objectLength,
                recordCount,
                entryCount,
                logicalBytes,
                schemaRefs,
                entryIndexRef,
                sliceChecksumType,
                sliceChecksumValue,
                payloadFormat,
                newState);
    }

    private static String requireNonBlank(String value, String fieldName) {
        Objects.requireNonNull(value, fieldName);
        if (value.isBlank()) {
            throw new IllegalArgumentException(fieldName + " cannot be blank");
        }
        return value;
    }
}
