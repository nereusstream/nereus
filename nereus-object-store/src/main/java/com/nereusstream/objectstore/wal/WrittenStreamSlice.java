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

import com.nereusstream.api.Checksum;
import com.nereusstream.api.EntryIndexRef;
import com.nereusstream.api.MetadataCanonicalizer;
import com.nereusstream.api.PayloadFormat;
import com.nereusstream.api.SchemaRef;
import com.nereusstream.api.StreamId;
import com.nereusstream.objectstore.RangeChecks;
import java.util.List;
import java.util.Objects;

public record WrittenStreamSlice(
        StreamId streamId,
        String sliceId,
        long objectOffset,
        long objectLength,
        int recordCount,
        int entryCount,
        long logicalBytes,
        List<SchemaRef> schemaRefs,
        PayloadFormat payloadFormat,
        EntryIndexRef entryIndexRef,
        Checksum sliceChecksum,
        long minEventTimeMillis,
        long maxEventTimeMillis) {
    public WrittenStreamSlice {
        Objects.requireNonNull(streamId, "streamId");
        sliceId = requireNonBlank(sliceId, "sliceId");
        RangeChecks.requireNonNegativeNonOverflowingRange(objectOffset, objectLength, "written slice");
        schemaRefs = MetadataCanonicalizer.canonicalSchemaRefs(schemaRefs);
        Objects.requireNonNull(payloadFormat, "payloadFormat");
        Objects.requireNonNull(entryIndexRef, "entryIndexRef");
        Objects.requireNonNull(sliceChecksum, "sliceChecksum");
        if (recordCount <= 0 || entryCount <= 0 || logicalBytes < 0
                || minEventTimeMillis < 0 || maxEventTimeMillis < minEventTimeMillis) {
            throw new IllegalArgumentException("written slice numeric fields are invalid");
        }
    }

    private static String requireNonBlank(String value, String fieldName) {
        Objects.requireNonNull(value, fieldName);
        if (value.isBlank()) {
            throw new IllegalArgumentException(fieldName + " cannot be blank");
        }
        return value;
    }
}
