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

import java.util.List;
import java.util.Objects;
import java.util.Optional;

/** Result returned after a batch is durable and visible through Oxia offset index. */
public record AppendResult(
        StreamId streamId,
        OffsetRange range,
        long committedEndOffset,
        long generation,
        ObjectId objectId,
        ObjectKey objectKey,
        String sliceId,
        long objectOffset,
        long objectLength,
        PayloadFormat payloadFormat,
        int recordCount,
        int entryCount,
        long logicalBytes,
        List<SchemaRef> schemaRefs,
        EntryIndexRef entryIndexRef,
        Checksum objectChecksum,
        Checksum sliceChecksum,
        Optional<ProjectionRef> projectionRef,
        long commitVersion) {
    public AppendResult {
        Objects.requireNonNull(streamId, "streamId");
        Objects.requireNonNull(range, "range");
        Objects.requireNonNull(objectId, "objectId");
        Objects.requireNonNull(objectKey, "objectKey");
        Objects.requireNonNull(sliceId, "sliceId");
        Objects.requireNonNull(payloadFormat, "payloadFormat");
        schemaRefs = MetadataCanonicalizer.canonicalSchemaRefs(schemaRefs);
        Objects.requireNonNull(entryIndexRef, "entryIndexRef");
        Objects.requireNonNull(objectChecksum, "objectChecksum");
        Objects.requireNonNull(sliceChecksum, "sliceChecksum");
        projectionRef = Objects.requireNonNull(projectionRef, "projectionRef");
        if (committedEndOffset != range.endOffset()) {
            throw new IllegalArgumentException("committedEndOffset must equal range.endOffset");
        }
        if (generation < 0) {
            throw new IllegalArgumentException("generation must be non-negative");
        }
        if (sliceId.isBlank()) {
            throw new IllegalArgumentException("sliceId cannot be blank");
        }
        ApiRangeValidation.requireNonNegativeNonOverflowingRange(objectOffset, objectLength, "object");
        if (recordCount <= 0 || entryCount <= 0) {
            throw new IllegalArgumentException("recordCount and entryCount must be positive");
        }
        if (logicalBytes < 0) {
            throw new IllegalArgumentException("logicalBytes must be non-negative");
        }
        if (recordCount != range.recordCount()) {
            throw new IllegalArgumentException("recordCount must equal range.recordCount");
        }
        if (payloadFormat == PayloadFormat.OPAQUE_RECORD_BATCH && entryCount != recordCount) {
            throw new IllegalArgumentException("OPAQUE_RECORD_BATCH entryCount must equal recordCount");
        }
        if (commitVersion < 0) {
            throw new IllegalArgumentException("commitVersion must be non-negative");
        }
    }
}
