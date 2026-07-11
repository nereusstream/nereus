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

package com.nereusstream.api;

import java.util.List;
import java.util.Objects;
import java.util.Optional;

import com.nereusstream.api.target.ObjectSliceReadTarget;

/** Compatibility view of a generic range when its target is an object slice. */
@Deprecated(forRemoval = true)
public record ResolvedObjectRange(
        OffsetRange offsetRange,
        long generation,
        ObjectId objectId,
        ObjectKey objectKey,
        ObjectType objectType,
        long objectOffset,
        long objectLength,
        Checksum sliceChecksum,
        PayloadFormat payloadFormat,
        List<SchemaRef> schemaRefs,
        EntryIndexRef entryIndexRef,
        Optional<ProjectionRef> projectionRef,
        long commitVersion) {
    public ResolvedObjectRange {
        Objects.requireNonNull(offsetRange, "offsetRange");
        Objects.requireNonNull(objectId, "objectId");
        Objects.requireNonNull(objectKey, "objectKey");
        Objects.requireNonNull(objectType, "objectType");
        Objects.requireNonNull(sliceChecksum, "sliceChecksum");
        Objects.requireNonNull(payloadFormat, "payloadFormat");
        schemaRefs = MetadataCanonicalizer.canonicalSchemaRefs(schemaRefs);
        Objects.requireNonNull(entryIndexRef, "entryIndexRef");
        projectionRef = Objects.requireNonNull(projectionRef, "projectionRef");
        ApiRangeValidation.requireNonNegativeNonOverflowingRange(objectOffset, objectLength, "object");
        if (generation < 0 || commitVersion < 0) {
            throw new IllegalArgumentException("range numeric fields must be non-negative");
        }
    }

    public static ResolvedObjectRange from(ResolvedRange range) {
        Objects.requireNonNull(range, "range");
        if (!(range.readTarget() instanceof ObjectSliceReadTarget target)) {
            throw new IllegalArgumentException("resolved range is not backed by an object slice");
        }
        return new ResolvedObjectRange(
                range.offsetRange(),
                range.generation(),
                target.objectId(),
                target.objectKey(),
                target.objectType(),
                target.objectOffset(),
                target.objectLength(),
                target.sliceChecksum(),
                range.payloadFormat(),
                range.schemaRefs(),
                target.entryIndexRef(),
                range.projectionRef(),
                range.commitVersion());
    }
}
