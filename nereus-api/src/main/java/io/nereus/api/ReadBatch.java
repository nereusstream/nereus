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

/** One returned logical batch clipped to a read request. */
public record ReadBatch(
        OffsetRange range,
        PayloadFormat payloadFormat,
        byte[] payload,
        List<SchemaRef> schemaRefs,
        EntryIndexRef entryIndexRef,
        Optional<ProjectionRef> projectionRef,
        ObjectId sourceObjectId,
        long sourceObjectOffset,
        long sourceObjectLength) {
    public ReadBatch {
        Objects.requireNonNull(range, "range");
        Objects.requireNonNull(payloadFormat, "payloadFormat");
        payload = Objects.requireNonNull(payload, "payload").clone();
        schemaRefs = MetadataCanonicalizer.canonicalSchemaRefs(schemaRefs);
        Objects.requireNonNull(entryIndexRef, "entryIndexRef");
        projectionRef = Objects.requireNonNull(projectionRef, "projectionRef");
        Objects.requireNonNull(sourceObjectId, "sourceObjectId");
        ApiRangeValidation.requireNonNegativeNonOverflowingRange(
                sourceObjectOffset,
                sourceObjectLength,
                "source object");
    }

    @Override
    public byte[] payload() {
        return payload.clone();
    }
}
