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

import com.nereusstream.api.target.ReadTarget;
import java.util.List;
import java.util.Objects;
import java.util.Optional;

/**
 * Result returned after a batch is durable and has a stable logical commit.
 *
 * <p>The requested {@link DurabilityLevel} determines whether generation-zero read-index confirmation is also
 * required. Phase 1 executes only {@link DurabilityLevel#WAL_DURABLE_AND_INDEX_COMMITTED} on the Object WAL
 * profile. Physical selection is represented by a protocol-neutral target.
 */
public record AppendResult(
        StreamId streamId,
        OffsetRange range,
        long committedEndOffset,
        long generation,
        ReadTarget readTarget,
        PayloadFormat payloadFormat,
        int recordCount,
        int entryCount,
        long logicalBytes,
        List<SchemaRef> schemaRefs,
        Optional<ProjectionRef> projectionRef,
        long commitVersion) {
    public AppendResult {
        Objects.requireNonNull(streamId, "streamId");
        Objects.requireNonNull(range, "range");
        Objects.requireNonNull(readTarget, "readTarget");
        Objects.requireNonNull(payloadFormat, "payloadFormat");
        schemaRefs = MetadataCanonicalizer.canonicalSchemaRefs(schemaRefs);
        projectionRef = Objects.requireNonNull(projectionRef, "projectionRef");
        if (committedEndOffset != range.endOffset()) {
            throw new IllegalArgumentException("committedEndOffset must equal range.endOffset");
        }
        if (generation != 0) {
            throw new IllegalArgumentException("append generation must be zero");
        }
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
        if (commitVersion <= 0) {
            throw new IllegalArgumentException("commitVersion must be positive");
        }
    }
}
