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
import java.util.Map;
import java.util.Objects;
import java.util.Optional;

/** A batch of entries submitted through the L0 append API. */
public record AppendBatch(
        PayloadFormat payloadFormat,
        List<AppendEntry> entries,
        int recordCount,
        int entryCount,
        long minEventTimeMillis,
        long maxEventTimeMillis,
        List<SchemaRef> schemaRefs,
        Map<String, String> projectionHints,
        Optional<Checksum> checksum) {
    public AppendBatch {
        Objects.requireNonNull(payloadFormat, "payloadFormat");
        entries = List.copyOf(entries);
        schemaRefs = List.copyOf(schemaRefs);
        projectionHints = Map.copyOf(projectionHints);
        checksum = Objects.requireNonNull(checksum, "checksum");
        if (entries.isEmpty()) {
            throw new IllegalArgumentException("entries cannot be empty");
        }
        if (recordCount <= 0) {
            throw new IllegalArgumentException("recordCount must be positive");
        }
        if (entryCount != entries.size()) {
            throw new IllegalArgumentException("entryCount must equal entries.size");
        }
        long sum = 0;
        for (AppendEntry entry : entries) {
            sum = Math.addExact(sum, entry.recordCount());
            if (entry.eventTimeMillis() < minEventTimeMillis || entry.eventTimeMillis() > maxEventTimeMillis) {
                throw new IllegalArgumentException("entry event time must be within batch range");
            }
        }
        if (sum != recordCount) {
            throw new IllegalArgumentException("recordCount must equal the sum of entry record counts");
        }
        if (minEventTimeMillis < 0 || maxEventTimeMillis < minEventTimeMillis) {
            throw new IllegalArgumentException("invalid event time range");
        }
    }
}
