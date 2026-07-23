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
import java.util.Locale;
import java.util.Map;
import java.util.Objects;
import java.util.Optional;
import java.util.zip.CRC32C;

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
        entries = List.copyOf(Objects.requireNonNull(entries, "entries"));
        schemaRefs = MetadataCanonicalizer.canonicalSchemaRefs(schemaRefs);
        projectionHints = MetadataCanonicalizer.canonicalStringMap(
                projectionHints,
                Integer.MAX_VALUE,
                "projectionHints");
        checksum = Objects.requireNonNull(checksum, "checksum");
        if (!projectionHints.isEmpty()) {
            throw new IllegalArgumentException("executable append formats do not accept projectionHints");
        }
        if (entries.isEmpty()) {
            throw new IllegalArgumentException("entries cannot be empty");
        }
        if (entries.size() > ApiLimits.MAX_APPEND_ENTRIES) {
            throw new IllegalArgumentException("entries exceed the maximum append entry count");
        }
        if (recordCount <= 0) {
            throw new IllegalArgumentException("recordCount must be positive");
        }
        if (entryCount != entries.size()) {
            throw new IllegalArgumentException("entryCount must equal entries.size");
        }
        if (minEventTimeMillis < 0 || maxEventTimeMillis < minEventTimeMillis) {
            throw new IllegalArgumentException("invalid event time range");
        }
        if (payloadFormat != PayloadFormat.OPAQUE_RECORD_BATCH
                && payloadFormat != PayloadFormat.KAFKA_RECORD_BATCH) {
            throw new IllegalArgumentException("payload format is reserved and not executable: " + payloadFormat);
        }
        int sum = 0;
        for (AppendEntry entry : entries) {
            Objects.requireNonNull(entry, "entry");
            if (payloadFormat == PayloadFormat.OPAQUE_RECORD_BATCH && entry.recordCount() != 1) {
                throw new IllegalArgumentException("OPAQUE_RECORD_BATCH entries must have recordCount == 1");
            }
            try {
                sum = Math.addExact(sum, entry.recordCount());
            } catch (ArithmeticException overflow) {
                throw new IllegalArgumentException("sum of entry record counts overflows int", overflow);
            }
            if (entry.eventTimeMillis() < minEventTimeMillis || entry.eventTimeMillis() > maxEventTimeMillis) {
                throw new IllegalArgumentException("entry event time must be within batch range");
            }
        }
        if (sum != recordCount) {
            throw new IllegalArgumentException("recordCount must equal the sum of entry record counts");
        }
        validatePayloadChecksum(entries, checksum);
    }

    private static void validatePayloadChecksum(List<AppendEntry> entries, Optional<Checksum> checksum) {
        if (checksum.isEmpty()) {
            return;
        }
        Checksum expected = checksum.get();
        if (expected.type() != ChecksumType.CRC32C) {
            throw new IllegalArgumentException("append payload checksum must be CRC32C");
        }
        CRC32C crc32c = new CRC32C();
        for (AppendEntry entry : entries) {
            byte[] payload = entry.payload();
            crc32c.update(payload, 0, payload.length);
        }
        String actual = String.format(Locale.ROOT, "%08x", crc32c.getValue());
        if (!actual.equals(expected.value())) {
            throw new IllegalArgumentException("append payload checksum mismatch");
        }
    }
}
