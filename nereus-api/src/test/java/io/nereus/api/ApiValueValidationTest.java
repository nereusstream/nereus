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

import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.assertThatThrownBy;

import java.time.Duration;
import java.util.HashMap;
import java.util.LinkedHashMap;
import java.util.List;
import java.util.Locale;
import java.util.Map;
import java.util.Optional;
import java.util.zip.CRC32C;
import org.junit.jupiter.api.Test;

class ApiValueValidationTest {
    @Test
    void offsetRangeRejectsInvalidRangesAndKeepsHalfOpenHelpers() {
        assertThatThrownBy(() -> new OffsetRange(-1, 0))
                .isInstanceOf(IllegalArgumentException.class);
        assertThatThrownBy(() -> new OffsetRange(2, 1))
                .isInstanceOf(IllegalArgumentException.class);

        OffsetRange range = new OffsetRange(10, 13);

        assertThat(range.recordCount()).isEqualTo(3);
        assertThat(range.isEmpty()).isFalse();
        assertThat(range.contains(10)).isTrue();
        assertThat(range.contains(13)).isFalse();
        assertThat(range.overlaps(new OffsetRange(12, 20))).isTrue();
        assertThat(range.overlaps(new OffsetRange(13, 20))).isFalse();
    }

    @Test
    void checksumUsesCanonicalLowercaseHexWithFixedWidth() {
        assertThat(new Checksum(ChecksumType.CRC32C, "ABCDEF12").value())
                .isEqualTo("abcdef12");
        assertThat(new Checksum(ChecksumType.SHA256, "A".repeat(64)).value())
                .isEqualTo("a".repeat(64));

        assertThatThrownBy(() -> new Checksum(ChecksumType.CRC32C, "abc"))
                .isInstanceOf(IllegalArgumentException.class);
        assertThatThrownBy(() -> new Checksum(ChecksumType.SHA256, "g".repeat(64)))
                .isInstanceOf(IllegalArgumentException.class);
    }

    @Test
    void appendEntryDefensivelyCopiesPayloadAndCanonicalizesAttributes() {
        byte[] payload = new byte[] {1, 2};
        AppendEntry entry = new AppendEntry(payload, 1, 7, Map.of("z", "last", "a", "first"));

        payload[0] = 99;
        assertThat(entry.payload()).containsExactly(1, 2);

        byte[] returned = entry.payload();
        returned[1] = 99;
        assertThat(entry.payload()).containsExactly(1, 2);
        assertThat(entry.attributes().keySet()).containsExactly("a", "z");
        assertThatThrownBy(() -> entry.attributes().put("b", "blocked"))
                .isInstanceOf(UnsupportedOperationException.class);
    }

    @Test
    void metadataMapsRejectNullsAndEncodedSizeOverflow() {
        HashMap<String, String> nullKey = new HashMap<>();
        nullKey.put(null, "value");

        assertThatThrownBy(() -> new StreamCreateOptions(StorageProfile.OBJECT_WAL, nullKey))
                .isInstanceOf(NullPointerException.class);
        assertThatThrownBy(() -> new AppendEntry(
                new byte[] {1},
                1,
                1,
                Map.of("k", "x".repeat(ApiLimits.MAX_ENTRY_ATTRIBUTES_ENCODED_BYTES))))
                .isInstanceOf(IllegalArgumentException.class);
    }

    @Test
    void schemaRefsAreCanonicalizedAndBounded() {
        SchemaRef laterId = new SchemaRef("a", "z", 2);
        SchemaRef earlierId = new SchemaRef("a", "a", 1);
        SchemaRef unicodeNamespace = new SchemaRef("zhang", "a", 0);

        AppendBatch batch = batch(
                List.of(entry(new byte[] {1}, 10)),
                List.of(unicodeNamespace, laterId, earlierId),
                Optional.empty());

        assertThat(batch.schemaRefs()).containsExactly(earlierId, laterId, unicodeNamespace);
        assertThatThrownBy(() -> batch(
                List.of(entry(new byte[] {1}, 10)),
                List.of(earlierId, earlierId),
                Optional.empty()))
                .isInstanceOf(IllegalArgumentException.class);
        assertThatThrownBy(() -> batch(
                List.of(entry(new byte[] {1}, 10)),
                List.of(new SchemaRef("n".repeat(ApiLimits.MAX_SCHEMA_REFS_ENCODED_BYTES), "id", 1)),
                Optional.empty()))
                .isInstanceOf(IllegalArgumentException.class);
    }

    @Test
    void appendBatchRejectsNonOpaqueProjectionHintsAndBadPayloadChecksums() {
        AppendEntry entry = entry(new byte[] {1, 2, 3}, 10);

        assertThatThrownBy(() -> new AppendBatch(
                PayloadFormat.KAFKA_RECORD_BATCH,
                List.of(entry),
                1,
                1,
                10,
                10,
                List.of(),
                Map.of(),
                Optional.empty()))
                .isInstanceOf(IllegalArgumentException.class);
        assertThatThrownBy(() -> new AppendBatch(
                PayloadFormat.OPAQUE_RECORD_BATCH,
                List.of(entry),
                1,
                1,
                10,
                10,
                List.of(),
                Map.of("future", "projection"),
                Optional.empty()))
                .isInstanceOf(IllegalArgumentException.class);
        assertThatThrownBy(() -> new AppendBatch(
                PayloadFormat.OPAQUE_RECORD_BATCH,
                List.of(entry),
                1,
                1,
                10,
                10,
                List.of(),
                Map.of(),
                Optional.of(new Checksum(ChecksumType.CRC32C, "00000000"))))
                .isInstanceOf(IllegalArgumentException.class)
                .hasMessageContaining("checksum mismatch");
    }

    @Test
    void appendBatchAcceptsZeroByteOpaqueEntryWithMatchingCrc32c() {
        AppendEntry empty = entry(new byte[0], 10);

        AppendBatch batch = batch(List.of(empty), List.of(), Optional.of(crc32c(empty)));

        assertThat(batch.recordCount()).isEqualTo(1);
        assertThat(batch.entries()).containsExactly(empty);
    }

    @Test
    void appendBatchRejectsEmptyBatchRecordMismatchesAndOpaqueSubRecords() {
        AppendEntry entry = entry(new byte[] {1}, 10);
        AppendEntry subRecords = new AppendEntry(new byte[] {1}, 2, 10, Map.of());

        assertThatThrownBy(() -> batch(List.of(), List.of(), Optional.empty()))
                .isInstanceOf(IllegalArgumentException.class);
        assertThatThrownBy(() -> new AppendBatch(
                PayloadFormat.OPAQUE_RECORD_BATCH,
                List.of(entry),
                2,
                1,
                10,
                10,
                List.of(),
                Map.of(),
                Optional.empty()))
                .isInstanceOf(IllegalArgumentException.class);
        assertThatThrownBy(() -> new AppendBatch(
                PayloadFormat.OPAQUE_RECORD_BATCH,
                List.of(subRecords),
                2,
                1,
                10,
                10,
                List.of(),
                Map.of(),
                Optional.empty()))
                .isInstanceOf(IllegalArgumentException.class);
    }

    @Test
    void entryIndexRefValidatesLocationSpecificShapeAndDefensiveCopiesInlineData() {
        assertThatThrownBy(() -> new EntryIndexRef(
                EntryIndexLocation.INLINE,
                Optional.empty(),
                Optional.empty(),
                Optional.empty(),
                0,
                0,
                checksum()))
                .isInstanceOf(IllegalArgumentException.class);
        assertThatThrownBy(() -> new EntryIndexRef(
                EntryIndexLocation.INDEX_OBJECT,
                Optional.empty(),
                Optional.empty(),
                Optional.empty(),
                0,
                1,
                checksum()))
                .isInstanceOf(IllegalArgumentException.class);
        assertThatThrownBy(() -> new EntryIndexRef(
                EntryIndexLocation.OBJECT_FOOTER,
                Optional.of(new ObjectId("index")),
                Optional.empty(),
                Optional.empty(),
                0,
                1,
                checksum()))
                .isInstanceOf(IllegalArgumentException.class);

        byte[] inline = new byte[] {1, 2};
        EntryIndexRef ref = new EntryIndexRef(
                EntryIndexLocation.INLINE,
                Optional.empty(),
                Optional.empty(),
                Optional.of(inline),
                0,
                0,
                checksum());

        inline[0] = 9;
        assertThat(ref.inlineData()).hasValueSatisfying(bytes -> assertThat(bytes).containsExactly(1, 2));
        byte[] returned = ref.inlineData().orElseThrow();
        returned[1] = 9;
        assertThat(ref.inlineData()).hasValueSatisfying(bytes -> assertThat(bytes).containsExactly(1, 2));
    }

    @Test
    void basicOptionsAndIdsRejectInvalidValues() {
        assertThatThrownBy(() -> new StreamId(" "))
                .isInstanceOf(IllegalArgumentException.class);
        assertThatThrownBy(() -> new StreamName(""))
                .isInstanceOf(IllegalArgumentException.class);
        assertThatThrownBy(() -> new AppendSessionOptions(" ", Duration.ofSeconds(1), false))
                .isInstanceOf(IllegalArgumentException.class);
        assertThatThrownBy(() -> new ReadOptions(0, 1, ReadIsolation.COMMITTED, Duration.ofSeconds(1)))
                .isInstanceOf(IllegalArgumentException.class);
        assertThatThrownBy(() -> new ResolveOptions(0, true, true))
                .isInstanceOf(IllegalArgumentException.class);
        assertThatThrownBy(() -> new TrimOptions(Duration.ZERO, "test"))
                .isInstanceOf(IllegalArgumentException.class);
    }

    @Test
    void canonicalStringMapOrdersByUtf8KeyBytes() {
        LinkedHashMap<String, String> values = new LinkedHashMap<>();
        values.put("z", "last");
        values.put("a", "first");
        values.put("zhang", "middle");

        Map<String, String> canonical = MetadataCanonicalizer.canonicalStringMap(values, 1024, "values");

        assertThat(canonical.keySet()).containsExactly("a", "z", "zhang");
    }

    private static AppendBatch batch(
            List<AppendEntry> entries,
            List<SchemaRef> schemaRefs,
            Optional<Checksum> checksum) {
        return new AppendBatch(
                PayloadFormat.OPAQUE_RECORD_BATCH,
                entries,
                entries.stream().mapToInt(AppendEntry::recordCount).sum(),
                entries.size(),
                entries.stream().mapToLong(AppendEntry::eventTimeMillis).min().orElse(0),
                entries.stream().mapToLong(AppendEntry::eventTimeMillis).max().orElse(0),
                schemaRefs,
                Map.of(),
                checksum);
    }

    private static AppendEntry entry(byte[] payload, long eventTimeMillis) {
        return new AppendEntry(payload, 1, eventTimeMillis, Map.of());
    }

    private static Checksum crc32c(AppendEntry... entries) {
        CRC32C crc32c = new CRC32C();
        for (AppendEntry entry : entries) {
            byte[] payload = entry.payload();
            crc32c.update(payload, 0, payload.length);
        }
        return new Checksum(ChecksumType.CRC32C, String.format(Locale.ROOT, "%08x", crc32c.getValue()));
    }

    private static Checksum checksum() {
        return new Checksum(ChecksumType.CRC32C, "00000000");
    }
}
