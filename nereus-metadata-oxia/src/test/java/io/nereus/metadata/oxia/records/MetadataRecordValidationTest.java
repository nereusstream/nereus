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

package io.nereus.metadata.oxia.records;

import static org.assertj.core.api.Assertions.assertThatThrownBy;

import java.util.List;
import org.junit.jupiter.api.Test;

class MetadataRecordValidationTest {
    @Test
    void decodedEntryIndexRecordsRejectOverflowingRanges() {
        assertThatThrownBy(() -> entryIndex(Long.MAX_VALUE, 1))
                .isInstanceOf(IllegalArgumentException.class)
                .hasMessageContaining("overflow");
    }

    @Test
    void decodedEntryIndexRecordsRejectInvalidLocationShapes() {
        assertThatThrownBy(() -> new EntryIndexReferenceRecord(
                "MISSING",
                "",
                "",
                new byte[0],
                0,
                0,
                "CRC32C",
                "11111111"))
                .isInstanceOf(IllegalArgumentException.class);
        assertThatThrownBy(() -> new EntryIndexReferenceRecord(
                "INLINE",
                "",
                "",
                new byte[0],
                0,
                0,
                "CRC32C",
                "11111111"))
                .isInstanceOf(IllegalArgumentException.class)
                .hasMessageContaining("inlineData");
        assertThatThrownBy(() -> new EntryIndexReferenceRecord(
                "OBJECT_FOOTER",
                "index-object",
                "",
                new byte[0],
                0,
                1,
                "CRC32C",
                "11111111"))
                .isInstanceOf(IllegalArgumentException.class)
                .hasMessageContaining("both be present");
        assertThatThrownBy(() -> new EntryIndexReferenceRecord(
                "OBJECT_FOOTER",
                "",
                "",
                new byte[] {1},
                0,
                1,
                "CRC32C",
                "11111111"))
                .isInstanceOf(IllegalArgumentException.class)
                .hasMessageContaining("inlineData");
        assertThatThrownBy(() -> new EntryIndexReferenceRecord(
                "INDEX_OBJECT",
                "index-object",
                "",
                new byte[0],
                0,
                1,
                "CRC32C",
                "11111111"))
                .isInstanceOf(IllegalArgumentException.class)
                .hasMessageContaining("requires objectId and objectKey");
    }

    @Test
    void decodedOffsetRecordsRejectOverflowingObjectRanges() {
        assertThatThrownBy(() -> offsetIndex(Long.MAX_VALUE, 1))
                .isInstanceOf(IllegalArgumentException.class)
                .hasMessageContaining("overflow");
        assertThatThrownBy(() -> streamCommit(Long.MAX_VALUE, 1))
                .isInstanceOf(IllegalArgumentException.class)
                .hasMessageContaining("overflow");
        assertThatThrownBy(() -> sliceManifest(Long.MAX_VALUE, 1))
                .isInstanceOf(IllegalArgumentException.class)
                .hasMessageContaining("overflow");
    }

    private EntryIndexReferenceRecord entryIndex(long offset, long length) {
        return new EntryIndexReferenceRecord(
                "OBJECT_FOOTER",
                "",
                "",
                new byte[0],
                offset,
                length,
                "CRC32C",
                "11111111");
    }

    private OffsetIndexRecord offsetIndex(long objectOffset, long objectLength) {
        return new OffsetIndexRecord(
                "stream",
                0,
                1,
                0,
                7,
                "object",
                "object-key",
                "slice",
                "MULTI_STREAM_WAL_OBJECT",
                "WAL_OBJECT_V1",
                "OPAQUE_SLICE",
                objectOffset,
                objectLength,
                1,
                1,
                7,
                List.of(),
                entryIndex(0, 1),
                "",
                "CRC32C",
                "22222222",
                1,
                1,
                1,
                false,
                1);
    }

    private StreamCommitRecord streamCommit(long objectOffset, long objectLength) {
        return new StreamCommitRecord(
                "stream",
                "commit",
                "",
                0,
                1,
                0,
                7,
                1,
                "writer",
                "run-hash",
                1,
                "token-hash",
                "object",
                "object-key",
                "slice",
                "MULTI_STREAM_WAL_OBJECT",
                "WAL_OBJECT_V1",
                "OPAQUE_SLICE",
                "OPAQUE_RECORD_BATCH",
                "CRC32C",
                "11111111",
                objectOffset,
                objectLength,
                1,
                1,
                7,
                List.of(),
                entryIndex(0, 1),
                "",
                "CRC32C",
                "22222222",
                1,
                1,
                1,
                1);
    }

    private StreamSliceManifestRecord sliceManifest(long objectOffset, long objectLength) {
        return new StreamSliceManifestRecord(
                0,
                "stream",
                "slice",
                1,
                objectOffset,
                objectLength,
                1,
                1,
                7,
                List.of(),
                entryIndex(0, 1),
                "CRC32C",
                "22222222",
                "OPAQUE_RECORD_BATCH",
                "UPLOADED");
    }
}
