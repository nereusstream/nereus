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

import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.assertThatThrownBy;

import java.util.List;
import java.util.Map;
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

    @Test
    void decodedSliceRecordsRejectZeroLengthObjects() {
        assertThatThrownBy(() -> offsetIndex(0, 0))
                .isInstanceOf(IllegalArgumentException.class)
                .hasMessageContaining("positive");
        assertThatThrownBy(() -> streamCommit(0, 0))
                .isInstanceOf(IllegalArgumentException.class)
                .hasMessageContaining("positive");
        assertThatThrownBy(() -> sliceManifest(0, 0))
                .isInstanceOf(IllegalArgumentException.class)
                .hasMessageContaining("positive");
    }

    @Test
    void decodedCommitAndIndexRecordsRejectInconsistentLogicalProgress() {
        assertThatThrownBy(() -> offsetIndex(0, 1, 2, 1, 7, 7, 1))
                .isInstanceOf(IllegalArgumentException.class)
                .hasMessageContaining("start + recordCount");
        assertThatThrownBy(() -> streamCommit(0, 1, 2, 1, 7, 7, 1))
                .isInstanceOf(IllegalArgumentException.class)
                .hasMessageContaining("start + recordCount");
        assertThatThrownBy(() -> offsetIndex(0, 1, 1, 1, 6, 7, 1))
                .isInstanceOf(IllegalArgumentException.class);
        assertThatThrownBy(() -> streamCommit(0, 1, 1, 1, 6, 7, 1))
                .isInstanceOf(IllegalArgumentException.class);
        assertThatThrownBy(() -> offsetIndex(0, 1, 1, 1, 7, 7, 0))
                .isInstanceOf(IllegalArgumentException.class);
        assertThatThrownBy(() -> streamCommit(0, 1, 1, 1, 7, 7, 0))
                .isInstanceOf(IllegalArgumentException.class);
        assertThatThrownBy(() -> new StreamCommitRecord(
                        "stream",
                        "commit",
                        "",
                        0,
                        1,
                        0,
                        8,
                        2,
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
                        0,
                        1,
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
                        1))
                .isInstanceOf(IllegalArgumentException.class)
                .hasMessageContaining("first stream commit");
        assertThatThrownBy(() -> new CommittedSliceRecord("stream", "object", "slice", 0, 1, 0, 0, 1))
                .isInstanceOf(IllegalArgumentException.class);
    }

    @Test
    void decodedHeadAndAuditRecordsRejectImpossibleCommitAnchors() {
        assertThatThrownBy(() -> streamHead(1, 7, 0, ""))
                .isInstanceOf(IllegalArgumentException.class)
                .hasMessageContaining("anchor");
        assertThatThrownBy(() -> streamHead(0, 0, 1, "commit"))
                .isInstanceOf(IllegalArgumentException.class)
                .hasMessageContaining("anchor");
        assertThatThrownBy(() -> new CommittedEndOffsetRecord("stream", 1, 7, 0, 1))
                .isInstanceOf(IllegalArgumentException.class);
        assertThatThrownBy(() -> new VisibleSliceReferenceRecord("stream", "slice", 0, 1, 0, 0))
                .isInstanceOf(IllegalArgumentException.class);
        assertThatThrownBy(() -> new ObjectReferenceRecord(
                        "object",
                        List.of(
                                new VisibleSliceReferenceRecord("stream", "slice", 0, 1, 0, 1),
                                new VisibleSliceReferenceRecord("stream", "slice", 1, 2, 0, 2)),
                        1,
                        1))
                .isInstanceOf(IllegalArgumentException.class)
                .hasMessageContaining("unique");
    }

    @Test
    void decodedNonEmptyAppendSessionsRequirePositiveLeaseIdentity() {
        assertThatThrownBy(() -> new AppendSessionSnapshotRecord("writer", 0, "token", 1, 1))
                .isInstanceOf(IllegalArgumentException.class);
        assertThatThrownBy(() -> new AppendSessionSnapshotRecord("writer", 1, "token", 0, 1))
                .isInstanceOf(IllegalArgumentException.class);
        assertThatThrownBy(() -> new AppendSessionRecord("stream", "writer", 1, "token", 1, 0))
                .isInstanceOf(IllegalArgumentException.class);
    }

    @Test
    void objectReferenceCanonicalizesVisibleSliceOrder() {
        VisibleSliceReferenceRecord later =
                new VisibleSliceReferenceRecord("stream-b", "slice-b", 10, 11, 0, 2);
        VisibleSliceReferenceRecord earlier =
                new VisibleSliceReferenceRecord("stream-a", "slice-a", 0, 1, 0, 1);

        ObjectReferenceRecord references =
                new ObjectReferenceRecord("object", List.of(later, earlier), 1, 1);

        assertThat(references.visibleSlices()).containsExactly(earlier, later);
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
        return offsetIndex(objectOffset, objectLength, 1, 1, 7, 7, 1);
    }

    private OffsetIndexRecord offsetIndex(
            long objectOffset,
            long objectLength,
            long offsetEnd,
            int recordCount,
            long cumulativeSize,
            long logicalBytes,
            long commitVersion) {
        return new OffsetIndexRecord(
                "stream",
                0,
                offsetEnd,
                0,
                cumulativeSize,
                "object",
                "object-key",
                "slice",
                "MULTI_STREAM_WAL_OBJECT",
                "WAL_OBJECT_V1",
                "OPAQUE_SLICE",
                "OPAQUE_RECORD_BATCH",
                objectOffset,
                objectLength,
                recordCount,
                1,
                logicalBytes,
                List.of(),
                entryIndex(0, 1),
                "",
                "CRC32C",
                "22222222",
                1,
                1,
                commitVersion,
                false,
                1);
    }

    private StreamCommitRecord streamCommit(long objectOffset, long objectLength) {
        return streamCommit(objectOffset, objectLength, 1, 1, 7, 7, 1);
    }

    private StreamCommitRecord streamCommit(
            long objectOffset,
            long objectLength,
            long offsetEnd,
            int recordCount,
            long cumulativeSize,
            long logicalBytes,
            long commitVersion) {
        return new StreamCommitRecord(
                "stream",
                "commit",
                "",
                0,
                offsetEnd,
                0,
                cumulativeSize,
                commitVersion,
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
                recordCount,
                1,
                logicalBytes,
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

    private StreamHeadRecord streamHead(
            long committedEndOffset,
            long cumulativeSize,
            long commitVersion,
            String lastCommitId) {
        return new StreamHeadRecord(
                "stream",
                "tenant/ns/topic",
                "stream-name-hash",
                "ACTIVE",
                "OBJECT_WAL_SYNC_OBJECT",
                Map.of(),
                1,
                0,
                committedEndOffset,
                cumulativeSize,
                commitVersion,
                0,
                lastCommitId,
                AppendSessionSnapshotRecord.EMPTY,
                1);
    }
}
