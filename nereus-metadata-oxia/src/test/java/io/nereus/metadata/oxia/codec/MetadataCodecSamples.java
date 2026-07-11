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

package io.nereus.metadata.oxia.codec;

import io.nereus.api.SchemaRef;
import io.nereus.metadata.oxia.records.AppendSessionRecord;
import io.nereus.metadata.oxia.records.AppendSessionSnapshotRecord;
import io.nereus.metadata.oxia.records.CommittedEndOffsetRecord;
import io.nereus.metadata.oxia.records.CommittedSliceRecord;
import io.nereus.metadata.oxia.records.EntryIndexReferenceRecord;
import io.nereus.metadata.oxia.records.ObjectManifestRecord;
import io.nereus.metadata.oxia.records.ObjectReferenceRecord;
import io.nereus.metadata.oxia.records.OffsetIndexRecord;
import io.nereus.metadata.oxia.records.StreamCommitRecord;
import io.nereus.metadata.oxia.records.StreamHeadRecord;
import io.nereus.metadata.oxia.records.StreamMetadataRecord;
import io.nereus.metadata.oxia.records.StreamNameRecord;
import io.nereus.metadata.oxia.records.StreamSliceManifestRecord;
import io.nereus.metadata.oxia.records.TrimRecord;
import io.nereus.metadata.oxia.records.VisibleSliceReferenceRecord;
import java.util.List;
import java.util.Map;

public final class MetadataCodecSamples {
    static final String ABSENT_PROJECTION_IDENTITY = "13:projectionRef6:absent";
    static final String PRESENT_PROJECTION_IDENTITY =
            "13:projectionRef7:present13:PROTOCOL_HINT16:projection-value";

    private static final SchemaRef SCHEMA_REF = new SchemaRef("namespace", "schema", 1);
    private static final EntryIndexReferenceRecord ENTRY_INDEX = new EntryIndexReferenceRecord(
            "INLINE",
            "",
            "",
            new byte[] {1, 2, 3},
            0,
            0,
            "CRC32C",
            "11111111");
    private static final StreamSliceManifestRecord SLICE_MANIFEST = new StreamSliceManifestRecord(
            0,
            "stream",
            "slice",
            1,
            5,
            7,
            2,
            3,
            11,
            List.of(SCHEMA_REF),
            ENTRY_INDEX,
            "CRC32C",
            "22222222",
            "OPAQUE_RECORD_BATCH",
            "UPLOADED");
    private static final VisibleSliceReferenceRecord VISIBLE_SLICE =
            new VisibleSliceReferenceRecord("stream", "slice", 0, 10, 0, 2);
    private static final AppendSessionSnapshotRecord SESSION =
            new AppendSessionSnapshotRecord("writer", 1, "token", 2, 3_000);

    private MetadataCodecSamples() {
    }

    public static List<Sample<?>> samples() {
        return List.of(
                new Sample<>(AppendSessionRecord.class,
                        new AppendSessionRecord("stream", "writer", 1, "token", 2, 3_000)),
                new Sample<>(AppendSessionSnapshotRecord.class, SESSION),
                new Sample<>(CommittedEndOffsetRecord.class,
                        new CommittedEndOffsetRecord("stream", 10, 99, 2, 3)),
                new Sample<>(CommittedSliceRecord.class,
                        new CommittedSliceRecord("stream", "object", "slice", 0, 10, 0, 2, 3)),
                new Sample<>(EntryIndexReferenceRecord.class, ENTRY_INDEX),
                new Sample<>(ObjectManifestRecord.class,
                        new ObjectManifestRecord(
                                "object",
                                "object-key",
                                "MULTI_STREAM_WAL_OBJECT",
                                "UPLOADED",
                                1,
                                0,
                                "writer-version",
                                "writer",
                                "run-hash",
                                1,
                                1_000,
                                1_001,
                                128,
                                "CRC32C",
                                "33333333",
                                "CRC32C",
                                "44444444",
                                List.of(SLICE_MANIFEST),
                                9_999,
                                5)),
                new Sample<>(ObjectReferenceRecord.class,
                        new ObjectReferenceRecord("object", List.of(VISIBLE_SLICE), 1_111, 6)),
                new Sample<>(OffsetIndexRecord.class,
                        new OffsetIndexRecord(
                                "stream",
                                0,
                                10,
                                0,
                                99,
                                "object",
                                "object-key",
                                "slice",
                                "MULTI_STREAM_WAL_OBJECT",
                                "WAL_OBJECT_V1",
                                "OPAQUE_SLICE",
                                5,
                                7,
                                10,
                                3,
                                11,
                                List.of(SCHEMA_REF),
                                ENTRY_INDEX,
                                ABSENT_PROJECTION_IDENTITY,
                                "CRC32C",
                                "22222222",
                                7,
                                8,
                                2,
                                false,
                                4)),
                new Sample<>(StreamCommitRecord.class,
                        new StreamCommitRecord(
                                "stream",
                                "commit",
                                "previous-commit",
                                1,
                                10,
                                0,
                                99,
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
                                "33333333",
                                5,
                                7,
                                9,
                                3,
                                11,
                                List.of(SCHEMA_REF),
                                ENTRY_INDEX,
                                PRESENT_PROJECTION_IDENTITY,
                                "CRC32C",
                                "22222222",
                                7,
                                8,
                                1_234,
                                5)),
                new Sample<>(StreamHeadRecord.class,
                        new StreamHeadRecord(
                                "stream",
                                "tenant/ns/topic",
                                "stream-name-hash",
                                "ACTIVE",
                                "OBJECT_WAL",
                                Map.of("b", "2", "a", "1"),
                                1_000,
                                1,
                                10,
                                99,
                                2,
                                0,
                                "commit",
                                SESSION,
                                7)),
                new Sample<>(StreamMetadataRecord.class,
                        new StreamMetadataRecord(
                                "stream",
                                "tenant/ns/topic",
                                "stream-name-hash",
                                "ACTIVE",
                                "OBJECT_WAL",
                                Map.of("b", "2", "a", "1"),
                                1_000,
                                1,
                                7)),
                new Sample<>(StreamNameRecord.class,
                        new StreamNameRecord("tenant/ns/topic", "stream", "stream-name-hash", 1_000, 7)),
                new Sample<>(StreamSliceManifestRecord.class, SLICE_MANIFEST),
                new Sample<>(TrimRecord.class, new TrimRecord("stream", 4, "", 1_200, 8)),
                new Sample<>(VisibleSliceReferenceRecord.class, VISIBLE_SLICE));
    }

    public record Sample<T>(Class<T> recordClass, T record) {
    }
}
