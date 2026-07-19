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

package com.nereusstream.metadata.oxia;

import static org.assertj.core.api.Assertions.assertThat;

import com.nereusstream.metadata.oxia.records.CommittedEndOffsetRecord;
import com.nereusstream.metadata.oxia.records.StreamMetadataRecord;
import com.nereusstream.metadata.oxia.records.TrimRecord;
import java.util.Map;
import org.junit.jupiter.api.Test;

class StreamMetadataSnapshotTest {
    private static final String STREAM_ID =
            "0123456789abcdef0123456789abcdef";

    @Test
    void versionedAuthorityIgnoresHydratedTrimObservationFields() {
        StreamMetadataSnapshot first = snapshot(
                "ACTIVE",
                "OBJECT_WAL_SYNC_OBJECT",
                3,
                10,
                100,
                1,
                0,
                7,
                "trim-response",
                100);
        StreamMetadataSnapshot reread = snapshot(
                "ACTIVE",
                "OBJECT_WAL_SYNC_OBJECT",
                3,
                10,
                100,
                1,
                0,
                7,
                "",
                999);

        assertThat(first).isNotEqualTo(reread);
        assertThat(first.sameVersionedAuthority(reread)).isTrue();
        assertThat(first.sameSemanticAuthority(reread)).isTrue();
    }

    @Test
    void semanticAuthorityIgnoresOnlyVersionAndObservationDrift() {
        StreamMetadataSnapshot expected = snapshot(
                "ACTIVE",
                "OBJECT_WAL_SYNC_OBJECT",
                3,
                10,
                100,
                1,
                0,
                7,
                "",
                100);
        StreamMetadataSnapshot renewed = snapshot(
                "ACTIVE",
                "OBJECT_WAL_SYNC_OBJECT",
                3,
                10,
                100,
                1,
                0,
                8,
                "another-observation",
                200);

        assertThat(expected.sameSemanticAuthority(renewed)).isTrue();
        assertThat(expected.sameVersionedAuthority(renewed)).isFalse();
        assertThat(expected.sameSemanticAuthority(snapshot(
                        "SEALED",
                        "OBJECT_WAL_SYNC_OBJECT",
                        3,
                        10,
                        100,
                        1,
                        0,
                        8,
                        "",
                        200)))
                .isFalse();
        assertThat(expected.sameSemanticAuthority(snapshot(
                        "ACTIVE",
                        "OBJECT_WAL_ASYNC_OBJECT",
                        3,
                        10,
                        100,
                        1,
                        0,
                        8,
                        "",
                        200)))
                .isFalse();
        assertThat(expected.sameSemanticAuthority(snapshot(
                        "ACTIVE",
                        "OBJECT_WAL_SYNC_OBJECT",
                        4,
                        10,
                        100,
                        1,
                        0,
                        8,
                        "",
                        200)))
                .isFalse();
        assertThat(expected.sameSemanticAuthority(snapshot(
                        "ACTIVE",
                        "OBJECT_WAL_SYNC_OBJECT",
                        3,
                        11,
                        110,
                        2,
                        0,
                        8,
                        "",
                        200)))
                .isFalse();
        assertThat(expected.sameSemanticAuthority(snapshot(
                        "ACTIVE",
                        "OBJECT_WAL_SYNC_OBJECT",
                        3,
                        10,
                        100,
                        1,
                        1,
                        8,
                        "",
                        200)))
                .isFalse();
        assertThat(expected.sameSemanticAuthority(null)).isFalse();
        assertThat(expected.sameVersionedAuthority(null)).isFalse();
    }

    private static StreamMetadataSnapshot snapshot(
            String state,
            String profile,
            long policyVersion,
            long committedEndOffset,
            long cumulativeSize,
            long commitVersion,
            long trimOffset,
            long metadataVersion,
            String trimReason,
            long observedAtMillis) {
        return new StreamMetadataSnapshot(
                new StreamMetadataRecord(
                        STREAM_ID,
                        "persistent://tenant/ns/topic",
                        "stream-name-hash",
                        state,
                        profile,
                        Map.of("retention", "phase4"),
                        1,
                        policyVersion,
                        metadataVersion),
                new CommittedEndOffsetRecord(
                        STREAM_ID,
                        committedEndOffset,
                        cumulativeSize,
                        commitVersion,
                        metadataVersion),
                new TrimRecord(
                        STREAM_ID,
                        trimOffset,
                        trimReason,
                        observedAtMillis,
                        metadataVersion));
    }
}
