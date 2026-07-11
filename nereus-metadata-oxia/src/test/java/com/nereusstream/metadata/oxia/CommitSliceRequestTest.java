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
import static org.assertj.core.api.Assertions.assertThatThrownBy;

import com.nereusstream.api.Checksum;
import com.nereusstream.api.ChecksumType;
import com.nereusstream.api.EntryIndexLocation;
import com.nereusstream.api.EntryIndexRef;
import com.nereusstream.api.ObjectId;
import com.nereusstream.api.ObjectKey;
import com.nereusstream.api.PayloadFormat;
import com.nereusstream.api.ProjectionRef;
import com.nereusstream.api.ProjectionType;
import com.nereusstream.api.SchemaRef;
import com.nereusstream.api.StreamId;
import java.util.List;
import java.util.Optional;
import org.junit.jupiter.api.Test;

class CommitSliceRequestTest {
    @Test
    void commitIdUsesNestedLengthPrefixedEntryIndexIdentity() {
        EntryIndexRef left = indexObjectRef("a|b", "c");
        EntryIndexRef right = indexObjectRef("a", "b|c");

        assertThat(request(left, 1, 1, Optional.empty()).commitId())
                .isNotEqualTo(request(right, 1, 1, Optional.empty()).commitId());
    }

    @Test
    void commitIdIncludesEventTimeRangeAndProjectionIdentity() {
        EntryIndexRef ref = indexObjectRef("index-object", "index-key");
        CommitSliceRequest base = request(ref, 1, 2, Optional.empty());
        CommitSliceRequest differentEventTime = request(ref, 2, 3, Optional.empty());
        CommitSliceRequest projected = request(
                ref,
                1,
                2,
                Optional.of(new ProjectionRef(ProjectionType.PROTOCOL_HINT, "projection|value")));

        assertThat(base.commitId()).isNotEqualTo(differentEventTime.commitId());
        assertThat(base.commitId()).isNotEqualTo(projected.commitId());
    }

    @Test
    void rejectsLogicalOffsetRangeOverflowAtConstruction() {
        EntryIndexRef ref = indexObjectRef("index-object", "index-key");

        assertThatThrownBy(() -> request(ref, 1, 2, Optional.empty(), Long.MAX_VALUE, 1))
                .isInstanceOf(IllegalArgumentException.class)
                .hasMessageContaining("logical offset");
    }

    @Test
    void rejectsZeroLengthWalSliceAtConstruction() {
        EntryIndexRef ref = indexObjectRef("index-object", "index-key");

        assertThatThrownBy(() -> request(ref, 1, 2, Optional.empty(), 0, 1, 0))
                .isInstanceOf(IllegalArgumentException.class)
                .hasMessageContaining("object length must be positive");
    }

    private CommitSliceRequest request(
            EntryIndexRef entryIndexRef,
            long minEventTimeMillis,
            long maxEventTimeMillis,
            Optional<ProjectionRef> projectionRef) {
        return request(entryIndexRef, minEventTimeMillis, maxEventTimeMillis, projectionRef, 0, 1);
    }

    private CommitSliceRequest request(
            EntryIndexRef entryIndexRef,
            long minEventTimeMillis,
            long maxEventTimeMillis,
            Optional<ProjectionRef> projectionRef,
            long expectedStartOffset,
            int recordCount) {
        return request(
                entryIndexRef,
                minEventTimeMillis,
                maxEventTimeMillis,
                projectionRef,
                expectedStartOffset,
                recordCount,
                16);
    }

    private CommitSliceRequest request(
            EntryIndexRef entryIndexRef,
            long minEventTimeMillis,
            long maxEventTimeMillis,
            Optional<ProjectionRef> projectionRef,
            long expectedStartOffset,
            int recordCount,
            long objectLength) {
        return new CommitSliceRequest(
                new StreamId("stream"),
                "writer",
                "run-hash",
                1,
                "fencing-token",
                expectedStartOffset,
                "slice",
                recordCount,
                1,
                7,
                List.of(new SchemaRef("namespace", "schema", 1)),
                new ObjectId("object"),
                new ObjectKey("object-key"),
                checksum("11111111"),
                0,
                objectLength,
                entryIndexRef,
                checksum("22222222"),
                PayloadFormat.OPAQUE_RECORD_BATCH,
                minEventTimeMillis,
                maxEventTimeMillis,
                projectionRef);
    }

    private EntryIndexRef indexObjectRef(String objectId, String objectKey) {
        return new EntryIndexRef(
                EntryIndexLocation.INDEX_OBJECT,
                Optional.of(new ObjectId(objectId)),
                Optional.of(new ObjectKey(objectKey)),
                Optional.empty(),
                0,
                1,
                checksum("33333333"));
    }

    private Checksum checksum(String value) {
        return new Checksum(ChecksumType.CRC32C, value);
    }
}
