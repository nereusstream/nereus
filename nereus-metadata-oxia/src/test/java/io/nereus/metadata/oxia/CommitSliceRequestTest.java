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

package io.nereus.metadata.oxia;

import static org.assertj.core.api.Assertions.assertThat;

import io.nereus.api.Checksum;
import io.nereus.api.ChecksumType;
import io.nereus.api.EntryIndexLocation;
import io.nereus.api.EntryIndexRef;
import io.nereus.api.ObjectId;
import io.nereus.api.ObjectKey;
import io.nereus.api.PayloadFormat;
import io.nereus.api.ProjectionRef;
import io.nereus.api.ProjectionType;
import io.nereus.api.SchemaRef;
import io.nereus.api.StreamId;
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

    private CommitSliceRequest request(
            EntryIndexRef entryIndexRef,
            long minEventTimeMillis,
            long maxEventTimeMillis,
            Optional<ProjectionRef> projectionRef) {
        return new CommitSliceRequest(
                new StreamId("stream"),
                "writer",
                "run-hash",
                1,
                "fencing-token",
                0,
                "slice",
                1,
                1,
                7,
                List.of(new SchemaRef("namespace", "schema", 1)),
                new ObjectId("object"),
                new ObjectKey("object-key"),
                checksum("11111111"),
                0,
                16,
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
