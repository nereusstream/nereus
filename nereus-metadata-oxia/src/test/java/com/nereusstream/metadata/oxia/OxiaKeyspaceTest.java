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

import com.nereusstream.api.ObjectId;
import com.nereusstream.api.StreamId;
import com.nereusstream.api.StreamName;
import com.nereusstream.api.keys.DeterministicIds;
import java.util.List;
import org.junit.jupiter.api.Test;

class OxiaKeyspaceTest {
    @Test
    void keyspaceUsesSharedEncodingAndFixedWidthOffsetOrdering() {
        OxiaKeyspace keyspace = new OxiaKeyspace("cluster/a");
        StreamId streamId = DeterministicIds.streamIdFor(new StreamName("tenant/ns/topic"));

        assertThat(keyspace.prefix()).contains("/clusters/b32-");
        assertThat(keyspace.offsetIndexKey(streamId, 9, 0))
                .isLessThan(keyspace.offsetIndexKey(streamId, 10, 0))
                .isLessThan(keyspace.offsetIndexKey(streamId, 100, 0));
        assertThat(List.of(
                        keyspace.offsetIndexKey(streamId, 100, 0),
                        keyspace.offsetIndexKey(streamId, 9, 0),
                        keyspace.offsetIndexKey(streamId, 10, 0)).stream().sorted().toList())
                .containsExactly(
                        keyspace.offsetIndexKey(streamId, 9, 0),
                        keyspace.offsetIndexKey(streamId, 10, 0),
                        keyspace.offsetIndexKey(streamId, 100, 0));
    }

    @Test
    void committedSliceKeyHashesObjectLocalSliceId() {
        OxiaKeyspace keyspace = new OxiaKeyspace("cluster");
        StreamId streamId = new StreamId("s-stream");

        String key = keyspace.committedSliceKey(streamId, new ObjectId("object/1"), "slice/with/slash");

        assertThat(key).contains("/committed-slices/").doesNotContain("object/1");
        assertThat(key).doesNotContain("slice/with/slash");
    }

    @Test
    void partitionKeysUseRawStableIdentities() {
        OxiaKeyspace keyspace = new OxiaKeyspace("cluster");

        assertThat(keyspace.streamPartitionKey(new StreamId("s-stream")).value()).isEqualTo("s-stream");
        assertThat(keyspace.objectPartitionKey(new ObjectId("object-1")).value()).isEqualTo("object-1");
    }

    @Test
    void parsesOnlyCanonicalCommitLogKeysFromTheSameCluster() {
        OxiaKeyspace keys = new OxiaKeyspace("cluster/a");
        StreamId stream = new StreamId("stream/one");
        String commit = "commit/one";
        String key = keys.streamCommitKey(stream, commit);

        assertThat(keys.parseStreamCommitKey(key))
                .isEqualTo(new StreamCommitKeyIdentity(stream, commit));
        assertThatThrownBy(() -> keys.parseStreamCommitKey(
                        new OxiaKeyspace("cluster/b").streamCommitKey(stream, commit)))
                .isInstanceOf(IllegalArgumentException.class)
                .hasMessageContaining("another cluster");
        assertThatThrownBy(() -> keys.parseStreamCommitKey(
                        key.replace("/commit-log/", "/committed-appends/")))
                .isInstanceOf(IllegalArgumentException.class)
                .hasMessageContaining("unknown family");
        assertThatThrownBy(() -> keys.parseStreamCommitKey(key + "/extra"))
                .isInstanceOf(IllegalArgumentException.class)
                .hasMessageContaining("identity depth");
    }
}
