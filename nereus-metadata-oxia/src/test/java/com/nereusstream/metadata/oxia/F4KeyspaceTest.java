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

import com.nereusstream.api.ObjectKey;
import com.nereusstream.api.ObjectKeyHash;
import com.nereusstream.api.ReadView;
import com.nereusstream.api.StreamId;
import com.nereusstream.metadata.oxia.records.ObjectProtectionType;
import org.junit.jupiter.api.Test;

class F4KeyspaceTest {
    private final F4Keyspace keys = new F4Keyspace("test-cluster");
    private final StreamId streamId = new StreamId("stream-1");

    @Test
    void keepsCommittedAndTopicCompactedIndexesInDisjointNamespaces() {
        assertThat(keys.generationIndexKey(streamId, ReadView.COMMITTED, 12, 3))
                .isEqualTo("/nereus/clusters/test-cluster/streams/stream-1/offset-index/0000000000000000012/0000000000000000003");
        assertThat(keys.generationIndexKey(streamId, ReadView.TOPIC_COMPACTED, 12, 3))
                .isEqualTo("/nereus/clusters/test-cluster/streams/stream-1/views/v1/topic-compacted/offset-index/"
                        + "0000000000000000012/0000000000000000003");
    }

    @Test
    void rangeUpperBoundIncludesEverySuffixEvenAtLongMaxValue() {
        String from = keys.generationIndexScanFrom(streamId, ReadView.COMMITTED, Long.MAX_VALUE);
        String to = keys.generationIndexScanToAfterEnd(streamId, ReadView.COMMITTED, Long.MAX_VALUE);

        assertThat(from).endsWith("/9223372036854775807/");
        assertThat(to).endsWith("/92233720368547758070");
        assertThat(keys.generationIndexKey(streamId, ReadView.COMMITTED, Long.MAX_VALUE, Long.MAX_VALUE))
                .isBetween(from, to);
    }

    @Test
    void streamRegistryUsesFrozenLowSixBitShardAndDedicatedPartition() {
        assertThat(keys.materializationRegistryShard(streamId)).isEqualTo(51);
        assertThat(keys.materializationRegistryKey(streamId))
                .isEqualTo("/nereus/clusters/test-cluster/materialization/v1/stream-registry/51/stream-1");
        assertThat(keys.materializationRegistryPartitionKey(51).value())
                .isEqualTo("materialization-registry-v1-51");
        assertThatThrownBy(() -> keys.materializationRegistryPrefix(64))
                .isInstanceOf(IllegalArgumentException.class);
    }

    @Test
    void physicalRecordsShareTheFrozenFirstHashByteShardAndPartition() {
        ObjectKeyHash hash = ObjectKeyHash.from(new ObjectKey("cluster/objects/example"));

        assertThat(keys.physicalObjectShard(hash)).isEqualTo(246);
        assertThat(keys.physicalRootKey(hash))
                .isEqualTo("/nereus/clusters/test-cluster/physical-objects/v1/246/roots/" + hash.value());
        assertThat(keys.readerLeaseKey(hash, "process/run"))
                .startsWith("/nereus/clusters/test-cluster/physical-objects/v1/246/objects/" + hash.value())
                .endsWith("/readers/b32-obzg6y3fonzs64tvny");
        assertThat(keys.protectionKey(hash, ObjectProtectionType.VISIBLE_GENERATION, "index/1"))
                .startsWith("/nereus/clusters/test-cluster/physical-objects/v1/246/objects/" + hash.value())
                .endsWith("/protections/01/b32-nfxgizlyf4yq");
        assertThat(keys.physicalObjectPartitionKey(hash).value())
                .isEqualTo("physical-object-v1-246");
    }

    @Test
    void activationAndStreamKeysCannotSharePartitions() {
        assertThat(keys.generationProtocolActivationKey())
                .isEqualTo("/nereus/clusters/test-cluster/capabilities/generation-v1/activation");
        assertThat(keys.generationProtocolActivationPartitionKey())
                .isNotEqualTo(keys.streamPartitionKey(streamId));
    }
}
