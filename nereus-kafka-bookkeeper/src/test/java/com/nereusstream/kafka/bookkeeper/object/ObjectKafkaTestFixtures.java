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

package com.nereusstream.kafka.bookkeeper.object;

import com.nereusstream.domain.bytes.Sha256Digest;
import com.nereusstream.domain.identity.Id128;
import com.nereusstream.domain.identity.KafkaTopicId;
import com.nereusstream.domain.identity.StorageEpochId;
import com.nereusstream.domain.identity.TopicBindingId;
import com.nereusstream.domain.protocol.KafkaTopicIncarnationIdentity;
import com.nereusstream.domain.protocol.KafkaTopicName;
import com.nereusstream.kafka.bookkeeper.checkpoint.KafkaProtocolCheckpointStateV1;
import com.nereusstream.kafka.bookkeeper.commit.KafkaProtocolAppendPlanV1;
import com.nereusstream.kafka.bookkeeper.commit.KafkaProtocolBatchDeltaV1;
import com.nereusstream.kafka.bookkeeper.commit.KafkaSpeculativeCommitV1;
import com.nereusstream.kafka.bookkeeper.commit.KafkaTransactionBatchKindV1;
import com.nereusstream.kafka.bookkeeper.nbke2.Nbke2RunBindingV1;
import com.nereusstream.kafka.bookkeeper.protocol.KafkaPartitionFenceV1;
import com.nereusstream.storage.api.bookkeeper.CellProviderScopeId;
import com.nereusstream.storage.api.bookkeeper.StorageRunId;
import java.util.Arrays;
import java.util.List;
import java.util.Optional;

public final class ObjectKafkaTestFixtures {
    private ObjectKafkaTestFixtures() {}

    public static Sha256Digest digest(int seed) {
        byte[] value = new byte[Sha256Digest.LENGTH];
        Arrays.fill(value, (byte) seed);
        return Sha256Digest.copyOf(value);
    }

    public static Nbke2RunBindingV1 runBinding() {
        return new Nbke2RunBindingV1(
                new TopicBindingId(digest(1)),
                new KafkaTopicIncarnationIdentity(new KafkaTopicId(new Id128(2, 3)), new KafkaTopicName("orders")),
                4,
                new StorageEpochId(digest(5)),
                6,
                7,
                new CellProviderScopeId(digest(8)),
                new StorageRunId(new Id128(9, 10)));
    }

    public static KafkaPartitionFenceV1 fence() {
        Nbke2RunBindingV1 binding = runBinding();
        return new KafkaPartitionFenceV1(
                binding.bindingId(),
                binding.topicIncarnation(),
                binding.partitionId(),
                11,
                binding.storageEpochId(),
                binding.creatorOwnerEpoch(),
                binding.kafkaLeaderEpoch());
    }

    public static KafkaProtocolCheckpointStateV1 checkpoint(long coveredThrough) {
        return KafkaProtocolCheckpointStateV1.empty(runBinding(), coveredThrough);
    }

    public static KafkaSpeculativeCommitV1 commit(long startOffset) {
        KafkaProtocolBatchDeltaV1 delta =
                new KafkaProtocolBatchDeltaV1(1, Optional.empty(), KafkaTransactionBatchKindV1.NONE, -1, -1);
        KafkaProtocolAppendPlanV1 plan = new KafkaProtocolAppendPlanV1(fence(), List.of(delta));
        return KafkaSpeculativeCommitV1.assign(plan, startOffset, startOffset + 1);
    }
}
