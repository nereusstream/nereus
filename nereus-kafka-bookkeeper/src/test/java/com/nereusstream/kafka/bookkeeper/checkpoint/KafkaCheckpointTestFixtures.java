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

package com.nereusstream.kafka.bookkeeper.checkpoint;

import com.nereusstream.kafka.bookkeeper.commit.KafkaAssignedProtocolBatchV1;
import com.nereusstream.kafka.bookkeeper.commit.KafkaBatchDuplicateIdentityV1;
import com.nereusstream.kafka.bookkeeper.commit.KafkaCommittedProducerStateV1;
import com.nereusstream.kafka.bookkeeper.commit.KafkaLeaderEpochIndexV1;
import com.nereusstream.kafka.bookkeeper.commit.KafkaProtocolBatchDeltaV1;
import com.nereusstream.kafka.bookkeeper.commit.KafkaSpeculativeCommitV1;
import com.nereusstream.kafka.bookkeeper.commit.KafkaTransactionBatchKindV1;
import com.nereusstream.kafka.bookkeeper.commit.KafkaTransactionStateV1;
import com.nereusstream.kafka.bookkeeper.protocol.KafkaPartitionFenceV1;
import com.nereusstream.kafka.bookkeeper.run.KafkaRunTestFixtures;
import java.util.List;
import java.util.Optional;

final class KafkaCheckpointTestFixtures {
    private KafkaCheckpointTestFixtures() {}

    static KafkaPartitionFenceV1 fence() {
        var binding = KafkaRunTestFixtures.binding(6, 11, 5);
        return new KafkaPartitionFenceV1(
                binding.bindingId(),
                binding.topicIncarnation(),
                binding.partitionId(),
                13,
                binding.storageEpochId(),
                binding.creatorOwnerEpoch(),
                binding.kafkaLeaderEpoch());
    }

    static KafkaProtocolCheckpointStateV1 richState() {
        KafkaBatchDuplicateIdentityV1 dataIdentity = new KafkaBatchDuplicateIdentityV1(71, (short) 0, 0, 0);
        KafkaBatchDuplicateIdentityV1 abortIdentity = new KafkaBatchDuplicateIdentityV1(71, (short) 0, 1, 1);
        KafkaProtocolBatchDeltaV1 data = new KafkaProtocolBatchDeltaV1(
                1, Optional.of(dataIdentity), KafkaTransactionBatchKindV1.TRANSACTIONAL_DATA, 71, -1);
        KafkaProtocolBatchDeltaV1 abort = new KafkaProtocolBatchDeltaV1(
                1, Optional.of(abortIdentity), KafkaTransactionBatchKindV1.ABORT_MARKER, 71, 3);
        KafkaSpeculativeCommitV1 commit = new KafkaSpeculativeCommitV1(
                100,
                102,
                fence(),
                List.of(
                        new KafkaAssignedProtocolBatchV1(100, 101, data),
                        new KafkaAssignedProtocolBatchV1(101, 102, abort)));
        KafkaRecoveryCheckpointVectorV1 vector =
                new KafkaRecoveryCheckpointVectorV1(KafkaRunTestFixtures.binding(6, 11, 5), 102, 102, 102, 102);
        return new KafkaProtocolCheckpointStateV1(
                vector,
                KafkaCommittedProducerStateV1.empty().apply(commit),
                KafkaTransactionStateV1.empty().apply(commit),
                KafkaLeaderEpochIndexV1.empty().observe(5, 100));
    }
}
