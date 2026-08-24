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

package com.nereusstream.kafka.bookkeeper.object.publication;

import com.nereusstream.kafka.bookkeeper.commit.KafkaCommittedProducerStateV1;
import com.nereusstream.kafka.bookkeeper.commit.KafkaLeaderEpochIndexV1;
import com.nereusstream.kafka.bookkeeper.commit.KafkaSpeculativeQueueV1;
import com.nereusstream.kafka.bookkeeper.commit.KafkaTransactionStateV1;
import com.nereusstream.kafka.bookkeeper.object.read.KafkaObjectActiveTailStateV1;
import com.nereusstream.kafka.bookkeeper.protocol.KafkaPartitionProtocolStateV1;
import java.util.Objects;

/** Exact Object-profile component set selected by one M2 Kafka publication root. */
public record KafkaObjectCoherentProtocolSnapshotV1(
        KafkaPartitionProtocolStateV1 root,
        KafkaObjectActiveTailStateV1 activeTail,
        KafkaCommittedProducerStateV1 committedProducerState,
        KafkaSpeculativeQueueV1 speculativeQueue,
        KafkaTransactionStateV1 transactionState,
        KafkaLeaderEpochIndexV1 leaderEpochIndex) {
    public KafkaObjectCoherentProtocolSnapshotV1 {
        Objects.requireNonNull(root, "root");
        Objects.requireNonNull(activeTail, "activeTail");
        Objects.requireNonNull(committedProducerState, "committedProducerState");
        Objects.requireNonNull(speculativeQueue, "speculativeQueue");
        Objects.requireNonNull(transactionState, "transactionState");
        Objects.requireNonNull(leaderEpochIndex, "leaderEpochIndex");
        if (activeTail.endOffsetExclusive() != root.frontiers().readableEndOffset()
                || !activeTail.binding().bindingId().equals(root.fence().bindingId())
                || !activeTail
                        .binding()
                        .topicId()
                        .equals(root.fence().topicIncarnation().topicId())
                || activeTail.binding().partitionId() != root.fence().partitionId()
                || !activeTail.binding().storageEpochId().equals(root.fence().storageEpochId())) {
            throw new IllegalArgumentException("Object active-tail coverage/binding differs from the coherent root");
        }
    }
}
