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

import com.nereusstream.kafka.bookkeeper.commit.KafkaCommittedProducerStateV1;
import com.nereusstream.kafka.bookkeeper.commit.KafkaLeaderEpochIndexV1;
import com.nereusstream.kafka.bookkeeper.commit.KafkaTransactionStateV1;
import java.util.Objects;

/** Complete protocol recovery state, independent of a storage run and its physical read locators. */
public record KafkaProtocolCheckpointComponentsV1(
        KafkaCommittedProducerStateV1 producerState,
        KafkaTransactionStateV1 transactionState,
        KafkaLeaderEpochIndexV1 leaderEpochIndex) {
    public KafkaProtocolCheckpointComponentsV1 {
        Objects.requireNonNull(producerState, "producerState");
        Objects.requireNonNull(transactionState, "transactionState");
        Objects.requireNonNull(leaderEpochIndex, "leaderEpochIndex");
    }

    public void requireCoverage(long producerThrough, long transactionThrough, long epochThrough) {
        if (producerThrough < 0 || transactionThrough < 0 || epochThrough < 0) {
            throw new IllegalArgumentException("checkpoint coverage is outside the Kafka offset domain");
        }
        producerState.producers().values().forEach(producer -> {
            if (Math.max(producer.lastOffset(), producer.lastMarkerOffset()) >= producerThrough) {
                throw new IllegalArgumentException("producer checkpoint state escapes its component coverage");
            }
        });
        transactionState.ongoingTransactions().values().forEach(transaction -> {
            if (transaction.firstOffset() >= transactionThrough) {
                throw new IllegalArgumentException("ongoing transaction escapes its checkpoint coverage");
            }
        });
        transactionState.completedTransactions().forEach(transaction -> {
            if (transaction.markerEndOffsetExclusive() > transactionThrough) {
                throw new IllegalArgumentException("completed transaction escapes its checkpoint coverage");
            }
        });
        leaderEpochIndex.startOffsets().values().forEach(startOffset -> {
            if (startOffset >= epochThrough) {
                throw new IllegalArgumentException("leader-epoch state escapes its checkpoint coverage");
            }
        });
    }
}
