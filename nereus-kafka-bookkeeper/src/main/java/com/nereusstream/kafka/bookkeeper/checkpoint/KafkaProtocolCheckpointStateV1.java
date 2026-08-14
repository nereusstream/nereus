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
import com.nereusstream.kafka.bookkeeper.nbke2.Nbke2ProtocolCheckpointV1;
import java.util.Objects;

/** Exact protocol components named by one compatible recovery vector. */
public record KafkaProtocolCheckpointStateV1(
        KafkaRecoveryCheckpointVectorV1 vector,
        KafkaCommittedProducerStateV1 producerState,
        KafkaTransactionStateV1 transactionState,
        KafkaLeaderEpochIndexV1 leaderEpochIndex) {
    public KafkaProtocolCheckpointStateV1 {
        Objects.requireNonNull(vector, "vector");
        Objects.requireNonNull(producerState, "producerState");
        Objects.requireNonNull(transactionState, "transactionState");
        Objects.requireNonNull(leaderEpochIndex, "leaderEpochIndex");
        long producerCoveredThrough = vector.producerStateCoveredThrough();
        producerState.producers().values().forEach(producer -> {
            if (producer.lastOffset() >= producerCoveredThrough) {
                throw new IllegalArgumentException("producer checkpoint state escapes its component coverage");
            }
        });
        transactionState.ongoingTransactions().values().forEach(transaction -> {
            if (transaction.firstOffset() >= vector.transactionIndexCoveredThrough()) {
                throw new IllegalArgumentException("ongoing transaction escapes its checkpoint coverage");
            }
        });
        transactionState.completedTransactions().forEach(transaction -> {
            if (transaction.markerEndOffsetExclusive() > vector.transactionIndexCoveredThrough()) {
                throw new IllegalArgumentException("completed transaction escapes its checkpoint coverage");
            }
        });
        leaderEpochIndex.startOffsets().values().forEach(startOffset -> {
            if (startOffset >= vector.leaderEpochCoveredThrough()) {
                throw new IllegalArgumentException("leader-epoch state escapes its checkpoint coverage");
            }
        });
    }

    public static KafkaProtocolCheckpointStateV1 empty(
            com.nereusstream.kafka.bookkeeper.nbke2.Nbke2RunBindingV1 runBinding, long startOffset) {
        KafkaRecoveryCheckpointVectorV1 vector =
                new KafkaRecoveryCheckpointVectorV1(runBinding, startOffset, startOffset, startOffset, startOffset);
        return new KafkaProtocolCheckpointStateV1(
                vector,
                KafkaCommittedProducerStateV1.empty(),
                KafkaTransactionStateV1.empty(),
                KafkaLeaderEpochIndexV1.empty());
    }

    public Nbke2ProtocolCheckpointV1 toNbke2() {
        KafkaProtocolCheckpointSectionsV1 sections = KafkaProtocolCheckpointCodecV1.encode(this);
        return new Nbke2ProtocolCheckpointV1(
                vector.runBinding(),
                vector.rangeIndexCoveredThrough(),
                vector.producerStateCoveredThrough(),
                vector.transactionIndexCoveredThrough(),
                vector.leaderEpochCoveredThrough(),
                sections.producerState(),
                sections.transactionIndex(),
                sections.leaderEpochIndex());
    }

    public static KafkaProtocolCheckpointStateV1 fromNbke2(Nbke2ProtocolCheckpointV1 checkpoint) {
        Objects.requireNonNull(checkpoint, "checkpoint");
        return KafkaProtocolCheckpointCodecV1.decode(
                KafkaRecoveryCheckpointVectorV1.from(checkpoint),
                new KafkaProtocolCheckpointSectionsV1(
                        checkpoint.producerState(), checkpoint.transactionIndex(), checkpoint.leaderEpochIndex()));
    }
}
