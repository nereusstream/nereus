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
import com.nereusstream.kafka.bookkeeper.commit.KafkaTransactionStateV1;
import java.util.Objects;

/** Exact post-commit native producer/transaction/leader state carried into one coherent publication cut. */
public record KafkaObjectNativeStateV1(
        KafkaCommittedProducerStateV1 producerState,
        KafkaTransactionStateV1 transactionState,
        KafkaLeaderEpochIndexV1 leaderEpochIndex,
        long highWatermark,
        long lastStableOffset) {
    public KafkaObjectNativeStateV1 {
        Objects.requireNonNull(producerState, "producerState");
        Objects.requireNonNull(transactionState, "transactionState");
        Objects.requireNonNull(leaderEpochIndex, "leaderEpochIndex");
        if (lastStableOffset < 0 || lastStableOffset > highWatermark) {
            throw new IllegalArgumentException("Kafka Object native HW/LSO is outside its domain");
        }
    }
}
