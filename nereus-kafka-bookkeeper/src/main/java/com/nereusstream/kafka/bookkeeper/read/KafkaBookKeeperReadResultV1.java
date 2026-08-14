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

package com.nereusstream.kafka.bookkeeper.read;

import com.nereusstream.kafka.bookkeeper.commit.KafkaTransactionStateV1.CompletedTransactionV1;
import java.util.List;
import java.util.Objects;
import java.util.Optional;

/** Validated K6 read result and protocol-native aborted-transaction metadata. */
public record KafkaBookKeeperReadResultV1(
        KafkaBookKeeperReadOutcomeV1 outcome,
        List<KafkaBookKeeperReadBatchV1> batches,
        List<CompletedTransactionV1> abortedTransactions,
        Optional<KafkaBookKeeperReadCursorV1> nextCursor,
        boolean suppliedCursorAccepted,
        String detail) {
    public KafkaBookKeeperReadResultV1 {
        Objects.requireNonNull(outcome, "outcome");
        batches = List.copyOf(Objects.requireNonNull(batches, "batches"));
        abortedTransactions = List.copyOf(Objects.requireNonNull(abortedTransactions, "abortedTransactions"));
        nextCursor = Objects.requireNonNull(nextCursor, "nextCursor");
        Objects.requireNonNull(detail, "detail");
        if ((outcome == KafkaBookKeeperReadOutcomeV1.FOUND) != !batches.isEmpty()) {
            throw new IllegalArgumentException("only FOUND carries validated RecordBatches");
        }
        if (outcome != KafkaBookKeeperReadOutcomeV1.FOUND
                && (!abortedTransactions.isEmpty() || nextCursor.isPresent())) {
            throw new IllegalArgumentException("non-FOUND result carries response metadata or a cursor");
        }
    }
}
