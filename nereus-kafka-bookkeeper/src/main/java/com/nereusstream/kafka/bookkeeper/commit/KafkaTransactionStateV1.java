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

package com.nereusstream.kafka.bookkeeper.commit;

import java.util.ArrayList;
import java.util.Collections;
import java.util.List;
import java.util.NavigableMap;
import java.util.Objects;
import java.util.OptionalLong;
import java.util.TreeMap;

/** Immutable ongoing/completed/aborted partition transaction state and first-unstable offset. */
public record KafkaTransactionStateV1(
        NavigableMap<Long, OngoingTransactionV1> ongoingTransactions,
        List<CompletedTransactionV1> completedTransactions) {
    public KafkaTransactionStateV1 {
        ongoingTransactions = Collections.unmodifiableNavigableMap(
                new TreeMap<>(Objects.requireNonNull(ongoingTransactions, "ongoingTransactions")));
        completedTransactions = List.copyOf(Objects.requireNonNull(completedTransactions, "completedTransactions"));
        ongoingTransactions.forEach((producerId, transaction) -> {
            if (producerId != transaction.producerId()) {
                throw new IllegalArgumentException("ongoing transaction key differs from its producer");
            }
        });
        long previousMarkerEnd = -1;
        for (CompletedTransactionV1 transaction : completedTransactions) {
            if (transaction.markerEndOffsetExclusive() < previousMarkerEnd) {
                throw new IllegalArgumentException("completed transactions are not in marker offset order");
            }
            previousMarkerEnd = transaction.markerEndOffsetExclusive();
        }
    }

    public static KafkaTransactionStateV1 empty() {
        return new KafkaTransactionStateV1(new TreeMap<>(), List.of());
    }

    public OptionalLong firstUnstableOffset(long highWatermark) {
        if (highWatermark < 0) {
            throw new IllegalArgumentException("high watermark must be non-negative");
        }
        long first = Long.MAX_VALUE;
        for (OngoingTransactionV1 transaction : ongoingTransactions.values()) {
            first = Math.min(first, transaction.firstOffset());
        }
        for (CompletedTransactionV1 transaction : completedTransactions) {
            if (transaction.markerEndOffsetExclusive() > highWatermark) {
                first = Math.min(first, transaction.firstOffset());
            }
        }
        return first == Long.MAX_VALUE ? OptionalLong.empty() : OptionalLong.of(first);
    }

    public List<CompletedTransactionV1> abortedTransactions() {
        return completedTransactions.stream()
                .filter(CompletedTransactionV1::aborted)
                .toList();
    }

    public KafkaTransactionStateV1 apply(KafkaSpeculativeCommitV1 commit) {
        TreeMap<Long, OngoingTransactionV1> ongoing = new TreeMap<>(ongoingTransactions);
        List<CompletedTransactionV1> completed = new ArrayList<>(completedTransactions);
        for (KafkaAssignedProtocolBatchV1 batch : commit.batches()) {
            KafkaProtocolBatchDeltaV1 delta = batch.delta();
            switch (delta.transactionKind()) {
                case NONE -> {
                    // No partition transaction state.
                }
                case TRANSACTIONAL_DATA ->
                    ongoing.putIfAbsent(
                            delta.transactionalProducerId(),
                            new OngoingTransactionV1(delta.transactionalProducerId(), batch.startOffset()));
                case COMMIT_MARKER, ABORT_MARKER -> {
                    OngoingTransactionV1 opened = ongoing.remove(delta.transactionalProducerId());
                    if (opened == null) {
                        throw new IllegalArgumentException("transaction marker has no ongoing partition transaction");
                    }
                    completed.add(new CompletedTransactionV1(
                            delta.transactionalProducerId(),
                            opened.firstOffset(),
                            batch.endOffsetExclusive(),
                            delta.transactionKind() == KafkaTransactionBatchKindV1.ABORT_MARKER,
                            delta.coordinatorEpoch()));
                }
            }
        }
        return new KafkaTransactionStateV1(ongoing, completed);
    }

    public record OngoingTransactionV1(long producerId, long firstOffset) {
        public OngoingTransactionV1 {
            if (producerId < 0 || firstOffset < 0) {
                throw new IllegalArgumentException("ongoing transaction is outside its domain");
            }
        }
    }

    public record CompletedTransactionV1(
            long producerId, long firstOffset, long markerEndOffsetExclusive, boolean aborted, int coordinatorEpoch) {
        public CompletedTransactionV1 {
            if (producerId < 0 || firstOffset < 0 || markerEndOffsetExclusive <= firstOffset || coordinatorEpoch < 0) {
                throw new IllegalArgumentException("completed transaction is outside its domain");
            }
        }
    }
}
