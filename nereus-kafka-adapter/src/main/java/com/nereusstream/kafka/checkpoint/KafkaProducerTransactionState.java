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

package com.nereusstream.kafka.checkpoint;

import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.Objects;
import java.util.OptionalLong;

/**
 * Kafka-artifact-neutral canonical state for NKC1 producer, open-transaction, and aborted-transaction sections.
 *
 * <p>Committed RecordBatch bytes remain truth. This state is only a bounded recovery accelerator and therefore rejects
 * collection order, offset, sequence, or cross-section facts that cannot be imported into a fresh stock state manager
 * without interpretation.
 */
public record KafkaProducerTransactionState(
        long mapEndOffset,
        List<ProducerState> producers,
        List<OpenTransaction> openTransactions,
        List<AbortedTransaction> abortedTransactions) {
    public static final int RETAINED_BATCH_LIMIT = 5;
    public static final short ABORTED_TRANSACTION_VERSION = 0;
    public static final long NO_TIMESTAMP = -1L;
    public static final short NO_PRODUCER_EPOCH = -1;
    public static final int NO_COORDINATOR_EPOCH = -1;

    public KafkaProducerTransactionState {
        if (mapEndOffset < 0) {
            throw new IllegalArgumentException("Kafka producer map end offset must be non-negative");
        }
        producers = List.copyOf(Objects.requireNonNull(producers, "producers"));
        openTransactions = List.copyOf(Objects.requireNonNull(openTransactions, "openTransactions"));
        abortedTransactions = List.copyOf(Objects.requireNonNull(abortedTransactions, "abortedTransactions"));
        validateProducers(producers, mapEndOffset);
        validateOpenTransactions(producers, openTransactions, mapEndOffset);
        validateAbortedTransactions(abortedTransactions, mapEndOffset);
    }

    public void requireCheckpointOffset(long checkpointOffset) {
        if (checkpointOffset < 0 || mapEndOffset != checkpointOffset) {
            throw new IllegalArgumentException(
                    "Kafka producer map end offset must equal the NKC1 checkpoint offset");
        }
    }

    private static void validateProducers(List<ProducerState> producers, long mapEndOffset) {
        long previousProducerId = -1;
        for (ProducerState producer : producers) {
            Objects.requireNonNull(producer, "producer");
            if (producer.producerId() <= previousProducerId) {
                throw new IllegalArgumentException("Kafka producer IDs must be unique and strictly sorted");
            }
            previousProducerId = producer.producerId();
            if (producer.currentTransactionFirstOffset().isPresent()
                    && producer.currentTransactionFirstOffset().getAsLong() >= mapEndOffset) {
                throw new IllegalArgumentException(
                        "Kafka producer current transaction must start before map end");
            }
            BatchMetadata previous = null;
            for (BatchMetadata batch : producer.batches()) {
                if (batch.lastOffset() >= mapEndOffset) {
                    throw new IllegalArgumentException(
                            "Kafka producer retained batch must end before map end");
                }
                if (previous != null) {
                    if (batch.firstOffset() <= previous.lastOffset()) {
                        throw new IllegalArgumentException(
                                "Kafka producer retained batch offsets must be strictly ordered");
                    }
                    if (batch.firstSequence() != incrementSequence(previous.lastSequence())) {
                        throw new IllegalArgumentException(
                                "Kafka producer retained batch sequences must be contiguous with wrap");
                    }
                }
                previous = batch;
            }
        }
    }

    private static void validateOpenTransactions(
            List<ProducerState> producers,
            List<OpenTransaction> openTransactions,
            long mapEndOffset) {
        Map<Long, ProducerState> byProducer = new HashMap<>();
        for (ProducerState producer : producers) {
            byProducer.put(producer.producerId(), producer);
        }
        long previousFirstOffset = -1;
        long previousProducerId = -1;
        Map<Long, OpenTransaction> openByProducer = new HashMap<>();
        for (OpenTransaction transaction : openTransactions) {
            Objects.requireNonNull(transaction, "openTransaction");
            if (transaction.firstOffset() < previousFirstOffset
                    || (transaction.firstOffset() == previousFirstOffset
                    && transaction.producerId() <= previousProducerId)) {
                throw new IllegalArgumentException(
                        "Kafka open transactions must be strictly sorted by first offset and producer");
            }
            if (transaction.firstOffset() >= mapEndOffset) {
                throw new IllegalArgumentException(
                        "Kafka open transaction must start before map end");
            }
            if (transaction.lastOffset().isPresent()) {
                throw new IllegalArgumentException(
                        "normal NKC1 checkpoint barriers cannot contain completed open transactions");
            }
            ProducerState producer = byProducer.get(transaction.producerId());
            if (producer == null
                    || producer.currentTransactionFirstOffset().isEmpty()
                    || producer.currentTransactionFirstOffset().getAsLong()
                    != transaction.firstOffset()
                    || openByProducer.put(transaction.producerId(), transaction) != null) {
                throw new IllegalArgumentException(
                        "Kafka open transaction does not match exactly one producer state entry");
            }
            previousFirstOffset = transaction.firstOffset();
            previousProducerId = transaction.producerId();
        }
        for (ProducerState producer : producers) {
            if (producer.currentTransactionFirstOffset().isPresent()
                    != openByProducer.containsKey(producer.producerId())) {
                throw new IllegalArgumentException(
                        "Kafka producer/open-transaction sections are not equivalent");
            }
        }
    }

    private static void validateAbortedTransactions(
            List<AbortedTransaction> abortedTransactions, long mapEndOffset) {
        AbortedTransaction previous = null;
        for (AbortedTransaction transaction : abortedTransactions) {
            Objects.requireNonNull(transaction, "abortedTransaction");
            if (transaction.lastOffset() >= mapEndOffset
                    || transaction.lastStableOffset() > mapEndOffset) {
                throw new IllegalArgumentException(
                        "Kafka aborted transaction extends beyond the checkpoint");
            }
            if (previous != null
                    && previous.lastOffset() >= transaction.lastOffset()) {
                throw new IllegalArgumentException(
                        "Kafka aborted transaction marker offsets must be strictly increasing");
            }
            previous = transaction;
        }
    }

    private static int incrementSequence(int sequence) {
        return sequence == Integer.MAX_VALUE ? 0 : sequence + 1;
    }

    private static int decrementSequence(int sequence, int decrement) {
        if (decrement > sequence) {
            return Math.toIntExact(
                    sequence - (long) decrement + Integer.MAX_VALUE + 1L);
        }
        return sequence - decrement;
    }

    /** One stock ProducerStateEntry image. */
    public record ProducerState(
            long producerId,
            short producerEpoch,
            int coordinatorEpoch,
            long lastTimestamp,
            OptionalLong currentTransactionFirstOffset,
            List<BatchMetadata> batches) {
        public ProducerState {
            currentTransactionFirstOffset =
                    Objects.requireNonNull(currentTransactionFirstOffset, "currentTransactionFirstOffset");
            batches = List.copyOf(Objects.requireNonNull(batches, "batches"));
            if (producerId < 0
                    || producerEpoch < NO_PRODUCER_EPOCH
                    || coordinatorEpoch < NO_COORDINATOR_EPOCH
                    || lastTimestamp < NO_TIMESTAMP
                    || (currentTransactionFirstOffset.isPresent()
                    && currentTransactionFirstOffset.getAsLong() < 0)
                    || batches.size() > RETAINED_BATCH_LIMIT) {
                throw new IllegalArgumentException("invalid Kafka producer state");
            }
            if (producerEpoch == NO_PRODUCER_EPOCH
                    && (!batches.isEmpty() || currentTransactionFirstOffset.isPresent())) {
                throw new IllegalArgumentException(
                        "empty Kafka producer epoch cannot retain batches or a transaction");
            }
            batches.forEach(batch -> Objects.requireNonNull(batch, "batch"));
            if (!batches.isEmpty()
                    && lastTimestamp != batches.get(batches.size() - 1).timestamp()) {
                throw new IllegalArgumentException(
                        "Kafka producer timestamp must match its newest retained batch");
            }
        }
    }

    /** Stock BatchMetadata fields in oldest-to-newest order. */
    public record BatchMetadata(
            int lastSequence,
            long lastOffset,
            int offsetDelta,
            long timestamp) {
        public BatchMetadata {
            if (lastSequence < 0
                    || lastOffset < 0
                    || offsetDelta < 0
                    || offsetDelta > lastOffset
                    || timestamp < NO_TIMESTAMP) {
                throw new IllegalArgumentException("invalid Kafka retained producer batch");
            }
        }

        public int firstSequence() {
            return decrementSequence(lastSequence, offsetDelta);
        }

        public long firstOffset() {
            return lastOffset - offsetDelta;
        }
    }

    /** Ordered first-unstable state. Completed entries are reserved for a future explicitly flagged capture mode. */
    public record OpenTransaction(
            long producerId,
            long firstOffset,
            OptionalLong lastOffset) {
        public OpenTransaction {
            lastOffset = Objects.requireNonNull(lastOffset, "lastOffset");
            if (producerId < 0
                    || firstOffset < 0
                    || (lastOffset.isPresent() && lastOffset.getAsLong() < firstOffset)) {
                throw new IllegalArgumentException("invalid Kafka open transaction");
            }
        }
    }

    /** Canonical stock AbortedTxn fields, independent of its ByteBuffer layout. */
    public record AbortedTransaction(
            short version,
            long producerId,
            long firstOffset,
            long lastOffset,
            long lastStableOffset) {
        public AbortedTransaction {
            if (version != ABORTED_TRANSACTION_VERSION
                    || producerId < 0
                    || firstOffset < 0
                    || lastOffset < firstOffset
                    || lastStableOffset < 0) {
                throw new IllegalArgumentException("invalid Kafka aborted transaction");
            }
        }
    }
}
