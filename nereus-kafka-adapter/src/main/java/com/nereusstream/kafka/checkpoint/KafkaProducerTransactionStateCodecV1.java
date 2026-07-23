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

import com.nereusstream.kafka.checkpoint.KafkaProducerTransactionState.AbortedTransaction;
import com.nereusstream.kafka.checkpoint.KafkaProducerTransactionState.BatchMetadata;
import com.nereusstream.kafka.checkpoint.KafkaProducerTransactionState.OpenTransaction;
import com.nereusstream.kafka.checkpoint.KafkaProducerTransactionState.ProducerState;
import com.nereusstream.objectstore.kafka.checkpoint.KafkaCheckpointFormatException;
import com.nereusstream.objectstore.kafka.checkpoint.KafkaCheckpointFormatV1;
import com.nereusstream.objectstore.kafka.checkpoint.KafkaCheckpointSection;
import com.nereusstream.objectstore.kafka.checkpoint.KafkaCheckpointSectionType;
import java.io.ByteArrayOutputStream;
import java.io.DataOutputStream;
import java.io.IOException;
import java.nio.ByteBuffer;
import java.nio.ByteOrder;
import java.util.ArrayList;
import java.util.List;
import java.util.Objects;
import java.util.OptionalLong;

/** Strict big-endian V1 codec for NKC1 sections 1, 2, and 7. */
public final class KafkaProducerTransactionStateCodecV1 {
    private static final int PAYLOAD_VERSION = 1;
    private static final int MIN_PRODUCER_BYTES =
            Long.BYTES + Short.BYTES + Integer.BYTES + Long.BYTES + 1 + 1;
    private static final int BATCH_BYTES =
            Integer.BYTES + Long.BYTES + Integer.BYTES + Long.BYTES;
    private static final int MIN_OPEN_TRANSACTION_BYTES = Long.BYTES + Long.BYTES + 1;
    private static final int ABORTED_TRANSACTION_BYTES =
            Short.BYTES + Long.BYTES * 4;

    public List<KafkaCheckpointSection> encodeSections(
            KafkaProducerTransactionState state, long checkpointOffset) {
        KafkaProducerTransactionState exact = Objects.requireNonNull(state, "state");
        exact.requireCheckpointOffset(checkpointOffset);
        return List.of(
                required(KafkaCheckpointSectionType.PRODUCER_STATE, encodeProducerState(exact)),
                required(
                        KafkaCheckpointSectionType.ABORTED_TRANSACTION_INDEX,
                        encodeAbortedTransactions(exact.abortedTransactions())),
                required(
                        KafkaCheckpointSectionType.OPEN_TRANSACTION_SUMMARY,
                        encodeOpenTransactions(exact.openTransactions())));
    }

    public KafkaProducerTransactionState decodeSections(
            List<KafkaCheckpointSection> sections, long checkpointOffset) {
        if (checkpointOffset < 0) {
            throw new IllegalArgumentException("checkpointOffset must be non-negative");
        }
        try {
            List<KafkaCheckpointSection> exact =
                    List.copyOf(Objects.requireNonNull(sections, "sections"));
            KafkaCheckpointSection producer =
                    locate(exact, KafkaCheckpointSectionType.PRODUCER_STATE);
            KafkaCheckpointSection aborted =
                    locate(exact, KafkaCheckpointSectionType.ABORTED_TRANSACTION_INDEX);
            KafkaCheckpointSection open =
                    locate(exact, KafkaCheckpointSectionType.OPEN_TRANSACTION_SUMMARY);
            ProducerPayload producerPayload = decodeProducerState(producer.payload());
            KafkaProducerTransactionState state = new KafkaProducerTransactionState(
                    producerPayload.mapEndOffset(),
                    producerPayload.producers(),
                    decodeOpenTransactions(open.payload()),
                    decodeAbortedTransactions(aborted.payload()));
            state.requireCheckpointOffset(checkpointOffset);
            return state;
        } catch (KafkaCheckpointFormatException failure) {
            throw failure;
        } catch (IllegalArgumentException failure) {
            throw new KafkaCheckpointFormatException(
                    "malformed NKC1 producer/transaction state", failure);
        }
    }

    private static KafkaCheckpointSection required(
            KafkaCheckpointSectionType type, byte[] payload) {
        return KafkaCheckpointSection.required(type, payload);
    }

    private static KafkaCheckpointSection locate(
            List<KafkaCheckpointSection> sections, KafkaCheckpointSectionType type) {
        KafkaCheckpointSection found = null;
        for (KafkaCheckpointSection section : sections) {
            Objects.requireNonNull(section, "section");
            if (section.sectionType() != type.wireId()) continue;
            if (found != null) {
                throw malformed("duplicate NKC1 " + type + " section");
            }
            if (!section.required()
                    || section.sectionVersion() != PAYLOAD_VERSION
                    || section.sectionFlags() != KafkaCheckpointFormatV1.SECTION_REQUIRED_FLAG) {
                throw malformed("unsupported NKC1 " + type + " section header");
            }
            found = section;
        }
        if (found == null) {
            throw malformed("missing NKC1 " + type + " section");
        }
        return found;
    }

    private static byte[] encodeProducerState(KafkaProducerTransactionState state) {
        return writePayload(output -> {
            output.writeShort(PAYLOAD_VERSION);
            output.writeLong(state.mapEndOffset());
            writeCount(output, state.producers().size());
            for (ProducerState producer : state.producers()) {
                output.writeLong(producer.producerId());
                output.writeShort(producer.producerEpoch());
                output.writeInt(producer.coordinatorEpoch());
                output.writeLong(producer.lastTimestamp());
                output.writeByte(producer.currentTransactionFirstOffset().isPresent() ? 1 : 0);
                if (producer.currentTransactionFirstOffset().isPresent()) {
                    output.writeLong(producer.currentTransactionFirstOffset().getAsLong());
                }
                output.writeByte(producer.batches().size());
                for (BatchMetadata batch : producer.batches()) {
                    output.writeInt(batch.lastSequence());
                    output.writeLong(batch.lastOffset());
                    output.writeInt(batch.offsetDelta());
                    output.writeLong(batch.timestamp());
                }
            }
        });
    }

    private static ProducerPayload decodeProducerState(byte[] payload) {
        Reader input = new Reader(payload);
        input.requireVersion();
        long mapEndOffset = input.readLong("mapEndOffset");
        int producerCount = input.readCount("producerCount", MIN_PRODUCER_BYTES);
        ArrayList<ProducerState> producers = new ArrayList<>(boundedInitialCapacity(producerCount));
        for (int index = 0; index < producerCount; index++) {
            long producerId = input.readLong("producerId");
            short producerEpoch = input.readShort("producerEpoch");
            int coordinatorEpoch = input.readInt("coordinatorEpoch");
            long lastTimestamp = input.readLong("lastTimestamp");
            OptionalLong currentTransactionFirstOffset = input.readOptionalLong(
                    "hasCurrentTxn", "currentTxnFirstOffset");
            int batchCount = input.readUnsignedByte("batchCount");
            if (batchCount > KafkaProducerTransactionState.RETAINED_BATCH_LIMIT
                    || input.remaining() < (long) batchCount * BATCH_BYTES) {
                throw malformed("invalid NKC1 producer retained batch count");
            }
            ArrayList<BatchMetadata> batches = new ArrayList<>(batchCount);
            for (int batchIndex = 0; batchIndex < batchCount; batchIndex++) {
                batches.add(new BatchMetadata(
                        input.readInt("lastSequence"),
                        input.readLong("lastOffset"),
                        input.readInt("offsetDelta"),
                        input.readLong("timestamp")));
            }
            producers.add(new ProducerState(
                    producerId,
                    producerEpoch,
                    coordinatorEpoch,
                    lastTimestamp,
                    currentTransactionFirstOffset,
                    batches));
        }
        input.requireEnd("producer state");
        return new ProducerPayload(mapEndOffset, producers);
    }

    private static byte[] encodeOpenTransactions(List<OpenTransaction> transactions) {
        return writePayload(output -> {
            output.writeShort(PAYLOAD_VERSION);
            writeCount(output, transactions.size());
            for (OpenTransaction transaction : transactions) {
                output.writeLong(transaction.producerId());
                output.writeLong(transaction.firstOffset());
                output.writeByte(transaction.lastOffset().isPresent() ? 1 : 0);
                if (transaction.lastOffset().isPresent()) {
                    output.writeLong(transaction.lastOffset().getAsLong());
                }
            }
        });
    }

    private static List<OpenTransaction> decodeOpenTransactions(byte[] payload) {
        Reader input = new Reader(payload);
        input.requireVersion();
        int transactionCount =
                input.readCount("transactionCount", MIN_OPEN_TRANSACTION_BYTES);
        ArrayList<OpenTransaction> transactions =
                new ArrayList<>(boundedInitialCapacity(transactionCount));
        for (int index = 0; index < transactionCount; index++) {
            transactions.add(new OpenTransaction(
                    input.readLong("producerId"),
                    input.readLong("firstOffset"),
                    input.readOptionalLong("hasLastOffset", "lastOffset")));
        }
        input.requireEnd("open transaction");
        return List.copyOf(transactions);
    }

    private static byte[] encodeAbortedTransactions(List<AbortedTransaction> transactions) {
        return writePayload(output -> {
            output.writeShort(PAYLOAD_VERSION);
            writeCount(output, transactions.size());
            for (AbortedTransaction transaction : transactions) {
                output.writeShort(transaction.version());
                output.writeLong(transaction.producerId());
                output.writeLong(transaction.firstOffset());
                output.writeLong(transaction.lastOffset());
                output.writeLong(transaction.lastStableOffset());
            }
        });
    }

    private static List<AbortedTransaction> decodeAbortedTransactions(byte[] payload) {
        Reader input = new Reader(payload);
        input.requireVersion();
        int entryCount = input.readCount("entryCount", ABORTED_TRANSACTION_BYTES);
        ArrayList<AbortedTransaction> transactions =
                new ArrayList<>(boundedInitialCapacity(entryCount));
        for (int index = 0; index < entryCount; index++) {
            transactions.add(new AbortedTransaction(
                    input.readShort("kafkaAbortedTxnVersion"),
                    input.readLong("producerId"),
                    input.readLong("firstOffset"),
                    input.readLong("lastOffset"),
                    input.readLong("lastStableOffset")));
        }
        input.requireEnd("aborted transaction");
        return List.copyOf(transactions);
    }

    private static byte[] writePayload(PayloadWriter writer) {
        try {
            ByteArrayOutputStream bytes = new ByteArrayOutputStream();
            DataOutputStream output = new DataOutputStream(bytes);
            writer.write(output);
            output.flush();
            byte[] payload = bytes.toByteArray();
            if (payload.length > KafkaCheckpointFormatV1.MAX_SECTION_BYTES) {
                throw malformed("NKC1 state section exceeds its hard limit");
            }
            return payload;
        } catch (IOException failure) {
            throw new KafkaCheckpointFormatException(
                    "failed to encode NKC1 producer/transaction state", failure);
        }
    }

    private static void writeCount(DataOutputStream output, int count) throws IOException {
        if (count < 0) throw new IllegalArgumentException("count must be non-negative");
        output.writeInt(count);
    }

    private static int boundedInitialCapacity(int count) {
        return Math.min(count, 1 << 16);
    }

    private static KafkaCheckpointFormatException malformed(String message) {
        return new KafkaCheckpointFormatException(message);
    }

    @FunctionalInterface
    private interface PayloadWriter {
        void write(DataOutputStream output) throws IOException;
    }

    private record ProducerPayload(long mapEndOffset, List<ProducerState> producers) { }

    private static final class Reader {
        private final ByteBuffer input;

        private Reader(byte[] payload) {
            Objects.requireNonNull(payload, "payload");
            if (payload.length > KafkaCheckpointFormatV1.MAX_SECTION_BYTES) {
                throw malformed("NKC1 state section exceeds its hard limit");
            }
            input = ByteBuffer.wrap(payload).order(ByteOrder.BIG_ENDIAN);
        }

        private void requireVersion() {
            if (readUnsignedShort("sectionVersion") != PAYLOAD_VERSION) {
                throw malformed("unsupported NKC1 producer/transaction payload version");
            }
        }

        private int readCount(String field, int minimumEntryBytes) {
            long count = Integer.toUnsignedLong(readInt(field));
            if (count > Integer.MAX_VALUE
                    || count > remaining() / minimumEntryBytes) {
                throw malformed("invalid NKC1 " + field);
            }
            return (int) count;
        }

        private OptionalLong readOptionalLong(String flagField, String valueField) {
            int flag = readUnsignedByte(flagField);
            if (flag == 0) return OptionalLong.empty();
            if (flag != 1) throw malformed("invalid NKC1 " + flagField);
            return OptionalLong.of(readLong(valueField));
        }

        private int readUnsignedByte(String field) {
            requireRemaining(1, field);
            return Byte.toUnsignedInt(input.get());
        }

        private int readUnsignedShort(String field) {
            requireRemaining(Short.BYTES, field);
            return Short.toUnsignedInt(input.getShort());
        }

        private short readShort(String field) {
            requireRemaining(Short.BYTES, field);
            return input.getShort();
        }

        private int readInt(String field) {
            requireRemaining(Integer.BYTES, field);
            return input.getInt();
        }

        private long readLong(String field) {
            requireRemaining(Long.BYTES, field);
            return input.getLong();
        }

        private int remaining() {
            return input.remaining();
        }

        private void requireRemaining(int bytes, String field) {
            if (bytes < 0 || input.remaining() < bytes) {
                throw malformed("truncated NKC1 " + field);
            }
        }

        private void requireEnd(String section) {
            if (input.hasRemaining()) {
                throw malformed("NKC1 " + section + " section has trailing bytes");
            }
        }
    }
}
