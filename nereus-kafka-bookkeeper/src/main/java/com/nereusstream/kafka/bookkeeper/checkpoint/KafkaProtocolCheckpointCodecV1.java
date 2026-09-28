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

import com.nereusstream.domain.bytes.CanonicalBytes;
import com.nereusstream.kafka.bookkeeper.commit.KafkaBatchDuplicateIdentityV1;
import com.nereusstream.kafka.bookkeeper.commit.KafkaCommittedProducerStateV1;
import com.nereusstream.kafka.bookkeeper.commit.KafkaLeaderEpochIndexV1;
import com.nereusstream.kafka.bookkeeper.commit.KafkaProducerBatchResultV1;
import com.nereusstream.kafka.bookkeeper.commit.KafkaProducerSessionStateV1;
import com.nereusstream.kafka.bookkeeper.commit.KafkaTransactionStateV1;
import com.nereusstream.kafka.bookkeeper.nbke2.Nbke2ConstantsV1;
import java.io.ByteArrayInputStream;
import java.io.ByteArrayOutputStream;
import java.io.DataInputStream;
import java.io.DataOutputStream;
import java.io.EOFException;
import java.io.IOException;
import java.util.ArrayList;
import java.util.List;
import java.util.TreeMap;

/** Strict-EOF canonical KPC1 codec for the three profile-neutral checkpoint sections. */
public final class KafkaProtocolCheckpointCodecV1 {
    private static final int PRODUCER_MAGIC = 0x4b_50_43_31;
    private static final int PRODUCER_MAGIC_V3 = 0x4b504333; // KPC3
    private static final int PRODUCER_MAGIC_V2 = 0x4b_50_43_32;
    private static final int TRANSACTION_MAGIC = 0x4b_54_43_31;
    private static final int LEADER_EPOCH_MAGIC = 0x4b_4c_43_31;
    private static final int MAX_ROWS = 65_536;

    private KafkaProtocolCheckpointCodecV1() {}

    public static KafkaProtocolCheckpointSectionsV1 encode(KafkaProtocolCheckpointStateV1 state) {
        return new KafkaProtocolCheckpointSectionsV1(
                encodeProducers(state.producerState()),
                encodeTransactions(state.transactionState()),
                encodeLeaderEpochs(state));
    }

    public static KafkaProtocolCheckpointStateV1 decode(
            KafkaRecoveryCheckpointVectorV1 vector, KafkaProtocolCheckpointSectionsV1 sections) {
        var source = decodeLeaderEpochs(sections.leaderEpochIndex());
        return new KafkaProtocolCheckpointStateV1(
                vector,
                decodeProducers(sections.producerState()),
                decodeTransactions(sections.transactionIndex()),
                source.epochs(),
                source.indexes());
    }

    private static CanonicalBytes encodeProducers(KafkaCommittedProducerStateV1 state) {
        boolean markerState = state.producers().values().stream()
                .anyMatch(producer -> producer.coordinatorEpoch() >= 0
                        || producer.recentBatches().isEmpty());
        boolean timestampState = state.producers().values().stream()
                .anyMatch(producer -> producer.lastTimestamp() != -1
                        || producer.recentBatches().stream().anyMatch(batch -> batch.maxTimestamp() != -1));
        return encode(out -> {
            out.writeInt(timestampState ? PRODUCER_MAGIC_V3 : markerState ? PRODUCER_MAGIC_V2 : PRODUCER_MAGIC);
            out.writeInt(state.producers().size());
            for (KafkaProducerSessionStateV1 producer : state.producers().values()) {
                out.writeLong(producer.producerId());
                out.writeShort(producer.producerEpoch());
                out.writeInt(producer.lastSequence());
                out.writeLong(producer.lastOffset());
                if (markerState || timestampState) {
                    out.writeInt(producer.coordinatorEpoch());
                    out.writeLong(producer.lastMarkerOffset());
                }
                if (timestampState) {
                    out.writeLong(producer.lastTimestamp());
                }
                out.writeInt(producer.recentBatches().size());
                for (KafkaProducerBatchResultV1 batch : producer.recentBatches()) {
                    KafkaBatchDuplicateIdentityV1 identity = batch.identity();
                    out.writeLong(identity.producerId());
                    out.writeShort(identity.producerEpoch());
                    out.writeInt(identity.baseSequence());
                    out.writeInt(identity.lastSequence());
                    out.writeLong(batch.startOffset());
                    out.writeLong(batch.endOffsetExclusive());
                    if (timestampState) {
                        out.writeLong(batch.maxTimestamp());
                    }
                }
            }
        });
    }

    private static KafkaCommittedProducerStateV1 decodeProducers(CanonicalBytes section) {
        return decode(section, in -> {
            int magic = in.readInt();
            if (magic != PRODUCER_MAGIC && magic != PRODUCER_MAGIC_V2 && magic != PRODUCER_MAGIC_V3) {
                throw new IllegalArgumentException("unknown producer checkpoint section wire");
            }
            int count = count(in, "producer");
            TreeMap<Long, KafkaProducerSessionStateV1> producers = new TreeMap<>();
            for (int index = 0; index < count; index++) {
                long producerId = in.readLong();
                short producerEpoch = in.readShort();
                int lastSequence = in.readInt();
                long lastOffset = in.readLong();
                int coordinatorEpoch = magic != PRODUCER_MAGIC ? in.readInt() : -1;
                long lastMarkerOffset = magic != PRODUCER_MAGIC ? in.readLong() : -1;
                long lastTimestamp = magic == PRODUCER_MAGIC_V3 ? in.readLong() : -1;
                int recentCount = count(in, "recent producer result");
                if (recentCount > KafkaProducerSessionStateV1.MAX_RECENT_BATCHES) {
                    throw new IllegalArgumentException("recent producer result count is outside its bound");
                }
                List<KafkaProducerBatchResultV1> recent = new ArrayList<>(recentCount);
                for (int recentIndex = 0; recentIndex < recentCount; recentIndex++) {
                    KafkaBatchDuplicateIdentityV1 identity = new KafkaBatchDuplicateIdentityV1(
                            in.readLong(), in.readShort(), in.readInt(), in.readInt());
                    recent.add(new KafkaProducerBatchResultV1(
                            identity, in.readLong(), in.readLong(), magic == PRODUCER_MAGIC_V3 ? in.readLong() : -1));
                }
                KafkaProducerSessionStateV1 decoded = new KafkaProducerSessionStateV1(
                        producerId,
                        producerEpoch,
                        lastSequence,
                        lastOffset,
                        recent,
                        coordinatorEpoch,
                        lastMarkerOffset,
                        lastTimestamp);
                if (producers.put(producerId, decoded) != null) {
                    throw new IllegalArgumentException("duplicate producer checkpoint row");
                }
            }
            return new KafkaCommittedProducerStateV1(producers);
        });
    }

    private static CanonicalBytes encodeTransactions(KafkaTransactionStateV1 state) {
        return encode(out -> {
            out.writeInt(TRANSACTION_MAGIC);
            out.writeInt(state.ongoingTransactions().size());
            for (KafkaTransactionStateV1.OngoingTransactionV1 transaction :
                    state.ongoingTransactions().values()) {
                out.writeLong(transaction.producerId());
                out.writeLong(transaction.firstOffset());
            }
            out.writeInt(state.completedTransactions().size());
            for (KafkaTransactionStateV1.CompletedTransactionV1 transaction : state.completedTransactions()) {
                out.writeLong(transaction.producerId());
                out.writeLong(transaction.firstOffset());
                out.writeLong(transaction.markerEndOffsetExclusive());
                out.writeBoolean(transaction.aborted());
                out.writeInt(transaction.coordinatorEpoch());
            }
        });
    }

    private static KafkaTransactionStateV1 decodeTransactions(CanonicalBytes section) {
        return decode(section, in -> {
            requireMagic(in, TRANSACTION_MAGIC, "transaction");
            int ongoingCount = count(in, "ongoing transaction");
            TreeMap<Long, KafkaTransactionStateV1.OngoingTransactionV1> ongoing = new TreeMap<>();
            for (int index = 0; index < ongoingCount; index++) {
                KafkaTransactionStateV1.OngoingTransactionV1 transaction =
                        new KafkaTransactionStateV1.OngoingTransactionV1(in.readLong(), in.readLong());
                if (ongoing.put(transaction.producerId(), transaction) != null) {
                    throw new IllegalArgumentException("duplicate ongoing transaction checkpoint row");
                }
            }
            int completedCount = count(in, "completed transaction");
            List<KafkaTransactionStateV1.CompletedTransactionV1> completed = new ArrayList<>(completedCount);
            for (int index = 0; index < completedCount; index++) {
                completed.add(new KafkaTransactionStateV1.CompletedTransactionV1(
                        in.readLong(), in.readLong(), in.readLong(), in.readBoolean(), in.readInt()));
            }
            return new KafkaTransactionStateV1(ongoing, completed);
        });
    }

    private static CanonicalBytes encodeLeaderEpochs(KafkaProtocolCheckpointStateV1 state) {
        return encode(out -> {
            out.writeInt(state.readIndexes().isEmpty() ? LEADER_EPOCH_MAGIC : 0x4b4c4332);
            out.writeInt(state.leaderEpochIndex().startOffsets().size());
            for (var entry : state.leaderEpochIndex().startOffsets().entrySet()) {
                out.writeInt(entry.getKey());
                out.writeLong(entry.getValue());
            }
            if (!state.readIndexes().isEmpty()) {
                out.writeInt(state.readIndexes().size());
                for (var source : state.readIndexes()) {
                    out.write(source.runId().value().bytes().toByteArray());
                    out.writeInt(source.rows().size());
                    for (var row : source.rows()) {
                        out.writeLong(row.startOffset());
                        out.writeLong(row.endOffsetExclusive());
                        out.writeLong(row.entryId());
                        out.writeLong(row.groupOrdinal());
                        out.writeLong(row.payloadBytes());
                    }
                }
            }
        });
    }

    private record Sources(KafkaLeaderEpochIndexV1 epochs, List<KafkaCheckpointReadIndexV1> indexes) {}

    private static Sources decodeLeaderEpochs(CanonicalBytes section) {
        return decode(section, in -> {
            int magic = in.readInt();
            if (magic != LEADER_EPOCH_MAGIC && magic != 0x4b4c4332) {
                throw new IllegalArgumentException("unknown leader/source checkpoint magic/version");
            }
            int count = count(in, "leader epoch");
            TreeMap<Integer, Long> epochs = new TreeMap<>();
            for (int index = 0; index < count; index++) {
                if (epochs.put(in.readInt(), in.readLong()) != null) {
                    throw new IllegalArgumentException("duplicate leader-epoch checkpoint row");
                }
            }
            var sources = new ArrayList<KafkaCheckpointReadIndexV1>();
            int sourceCount = magic == LEADER_EPOCH_MAGIC ? 0 : count(in, "source");
            for (int index = 0; index < sourceCount; index++) {
                byte[] id = new byte[16];
                in.readFully(id);
                int rows = count(in, "source locator");
                var locators = new ArrayList<KafkaCheckpointReadIndexV1.Row>();
                for (int row = 0; row < rows; row++) {
                    locators.add(new KafkaCheckpointReadIndexV1.Row(
                            in.readLong(), in.readLong(), in.readLong(), in.readLong(), in.readLong()));
                }
                sources.add(new KafkaCheckpointReadIndexV1(
                        new com.nereusstream.storage.api.bookkeeper.StorageRunId(
                                com.nereusstream.domain.identity.Id128.fromBytes(id)),
                        locators));
            }
            return new Sources(new KafkaLeaderEpochIndexV1(epochs), sources);
        });
    }

    private static CanonicalBytes encode(Encoder encoder) {
        try {
            ByteArrayOutputStream bytes = new ByteArrayOutputStream();
            try (DataOutputStream out = new DataOutputStream(bytes)) {
                encoder.encode(out);
            }
            if (bytes.size() > Nbke2ConstantsV1.FORMAT_MAX_CHECKPOINT_SECTION_BYTES) {
                throw new IllegalArgumentException("canonical checkpoint section exceeds its persisted cap");
            }
            return CanonicalBytes.copyOf(bytes.toByteArray());
        } catch (IOException failure) {
            throw new IllegalStateException("in-memory checkpoint encoding failed", failure);
        }
    }

    private static <T> T decode(CanonicalBytes section, Decoder<T> decoder) {
        if (section.length() > Nbke2ConstantsV1.FORMAT_MAX_CHECKPOINT_SECTION_BYTES) {
            throw new IllegalArgumentException("checkpoint section exceeds its persisted cap");
        }
        try (DataInputStream in = new DataInputStream(new ByteArrayInputStream(section.toByteArray()))) {
            T decoded = decoder.decode(in);
            if (in.read() != -1) {
                throw new IllegalArgumentException("checkpoint section has trailing bytes");
            }
            return decoded;
        } catch (EOFException failure) {
            throw new IllegalArgumentException("checkpoint section is truncated", failure);
        } catch (IOException failure) {
            throw new IllegalArgumentException("checkpoint section cannot be decoded", failure);
        }
    }

    private static int count(DataInputStream in, String kind) throws IOException {
        int count = in.readInt();
        if (count < 0 || count > MAX_ROWS) {
            throw new IllegalArgumentException(kind + " checkpoint count exceeds its bound");
        }
        return count;
    }

    private static void requireMagic(DataInputStream in, int expected, String kind) throws IOException {
        if (in.readInt() != expected) {
            throw new IllegalArgumentException(kind + " checkpoint magic/version mismatch");
        }
    }

    @FunctionalInterface
    private interface Encoder {
        void encode(DataOutputStream out) throws IOException;
    }

    @FunctionalInterface
    private interface Decoder<T> {
        T decode(DataInputStream in) throws IOException;
    }
}
