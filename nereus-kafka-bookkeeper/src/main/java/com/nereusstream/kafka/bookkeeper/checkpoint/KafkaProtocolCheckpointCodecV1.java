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
import java.util.Map;
import java.util.TreeMap;

/** Strict-EOF canonical KPC1 codec for the three profile-neutral checkpoint sections. */
public final class KafkaProtocolCheckpointCodecV1 {
    private static final int PRODUCER_MAGIC = 0x4b_50_43_31;
    private static final int TRANSACTION_MAGIC = 0x4b_54_43_31;
    private static final int LEADER_EPOCH_MAGIC = 0x4b_4c_43_31;
    private static final int MAX_ROWS = 65_536;

    private KafkaProtocolCheckpointCodecV1() {}

    public static KafkaProtocolCheckpointSectionsV1 encode(KafkaProtocolCheckpointStateV1 state) {
        return new KafkaProtocolCheckpointSectionsV1(
                encodeProducers(state.producerState()),
                encodeTransactions(state.transactionState()),
                encodeLeaderEpochs(state.leaderEpochIndex()));
    }

    public static KafkaProtocolCheckpointStateV1 decode(
            KafkaRecoveryCheckpointVectorV1 vector, KafkaProtocolCheckpointSectionsV1 sections) {
        return new KafkaProtocolCheckpointStateV1(
                vector,
                decodeProducers(sections.producerState()),
                decodeTransactions(sections.transactionIndex()),
                decodeLeaderEpochs(sections.leaderEpochIndex()));
    }

    private static CanonicalBytes encodeProducers(KafkaCommittedProducerStateV1 state) {
        return encode(out -> {
            out.writeInt(PRODUCER_MAGIC);
            out.writeInt(state.producers().size());
            for (KafkaProducerSessionStateV1 producer : state.producers().values()) {
                out.writeLong(producer.producerId());
                out.writeShort(producer.producerEpoch());
                out.writeInt(producer.lastSequence());
                out.writeLong(producer.lastOffset());
                out.writeInt(producer.recentBatches().size());
                for (KafkaProducerBatchResultV1 batch : producer.recentBatches()) {
                    KafkaBatchDuplicateIdentityV1 identity = batch.identity();
                    out.writeLong(identity.producerId());
                    out.writeShort(identity.producerEpoch());
                    out.writeInt(identity.baseSequence());
                    out.writeInt(identity.lastSequence());
                    out.writeLong(batch.startOffset());
                    out.writeLong(batch.endOffsetExclusive());
                }
            }
        });
    }

    private static KafkaCommittedProducerStateV1 decodeProducers(CanonicalBytes section) {
        return decode(section, in -> {
            requireMagic(in, PRODUCER_MAGIC, "producer");
            int count = count(in, "producer");
            TreeMap<Long, KafkaProducerSessionStateV1> producers = new TreeMap<>();
            for (int index = 0; index < count; index++) {
                long producerId = in.readLong();
                short producerEpoch = in.readShort();
                int lastSequence = in.readInt();
                long lastOffset = in.readLong();
                int recentCount = count(in, "recent producer result");
                if (recentCount <= 0 || recentCount > KafkaProducerSessionStateV1.MAX_RECENT_BATCHES) {
                    throw new IllegalArgumentException("recent producer result count is outside its bound");
                }
                List<KafkaProducerBatchResultV1> recent = new ArrayList<>(recentCount);
                for (int recentIndex = 0; recentIndex < recentCount; recentIndex++) {
                    KafkaBatchDuplicateIdentityV1 identity = new KafkaBatchDuplicateIdentityV1(
                            in.readLong(), in.readShort(), in.readInt(), in.readInt());
                    recent.add(new KafkaProducerBatchResultV1(identity, in.readLong(), in.readLong()));
                }
                KafkaProducerSessionStateV1 decoded =
                        new KafkaProducerSessionStateV1(producerId, producerEpoch, lastSequence, lastOffset, recent);
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

    private static CanonicalBytes encodeLeaderEpochs(KafkaLeaderEpochIndexV1 state) {
        return encode(out -> {
            out.writeInt(LEADER_EPOCH_MAGIC);
            out.writeInt(state.startOffsets().size());
            for (Map.Entry<Integer, Long> entry : state.startOffsets().entrySet()) {
                out.writeInt(entry.getKey());
                out.writeLong(entry.getValue());
            }
        });
    }

    private static KafkaLeaderEpochIndexV1 decodeLeaderEpochs(CanonicalBytes section) {
        return decode(section, in -> {
            requireMagic(in, LEADER_EPOCH_MAGIC, "leader epoch");
            int count = count(in, "leader epoch");
            TreeMap<Integer, Long> epochs = new TreeMap<>();
            for (int index = 0; index < count; index++) {
                if (epochs.put(in.readInt(), in.readLong()) != null) {
                    throw new IllegalArgumentException("duplicate leader-epoch checkpoint row");
                }
            }
            return new KafkaLeaderEpochIndexV1(epochs);
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
