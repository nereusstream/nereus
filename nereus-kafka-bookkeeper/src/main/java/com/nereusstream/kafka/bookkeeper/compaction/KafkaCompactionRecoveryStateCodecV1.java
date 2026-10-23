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

package com.nereusstream.kafka.bookkeeper.compaction;

import com.nereusstream.domain.bytes.CanonicalBytes;
import com.nereusstream.domain.bytes.Sha256Digest;
import com.nereusstream.domain.identity.Id128;
import com.nereusstream.domain.identity.KafkaTopicId;
import com.nereusstream.domain.identity.StorageEpochId;
import com.nereusstream.domain.identity.TopicBindingId;
import com.nereusstream.domain.protocol.KafkaTopicIncarnationIdentity;
import com.nereusstream.domain.protocol.KafkaTopicName;
import com.nereusstream.kafka.bookkeeper.checkpoint.KafkaProtocolCheckpointCodecV1;
import com.nereusstream.kafka.bookkeeper.checkpoint.KafkaProtocolCheckpointSectionsV1;
import com.nereusstream.kafka.bookkeeper.nbke2.Nbke2ConstantsV1;
import com.nereusstream.kafka.bookkeeper.protocol.KafkaPartitionFenceV1;
import java.io.ByteArrayInputStream;
import java.io.ByteArrayOutputStream;
import java.io.DataInputStream;
import java.io.DataOutputStream;
import java.io.IOException;
import java.util.List;
import java.util.Objects;

/** Bounded, strict-EOF KCRS1 wire containing complete protocol components and no physical source locators. */
public final class KafkaCompactionRecoveryStateCodecV1 {
    private static final int MAGIC = 0x4b435253;
    public static final int MAX_ENCODED_BYTES = 3 * Nbke2ConstantsV1.FORMAT_MAX_CHECKPOINT_SECTION_BYTES
            + Nbke2ConstantsV1.FORMAT_MAX_TOPIC_NAME_BYTES
            + 256;

    private KafkaCompactionRecoveryStateCodecV1() {}

    public static CanonicalBytes encode(KafkaCompactionRecoveryStateV1 state) {
        Objects.requireNonNull(state, "state");
        var sections = KafkaProtocolCheckpointCodecV1.encodeComponents(state.components());
        try {
            var bytes = new ByteArrayOutputStream();
            try (var out = new DataOutputStream(bytes)) {
                out.writeInt(MAGIC);
                out.writeShort(1);
                var fence = state.sourceFence();
                out.write(fence.bindingId().digest().bytes().toByteArray());
                out.write(fence.topicIncarnation().topicId().value().bytes().toByteArray());
                var name = fence.topicIncarnation().topicName().bytes();
                if (name.length() > Nbke2ConstantsV1.FORMAT_MAX_TOPIC_NAME_BYTES) {
                    throw new IllegalArgumentException("compaction recovery topic name exceeds its bound");
                }
                out.writeInt(name.length());
                out.write(name.toByteArray());
                out.writeInt(fence.partitionId());
                out.writeLong(fence.bindingGeneration());
                out.write(fence.storageEpochId().digest().bytes().toByteArray());
                out.writeLong(fence.ownerEpoch());
                out.writeInt(fence.kafkaLeaderEpoch());
                out.writeLong(state.coveredThrough());
                out.write(state.sourceCheckpointSha256().bytes().toByteArray());
                for (var section :
                        List.of(sections.producerState(), sections.transactionIndex(), sections.leaderEpochIndex())) {
                    out.writeInt(section.length());
                    out.write(section.toByteArray());
                }
            }
            if (bytes.size() > MAX_ENCODED_BYTES) {
                throw new IllegalArgumentException("compaction recovery state exceeds its encoded bound");
            }
            return CanonicalBytes.copyOf(bytes.toByteArray());
        } catch (IOException impossible) {
            throw new IllegalStateException("in-memory compaction recovery state encoding failed", impossible);
        }
    }

    public static KafkaCompactionRecoveryStateV1 decode(CanonicalBytes bytes) {
        Objects.requireNonNull(bytes, "bytes");
        if (bytes.length() > MAX_ENCODED_BYTES) {
            throw new IllegalArgumentException("compaction recovery state exceeds its encoded bound");
        }
        try (var in = new DataInputStream(new ByteArrayInputStream(bytes.toByteArray()))) {
            if (in.readInt() != MAGIC || in.readUnsignedShort() != 1) {
                throw new IllegalArgumentException("compaction recovery state has a foreign magic or version");
            }
            var binding = new TopicBindingId(digest(in));
            byte[] id = new byte[Id128.LENGTH];
            in.readFully(id);
            var topic = new KafkaTopicId(Id128.fromBytes(id));
            var name = KafkaTopicName.fromBytes(boundedBytes(in, Nbke2ConstantsV1.FORMAT_MAX_TOPIC_NAME_BYTES));
            var fence = new KafkaPartitionFenceV1(
                    binding,
                    new KafkaTopicIncarnationIdentity(topic, name),
                    in.readInt(),
                    in.readLong(),
                    new StorageEpochId(digest(in)),
                    in.readLong(),
                    in.readInt());
            long through = in.readLong();
            var sourceCheckpoint = digest(in);
            var sections = new KafkaProtocolCheckpointSectionsV1(section(in), section(in), section(in));
            var state = new KafkaCompactionRecoveryStateV1(
                    fence, through, sourceCheckpoint, KafkaProtocolCheckpointCodecV1.decodeComponents(sections));
            if (in.available() != 0 || !encode(state).equals(bytes)) {
                throw new IllegalArgumentException("compaction recovery state is noncanonical or has trailing bytes");
            }
            return state;
        } catch (IOException failure) {
            throw new IllegalArgumentException("compaction recovery state is truncated", failure);
        }
    }

    private static CanonicalBytes section(DataInputStream in) throws IOException {
        return CanonicalBytes.copyOf(boundedBytes(in, Nbke2ConstantsV1.FORMAT_MAX_CHECKPOINT_SECTION_BYTES));
    }

    private static byte[] boundedBytes(DataInputStream in, int maximum) throws IOException {
        int length = in.readInt();
        if (length <= 0 || length > maximum || length > in.available()) {
            throw new IllegalArgumentException("compaction recovery field length exceeds its bound");
        }
        return in.readNBytes(length);
    }

    private static Sha256Digest digest(DataInputStream in) throws IOException {
        byte[] bytes = new byte[Sha256Digest.LENGTH];
        in.readFully(bytes);
        return Sha256Digest.copyOf(bytes);
    }
}
