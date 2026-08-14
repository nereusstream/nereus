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

package com.nereusstream.kafka.bookkeeper.replication;

import com.nereusstream.domain.bytes.CanonicalBytes;
import com.nereusstream.domain.bytes.Sha256Digest;
import com.nereusstream.domain.identity.Id128;
import com.nereusstream.domain.identity.KafkaTopicId;
import com.nereusstream.domain.identity.StorageEpochId;
import com.nereusstream.domain.identity.TopicBindingId;
import com.nereusstream.domain.protocol.KafkaTopicIncarnationIdentity;
import com.nereusstream.domain.protocol.KafkaTopicName;
import com.nereusstream.kafka.bookkeeper.protocol.KafkaPartitionFenceV1;
import com.nereusstream.storage.api.bookkeeper.CellProviderScopeId;
import java.io.ByteArrayInputStream;
import java.io.ByteArrayOutputStream;
import java.io.DataInputStream;
import java.io.DataOutputStream;
import java.io.EOFException;
import java.io.IOException;
import java.util.zip.CRC32C;

/** Strict-EOF, CRC-protected KRD1 compact descriptor codec. */
public final class KafkaReplicaCommitDescriptorCodecV1 {
    public static final int FORMAT_MAX_DESCRIPTOR_BYTES = 1_024;
    private static final int MAGIC = 0x4b_52_44_31;
    private static final int FRAME_OVERHEAD = 12;

    private KafkaReplicaCommitDescriptorCodecV1() {}

    public static CanonicalBytes encode(KafkaReplicaCommitDescriptorV1 descriptor) {
        try {
            ByteArrayOutputStream bodyBytes = new ByteArrayOutputStream();
            try (DataOutputStream out = new DataOutputStream(bodyBytes)) {
                writeDigest(out, descriptor.fence().bindingId().digest());
                writeId(out, descriptor.fence().topicIncarnation().topicId().value());
                byte[] topicName = descriptor
                        .fence()
                        .topicIncarnation()
                        .topicName()
                        .bytes()
                        .toByteArray();
                out.writeShort(topicName.length);
                out.write(topicName);
                out.writeInt(descriptor.fence().partitionId());
                out.writeLong(descriptor.fence().bindingGeneration());
                writeDigest(out, descriptor.fence().storageEpochId().digest());
                out.writeLong(descriptor.fence().ownerEpoch());
                out.writeInt(descriptor.fence().kafkaLeaderEpoch());
                out.writeLong(descriptor.validatedStateVersion());
                out.writeLong(descriptor.startOffset());
                out.writeLong(descriptor.endOffsetExclusive());
                out.writeLong(descriptor.encodedDataBytes());
                writeDigest(out, descriptor.aggregateAssignedPayloadSha256());
                out.writeByte(descriptor.source().sourceKind().wireCode());
                out.writeByte(descriptor.observationMode().wireCode());
                writeDigest(out, descriptor.source().providerScopeId().digest());
                writeId(out, descriptor.source().sourceId());
                out.writeLong(descriptor.source().sourceGeneration());
                out.writeLong(descriptor.source().physicalStartUnit());
                out.writeLong(descriptor.source().physicalEndUnitExclusive());
                out.writeLong(descriptor.source().kafkaStartOffset());
                out.writeLong(descriptor.source().kafkaEndOffsetExclusive());
                writeDigest(out, descriptor.source().sourceIdentityDigest());
                writeDigest(out, descriptor.source().payloadContentDigest());
                writeDigest(out, descriptor.protocolProof().producerStateDigest());
                writeDigest(out, descriptor.protocolProof().transactionStateDigest());
                writeDigest(out, descriptor.protocolProof().leaderEpochDigest());
                writeDigest(out, descriptor.protocolProof().checkpointVectorDigest());
            }
            byte[] body = bodyBytes.toByteArray();
            ByteArrayOutputStream frameBytes = new ByteArrayOutputStream();
            try (DataOutputStream frame = new DataOutputStream(frameBytes)) {
                frame.writeInt(MAGIC);
                frame.writeInt(body.length);
                frame.write(body);
                frame.flush();
                CRC32C crc = new CRC32C();
                byte[] withoutCrc = frameBytes.toByteArray();
                crc.update(withoutCrc, 0, withoutCrc.length);
                frame.writeInt((int) crc.getValue());
            }
            if (frameBytes.size() > FORMAT_MAX_DESCRIPTOR_BYTES) {
                throw new IllegalArgumentException("KRD1 descriptor exceeds its persisted cap");
            }
            return CanonicalBytes.copyOf(frameBytes.toByteArray());
        } catch (IOException failure) {
            throw new IllegalStateException("in-memory KRD1 encoding failed", failure);
        }
    }

    public static KafkaReplicaCommitDescriptorV1 decode(CanonicalBytes encoded) {
        byte[] bytes = encoded.toByteArray();
        if (bytes.length < FRAME_OVERHEAD || bytes.length > FORMAT_MAX_DESCRIPTOR_BYTES) {
            throw new IllegalArgumentException("KRD1 descriptor length is outside its persisted cap");
        }
        CRC32C crc = new CRC32C();
        crc.update(bytes, 0, bytes.length - Integer.BYTES);
        int storedCrc = java.nio.ByteBuffer.wrap(bytes, bytes.length - Integer.BYTES, Integer.BYTES)
                .getInt();
        if ((int) crc.getValue() != storedCrc) {
            throw new IllegalArgumentException("KRD1 descriptor CRC mismatch");
        }
        try (DataInputStream frame = new DataInputStream(new ByteArrayInputStream(bytes))) {
            if (frame.readInt() != MAGIC) {
                throw new IllegalArgumentException("KRD1 descriptor magic/version mismatch");
            }
            int bodyLength = frame.readInt();
            if (bodyLength != bytes.length - FRAME_OVERHEAD) {
                throw new IllegalArgumentException("KRD1 descriptor body length mismatch");
            }
            byte[] body = frame.readNBytes(bodyLength);
            if (body.length != bodyLength) {
                throw new IllegalArgumentException("KRD1 descriptor is truncated");
            }
            frame.readInt();
            if (frame.read() != -1) {
                throw new IllegalArgumentException("KRD1 descriptor has trailing bytes");
            }
            return decodeBody(body);
        } catch (EOFException failure) {
            throw new IllegalArgumentException("KRD1 descriptor is truncated", failure);
        } catch (IOException failure) {
            throw new IllegalArgumentException("KRD1 descriptor cannot be decoded", failure);
        }
    }

    public static Sha256Digest digest(KafkaReplicaCommitDescriptorV1 descriptor) {
        return Sha256Digest.hash(encode(descriptor));
    }

    private static KafkaReplicaCommitDescriptorV1 decodeBody(byte[] body) throws IOException {
        try (DataInputStream in = new DataInputStream(new ByteArrayInputStream(body))) {
            TopicBindingId bindingId = new TopicBindingId(readDigest(in));
            KafkaTopicId topicId = new KafkaTopicId(readId(in));
            int topicNameLength = in.readUnsignedShort();
            if (topicNameLength <= 0 || topicNameLength > KafkaTopicName.MAX_LENGTH) {
                throw new IllegalArgumentException("KRD1 topic-name length exceeds its cap");
            }
            byte[] topicName = in.readNBytes(topicNameLength);
            if (topicName.length != topicNameLength) {
                throw new IllegalArgumentException("KRD1 topic name is truncated");
            }
            KafkaPartitionFenceV1 fence = new KafkaPartitionFenceV1(
                    bindingId,
                    new KafkaTopicIncarnationIdentity(topicId, KafkaTopicName.fromBytes(topicName)),
                    in.readInt(),
                    in.readLong(),
                    new StorageEpochId(readDigest(in)),
                    in.readLong(),
                    in.readInt());
            long stateVersion = in.readLong();
            long startOffset = in.readLong();
            long endOffset = in.readLong();
            long encodedBytes = in.readLong();
            Sha256Digest aggregate = readDigest(in);
            KafkaReplicaSourceKindV1 sourceKind = KafkaReplicaSourceKindV1.fromWireCode(in.readUnsignedByte());
            KafkaReplicaObservationModeV1 observationMode =
                    KafkaReplicaObservationModeV1.fromWireCode(in.readUnsignedByte());
            KafkaReplicaSourceReferenceV1 source = new KafkaReplicaSourceReferenceV1(
                    sourceKind,
                    new CellProviderScopeId(readDigest(in)),
                    readId(in),
                    in.readLong(),
                    in.readLong(),
                    in.readLong(),
                    in.readLong(),
                    in.readLong(),
                    readDigest(in),
                    readDigest(in));
            KafkaReplicaProtocolProofV1 protocolProof =
                    new KafkaReplicaProtocolProofV1(readDigest(in), readDigest(in), readDigest(in), readDigest(in));
            if (in.read() != -1) {
                throw new IllegalArgumentException("KRD1 descriptor body has unknown tail bytes");
            }
            return new KafkaReplicaCommitDescriptorV1(
                    fence,
                    stateVersion,
                    startOffset,
                    endOffset,
                    encodedBytes,
                    aggregate,
                    source,
                    protocolProof,
                    observationMode);
        }
    }

    private static void writeDigest(DataOutputStream out, Sha256Digest digest) throws IOException {
        out.write(digest.bytes().toByteArray());
    }

    private static Sha256Digest readDigest(DataInputStream in) throws IOException {
        byte[] value = in.readNBytes(Sha256Digest.LENGTH);
        if (value.length != Sha256Digest.LENGTH) {
            throw new EOFException("truncated SHA-256 identity");
        }
        return Sha256Digest.copyOf(value);
    }

    private static void writeId(DataOutputStream out, Id128 id) throws IOException {
        out.writeLong(id.highBits());
        out.writeLong(id.lowBits());
    }

    private static Id128 readId(DataInputStream in) throws IOException {
        return new Id128(in.readLong(), in.readLong());
    }
}
