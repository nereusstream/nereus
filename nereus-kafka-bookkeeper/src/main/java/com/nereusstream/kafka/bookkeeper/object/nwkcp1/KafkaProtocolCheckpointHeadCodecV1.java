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

package com.nereusstream.kafka.bookkeeper.object.nwkcp1;

import com.nereusstream.domain.bytes.CanonicalBytes;
import com.nereusstream.domain.bytes.Sha256Digest;
import com.nereusstream.domain.identity.Id128;
import com.nereusstream.domain.identity.KafkaTopicId;
import com.nereusstream.domain.identity.StorageEpochId;
import com.nereusstream.domain.identity.TopicBindingId;
import java.io.ByteArrayInputStream;
import java.io.ByteArrayOutputStream;
import java.io.DataInputStream;
import java.io.DataOutputStream;
import java.io.EOFException;
import java.io.IOException;
import java.nio.charset.StandardCharsets;
import java.util.ArrayList;
import java.util.List;
import java.util.Objects;
import java.util.zip.CRC32C;

/** Strict canonical wire for {@link KafkaProtocolCheckpointHeadV1}. */
public final class KafkaProtocolCheckpointHeadCodecV1 {
    private KafkaProtocolCheckpointHeadCodecV1() {}

    public static CanonicalBytes encode(
            String expectedWalRunPrefix, Sha256Digest expectedWalRunRootSha, KafkaProtocolCheckpointHeadV1 head) {
        requireContext(expectedWalRunPrefix, expectedWalRunRootSha, head);
        return encodeRaw(head);
    }

    private static CanonicalBytes encodeRaw(KafkaProtocolCheckpointHeadV1 head) {
        Objects.requireNonNull(head, "head");
        try {
            byte[] key = head.checkpointObjectKey().getBytes(StandardCharsets.US_ASCII);
            if (key.length <= 0 || key.length > Nwkcp1ConstantsV1.FORMAT_MAX_KEY_BYTES) {
                throw new IllegalArgumentException("checkpoint Head object key exceeds its cap");
            }
            ByteArrayOutputStream payloadBytes = new ByteArrayOutputStream();
            try (DataOutputStream out = new DataOutputStream(payloadBytes)) {
                out.writeLong(Nwkcp1ConstantsV1.HEAD_MAGIC);
                out.writeInt(0); // canonical total length is patched after payload assembly
                out.write(head.walRunRootSha().bytes().toByteArray());
                out.writeLong(head.publisherEpoch());
                out.writeByte(head.state().wireId());
                out.write(new byte[3]);
                out.writeLong(head.checkpointOrdinal());
                out.write(head.predecessorCheckpointDigest().bytes().toByteArray());
                out.writeShort(key.length);
                out.write(key);
                out.writeLong(head.checkpointObjectLength());
                out.write(head.checkpointObjectDigest().bytes().toByteArray());
                out.writeInt(head.coveredThroughVector().size());
                for (KafkaCheckpointCoverageV1 coverage : head.coveredThroughVector()) {
                    out.write(coverage.bindingId().digest().bytes().toByteArray());
                    out.write(coverage.topicId().value().bytes().toByteArray());
                    out.writeInt(coverage.partitionId());
                    out.write(coverage.storageEpochId().digest().bytes().toByteArray());
                    out.writeLong(coverage.ownerEpoch());
                    out.writeInt(coverage.kafkaLeaderEpoch());
                    out.writeLong(coverage.rangeIndexCoveredThrough());
                    out.writeLong(coverage.producerStateCoveredThrough());
                    out.writeLong(coverage.transactionIndexCoveredThrough());
                    out.writeLong(coverage.leaderEpochCoveredThrough());
                }
            }
            byte[] payload = payloadBytes.toByteArray();
            int totalLength = Math.addExact(payload.length, Integer.BYTES);
            if (totalLength > Nwkcp1ConstantsV1.FORMAT_MAX_HEAD_BYTES) {
                throw new IllegalArgumentException("checkpoint Head exceeds its persisted cap");
            }
            java.nio.ByteBuffer.wrap(payload, Long.BYTES, Integer.BYTES).putInt(totalLength);
            CRC32C crc = new CRC32C();
            crc.update(payload, 0, payload.length);
            ByteArrayOutputStream bytes = new ByteArrayOutputStream(totalLength);
            try (DataOutputStream out = new DataOutputStream(bytes)) {
                out.write(payload);
                out.writeInt((int) crc.getValue());
            }
            return CanonicalBytes.copyOf(bytes.toByteArray());
        } catch (IOException failure) {
            throw new IllegalStateException("in-memory checkpoint Head encoding failed", failure);
        }
    }

    public static KafkaProtocolCheckpointHeadV1 decode(
            String expectedWalRunPrefix, Sha256Digest expectedWalRunRootSha, CanonicalBytes bytes) {
        KafkaProtocolCheckpointHeadV1 head = decodeRaw(bytes);
        requireContext(expectedWalRunPrefix, expectedWalRunRootSha, head);
        return head;
    }

    private static KafkaProtocolCheckpointHeadV1 decodeRaw(CanonicalBytes bytes) {
        Objects.requireNonNull(bytes, "bytes");
        if (bytes.length() <= 0 || bytes.length() > Nwkcp1ConstantsV1.FORMAT_MAX_HEAD_BYTES) {
            throw new IllegalArgumentException("checkpoint Head length exceeds its cap");
        }
        byte[] value = bytes.toByteArray();
        if (value.length < 144) {
            throw new IllegalArgumentException("checkpoint Head is truncated");
        }
        CRC32C crc = new CRC32C();
        crc.update(value, 0, value.length - Integer.BYTES);
        int storedCrc = java.nio.ByteBuffer.wrap(value, value.length - Integer.BYTES, Integer.BYTES)
                .getInt();
        if (storedCrc != (int) crc.getValue()) {
            throw new IllegalArgumentException("checkpoint Head CRC32C mismatch");
        }
        try (DataInputStream in =
                new DataInputStream(new ByteArrayInputStream(value, 0, value.length - Integer.BYTES))) {
            if (in.readLong() != Nwkcp1ConstantsV1.HEAD_MAGIC) {
                throw new IllegalArgumentException("checkpoint Head magic/version mismatch");
            }
            if (in.readInt() != value.length) {
                throw new IllegalArgumentException("checkpoint Head canonical length mismatch");
            }
            Sha256Digest rootSha = Sha256Digest.copyOf(readExact(in, Sha256Digest.LENGTH));
            long publisherEpoch = in.readLong();
            KafkaProtocolCheckpointHeadStateV1 state =
                    KafkaProtocolCheckpointHeadStateV1.fromWire(Byte.toUnsignedInt(in.readByte()));
            if (in.readByte() != 0 || in.readByte() != 0 || in.readByte() != 0) {
                throw new IllegalArgumentException("checkpoint Head reserved bytes are non-zero");
            }
            long ordinal = in.readLong();
            Sha256Digest predecessor = Sha256Digest.copyOf(readExact(in, Sha256Digest.LENGTH));
            int keyLength = Short.toUnsignedInt(in.readShort());
            if (keyLength <= 0 || keyLength > Nwkcp1ConstantsV1.FORMAT_MAX_KEY_BYTES) {
                throw new IllegalArgumentException("checkpoint Head key length exceeds its cap");
            }
            byte[] keyBytes = readExact(in, keyLength);
            for (byte item : keyBytes) {
                if (item <= 0 || (item & 0x80) != 0) {
                    throw new IllegalArgumentException("checkpoint Head key is not canonical ASCII");
                }
            }
            String key = new String(keyBytes, StandardCharsets.US_ASCII);
            long objectLength = in.readLong();
            Sha256Digest objectDigest = Sha256Digest.copyOf(readExact(in, Sha256Digest.LENGTH));
            int vectorCount = in.readInt();
            if (vectorCount <= 0 || vectorCount > Nwkcp1ConstantsV1.FORMAT_MAX_HEAD_VECTOR_ROWS) {
                throw new IllegalArgumentException("checkpoint Head vector count exceeds its cap");
            }
            List<KafkaCheckpointCoverageV1> vector = new ArrayList<>(vectorCount);
            for (int index = 0; index < vectorCount; index++) {
                vector.add(new KafkaCheckpointCoverageV1(
                        new TopicBindingId(Sha256Digest.copyOf(readExact(in, Sha256Digest.LENGTH))),
                        new KafkaTopicId(Id128.fromBytes(readExact(in, Id128.LENGTH))),
                        in.readInt(),
                        new StorageEpochId(Sha256Digest.copyOf(readExact(in, Sha256Digest.LENGTH))),
                        in.readLong(),
                        in.readInt(),
                        in.readLong(),
                        in.readLong(),
                        in.readLong(),
                        in.readLong()));
            }
            if (in.read() != -1) {
                throw new IllegalArgumentException("checkpoint Head has trailing bytes");
            }
            return new KafkaProtocolCheckpointHeadV1(
                    rootSha, publisherEpoch, state, ordinal, predecessor, key, objectLength, objectDigest, vector);
        } catch (EOFException failure) {
            throw new IllegalArgumentException("checkpoint Head is truncated", failure);
        } catch (IOException failure) {
            throw new IllegalArgumentException("checkpoint Head cannot be decoded", failure);
        }
    }

    public static Sha256Digest canonicalValueDigest(
            String expectedWalRunPrefix, Sha256Digest expectedWalRunRootSha, KafkaProtocolCheckpointHeadV1 head) {
        return Sha256Digest.hash(encode(expectedWalRunPrefix, expectedWalRunRootSha, head));
    }

    private static void requireContext(
            String expectedWalRunPrefix, Sha256Digest expectedWalRunRootSha, KafkaProtocolCheckpointHeadV1 head) {
        Objects.requireNonNull(expectedWalRunRootSha, "expectedWalRunRootSha");
        Objects.requireNonNull(head, "head");
        if (expectedWalRunRootSha.isZero() || !head.walRunRootSha().equals(expectedWalRunRootSha)) {
            throw new IllegalArgumentException("checkpoint Head belongs to another WalRun Root");
        }
        Nwkcp1ObjectKeyV1.requireExactObjectKey(
                expectedWalRunPrefix, head.checkpointObjectKey(), head.checkpointObjectDigest());
    }

    private static byte[] readExact(DataInputStream in, int length) throws IOException {
        byte[] value = in.readNBytes(length);
        if (value.length != length) {
            throw new EOFException("checkpoint Head is truncated");
        }
        return value;
    }
}
