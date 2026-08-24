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
import com.nereusstream.domain.protocol.KafkaTopicIncarnationIdentity;
import com.nereusstream.domain.protocol.KafkaTopicName;
import com.nereusstream.kafka.bookkeeper.checkpoint.KafkaProtocolCheckpointCodecV1;
import com.nereusstream.kafka.bookkeeper.checkpoint.KafkaProtocolCheckpointSectionsV1;
import com.nereusstream.kafka.bookkeeper.checkpoint.KafkaProtocolCheckpointStateV1;
import com.nereusstream.kafka.bookkeeper.checkpoint.KafkaRecoveryCheckpointVectorV1;
import com.nereusstream.kafka.bookkeeper.nbke2.Nbke2RunBindingV1;
import com.nereusstream.storage.api.bookkeeper.CellProviderScopeId;
import com.nereusstream.storage.api.bookkeeper.StorageRunId;
import java.io.ByteArrayInputStream;
import java.io.ByteArrayOutputStream;
import java.io.DataInputStream;
import java.io.DataOutputStream;
import java.io.EOFException;
import java.io.IOException;
import java.util.ArrayList;
import java.util.List;
import java.util.Objects;
import java.util.zip.CRC32C;

/** Strict big-endian, strict-EOF NWKCP1 v1 encoder and decoder. */
public final class Nwkcp1CodecV1 {
    private Nwkcp1CodecV1() {}

    public static Nwkcp1EncodedObjectV1 encode(String walRunPrefix, Nwkcp1ObjectV1 object) {
        Objects.requireNonNull(object, "object");
        try {
            List<byte[]> rows = new ArrayList<>(object.rows().size());
            long total = Nwkcp1ConstantsV1.HEADER_BYTES;
            for (KafkaProtocolCheckpointStateV1 state : object.rows()) {
                byte[] encoded = encodeRow(state);
                total = Math.addExact(total, 4L + encoded.length + Sha256Digest.LENGTH);
                if (total > Nwkcp1ConstantsV1.FORMAT_MAX_OBJECT_BYTES) {
                    throw new IllegalArgumentException("NWKCP1 object exceeds its persisted cap");
                }
                rows.add(encoded);
            }

            ByteArrayOutputStream headerBytes = new ByteArrayOutputStream(Nwkcp1ConstantsV1.HEADER_BYTES);
            try (DataOutputStream header = new DataOutputStream(headerBytes)) {
                header.writeLong(Nwkcp1ConstantsV1.MAGIC);
                header.writeInt(Nwkcp1ConstantsV1.HEADER_BYTES);
                header.writeInt(0);
                header.writeLong(total);
                header.write(object.walRunRootSha().bytes().toByteArray());
                header.writeInt(rows.size());
            }
            byte[] withoutCrc = headerBytes.toByteArray();
            if (withoutCrc.length != Nwkcp1ConstantsV1.HEADER_BYTES - Integer.BYTES) {
                throw new IllegalStateException("NWKCP1 header layout drifted");
            }
            CRC32C headerCrc = new CRC32C();
            headerCrc.update(withoutCrc, 0, withoutCrc.length);

            ByteArrayOutputStream bodyBytes = new ByteArrayOutputStream(Math.toIntExact(total));
            try (DataOutputStream body = new DataOutputStream(bodyBytes)) {
                body.write(withoutCrc);
                body.writeInt((int) headerCrc.getValue());
                for (byte[] row : rows) {
                    body.writeInt(row.length);
                    body.write(row);
                    body.write(Sha256Digest.hash(CanonicalBytes.copyOf(row))
                            .bytes()
                            .toByteArray());
                }
            }
            CanonicalBytes canonical = CanonicalBytes.copyOf(bodyBytes.toByteArray());
            Sha256Digest digest = Sha256Digest.hash(canonical);
            return new Nwkcp1EncodedObjectV1(
                    Nwkcp1ObjectKeyV1.objectKey(walRunPrefix, digest), canonical.length(), digest, canonical);
        } catch (ArithmeticException failure) {
            throw new IllegalArgumentException("NWKCP1 object length overflows", failure);
        } catch (IOException failure) {
            throw new IllegalStateException("in-memory NWKCP1 encoding failed", failure);
        }
    }

    public static Nwkcp1ObjectV1 decode(CanonicalBytes body) {
        Objects.requireNonNull(body, "body");
        if (body.length() > Nwkcp1ConstantsV1.FORMAT_MAX_OBJECT_BYTES) {
            throw reject(Nwkcp1RejectionV1.OBJECT_TOO_LARGE, "NWKCP1 object exceeds its persisted cap");
        }
        if (body.length() < Nwkcp1ConstantsV1.HEADER_BYTES) {
            throw reject(Nwkcp1RejectionV1.TRUNCATED, "NWKCP1 object is shorter than its fixed header");
        }
        byte[] value = body.toByteArray();
        try (DataInputStream in = new DataInputStream(new ByteArrayInputStream(value))) {
            if (in.readLong() != Nwkcp1ConstantsV1.MAGIC) {
                throw reject(Nwkcp1RejectionV1.MAGIC_VERSION, "NWKCP1 magic/version mismatch");
            }
            if (in.readInt() != Nwkcp1ConstantsV1.HEADER_BYTES) {
                throw reject(Nwkcp1RejectionV1.HEADER_LENGTH, "NWKCP1 fixed header length mismatch");
            }
            if (in.readInt() != 0) {
                throw reject(Nwkcp1RejectionV1.HEADER_FLAGS, "NWKCP1 reserved flags are non-zero");
            }
            long declaredLength = in.readLong();
            if (declaredLength != value.length) {
                throw reject(Nwkcp1RejectionV1.OBJECT_LENGTH, "NWKCP1 declared object length mismatch");
            }
            Sha256Digest rootSha = Sha256Digest.copyOf(readExact(in, Sha256Digest.LENGTH));
            if (rootSha.isZero()) {
                throw reject(Nwkcp1RejectionV1.ROOT_IDENTITY, "NWKCP1 Root SHA is zero");
            }
            int rowCount = in.readInt();
            if (rowCount <= 0 || rowCount > Nwkcp1ConstantsV1.FORMAT_MAX_ROWS) {
                throw reject(Nwkcp1RejectionV1.ROW_COUNT, "NWKCP1 row count exceeds its persisted cap");
            }
            int actualCrc = in.readInt();
            CRC32C crc = new CRC32C();
            crc.update(value, 0, Nwkcp1ConstantsV1.HEADER_BYTES - Integer.BYTES);
            if (actualCrc != (int) crc.getValue()) {
                throw reject(Nwkcp1RejectionV1.HEADER_CRC, "NWKCP1 fixed header CRC32C mismatch");
            }

            List<KafkaProtocolCheckpointStateV1> rows = new ArrayList<>(rowCount);
            for (int index = 0; index < rowCount; index++) {
                int rowLength = in.readInt();
                if (rowLength <= 0 || rowLength > Nwkcp1ConstantsV1.FORMAT_MAX_ROW_BYTES) {
                    throw reject(Nwkcp1RejectionV1.ROW_LENGTH, "NWKCP1 row length exceeds its persisted cap");
                }
                if (rowLength > in.available() - Sha256Digest.LENGTH) {
                    throw reject(Nwkcp1RejectionV1.TRUNCATED, "NWKCP1 row is truncated");
                }
                byte[] row = readExact(in, rowLength);
                Sha256Digest expected = Sha256Digest.copyOf(readExact(in, Sha256Digest.LENGTH));
                if (!Sha256Digest.hash(CanonicalBytes.copyOf(row)).equals(expected)) {
                    throw reject(Nwkcp1RejectionV1.ROW_DIGEST, "NWKCP1 row SHA-256 mismatch");
                }
                rows.add(decodeRow(row));
            }
            if (in.read() != -1) {
                throw reject(Nwkcp1RejectionV1.TRAILING_BYTES, "NWKCP1 object has trailing bytes");
            }
            try {
                return new Nwkcp1ObjectV1(rootSha, rows);
            } catch (IllegalArgumentException failure) {
                throw new Nwkcp1DecodingException(
                        Nwkcp1RejectionV1.ROW_ORDER, "NWKCP1 row order or identity is not canonical", failure);
            }
        } catch (Nwkcp1DecodingException failure) {
            throw failure;
        } catch (EOFException failure) {
            throw new Nwkcp1DecodingException(Nwkcp1RejectionV1.TRUNCATED, "NWKCP1 object is truncated", failure);
        } catch (IOException failure) {
            throw new Nwkcp1DecodingException(Nwkcp1RejectionV1.ROW_STATE, "NWKCP1 object cannot be decoded", failure);
        }
    }

    public static Nwkcp1ObjectV1 decodeVerified(
            String walRunPrefix, String key, long expectedLength, Sha256Digest expectedDigest, CanonicalBytes body) {
        Objects.requireNonNull(expectedDigest, "expectedDigest");
        Objects.requireNonNull(body, "body");
        if (expectedLength != body.length()) {
            throw reject(Nwkcp1RejectionV1.OBJECT_LENGTH, "NWKCP1 provider length differs from the Head");
        }
        Sha256Digest actualDigest = Sha256Digest.hash(body);
        if (!actualDigest.equals(expectedDigest)) {
            throw reject(Nwkcp1RejectionV1.OBJECT_DIGEST, "NWKCP1 provider body digest differs from the Head");
        }
        Sha256Digest keyDigest;
        try {
            keyDigest = Nwkcp1ObjectKeyV1.parseObjectDigest(walRunPrefix, key);
        } catch (IllegalArgumentException failure) {
            throw new Nwkcp1DecodingException(
                    Nwkcp1RejectionV1.OBJECT_KEY, "NWKCP1 key is not the exact Root-bound content key", failure);
        }
        if (!keyDigest.equals(expectedDigest)) {
            throw reject(Nwkcp1RejectionV1.OBJECT_KEY, "NWKCP1 key digest differs from the Head");
        }
        return decode(body);
    }

    private static byte[] encodeRow(KafkaProtocolCheckpointStateV1 state) throws IOException {
        KafkaProtocolCheckpointSectionsV1 sections = KafkaProtocolCheckpointCodecV1.encode(state);
        byte[] producer = sections.producerState().toByteArray();
        byte[] transaction = sections.transactionIndex().toByteArray();
        byte[] leader = sections.leaderEpochIndex().toByteArray();
        requireSectionCap(producer.length);
        requireSectionCap(transaction.length);
        requireSectionCap(leader.length);
        var vector = state.vector();
        var binding = vector.runBinding();
        byte[] topicName = binding.topicIncarnation().topicName().bytes().toByteArray();
        ByteArrayOutputStream bytes = new ByteArrayOutputStream();
        try (DataOutputStream out = new DataOutputStream(bytes)) {
            out.write(binding.bindingId().digest().bytes().toByteArray());
            out.write(binding.topicIncarnation().topicId().value().bytes().toByteArray());
            out.writeShort(topicName.length);
            out.write(topicName);
            out.writeInt(binding.partitionId());
            out.write(binding.storageEpochId().digest().bytes().toByteArray());
            out.writeLong(binding.creatorOwnerEpoch());
            out.writeInt(binding.kafkaLeaderEpoch());
            out.write(binding.providerScopeId().digest().bytes().toByteArray());
            out.write(binding.runId().value().bytes().toByteArray());
            out.writeLong(vector.rangeIndexCoveredThrough());
            out.writeLong(vector.producerStateCoveredThrough());
            out.writeLong(vector.transactionIndexCoveredThrough());
            out.writeLong(vector.leaderEpochCoveredThrough());
            out.writeInt(producer.length);
            out.writeInt(transaction.length);
            out.writeInt(leader.length);
            out.write(producer);
            out.write(transaction);
            out.write(leader);
        }
        byte[] row = bytes.toByteArray();
        if (row.length > Nwkcp1ConstantsV1.FORMAT_MAX_ROW_BYTES) {
            throw new IllegalArgumentException("NWKCP1 row exceeds its persisted cap");
        }
        return row;
    }

    private static KafkaProtocolCheckpointStateV1 decodeRow(byte[] row) {
        try (DataInputStream in = new DataInputStream(new ByteArrayInputStream(row))) {
            TopicBindingId bindingId = new TopicBindingId(Sha256Digest.copyOf(readExact(in, Sha256Digest.LENGTH)));
            KafkaTopicId topicId = new KafkaTopicId(Id128.fromBytes(readExact(in, Id128.LENGTH)));
            int topicNameLength = Short.toUnsignedInt(in.readShort());
            if (topicNameLength <= 0 || topicNameLength > Nwkcp1ConstantsV1.FORMAT_MAX_TOPIC_NAME_BYTES) {
                throw reject(Nwkcp1RejectionV1.ROW_STATE, "NWKCP1 topic-name length exceeds its cap");
            }
            KafkaTopicName topicName = KafkaTopicName.fromBytes(readExact(in, topicNameLength));
            int partitionId = in.readInt();
            StorageEpochId storageEpoch = new StorageEpochId(Sha256Digest.copyOf(readExact(in, Sha256Digest.LENGTH)));
            long ownerEpoch = in.readLong();
            int leaderEpoch = in.readInt();
            CellProviderScopeId providerScope =
                    new CellProviderScopeId(Sha256Digest.copyOf(readExact(in, Sha256Digest.LENGTH)));
            StorageRunId runId = new StorageRunId(Id128.fromBytes(readExact(in, Id128.LENGTH)));
            KafkaRecoveryCheckpointVectorV1 vector = new KafkaRecoveryCheckpointVectorV1(
                    new Nbke2RunBindingV1(
                            bindingId,
                            new KafkaTopicIncarnationIdentity(topicId, topicName),
                            partitionId,
                            storageEpoch,
                            ownerEpoch,
                            leaderEpoch,
                            providerScope,
                            runId),
                    in.readLong(),
                    in.readLong(),
                    in.readLong(),
                    in.readLong());
            int producerLength = sectionLength(in, "producer");
            int transactionLength = sectionLength(in, "transaction");
            int leaderLength = sectionLength(in, "leader epoch");
            long sectionTotal = (long) producerLength + transactionLength + leaderLength;
            if (sectionTotal != in.available()) {
                throw reject(Nwkcp1RejectionV1.ROW_LENGTH, "NWKCP1 section lengths differ from the row length");
            }
            KafkaProtocolCheckpointSectionsV1 sections = new KafkaProtocolCheckpointSectionsV1(
                    CanonicalBytes.copyOf(readExact(in, producerLength)),
                    CanonicalBytes.copyOf(readExact(in, transactionLength)),
                    CanonicalBytes.copyOf(readExact(in, leaderLength)));
            if (in.read() != -1) {
                throw reject(Nwkcp1RejectionV1.TRAILING_BYTES, "NWKCP1 row has trailing bytes");
            }
            return KafkaProtocolCheckpointCodecV1.decode(vector, sections);
        } catch (Nwkcp1DecodingException failure) {
            throw failure;
        } catch (EOFException failure) {
            throw new Nwkcp1DecodingException(Nwkcp1RejectionV1.TRUNCATED, "NWKCP1 row is truncated", failure);
        } catch (IOException | IllegalArgumentException failure) {
            throw new Nwkcp1DecodingException(Nwkcp1RejectionV1.ROW_STATE, "NWKCP1 row state is invalid", failure);
        }
    }

    private static int sectionLength(DataInputStream in, String kind) throws IOException {
        int length = in.readInt();
        if (length <= 0 || length > Nwkcp1ConstantsV1.FORMAT_MAX_SECTION_BYTES) {
            throw reject(Nwkcp1RejectionV1.ROW_LENGTH, "NWKCP1 " + kind + " section exceeds its cap");
        }
        return length;
    }

    private static void requireSectionCap(int length) {
        if (length <= 0 || length > Nwkcp1ConstantsV1.FORMAT_MAX_SECTION_BYTES) {
            throw new IllegalArgumentException("NWKCP1 checkpoint section exceeds its persisted cap");
        }
    }

    private static byte[] readExact(DataInputStream in, int length) throws IOException {
        byte[] value = in.readNBytes(length);
        if (value.length != length) {
            throw new EOFException("expected " + length + " bytes, found " + value.length);
        }
        return value;
    }

    private static Nwkcp1DecodingException reject(Nwkcp1RejectionV1 rejection, String message) {
        return new Nwkcp1DecodingException(rejection, message);
    }
}
