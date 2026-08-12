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

package com.nereusstream.domain.codec;

import com.nereusstream.domain.aggregate.FrameEncodingPolicyCatalogV1;
import com.nereusstream.domain.aggregate.FrameEncodingPolicyValueV1;
import com.nereusstream.domain.aggregate.InitialStorageEpochV1;
import com.nereusstream.domain.aggregate.PolicyCatalogDigest;
import com.nereusstream.domain.aggregate.ProfileOriginV1;
import com.nereusstream.domain.aggregate.StorageProfileV1;
import com.nereusstream.domain.aggregate.TopicBindingAggregateV1;
import com.nereusstream.domain.aggregate.TopicBindingAggregateValidatorV1;
import com.nereusstream.domain.aggregate.TopicBindingV1;
import com.nereusstream.domain.bytes.CanonicalBytes;
import com.nereusstream.domain.bytes.Sha256Digest;
import com.nereusstream.domain.identity.StorageEpochId;
import com.nereusstream.domain.identity.TopicBindingId;
import com.nereusstream.domain.protocol.KafkaTopicName;
import com.nereusstream.domain.protocol.ProtocolCellIdentity;
import com.nereusstream.domain.protocol.ProtocolKindV1;
import com.nereusstream.domain.protocol.PulsarClassicNameV1;
import com.nereusstream.domain.protocol.TopicIncarnationIdentity;
import java.nio.ByteBuffer;
import java.nio.charset.StandardCharsets;
import java.util.Arrays;
import java.util.Objects;

/** Strict canonical production encoder and bounded parser for NTA1 v1. */
public final class Nta1CodecV1 {
    public static final int FIXED_BYTES = 129;
    public static final int MAX_CELL_BYTES = 54;
    public static final int MAX_INCARNATION_BYTES = Math.addExact(
            22,
            Math.addExact(PulsarClassicNameV1.MAX_PERSISTENCE_NAME_BYTES, PulsarClassicNameV1.MAX_TOPIC_NAME_BYTES));
    public static final int MAX_NTA1_BYTES =
            Math.addExact(FIXED_BYTES, Math.addExact(MAX_CELL_BYTES, MAX_INCARNATION_BYTES));

    private static final byte[] MAGIC = "NTA1".getBytes(StandardCharsets.US_ASCII);
    private static final int SCHEMA_VERSION = 1;
    private static final int ABSENT_SEALED_END = 0;

    private Nta1CodecV1() {}

    public static CanonicalBytes encode(TopicBindingAggregateV1 aggregate) {
        TopicBindingAggregateValidatorV1.validate(aggregate);
        byte[] cellBytes = ProtocolCellIdentityCodecV1.encode(
                        aggregate.binding().cellIdentity())
                .toByteArray();
        byte[] incarnationBytes = TopicIncarnationIdentityCodecV1.encode(
                        aggregate.binding().incarnationIdentity())
                .toByteArray();
        int length = Math.addExact(FIXED_BYTES, Math.addExact(cellBytes.length, incarnationBytes.length));
        if (length > MAX_NTA1_BYTES) {
            throw new IllegalArgumentException("aggregate exceeds the NTA1 v1 parser cap");
        }

        ByteBuffer buffer = ByteBuffer.allocate(length);
        buffer.put(MAGIC);
        putU16(buffer, aggregate.aggregateSchemaVersion(), "aggregate schema version");
        putU16(buffer, aggregate.binding().protocolKind().code(), "protocol kind");
        buffer.put(aggregate.binding().bindingId().digest().bytes().toByteArray());
        putU32(buffer, cellBytes.length);
        buffer.put(cellBytes);
        putU32(buffer, incarnationBytes.length);
        buffer.put(incarnationBytes);
        buffer.put(aggregate.initialEpoch().storageEpochId().digest().bytes().toByteArray());
        buffer.putLong(aggregate.initialEpoch().epochOrdinal());
        putU16(buffer, aggregate.initialEpoch().storageProfile().code(), "storage profile");
        putU16(buffer, aggregate.initialEpoch().profileOrigin().code(), "profile origin");
        buffer.put(
                aggregate.initialEpoch().policyCatalogDigest().digest().bytes().toByteArray());
        putU16(buffer, aggregate.initialEpoch().frameEncodingPolicy().kind(), "frame-policy kind");
        putU16(buffer, aggregate.initialEpoch().frameEncodingPolicy().formatVersion(), "frame-policy version");
        buffer.put((byte) ABSENT_SEALED_END);
        if (buffer.hasRemaining()) {
            throw new IllegalStateException("NTA1 encoder did not fill its exact checked allocation");
        }
        return CanonicalBytes.copyOf(buffer.array());
    }

    public static TopicBindingAggregateV1 decode(CanonicalBytes encoded) {
        Objects.requireNonNull(encoded, "encoded");
        return decode(encoded.toByteArray());
    }

    public static TopicBindingAggregateV1 decode(byte[] encoded) {
        Objects.requireNonNull(encoded, "encoded");
        if (encoded.length > MAX_NTA1_BYTES) {
            throw new IllegalArgumentException("NTA1 input exceeds the 8397-byte persisted-v1 parser cap");
        }

        BoundedReader reader = new BoundedReader(encoded);
        reader.requireMagic();
        int schemaVersion = reader.readU16("aggregate schema version");
        if (schemaVersion != SCHEMA_VERSION) {
            throw new IllegalArgumentException("unknown NTA1 aggregate schema version: " + schemaVersion);
        }
        ProtocolKindV1 protocolKind = ProtocolKindV1.fromCode(reader.readU16("protocol kind"));
        TopicBindingId bindingId =
                new TopicBindingId(Sha256Digest.copyOf(reader.readFixed(Sha256Digest.LENGTH, "Topic Binding ID")));
        byte[] cellBytes = reader.readLengthFramed("Cell identity", MAX_CELL_BYTES);
        byte[] incarnationBytes = reader.readLengthFramed("incarnation identity", MAX_INCARNATION_BYTES);
        StorageEpochId storageEpochId =
                new StorageEpochId(Sha256Digest.copyOf(reader.readFixed(Sha256Digest.LENGTH, "Storage Epoch ID")));
        long epochOrdinal = reader.readLong("epoch ordinal");
        StorageProfileV1 storageProfile = StorageProfileV1.fromCode(reader.readU16("storage profile"));
        ProfileOriginV1 profileOrigin = ProfileOriginV1.fromCode(reader.readU16("profile origin"));
        PolicyCatalogDigest catalogDigest = new PolicyCatalogDigest(
                Sha256Digest.copyOf(reader.readFixed(Sha256Digest.LENGTH, "policy catalog SHA-256")));
        FrameEncodingPolicyValueV1 framePolicy = FrameEncodingPolicyCatalogV1.fromCodes(
                reader.readU16("frame-policy kind"), reader.readU16("frame-policy version"));
        if (reader.readUnsignedByte("sealed-end presence") != ABSENT_SEALED_END) {
            throw new IllegalArgumentException("NTA1 v1 sealed-end presence must be zero");
        }
        reader.requireEof();

        ProtocolCellIdentity cellIdentity = ProtocolCellIdentityCodecV1.decode(cellBytes);
        validateIncarnationFraming(incarnationBytes, protocolKind);
        TopicIncarnationIdentity incarnationIdentity = TopicIncarnationIdentityCodecV1.decode(incarnationBytes);
        TopicBindingAggregateV1 aggregate = new TopicBindingAggregateV1(
                schemaVersion,
                new TopicBindingV1(protocolKind, bindingId, cellIdentity, incarnationIdentity),
                new InitialStorageEpochV1(
                        storageEpochId, epochOrdinal, storageProfile, profileOrigin, catalogDigest, framePolicy));
        TopicBindingAggregateValidatorV1.validate(aggregate);
        if (!Arrays.equals(encoded, encode(aggregate).toByteArray())) {
            throw new IllegalArgumentException("NTA1 input is not the canonical v1 encoding");
        }
        return aggregate;
    }

    private static void validateIncarnationFraming(byte[] encoded, ProtocolKindV1 expectedProtocol) {
        ByteBuffer buffer = ByteBuffer.wrap(encoded);
        requireBytes(buffer, 4, "NTI1 magic");
        if (buffer.get() != 'N' || buffer.get() != 'T' || buffer.get() != 'I' || buffer.get() != '1') {
            throw new IllegalArgumentException("wrong NTI1 magic");
        }
        requireBytes(buffer, Short.BYTES, "NTI1 protocol kind");
        ProtocolKindV1 protocol = ProtocolKindV1.fromCode(Short.toUnsignedInt(buffer.getShort()));
        if (protocol != expectedProtocol) {
            throw new IllegalArgumentException("NTA1 and NTI1 protocol kinds do not agree");
        }
        switch (protocol) {
            case KAFKA -> {
                requireBytes(buffer, 16, "Kafka topic ID");
                buffer.position(buffer.position() + 16);
                skipBoundedLength(buffer, "Kafka topic name", KafkaTopicName.MAX_LENGTH);
                requireEof(buffer, "Kafka NTI1");
            }
            case PULSAR -> {
                skipBoundedLength(buffer, "Pulsar persistence name", PulsarClassicNameV1.MAX_PERSISTENCE_NAME_BYTES);
                skipBoundedLength(buffer, "Pulsar topic name", PulsarClassicNameV1.MAX_TOPIC_NAME_BYTES);
                requireBytes(buffer, Long.BYTES, "Pulsar binding generation");
                buffer.position(buffer.position() + Long.BYTES);
                requireEof(buffer, "Pulsar NTI1");
            }
        }
    }

    private static void skipBoundedLength(ByteBuffer buffer, String field, int cap) {
        requireBytes(buffer, Integer.BYTES, field + " length");
        long declared = Integer.toUnsignedLong(buffer.getInt());
        if (declared > Integer.MAX_VALUE) {
            throw new IllegalArgumentException(field + " unsigned length exceeds Integer.MAX_VALUE");
        }
        if (declared > cap) {
            throw new IllegalArgumentException(field + " length exceeds its NTA1 v1 cap");
        }
        if (declared > buffer.remaining()) {
            throw new IllegalArgumentException(field + " length exceeds remaining NTI1 input");
        }
        buffer.position(Math.addExact(buffer.position(), Math.toIntExact(declared)));
    }

    private static void requireBytes(ByteBuffer buffer, int required, String field) {
        if (required < 0 || buffer.remaining() < required) {
            throw new IllegalArgumentException("truncated " + field);
        }
    }

    private static void requireEof(ByteBuffer buffer, String encoding) {
        if (buffer.hasRemaining()) {
            throw new IllegalArgumentException(encoding + " has trailing bytes");
        }
    }

    private static void putU16(ByteBuffer buffer, int value, String field) {
        if (value < 0 || value > 0xffff) {
            throw new IllegalArgumentException(field + " does not fit u16");
        }
        buffer.putShort((short) value);
    }

    private static void putU32(ByteBuffer buffer, int value) {
        if (value < 0) {
            throw new IllegalArgumentException("length does not fit u32");
        }
        buffer.putInt(value);
    }

    private static final class BoundedReader {
        private final ByteBuffer buffer;

        private BoundedReader(byte[] encoded) {
            buffer = ByteBuffer.wrap(encoded);
        }

        private void requireMagic() {
            byte[] actual = readFixed(MAGIC.length, "NTA1 magic");
            if (!Arrays.equals(actual, MAGIC)) {
                throw new IllegalArgumentException("wrong NTA1 magic");
            }
        }

        private int readU16(String field) {
            requireRemaining(Short.BYTES, field);
            return Short.toUnsignedInt(buffer.getShort());
        }

        private long readU32(String field) {
            requireRemaining(Integer.BYTES, field + " length");
            return Integer.toUnsignedLong(buffer.getInt());
        }

        private int readUnsignedByte(String field) {
            requireRemaining(Byte.BYTES, field);
            return Byte.toUnsignedInt(buffer.get());
        }

        private long readLong(String field) {
            requireRemaining(Long.BYTES, field);
            return buffer.getLong();
        }

        private byte[] readFixed(int length, String field) {
            requireRemaining(length, field);
            byte[] value = new byte[length];
            buffer.get(value);
            return value;
        }

        private byte[] readLengthFramed(String field, int cap) {
            long declared = readU32(field);
            if (declared > Integer.MAX_VALUE) {
                throw new IllegalArgumentException(field + " unsigned length exceeds Integer.MAX_VALUE");
            }
            if (declared > cap) {
                throw new IllegalArgumentException(field + " length exceeds its NTA1 v1 cap");
            }
            if (declared > buffer.remaining()) {
                throw new IllegalArgumentException(field + " length exceeds remaining NTA1 input");
            }
            int actualLength = Math.toIntExact(declared);
            byte[] value = new byte[actualLength];
            buffer.get(value);
            return value;
        }

        private void requireRemaining(int required, String field) {
            if (required < 0 || buffer.remaining() < required) {
                throw new IllegalArgumentException("truncated NTA1 " + field);
            }
        }

        private void requireEof() {
            if (buffer.hasRemaining()) {
                throw new IllegalArgumentException("NTA1 input has trailing bytes");
            }
        }
    }
}
