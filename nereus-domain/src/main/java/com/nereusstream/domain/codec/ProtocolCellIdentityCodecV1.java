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

import com.nereusstream.domain.bytes.CanonicalBytes;
import com.nereusstream.domain.identity.DeploymentId;
import com.nereusstream.domain.identity.Id128;
import com.nereusstream.domain.identity.KafkaCellId;
import com.nereusstream.domain.identity.PulsarCellId;
import com.nereusstream.domain.identity.ReservationDomainId;
import com.nereusstream.domain.protocol.KafkaProtocolCellIdentity;
import com.nereusstream.domain.protocol.ProtocolCellIdentity;
import com.nereusstream.domain.protocol.ProtocolKindV1;
import com.nereusstream.domain.protocol.PulsarProtocolCellIdentity;
import java.nio.ByteBuffer;
import java.nio.charset.StandardCharsets;
import java.util.Objects;

/** Strict encoder and decoder for canonical NPC1 Protocol Cell identities. */
public final class ProtocolCellIdentityCodecV1 {
    private static final String MAGIC = "NPC1";
    private static final int KAFKA_LENGTH = 4 + Short.BYTES + 2 * Id128.LENGTH;
    private static final int PULSAR_LENGTH = 4 + Short.BYTES + 3 * Id128.LENGTH;

    private ProtocolCellIdentityCodecV1() {}

    public static CanonicalBytes encode(ProtocolCellIdentity identity) {
        Objects.requireNonNull(identity, "identity");
        if (identity instanceof KafkaProtocolCellIdentity kafka) {
            ByteBuffer buffer = WireCodecSupport.allocate(KAFKA_LENGTH, MAGIC, ProtocolKindV1.KAFKA);
            WireCodecSupport.putId128(buffer, kafka.deploymentId().value());
            WireCodecSupport.putId128(buffer, kafka.cellId().value());
            return WireCodecSupport.finish(buffer);
        }
        if (identity instanceof PulsarProtocolCellIdentity pulsar) {
            ByteBuffer buffer = WireCodecSupport.allocate(PULSAR_LENGTH, MAGIC, ProtocolKindV1.PULSAR);
            WireCodecSupport.putId128(buffer, pulsar.deploymentId().value());
            WireCodecSupport.putId128(buffer, pulsar.reservationDomainId().value());
            WireCodecSupport.putId128(buffer, pulsar.cellId().value());
            return WireCodecSupport.finish(buffer);
        }
        throw new IllegalArgumentException("unsupported Protocol Cell identity variant");
    }

    public static ProtocolCellIdentity decode(byte[] encoded) {
        Objects.requireNonNull(encoded, "encoded");
        ByteBuffer buffer = ByteBuffer.wrap(encoded);
        WireCodecSupport.requireMagic(buffer, MAGIC);
        ProtocolKindV1 protocolKind = WireCodecSupport.readProtocolKind(buffer);
        return switch (protocolKind) {
            case KAFKA -> {
                WireCodecSupport.requireRemaining(buffer, 2 * Id128.LENGTH);
                DeploymentId deploymentId = new DeploymentId(WireCodecSupport.readId128(buffer));
                KafkaCellId cellId = new KafkaCellId(WireCodecSupport.readId128(buffer));
                yield new KafkaProtocolCellIdentity(deploymentId, cellId);
            }
            case PULSAR -> {
                WireCodecSupport.requireRemaining(buffer, 3 * Id128.LENGTH);
                DeploymentId deploymentId = new DeploymentId(WireCodecSupport.readId128(buffer));
                ReservationDomainId reservationDomainId = new ReservationDomainId(WireCodecSupport.readId128(buffer));
                PulsarCellId cellId = new PulsarCellId(WireCodecSupport.readId128(buffer));
                yield new PulsarProtocolCellIdentity(deploymentId, reservationDomainId, cellId);
            }
        };
    }
}

final class WireCodecSupport {
    private WireCodecSupport() {}

    static ByteBuffer allocate(int length, String magic, ProtocolKindV1 protocolKind) {
        ByteBuffer buffer = ByteBuffer.allocate(length);
        buffer.put(magic.getBytes(StandardCharsets.US_ASCII));
        buffer.putShort((short) protocolKind.code());
        return buffer;
    }

    static void putId128(ByteBuffer buffer, Id128 value) {
        buffer.putLong(value.highBits());
        buffer.putLong(value.lowBits());
    }

    static void putLength(ByteBuffer buffer, int length) {
        if (length < 0) {
            throw new IllegalArgumentException("length must be non-negative");
        }
        buffer.putInt(length);
    }

    static CanonicalBytes finish(ByteBuffer buffer) {
        if (buffer.hasRemaining()) {
            throw new IllegalStateException("canonical encoder did not fill its exact allocation");
        }
        return CanonicalBytes.copyOf(buffer.array());
    }

    static void requireMagic(ByteBuffer buffer, String expected) {
        byte[] magic = expected.getBytes(StandardCharsets.US_ASCII);
        if (buffer.remaining() < magic.length) {
            throw new IllegalArgumentException("truncated " + expected + " magic");
        }
        for (byte expectedByte : magic) {
            if (buffer.get() != expectedByte) {
                throw new IllegalArgumentException("wrong " + expected + " magic");
            }
        }
    }

    static ProtocolKindV1 readProtocolKind(ByteBuffer buffer) {
        if (buffer.remaining() < Short.BYTES) {
            throw new IllegalArgumentException("truncated protocol kind");
        }
        return ProtocolKindV1.fromCode(Short.toUnsignedInt(buffer.getShort()));
    }

    static Id128 readId128(ByteBuffer buffer) {
        if (buffer.remaining() < Id128.LENGTH) {
            throw new IllegalArgumentException("truncated 128-bit identity");
        }
        return new Id128(buffer.getLong(), buffer.getLong());
    }

    static int readBoundedLength(ByteBuffer buffer, String fieldName) {
        if (buffer.remaining() < Integer.BYTES) {
            throw new IllegalArgumentException("truncated " + fieldName + " length");
        }
        long length = Integer.toUnsignedLong(buffer.getInt());
        if (length > Integer.MAX_VALUE || length > buffer.remaining()) {
            throw new IllegalArgumentException(fieldName + " length exceeds bounded input");
        }
        return (int) length;
    }

    static byte[] readBytes(ByteBuffer buffer, int length) {
        if (length < 0 || length > buffer.remaining()) {
            throw new IllegalArgumentException("byte length exceeds bounded input");
        }
        byte[] value = new byte[length];
        buffer.get(value);
        return value;
    }

    static void requireRemaining(ByteBuffer buffer, int expected) {
        if (buffer.remaining() != expected) {
            throw new IllegalArgumentException("unexpected trailing or truncated bytes");
        }
    }

    static int checkedSize(int... fields) {
        int size = 0;
        for (int field : fields) {
            size = Math.addExact(size, field);
        }
        return size;
    }
}
