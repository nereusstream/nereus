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
import com.nereusstream.domain.identity.Id128;
import com.nereusstream.domain.identity.KafkaTopicId;
import com.nereusstream.domain.protocol.KafkaTopicIncarnationIdentity;
import com.nereusstream.domain.protocol.KafkaTopicName;
import com.nereusstream.domain.protocol.ProtocolKindV1;
import com.nereusstream.domain.protocol.PulsarBindingGeneration;
import com.nereusstream.domain.protocol.PulsarPersistenceName;
import com.nereusstream.domain.protocol.PulsarTopicIncarnationIdentity;
import com.nereusstream.domain.protocol.PulsarTopicName;
import com.nereusstream.domain.protocol.TopicIncarnationIdentity;
import java.nio.ByteBuffer;
import java.util.Objects;

/** Strict encoder and decoder for canonical NTI1 native topic incarnations. */
public final class TopicIncarnationIdentityCodecV1 {
    private static final String MAGIC = "NTI1";
    private static final int HEADER_LENGTH = 4 + Short.BYTES;

    private TopicIncarnationIdentityCodecV1() {}

    public static CanonicalBytes encode(TopicIncarnationIdentity identity) {
        Objects.requireNonNull(identity, "identity");
        if (identity instanceof KafkaTopicIncarnationIdentity kafka) {
            CanonicalBytes name = kafka.topicName().bytes();
            int length = WireCodecSupport.checkedSize(HEADER_LENGTH, Id128.LENGTH, Integer.BYTES, name.length());
            ByteBuffer buffer = WireCodecSupport.allocate(length, MAGIC, ProtocolKindV1.KAFKA);
            WireCodecSupport.putId128(buffer, kafka.topicId().value());
            WireCodecSupport.putLength(buffer, name.length());
            buffer.put(name.toByteArray());
            return WireCodecSupport.finish(buffer);
        }
        if (identity instanceof PulsarTopicIncarnationIdentity pulsar) {
            CanonicalBytes persistenceName = pulsar.persistenceName().value().bytes();
            CanonicalBytes topicName = pulsar.topicName().value().bytes();
            int length = WireCodecSupport.checkedSize(
                    HEADER_LENGTH,
                    Integer.BYTES,
                    persistenceName.length(),
                    Integer.BYTES,
                    topicName.length(),
                    Long.BYTES);
            ByteBuffer buffer = WireCodecSupport.allocate(length, MAGIC, ProtocolKindV1.PULSAR);
            WireCodecSupport.putLength(buffer, persistenceName.length());
            buffer.put(persistenceName.toByteArray());
            WireCodecSupport.putLength(buffer, topicName.length());
            buffer.put(topicName.toByteArray());
            buffer.putLong(pulsar.bindingGeneration().value());
            return WireCodecSupport.finish(buffer);
        }
        throw new IllegalArgumentException("unsupported Topic incarnation identity variant");
    }

    public static TopicIncarnationIdentity decode(byte[] encoded) {
        Objects.requireNonNull(encoded, "encoded");
        ByteBuffer buffer = ByteBuffer.wrap(encoded);
        WireCodecSupport.requireMagic(buffer, MAGIC);
        ProtocolKindV1 protocolKind = WireCodecSupport.readProtocolKind(buffer);
        return switch (protocolKind) {
            case KAFKA -> decodeKafka(buffer);
            case PULSAR -> decodePulsar(buffer);
        };
    }

    private static KafkaTopicIncarnationIdentity decodeKafka(ByteBuffer buffer) {
        KafkaTopicId topicId = new KafkaTopicId(WireCodecSupport.readId128(buffer));
        int topicNameLength = WireCodecSupport.readBoundedLength(buffer, "Kafka topic name");
        byte[] topicName = WireCodecSupport.readBytes(buffer, topicNameLength);
        WireCodecSupport.requireRemaining(buffer, 0);
        return new KafkaTopicIncarnationIdentity(topicId, KafkaTopicName.fromBytes(topicName));
    }

    private static PulsarTopicIncarnationIdentity decodePulsar(ByteBuffer buffer) {
        int persistenceNameLength = WireCodecSupport.readBoundedLength(buffer, "Pulsar persistence name");
        byte[] persistenceName = WireCodecSupport.readBytes(buffer, persistenceNameLength);
        int topicNameLength = WireCodecSupport.readBoundedLength(buffer, "Pulsar topic name");
        byte[] topicName = WireCodecSupport.readBytes(buffer, topicNameLength);
        if (buffer.remaining() < Long.BYTES) {
            throw new IllegalArgumentException("truncated Pulsar binding generation");
        }
        PulsarBindingGeneration generation = new PulsarBindingGeneration(buffer.getLong());
        WireCodecSupport.requireRemaining(buffer, 0);
        return new PulsarTopicIncarnationIdentity(
                PulsarPersistenceName.fromBytes(persistenceName), PulsarTopicName.fromBytes(topicName), generation);
    }
}
