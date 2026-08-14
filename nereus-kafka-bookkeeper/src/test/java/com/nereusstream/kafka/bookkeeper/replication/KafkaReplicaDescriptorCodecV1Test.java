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

import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.assertThatThrownBy;
import com.nereusstream.domain.bytes.CanonicalBytes;
import com.nereusstream.domain.bytes.Sha256Digest;
import com.nereusstream.domain.protocol.KafkaTopicIncarnationIdentity;
import com.nereusstream.domain.protocol.KafkaTopicName;
import com.nereusstream.kafka.bookkeeper.protocol.KafkaPartitionFenceV1;
import java.nio.ByteBuffer;
import java.util.Arrays;
import java.util.zip.CRC32C;
import org.junit.jupiter.api.Test;

class KafkaReplicaDescriptorCodecV1Test {
    @Test
    void roundTripsTheExactKrd1Golden() {
        KafkaReplicaCommitDescriptorV1 descriptor = KafkaReplicationTestFixtures.descriptor(100, 103, 2);

        CanonicalBytes encoded = KafkaReplicaCommitDescriptorCodecV1.encode(descriptor);

        assertThat(KafkaReplicaCommitDescriptorCodecV1.decode(encoded)).isEqualTo(descriptor);
        assertThat(encoded.length()).isEqualTo(470);
        assertThat(Sha256Digest.hash(encoded).toHex())
                .isEqualTo("0eb0a4afe61086fb6574e98279a68549d4a80e0bfd47c9d81147e9530946108d");
    }

    @Test
    void observationRecordRoundTripsItsPredecessorChainAndDescriptor() {
        KafkaReplicaObservationRecordV1 first = new KafkaReplicaObservationRecordV1(
                0,
                Sha256Digest.copyOf(new byte[Sha256Digest.LENGTH]),
                10,
                KafkaReplicationTestFixtures.descriptor(100, 101, 2));
        KafkaReplicaObservationRecordV1 second = new KafkaReplicaObservationRecordV1(
                1,
                KafkaReplicaObservationRecordCodecV1.digest(first),
                20,
                KafkaReplicationTestFixtures.descriptor(101, 102, 3));

        CanonicalBytes encoded = KafkaReplicaObservationRecordCodecV1.encode(second);

        assertThat(KafkaReplicaObservationRecordCodecV1.decode(encoded)).isEqualTo(second);
        assertThat(encoded.length()).isEqualTo(534);
        assertThat(Sha256Digest.hash(encoded).toHex())
                .isEqualTo("6c8433d3a1b0e4f946b9a58ab0f9eab70f9bc54fd26ae8e2a2e75a67bcd0c58b");
    }

    @Test
    void rejectsCrcCorruptionTruncationAndTrailingBytes() {
        byte[] encoded = KafkaReplicaCommitDescriptorCodecV1.encode(
                        KafkaReplicationTestFixtures.descriptor(100, 101, 2))
                .toByteArray();
        byte[] corrupt = encoded.clone();
        corrupt[40] ^= 1;

        assertThatThrownBy(() -> KafkaReplicaCommitDescriptorCodecV1.decode(CanonicalBytes.copyOf(corrupt)))
                .isInstanceOf(IllegalArgumentException.class);
        assertThatThrownBy(() -> KafkaReplicaCommitDescriptorCodecV1.decode(
                        CanonicalBytes.copyOf(Arrays.copyOf(encoded, encoded.length - 1))))
                .isInstanceOf(IllegalArgumentException.class);
        byte[] trailing = Arrays.copyOf(encoded, encoded.length + 1);
        trailing[trailing.length - 1] = 1;
        assertThatThrownBy(() -> KafkaReplicaCommitDescriptorCodecV1.decode(CanonicalBytes.copyOf(trailing)))
                .isInstanceOf(IllegalArgumentException.class);
    }

    @Test
    void rejectsUnknownSourceKindObservationModeAndBodyTail() {
        KafkaReplicaCommitDescriptorV1 descriptor = KafkaReplicationTestFixtures.descriptor(100, 101, 2);
        byte[] sourceKind =
                KafkaReplicaCommitDescriptorCodecV1.encode(descriptor).toByteArray();
        int topicNameLength =
                descriptor.fence().topicIncarnation().topicName().bytes().length();
        sourceKind[178 + topicNameLength] = 99;
        rewriteCrc(sourceKind);
        byte[] observationMode =
                KafkaReplicaCommitDescriptorCodecV1.encode(descriptor).toByteArray();
        observationMode[179 + topicNameLength] = 99;
        rewriteCrc(observationMode);
        byte[] tail = addUnknownBodyTail(
                KafkaReplicaCommitDescriptorCodecV1.encode(descriptor).toByteArray());

        assertThatThrownBy(() -> KafkaReplicaCommitDescriptorCodecV1.decode(CanonicalBytes.copyOf(sourceKind)))
                .isInstanceOf(IllegalArgumentException.class);
        assertThatThrownBy(() -> KafkaReplicaCommitDescriptorCodecV1.decode(CanonicalBytes.copyOf(observationMode)))
                .isInstanceOf(IllegalArgumentException.class);
        assertThatThrownBy(() -> KafkaReplicaCommitDescriptorCodecV1.decode(CanonicalBytes.copyOf(tail)))
                .isInstanceOf(IllegalArgumentException.class);
    }

    @Test
    void maximumCanonicalTopicNameRemainsInsideTheDescriptorCap() {
        KafkaReplicaCommitDescriptorV1 original = KafkaReplicationTestFixtures.descriptor(100, 101, 2);
        KafkaPartitionFenceV1 fence = new KafkaPartitionFenceV1(
                original.fence().bindingId(),
                new KafkaTopicIncarnationIdentity(
                        original.fence().topicIncarnation().topicId(), new KafkaTopicName("a".repeat(249))),
                original.fence().partitionId(),
                original.fence().bindingGeneration(),
                original.fence().storageEpochId(),
                original.fence().ownerEpoch(),
                original.fence().kafkaLeaderEpoch());
        KafkaReplicaCommitDescriptorV1 maximum = new KafkaReplicaCommitDescriptorV1(
                fence,
                original.validatedStateVersion(),
                original.startOffset(),
                original.endOffsetExclusive(),
                original.encodedDataBytes(),
                original.aggregateAssignedPayloadSha256(),
                original.source(),
                original.protocolProof(),
                original.observationMode());

        CanonicalBytes encoded = KafkaReplicaCommitDescriptorCodecV1.encode(maximum);

        assertThat(encoded.length())
                .isLessThanOrEqualTo(KafkaReplicaCommitDescriptorCodecV1.FORMAT_MAX_DESCRIPTOR_BYTES);
        assertThat(KafkaReplicaCommitDescriptorCodecV1.decode(encoded)).isEqualTo(maximum);
    }

    @Test
    void descriptorRejectsSourceCoverageOrPayloadSubstitution() {
        KafkaReplicaCommitDescriptorV1 descriptor = KafkaReplicationTestFixtures.descriptor(100, 101, 2);

        assertThatThrownBy(() -> new KafkaReplicaCommitDescriptorV1(
                        descriptor.fence(),
                        descriptor.validatedStateVersion(),
                        descriptor.startOffset(),
                        descriptor.endOffsetExclusive() + 1,
                        descriptor.encodedDataBytes(),
                        descriptor.aggregateAssignedPayloadSha256(),
                        descriptor.source(),
                        descriptor.protocolProof(),
                        descriptor.observationMode()))
                .isInstanceOf(IllegalArgumentException.class);
        assertThatThrownBy(() -> new KafkaReplicaCommitDescriptorV1(
                        descriptor.fence(),
                        descriptor.validatedStateVersion(),
                        descriptor.startOffset(),
                        descriptor.endOffsetExclusive(),
                        descriptor.encodedDataBytes(),
                        KafkaReplicationTestFixtures.digest(99),
                        descriptor.source(),
                        descriptor.protocolProof(),
                        descriptor.observationMode()))
                .isInstanceOf(IllegalArgumentException.class);
    }

    @Test
    void bookKeeperFactoryBindsTheExactPublishedSourceAndFourProtocolComponents() {
        KafkaReplicaCommitDescriptorV1 descriptor = KafkaReplicationTestFixtures.descriptorFromBookKeeperCut();

        assertThat(descriptor.source().sourceKind()).isEqualTo(KafkaReplicaSourceKindV1.BOOKKEEPER_RUN);
        assertThat(descriptor.source().physicalStartUnit()).isEqualTo(1);
        assertThat(descriptor.source().physicalEndUnitExclusive()).isEqualTo(2);
        assertThat(descriptor.source().sourceGeneration()).isEqualTo(3);
        assertThat(descriptor.protocolProof())
                .isEqualTo(new KafkaReplicaProtocolProofV1(
                        KafkaReplicationTestFixtures.ref(4).contentDigest(),
                        KafkaReplicationTestFixtures.ref(6).contentDigest(),
                        KafkaReplicationTestFixtures.ref(7).contentDigest(),
                        KafkaReplicationTestFixtures.ref(8).contentDigest()));
    }

    private static byte[] addUnknownBodyTail(byte[] encoded) {
        int bodyLength = ByteBuffer.wrap(encoded, 4, 4).getInt();
        byte[] result = new byte[encoded.length + 1];
        System.arraycopy(encoded, 0, result, 0, 8 + bodyLength);
        result[8 + bodyLength] = 1;
        ByteBuffer.wrap(result, 4, 4).putInt(bodyLength + 1);
        rewriteCrc(result);
        return result;
    }

    private static void rewriteCrc(byte[] bytes) {
        CRC32C crc = new CRC32C();
        crc.update(bytes, 0, bytes.length - Integer.BYTES);
        ByteBuffer.wrap(bytes, bytes.length - Integer.BYTES, Integer.BYTES).putInt((int) crc.getValue());
    }
}
