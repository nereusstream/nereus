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

import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.assertThatThrownBy;
import com.nereusstream.domain.aggregate.FrameEncodingPolicyCatalogV1;
import com.nereusstream.domain.aggregate.FrameEncodingPolicyValueV1;
import com.nereusstream.domain.aggregate.InitialStorageEpochV1;
import com.nereusstream.domain.aggregate.StorageProfileV1;
import com.nereusstream.domain.aggregate.TopicBindingAggregateV1;
import com.nereusstream.domain.bytes.CanonicalBytes;
import com.nereusstream.domain.protocol.KafkaTopicName;
import java.nio.ByteBuffer;
import java.util.Arrays;
import org.junit.jupiter.api.Test;

class Nta1CodecV1NegativeTest {
    @Test
    void acceptsExactlySixProtocolProfilePolicyRows() {
        for (StorageProfileV1 profile : StorageProfileV1.values()) {
            TopicBindingAggregateV1 kafka = Nta1ProductionTestFixtures.kafka("matrix", profile);
            TopicBindingAggregateV1 pulsar = Nta1ProductionTestFixtures.pulsar("t", "n", "matrix", profile);
            assertThat(Nta1CodecV1.decode(Nta1CodecV1.encode(kafka))).isEqualTo(kafka);
            assertThat(Nta1CodecV1.decode(Nta1CodecV1.encode(pulsar))).isEqualTo(pulsar);
        }
    }

    @Test
    void rejectsEveryNonAcceptedPolicyShapeAndProfileMismatch() {
        assertEncodeRejected(
                withPolicy(Nta1ProductionTestFixtures.kafkaTypical(), FrameEncodingPolicyCatalogV1.none()));
        assertEncodeRejected(withPolicy(
                Nta1ProductionTestFixtures.kafkaMinimum(), FrameEncodingPolicyCatalogV1.zstdFastIfSmaller()));
        assertEncodeRejected(withPolicy(
                Nta1ProductionTestFixtures.kafkaTypical(),
                new FrameEncodingPolicyValueV1(2, 1, CanonicalBytes.empty())));
        assertEncodeRejected(withPolicy(
                Nta1ProductionTestFixtures.kafkaTypical(),
                new FrameEncodingPolicyValueV1(1, 1, CanonicalBytes.copyOf(new byte[] {1}))));
    }

    @Test
    void rejectsUnknownOuterDiscriminatorsAndVersions() {
        byte[] valid = kafkaTypicalBytes();
        assertDecodeRejected(withU16(valid, 4, 2));
        assertDecodeRejected(withU16(valid, 6, 3));
        Layout layout = Layout.of(valid);
        assertDecodeRejected(withU16(valid, layout.profileOffset(), 4));
        assertDecodeRejected(withU16(valid, layout.originOffset(), 6));
        assertDecodeRejected(withU16(valid, layout.policyKindOffset(), 2));
        assertDecodeRejected(withU16(valid, layout.policyVersionOffset(), 2));
    }

    @Test
    void rejectsProtocolAndDeterministicIdentityMismatches() {
        byte[] valid = kafkaTypicalBytes();
        assertDecodeRejected(withU16(valid, 6, 2));
        assertDecodeRejected(withFlippedByte(valid, 8));
        Layout layout = Layout.of(valid);
        assertDecodeRejected(withFlippedByte(valid, layout.epochIdOffset()));
        assertDecodeRejected(withLong(valid, layout.ordinalOffset(), 1));
    }

    @Test
    void rejectsUnsignedOversizeLengthsBeforeNestedAllocation() {
        byte[] valid = kafkaTypicalBytes();
        assertDecodeRejected(withInt(valid, 40, -1));
        assertDecodeRejected(withInt(valid, 40, Nta1CodecV1.MAX_CELL_BYTES + 1));
        Layout layout = Layout.of(valid);
        int kafkaNameLengthOffset = layout.incarnationStart() + 4 + 2 + 16;
        assertDecodeRejected(withInt(valid, kafkaNameLengthOffset, -1));
        assertDecodeRejected(withInt(valid, kafkaNameLengthOffset, KafkaTopicName.MAX_LENGTH + 1));
    }

    @Test
    void rejectsMalformedUtf8TruncationTrailingPayloadAndIllegalPresence() {
        byte[] valid = kafkaTypicalBytes();
        Layout layout = Layout.of(valid);
        int kafkaNameStart = layout.incarnationStart() + 4 + 2 + 16 + 4;
        assertDecodeRejected(withByte(valid, kafkaNameStart, (byte) 0xc3));
        assertDecodeRejected(Arrays.copyOf(valid, valid.length - 1));
        assertDecodeRejected(Arrays.copyOf(valid, 3));
        assertDecodeRejected(Arrays.copyOf(valid, Nta1CodecV1.MAX_NTA1_BYTES + 1));
        assertDecodeRejected(withByte(valid, layout.sealedPresenceOffset(), (byte) 1));
        assertDecodeRejected(append(valid, (byte) 0));
        assertDecodeRejected(insertBefore(valid, layout.sealedPresenceOffset(), (byte) 0x7f));
    }

    @Test
    void rejectsKafka250AndPulsar4097ByteNames() {
        assertThatThrownBy(() -> new KafkaTopicName("k".repeat(250))).isInstanceOf(IllegalArgumentException.class);

        String topicPrefix = "persistent://t/n/";
        String oversizeTopic = topicPrefix + "p".repeat(4097 - topicPrefix.length());
        assertEncodeRejected(Nta1ProductionTestFixtures.pulsar(
                "t/n/persistent/p",
                oversizeTopic,
                StorageProfileV1.OBJECT_WAL,
                FrameEncodingPolicyCatalogV1.zstdFastIfSmaller()));
        assertEncodeRejected(Nta1ProductionTestFixtures.pulsar(
                "p".repeat(4097),
                "persistent://t/n/p",
                StorageProfileV1.OBJECT_WAL,
                FrameEncodingPolicyCatalogV1.zstdFastIfSmaller()));
    }

    @Test
    void rejectsNonClassicMismatchedAndNonCanonicalPulsarNames() {
        assertEncodeRejected(Nta1ProductionTestFixtures.pulsar(
                "t/n/persistent/a",
                "topic://t/n/a",
                StorageProfileV1.OBJECT_WAL,
                FrameEncodingPolicyCatalogV1.zstdFastIfSmaller()));
        assertEncodeRejected(Nta1ProductionTestFixtures.pulsar(
                "t/n/persistent/a",
                "segment://t/n/a/0000-ffff-1",
                StorageProfileV1.OBJECT_WAL,
                FrameEncodingPolicyCatalogV1.zstdFastIfSmaller()));
        assertEncodeRejected(Nta1ProductionTestFixtures.pulsar(
                "t/n/persistent/a",
                "persistent://t/n/b",
                StorageProfileV1.OBJECT_WAL,
                FrameEncodingPolicyCatalogV1.zstdFastIfSmaller()));
        assertEncodeRejected(Nta1ProductionTestFixtures.pulsar(
                "t/n/persistent/%61",
                "persistent://t/n/a", StorageProfileV1.OBJECT_WAL, FrameEncodingPolicyCatalogV1.zstdFastIfSmaller()));
    }

    @Test
    void exactCheckedCapsAreNotRounded() {
        assertThat(Nta1CodecV1.FIXED_BYTES).isEqualTo(129);
        assertThat(Nta1CodecV1.MAX_CELL_BYTES).isEqualTo(54);
        assertThat(Nta1CodecV1.MAX_INCARNATION_BYTES).isEqualTo(8214);
        assertThat(Nta1CodecV1.MAX_NTA1_BYTES).isEqualTo(8397);
        assertThat(Nta1CodecV1.encode(Nta1ProductionTestFixtures.pulsarBoundary())
                        .length())
                .isEqualTo(8395);
    }

    private static TopicBindingAggregateV1 withPolicy(
            TopicBindingAggregateV1 aggregate, FrameEncodingPolicyValueV1 policy) {
        InitialStorageEpochV1 epoch = aggregate.initialEpoch();
        return new TopicBindingAggregateV1(
                aggregate.aggregateSchemaVersion(),
                aggregate.binding(),
                new InitialStorageEpochV1(
                        epoch.storageEpochId(),
                        epoch.epochOrdinal(),
                        epoch.storageProfile(),
                        epoch.profileOrigin(),
                        epoch.policyCatalogDigest(),
                        policy));
    }

    private static byte[] kafkaTypicalBytes() {
        return Nta1CodecV1.encode(Nta1ProductionTestFixtures.kafkaTypical()).toByteArray();
    }

    private static void assertEncodeRejected(TopicBindingAggregateV1 aggregate) {
        assertThatThrownBy(() -> Nta1CodecV1.encode(aggregate)).isInstanceOf(IllegalArgumentException.class);
    }

    private static void assertDecodeRejected(byte[] encoded) {
        assertThatThrownBy(() -> Nta1CodecV1.decode(encoded)).isInstanceOf(IllegalArgumentException.class);
    }

    private static byte[] withU16(byte[] source, int offset, int value) {
        byte[] copy = source.clone();
        ByteBuffer.wrap(copy).putShort(offset, (short) value);
        return copy;
    }

    private static byte[] withInt(byte[] source, int offset, int value) {
        byte[] copy = source.clone();
        ByteBuffer.wrap(copy).putInt(offset, value);
        return copy;
    }

    private static byte[] withLong(byte[] source, int offset, long value) {
        byte[] copy = source.clone();
        ByteBuffer.wrap(copy).putLong(offset, value);
        return copy;
    }

    private static byte[] withByte(byte[] source, int offset, byte value) {
        byte[] copy = source.clone();
        copy[offset] = value;
        return copy;
    }

    private static byte[] withFlippedByte(byte[] source, int offset) {
        byte[] copy = source.clone();
        copy[offset] ^= 1;
        return copy;
    }

    private static byte[] append(byte[] source, byte value) {
        byte[] copy = Arrays.copyOf(source, source.length + 1);
        copy[source.length] = value;
        return copy;
    }

    private static byte[] insertBefore(byte[] source, int offset, byte value) {
        byte[] copy = new byte[source.length + 1];
        System.arraycopy(source, 0, copy, 0, offset);
        copy[offset] = value;
        System.arraycopy(source, offset, copy, offset + 1, source.length - offset);
        return copy;
    }

    private record Layout(
            int incarnationStart,
            int epochIdOffset,
            int ordinalOffset,
            int profileOffset,
            int originOffset,
            int policyKindOffset,
            int policyVersionOffset,
            int sealedPresenceOffset) {
        static Layout of(byte[] encoded) {
            ByteBuffer buffer = ByteBuffer.wrap(encoded);
            int cellLength = buffer.getInt(40);
            int incarnationLengthOffset = 44 + cellLength;
            int incarnationLength = buffer.getInt(incarnationLengthOffset);
            int incarnationStart = incarnationLengthOffset + 4;
            int epochIdOffset = incarnationStart + incarnationLength;
            int ordinalOffset = epochIdOffset + 32;
            int profileOffset = ordinalOffset + 8;
            int originOffset = profileOffset + 2;
            int policyKindOffset = originOffset + 2 + 32;
            int policyVersionOffset = policyKindOffset + 2;
            int sealedPresenceOffset = policyVersionOffset + 2;
            return new Layout(
                    incarnationStart,
                    epochIdOffset,
                    ordinalOffset,
                    profileOffset,
                    originOffset,
                    policyKindOffset,
                    policyVersionOffset,
                    sealedPresenceOffset);
        }
    }
}
