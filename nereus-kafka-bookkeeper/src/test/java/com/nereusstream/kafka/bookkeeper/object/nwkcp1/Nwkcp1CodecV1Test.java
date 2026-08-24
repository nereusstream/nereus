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

import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.assertThatThrownBy;
import com.nereusstream.domain.bytes.CanonicalBytes;
import com.nereusstream.kafka.bookkeeper.object.ObjectKafkaTestFixtures;
import java.nio.ByteBuffer;
import java.util.List;
import java.util.zip.CRC32C;
import org.junit.jupiter.api.Test;

class Nwkcp1CodecV1Test {
    private static final String ROOT_PREFIX = "cells/01/shards/0007/runs/0000000000000000001";

    @Test
    void roundTripsStrictWireKeyAndHead() {
        var state = ObjectKafkaTestFixtures.checkpoint(100);
        var encoded = Nwkcp1CodecV1.encode(
                ROOT_PREFIX, new Nwkcp1ObjectV1(ObjectKafkaTestFixtures.digest(12), List.of(state)));

        assertThat(encoded.body().length()).isEqualTo(encoded.length());
        assertThat(Nwkcp1ObjectKeyV1.parseObjectDigest(ROOT_PREFIX, encoded.key()))
                .isEqualTo(encoded.digest());
        assertThat(Nwkcp1CodecV1.decodeVerified(
                        ROOT_PREFIX, encoded.key(), encoded.length(), encoded.digest(), encoded.body()))
                .isEqualTo(new Nwkcp1ObjectV1(ObjectKafkaTestFixtures.digest(12), List.of(state)));

        var head = KafkaProtocolCheckpointHeadV1.open(
                ObjectKafkaTestFixtures.digest(12),
                9,
                null,
                encoded,
                List.of(KafkaCheckpointCoverageV1.from(state.vector())));
        CanonicalBytes headBytes =
                KafkaProtocolCheckpointHeadCodecV1.encode(ROOT_PREFIX, ObjectKafkaTestFixtures.digest(12), head);
        assertThat(encoded.length()).isEqualTo(324);
        assertThat(encoded.digest().toHex())
                .isEqualTo("396e99b1b09eaeffc1f26198426d2427550a2033095e4976c40677321c62e8e2");
        assertThat(encoded.key())
                .isEqualTo(ROOT_PREFIX
                        + "/protocol/kafka/nwkcp1-v1/objects/sha256-v1-"
                        + "396e99b1b09eaeffc1f26198426d2427550a2033095e4976c40677321c62e8e2.nwkcp1");
        ByteBuffer objectWire = ByteBuffer.wrap(encoded.body().toByteArray());
        assertThat(objectWire.getLong(0)).isEqualTo(Nwkcp1ConstantsV1.MAGIC);
        assertThat(objectWire.getInt(8)).isEqualTo(64);
        assertThat(objectWire.getInt(12)).isZero();
        assertThat(objectWire.getLong(16)).isEqualTo(324);
        assertThat(objectWire.getInt(56)).isEqualTo(1);
        assertThat(objectWire.getInt(64)).isEqualTo(224);
        assertThat(headBytes.length()).isEqualTo(434);
        assertThat(ByteBuffer.wrap(headBytes.toByteArray()).getLong(0)).isEqualTo(Nwkcp1ConstantsV1.HEAD_MAGIC);
        assertThat(KafkaProtocolCheckpointHeadCodecV1.canonicalValueDigest(
                                ROOT_PREFIX, ObjectKafkaTestFixtures.digest(12), head)
                        .toHex())
                .isEqualTo("a6a88b5a41abef3ea5569541cb869efaa3b33d251015eebd3f312cebed0b7924");
        assertThat(KafkaProtocolCheckpointHeadCodecV1.decode(
                        ROOT_PREFIX, ObjectKafkaTestFixtures.digest(12), headBytes))
                .isEqualTo(head);
        assertThat(KafkaProtocolCheckpointHeadCodecV1.canonicalValueDigest(
                                ROOT_PREFIX, ObjectKafkaTestFixtures.digest(12), head)
                        .isZero())
                .isFalse();
        assertThat(head.terminal().state()).isEqualTo(KafkaProtocolCheckpointHeadStateV1.TERMINAL);
    }

    @Test
    void rejectsHeadOutsideExpectedRootBoundKeyContext() {
        var state = ObjectKafkaTestFixtures.checkpoint(100);
        var rootSha = ObjectKafkaTestFixtures.digest(12);
        var encoded = Nwkcp1CodecV1.encode(ROOT_PREFIX, new Nwkcp1ObjectV1(rootSha, List.of(state)));
        var head = KafkaProtocolCheckpointHeadV1.open(
                rootSha, 9, null, encoded, List.of(KafkaCheckpointCoverageV1.from(state.vector())));
        CanonicalBytes headBytes = KafkaProtocolCheckpointHeadCodecV1.encode(ROOT_PREFIX, rootSha, head);

        assertThatThrownBy(() -> KafkaProtocolCheckpointHeadCodecV1.decode(
                        "cells/01/shards/0008/runs/0000000000000000001", rootSha, headBytes))
                .isInstanceOf(IllegalArgumentException.class);
        assertThatThrownBy(() -> KafkaProtocolCheckpointHeadCodecV1.decode(
                        ROOT_PREFIX, ObjectKafkaTestFixtures.digest(13), headBytes))
                .isInstanceOf(IllegalArgumentException.class);
        assertThatThrownBy(() -> KafkaProtocolCheckpointHeadCodecV1.encode(ROOT_PREFIX + "/other", rootSha, head))
                .isInstanceOf(IllegalArgumentException.class);

        assertThatThrownBy(() -> KafkaProtocolCheckpointHeadCodecV1.decode(
                        ROOT_PREFIX, rootSha, mutateHeadKey(headBytes, 0, 'x')))
                .isInstanceOf(IllegalArgumentException.class);
        assertThatThrownBy(() -> KafkaProtocolCheckpointHeadCodecV1.decode(
                        ROOT_PREFIX, rootSha, mutateHeadKey(headBytes, 6, '/')))
                .isInstanceOf(IllegalArgumentException.class);
        assertThatThrownBy(() -> KafkaProtocolCheckpointHeadCodecV1.decode(
                        ROOT_PREFIX, rootSha, mutateHeadKey(mutateHeadKey(headBytes, 6, '.'), 7, '.')))
                .isInstanceOf(IllegalArgumentException.class);

        int digestOffset = head.checkpointObjectKey().indexOf(Nwkcp1ConstantsV1.DIGEST_TOKEN)
                + Nwkcp1ConstantsV1.DIGEST_TOKEN.length();
        assertThatThrownBy(() -> KafkaProtocolCheckpointHeadCodecV1.decode(
                        ROOT_PREFIX, rootSha, mutateHeadKey(headBytes, digestOffset, '0')))
                .isInstanceOf(IllegalArgumentException.class);
    }

    @Test
    void rejectsHeaderCorruptionTrailingBytesAndNonCanonicalKeys() {
        var encoded = Nwkcp1CodecV1.encode(
                ROOT_PREFIX,
                new Nwkcp1ObjectV1(
                        ObjectKafkaTestFixtures.digest(12), List.of(ObjectKafkaTestFixtures.checkpoint(100))));
        byte[] corrupt = encoded.body().toByteArray();
        corrupt[20] ^= 1;
        assertThatThrownBy(() -> Nwkcp1CodecV1.decode(CanonicalBytes.copyOf(corrupt)))
                .isInstanceOf(Nwkcp1DecodingException.class)
                .extracting(failure -> ((Nwkcp1DecodingException) failure).rejection())
                .isEqualTo(Nwkcp1RejectionV1.OBJECT_LENGTH);

        byte[] trailing = java.util.Arrays.copyOf(
                encoded.body().toByteArray(), encoded.body().length() + 1);
        assertThatThrownBy(() -> Nwkcp1CodecV1.decode(CanonicalBytes.copyOf(trailing)))
                .isInstanceOf(Nwkcp1DecodingException.class)
                .extracting(failure -> ((Nwkcp1DecodingException) failure).rejection())
                .isEqualTo(Nwkcp1RejectionV1.OBJECT_LENGTH);

        assertThatThrownBy(() -> Nwkcp1ObjectKeyV1.parseObjectDigest(
                        ROOT_PREFIX, encoded.key().replace("sha256-v1-", "SHA256-v1-")))
                .isInstanceOf(IllegalArgumentException.class);
    }

    @Test
    void rejectsNullHeadVectorElementAtConstruction() {
        var root = ObjectKafkaTestFixtures.digest(12);
        var encoded = Nwkcp1CodecV1.encode(
                ROOT_PREFIX, new Nwkcp1ObjectV1(root, List.of(ObjectKafkaTestFixtures.checkpoint(100))));

        assertThatThrownBy(() -> new KafkaProtocolCheckpointHeadV1(
                        root,
                        9,
                        KafkaProtocolCheckpointHeadStateV1.OPEN,
                        0,
                        ObjectKafkaTestFixtures.digest(0),
                        encoded.key(),
                        encoded.length(),
                        encoded.digest(),
                        java.util.Arrays.asList((KafkaCheckpointCoverageV1) null)))
                .isInstanceOf(NullPointerException.class)
                .hasMessageContaining("coveredThroughVector element");
    }

    private static CanonicalBytes mutateHeadKey(CanonicalBytes original, int keyOffset, char replacement) {
        byte[] bytes = original.toByteArray();
        int keyLength = Short.toUnsignedInt(ByteBuffer.wrap(bytes).getShort(96));
        if (keyOffset < 0 || keyOffset >= keyLength || replacement <= 0 || replacement >= 128) {
            throw new IllegalArgumentException("test key mutation is outside the encoded Head key");
        }
        bytes[98 + keyOffset] = (byte) replacement;
        CRC32C crc = new CRC32C();
        crc.update(bytes, 0, bytes.length - Integer.BYTES);
        ByteBuffer.wrap(bytes, bytes.length - Integer.BYTES, Integer.BYTES).putInt((int) crc.getValue());
        return CanonicalBytes.copyOf(bytes);
    }
}
