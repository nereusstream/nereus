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

package com.nereusstream.metadata.oxia.v2.codec;

import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.assertThatThrownBy;
import com.nereusstream.domain.bytes.CanonicalBytes;
import com.nereusstream.domain.bytes.Sha256Digest;
import com.nereusstream.domain.codec.PulsarAuthorityLeafCodecV1;
import com.nereusstream.domain.identity.TopicBindingId;
import com.nereusstream.domain.protocol.PulsarBindingGeneration;
import com.nereusstream.domain.protocol.PulsarPersistenceName;
import com.nereusstream.metadata.oxia.v2.mutation.MetadataVersionMapper;
import com.nereusstream.metadata.spi.model.PulsarTopicGenerationSelectorStateV1;
import com.nereusstream.metadata.spi.model.PulsarTopicGenerationSelectorValueV1;
import java.util.Arrays;
import java.util.HexFormat;
import org.junit.jupiter.api.Test;

class Nps1SelectorAuthorityCodecTest {
    private static final PulsarPersistenceName NAME = PulsarPersistenceName.fromString("tenant/ns/persistent/topic");
    private static final TopicBindingId BINDING_ID = new TopicBindingId(digestRange(0));
    private static final Sha256Digest AGGREGATE_DIGEST = digestRange(32);
    private static final String AUTHORITY_KEY =
            "/nereus/test/selectors/v1/" + PulsarAuthorityLeafCodecV1.selectorLeaf(NAME);
    private static final String GOLDEN_HEX = "4e5053310001000200000000000000070000001a"
            + "74656e616e742f6e732f70657273697374656e742f746f706963"
            + "000102030405060708090a0b0c0d0e0f101112131415161718191a1b1c1d1e1f"
            + "202122232425262728292a2b2c2d2e2f303132333435363738393a3b3c3d3e3f";

    private final Nps1SelectorAuthorityCodec codec = new Nps1SelectorAuthorityCodec();

    @Test
    void freezesExactGoldenAndRoundTrip() {
        PulsarTopicGenerationSelectorValueV1 value = value();

        assertThat(codec.available()).isTrue();
        assertThat(value.canonicalStoredBytes().toHex()).isEqualTo(GOLDEN_HEX);
        assertThat(value.canonicalStoredBytes().length()).isEqualTo(110);
        assertThat(codec.encode(value)).isEqualTo(value.canonicalStoredBytes());

        var snapshot =
                codec.decode(AUTHORITY_KEY, NAME, value.canonicalStoredBytes(), MetadataVersionMapper.fromOxia(9));
        assertThat(snapshot.value()).isEqualTo(value);
        assertThat(snapshot.metadataVersion()).isEqualTo(MetadataVersionMapper.fromOxia(9));
    }

    @Test
    void rejectsNonCanonicalCandidateBytesOrDigest() {
        PulsarTopicGenerationSelectorValueV1 value = value();
        CanonicalBytes other = CanonicalBytes.copyOf(new byte[] {1});
        var invalid = new PulsarTopicGenerationSelectorValueV1(
                value.persistenceName(),
                value.generation(),
                value.state(),
                value.aggregateBindingId(),
                value.aggregateCanonicalStoredDigest(),
                other,
                Sha256Digest.hash(other));

        assertThatThrownBy(() -> codec.encode(invalid)).isInstanceOf(IllegalArgumentException.class);
    }

    @Test
    void rejectsWrongKeyIdentityAndExpectedName() {
        PulsarTopicGenerationSelectorValueV1 value = value();
        assertThatThrownBy(() -> codec.decode(
                        "/nereus/test/selectors/v1/wrong",
                        NAME,
                        value.canonicalStoredBytes(),
                        MetadataVersionMapper.fromOxia(1)))
                .isInstanceOf(IllegalArgumentException.class);
        assertThatThrownBy(() -> codec.decode(
                        AUTHORITY_KEY,
                        PulsarPersistenceName.fromString("tenant/ns/persistent/other"),
                        value.canonicalStoredBytes(),
                        MetadataVersionMapper.fromOxia(1)))
                .isInstanceOf(IllegalArgumentException.class);
    }

    @Test
    void rejectsCorruptHeaderStateGenerationLengthUtf8AndTrailingBytes() {
        byte[] valid = value().canonicalStoredBytes().toByteArray();
        assertRejected(mutate(valid, 0, (byte) 'X'));
        assertRejected(mutate(valid, 5, (byte) 2));
        assertRejected(mutate(valid, 7, (byte) 5));
        assertRejected(mutate(valid, 15, (byte) 0));
        assertRejected(mutate(valid, 19, (byte) 25));
        assertRejected(mutate(valid, 20, (byte) 0xc3));
        assertRejected(Arrays.copyOf(valid, valid.length + 1));
    }

    @Test
    void rejectsNonCanonicalPersistenceNameBeforeEncoding() {
        assertThatThrownBy(() -> Nps1SelectorAuthorityCodec.createValue(
                        PulsarPersistenceName.fromString("tenant/ns/persistent/a%2fb"),
                        new PulsarBindingGeneration(1),
                        PulsarTopicGenerationSelectorStateV1.RESERVED,
                        BINDING_ID,
                        AGGREGATE_DIGEST))
                .isInstanceOf(IllegalArgumentException.class);
    }

    private void assertRejected(byte[] encoded) {
        assertThatThrownBy(() -> codec.decode(
                        AUTHORITY_KEY, NAME, CanonicalBytes.copyOf(encoded), MetadataVersionMapper.fromOxia(1)))
                .isInstanceOf(IllegalArgumentException.class);
    }

    private static PulsarTopicGenerationSelectorValueV1 value() {
        return Nps1SelectorAuthorityCodec.createValue(
                NAME,
                new PulsarBindingGeneration(7),
                PulsarTopicGenerationSelectorStateV1.ACTIVE,
                BINDING_ID,
                AGGREGATE_DIGEST);
    }

    private static byte[] mutate(byte[] source, int index, byte value) {
        byte[] copy = source.clone();
        copy[index] = value;
        return copy;
    }

    private static Sha256Digest digestRange(int start) {
        byte[] bytes = new byte[Sha256Digest.LENGTH];
        for (int index = 0; index < bytes.length; index++) {
            bytes[index] = (byte) (start + index);
        }
        return Sha256Digest.copyOf(HexFormat.of().parseHex(HexFormat.of().formatHex(bytes)));
    }
}
