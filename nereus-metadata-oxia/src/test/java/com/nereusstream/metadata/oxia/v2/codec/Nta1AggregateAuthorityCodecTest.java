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
import com.nereusstream.metadata.oxia.v2.mutation.MetadataVersionMapper;
import com.nereusstream.metadata.oxia.v2.testing.O2TestValues;
import com.nereusstream.metadata.spi.model.AggregatePublicationCandidate;
import org.junit.jupiter.api.Test;

class Nta1AggregateAuthorityCodecTest {
    private final Nta1AggregateAuthorityCodec codec = new Nta1AggregateAuthorityCodec();

    @Test
    void encodesAndDecodesExactCanonicalNta1Snapshot() {
        AggregatePublicationCandidate candidate = O2TestValues.productionAggregateCandidate();

        CanonicalBytes encoded = codec.encode(candidate);
        var snapshot = codec.decode(
                "/nereus/test/aggregates/v1/key",
                O2TestValues.incarnation(1),
                encoded,
                MetadataVersionMapper.fromOxia(7));

        assertThat(codec.available()).isTrue();
        assertThat(encoded).isEqualTo(candidate.canonicalStoredBytes());
        assertThat(snapshot.aggregate()).isEqualTo(candidate.aggregate());
        assertThat(snapshot.canonicalStoredDigest()).isEqualTo(Sha256Digest.hash(encoded));
        assertThat(snapshot.canonicalStoredBytes()).isEqualTo(encoded);
    }

    @Test
    void rejectsCallerBytesThatAreNotTheProductionCanonicalEncoding() {
        assertThatThrownBy(() -> codec.encode(O2TestValues.aggregateCandidate("not-nta1")))
                .isInstanceOf(IllegalArgumentException.class);
    }

    @Test
    void rejectsMalformedBytesAndExpectedIncarnationMismatch() {
        AggregatePublicationCandidate candidate = O2TestValues.productionAggregateCandidate();
        assertThatThrownBy(() -> codec.decode(
                        "/nereus/test/aggregates/v1/key",
                        O2TestValues.incarnation(1),
                        CanonicalBytes.copyOf(new byte[] {1, 2, 3}),
                        MetadataVersionMapper.fromOxia(7)))
                .isInstanceOf(IllegalArgumentException.class);
        assertThatThrownBy(() -> codec.decode(
                        "/nereus/test/aggregates/v1/key",
                        O2TestValues.incarnation(2),
                        candidate.canonicalStoredBytes(),
                        MetadataVersionMapper.fromOxia(7)))
                .isInstanceOf(IllegalArgumentException.class);
    }
}
