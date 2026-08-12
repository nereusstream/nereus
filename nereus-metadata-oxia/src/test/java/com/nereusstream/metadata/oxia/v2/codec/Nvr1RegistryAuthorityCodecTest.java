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
import com.nereusstream.domain.identity.DeploymentId;
import com.nereusstream.domain.identity.Id128;
import com.nereusstream.metadata.oxia.v2.mutation.MetadataVersionMapper;
import com.nereusstream.metadata.oxia.v2.testing.R1TestValues;
import com.nereusstream.metadata.spi.model.PulsarVirtualLedgerNamespaceRegistryValueV1;
import java.util.List;
import org.junit.jupiter.api.Test;

class Nvr1RegistryAuthorityCodecTest {
    private final RegistryAuthorityCodec codec = OxiaV2CodecSet.productionR1().registry();

    @Test
    void mapsExactDomainValueWithoutAnOpaqueSecondSchema() {
        var evidence = R1TestValues.initialEvidence(2);
        PulsarVirtualLedgerNamespaceRegistryValueV1 value =
                R1TestValues.storedValue(evidence, List.of(R1TestValues.assignment(0)));

        var decoded = codec.decode(
                "/registry",
                value.deploymentId(),
                value.reservationDomainId(),
                value.ledgerIdCompatibilityNamespaceId(),
                codec.encode(value),
                MetadataVersionMapper.fromOxia(7));

        assertThat(decoded.value()).isEqualTo(value);
        assertThat(decoded.value().domainValue()).isEqualTo(value.domainValue());
        assertThat(decoded.sliceView(R1TestValues.assignment(0).pulsarCellId()).registryMetadataVersion())
                .isEqualTo(MetadataVersionMapper.fromOxia(7));
    }

    @Test
    void rejectsExpectedKeyIdentityMismatch() {
        var value = R1TestValues.storedValue(R1TestValues.initialEvidence(2), List.of());

        assertThatThrownBy(() -> codec.decode(
                        "/registry",
                        new DeploymentId(new Id128(9, 10)),
                        value.reservationDomainId(),
                        value.ledgerIdCompatibilityNamespaceId(),
                        value.canonicalStoredBytes(),
                        MetadataVersionMapper.fromOxia(1)))
                .isInstanceOf(IllegalArgumentException.class);
    }

    @Test
    void productionR1MakesAllThreeCodecsAvailable() {
        assertThat(OxiaV2CodecSet.productionR1().allAvailable()).isTrue();
        assertThat(OxiaV2CodecSet.productionP1().allAvailable()).isFalse();
    }
}
