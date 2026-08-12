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

package com.nereusstream.metadata.oxia.v2.key;

import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.assertThatThrownBy;
import com.nereusstream.domain.codec.PulsarAuthorityLeafCodecV1;
import com.nereusstream.domain.identity.DeploymentId;
import com.nereusstream.domain.identity.Id128;
import com.nereusstream.domain.identity.KafkaTopicId;
import com.nereusstream.domain.identity.ReservationDomainId;
import com.nereusstream.domain.protocol.KafkaTopicIncarnationIdentity;
import com.nereusstream.domain.protocol.KafkaTopicName;
import com.nereusstream.metadata.oxia.v2.testing.O2TestValues;
import org.junit.jupiter.api.Test;

class OxiaV2AuthorityKeysTest {
    private final OxiaV2AuthorityKeys keys = new OxiaV2AuthorityKeys("/nereus/deployments/test");

    @Test
    void selectorKeyUsesTheExactNpn1Digest() {
        var name = O2TestValues.incarnation(1).persistenceName();

        assertThat(keys.selectorKey(name))
                .isEqualTo("/nereus/deployments/test/selectors/v1/" + PulsarAuthorityLeafCodecV1.selectorLeaf(name));
    }

    @Test
    void aggregateKeyUsesTheSameNameDigestAndGeneration19Leaf() {
        var incarnation = O2TestValues.incarnation(1);

        assertThat(keys.aggregateKey(incarnation))
                .isEqualTo("/nereus/deployments/test/aggregates/v1/"
                        + PulsarAuthorityLeafCodecV1.aggregateLeaf(
                                incarnation.persistenceName(), incarnation.bindingGeneration()));
    }

    @Test
    void aggregateKeyRejectsKafkaWithoutDiscoveryFallback() {
        var kafka = new KafkaTopicIncarnationIdentity(new KafkaTopicId(new Id128(7, 8)), new KafkaTopicName("orders"));

        assertThatThrownBy(() -> keys.aggregateKey(kafka))
                .isInstanceOf(IllegalArgumentException.class)
                .hasMessageContaining("only Pulsar");
    }

    @Test
    void registryKeyBindsAllThreeFixedWidthIdentities() {
        DeploymentId deployment = new DeploymentId(new Id128(1, 2));
        ReservationDomainId reservation = new ReservationDomainId(new Id128(3, 4));

        assertThat(keys.registryKey(deployment, reservation, O2TestValues.NAMESPACE_ID))
                .isEqualTo("/nereus/deployments/test/registries/v1/"
                        + deployment.value().toHex()
                        + "/"
                        + reservation.value().toHex()
                        + "/"
                        + O2TestValues.NAMESPACE_ID.toHex());
    }

    @Test
    void rootMustBeAbsoluteNormalizedAndNonRoot() {
        assertThatThrownBy(() -> new OxiaV2AuthorityKeys("relative")).isInstanceOf(IllegalArgumentException.class);
        assertThatThrownBy(() -> new OxiaV2AuthorityKeys("/trailing/")).isInstanceOf(IllegalArgumentException.class);
        assertThatThrownBy(() -> new OxiaV2AuthorityKeys("/double//slash"))
                .isInstanceOf(IllegalArgumentException.class);
    }
}
