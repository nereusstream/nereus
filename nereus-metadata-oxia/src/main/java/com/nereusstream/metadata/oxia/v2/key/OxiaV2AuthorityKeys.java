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

import com.nereusstream.domain.bytes.Sha256Digest;
import com.nereusstream.domain.codec.PulsarAuthorityLeafCodecV1;
import com.nereusstream.domain.identity.DeploymentId;
import com.nereusstream.domain.identity.ReservationDomainId;
import com.nereusstream.domain.protocol.PulsarPersistenceName;
import com.nereusstream.domain.protocol.PulsarTopicIncarnationIdentity;
import com.nereusstream.domain.protocol.TopicIncarnationIdentity;
import java.util.Objects;

/** Exact, versioned single-key authority grammar for the O2 scaffold. */
public final class OxiaV2AuthorityKeys {
    private final String root;

    public OxiaV2AuthorityKeys(String root) {
        this.root = Objects.requireNonNull(root, "root");
        if (!root.startsWith("/") || root.endsWith("/") || root.contains("//")) {
            throw new IllegalArgumentException("root must be an absolute normalized Oxia prefix");
        }
    }

    public String selectorKey(PulsarPersistenceName persistenceName) {
        return root + "/selectors/v1/" + PulsarAuthorityLeafCodecV1.selectorLeaf(persistenceName);
    }

    public String aggregateKey(TopicIncarnationIdentity incarnationIdentity) {
        if (!(Objects.requireNonNull(incarnationIdentity, "incarnationIdentity")
                instanceof PulsarTopicIncarnationIdentity pulsar)) {
            throw new IllegalArgumentException("O2 Oxia aggregate authority accepts only Pulsar incarnations");
        }
        return root
                + "/aggregates/v1/"
                + PulsarAuthorityLeafCodecV1.aggregateLeaf(pulsar.persistenceName(), pulsar.bindingGeneration());
    }

    public String registryKey(
            DeploymentId deploymentId,
            ReservationDomainId reservationDomainId,
            Sha256Digest ledgerIdCompatibilityNamespaceId) {
        Objects.requireNonNull(deploymentId, "deploymentId");
        Objects.requireNonNull(reservationDomainId, "reservationDomainId");
        Objects.requireNonNull(ledgerIdCompatibilityNamespaceId, "ledgerIdCompatibilityNamespaceId");
        return root
                + "/registries/v1/"
                + deploymentId.value().toHex()
                + "/"
                + reservationDomainId.value().toHex()
                + "/"
                + ledgerIdCompatibilityNamespaceId.toHex();
    }
}
