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

package com.nereusstream.metadata.oxia.v2.testing;

import com.nereusstream.domain.aggregate.TopicBindingAggregateV1;
import com.nereusstream.domain.bytes.CanonicalBytes;
import com.nereusstream.domain.bytes.Sha256Digest;
import com.nereusstream.domain.identity.DeploymentId;
import com.nereusstream.domain.identity.ReservationDomainId;
import com.nereusstream.domain.protocol.PulsarPersistenceName;
import com.nereusstream.domain.protocol.PulsarTopicIncarnationIdentity;
import com.nereusstream.metadata.oxia.v2.codec.AggregateAuthorityCodec;
import com.nereusstream.metadata.oxia.v2.codec.OxiaV2CodecSet;
import com.nereusstream.metadata.oxia.v2.codec.RegistryAuthorityCodec;
import com.nereusstream.metadata.oxia.v2.codec.SelectorAuthorityCodec;
import com.nereusstream.metadata.spi.model.AggregatePublicationCandidate;
import com.nereusstream.metadata.spi.model.MetadataVersion;
import com.nereusstream.metadata.spi.model.PulsarTopicGenerationSelectorValueV1;
import com.nereusstream.metadata.spi.model.PulsarVirtualLedgerNamespaceRegistryValueV1;
import com.nereusstream.metadata.spi.model.VersionedAggregateSnapshot;
import com.nereusstream.metadata.spi.model.VersionedRegistrySnapshot;
import com.nereusstream.metadata.spi.model.VersionedSelectorSnapshot;
import java.util.HashMap;
import java.util.Map;

/** Test-only typed lookup codecs; unknown bytes deterministically fail as malformed. */
public final class DeterministicAuthorityCodecs {
    private final Map<CanonicalBytes, TopicBindingAggregateV1> aggregates = new HashMap<>();
    private final Map<CanonicalBytes, PulsarTopicGenerationSelectorValueV1> selectors = new HashMap<>();
    private final Map<CanonicalBytes, PulsarVirtualLedgerNamespaceRegistryValueV1> registries = new HashMap<>();

    private final AggregateAuthorityCodec aggregateCodec = new AggregateCodec();
    private final SelectorAuthorityCodec selectorCodec = new SelectorCodec();
    private final RegistryAuthorityCodec registryCodec = new RegistryCodec();

    public OxiaV2CodecSet codecSet() {
        return new OxiaV2CodecSet(aggregateCodec, selectorCodec, registryCodec);
    }

    public AggregateAuthorityCodec aggregate() {
        return aggregateCodec;
    }

    public SelectorAuthorityCodec selector() {
        return selectorCodec;
    }

    public RegistryAuthorityCodec registry() {
        return registryCodec;
    }

    public void register(AggregatePublicationCandidate candidate) {
        aggregates.put(candidate.canonicalStoredBytes(), candidate.aggregate());
    }

    public void register(PulsarTopicGenerationSelectorValueV1 value) {
        selectors.put(value.canonicalStoredBytes(), value);
    }

    public void register(PulsarVirtualLedgerNamespaceRegistryValueV1 value) {
        registries.put(value.canonicalStoredBytes(), value);
    }

    private final class AggregateCodec implements AggregateAuthorityCodec {
        @Override
        public boolean available() {
            return true;
        }

        @Override
        public CanonicalBytes encode(AggregatePublicationCandidate candidate) {
            register(candidate);
            return candidate.canonicalStoredBytes();
        }

        @Override
        public VersionedAggregateSnapshot decode(
                String expectedAuthorityKey,
                PulsarTopicIncarnationIdentity expectedIncarnation,
                CanonicalBytes storedBytes,
                MetadataVersion metadataVersion) {
            TopicBindingAggregateV1 aggregate = require(aggregates, storedBytes);
            return new VersionedAggregateSnapshot(
                    aggregate, storedBytes, Sha256Digest.hash(storedBytes), metadataVersion);
        }
    }

    private final class SelectorCodec implements SelectorAuthorityCodec {
        @Override
        public boolean available() {
            return true;
        }

        @Override
        public CanonicalBytes encode(PulsarTopicGenerationSelectorValueV1 candidate) {
            register(candidate);
            return candidate.canonicalStoredBytes();
        }

        @Override
        public VersionedSelectorSnapshot decode(
                String expectedAuthorityKey,
                PulsarPersistenceName expectedPersistenceName,
                CanonicalBytes storedBytes,
                MetadataVersion metadataVersion) {
            return new VersionedSelectorSnapshot(require(selectors, storedBytes), metadataVersion);
        }
    }

    private final class RegistryCodec implements RegistryAuthorityCodec {
        @Override
        public boolean available() {
            return true;
        }

        @Override
        public CanonicalBytes encode(PulsarVirtualLedgerNamespaceRegistryValueV1 candidate) {
            register(candidate);
            return candidate.canonicalStoredBytes();
        }

        @Override
        public VersionedRegistrySnapshot decode(
                String expectedAuthorityKey,
                DeploymentId expectedDeploymentId,
                ReservationDomainId expectedReservationDomainId,
                Sha256Digest expectedNamespaceId,
                CanonicalBytes storedBytes,
                MetadataVersion metadataVersion) {
            return new VersionedRegistrySnapshot(require(registries, storedBytes), metadataVersion);
        }
    }

    private static <T> T require(Map<CanonicalBytes, T> values, CanonicalBytes storedBytes) {
        T value = values.get(storedBytes);
        if (value == null) {
            throw new IllegalArgumentException("unknown or malformed deterministic authority bytes");
        }
        return value;
    }
}
