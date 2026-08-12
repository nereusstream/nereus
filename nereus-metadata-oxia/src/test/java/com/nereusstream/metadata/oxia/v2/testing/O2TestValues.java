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

import com.nereusstream.domain.aggregate.FrameEncodingPolicyValueV1;
import com.nereusstream.domain.aggregate.InitialStorageEpochV1;
import com.nereusstream.domain.aggregate.PolicyCatalogDigest;
import com.nereusstream.domain.aggregate.ProfileOriginV1;
import com.nereusstream.domain.aggregate.StorageProfileV1;
import com.nereusstream.domain.aggregate.TopicBindingAggregateV1;
import com.nereusstream.domain.aggregate.TopicBindingV1;
import com.nereusstream.domain.bytes.CanonicalBytes;
import com.nereusstream.domain.bytes.Sha256Digest;
import com.nereusstream.domain.codec.DeterministicTopicIdsV1;
import com.nereusstream.domain.identity.DeploymentId;
import com.nereusstream.domain.identity.Id128;
import com.nereusstream.domain.identity.PulsarCellId;
import com.nereusstream.domain.identity.ReservationDomainId;
import com.nereusstream.domain.identity.StorageEpochId;
import com.nereusstream.domain.identity.TopicBindingId;
import com.nereusstream.domain.protocol.ProtocolKindV1;
import com.nereusstream.domain.protocol.PulsarBindingGeneration;
import com.nereusstream.domain.protocol.PulsarPersistenceName;
import com.nereusstream.domain.protocol.PulsarProtocolCellIdentity;
import com.nereusstream.domain.protocol.PulsarTopicIncarnationIdentity;
import com.nereusstream.domain.protocol.PulsarTopicName;
import com.nereusstream.metadata.oxia.v2.mutation.MetadataVersionMapper;
import com.nereusstream.metadata.spi.model.AggregatePublicationCandidate;
import com.nereusstream.metadata.spi.model.PulsarTopicGenerationSelectorStateV1;
import com.nereusstream.metadata.spi.model.PulsarTopicGenerationSelectorValueV1;
import com.nereusstream.metadata.spi.model.PulsarVirtualLedgerNamespaceRegistryValueV1;
import com.nereusstream.metadata.spi.model.VersionedAggregateSnapshot;
import com.nereusstream.metadata.spi.model.VersionedRegistrySnapshot;
import com.nereusstream.metadata.spi.model.VersionedSelectorSnapshot;
import java.nio.charset.StandardCharsets;

/** Valid deterministic Pulsar values for O2 adapter tests. */
public final class O2TestValues {
    public static final DeploymentId DEPLOYMENT = new DeploymentId(new Id128(1, 2));
    public static final ReservationDomainId RESERVATION = new ReservationDomainId(new Id128(3, 4));
    public static final Sha256Digest NAMESPACE_ID = Sha256Digest.hash(bytes("namespace"));

    private O2TestValues() {}

    public static PulsarTopicIncarnationIdentity incarnation(long generation) {
        return new PulsarTopicIncarnationIdentity(
                PulsarPersistenceName.fromString("persistent://tenant/ns/orders"),
                PulsarTopicName.fromString("orders"),
                new PulsarBindingGeneration(generation));
    }

    public static AggregatePublicationCandidate aggregateCandidate(String storedValue) {
        PulsarProtocolCellIdentity cell =
                new PulsarProtocolCellIdentity(DEPLOYMENT, RESERVATION, new PulsarCellId(new Id128(5, 6)));
        PulsarTopicIncarnationIdentity incarnation = incarnation(1);
        TopicBindingId bindingId = DeterministicTopicIdsV1.deriveBindingId(cell, incarnation);
        StorageEpochId epochId = DeterministicTopicIdsV1.deriveStorageEpochId(bindingId, 0);
        TopicBindingAggregateV1 aggregate = new TopicBindingAggregateV1(
                TopicBindingAggregateV1.SCHEMA_VERSION,
                new TopicBindingV1(ProtocolKindV1.PULSAR, bindingId, cell, incarnation),
                new InitialStorageEpochV1(
                        epochId,
                        0,
                        StorageProfileV1.BOOKKEEPER_WAL_ONLY,
                        ProfileOriginV1.TOPIC_EXPLICIT,
                        new PolicyCatalogDigest(Sha256Digest.hash(bytes("catalog"))),
                        FrameEncodingPolicyValueV1.none()));
        CanonicalBytes bytes = bytes(storedValue);
        return new AggregatePublicationCandidate(aggregate, bytes, Sha256Digest.hash(bytes));
    }

    public static VersionedAggregateSnapshot aggregateSnapshot(String storedValue, long version) {
        AggregatePublicationCandidate candidate = aggregateCandidate(storedValue);
        return new VersionedAggregateSnapshot(
                candidate.aggregate(),
                candidate.canonicalStoredBytes(),
                candidate.canonicalStoredDigest(),
                MetadataVersionMapper.fromOxia(version));
    }

    public static PulsarTopicGenerationSelectorValueV1 selector(
            PulsarTopicGenerationSelectorStateV1 state, String storedValue) {
        AggregatePublicationCandidate aggregate = aggregateCandidate("aggregate");
        CanonicalBytes bytes = bytes(storedValue);
        return new PulsarTopicGenerationSelectorValueV1(
                incarnation(1).persistenceName(),
                incarnation(1).bindingGeneration(),
                state,
                aggregate.aggregate().binding().bindingId(),
                aggregate.canonicalStoredDigest(),
                bytes,
                Sha256Digest.hash(bytes));
    }

    public static VersionedSelectorSnapshot selectorSnapshot(
            PulsarTopicGenerationSelectorStateV1 state, String storedValue, long version) {
        return new VersionedSelectorSnapshot(selector(state, storedValue), MetadataVersionMapper.fromOxia(version));
    }

    public static PulsarVirtualLedgerNamespaceRegistryValueV1 registry(long epoch, String storedValue) {
        CanonicalBytes bytes = bytes(storedValue);
        return new PulsarVirtualLedgerNamespaceRegistryValueV1(
                DEPLOYMENT, RESERVATION, NAMESPACE_ID, epoch, bytes, Sha256Digest.hash(bytes));
    }

    public static VersionedRegistrySnapshot registrySnapshot(long epoch, String storedValue, long version) {
        return new VersionedRegistrySnapshot(registry(epoch, storedValue), MetadataVersionMapper.fromOxia(version));
    }

    public static CanonicalBytes bytes(String value) {
        return CanonicalBytes.copyOf(value.getBytes(StandardCharsets.UTF_8));
    }
}
