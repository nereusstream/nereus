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

package com.nereusstream.metadata.spi;

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
import com.nereusstream.domain.identity.KafkaCellId;
import com.nereusstream.domain.identity.KafkaTopicId;
import com.nereusstream.domain.identity.StorageEpochId;
import com.nereusstream.domain.identity.TopicBindingId;
import com.nereusstream.domain.protocol.KafkaProtocolCellIdentity;
import com.nereusstream.domain.protocol.KafkaTopicIncarnationIdentity;
import com.nereusstream.domain.protocol.KafkaTopicName;
import com.nereusstream.domain.protocol.ProtocolKindV1;
import com.nereusstream.metadata.spi.model.AggregatePublicationCandidate;
import com.nereusstream.metadata.spi.model.MetadataVersion;
import com.nereusstream.metadata.spi.model.PulsarTopicGenerationSelectorStateV1;
import com.nereusstream.metadata.spi.model.PulsarTopicGenerationSelectorValueV1;
import com.nereusstream.metadata.spi.model.VersionedAggregateSnapshot;
import com.nereusstream.metadata.spi.model.VersionedSelectorSnapshot;
import java.nio.ByteBuffer;
import java.nio.charset.StandardCharsets;

/** Shared deterministic values for metadata SPI tests. */
public final class SpiTestFixtures {
    private SpiTestFixtures() {}

    public static TopicBindingAggregateV1 aggregate() {
        KafkaProtocolCellIdentity cell =
                new KafkaProtocolCellIdentity(new DeploymentId(new Id128(1, 2)), new KafkaCellId(new Id128(3, 4)));
        KafkaTopicIncarnationIdentity incarnation =
                new KafkaTopicIncarnationIdentity(new KafkaTopicId(new Id128(5, 6)), new KafkaTopicName("orders"));
        TopicBindingId bindingId = DeterministicTopicIdsV1.deriveBindingId(cell, incarnation);
        StorageEpochId epochId = DeterministicTopicIdsV1.deriveStorageEpochId(bindingId, 0);
        return new TopicBindingAggregateV1(
                1,
                new TopicBindingV1(ProtocolKindV1.KAFKA, bindingId, cell, incarnation),
                new InitialStorageEpochV1(
                        epochId,
                        0,
                        StorageProfileV1.OBJECT_WAL,
                        ProfileOriginV1.TOPIC_EXPLICIT,
                        new PolicyCatalogDigest(Sha256Digest.hash(bytes("catalog"))),
                        new FrameEncodingPolicyValueV1(17, 29, bytes("opaque"))));
    }

    public static AggregatePublicationCandidate aggregateCandidate(String storedValue) {
        CanonicalBytes bytes = bytes(storedValue);
        return new AggregatePublicationCandidate(aggregate(), bytes, Sha256Digest.hash(bytes));
    }

    public static VersionedAggregateSnapshot aggregateSnapshot(String storedValue, long version) {
        AggregatePublicationCandidate candidate = aggregateCandidate(storedValue);
        return new VersionedAggregateSnapshot(
                candidate.aggregate(),
                candidate.canonicalStoredBytes(),
                candidate.canonicalStoredDigest(),
                metadataVersion(version));
    }

    public static PulsarTopicGenerationSelectorValueV1 selectorValue(
            PulsarTopicGenerationSelectorStateV1 state, String storedValue) {
        AggregatePublicationCandidate aggregateCandidate = aggregateCandidate("aggregate");
        CanonicalBytes bytes = bytes(storedValue);
        return new PulsarTopicGenerationSelectorValueV1(
                com.nereusstream.domain.protocol.PulsarPersistenceName.fromString("persistent://tenant/ns/orders"),
                new com.nereusstream.domain.protocol.PulsarBindingGeneration(1),
                state,
                aggregateCandidate.aggregate().binding().bindingId(),
                aggregateCandidate.canonicalStoredDigest(),
                bytes,
                Sha256Digest.hash(bytes));
    }

    public static VersionedSelectorSnapshot selectorSnapshot(
            PulsarTopicGenerationSelectorStateV1 state, String storedValue, long version) {
        return new VersionedSelectorSnapshot(selectorValue(state, storedValue), metadataVersion(version));
    }

    public static MetadataVersion metadataVersion(long version) {
        return new MetadataVersion(CanonicalBytes.copyOf(
                ByteBuffer.allocate(Long.BYTES).putLong(version).array()));
    }

    public static CanonicalBytes bytes(String value) {
        return CanonicalBytes.copyOf(value.getBytes(StandardCharsets.UTF_8));
    }
}
