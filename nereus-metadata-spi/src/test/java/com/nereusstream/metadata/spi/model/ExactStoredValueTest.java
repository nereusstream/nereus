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

package com.nereusstream.metadata.spi.model;

import static com.nereusstream.metadata.spi.SpiTestFixtures.aggregate;
import static com.nereusstream.metadata.spi.SpiTestFixtures.aggregateCandidate;
import static com.nereusstream.metadata.spi.SpiTestFixtures.aggregateSnapshot;
import static com.nereusstream.metadata.spi.SpiTestFixtures.bytes;
import static com.nereusstream.metadata.spi.SpiTestFixtures.metadataVersion;
import static com.nereusstream.metadata.spi.SpiTestFixtures.selectorValue;
import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.assertThatThrownBy;
import com.nereusstream.domain.bytes.Sha256Digest;
import com.nereusstream.domain.identity.DeploymentId;
import com.nereusstream.domain.identity.Id128;
import com.nereusstream.domain.identity.ReservationDomainId;
import org.junit.jupiter.api.Test;

class ExactStoredValueTest {
    @Test
    void aggregateSnapshotProjectsOneAggregateInstance() {
        VersionedAggregateSnapshot snapshot = aggregateSnapshot("aggregate-bytes", 7);

        assertThat(snapshot.binding()).isSameAs(snapshot.aggregate().binding());
        assertThat(snapshot.initialEpoch()).isSameAs(snapshot.aggregate().initialEpoch());
        assertThat(snapshot.canonicalStoredDigest()).isEqualTo(Sha256Digest.hash(snapshot.canonicalStoredBytes()));
    }

    @Test
    void everyStoredValueRejectsAByteDigestMismatch() {
        Sha256Digest wrongDigest = Sha256Digest.hash(bytes("different"));

        assertThatThrownBy(() -> new AggregatePublicationCandidate(aggregate(), bytes("aggregate"), wrongDigest))
                .isInstanceOf(IllegalArgumentException.class);
        assertThatThrownBy(() -> new VersionedAggregateSnapshot(
                        aggregate(), bytes("aggregate"), wrongDigest, metadataVersion(1)))
                .isInstanceOf(IllegalArgumentException.class);

        PulsarTopicGenerationSelectorValueV1 selector =
                selectorValue(PulsarTopicGenerationSelectorStateV1.RESERVED, "selector");
        assertThatThrownBy(() -> new PulsarTopicGenerationSelectorValueV1(
                        selector.persistenceName(),
                        selector.generation(),
                        selector.state(),
                        selector.aggregateBindingId(),
                        selector.aggregateCanonicalStoredDigest(),
                        selector.canonicalStoredBytes(),
                        wrongDigest))
                .isInstanceOf(IllegalArgumentException.class);

        assertThatThrownBy(() -> new PulsarVirtualLedgerNamespaceRegistryValueV1(
                        new DeploymentId(new Id128(1, 1)),
                        new ReservationDomainId(new Id128(2, 2)),
                        Sha256Digest.hash(bytes("instance-id")),
                        0,
                        bytes("registry"),
                        wrongDigest))
                .isInstanceOf(IllegalArgumentException.class);
    }

    @Test
    void registryFoundationDoesNotFreezeWriterCapacityOrSchema() {
        assertThat(PulsarVirtualLedgerNamespaceRegistryValueV1.class.getDeclaredFields())
                .extracting(java.lang.reflect.Field::getName)
                .doesNotContain("maxWriterCount", "writers", "assignments", "allocatorMode");
        assertThat(aggregateCandidate("aggregate").canonicalStoredBytes().toHex())
                .isNotEmpty();
    }
}
