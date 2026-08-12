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

package com.nereusstream.metadata.oxia.v2;

import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.assertThatThrownBy;
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
import com.nereusstream.domain.codec.Nta1CodecV1;
import com.nereusstream.domain.identity.DeploymentId;
import com.nereusstream.domain.identity.Id128;
import com.nereusstream.domain.identity.PulsarCellId;
import com.nereusstream.domain.identity.ReservationDomainId;
import com.nereusstream.domain.protocol.ProtocolKindV1;
import com.nereusstream.domain.protocol.PulsarBindingGeneration;
import com.nereusstream.domain.protocol.PulsarPersistenceName;
import com.nereusstream.domain.protocol.PulsarProtocolCellIdentity;
import com.nereusstream.domain.protocol.PulsarTopicIncarnationIdentity;
import com.nereusstream.domain.protocol.PulsarTopicName;
import com.nereusstream.metadata.oxia.v2.capability.PulsarTopicAuthorityException;
import com.nereusstream.metadata.oxia.v2.continuity.RevalidationScheduler;
import com.nereusstream.metadata.oxia.v2.continuity.StoreContinuityState;
import com.nereusstream.metadata.spi.model.AggregatePublicationCandidate;
import com.nereusstream.metadata.spi.model.PulsarTopicGenerationSelectorStateV1;
import io.oxia.testcontainers.OxiaContainer;
import java.nio.charset.StandardCharsets;
import java.time.Duration;
import java.util.UUID;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicBoolean;
import java.util.concurrent.atomic.AtomicInteger;
import org.junit.jupiter.api.Test;
import org.testcontainers.junit.jupiter.Container;
import org.testcontainers.junit.jupiter.Testcontainers;
import org.testcontainers.utility.DockerImageName;

@Testcontainers
class PulsarP1OxiaIntegrationTest {
    private static final String IMAGE = "nereus/oxia-o1:37a17bef1720";

    @Container
    private static final OxiaContainer OXIA =
            new OxiaContainer(DockerImageName.parse(IMAGE).asCompatibleSubstituteFor("oxia/oxia")).withShards(4);

    @Test
    void exactLifecycleSurvivesRestartAndRecordNotificationInvalidates() throws Exception {
        String authorityRoot = "/nereus/v2/p1/" + UUID.randomUUID();
        AggregatePublicationCandidate generationOne = candidate(1, "orders");
        CountDownLatch invalidated = new CountDownLatch(1);

        try (TestScheduler scheduler = new TestScheduler();
                OxiaV2CapabilityStore store = connect(authorityRoot, scheduler)) {
            var coordinator = store.pulsarTopicAuthorityCoordinator();
            var active =
                    coordinator.activate(generationOne).toCompletableFuture().join();
            assertThat(active.selector().value().state()).isEqualTo(PulsarTopicGenerationSelectorStateV1.ACTIVE);
            assertThat(coordinator.activate(generationOne).toCompletableFuture().join())
                    .isEqualTo(active);

            try (var registration =
                    store.registerPulsarAuthorityInvalidation(incarnation(generationOne), invalidated::countDown)) {
                var deleting = coordinator
                        .beginDeletion(incarnation(generationOne).persistenceName())
                        .toCompletableFuture()
                        .join();
                assertThat(invalidated.await(10, TimeUnit.SECONDS)).isTrue();
                coordinator.completeDeletion(deleting).toCompletableFuture().join();
            }
            assertThat(scheduler.requests()).isPositive();
        }

        try (TestScheduler scheduler = new TestScheduler();
                OxiaV2CapabilityStore restarted = connect(authorityRoot, scheduler)) {
            AggregatePublicationCandidate generationTwo = candidate(2, "orders");
            var recreated = restarted
                    .pulsarTopicAuthorityCoordinator()
                    .activate(generationTwo)
                    .toCompletableFuture()
                    .join();
            assertThat(recreated.selector().value().generation().value()).isEqualTo(2);
            assertThat(recreated.selector().value().state()).isEqualTo(PulsarTopicGenerationSelectorStateV1.ACTIVE);
            assertThat(restarted
                            .aggregateReader()
                            .readAggregate(incarnation(generationTwo))
                            .toCompletableFuture()
                            .join())
                    .contains(recreated.aggregate());
        }
    }

    @Test
    void concurrentExactCreatorsConvergeAndConflictingIncarnationFailsClosed() throws Exception {
        String authorityRoot = "/nereus/v2/p1/" + UUID.randomUUID();
        AggregatePublicationCandidate candidate = candidate(1, "orders");
        try (TestScheduler firstScheduler = new TestScheduler();
                TestScheduler secondScheduler = new TestScheduler();
                OxiaV2CapabilityStore first = connect(authorityRoot, firstScheduler);
                OxiaV2CapabilityStore second = connect(authorityRoot, secondScheduler)) {
            CompletableFuture<?> left =
                    first.pulsarTopicAuthorityCoordinator().activate(candidate).toCompletableFuture();
            CompletableFuture<?> right =
                    second.pulsarTopicAuthorityCoordinator().activate(candidate).toCompletableFuture();
            CompletableFuture.allOf(left, right).join();
            assertThat(left.join()).isEqualTo(right.join());

            assertThatThrownBy(() -> second.pulsarTopicAuthorityCoordinator()
                            .activate(candidate(1, "orders", 7))
                            .toCompletableFuture()
                            .join())
                    .hasRootCauseInstanceOf(PulsarTopicAuthorityException.class);
        }
    }

    private static OxiaV2CapabilityStore connect(String authorityRoot, TestScheduler scheduler) throws Exception {
        OxiaV2CapabilityStore store = OxiaV2CapabilityStoreFactory.connect(
                        new OxiaV2StoreConfiguration(OXIA.getServiceAddress(), "default", authorityRoot), scheduler)
                .get(30, TimeUnit.SECONDS);
        long deadline = System.nanoTime() + Duration.ofSeconds(30).toNanos();
        while (store.continuitySnapshot().state() != StoreContinuityState.READY && System.nanoTime() < deadline) {
            Thread.sleep(10);
        }
        assertThat(store.continuitySnapshot().state()).isEqualTo(StoreContinuityState.READY);
        assertThat(store.pulsarSelectorReady()).isTrue();
        return store;
    }

    private static PulsarTopicIncarnationIdentity incarnation(AggregatePublicationCandidate candidate) {
        return (PulsarTopicIncarnationIdentity) candidate.aggregate().binding().incarnationIdentity();
    }

    private static AggregatePublicationCandidate candidate(long generation, String logicalTopic) {
        return candidate(generation, logicalTopic, 6);
    }

    private static AggregatePublicationCandidate candidate(long generation, String logicalTopic, long cellLow) {
        PulsarProtocolCellIdentity cell = new PulsarProtocolCellIdentity(
                new DeploymentId(new Id128(1, 2)),
                new ReservationDomainId(new Id128(3, 4)),
                new PulsarCellId(new Id128(5, cellLow)));
        PulsarTopicIncarnationIdentity incarnation = new PulsarTopicIncarnationIdentity(
                PulsarPersistenceName.fromString("tenant/ns/persistent/orders"),
                PulsarTopicName.fromString("persistent://tenant/ns/" + logicalTopic),
                new PulsarBindingGeneration(generation));
        var bindingId = DeterministicTopicIdsV1.deriveBindingId(cell, incarnation);
        var epochId = DeterministicTopicIdsV1.deriveStorageEpochId(bindingId, 0);
        CanonicalBytes catalog = CanonicalBytes.copyOf("catalog".getBytes(StandardCharsets.UTF_8));
        TopicBindingAggregateV1 aggregate = new TopicBindingAggregateV1(
                TopicBindingAggregateV1.SCHEMA_VERSION,
                new TopicBindingV1(ProtocolKindV1.PULSAR, bindingId, cell, incarnation),
                new InitialStorageEpochV1(
                        epochId,
                        0,
                        StorageProfileV1.BOOKKEEPER_WAL_ONLY,
                        ProfileOriginV1.TOPIC_EXPLICIT,
                        new PolicyCatalogDigest(Sha256Digest.hash(catalog)),
                        FrameEncodingPolicyValueV1.none()));
        CanonicalBytes bytes = Nta1CodecV1.encode(aggregate);
        return new AggregatePublicationCandidate(aggregate, bytes, Sha256Digest.hash(bytes));
    }

    private static final class TestScheduler implements RevalidationScheduler {
        private final AtomicBoolean closed = new AtomicBoolean();
        private final AtomicInteger requests = new AtomicInteger();

        @Override
        public boolean request(long clientGeneration, long invalidationEpoch) {
            if (closed.get()) {
                return false;
            }
            requests.incrementAndGet();
            return true;
        }

        int requests() {
            return requests.get();
        }

        @Override
        public void close() {
            closed.set(true);
        }
    }
}
