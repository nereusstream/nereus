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

package com.nereusstream.kafka.bookkeeper.broker;

import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.assertThatThrownBy;
import com.nereusstream.domain.bytes.CanonicalBytes;
import com.nereusstream.domain.bytes.Sha256Digest;
import com.nereusstream.domain.codec.TopicIncarnationIdentityCodecV1;
import com.nereusstream.domain.identity.Id128;
import com.nereusstream.domain.identity.KafkaTopicId;
import com.nereusstream.domain.identity.StorageEpochId;
import com.nereusstream.domain.identity.TopicBindingId;
import com.nereusstream.domain.protocol.KafkaTopicIncarnationIdentity;
import com.nereusstream.domain.protocol.KafkaTopicName;
import com.nereusstream.kafka.bookkeeper.nbke2.Nbke2IndexDirectoryEntryV1;
import com.nereusstream.kafka.bookkeeper.nbke2.Nbke2RunBindingV1;
import com.nereusstream.kafka.bookkeeper.read.KafkaBookKeeperReadRunV1;
import com.nereusstream.kafka.bookkeeper.read.KafkaPackedIndexDirectoryV1;
import com.nereusstream.storage.api.bookkeeper.BookKeeperLedgerIdentity;
import com.nereusstream.storage.api.bookkeeper.CellProviderScopeId;
import com.nereusstream.storage.api.bookkeeper.RunLedgerHandleV1;
import com.nereusstream.storage.api.bookkeeper.StorageRunId;
import com.nereusstream.storage.api.kafka.KafkaRunRootSnapshotV1;
import com.nereusstream.storage.api.kafka.KafkaRunRootStateV1;
import com.nereusstream.storage.object.control.CanonicalControlMetadataStore;
import com.nereusstream.storage.object.control.ControlMutationOutcome;
import com.nereusstream.storage.object.read.BindingReadAuthorityV1;
import com.nereusstream.storage.object.read.BindingReadHazardPoolV1.ScanOutcome;
import com.nereusstream.storage.object.read.BindingReadProtocolV1;
import com.nereusstream.storage.object.read.BindingReadPublicationCellV1;
import com.nereusstream.storage.object.read.BindingReadRouteTableV1;
import com.nereusstream.storage.object.read.control.M4ReadControlCodecV1;
import com.nereusstream.storage.object.read.control.M4ReadControlCoordinatorV1;
import com.nereusstream.storage.object.read.control.M4ReadControlRecordsV1.AdmissionState;
import com.nereusstream.storage.object.read.control.M4ReadControlRecordsV1.BindingIdentity;
import com.nereusstream.storage.object.read.control.M4ReadControlRecordsV1.BindingReadSelector;
import com.nereusstream.storage.object.read.control.M4ReadControlRecordsV1.CapabilityBinding;
import com.nereusstream.storage.object.read.control.M4ReadControlRecordsV1.CapabilityEvidence;
import com.nereusstream.storage.object.read.control.M4ReadControlRecordsV1.CapabilityKind;
import com.nereusstream.storage.object.read.control.M4ReadControlRecordsV1.CapabilityState;
import com.nereusstream.storage.object.read.control.M4ReadControlRecordsV1.SelectorMode;
import java.nio.charset.StandardCharsets;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.Optional;
import java.util.OptionalLong;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.Executors;
import java.util.concurrent.Semaphore;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.TimeoutException;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.params.ParameterizedTest;
import org.junit.jupiter.params.provider.ValueSource;

class KafkaBookKeeperM4BrokerReadOwnerV2Test {
    @Test
    void callerCancellationCannotClearTheNativeReadHazardBeforeProviderCompletion() throws Exception {
        var fixture = admissionFixture();
        var binding = fixture.binding();
        var coordinator = fixture.coordinator();
        var selector = fixture.selector();
        var authority = fixture.authority();
        var loop = Executors.newSingleThreadExecutor();
        try {
            var cellSlots = new Semaphore(3);
            var bindingSlots = new Semaphore(2);
            var owner = new KafkaBookKeeperM4BrokerReadOwnerV2(
                    coordinator, selector, authority, loop, 2, cellSlots, bindingSlots);
            var successor = new KafkaBookKeeperM4BrokerReadOwnerV2(
                    coordinator, selector, authority, loop, 2, cellSlots, bindingSlots);
            var started = new CountDownLatch(1);
            var provider = new CompletableFuture<String>();
            var observer = owner.read(captured -> {
                        assertThat(captured).isSameAs(authority);
                        started.countDown();
                        return provider;
                    })
                    .toCompletableFuture();
            assertThat(started.await(5, TimeUnit.SECONDS)).isTrue();
            assertThat(owner.hazards().scan(binding.bindingId(), 1)).isEqualTo(ScanOutcome.PINNED);

            var loopBlocked = new CountDownLatch(1);
            var releaseLoop = new CountDownLatch(1);
            loop.execute(() -> {
                loopBlocked.countDown();
                try {
                    releaseLoop.await();
                } catch (InterruptedException interrupted) {
                    Thread.currentThread().interrupt();
                }
            });
            assertThat(loopBlocked.await(5, TimeUnit.SECONDS)).isTrue();
            var secondProvider = new CompletableFuture<String>();
            var secondStarted = new CountDownLatch(1);
            var second = successor.read(captured -> {
                secondStarted.countDown();
                return secondProvider;
            });
            var rejectedSourceStarted = new java.util.concurrent.atomic.AtomicBoolean();
            var rejected = owner.read(captured -> {
                rejectedSourceStarted.set(true);
                return CompletableFuture.completedFuture("unexpected");
            });
            assertThatThrownBy(() -> rejected.toCompletableFuture().get(5, TimeUnit.SECONDS))
                    .hasRootCauseMessage("native M4 Binding read capacity is full");
            assertThat(rejectedSourceStarted).isFalse();
            releaseLoop.countDown();
            assertThat(secondStarted.await(5, TimeUnit.SECONDS)).isTrue();

            observer.cancel(true);
            var drained = owner.drain().toCompletableFuture();
            assertThat(drained).isNotDone();
            assertThatThrownBy(() -> owner.successorAfterDrain(selector, authority))
                    .hasMessage("native fallback actor has not confirmed its actual drain");
            assertThatThrownBy(() -> drained.get(25, TimeUnit.MILLISECONDS)).isInstanceOf(TimeoutException.class);
            assertThat(loop.isShutdown()).isFalse();
            assertThat(owner.hazards().scan(binding.bindingId(), 1)).isEqualTo(ScanOutcome.PINNED);
            provider.complete("native result");
            secondProvider.complete("second native result");
            assertThat(second.toCompletableFuture().get(5, TimeUnit.SECONDS)).isEqualTo("second native result");
            drained.get(5, TimeUnit.SECONDS);
            successor.drain().toCompletableFuture().get(5, TimeUnit.SECONDS);
            var resumed = owner.successorAfterDrain(selector, authority);
            assertThat(resumed.read(captured -> CompletableFuture.completedFuture("resumed native actor"))
                            .toCompletableFuture()
                            .get(5, TimeUnit.SECONDS))
                    .isEqualTo("resumed native actor");
            resumed.drain().toCompletableFuture().get(5, TimeUnit.SECONDS);
            assertThat(owner.hazards().scan(binding.bindingId(), 1)).isEqualTo(ScanOutcome.CLEAN);
            assertThat(cellSlots.availablePermits()).isEqualTo(3);
            assertThat(bindingSlots.availablePermits()).isEqualTo(2);
        } finally {
            loop.shutdownNow();
        }
    }

    @ParameterizedTest
    @ValueSource(strings = {"HELD", "UNKNOWN", "CANCEL"})
    void returnedFetchStateCannotReleaseCapacityOrHazardBeforeConfirmedNativeClose(String mode) throws Exception {
        var fixture = admissionFixture();
        var loop = Executors.newSingleThreadExecutor();
        try {
            var cellSlots = new Semaphore(1);
            var bindingSlots = new Semaphore(1);
            var owner = new KafkaBookKeeperM4BrokerReadOwnerV2(
                    fixture.coordinator(), fixture.selector(), fixture.authority(), loop, 1, cellSlots, bindingSlots);
            var result = new CompletableFuture<String>();
            var physicalDrain = new CompletableFuture<Void>();
            var unknown = new CompletableFuture<Void>();
            var started = new CountDownLatch(1);
            var observer = owner.readOwned(captured -> {
                        started.countDown();
                        return new KafkaBookKeeperM4BrokerReadOwnerV2.ReadLifetime<>(result, physicalDrain, unknown);
                    })
                    .toCompletableFuture();
            assertThat(started.await(5, TimeUnit.SECONDS)).isTrue();
            if (mode.equals("CANCEL")) {
                observer.cancel(true);
            }
            if (mode.equals("UNKNOWN")) {
                var failure = new TimeoutException("native historical close unknown");
                unknown.completeExceptionally(failure);
                result.completeExceptionally(failure);
                assertThatThrownBy(() -> observer.get(5, TimeUnit.SECONDS))
                        .hasRootCauseMessage("native historical close unknown");
                assertThatThrownBy(() -> owner.drain().toCompletableFuture().get(5, TimeUnit.SECONDS))
                        .hasRootCauseMessage("native historical close unknown");
                assertThatThrownBy(() -> owner.successorAfterDrain(fixture.selector(), fixture.authority()))
                        .hasMessage("native fallback actor has not confirmed its actual drain");
            } else {
                result.complete("native result");
                if (!mode.equals("CANCEL")) {
                    assertThat(observer.get(5, TimeUnit.SECONDS)).isEqualTo("native result");
                }
            }
            assertThat(owner.hazards().scan(fixture.binding().bindingId(), 1)).isEqualTo(ScanOutcome.PINNED);
            assertThat(cellSlots.availablePermits()).isZero();
            assertThat(bindingSlots.availablePermits()).isZero();
            var other = new KafkaBookKeeperM4BrokerReadOwnerV2(
                    fixture.coordinator(), fixture.selector(), fixture.authority(), loop, 1, cellSlots, bindingSlots);
            var dispatched = new java.util.concurrent.atomic.AtomicBoolean();
            assertThatThrownBy(() -> other.read(captured -> {
                                dispatched.set(true);
                                return CompletableFuture.completedFuture("unexpected");
                            })
                            .toCompletableFuture()
                            .get(5, TimeUnit.SECONDS))
                    .hasRootCauseMessage("native M4 Cell read capacity is full");
            assertThat(dispatched).isFalse();
            if (!mode.equals("UNKNOWN")) {
                var drained = owner.drain().toCompletableFuture();
                assertThat(drained).isNotDone();
                physicalDrain.complete(null);
                drained.get(5, TimeUnit.SECONDS);
                assertThat(owner.hazards().scan(fixture.binding().bindingId(), 1))
                        .isEqualTo(ScanOutcome.CLEAN);
                assertThat(cellSlots.availablePermits()).isEqualTo(1);
                assertThat(bindingSlots.availablePermits()).isEqualTo(1);
            }
        } finally {
            loop.shutdownNow();
        }
    }

    private record AdmissionFixture(
            BindingIdentity binding,
            M4ReadControlCoordinatorV1 coordinator,
            BindingReadSelector selector,
            BindingReadAuthorityV1 authority) {}

    private static AdmissionFixture admissionFixture() {
        BindingIdentity binding =
                new BindingIdentity(new TopicBindingId(digest("binding")), digest("incarnation"), digest("storage"));
        var coordinator = new M4ReadControlCoordinatorV1(new Store(), 3, binding);
        var evidence = new CapabilityEvidence(
                binding,
                1,
                1,
                CapabilityKind.DURABLE_DRAIN_ONLY_V1,
                CapabilityState.ADMITTED,
                digest("adapter"),
                digest("backend-config"),
                digest("read-contract"),
                digest("verifier"),
                digest("receipt-id"),
                digest("receipt"),
                digest("authority-time"),
                10_000,
                0,
                0);
        assertThat(coordinator.createCapability(evidence)).isEqualTo(M4ReadControlCoordinatorV1.Outcome.APPLIED);
        var capability = new CapabilityBinding(1, M4ReadControlCodecV1.capabilityEvidenceSha256(evidence));
        var view = digest("native-read-source-map");
        var selector = new BindingReadSelector(
                binding,
                view,
                1,
                1,
                1,
                SelectorMode.PREFERRED_ONLY,
                AdmissionState.ADMITTING,
                Optional.empty(),
                capability,
                List.of(),
                List.of());
        assertThat(coordinator.createSelector(selector)).isEqualTo(M4ReadControlCoordinatorV1.Outcome.APPLIED);
        var authority = new BindingReadAuthorityV1(
                binding.bindingId(),
                binding.incarnationSha256(),
                new StorageEpochId(binding.storageEpochSha256()),
                BindingReadProtocolV1.KAFKA_OFFSET,
                view,
                1,
                1,
                true,
                1,
                capability.evidenceSha256(),
                new BindingReadPublicationCellV1(1, 10, 0, new BindingReadRouteTableV1(List.of()), List.of()));
        return new AdmissionFixture(binding, coordinator, selector, authority);
    }

    @Test
    void rolloverPublishesExactSuccessorWhileOldNativeReadRemainsPinned() throws Exception {
        var topic =
                new KafkaTopicIncarnationIdentity(new KafkaTopicId(new Id128(1, 2)), new KafkaTopicName("rollover"));
        var binding = new BindingIdentity(
                new TopicBindingId(digest("binding-rollover")),
                Sha256Digest.hash(TopicIncarnationIdentityCodecV1.encode(topic)),
                digest("storage-rollover"));
        var storage = new StorageEpochId(binding.storageEpochSha256());
        var scope = new CellProviderScopeId(digest("scope-rollover"));
        var oldRun = new StorageRunId(new Id128(3, 4));
        var newRun = new StorageRunId(new Id128(5, 6));
        var oldActive = new KafkaRunRootSnapshotV1(
                binding.bindingId(),
                topic,
                3,
                storage,
                1,
                0,
                scope,
                oldRun,
                new BookKeeperLedgerIdentity(41),
                0,
                OptionalLong.empty(),
                KafkaRunRootStateV1.ACTIVE,
                Optional.empty());
        var oldSealed = new KafkaRunRootSnapshotV1(
                binding.bindingId(),
                topic,
                3,
                storage,
                1,
                0,
                scope,
                oldRun,
                new BookKeeperLedgerIdentity(41),
                0,
                OptionalLong.of(10),
                KafkaRunRootStateV1.SEALED,
                Optional.empty());
        var nextActive = new KafkaRunRootSnapshotV1(
                binding.bindingId(),
                topic,
                3,
                storage,
                1,
                0,
                scope,
                newRun,
                new BookKeeperLedgerIdentity(42),
                10,
                OptionalLong.empty(),
                KafkaRunRootStateV1.ACTIVE,
                Optional.of(oldRun));
        var config = digest("config-rollover");
        var initialSources = new KafkaBookKeeperPartitionV1.NativeReadSources(List.of(), oldActive, List.of(), config);
        var sealedRead = new KafkaBookKeeperReadRunV1(
                new Nbke2RunBindingV1(binding.bindingId(), topic, 3, storage, 1, 0, scope, oldRun),
                new RunLedgerHandleV1(scope, oldRun, oldSealed.ledgerIdentity(), config),
                0,
                10,
                0,
                Optional.empty(),
                Optional.of(new KafkaPackedIndexDirectoryV1(List.of(new Nbke2IndexDirectoryEntryV1(5, 0, 10)))));
        var successorSources = new KafkaBookKeeperPartitionV1.NativeReadSources(
                List.of(oldSealed), nextActive, List.of(sealedRead), config);
        var store = new Store();
        var coordinator = new M4ReadControlCoordinatorV1(store, 3, binding);
        var evidence = new CapabilityEvidence(
                binding,
                1,
                1,
                CapabilityKind.DURABLE_DRAIN_ONLY_V1,
                CapabilityState.ADMITTED,
                digest("adapter"),
                config,
                digest("read-contract"),
                digest("verifier"),
                digest("receipt-id"),
                digest("receipt"),
                digest("authority-time"),
                10_000,
                0,
                0);
        assertThat(coordinator.createCapability(evidence)).isEqualTo(M4ReadControlCoordinatorV1.Outcome.APPLIED);
        var capability = new CapabilityBinding(1, M4ReadControlCodecV1.capabilityEvidenceSha256(evidence));
        var firstView = initialSources.selectedViewSha256();
        var durable = new BindingReadSelector(
                binding,
                firstView,
                1,
                1,
                1,
                SelectorMode.PREFERRED_ONLY,
                AdmissionState.ADMITTING,
                Optional.empty(),
                capability,
                List.of(),
                List.of());
        assertThat(coordinator.createSelector(durable)).isEqualTo(M4ReadControlCoordinatorV1.Outcome.APPLIED);
        var authority = new BindingReadAuthorityV1(
                binding.bindingId(),
                binding.incarnationSha256(),
                storage,
                BindingReadProtocolV1.KAFKA_OFFSET,
                firstView,
                1,
                1,
                true,
                1,
                capability.evidenceSha256(),
                new BindingReadPublicationCellV1(1, 10, 0, new BindingReadRouteTableV1(List.of()), initialSources));
        var loop = Executors.newSingleThreadExecutor();
        try {
            var owner = new KafkaBookKeeperM4BrokerReadOwnerV2(coordinator, durable, authority, loop, 2);
            var started = new CountDownLatch(1);
            var oldProvider = new CompletableFuture<String>();
            var oldObserver = owner.read(captured -> {
                assertThat(captured.selectedViewSha256()).isEqualTo(firstView);
                started.countDown();
                return oldProvider;
            });
            assertThat(started.await(5, TimeUnit.SECONDS)).isTrue();

            var stopped = owner.stopNativeRolloverAdmission();
            store.blockNextCas();
            var publication =
                    CompletableFuture.runAsync(() -> owner.publishNativeRollover(stopped, successorSources, 10));
            assertThat(store.casEntered.await(5, TimeUnit.SECONDS)).isTrue();
            oldProvider.complete("old native result");
            assertThat(oldObserver.toCompletableFuture().get(5, TimeUnit.SECONDS))
                    .isEqualTo("old native result");
            assertThat(owner.hazards().scan(binding.bindingId(), 1)).isEqualTo(ScanOutcome.CLEAN);
            var startedAfterClose = new java.util.concurrent.atomic.AtomicBoolean();
            var denied = owner.read(captured -> {
                startedAfterClose.set(true);
                return CompletableFuture.completedFuture(captured.selectedViewSha256());
            });
            assertThatThrownBy(() -> denied.toCompletableFuture().get(5, TimeUnit.SECONDS))
                    .hasCauseInstanceOf(IllegalStateException.class);
            assertThat(startedAfterClose).isFalse();
            store.allowCas.countDown();
            publication.get(5, TimeUnit.SECONDS);
            var updated = coordinator.readSelector().orElseThrow();
            assertThat(updated.selectedViewSha256()).isEqualTo(successorSources.selectedViewSha256());
            assertThat(updated.sourceGeneration()).isEqualTo(2);
            assertThat(updated.readAdmissionEpoch()).isEqualTo(1);
            assertThat(owner.read(captured -> CompletableFuture.completedFuture(captured.selectedViewSha256()))
                            .toCompletableFuture()
                            .get(5, TimeUnit.SECONDS))
                    .isEqualTo(successorSources.selectedViewSha256());

            owner.drain().toCompletableFuture().get(5, TimeUnit.SECONDS);
            assertThat(owner.hazards().scan(binding.bindingId(), 1)).isEqualTo(ScanOutcome.CLEAN);
        } finally {
            store.allowCas.countDown();
            loop.shutdownNow();
        }
    }

    private static Sha256Digest digest(String value) {
        return Sha256Digest.hash(CanonicalBytes.copyOf(value.getBytes(StandardCharsets.US_ASCII)));
    }

    private static final class Store implements CanonicalControlMetadataStore {
        private final Map<String, CanonicalBytes> values = new HashMap<>();
        private final CountDownLatch casEntered = new CountDownLatch(1);
        private final CountDownLatch allowCas = new CountDownLatch(1);
        private boolean blockCas;

        void blockNextCas() {
            blockCas = true;
        }

        @Override
        public Optional<CanonicalBytes> get(String key) {
            return Optional.ofNullable(values.get(key));
        }

        @Override
        public ControlMutationOutcome putIfAbsent(String key, CanonicalBytes exactValue) {
            return values.putIfAbsent(key, exactValue) == null
                    ? ControlMutationOutcome.APPLIED
                    : ControlMutationOutcome.DEFINITIVE_CONFLICT;
        }

        @Override
        public ControlMutationOutcome compareAndSet(
                String key, Optional<CanonicalBytes> exactExpected, CanonicalBytes exactCandidate) {
            if (blockCas) {
                blockCas = false;
                casEntered.countDown();
                try {
                    if (!allowCas.await(5, TimeUnit.SECONDS)) {
                        throw new IllegalStateException("test selector CAS was not released");
                    }
                } catch (InterruptedException failure) {
                    Thread.currentThread().interrupt();
                    throw new IllegalStateException("test selector CAS was interrupted", failure);
                }
            }
            if (!get(key).equals(exactExpected)) {
                return ControlMutationOutcome.DEFINITIVE_CONFLICT;
            }
            values.put(key, exactCandidate);
            return ControlMutationOutcome.APPLIED;
        }
    }
}
