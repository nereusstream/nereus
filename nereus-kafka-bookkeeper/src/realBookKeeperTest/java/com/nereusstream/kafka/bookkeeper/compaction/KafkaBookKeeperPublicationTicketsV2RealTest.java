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

package com.nereusstream.kafka.bookkeeper.compaction;

import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.assertThatThrownBy;
import com.nereusstream.domain.bytes.CanonicalBytes;
import com.nereusstream.domain.bytes.Sha256Digest;
import com.nereusstream.domain.identity.Id128;
import com.nereusstream.kafka.bookkeeper.compaction.KafkaBookKeeperOxiaControlV2RealTest.NativeContext;
import com.nereusstream.metadata.oxia.v2.retention.Oxia09ExactMetadataTransactionStoreV1;
import com.nereusstream.metadata.oxia.v2.retention.OxiaPhysicalMetadataNamespaceV2;
import com.nereusstream.metadata.oxia.v2.retention.OxiaQuotaTargetDeleteStoreV2;
import com.nereusstream.metadata.spi.retention.ExactMetadataTransactionStoreV1;
import com.nereusstream.metadata.spi.retention.ExactMetadataTransactionStoreV1.MutationOutcome;
import com.nereusstream.metadata.spi.retention.ExactMetadataTransactionStoreV1.VersionedValue;
import com.nereusstream.storage.api.bookkeeper.RunLedgerAppendRequestV1;
import com.nereusstream.storage.api.bookkeeper.RunLedgerConfigurationV1;
import com.nereusstream.storage.api.bookkeeper.RunLedgerHandleV1;
import com.nereusstream.storage.api.bookkeeper.StorageRunId;
import com.nereusstream.storage.api.lifecycle.PhysicalNamespaceAuthorityBindingV2;
import com.nereusstream.storage.api.lifecycle.PhysicalResourceIdV2;
import com.nereusstream.storage.bookkeeper.ImmutableRetainedStoragePayload;
import com.nereusstream.storage.bookkeeper.M5BookKeeperNamespaceAuthorityV2;
import com.nereusstream.storage.bookkeeper.M5BookKeeperNativeCreateClientV2;
import com.nereusstream.storage.bookkeeper.M5BookKeeperNativeCreateSpecV2;
import com.nereusstream.storage.bookkeeper.RealBookKeeperCellSessionV1;
import com.nereusstream.storage.object.gc.M5TargetDeleteAuthorityCodecV1;
import com.nereusstream.storage.object.gc.M5TargetDeleteAuthorityCoordinatorV1;
import com.nereusstream.storage.object.gc.M5TargetDeleteMultiWriterGuardV2;
import com.nereusstream.storage.object.gc.SyntheticDeleteAuthorityFixturesV2;
import com.nereusstream.storage.object.materialization.M5MaterializationCodecV1;
import com.nereusstream.storage.object.materialization.M5MaterializationRecordsV1.PublicationOutcome;
import com.nereusstream.storage.object.retention.M5MaterializationWorkerCoordinatorV1;
import io.oxia.client.api.AsyncOxiaClient;
import io.oxia.client.api.OxiaClientBuilder;
import java.nio.file.Files;
import java.nio.file.Path;
import java.time.Duration;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.List;
import java.util.Map;
import java.util.Optional;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.CompletionStage;
import java.util.concurrent.TimeUnit;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.Timeout;

/** Actual source/output bytes and publication; logical source ownership and deletion eligibility remain fixtures. */
@Timeout(value = 5, unit = TimeUnit.MINUTES)
class KafkaBookKeeperPublicationTicketsV2RealTest {
    @Test
    void concurrentWinnerReconcilesOlderTicketsWhileTheirNativeSelectorCasIsStillHeld() throws Exception {
        try (var fixture = new Fixture(1408, null)) {
            var descriptor = fixture.write();
            var resources = fixture.targets(descriptor);
            fixture.admit(resources);
            fixture.context.faults.holdSelector = true;
            var older = fixture.publish(descriptor);
            var selectorKey = new com.nereusstream.storage.object.read.control.M4ReadControlKeysV1(
                            7, fixture.context.binding)
                    .selector();
            VersionedValue selected;
            try {
                fixture.context.faults.selectorHeld.get(30, TimeUnit.SECONDS);
                try (var other =
                        new NativeContext(fixture.context.task, fixture.context.root, fixture.output.newSession())) {
                    var namespace = await(
                            OxiaPhysicalMetadataNamespaceV2.connect(other.oxia, fixture.binding.metadataNamespace()));
                    var route = await(namespace.openAuthorityRoute(
                            fixture.backend, new Oxia09ExactMetadataTransactionStoreV1(other.oxia)));
                    var guard = new M5TargetDeleteMultiWriterGuardV2(new M5TargetDeleteAuthorityCoordinatorV1(route));
                    var winner = other.onOwner(() -> new KafkaBookKeeperCompactionPublicationV2(
                                    other.store,
                                    7,
                                    other.binding,
                                    other.reader(),
                                    other.owner,
                                    new KafkaBookKeeperPublicationTicketsV2(guard, fixture::membership, other.owner))
                            .publish(
                                    fixture.input.plan(),
                                    fixture.input.semantic(),
                                    descriptor,
                                    fixture.context.protections,
                                    () -> new KafkaCompactionPublicationFenceV1().expected(fixture.input.plan())));
                    assertThat(await(winner)).isEqualTo(PublicationOutcome.APPLIED_EXACT);
                    assertThat(older).isNotDone();
                    for (var resource : resources) {
                        assertThat(fixture.tickets(resource)).isZero();
                    }
                    assertThat(other.faults.selectorCas.get()).isEqualTo(1);
                    selected = await(other.route.read(selectorKey)).orElseThrow();
                }
            } finally {
                fixture.releaseSelector();
            }
            assertThat(older.get(30, TimeUnit.SECONDS))
                    .isIn(PublicationOutcome.APPLIED_EXACT, PublicationOutcome.EXISTING_EXACT);
            assertThat(await(fixture.context.route.read(selectorKey))).contains(selected);
            for (var resource : resources) {
                assertThat(fixture.tickets(resource)).isZero();
            }
            assertThat(fixture.context.recover().descriptor()).isEqualTo(descriptor);
        }
    }

    @Test
    void everyInputAndOutputTicketPrecedesHeldNativePublicationAndDefeatsConcurrentGcFence() throws Exception {
        try (var fixture = new Fixture(1401, null)) {
            var descriptor = fixture.write();
            var resources = fixture.targets(descriptor);
            var original = fixture.admit(resources);
            fixture.context.faults.holdSelector = true;
            var publication = fixture.publish(descriptor);
            try {
                fixture.context.faults.selectorHeld.get(30, TimeUnit.SECONDS);
                for (int i = 0; i < resources.size(); i++) {
                    assertThat(fixture.tickets(resources.get(i))).isEqualTo(1);
                    var fenced = SyntheticDeleteAuthorityFixturesV2.phases(resources.get(i))
                            .get(1);
                    assertThat(await(fixture.route.compareAndSet(
                                    Optional.of(original.get(i)),
                                    resources.get(i).authorityKey(),
                                    M5TargetDeleteAuthorityCodecV1.encodeAuthority(fenced))))
                            .isEqualTo(MutationOutcome.DEFINITIVE_CONFLICT);
                }
                assertThat(publication).isNotDone();
            } finally {
                fixture.releaseSelector();
            }
            assertThat(publication.get(30, TimeUnit.SECONDS)).isEqualTo(PublicationOutcome.APPLIED_EXACT);
            for (var resource : resources) {
                assertThat(fixture.tickets(resource)).isZero();
            }
            assertThat(fixture.context.recover().descriptor()).isEqualTo(descriptor);
        }
    }

    @Test
    void fencedLaterTargetRollsBackEarlierTicketsWithoutCreatingPublicationRecords() throws Exception {
        try (var fixture = new Fixture(1402, null)) {
            var descriptor = fixture.write();
            var resources = fixture.targets(descriptor);
            var original = fixture.admit(resources);
            int last = resources.size() - 1;
            assertThat(await(fixture.route.compareAndSet(
                            Optional.of(original.get(last)),
                            resources.get(last).authorityKey(),
                            M5TargetDeleteAuthorityCodecV1.encodeAuthority(
                                    SyntheticDeleteAuthorityFixturesV2.phases(resources.get(last))
                                            .get(1)))))
                    .isEqualTo(MutationOutcome.APPLIED_EXACT);
            int prewrites = fixture.context.faults.recordCreates.get();
            assertThat(fixture.publish(descriptor).get(30, TimeUnit.SECONDS)).isEqualTo(PublicationOutcome.CONFLICT);
            assertThat(fixture.context.faults.selectorCas.get()).isZero();
            // The original membership is durable before ticket acquisition, but no descriptor/candidate was published.
            assertThat(fixture.context.faults.recordCreates.get()).isEqualTo(prewrites + 1);
            assertThat(fixture.context.onOwner(() -> fixture.context.store.get(
                            KafkaBookKeeperCompactionPublicationV2.descriptorKey(descriptor.descriptorSha256()))))
                    .isEmpty();
            for (var resource : resources) {
                assertThat(fixture.tickets(resource)).isZero();
            }
        }
    }

    @Test
    void nativeSelectorResponseLossReconcilesExactSelectionBeforeRemovingTickets() throws Exception {
        try (var fixture = new Fixture(1403, null)) {
            var descriptor = fixture.write();
            var resources = fixture.targets(descriptor);
            fixture.admit(resources);
            fixture.context.faults.loseSelector = true;
            assertThat(fixture.publish(descriptor).get(30, TimeUnit.SECONDS))
                    .isIn(PublicationOutcome.APPLIED_EXACT, PublicationOutcome.EXISTING_EXACT);
            for (var resource : resources) {
                assertThat(fixture.tickets(resource)).isZero();
            }
            assertThat(fixture.context.recover().descriptor()).isEqualTo(descriptor);
        }
    }

    @Test
    void staleProtocolLeavesTicketsUntilNativeTaskCancellationProvesNonPublication() throws Exception {
        try (var fixture = new Fixture(1404, null)) {
            var descriptor = fixture.write();
            var resources = fixture.targets(descriptor);
            fixture.admit(resources);
            var expected = new KafkaCompactionPublicationFenceV1().expected(fixture.input.plan());
            var stale = new KafkaCompactionPublicationFenceV1.Snapshot(
                    expected.compactionPlanRootSha256(),
                    expected.protocolStateRootSha256(),
                    expected.policyGeneration() + 1,
                    expected.frontiers());
            var result = fixture.context.onOwner(() -> fixture.publisher()
                    .publish(
                            fixture.input.plan(),
                            fixture.input.semantic(),
                            descriptor,
                            fixture.context.protections,
                            () -> stale));
            assertThat(await(result)).isEqualTo(PublicationOutcome.OUTCOME_UNKNOWN);
            for (var resource : resources) {
                assertThat(fixture.tickets(resource)).isEqualTo(1);
            }
            var terminal = fixture.context.onOwner(() -> new KafkaBookKeeperTaskTerminationV2(
                            fixture.context.store, 7, fixture.context.task, fixture.output, fixture.context.owner)
                    .terminate());
            assertThat(await(terminal).outcome())
                    .isEqualTo(KafkaBookKeeperTaskTerminationV2.Outcome.TERMINATED_UNPUBLISHED);
            assertThat(fixture.publish(descriptor).get(30, TimeUnit.SECONDS))
                    .isEqualTo(PublicationOutcome.CANCELLED_STALE);
            for (var resource : resources) {
                assertThat(fixture.tickets(resource)).isZero();
            }
        }
    }

    @Test
    void cancellingObserverDoesNotReleaseTicketsBeforeHeldNativeSelectionCompletes() throws Exception {
        try (var fixture = new Fixture(1405, null)) {
            var descriptor = fixture.write();
            var resources = fixture.targets(descriptor);
            fixture.admit(resources);
            fixture.context.faults.holdSelector = true;
            var publication = fixture.publish(descriptor);
            try {
                fixture.context.faults.selectorHeld.get(30, TimeUnit.SECONDS);
                assertThat(publication.cancel(false)).isTrue();
                for (var resource : resources) {
                    assertThat(fixture.tickets(resource)).isEqualTo(1);
                }
            } finally {
                fixture.releaseSelector();
            }
            long deadline = System.nanoTime() + TimeUnit.SECONDS.toNanos(15);
            while (fixture.tickets(resources.get(resources.size() - 1)) != 0 && System.nanoTime() < deadline) {
                Thread.sleep(10);
            }
            for (var resource : resources) {
                assertThat(fixture.tickets(resource)).isZero();
            }
            assertThat(fixture.context.recover().descriptor()).isEqualTo(descriptor);
        }
    }

    @Test
    void incompleteInputMembershipCannotInvokeNativePublication() throws Exception {
        try (var fixture = new Fixture(1406, null)) {
            var descriptor = fixture.write();
            var resources = fixture.targets(descriptor);
            fixture.admit(resources);
            var different = new ArrayList<>(resources);
            different.add(new PhysicalResourceIdV2.BookKeeperLedger(fixture.backend.namespace(), Long.MAX_VALUE));
            assertThat(KafkaBookKeeperPublicationTicketsV2.context(descriptor, resources))
                    .isNotEqualTo(KafkaBookKeeperPublicationTicketsV2.context(descriptor, different));
            assertThatThrownBy(() -> KafkaBookKeeperPublicationTicketsV2.targets(
                            descriptor,
                            Map.of(
                                    descriptor.sourceCut().sources().get(0).sourceIdentitySha256(),
                                    java.util.Collections.nCopies(2048, resources.get(0)))))
                    .hasMessage("publication physical target count exceeds the bound");
            int prewrites = fixture.context.faults.recordCreates.get();
            var missing = new KafkaBookKeeperPublicationTicketsV2(
                    fixture.guard, cut -> CompletableFuture.completedFuture(Map.of()), fixture.context.owner);
            assertThatThrownBy(() -> await(fixture.context.onOwner(() -> new KafkaBookKeeperCompactionPublicationV2(
                                    fixture.context.store,
                                    7,
                                    fixture.context.binding,
                                    fixture.context.reader(),
                                    fixture.context.owner,
                                    missing)
                            .publish(
                                    fixture.input.plan(),
                                    fixture.input.semantic(),
                                    descriptor,
                                    fixture.context.protections,
                                    () -> new KafkaCompactionPublicationFenceV1().expected(fixture.input.plan())))))
                    .hasRootCauseMessage("publication input membership is missing or has foreign logical sources");
            assertThat(fixture.context.faults.selectorCas.get()).isZero();
            assertThat(fixture.context.faults.recordCreates.get()).isEqualTo(prewrites);
        }
    }

    @Test
    void staleSelectedWorkerRetriesOriginalTerminalTicketsWithoutReopeningExpiredInputs() throws Exception {
        try (var fixture = new Fixture(1409, null, true)) {
            var descriptor = fixture.write();
            var resources = fixture.targets(descriptor);
            fixture.admit(resources);
            var worker = fixture.worker(descriptor);
            var faults = new CleanupFaults(fixture.route);
            var guard = new M5TargetDeleteMultiWriterGuardV2(new M5TargetDeleteAuthorityCoordinatorV1(faults));
            faults.rejectRemoval = true;
            assertThat(fixture.publishWorker(descriptor, worker, guard, fixture::membership, false))
                    .isEqualTo(PublicationOutcome.OUTCOME_UNKNOWN);
            var comparisonKey = KafkaBookKeeperReplacementSemanticsV2.key(descriptor.descriptorSha256());
            var comparisonFact = fixture.controlFact(comparisonKey);
            var comparison = KafkaBookKeeperReplacementSemanticsV2.decode(comparisonFact.canonicalStoredBytes());
            comparison.requireDescriptor(descriptor);
            var originalMembership =
                    fixture.controlFact(KafkaBookKeeperPublicationTargetsV2.key(descriptor.descriptorSha256()));
            assertThat(comparison.publicationTargetsSha256()).isEqualTo(originalMembership.canonicalStoredSha256());
            assertThat(comparison.transfers(
                            descriptor,
                            new com.nereusstream.storage.object.retention.M5RetentionRecordsV1.AuthorityFactV1(
                                    comparisonFact.key(),
                                    comparisonFact.metadataVersion(),
                                    comparisonFact.canonicalStoredSha256())))
                    .hasSize(com.nereusstream.storage.object.gc.DeleteEligibilitySnapshotV2.SemanticAspect.values()
                            .length);
            assertThat(fixture.context.onOwner(worker::isCurrent)).isFalse();
            for (var resource : resources) {
                assertThat(fixture.tickets(resource)).isEqualTo(1);
            }
            var selectorKey = new com.nereusstream.storage.object.read.control.M4ReadControlKeysV1(
                            7, fixture.context.binding)
                    .selector();
            var selectedAuthority = await(fixture.context.route.read(selectorKey));
            var original = KafkaBookKeeperPublicationTicketsV2.context(descriptor, resources);
            var siblingContext = new M5TargetDeleteMultiWriterGuardV2.Context(
                    original.writerClass(),
                    original.capabilitySha256(),
                    original.ownerFenceSha256(),
                    KafkaBookKeeperCompactionTestSupportV2.digest("independent descriptor obligations"));
            var held = new CompletableFuture<M5TargetDeleteMultiWriterGuardV2.Completion<String>>();
            var started = new CompletableFuture<Void>();
            var sibling = fixture.guard.execute(resources, siblingContext, () -> {
                started.complete(null);
                return held;
            });
            await(started);
            var resolutions = new java.util.concurrent.atomic.AtomicInteger();
            KafkaBookKeeperPublicationTicketsV2.InputMembership expired = plan -> {
                resolutions.incrementAndGet();
                return CompletableFuture.failedFuture(new IllegalStateException("native input admission expired"));
            };
            try {
                assertThat(fixture.publishWorker(descriptor, worker, guard, expired, false))
                        .isEqualTo(PublicationOutcome.OUTCOME_UNKNOWN);
                for (var resource : resources) {
                    assertThat(fixture.tickets(resource)).isEqualTo(2);
                }
                faults.rejectRemoval = false;
                assertThat(fixture.publishWorker(descriptor, worker, guard, expired, false))
                        .isEqualTo(PublicationOutcome.EXISTING_EXACT);
                for (var resource : resources) {
                    assertThat(fixture.tickets(resource)).isEqualTo(1);
                }
                assertThat(resolutions.get()).isZero();
                assertThat(fixture.controlFact(comparisonKey)).isEqualTo(comparisonFact);
                assertThat(sibling.toCompletableFuture()).isNotDone();
                assertThat(await(fixture.context.route.read(selectorKey))).isEqualTo(selectedAuthority);
            } finally {
                faults.rejectRemoval = false;
                held.complete(new M5TargetDeleteMultiWriterGuardV2.Completion<>(
                        "finished",
                        Optional.of(KafkaBookKeeperCompactionTestSupportV2.digest("independent terminal"))));
                await(sibling);
            }
            for (var resource : resources) {
                assertThat(fixture.tickets(resource)).isZero();
            }
        }
    }

    @Test
    void replacementWorkerWithoutTerminalRetainsTicketsUntilNativeTaskCancellation() throws Exception {
        try (var fixture = new Fixture(1410, null, true)) {
            var descriptor = fixture.write();
            var resources = fixture.targets(descriptor);
            fixture.admit(resources);
            var worker = fixture.worker(descriptor);
            assertThat(fixture.publishWorker(descriptor, worker, fixture.guard, fixture::membership, true))
                    .isEqualTo(PublicationOutcome.OUTCOME_UNKNOWN);
            fixture.context.onOwner(() -> {
                var cut = fixture.input.plan().sourceCut();
                var workers =
                        new M5MaterializationWorkerCoordinatorV1(fixture.context.store, 7, fixture.context.binding);
                var replacement = workers.propose(
                        cut.predecessorSelector(),
                        cut.identity().protocolCellSha256(),
                        cut.identity().providerScopeSha256(),
                        cut.identity().storageFence(),
                        KafkaBookKeeperCompactionTestSupportV2.digest("synthetic replacement native owner"),
                        KafkaBookKeeperCompactionTestSupportV2.digest("synthetic worker namespace"));
                assertThat(replacement
                                .orElseThrow()
                                .admit(
                                        M5MaterializationCodecV1.sourceCutSha256(cut),
                                        KafkaBookKeeperCompactionTestSupportV2.digest("synthetic replacement plan"),
                                        KafkaBookKeeperCompactionTestSupportV2.digest("synthetic replacement task")))
                        .isPresent();
                return null;
            });
            var selectorKey = new com.nereusstream.storage.object.read.control.M4ReadControlKeysV1(
                            7, fixture.context.binding)
                    .selector();
            var replacementAuthority = await(fixture.context.route.read(selectorKey));
            var resolutions = new java.util.concurrent.atomic.AtomicInteger();
            KafkaBookKeeperPublicationTicketsV2.InputMembership expired = plan -> {
                resolutions.incrementAndGet();
                return CompletableFuture.failedFuture(new IllegalStateException("native input admission expired"));
            };
            assertThat(fixture.publishWorker(descriptor, worker, fixture.guard, expired, false))
                    .isEqualTo(PublicationOutcome.OUTCOME_UNKNOWN);
            for (var resource : resources) {
                assertThat(fixture.tickets(resource)).isEqualTo(1);
            }
            assertThat(await(fixture.context.route.read(selectorKey))).isEqualTo(replacementAuthority);
            var terminal = fixture.context.onOwner(() -> new KafkaBookKeeperTaskTerminationV2(
                            fixture.context.store, 7, fixture.context.task, fixture.output, fixture.context.owner)
                    .terminate());
            assertThat(await(terminal).outcome())
                    .isEqualTo(KafkaBookKeeperTaskTerminationV2.Outcome.TERMINATED_UNPUBLISHED);
            assertThat(fixture.publishWorker(descriptor, worker, fixture.guard, expired, false))
                    .isEqualTo(PublicationOutcome.CANCELLED_STALE);
            assertThat(resolutions.get()).isZero();
            for (var resource : resources) {
                assertThat(fixture.tickets(resource)).isZero();
            }
        }
    }

    @Test
    void writeBeforeServerRestart() throws Exception {
        try (var fixture = new Fixture(1407, null)) {
            var descriptor = fixture.write();
            var resources = fixture.targets(descriptor);
            fixture.admit(resources);
            fixture.context.faults.rejectReadsAfterSelector = true;
            assertThat(fixture.publish(descriptor).get(30, TimeUnit.SECONDS))
                    .isEqualTo(PublicationOutcome.OUTCOME_UNKNOWN);
            var lines = new ArrayList<>(List.of(
                    fixture.context.root,
                    fixture.sourceSpec.encode().toHex(),
                    Long.toString(fixture.sourceHandle.ledgerIdentity().ledgerId()),
                    descriptor.descriptorSha256().toHex()));
            for (var resource : resources) {
                assertThat(fixture.tickets(resource)).isEqualTo(1);
                var value = await(fixture.route.read(resource.authorityKey())).orElseThrow();
                lines.add(resource.sha256().toHex() + ":"
                        + value.canonicalStoredSha256().toHex() + ":"
                        + value.metadataVersion().value().toHex());
            }
            Files.write(checkpoint(), lines);
        }
    }

    @Test
    void readAfterServerRestart() throws Exception {
        var lines = Files.readAllLines(checkpoint());
        try (var fixture = new Fixture(1407, lines)) {
            var descriptor = fixture.context.onOwner(() -> KafkaSealedBookKeeperDescriptorCodecV2.decode(fixture.context
                    .store
                    .get(KafkaBookKeeperCompactionPublicationV2.descriptorKey(
                            Sha256Digest.copyOf(java.util.HexFormat.of().parseHex(lines.get(3)))))
                    .orElseThrow()));
            var resources = fixture.targets(descriptor);
            assertThat(lines).hasSize(4 + resources.size());
            for (int i = 0; i < resources.size(); i++) {
                var resource = resources.get(i);
                var value = await(fixture.route.read(resource.authorityKey())).orElseThrow();
                assertThat(resource.sha256().toHex() + ":"
                                + value.canonicalStoredSha256().toHex() + ":"
                                + value.metadataVersion().value().toHex())
                        .isEqualTo(lines.get(i + 4));
                assertThat(fixture.tickets(resource)).isEqualTo(1);
            }
            assertThat(fixture.publish(descriptor).get(30, TimeUnit.SECONDS))
                    .isEqualTo(PublicationOutcome.EXISTING_EXACT);
            for (var resource : resources) {
                assertThat(fixture.tickets(resource)).isZero();
            }
            assertThat(fixture.context.faults.selectorCas.get()).isZero();
            assertThat(fixture.context.faults.recordCreates.get()).isZero();
            assertThat(fixture.context.recover().descriptor()).isEqualTo(descriptor);
        }
    }

    private static Path checkpoint() {
        return Path.of(System.getProperty("nereus.m5.publication.restartCheckpoint"));
    }

    private static <T> T await(CompletionStage<T> stage) throws Exception {
        return stage.toCompletableFuture().get(30, TimeUnit.SECONDS);
    }

    private static final class CleanupFaults implements ExactMetadataTransactionStoreV1 {
        final ExactMetadataTransactionStoreV1 delegate;
        volatile boolean rejectRemoval;

        CleanupFaults(ExactMetadataTransactionStoreV1 delegate) {
            this.delegate = delegate;
        }

        public CompletionStage<Optional<VersionedValue>> read(String key) {
            return delegate.read(key);
        }

        public CompletionStage<MutationOutcome> compareAndSet(
                Optional<VersionedValue> predecessor, String key, CanonicalBytes candidate) {
            if (rejectRemoval
                    && predecessor.isPresent()
                    && M5TargetDeleteAuthorityCodecV1.decodeAuthority(candidate)
                                    .activeWriterTickets()
                                    .size()
                            < M5TargetDeleteAuthorityCodecV1.decodeAuthority(
                                            predecessor.orElseThrow().canonicalStoredBytes())
                                    .activeWriterTickets()
                                    .size()) {
                return CompletableFuture.completedFuture(MutationOutcome.RESPONSE_UNKNOWN);
            }
            return delegate.compareAndSet(predecessor, key, candidate);
        }

        public CompletionStage<TransactionOutcome> conditionalTransaction(ExactTransaction transaction) {
            return delegate.conditionalTransaction(transaction);
        }

        public boolean supportsAtomicMultiKeyTransactions() {
            return delegate.supportsAtomicMultiKeyTransactions();
        }
    }

    private static final class Fixture implements AutoCloseable {
        final KafkaBookKeeperCompactionTestSupportV2.Input input;
        final Optional<KafkaCompactionRecoveryStateV1> recoveryState;
        final AsyncOxiaClient oxia;
        final M5BookKeeperNamespaceAuthorityV2 backend;
        final PhysicalNamespaceAuthorityBindingV2 binding;
        final M5BookKeeperNativeCreateClientV2 output;
        final M5BookKeeperNativeCreateClientV2 source;
        final M5BookKeeperNativeCreateSpecV2 sourceSpec;
        final RealBookKeeperCellSessionV1 sourceSession;
        final RunLedgerHandleV1 sourceHandle;
        final NativeContext context;
        final OxiaQuotaTargetDeleteStoreV2 route;
        final M5TargetDeleteMultiWriterGuardV2 guard;

        Fixture(long attempt, List<String> restore) throws Exception {
            this(attempt, restore, false);
        }

        Fixture(long attempt, List<String> restore, boolean worker) throws Exception {
            recoveryState = worker
                    ? Optional.of(KafkaBookKeeperCompactionTestSupportV2.recoveryState(false))
                    : Optional.empty();
            if (worker) {
                var synthetic =
                        KafkaBookKeeperCompactionTestSupportV2.inputWithRecovery(recoveryState.orElseThrow(), attempt);
                var capability = synthetic.layout().task().capability();
                var instance = M5BookKeeperNativeCreateClientV2.discoverInstanceId(
                        System.getProperty("nereus.bookkeeper.metadataServiceUri"), capability);
                var layout = KafkaBookKeeperCompactionLayoutV2.plan(
                        synthetic.plan(),
                        synthetic.semantic(),
                        recoveryState.orElseThrow(),
                        M5BookKeeperNativeCreateSpecV2.namespace(instance),
                        capability,
                        attempt,
                        64 * 1024,
                        256 * 1024);
                input = new KafkaBookKeeperCompactionTestSupportV2.Input(
                        synthetic.plan(), synthetic.semantic(), layout, synthetic.capabilityEvidence());
            } else {
                input = KafkaBookKeeperNativeCreateV2RealTest.nativeInput(false, attempt);
            }
            var task = input.layout().task();
            String uri = System.getProperty("nereus.bookkeeper.metadataServiceUri");
            oxia = OxiaClientBuilder.create(System.getProperty("nereus.m5.oxia.serviceAddress"))
                    .namespace("default")
                    .requestTimeout(Duration.ofSeconds(10))
                    .asyncClient()
                    .get(30, TimeUnit.SECONDS);
            backend = M5BookKeeperNamespaceAuthorityV2.connect(uri, task.capability());
            var namespace = restore == null
                    ? await(OxiaPhysicalMetadataNamespaceV2.provision(oxia))
                    : await(OxiaPhysicalMetadataNamespaceV2.connect(
                            oxia, await(backend.readBinding()).orElseThrow().metadataNamespace()));
            binding = await(namespace.bind(backend));
            if (restore == null) {
                await(backend.nativeDeleteQuota()
                        .initialize(backend.nativeDeleteQuota().capacityForResources(2)));
                await(backend.nativeDeleteCellBudget()
                        .initialize(
                                new com.nereusstream.storage.bookkeeper.M5BookKeeperDeleteCellBudgetV2.Limits(2, 2)));
            } else {
                await(backend.nativeDeleteQuota().snapshot());
                await(backend.nativeDeleteCellBudget().snapshot());
            }
            output = M5BookKeeperNativeCreateClientV2.connect(
                    uri, task.capability(), KafkaBookKeeperNativeCreateV2RealTest.spec(input), binding);
            String root = restore == null ? NativeContext.root() : restore.get(0);
            context = new NativeContext(task, root, output.newSession());
            if (restore != null) {
                var cut = input.plan().sourceCut();
                context.protections = context.onOwner(() -> cut.sources().stream()
                        .map(member -> {
                            var identity = new com.nereusstream.storage.object.read.control.M4ReadControlRecordsV1
                                    .SourceProtectionIdentity(
                                    member.sourceIdentitySha256(),
                                    1,
                                    Math.addExact(cut.predecessorSelector().readAdmissionEpoch(), 1),
                                    cut.predecessorSelector().sourceGeneration(),
                                    cut.identity().capability());
                            var key = new com.nereusstream.storage.object.read.control.M4ReadControlKeysV1(
                                            7, context.binding)
                                    .protection(identity.sourceIdentitySha256(), identity.protectionGeneration());
                            var nativeProtection =
                                    com.nereusstream.storage.object.read.control.M4ReadControlCodecV1.decodeProtection(
                                            context.store.get(key).orElseThrow());
                            assertThat(nativeProtection.identity()).isEqualTo(identity);
                            return identity;
                        })
                        .sorted(java.util.Comparator.comparing(
                                value -> value.sourceIdentitySha256().toHex()))
                        .toList());
            }
            var sourceId = SyntheticDeleteAuthorityFixturesV2.digest("native publication input " + root);
            sourceSpec = restore == null
                    ? M5BookKeeperNativeCreateSpecV2.of(
                            output.spec().nativeInstanceId(),
                            sourceId,
                            List.of(RunLedgerConfigurationV1.from(
                                    task.capability(),
                                    new StorageRunId(Id128.fromBytes(
                                            Arrays.copyOf(sourceId.bytes().toByteArray(), 16))))))
                    : M5BookKeeperNativeCreateSpecV2.decode(
                            CanonicalBytes.copyOf(java.util.HexFormat.of().parseHex(restore.get(1))));
            source = M5BookKeeperNativeCreateClientV2.connect(uri, task.capability(), sourceSpec, binding);
            sourceSession = source.newSession();
            if (restore == null) {
                var id = await(sourceSession.reserveLedgerIdentity())
                        .exactProof()
                        .orElseThrow();
                sourceHandle = await(sourceSession.createReservedRunLedger(
                                sourceSpec.configurations().get(0), id))
                        .exactProof()
                        .orElseThrow();
                var bytes = new java.io.ByteArrayOutputStream();
                input.plan()
                        .inputBatches()
                        .forEach(batch -> bytes.writeBytes(batch.canonicalBody().toByteArray()));
                var payload = ImmutableRetainedStoragePayload.copyOf(bytes.toByteArray());
                assertThat(await(sourceSession.appendExplicitEntry(
                                        new RunLedgerAppendRequestV1(sourceHandle, 0, payload)))
                                .exactProof())
                        .isPresent();
                assertThat(payload.release()).isTrue();
                assertThat(await(sourceSession.closeRunLedger(sourceHandle)).exactProof())
                        .isPresent();
            } else {
                var configuration = sourceSpec.configurations().get(0);
                sourceHandle = new RunLedgerHandleV1(
                        configuration.providerScopeId(),
                        configuration.runId(),
                        new com.nereusstream.storage.api.bookkeeper.BookKeeperLedgerIdentity(
                                Long.parseLong(restore.get(2))),
                        configuration.configurationDigest());
            }
            route = await(namespace.openAuthorityRoute(backend, new Oxia09ExactMetadataTransactionStoreV1(oxia)));
            var quota = await(route.initialize(250_000_000));
            if (quota.head().capacityBytes() < 250_000_000) {
                assertThat(await(route.quota().expand(250_000_000))).isTrue();
            }
            guard = new M5TargetDeleteMultiWriterGuardV2(new M5TargetDeleteAuthorityCoordinatorV1(route));
        }

        KafkaSealedBookKeeperDescriptorV2 write() throws Exception {
            return context.write(input, recoveryState);
        }

        M5MaterializationWorkerCoordinatorV1.Admission worker(KafkaSealedBookKeeperDescriptorV2 descriptor)
                throws Exception {
            return context.onOwner(() -> {
                var cut = input.plan().sourceCut();
                var workers = new M5MaterializationWorkerCoordinatorV1(context.store, 7, context.binding);
                assertThat(workers.migrateLegacyAuthority()).isTrue();
                return workers.propose(
                                cut.predecessorSelector(),
                                cut.identity().protocolCellSha256(),
                                cut.identity().providerScopeSha256(),
                                cut.identity().storageFence(),
                                KafkaBookKeeperCompactionTestSupportV2.digest("synthetic worker owner"),
                                KafkaBookKeeperCompactionTestSupportV2.digest("synthetic worker namespace"))
                        .orElseThrow()
                        .admit(
                                M5MaterializationCodecV1.sourceCutSha256(cut),
                                KafkaCompactionCanonicalV1.planRoot(input.plan()),
                                descriptor.task().taskIdSha256())
                        .orElseThrow();
            });
        }

        PublicationOutcome publishWorker(
                KafkaSealedBookKeeperDescriptorV2 descriptor,
                M5MaterializationWorkerCoordinatorV1.Admission worker,
                M5TargetDeleteMultiWriterGuardV2 writerGuard,
                KafkaBookKeeperPublicationTicketsV2.InputMembership membership,
                boolean stalePolicy)
                throws Exception {
            var expected = new KafkaCompactionPublicationFenceV1().expected(input.plan());
            var current = stalePolicy
                    ? new KafkaCompactionPublicationFenceV1.Snapshot(
                            expected.compactionPlanRootSha256(),
                            expected.protocolStateRootSha256(),
                            expected.policyGeneration() + 1,
                            expected.frontiers())
                    : expected;
            return await(context.onOwner(() -> new KafkaBookKeeperCompactionPublicationV2(
                            context.store,
                            7,
                            context.binding,
                            context.reader(),
                            context.owner,
                            new KafkaBookKeeperPublicationTicketsV2(writerGuard, membership, context.owner))
                    .publish(
                            input.plan(),
                            input.semantic(),
                            descriptor,
                            context.protections,
                            () -> current,
                            recoveryState.orElseThrow(),
                            worker)));
        }

        CompletionStage<Map<Sha256Digest, List<PhysicalResourceIdV2>>> membership(
                KafkaCompactionRecordsV1.CompactionPlan plan) {
            assertThat(plan).isEqualTo(input.plan());
            var cut = plan.sourceCut();
            assertThat(cut.sources()).hasSize(1);
            return source.captureExactTarget(sourceHandle)
                    .thenCompose(seal -> {
                        assertThat(seal.exactTarget()).isPresent();
                        return sourceSession.openRunLedger(sourceHandle);
                    })
                    .thenCompose(open -> {
                        assertThat(open.exactHandle()).contains(sourceHandle);
                        return sourceSession.readExactEntry(sourceHandle, 0);
                    })
                    .thenApply(read -> {
                        var body = read.exactEntry().orElseThrow().payload();
                        assertThat(Sha256Digest.hash(body))
                                .isEqualTo(cut.sources().get(0).bodySha256());
                        assertThat(body.length()).isEqualTo(cut.sources().get(0).canonicalLength());
                        PhysicalResourceIdV2 resource = new PhysicalResourceIdV2.BookKeeperLedger(
                                backend.namespace(),
                                sourceHandle.ledgerIdentity().ledgerId());
                        return Map.of(cut.sources().get(0).sourceIdentitySha256(), List.of(resource));
                    });
        }

        VersionedValue controlFact(String key) throws Exception {
            var stored = context.oxia.get(context.records.nativeKey(key)).get(30, TimeUnit.SECONDS);
            var bytes = CanonicalBytes.copyOf(stored.value());
            return new VersionedValue(
                    key,
                    bytes,
                    Sha256Digest.hash(bytes),
                    new com.nereusstream.metadata.spi.model.MetadataVersion(
                            CanonicalBytes.copyOf(java.nio.ByteBuffer.allocate(Long.BYTES)
                                    .putLong(stored.version().versionId())
                                    .array())));
        }

        List<PhysicalResourceIdV2> targets(KafkaSealedBookKeeperDescriptorV2 descriptor) throws Exception {
            assertThat(descriptor.sourceCut()).isEqualTo(input.plan().sourceCut());
            return KafkaBookKeeperPublicationTicketsV2.targets(descriptor, await(membership(input.plan())));
        }

        List<VersionedValue> admit(List<PhysicalResourceIdV2> resources) throws Exception {
            var result = new ArrayList<VersionedValue>();
            for (var resource : resources) {
                assertThat(await(route.compareAndSet(
                                Optional.empty(),
                                resource.authorityKey(),
                                M5TargetDeleteAuthorityCodecV1.encodeAuthority(
                                        SyntheticDeleteAuthorityFixturesV2.phases(resource)
                                                .get(0)))))
                        .isEqualTo(MutationOutcome.APPLIED_EXACT);
                result.add(await(route.read(resource.authorityKey())).orElseThrow());
            }
            return result;
        }

        int tickets(PhysicalResourceIdV2 resource) throws Exception {
            return M5TargetDeleteAuthorityCodecV1.decodeAuthority(await(route.read(resource.authorityKey()))
                            .orElseThrow()
                            .canonicalStoredBytes())
                    .activeWriterTickets()
                    .size();
        }

        KafkaBookKeeperCompactionPublicationV2 publisher() {
            return new KafkaBookKeeperCompactionPublicationV2(
                    context.store,
                    7,
                    context.binding,
                    context.reader(),
                    context.owner,
                    new KafkaBookKeeperPublicationTicketsV2(guard, this::membership, context.owner));
        }

        CompletableFuture<PublicationOutcome> publish(KafkaSealedBookKeeperDescriptorV2 descriptor) throws Exception {
            return context.onOwner(() -> publisher()
                            .publish(
                                    input.plan(),
                                    input.semantic(),
                                    descriptor,
                                    context.protections,
                                    () -> new KafkaCompactionPublicationFenceV1().expected(input.plan())))
                    .toCompletableFuture();
        }

        void releaseSelector() {
            Runnable release = context.faults.selectorRelease.getAndSet(null);
            if (release != null) {
                release.run();
            }
        }

        public void close() throws Exception {
            try {
                context.close();
            } finally {
                try {
                    await(sourceSession.closeAsync());
                } finally {
                    try {
                        source.close();
                    } finally {
                        try {
                            output.close();
                        } finally {
                            try {
                                backend.close();
                            } finally {
                                oxia.close();
                            }
                        }
                    }
                }
            }
        }
    }
}
