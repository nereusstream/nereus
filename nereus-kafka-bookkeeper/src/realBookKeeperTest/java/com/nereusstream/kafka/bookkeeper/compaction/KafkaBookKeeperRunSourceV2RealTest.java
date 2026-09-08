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
import com.nereusstream.domain.bytes.Sha256Digest;
import com.nereusstream.domain.identity.Id128;
import com.nereusstream.kafka.bookkeeper.compaction.KafkaBookKeeperOxiaControlV2RealTest.NativeContext;
import com.nereusstream.kafka.bookkeeper.compaction.KafkaBookKeeperRunRootsV2RealTest.Fixture;
import com.nereusstream.kafka.bookkeeper.compaction.KafkaBookKeeperRunSourceV2.Bounds;
import com.nereusstream.kafka.bookkeeper.compaction.KafkaBookKeeperRunSourceV2.Snapshot;
import com.nereusstream.kafka.bookkeeper.nbke2.Nbke2AppendGroupDescriptorV1;
import com.nereusstream.kafka.bookkeeper.nbke2.Nbke2BatchLocatorV1;
import com.nereusstream.kafka.bookkeeper.nbke2.Nbke2CodecV1;
import com.nereusstream.kafka.bookkeeper.nbke2.Nbke2DataV1;
import com.nereusstream.kafka.bookkeeper.nbke2.Nbke2IndexDirectoryEntryV1;
import com.nereusstream.kafka.bookkeeper.nbke2.Nbke2RangeIndexBlockV1;
import com.nereusstream.kafka.bookkeeper.nbke2.Nbke2RunFooterV1;
import com.nereusstream.kafka.bookkeeper.nbke2.Nbke2RunHeaderV1;
import com.nereusstream.kafka.bookkeeper.run.KafkaBookKeeperRunLifecycleV1;
import com.nereusstream.storage.api.kafka.KafkaRunRootCatalogV2;
import com.nereusstream.storage.api.kafka.KafkaRunRootRecordV2;
import com.nereusstream.storage.api.kafka.KafkaRunRootSnapshotV1;
import com.nereusstream.storage.api.kafka.KafkaRunRootStateV1;
import com.nereusstream.storage.bookkeeper.M5BookKeeperNativeCreateClientV2;
import com.nereusstream.storage.object.gc.M5TargetDeleteAuthorityCodecV1;
import com.nereusstream.storage.object.gc.M5TargetDeleteAuthorityCoordinatorV1;
import com.nereusstream.storage.object.gc.M5TargetDeleteMultiWriterGuardV2;
import com.nereusstream.storage.object.gc.SyntheticDeleteAuthorityFixturesV2;
import com.nereusstream.storage.object.materialization.M5MaterializationCodecV1;
import com.nereusstream.storage.object.materialization.M5MaterializationRecordsV1;
import com.nereusstream.storage.object.materialization.M5MaterializationRecordsV1.PublicationOutcome;
import com.nereusstream.storage.object.materialization.M5MaterializationRecordsV1.SourceExtent;
import com.nereusstream.storage.object.read.control.M4ReadControlCodecV1;
import com.nereusstream.storage.object.read.control.M4ReadControlRecordsV1;
import java.nio.file.Files;
import java.nio.file.Path;
import java.util.List;
import java.util.Optional;
import java.util.OptionalLong;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.CompletionStage;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicInteger;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.Timeout;

/** Actual root/DATA-derived sources; protocol semantic proofs and resource birth admission remain fixtures. */
@Timeout(value = 5, unit = TimeUnit.MINUTES)
class KafkaBookKeeperRunSourceV2RealTest {
    private static final Bounds BOUNDS = new Bounds(128, 32, 1000, 1_000_000, 500_000, 500_000);
    private static final List<String> TOPICS = List.of("orders", "__consumer_offsets", "__transaction_state");

    @Test
    void nativeRootsAndDataDriveTicketedCompactionForUserAndInternalTopicRoutes() throws Exception {
        for (int i = 0; i < TOPICS.size(); i++) {
            try (var f = new Fixture(2300 + i, TOPICS.get(i), null)) {
                var run = await(KafkaBookKeeperRunLifecycleV1.createActive(f.admitting, f.roots, f.runBinding(0), 0));
                f.writeData(run);
                await(run.drain());
                var root = await(run.seal(
                                f.footer(f.runBinding(0), 2, run.snapshot().nextEntryId())))
                        .root();
                var source = reader(f, f.roots, BOUNDS);
                var snapshot = await(source.capture(f.roots.nativeRootKey(root.runId())));
                assertThat(snapshot.batches())
                        .extracting(KafkaCompactionRecordsV1.InputBatch::canonicalBody)
                        .isEqualTo(f.bodies);
                assertThat(snapshot.extent().ledgerIdentitySha256())
                        .contains(f.record(root).resource().sha256());
                assertThat(snapshot.extent().recordCount()).isEqualTo(2);
                assertThat(snapshot.rangeIndexesCoverAllData()).isFalse();
                var successor = await(run.createSuccessor(f.runBinding(1)));
                assertThat(await(source.capture(snapshot.extent().physicalKey()))
                                .extent())
                        .isEqualTo(snapshot.extent());
                await(successor.drain());
                await(successor.seal(f.footer(f.runBinding(1), 2, 1)));
                var input = input(f, snapshot, 3300 + i);
                assertThat(await(source.resolve(input.plan()))
                                .get(snapshot.extent().sourceIdentitySha256()))
                        .containsExactly(snapshot.root().resource());
                try (var published = new Published(f, input, NativeContext.root())) {
                    var descriptor = published.writeAndPublish(source);
                    verify(published.context.recover(), input, descriptor.descriptorSha256());
                }
                assertThat(f.tickets(root.ledgerIdentity())).isZero();
            }
        }
    }

    @Test
    void fullNativeExtentMustMatchAndPhysicalFenceRejectsNewCapture() throws Exception {
        try (var f = new Fixture(2310, "orders", null)) {
            var root = admit(f, f.prepareSealedSource());
            var source = reader(f, f.roots, BOUNDS);
            var snapshot = await(source.capture(f.roots.nativeRootKey(root.runId())));
            var original = snapshot.extent();
            var wrong = new SourceExtent(
                    original.kind(),
                    original.sourceIdentitySha256(),
                    original.coverage(),
                    original.physicalKey(),
                    original.canonicalLength() + 1,
                    original.recordCount(),
                    original.minimumTimestamp(),
                    original.maximumTimestamp(),
                    original.bodySha256(),
                    original.immutableProviderVersionToken(),
                    original.ledgerIdentitySha256(),
                    original.formatRootSha256(),
                    original.encryptionPolicySha256(),
                    original.payloadLongLivedReadable(),
                    original.requiredIndexesPresent(),
                    original.memberBindingIds());
            var input = input(f, snapshot, 3310);
            assertThatThrownBy(
                            () -> await(source.resolve(withExtent(input.plan().sourceCut(), wrong))))
                    .hasRootCauseMessage("native run source differs from the complete frozen source extent");
            assertThat(f.tickets(root.ledgerIdentity())).isZero();
            var first = input.plan().inputBatches().get(0);
            var second = input.plan().inputBatches().get(1);
            var omitted = withBatches(input.plan(), List.of(second));
            for (var invalid : List.of(
                    omitted,
                    withBatches(input.plan(), List.of(second, first)),
                    withBatches(
                            input.plan(),
                            List.of(
                                    first,
                                    new KafkaCompactionRecordsV1.InputBatch(
                                            second.sourceIdentitySha256(), 2, second.canonicalBody()))),
                    withBatches(
                            input.plan(),
                            List.of(
                                    new KafkaCompactionRecordsV1.InputBatch(
                                            first.sourceIdentitySha256(), 0, second.canonicalBody()),
                                    second)))) {
                assertThatThrownBy(() -> await(source.resolve(invalid)))
                        .hasRootCauseMessage("native source input batches differ from compaction plan");
                assertThat(f.tickets(root.ledgerIdentity())).isZero();
            }
            var semantic = new KafkaSemanticCompactorV1().compileSemantic(omitted);
            var missingBatch = new KafkaBookKeeperCompactionTestSupportV2.Input(
                    omitted,
                    semantic,
                    KafkaBookKeeperCompactionLayoutV2.plan(
                            omitted,
                            semantic,
                            f.binding.physicalNamespace(),
                            f.source.capabilitySnapshot(),
                            3340,
                            512,
                            1024),
                    input.capabilityEvidence());
            try (var published = new Published(f, missingBatch, NativeContext.root())) {
                assertThatThrownBy(() -> published.writeAndPublish(source))
                        .hasRootCauseMessage("native source input batches differ from compaction plan");
                assertThat(published.context.faults.selectorCas.get()).isZero();
                assertThat(published.context.onOwner(
                                () -> published.context.store.get(KafkaBookKeeperCompactionPublicationV2.candidateKey(
                                        missingBatch.layout().task().taskIdSha256()))))
                        .isEmpty();
            }
            assertThat(f.tickets(root.ledgerIdentity())).isZero();
            var target = snapshot.root().resource();
            var before = await(f.route.read(target.authorityKey())).orElseThrow();
            var previous = M5TargetDeleteAuthorityCodecV1.decodeAuthority(before.canonicalStoredBytes());
            var synthetic = SyntheticDeleteAuthorityFixturesV2.phases(target)
                    .get(0)
                    .eligibilitySnapshot()
                    .orElseThrow();
            var refreshed = new com.nereusstream.storage.object.gc.DeleteEligibilitySnapshotV2(
                    target,
                    synthetic.reason(),
                    previous.authorityRevision() + 1,
                    synthetic.namespaceAdmission(),
                    synthetic.completeMemberInventory(),
                    synthetic.members());
            var qualified = com.nereusstream.storage.object.gc.M5TargetDeleteAuthorityStateMachineV1.qualifyEligibility(
                    previous, refreshed);
            assertThat(await(f.route.compareAndSet(
                            Optional.of(before),
                            target.authorityKey(),
                            M5TargetDeleteAuthorityCodecV1.encodeAuthority(qualified))))
                    .isEqualTo(
                            com.nereusstream.metadata.spi.retention.ExactMetadataTransactionStoreV1.MutationOutcome
                                    .APPLIED_EXACT);
            var current = await(f.route.read(target.authorityKey())).orElseThrow();
            var fenced = com.nereusstream.storage.object.gc.M5TargetDeleteAuthorityStateMachineV1.prepareIdentityRead(
                    qualified,
                    SyntheticDeleteAuthorityFixturesV2.digest("native source fence"),
                    SyntheticDeleteAuthorityFixturesV2.phases(target)
                            .get(1)
                            .readFence()
                            .orElseThrow()
                            .observationContext());
            assertThat(await(f.route.compareAndSet(
                            Optional.of(current),
                            target.authorityKey(),
                            M5TargetDeleteAuthorityCodecV1.encodeAuthority(fenced))))
                    .isEqualTo(
                            com.nereusstream.metadata.spi.retention.ExactMetadataTransactionStoreV1.MutationOutcome
                                    .APPLIED_EXACT);
            assertThatThrownBy(() -> await(source.capture(original.physicalKey())))
                    .hasRootCauseMessage("native source physical admission failed");
            assertThat(await(f.roots.openRoot(root.runId()))).contains(root);
        }
    }

    @Test
    void nativeBudgetsRejectBeforeReturningPartialInputAndReleaseTheirOwnReadTickets() throws Exception {
        try (var f = new Fixture(2311, "orders", null)) {
            var root = admit(f, f.prepareSealedSource());
            var key = f.roots.nativeRootKey(root.runId());
            for (var limit : List.of(
                    new Bounds(3, 32, 1000, 1_000_000, 500_000, 500_000),
                    new Bounds(128, 32, 1000, 1_000_000, 1, 500_000),
                    new Bounds(128, 32, 1, 1_000_000, 500_000, 500_000))) {
                assertThatThrownBy(() -> await(reader(f, f.roots, limit).capture(key)))
                        .hasRootCauseInstanceOf(IllegalArgumentException.class);
                assertThat(f.tickets(root.ledgerIdentity())).isZero();
            }
            assertThat(await(reader(f, f.roots, BOUNDS).capture(key)).batches()).hasSize(2);
        }
    }

    @Test
    void nativeGroupDigestAndIndexLocatorsAreCheckedAgainstActualData() throws Exception {
        for (int mode = 0; mode < 3; mode++) {
            try (var f = new Fixture(2312 + mode, "orders", null)) {
                var root = admit(f, customSource(f, mode));
                var source = reader(f, f.roots, BOUNDS);
                if (mode == 0) {
                    assertThatThrownBy(() -> await(source.capture(f.roots.nativeRootKey(root.runId()))))
                            .hasRootCauseMessage("native terminal append-group digest or bounds differ");
                } else if (mode == 1) {
                    assertThatThrownBy(() -> await(source.capture(f.roots.nativeRootKey(root.runId()))))
                            .hasRootCauseMessage("native index locator differs from actual DATA");
                } else {
                    var captured = await(source.capture(f.roots.nativeRootKey(root.runId())));
                    assertThat(captured.rangeIndexesCoverAllData()).isTrue();
                    assertThat(captured.extent().requiredIndexesPresent()).isFalse();
                }
                assertThat(f.tickets(root.ledgerIdentity())).isZero();
            }
        }
    }

    @Test
    void observerCancellationKeepsReadTicketUntilFinalNativeRootReadAndSessionClose() throws Exception {
        try (var f = new Fixture(2315, "orders", null)) {
            var root = admit(f, f.prepareSealedSource());
            var delivered = new CompletableFuture<Optional<KafkaRunRootRecordV2>>();
            var entered = new CompletableFuture<Optional<KafkaRunRootRecordV2>>();
            var calls = new AtomicInteger();
            KafkaRunRootCatalogV2 held = key -> f.roots.readSelectedRoot(key).thenCompose(value -> {
                if (calls.incrementAndGet() == 2) {
                    entered.complete(value);
                    return delivered;
                }
                return CompletableFuture.completedFuture(value);
            });
            var observer = reader(f, held, BOUNDS)
                    .capture(f.roots.nativeRootKey(root.runId()))
                    .toCompletableFuture();
            var selected = entered.get(30, TimeUnit.SECONDS);
            assertThat(f.tickets(root.ledgerIdentity())).isEqualTo(1);
            assertThat(observer.cancel(false)).isTrue();
            assertThat(f.tickets(root.ledgerIdentity())).isEqualTo(1);
            delivered.complete(selected);
            long end = System.nanoTime() + TimeUnit.SECONDS.toNanos(30);
            while (f.tickets(root.ledgerIdentity()) != 0 && System.nanoTime() < end) {
                Thread.sleep(10);
            }
            assertThat(f.tickets(root.ledgerIdentity())).isZero();
        }
    }

    @Test
    void writeBeforeServerRestart() throws Exception {
        Files.createDirectories(checkpoint());
        for (int i = 0; i < TOPICS.size(); i++) {
            try (var f = new Fixture(2400 + i, TOPICS.get(i), null)) {
                var root = admit(f, f.prepareSealedSource());
                var source = reader(f, f.roots, BOUNDS);
                var snapshot = await(source.capture(f.roots.nativeRootKey(root.runId())));
                var input = input(f, snapshot, 3400 + i);
                try (var published = new Published(f, input, NativeContext.root())) {
                    var descriptor = published.writeAndPublish(source);
                    var nativeRoot = await(f.nativeClient.read(snapshot.extent().physicalKey()))
                            .orElseThrow();
                    Files.write(
                            checkpoint().resolve(TOPICS.get(i)),
                            List.of(
                                    f.source.spec().encode().toHex(),
                                    root.runId().value().toHex(),
                                    Long.toString(root.ledgerIdentity().ledgerId()),
                                    snapshot.extent().sourceIdentitySha256().toHex(),
                                    M5MaterializationCodecV1.calculateSourceSetSha256(List.of(snapshot.extent()))
                                            .toHex(),
                                    Sha256Digest.hash(nativeRoot.storedBytes()).toHex() + ":" + nativeRoot.versionId(),
                                    published.context.root,
                                    descriptor.descriptorSha256().toHex()));
                }
            }
        }
    }

    @Test
    void readAfterServerRestart() throws Exception {
        for (int i = 0; i < TOPICS.size(); i++) {
            var lines = Files.readAllLines(checkpoint().resolve(TOPICS.get(i)));
            try (var f = new Fixture(2400 + i, TOPICS.get(i), lines)) {
                var root = await(f.roots.openRoot(f.runBinding(0).runId())).orElseThrow();
                assertThat(root.ledgerIdentity().ledgerId()).isEqualTo(Long.parseLong(lines.get(2)));
                var stored = await(f.nativeClient.read(f.roots.nativeRootKey(root.runId())))
                        .orElseThrow();
                assertThat(Sha256Digest.hash(stored.storedBytes()).toHex() + ":" + stored.versionId())
                        .isEqualTo(lines.get(5));
                var source = reader(f, f.roots, BOUNDS);
                var snapshot = await(source.capture(f.roots.nativeRootKey(root.runId())));
                assertThat(snapshot.extent().sourceIdentitySha256().toHex()).isEqualTo(lines.get(3));
                assertThat(M5MaterializationCodecV1.calculateSourceSetSha256(List.of(snapshot.extent()))
                                .toHex())
                        .isEqualTo(lines.get(4));
                var input = input(f, snapshot, 3400 + i);
                try (var published = new Published(f, input, lines.get(6))) {
                    verify(
                            published.context.recover(),
                            input,
                            Sha256Digest.copyOf(java.util.HexFormat.of().parseHex(lines.get(7))));
                    assertThat(published.context.faults.recordCreates.get()).isZero();
                    assertThat(published.context.faults.selectorCas.get()).isZero();
                }
                assertThat(f.tickets(root.ledgerIdentity())).isZero();
            }
        }
    }

    private static KafkaBookKeeperRunSourceV2 reader(Fixture f, KafkaRunRootCatalogV2 catalog, Bounds bounds) {
        return new KafkaBookKeeperRunSourceV2(
                catalog,
                f.source,
                new M5TargetDeleteMultiWriterGuardV2(new M5TargetDeleteAuthorityCoordinatorV1(f.route)),
                bounds,
                java.util.concurrent.ForkJoinPool.commonPool());
    }

    private static KafkaRunRootSnapshotV1 admit(Fixture f, KafkaRunRootSnapshotV1 root) throws Exception {
        assertThat(await(f.roots.createRoot(root)).exactProof()).contains(root);
        var sealed = new KafkaRunRootSnapshotV1(
                root.bindingId(),
                root.topicIncarnation(),
                root.partitionId(),
                root.storageEpochId(),
                root.creatorOwnerEpoch(),
                root.kafkaLeaderEpoch(),
                root.providerScopeId(),
                root.runId(),
                root.ledgerIdentity(),
                root.kafkaStartOffset(),
                OptionalLong.of(2),
                KafkaRunRootStateV1.SEALED,
                Optional.empty());
        assertThat(await(f.roots.sealRoot(root, sealed)).exactProof()).contains(sealed);
        return sealed;
    }

    private static KafkaRunRootSnapshotV1 customSource(Fixture f, int mode) throws Exception {
        var handle = await(f.admitting.createRunLedger(
                        f.source.spec().configurations().get(0)))
                .exactProof()
                .orElseThrow();
        var binding = f.runBinding(0);
        long ledger = handle.ledgerIdentity().ledgerId();
        f.append(
                handle,
                0,
                Nbke2CodecV1.encode(ledger, 0, new Nbke2RunHeaderV1(binding, 0, 1, f.verifier.capabilitySha256())));
        if (mode == 0) {
            var group = new Id128(100, f.attempt);
            var attempt = new Id128(101, f.attempt);
            f.append(
                    handle,
                    1,
                    Nbke2CodecV1.encode(
                            ledger,
                            1,
                            new Nbke2DataV1(binding, 0, 0, 0, 2, group, attempt, Optional.empty(), f.bodies.get(0))));
            var terminal = new Nbke2AppendGroupDescriptorV1(
                    0, 2, 1, 2, SyntheticDeleteAuthorityFixturesV2.digest("wrong group"));
            f.append(
                    handle,
                    2,
                    Nbke2CodecV1.encode(
                            ledger,
                            2,
                            new Nbke2DataV1(
                                    binding, 1, 0, 1, 2, group, attempt, Optional.of(terminal), f.bodies.get(1))));
            f.append(handle, 3, Nbke2CodecV1.encode(ledger, 3, f.footer(binding, 2, 3)));
        } else {
            for (int i = 0; i < 2; i++) {
                f.data(handle, binding, i + 1, i, f.bodies.get(i));
            }
            var rows = List.of(
                    new Nbke2BatchLocatorV1(0, 1, 0, 0, 0, f.bodies.get(0).length() + (mode == 1 ? 1 : 0), 1),
                    new Nbke2BatchLocatorV1(1, 1, 1, 1, 0, f.bodies.get(1).length(), 1));
            f.append(
                    handle,
                    3,
                    Nbke2CodecV1.encode(ledger, 3, new Nbke2RangeIndexBlockV1(binding, 0, 1, 2, 1, 2, -1, 4, rows)));
            f.append(
                    handle,
                    4,
                    Nbke2CodecV1.encode(
                            ledger,
                            4,
                            new Nbke2RunFooterV1(
                                    binding, 2, 5, 3, -1, 1, List.of(new Nbke2IndexDirectoryEntryV1(3, 0, 2)))));
        }
        assertThat(await(f.session.closeRunLedger(handle)).exactProof()).isPresent();
        return new KafkaRunRootSnapshotV1(
                f.scope.bindingId(),
                f.scope.topic(),
                f.scope.partition(),
                f.scope.storageEpoch(),
                1,
                1,
                f.scope.providerScope(),
                handle.runId(),
                handle.ledgerIdentity(),
                0,
                OptionalLong.empty(),
                KafkaRunRootStateV1.ACTIVE,
                Optional.empty());
    }

    private static KafkaBookKeeperCompactionTestSupportV2.Input input(Fixture f, Snapshot snapshot, long attempt) {
        var original = KafkaBookKeeperCompactionTestSupportV2.input(false, attempt);
        var p = original.plan();
        var old = p.sourceCut();
        var e = original.capabilityEvidence();
        var evidence = new M4ReadControlRecordsV1.CapabilityEvidence(
                snapshot.binding(),
                e.generation(),
                e.backendAdmissionGeneration(),
                e.kind(),
                e.state(),
                e.backendAdapterSha256(),
                e.backendProtocolConfigurationSha256(),
                e.readAdmissionContractSha256(),
                e.verifierSha256(),
                e.conformanceReceiptIdentitySha256(),
                e.conformanceReceiptSha256(),
                e.authorityTimeSemanticsSha256(),
                e.maximumSourceAccessLifetimeMillis(),
                e.maximumClockSkewMillis(),
                e.propagationGraceMillis());
        var capability = new M4ReadControlRecordsV1.CapabilityBinding(
                evidence.generation(), M4ReadControlCodecV1.capabilityEvidenceSha256(evidence));
        var s = old.predecessorSelector();
        var selector = new M4ReadControlRecordsV1.BindingReadSelector(
                snapshot.binding(),
                s.selectedViewSha256(),
                s.ownerEpoch(),
                s.readAdmissionEpoch(),
                s.sourceGeneration(),
                s.mode(),
                s.admissionState(),
                s.fallbackSetSha256(),
                capability,
                s.pendingAnchors(),
                s.activeBatches());
        var identity = new M5MaterializationRecordsV1.IdentityEnvelope(
                old.identity().protocolCellSha256(),
                f.scope.providerScope().digest(),
                snapshot.binding(),
                1,
                1,
                1,
                capability);
        var sources = List.of(snapshot.extent());
        var cut = new M5MaterializationRecordsV1.MaterializationSourceCut(
                identity,
                selector,
                Sha256Digest.hash(M4ReadControlCodecV1.encodeSelector(selector)),
                selector.selectedViewSha256(),
                snapshot.extent().coverage(),
                2,
                2,
                2,
                2,
                0,
                old.protocolStateRootSha256(),
                old.recoveryCheckpointRootSha256(),
                old.materializationPolicySha256(),
                old.outputFormatPolicySha256(),
                M5MaterializationCodecV1.calculateSourceSetSha256(sources),
                sources);
        var plan = new KafkaCompactionRecordsV1.CompactionPlan(
                cut,
                p.policy(),
                p.frontiers(),
                p.protocolRoots(),
                snapshot.batches(),
                p.keyProofs(),
                p.transactions(),
                p.leaderEpochs(),
                p.undecidableOffsets(),
                p.recoveryRequiredOffsets());
        var semantic = new KafkaSemanticCompactorV1().compileSemantic(plan);
        return new KafkaBookKeeperCompactionTestSupportV2.Input(
                plan,
                semantic,
                KafkaBookKeeperCompactionLayoutV2.plan(
                        plan,
                        semantic,
                        f.binding.physicalNamespace(),
                        f.source.capabilitySnapshot(),
                        attempt,
                        512,
                        1024),
                evidence);
    }

    private static M5MaterializationRecordsV1.MaterializationSourceCut withExtent(
            M5MaterializationRecordsV1.MaterializationSourceCut cut, SourceExtent extent) {
        var sources = List.of(extent);
        return new M5MaterializationRecordsV1.MaterializationSourceCut(
                cut.identity(),
                cut.predecessorSelector(),
                cut.predecessorSelectorValueSha256(),
                cut.predecessorViewSha256(),
                cut.coverage(),
                cut.durableFrontier(),
                cut.logEndFrontier(),
                cut.highWatermark(),
                cut.lastStableFrontier(),
                cut.trimFrontier(),
                cut.protocolStateRootSha256(),
                cut.recoveryCheckpointRootSha256(),
                cut.materializationPolicySha256(),
                cut.outputFormatPolicySha256(),
                M5MaterializationCodecV1.calculateSourceSetSha256(sources),
                sources);
    }

    private static KafkaCompactionRecordsV1.CompactionPlan withBatches(
            KafkaCompactionRecordsV1.CompactionPlan plan, List<KafkaCompactionRecordsV1.InputBatch> batches) {
        return new KafkaCompactionRecordsV1.CompactionPlan(
                plan.sourceCut(),
                plan.policy(),
                plan.frontiers(),
                plan.protocolRoots(),
                batches,
                plan.keyProofs(),
                plan.transactions(),
                plan.leaderEpochs(),
                plan.undecidableOffsets(),
                plan.recoveryRequiredOffsets());
    }

    private static final class Published implements AutoCloseable {
        final Fixture f;
        final KafkaBookKeeperCompactionTestSupportV2.Input input;
        final M5BookKeeperNativeCreateClientV2 output;
        final NativeContext context;

        Published(Fixture f, KafkaBookKeeperCompactionTestSupportV2.Input input, String root) throws Exception {
            this.f = f;
            this.input = input;
            output = M5BookKeeperNativeCreateClientV2.connect(
                    System.getProperty("nereus.bookkeeper.metadataServiceUri"),
                    input.layout().task().capability(),
                    KafkaBookKeeperNativeCreateV2RealTest.spec(input),
                    f.binding);
            context = new NativeContext(input.layout().task(), root, output.newSession());
        }

        KafkaSealedBookKeeperDescriptorV2 writeAndPublish(KafkaBookKeeperRunSourceV2 source) throws Exception {
            var descriptor = context.write(input);
            for (var seal : descriptor.sealedParts()) {
                await(f.admit(seal.handle()));
            }
            var guard = new M5TargetDeleteMultiWriterGuardV2(new M5TargetDeleteAuthorityCoordinatorV1(f.route));
            var publisher = new KafkaBookKeeperCompactionPublicationV2(
                    context.store,
                    7,
                    context.binding,
                    context.reader(),
                    context.owner,
                    new KafkaBookKeeperPublicationTicketsV2(guard, source, context.owner));
            assertThat(await(context.onOwner(() -> publisher.publish(
                            input.plan(),
                            input.semantic(),
                            descriptor,
                            context.protections,
                            () -> new KafkaCompactionPublicationFenceV1().expected(input.plan())))))
                    .isEqualTo(PublicationOutcome.APPLIED_EXACT);
            return descriptor;
        }

        public void close() throws Exception {
            try {
                context.close();
            } finally {
                output.close();
            }
        }
    }

    private static void verify(
            KafkaBookKeeperReadViewV2 view, KafkaBookKeeperCompactionTestSupportV2.Input input, Sha256Digest selected) {
        assertThat(view.descriptor().descriptorSha256()).isEqualTo(selected);
        assertThat(view.lookup(0).orElseThrow().coverage().inclusiveStart()).isEqualTo(1);
        assertThat(view.allowsPredecessorOffset(0)).isFalse();
        assertThat(view.readBatch(0))
                .isEqualTo(input.semantic().outputBatches().get(0).canonicalBody());
        for (var kind : M5MaterializationRecordsV1.IndexKind.values()) {
            assertThat(view.index(kind)).isEqualTo(input.semantic().indexes().get(kind.ordinal()));
        }
    }

    private static <T> T await(CompletionStage<T> stage) throws Exception {
        return stage.toCompletableFuture().get(45, TimeUnit.SECONDS);
    }

    private static Path checkpoint() {
        return Path.of(System.getProperty("nereus.m5.runsource.restartCheckpoint"));
    }
}
