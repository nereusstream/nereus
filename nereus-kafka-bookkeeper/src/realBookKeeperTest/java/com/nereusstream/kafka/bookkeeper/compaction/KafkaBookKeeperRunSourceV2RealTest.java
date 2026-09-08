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
import com.nereusstream.storage.api.bookkeeper.BookKeeperCellSession;
import com.nereusstream.storage.api.bookkeeper.RunLedgerReadResultV1;
import com.nereusstream.storage.api.kafka.KafkaRunRootCatalogV2;
import com.nereusstream.storage.api.kafka.KafkaRunRootRecordV2;
import com.nereusstream.storage.api.kafka.KafkaRunRootSnapshotV1;
import com.nereusstream.storage.api.kafka.KafkaRunRootStateV1;
import com.nereusstream.storage.api.lifecycle.PhysicalResourceIdV2;
import com.nereusstream.storage.bookkeeper.M5BookKeeperNativeCreateClientV2;
import com.nereusstream.storage.bookkeeper.M5BookKeeperNativeCreateSpecV2;
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
import java.lang.reflect.InvocationTargetException;
import java.lang.reflect.Proxy;
import java.nio.file.Files;
import java.nio.file.Path;
import java.util.List;
import java.util.Optional;
import java.util.OptionalLong;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.CompletionStage;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicInteger;
import java.util.concurrent.atomic.AtomicReference;
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
                    var selectedSource = selectedReader(f, published, 1000, 500000);
                    var selected = await(selectedSource.capture());
                    verifySelected(selected, descriptor);
                    assertThatThrownBy(() -> await(
                                    selectedReader(f, published, 0, 500000).capture()))
                            .hasRootCauseMessage("Kafka decoded record budget exhausted");
                    assertThatThrownBy(() ->
                                    await(selectedReader(f, published, 1000, 0).capture()))
                            .hasRootCauseMessage("Kafka decoded record budget exhausted");
                    var next = selectedPlan(input.plan(), selected);
                    assertThat(await(selectedSource.resolve(next))
                                    .get(selected.extent().sourceIdentitySha256()))
                            .containsExactlyElementsOf(selected.resources());
                    var repeated = new KafkaSemanticCompactorV1().compileSemantic(next);
                    assertThat(repeated.outputBatches())
                            .isEqualTo(input.semantic().outputBatches());
                    assertThat(repeated.gaps()).isEqualTo(input.semantic().gaps());
                    assertThatThrownBy(() -> com.nereusstream.storage.object.materialization
                                    .M5MaterializationValidatorV1.requireFallbackProtections(
                                    next.sourceCut(), published.context.protections))
                            .hasMessage("M5 fallback protections differ from the frozen source identities");
                    recompactSelected(f, published, input, selected, 4300 + i);
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
    void selectedIndexOnlyGenerationCanBeAnEmptyCompactionInput() throws Exception {
        try (var f = new Fixture(2320, "orders", null)) {
            var root = admit(f, customSource(f, 3));
            var source = reader(f, f.roots, BOUNDS);
            var raw = await(source.capture(f.roots.nativeRootKey(root.runId())));
            var input = input(f, raw, 3320);
            assertThat(input.semantic().outputBatches()).isEmpty();
            try (var published = new Published(f, input, NativeContext.root())) {
                var descriptor = published.writeAndPublish(source);
                var selectedSource = selectedReader(f, published, 0, 0);
                var selected = await(selectedSource.capture());
                verifySelected(selected, descriptor);
                assertThat(selected.batches()).isEmpty();
                assertThat(selected.extent().recordCount()).isZero();
                assertThat(selected.extent().canonicalLength()).isPositive();
                assertThat(descriptor.task().parts())
                        .allMatch(part -> part.kind() == KafkaBookKeeperInventoryV2.PartKind.INDEX);
                var next = selectedPlan(input.plan(), selected);
                assertThat(await(selectedSource.resolve(next))).hasSize(1);
                var repeated = new KafkaSemanticCompactorV1().compileSemantic(next);
                assertThat(repeated.outputBatches()).isEmpty();
                assertThat(repeated.gaps()).containsExactly(new KafkaCompactionRecordsV1.Gap(0, 2));
                assertThat(repeated.indexes()).hasSize(8);
                recompactSelected(f, published, input, selected, 4320);
            }
        }
    }

    @Test
    void nativeReadOwnerClosesAdmissionAndRetainsTicketsUntilSessionTermination() throws Exception {
        try (var f = new Fixture(2530, "orders", null)) {
            var root = admit(f, f.prepareSealedSource());
            var source = reader(f, f.roots, BOUNDS);
            var raw = await(source.capture(f.roots.nativeRootKey(root.runId())));
            var input = input(f, raw, 3530);
            try (var published = new Published(f, input, NativeContext.root())) {
                var descriptor = published.writeAndPublish(source);
                var ready = new CompletableFuture<KafkaBookKeeperReadOwnerV2>();
                var entered = new CompletableFuture<RunLedgerReadResultV1>();
                var delivered = new CompletableFuture<RunLedgerReadResultV1>();
                var closeEntered = new CompletableFuture<Void>();
                var closeDelivered = new CompletableFuture<Void>();
                var lifetime = new CompletableFuture<KafkaBookKeeperReadOwnerV2.DrainEvidence>();
                var read = new AtomicReference<CompletableFuture<KafkaBookKeeperM4RecoveryV2.RecoveryResult>>();
                var calls = new AtomicInteger();
                var closes = new AtomicInteger();
                var scope = KafkaBookKeeperReadOwnerV2.run(
                        descriptor,
                        published.context.m4,
                        new M5TargetDeleteMultiWriterGuardV2(new M5TargetDeleteAuthorityCoordinatorV1(f.route)),
                        () -> {
                            var nativeSession = published.output.newSession();
                            return (BookKeeperCellSession) Proxy.newProxyInstance(
                                    BookKeeperCellSession.class.getClassLoader(),
                                    new Class<?>[] {BookKeeperCellSession.class},
                                    (proxy, method, args) -> {
                                        Object result;
                                        try {
                                            result = method.invoke(nativeSession, args);
                                        } catch (InvocationTargetException failure) {
                                            throw failure.getCause();
                                        }
                                        if (method.getName().equals("readExactEntry") && calls.incrementAndGet() == 1) {
                                            ((CompletionStage<?>) result).whenComplete((value, failure) -> {
                                                if (failure == null) {
                                                    entered.complete((RunLedgerReadResultV1) value);
                                                } else {
                                                    entered.completeExceptionally(failure);
                                                }
                                            });
                                            return delivered;
                                        }
                                        if (method.getName().equals("closeAsync")) {
                                            closes.incrementAndGet();
                                            return ((CompletionStage<?>) result).thenCompose(ignored -> {
                                                closeEntered.complete(null);
                                                return closeDelivered;
                                            });
                                        }
                                        return result;
                                    });
                        },
                        published.output::captureExactTarget,
                        published.context.owner,
                        new KafkaBookKeeperSelectedSourceV2.Bounds(128, 32, 1000, 1000000, 500000),
                        2,
                        owner -> {
                            ready.complete(owner);
                            read.set(owner.recover());
                            return lifetime;
                        });
                var actual = await(entered);
                var owner = await(ready);
                assertThat(read.get().cancel(true)).isTrue();
                var drained = owner.closeFallbackAndDrain(published.context.protections);
                drained.whenComplete((value, failure) -> {
                    if (failure == null) {
                        lifetime.complete(value);
                    } else {
                        lifetime.completeExceptionally(failure);
                    }
                });
                assertThatThrownBy(() -> await(owner.recover()))
                        .hasRootCauseMessage("BK read owner admission is closed");
                assertThat(closes.get()).isZero();
                assertNativeReadTickets(f, descriptor, 1);
                assertThat(drained.toCompletableFuture()).isNotDone();
                delivered.complete(actual);
                await(closeEntered);
                assertThat(closes.get()).isEqualTo(1);
                assertThat(drained.toCompletableFuture()).isNotDone();
                assertNativeReadTickets(f, descriptor, 1);
                closeDelivered.complete(null);
                var evidence = await(scope);
                assertThat(evidence.anchor().closedReadAdmissionEpoch())
                        .isEqualTo(evidence.predecessor().readAdmissionEpoch());
                assertThat(evidence.successor().mode()).isEqualTo(M4ReadControlRecordsV1.SelectorMode.PREFERRED_ONLY);
                assertNativeReadTickets(f, descriptor, 0);
                var keys = new com.nereusstream.storage.object.read.control.M4ReadControlKeysV1(
                        7, published.context.binding);
                assertThat(published.context.onOwner(() -> published.context.store.get(
                                keys.terminal(evidence.predecessor().readAdmissionEpoch()))))
                        .isEmpty();
                for (var old : published.context.protections) {
                    var protection = published.context.onOwner(() -> M4ReadControlCodecV1.decodeProtection(published
                            .context
                            .store
                            .get(keys.protection(old.sourceIdentitySha256(), old.protectionGeneration()))
                            .orElseThrow()));
                    assertThat(protection.state()).isEqualTo(M4ReadControlRecordsV1.ProtectionState.PROTECTED);
                }
            }
        }
    }

    private static void assertNativeReadTickets(Fixture f, KafkaSealedBookKeeperDescriptorV2 descriptor, int expected)
            throws Exception {
        for (var part : descriptor.sealedParts()) {
            var resource = new PhysicalResourceIdV2.BookKeeperLedger(
                    descriptor.task().namespace(),
                    part.handle().ledgerIdentity().ledgerId());
            assertThat(M5TargetDeleteAuthorityCodecV1.decodeAuthority(await(f.route.read(resource.authorityKey()))
                                    .orElseThrow()
                                    .canonicalStoredBytes())
                            .activeWriterTickets())
                    .hasSize(expected);
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
                    var selected =
                            await(selectedReader(f, published, 1000, 500000).capture());
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
                                    descriptor.descriptorSha256().toHex(),
                                    selected.extent().sourceIdentitySha256().toHex(),
                                    M5MaterializationCodecV1.calculateSourceSetSha256(List.of(selected.extent()))
                                            .toHex()));
                }
            }
        }
        writeSecondGenerationsBeforeRestart();
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
                    var selected =
                            await(selectedReader(f, published, 1000, 500000).capture());
                    assertThat(selected.extent().sourceIdentitySha256().toHex()).isEqualTo(lines.get(8));
                    assertThat(M5MaterializationCodecV1.calculateSourceSetSha256(List.of(selected.extent()))
                                    .toHex())
                            .isEqualTo(lines.get(9));
                    verifyCurrentFallbackEpoch(
                            published.context, selected.view().descriptor());
                    assertThat(published.context.faults.recordCreates.get()).isZero();
                    assertThat(published.context.faults.selectorCas.get()).isZero();
                }
                assertThat(f.tickets(root.ledgerIdentity())).isZero();
            }
        }
        readSecondGenerationsAfterRestart();
    }

    private static void writeSecondGenerationsBeforeRestart() throws Exception {
        for (int i = 0; i <= TOPICS.size(); i++) {
            var topic = TOPICS.get(i % TOPICS.size());
            try (var f = new Fixture(2500 + i, topic, null)) {
                var root = admit(f, i == TOPICS.size() ? customSource(f, 3) : f.prepareSealedSource());
                var source = reader(f, f.roots, BOUNDS);
                var raw = await(source.capture(f.roots.nativeRootKey(root.runId())));
                var input = input(f, raw, 3500 + i);
                try (var first = new Published(f, input, NativeContext.root())) {
                    var firstDescriptor = first.writeAndPublish(source);
                    var firstSelected =
                            await(selectedReader(f, first, 1000, 500000).capture());
                    var second = recompactSelected(f, first, input, firstSelected, 4500 + i);
                    var selected = second.snapshot();
                    assertThat(selected.view().descriptor().descriptorSha256())
                            .isNotEqualTo(firstDescriptor.descriptorSha256());
                    Files.write(
                            checkpoint().resolve("second-" + i),
                            List.of(
                                    f.source.spec().encode().toHex(),
                                    first.context.root,
                                    second.spec().encode().toHex(),
                                    selected.view()
                                            .descriptor()
                                            .descriptorSha256()
                                            .toHex(),
                                    selected.extent().sourceIdentitySha256().toHex(),
                                    M5MaterializationCodecV1.calculateSourceSetSha256(List.of(selected.extent()))
                                            .toHex(),
                                    Sha256Digest.hash(M4ReadControlCodecV1.encodeSelector(selected.selector()))
                                            .toHex(),
                                    firstDescriptor.descriptorSha256().toHex(),
                                    firstSelected
                                            .extent()
                                            .sourceIdentitySha256()
                                            .toHex()));
                }
            }
        }
    }

    private static void readSecondGenerationsAfterRestart() throws Exception {
        for (int i = 0; i <= TOPICS.size(); i++) {
            var lines = Files.readAllLines(checkpoint().resolve("second-" + i));
            assertThat(lines).hasSize(9);
            try (var f = new Fixture(2500 + i, TOPICS.get(i % TOPICS.size()), lines)) {
                var root = await(f.roots.openRoot(f.runBinding(0).runId())).orElseThrow();
                var raw = await(reader(f, f.roots, BOUNDS).capture(f.roots.nativeRootKey(root.runId())));
                var previous = input(f, raw, 3500 + i);
                // The old task supplies only the admitted Binding route; it cannot recover the current task.
                try (var bootstrap = new Published(f, previous, lines.get(1))) {
                    var descriptor = bootstrap.context.onOwner(() -> {
                        var selector = bootstrap.context.m4.readSelector().orElseThrow();
                        assertThat(Sha256Digest.hash(M4ReadControlCodecV1.encodeSelector(selector))
                                        .toHex())
                                .isEqualTo(lines.get(6));
                        var actual = KafkaSealedBookKeeperDescriptorCodecV2.decode(bootstrap
                                .context
                                .store
                                .get(KafkaBookKeeperCompactionPublicationV2.descriptorKey(
                                        selector.selectedViewSha256()))
                                .orElseThrow());
                        var task = KafkaBookKeeperInventoryCodecV2.decodeTask(bootstrap
                                .context
                                .store
                                .get(KafkaBookKeeperInventoryV2.taskKey(
                                        actual.task().taskIdSha256()))
                                .orElseThrow());
                        assertThat(task)
                                .isEqualTo(actual.task())
                                .isNotEqualTo(previous.layout().task());
                        return actual;
                    });
                    assertThat(descriptor.descriptorSha256().toHex()).isEqualTo(lines.get(3));
                    assertThat(descriptor.sourceCut().predecessorViewSha256().toHex())
                            .isEqualTo(lines.get(7));
                    assertThat(descriptor.sourceCut().sources()).singleElement().satisfies(extent -> assertThat(
                                    extent.sourceIdentitySha256().toHex())
                            .isEqualTo(lines.get(8)));
                    var spec = M5BookKeeperNativeCreateSpecV2.decode(
                            CanonicalBytes.copyOf(java.util.HexFormat.of().parseHex(lines.get(2))));
                    assertThat(spec.taskId()).isEqualTo(descriptor.task().taskIdSha256());
                    assertThat(spec.namespace()).isEqualTo(descriptor.task().namespace());
                    try (var output = M5BookKeeperNativeCreateClientV2.connect(
                                    System.getProperty("nereus.bookkeeper.metadataServiceUri"),
                                    descriptor.task().capability(),
                                    spec,
                                    f.binding);
                            var context = new NativeContext(descriptor.task(), lines.get(1), output.newSession())) {
                        var view = context.recover();
                        assertThat(view.descriptor()).isEqualTo(descriptor);
                        assertThat(view.parsedBatches())
                                .isEqualTo(previous.semantic().outputBatches());
                        assertThat(view.gaps()).isEqualTo(previous.semantic().gaps());
                        assertThat(view.allowsPredecessorOffset(0)).isFalse();
                        var selected = await(new KafkaBookKeeperSelectedSourceV2(
                                        context.store,
                                        context.m4,
                                        output,
                                        new M5TargetDeleteMultiWriterGuardV2(
                                                new M5TargetDeleteAuthorityCoordinatorV1(f.route)),
                                        new KafkaBookKeeperSelectedSourceV2.Bounds(128, 32, 1000, 1000000, 500000),
                                        context.owner)
                                .capture());
                        verifySelected(selected, descriptor);
                        verifyCurrentFallbackEpoch(context, descriptor);
                        assertThat(selected.extent().sourceIdentitySha256().toHex())
                                .isEqualTo(lines.get(4));
                        assertThat(M5MaterializationCodecV1.calculateSourceSetSha256(List.of(selected.extent()))
                                        .toHex())
                                .isEqualTo(lines.get(5));
                        assertThat(Sha256Digest.hash(M4ReadControlCodecV1.encodeSelector(selected.selector()))
                                        .toHex())
                                .isEqualTo(lines.get(6));
                        var keys = new com.nereusstream.storage.object.read.control.M4ReadControlKeysV1(
                                7, context.binding);
                        for (var batch : selected.selector().activeBatches()) {
                            for (var old : batch.sources()) {
                                var protection =
                                        context.onOwner(() -> M4ReadControlCodecV1.decodeProtection(context.store
                                                .get(keys.protection(
                                                        old.sourceIdentitySha256(), old.protectionGeneration()))
                                                .orElseThrow()));
                                assertThat(protection.state())
                                        .isEqualTo(M4ReadControlRecordsV1.ProtectionState.PROTECTED);
                            }
                        }
                        if (i == TOPICS.size()) {
                            assertThat(selected.batches()).isEmpty();
                            assertThat(descriptor.task().parts())
                                    .allMatch(part -> part.kind() == KafkaBookKeeperInventoryV2.PartKind.INDEX);
                        }
                        assertThat(context.faults.recordCreates.get()).isZero();
                        assertThat(context.faults.selectorCas.get()).isZero();
                    }
                    assertThat(bootstrap.context.faults.recordCreates.get()).isZero();
                    assertThat(bootstrap.context.faults.selectorCas.get()).isZero();
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
        if (mode == 3) {
            for (int i = 0; i < 2; i++) {
                f.data(handle, binding, i + 1, i, KafkaSemanticCompactorV1Test.emptyBatch(i, 1));
            }
            f.append(handle, 3, Nbke2CodecV1.encode(ledger, 3, f.footer(binding, 2, 3)));
        } else if (mode == 0) {
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

    private static KafkaBookKeeperSelectedSourceV2 selectedReader(
            Fixture f, Published published, int records, long decodedBytes) {
        return new KafkaBookKeeperSelectedSourceV2(
                published.context.store,
                published.context.m4,
                published.output,
                new M5TargetDeleteMultiWriterGuardV2(new M5TargetDeleteAuthorityCoordinatorV1(f.route)),
                new KafkaBookKeeperSelectedSourceV2.Bounds(128, 32, records, 1000000, decodedBytes),
                published.context.owner);
    }

    private static SelectedRestart recompactSelected(
            Fixture f,
            Published first,
            KafkaBookKeeperCompactionTestSupportV2.Input previous,
            KafkaBookKeeperSelectedSourceV2.Snapshot captured,
            long attempt)
            throws Exception {
        var descriptor = captured.view().descriptor();
        assertThat(first.context.onOwner(() -> first.context.m4.closeFallback(
                        captured.selector(),
                        descriptor.descriptorSha256(),
                        captured.selector().sourceGeneration() + 1,
                        first.context.protections)))
                .isEqualTo(com.nereusstream.storage.object.read.control.M4ReadControlCoordinatorV1.Outcome.APPLIED);
        var source = selectedReader(f, first, 1000, 500000);
        var preferred = await(source.capture());
        assertThat(preferred.extent()).isEqualTo(captured.extent());
        assertThat(preferred.resources()).isEqualTo(captured.resources());
        assertThat(preferred.selector().mode()).isEqualTo(M4ReadControlRecordsV1.SelectorMode.PREFERRED_ONLY);
        var plan = selectedPlan(previous.plan(), preferred);
        var semantic = new KafkaSemanticCompactorV1().compileSemantic(plan);
        var input = new KafkaBookKeeperCompactionTestSupportV2.Input(
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
                previous.capabilityEvidence());
        SelectedRestart restart;
        try (var second = new Published(f, input, first.context.root)) {
            var next = second.writeAndPublish(source);
            var view = second.context.recover();
            assertThat(view.descriptor()).isEqualTo(next);
            assertThat(view.parsedBatches()).isEqualTo(semantic.outputBatches());
            assertThat(view.gaps()).isEqualTo(semantic.gaps());
            assertThat(view.allowsPredecessorOffset(0)).isFalse();
            for (var kind : M5MaterializationRecordsV1.IndexKind.values()) {
                assertThat(view.index(kind)).isEqualTo(semantic.indexes().get(kind.ordinal()));
            }
            assertThat(second.context.onOwner(
                            () -> second.context.m4.readSelector().orElseThrow().activeBatches()))
                    .anyMatch(batch -> batch.sources().equals(first.context.protections));
            var keys = new com.nereusstream.storage.object.read.control.M4ReadControlKeysV1(7, second.context.binding);
            for (var old : first.context.protections) {
                var protection = second.context.onOwner(() -> M4ReadControlCodecV1.decodeProtection(second.context
                        .store
                        .get(keys.protection(old.sourceIdentitySha256(), old.protectionGeneration()))
                        .orElseThrow()));
                assertThat(protection.state()).isEqualTo(M4ReadControlRecordsV1.ProtectionState.PROTECTED);
            }
            restart = new SelectedRestart(
                    second.output.spec(),
                    await(selectedReader(f, second, 1000, 500000).capture()));
        }
        for (var resource : captured.resources()) {
            assertThat(M5TargetDeleteAuthorityCodecV1.decodeAuthority(await(f.route.read(resource.authorityKey()))
                                    .orElseThrow()
                                    .canonicalStoredBytes())
                            .activeWriterTickets())
                    .isEmpty();
        }
        return restart;
    }

    private record SelectedRestart(
            M5BookKeeperNativeCreateSpecV2 spec, KafkaBookKeeperSelectedSourceV2.Snapshot snapshot) {}

    private static void verifyCurrentFallbackEpoch(NativeContext context, KafkaSealedBookKeeperDescriptorV2 descriptor)
            throws Exception {
        context.onOwner(() -> {
            var selector = context.m4.readSelector().orElseThrow();
            assertThat(selector.mode()).isEqualTo(M4ReadControlRecordsV1.SelectorMode.PREFERRED_WITH_FALLBACK);
            long introduced =
                    Math.addExact(descriptor.sourceCut().predecessorSelector().readAdmissionEpoch(), 1);
            assertThat(selector.readAdmissionEpoch()).isEqualTo(introduced);
            var keys = new com.nereusstream.storage.object.read.control.M4ReadControlKeysV1(7, context.binding);
            var identities = descriptor.sourceCut().sources().stream()
                    .map(source -> {
                        var protection = M4ReadControlCodecV1.decodeProtection(context.store
                                .get(keys.protection(source.sourceIdentitySha256(), 1))
                                .orElseThrow());
                        assertThat(protection.state()).isEqualTo(M4ReadControlRecordsV1.ProtectionState.PROTECTED);
                        assertThat(protection.identity().firstFallbackCapableReadAdmissionEpoch())
                                .isEqualTo(introduced);
                        return protection.identity();
                    })
                    .sorted(java.util.Comparator.comparing(
                            value -> value.sourceIdentitySha256().toHex()))
                    .toList();
            assertThat(selector.fallbackSetSha256())
                    .contains(M4ReadControlCodecV1.calculateFallbackSetSha256(identities));
            return null;
        });
    }

    private static void verifySelected(
            KafkaBookKeeperSelectedSourceV2.Snapshot selected, KafkaSealedBookKeeperDescriptorV2 descriptor) {
        assertThat(selected.extent().kind())
                .isEqualTo(M5MaterializationRecordsV1.SourceKind.KAFKA_BK_COMPACTED_GENERATION_V2);
        assertThat(selected.extent().ledgerIdentitySha256()).isEmpty();
        assertThat(selected.extent().bodySha256()).isEqualTo(descriptor.descriptorSha256());
        assertThat(selected.resources()).hasSize(descriptor.sealedParts().size());
        assertThat(selected.resources())
                .containsExactlyInAnyOrderElementsOf(descriptor.sealedParts().stream()
                        .map(part -> new PhysicalResourceIdV2.BookKeeperLedger(
                                descriptor.task().namespace(),
                                part.handle().ledgerIdentity().ledgerId()))
                        .toList());
        assertThat(selected.batches()).hasSize(descriptor.batchCount());
        assertThat(selected.view().descriptor()).isEqualTo(descriptor);
        for (var kind : M5MaterializationRecordsV1.IndexKind.values()) {
            assertThat(selected.view().index(kind).kind()).isEqualTo(kind);
        }
    }

    private static KafkaCompactionRecordsV1.CompactionPlan selectedPlan(
            KafkaCompactionRecordsV1.CompactionPlan previous, KafkaBookKeeperSelectedSourceV2.Snapshot selected) {
        var old = previous.sourceCut();
        var sources = List.of(selected.extent());
        var selector = selected.selector();
        var cut = new M5MaterializationRecordsV1.MaterializationSourceCut(
                old.identity(),
                selector,
                Sha256Digest.hash(M4ReadControlCodecV1.encodeSelector(selector)),
                selector.selectedViewSha256(),
                selected.extent().coverage(),
                old.durableFrontier(),
                old.logEndFrontier(),
                old.highWatermark(),
                old.lastStableFrontier(),
                old.trimFrontier(),
                old.protocolStateRootSha256(),
                old.recoveryCheckpointRootSha256(),
                old.materializationPolicySha256(),
                old.outputFormatPolicySha256(),
                M5MaterializationCodecV1.calculateSourceSetSha256(sources),
                sources);
        assertThat(M5MaterializationCodecV1.decodeSourceCut(M5MaterializationCodecV1.encodeSourceCut(cut)))
                .isEqualTo(cut);
        return new KafkaCompactionRecordsV1.CompactionPlan(
                cut,
                previous.policy(),
                previous.frontiers(),
                previous.protocolRoots(),
                selected.batches(),
                previous.keyProofs(),
                previous.transactions(),
                previous.leaderEpochs(),
                previous.undecidableOffsets(),
                previous.recoveryRequiredOffsets());
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

        KafkaSealedBookKeeperDescriptorV2 writeAndPublish(KafkaBookKeeperPublicationTicketsV2.InputMembership source)
                throws Exception {
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
            verifyCurrentFallbackEpoch(context, descriptor);
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
