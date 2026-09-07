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
import com.nereusstream.metadata.oxia.v2.compaction.OxiaKafkaBookKeeperRecordStoreV2;
import com.nereusstream.metadata.oxia.v2.mutation.AsyncOxiaConditionalClient;
import com.nereusstream.metadata.oxia.v2.mutation.AuthorityRecord;
import com.nereusstream.metadata.oxia.v2.mutation.OxiaConditionalClient;
import com.nereusstream.metadata.oxia.v2.retention.OxiaBindingLifecycleMetadataStoreV2;
import com.nereusstream.storage.api.bookkeeper.BookKeeperCellSession;
import com.nereusstream.storage.bookkeeper.M5BookKeeperDeleteAdapterV1;
import com.nereusstream.storage.bookkeeper.RealBookKeeperCellSessionV1;
import com.nereusstream.storage.bookkeeper.RealBookKeeperClientConfigurationV1;
import com.nereusstream.storage.object.materialization.M5MaterializationCodecV1;
import com.nereusstream.storage.object.materialization.M5MaterializationRecordsV1.IndexKind;
import com.nereusstream.storage.object.materialization.M5MaterializationRecordsV1.PublicationOutcome;
import com.nereusstream.storage.object.read.BindingReadHazardPoolV1;
import com.nereusstream.storage.object.read.control.BindingReadSelectorRuntimeV1;
import com.nereusstream.storage.object.read.control.M4ReadControlCoordinatorV1;
import com.nereusstream.storage.object.read.control.M4ReadControlRecordsV1.BindingIdentity;
import com.nereusstream.storage.object.read.control.M4ReadControlRecordsV1.SourceProtectionIdentity;
import com.nereusstream.storage.object.retention.M5BindingAuthorityCodecV1;
import io.oxia.client.api.AsyncOxiaClient;
import io.oxia.client.api.OxiaClientBuilder;
import java.lang.reflect.InvocationTargetException;
import java.lang.reflect.Proxy;
import java.nio.file.Files;
import java.nio.file.Path;
import java.time.Duration;
import java.util.List;
import java.util.Optional;
import java.util.UUID;
import java.util.concurrent.ArrayBlockingQueue;
import java.util.concurrent.Callable;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.CompletionStage;
import java.util.concurrent.ThreadPoolExecutor;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicInteger;
import org.apache.bookkeeper.client.api.BookKeeper;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.Timeout;

/** Real locked Oxia/BK execution; source cuts, policy state and capability admission remain synthetic fixtures. */
@Timeout(value = 5, unit = TimeUnit.MINUTES)
class KafkaBookKeeperOxiaControlV2RealTest {
    @Test
    void multiLedgerPublicationAndFreshClientsRecoverOnlySelectedBkWithAllEightIndexes() throws Exception {
        var input = KafkaBookKeeperCompactionTestSupportV2.input(false, 801);
        String root = NativeContext.root();
        Sha256Digest selected;
        try (var context = new NativeContext(input.layout().task(), root)) {
            var descriptor = context.write(input);
            assertThat(context.publish(input, descriptor)).isEqualTo(PublicationOutcome.APPLIED_EXACT);
            selected = descriptor.descriptorSha256();
            assertThat(context.faults.selectorCas.get()).isEqualTo(1);
            var authority = context.onOwner(() -> M5BindingAuthorityCodecV1.decodeAuthority(context.route
                    .read(context.store.selectorKey())
                    .toCompletableFuture()
                    .join()
                    .orElseThrow()
                    .canonicalStoredBytes()));
            assertThat(authority.retiredHistory().count()).isZero();
            assertThat(authority.selectorProjection().selectedViewSha256()).isEqualTo(selected);
        }
        try (var context = new NativeContext(input.layout().task(), root)) {
            var view = context.recover();
            assertThat(view.descriptor().descriptorSha256()).isEqualTo(selected);
            assertThat(view.lookup(0).orElseThrow().coverage().inclusiveStart()).isEqualTo(1);
            assertThat(view.listOffset(2)).hasValue(1);
            assertThat(view.allowsPredecessorOffset(0)).isFalse();
            for (IndexKind kind : IndexKind.values()) {
                assertThat(view.index(kind))
                        .isEqualTo(input.semantic().indexes().get(kind.ordinal()));
            }
            assertThat(context.faults.recordCreates.get()).isZero();
            assertThat(context.faults.selectorCas.get()).isZero();
        }
    }

    @Test
    void lostNativeSelectorResponsePublishesEmptyIndexOnlyOutputOnce() throws Exception {
        var input = KafkaBookKeeperCompactionTestSupportV2.input(true, 802);
        try (var context = new NativeContext(input.layout().task(), NativeContext.root())) {
            var descriptor = context.write(input);
            context.faults.loseSelector = true;
            assertThat(context.publish(input, descriptor))
                    .isIn(PublicationOutcome.APPLIED_EXACT, PublicationOutcome.EXISTING_EXACT);
            assertThat(context.publish(input, descriptor)).isEqualTo(PublicationOutcome.EXISTING_EXACT);
            assertThat(context.faults.selectorCas.get()).isEqualTo(1);
            var view = context.recover();
            assertThat(view.descriptor().batchCount()).isZero();
            assertThat(view.lookup(0)).isEmpty();
            assertThat(view.allowsPredecessorOffset(0)).isFalse();
            assertThat(view.descriptor().task().parts())
                    .allMatch(part -> part.kind() == KafkaBookKeeperInventoryV2.PartKind.INDEX);
        }
    }

    @Test
    void lostNativePartInventoryResponseRetainsTheSameRecordedLedgerBeforeCreate() throws Exception {
        var input = KafkaBookKeeperCompactionTestSupportV2.input(false, 803);
        try (var context = new NativeContext(input.layout().task(), NativeContext.root())) {
            context.faults.losePart = true;
            var descriptor = context.write(input);
            assertThat(context.faults.partCreates.get())
                    .isEqualTo(input.layout().parts().size());
            assertThat(context.faults.injectedPartLoss.get()).isEqualTo(1);
            for (int ordinal = 0; ordinal < descriptor.sealedParts().size(); ordinal++) {
                int part = ordinal;
                var recorded = context.onOwner(() -> new KafkaBookKeeperInventoryV2(context.store)
                        .readPart(input.layout().task(), part)
                        .orElseThrow());
                assertThat(recorded.handle())
                        .isEqualTo(descriptor.sealedParts().get(ordinal).handle());
            }
            assertThat(context.publish(input, descriptor)).isEqualTo(PublicationOutcome.APPLIED_EXACT);
            assertThat(context.recover().descriptor()).isEqualTo(descriptor);
        }
    }

    @Test
    void missingNativeLedgerOrDescriptorCannotRecreateOutputOrUseObsoleteInput() throws Exception {
        var input = KafkaBookKeeperCompactionTestSupportV2.input(false, 804);
        try (var context = new NativeContext(input.layout().task(), NativeContext.root())) {
            var descriptor = context.write(input);
            context.publish(input, descriptor);
            // Fault injection is limited to resources created in this unique fixture; no M5-D authority is implied.
            context.bk
                    .newDeleteLedgerOp()
                    .withLedgerId(descriptor
                            .sealedParts()
                            .get(0)
                            .handle()
                            .ledgerIdentity()
                            .ledgerId())
                    .execute()
                    .get(30, TimeUnit.SECONDS);
            int creates = context.faults.recordCreates.get();
            assertThatThrownBy(context::recover)
                    .hasRootCauseMessage("selected BK native sealed metadata is missing, changed or unknown");
            String key = KafkaBookKeeperCompactionPublicationV2.descriptorKey(descriptor.descriptorSha256());
            context.oxia.delete(context.records.nativeKey(key)).get(30, TimeUnit.SECONDS);
            assertThatThrownBy(context::recover).hasRootCauseMessage("selected native BK descriptor is missing");
            assertThat(context.faults.recordCreates.get()).isEqualTo(creates);
            assertThat(context.faults.selectorCas.get()).isEqualTo(1);
            assertThat(context.onOwner(
                            () -> context.m4.readSelector().orElseThrow().selectedViewSha256()))
                    .isEqualTo(descriptor.descriptorSha256());
        }
    }

    @Test
    void staleProtocolFenceLeavesNativePrewritesUnselected() throws Exception {
        var input = KafkaBookKeeperCompactionTestSupportV2.input(false, 805);
        try (var context = new NativeContext(input.layout().task(), NativeContext.root())) {
            var descriptor = context.write(input);
            var expected = new KafkaCompactionPublicationFenceV1().expected(input.plan());
            var stale = new KafkaCompactionPublicationFenceV1.Snapshot(
                    expected.compactionPlanRootSha256(),
                    expected.protocolStateRootSha256(),
                    expected.policyGeneration() + 1,
                    expected.frontiers());
            assertThatThrownBy(() -> context.onOwner(() -> context.publication()
                                    .publish(
                                            input.plan(),
                                            input.semantic(),
                                            descriptor,
                                            context.protections,
                                            () -> stale))
                            .toCompletableFuture()
                            .get(30, TimeUnit.SECONDS))
                    .hasRootCauseMessage("M5-B policy/root/frontier fence is stale before publication");
            assertThat(context.onOwner(() -> context.store.get(KafkaBookKeeperCompactionPublicationV2.candidateKey(
                            input.layout().task().taskIdSha256()))))
                    .contains(descriptor.descriptorSha256().bytes());
            assertThat(context.onOwner(() -> context.m4.readSelector()))
                    .contains(input.plan().sourceCut().predecessorSelector());
            assertThat(context.faults.selectorCas.get()).isZero();
        }
    }

    static final class NativeContext implements AutoCloseable {
        final KafkaBookKeeperInventoryV2.Task task;
        final BindingIdentity binding;
        final String root;
        final ThreadPoolExecutor owner = new ThreadPoolExecutor(
                1,
                1,
                0,
                TimeUnit.MILLISECONDS,
                new ArrayBlockingQueue<>(64),
                runnable -> new Thread(runnable, "m5-bk-control"),
                new ThreadPoolExecutor.AbortPolicy());
        final AsyncOxiaClient oxia;
        final BookKeeper bk;
        final RealBookKeeperCellSessionV1 session;
        final Faults faults;
        final OxiaKafkaBookKeeperRecordStoreV2 records;
        final OxiaBindingLifecycleMetadataStoreV2 route;
        final KafkaBookKeeperControlMetadataStoreV2 store;
        final M4ReadControlCoordinatorV1 m4;
        List<SourceProtectionIdentity> protections;

        NativeContext(KafkaBookKeeperInventoryV2.Task task, String root) throws Exception {
            this(task, root, null);
        }

        NativeContext(KafkaBookKeeperInventoryV2.Task task, String root, RealBookKeeperCellSessionV1 guardedSession)
                throws Exception {
            this.task = task;
            this.root = root;
            binding = M5MaterializationCodecV1.decodeSourceCut(task.sourceCut())
                    .identity()
                    .binding();
            assertArtifact(
                    BookKeeper.class, task.capability().clientArtifactSha256().toHex());
            var builder = OxiaClientBuilder.create(System.getProperty("nereus.m5.oxia.serviceAddress"));
            assertArtifact(builder.getClass(), "0ca719e6d11bd2ee2c2e7e94b42c6843e60f776bea12f7b5814cff9928e2e4c5");
            bk = BookKeeper.newBuilder(RealBookKeeperClientConfigurationV1.from(
                            System.getProperty("nereus.bookkeeper.metadataServiceUri"), task.capability()))
                    .build();
            assertThat(bk.isDriverMetadataServiceAvailable().get(10, TimeUnit.SECONDS))
                    .isTrue();
            try {
                oxia = builder.namespace("default")
                        .requestTimeout(Duration.ofSeconds(10))
                        .asyncClient()
                        .get(30, TimeUnit.SECONDS);
            } catch (Exception failure) {
                bk.close();
                owner.shutdownNow();
                throw failure;
            }
            faults = new Faults(new AsyncOxiaConditionalClient(oxia));
            records = new OxiaKafkaBookKeeperRecordStoreV2(faults, root);
            route = new OxiaBindingLifecycleMetadataStoreV2(faults, root, 7, binding);
            store = new KafkaBookKeeperControlMetadataStoreV2(
                    ownerOnly(records),
                    ownerOnly(route.rawControlMetadata()),
                    7,
                    binding,
                    task.namespace(),
                    task.capability().providerScopeId());
            m4 = new M4ReadControlCoordinatorV1(store, 7, binding);
            session = guardedSession == null
                    ? new RealBookKeeperCellSessionV1(bk, task.capability(), new byte[0])
                    : guardedSession;
            assertThat(session.capabilitySnapshot()).isEqualTo(task.capability());
        }

        static String root() {
            return "/nereus/v2/m5/bk-control/" + UUID.randomUUID();
        }

        <T> T onOwner(Callable<T> operation) throws Exception {
            return owner.submit(operation).get(60, TimeUnit.SECONDS);
        }

        KafkaSealedBookKeeperDescriptorV2 write(KafkaBookKeeperCompactionTestSupportV2.Input input) throws Exception {
            protections = onOwner(() -> KafkaBookKeeperCompactionTestSupportV2.installReadAuthority(store, input));
            var inspector = new M5BookKeeperDeleteAdapterV1(bk, task.capability(), new byte[0]);
            var parts = onOwner(() -> new KafkaBookKeeperCompactionWriterV2(
                                    new KafkaBookKeeperInventoryV2(store, owner),
                                    session,
                                    inspector::captureExactTarget,
                                    owner)
                            .write(input.layout()))
                    .toCompletableFuture()
                    .get(60, TimeUnit.SECONDS);
            return KafkaSealedBookKeeperDescriptorV2.create(input.plan(), input.semantic(), input.layout(), parts);
        }

        private static com.nereusstream.storage.object.control.CanonicalControlMetadataStore ownerOnly(
                com.nereusstream.storage.object.control.CanonicalControlMetadataStore delegate) {
            return new com.nereusstream.storage.object.control.CanonicalControlMetadataStore() {
                public Optional<CanonicalBytes> get(String key) {
                    Faults.requireOwner();
                    return delegate.get(key);
                }

                public com.nereusstream.storage.object.control.ControlMutationOutcome putIfAbsent(
                        String key, CanonicalBytes value) {
                    Faults.requireOwner();
                    return delegate.putIfAbsent(key, value);
                }

                public com.nereusstream.storage.object.control.ControlMutationOutcome compareAndSet(
                        String key, Optional<CanonicalBytes> expected, CanonicalBytes value) {
                    Faults.requireOwner();
                    return delegate.compareAndSet(key, expected, value);
                }
            };
        }

        KafkaSealedBookKeeperReaderV2 reader() {
            var inspector = new M5BookKeeperDeleteAdapterV1(bk, task.capability(), new byte[0]);
            var readOnly = (BookKeeperCellSession) Proxy.newProxyInstance(
                    BookKeeperCellSession.class.getClassLoader(),
                    new Class<?>[] {BookKeeperCellSession.class},
                    (proxy, method, args) -> {
                        if (!List.of("capabilitySnapshot", "openRunLedger", "readExactEntry")
                                .contains(method.getName())) {
                            throw new AssertionError(
                                    "selected recovery attempted a native mutation: " + method.getName());
                        }
                        try {
                            return method.invoke(session, args);
                        } catch (InvocationTargetException failure) {
                            throw failure.getCause();
                        }
                    });
            return new KafkaSealedBookKeeperReaderV2(readOnly, inspector::captureExactTarget, 1_000_000);
        }

        KafkaBookKeeperCompactionPublicationV2 publication() {
            return new KafkaBookKeeperCompactionPublicationV2(store, 7, binding, reader(), owner);
        }

        PublicationOutcome publish(
                KafkaBookKeeperCompactionTestSupportV2.Input input, KafkaSealedBookKeeperDescriptorV2 descriptor)
                throws Exception {
            return onOwner(() -> publication()
                            .publish(
                                    input.plan(),
                                    input.semantic(),
                                    descriptor,
                                    protections,
                                    () -> new KafkaCompactionPublicationFenceV1().expected(input.plan())))
                    .toCompletableFuture()
                    .get(60, TimeUnit.SECONDS);
        }

        KafkaBookKeeperReadViewV2 recover() throws Exception {
            return onOwner(() -> {
                        var selected = m4.readSelector().orElseThrow();
                        var descriptor = KafkaSealedBookKeeperDescriptorCodecV2.decode(
                                store.get(KafkaBookKeeperCompactionPublicationV2.descriptorKey(
                                                selected.selectedViewSha256()))
                                        .orElseThrow(() ->
                                                new IllegalStateException("selected native BK descriptor is missing")));
                        assertThat(descriptor.task()).isEqualTo(task);
                        var authority = KafkaBookKeeperM4RecoveryV2.project(descriptor, selected);
                        var runtime = new BindingReadSelectorRuntimeV1(binding, m4, selected, authority);
                        runtime.installExactDurable(authority);
                        var hazards = new BindingReadHazardPoolV1(2, 4);
                        var recovery =
                                new KafkaBookKeeperM4RecoveryV2(runtime.currentAuthority(), hazards, owner, reader());
                        int metadataReads = faults.reads.get();
                        return recovery.recover(descriptor).thenApply(result -> {
                            assertThat(faults.reads.get()).isEqualTo(metadataReads);
                            assertThat(result.capturedAuthority()).isSameAs(authority);
                            return result.view();
                        });
                    })
                    .get(60, TimeUnit.SECONDS);
        }

        @Override
        public void close() throws Exception {
            try {
                session.closeAsync().toCompletableFuture().get(30, TimeUnit.SECONDS);
            } finally {
                try {
                    bk.close();
                } finally {
                    oxia.close();
                    owner.shutdown();
                    assertThat(owner.awaitTermination(30, TimeUnit.SECONDS)).isTrue();
                }
            }
        }

        private static void assertArtifact(Class<?> type, String expected) throws Exception {
            Path jar = Path.of(
                    type.getProtectionDomain().getCodeSource().getLocation().toURI());
            assertThat(Sha256Digest.hash(CanonicalBytes.copyOf(Files.readAllBytes(jar)))
                            .toHex())
                    .isEqualTo(expected);
        }
    }

    static final class Faults implements OxiaConditionalClient {
        final OxiaConditionalClient nativeClient;
        final AtomicInteger reads = new AtomicInteger();
        final AtomicInteger recordCreates = new AtomicInteger();
        final AtomicInteger partCreates = new AtomicInteger();
        final AtomicInteger injectedPartLoss = new AtomicInteger();
        final AtomicInteger selectorCas = new AtomicInteger();
        volatile boolean losePart;
        volatile boolean loseSelector;
        volatile boolean rejectReadsAfterSelector;
        volatile boolean readsRejected;
        volatile boolean loseTerminal;
        volatile boolean loseDecision;
        volatile boolean holdSelector;
        final CompletableFuture<Void> selectorHeld = new CompletableFuture<>();
        final java.util.concurrent.atomic.AtomicReference<Runnable> selectorRelease =
                new java.util.concurrent.atomic.AtomicReference<>();

        Faults(OxiaConditionalClient nativeClient) {
            this.nativeClient = nativeClient;
        }

        private static void requireOwner() {
            assertThat(Thread.currentThread().getName()).isEqualTo("m5-bk-control");
        }

        public CompletionStage<Optional<AuthorityRecord>> read(String key) {
            reads.incrementAndGet();
            if (readsRejected) {
                return CompletableFuture.failedFuture(
                        new IllegalStateException("native control read delivery unavailable"));
            }
            return nativeClient.read(key);
        }

        public CompletionStage<Void> createIfAbsent(String key, CanonicalBytes value) {
            recordCreates.incrementAndGet();
            boolean partLoss = key.contains("/part/") && losePart;
            boolean terminalLoss = key.endsWith("/terminal") && loseTerminal;
            boolean decisionLoss = key.contains("/decisions/") && loseDecision;
            boolean lose = partLoss || terminalLoss || decisionLoss;
            if (terminalLoss) {
                loseTerminal = false;
            }
            if (decisionLoss) {
                loseDecision = false;
            }
            if (key.contains("/part/")) {
                partCreates.incrementAndGet();
            }
            if (partLoss) {
                losePart = false;
                injectedPartLoss.incrementAndGet();
            }
            return nativeClient
                    .createIfAbsent(key, value)
                    .thenCompose(ignored -> lose
                            ? CompletableFuture.failedFuture(
                                    new IllegalStateException("post-native part response loss"))
                            : CompletableFuture.completedFuture(null));
        }

        public CompletionStage<Void> compareAndSet(String key, CanonicalBytes value, long version) {
            selectorCas.incrementAndGet();
            if (holdSelector) {
                holdSelector = false;
                var delayed = new CompletableFuture<Void>();
                selectorRelease.set(
                        () -> nativeClient.compareAndSet(key, value, version).whenComplete((ignored, failure) -> {
                            if (failure == null) {
                                delayed.complete(null);
                            } else {
                                delayed.completeExceptionally(failure);
                            }
                        }));
                selectorHeld.complete(null);
                return delayed;
            }
            boolean lose = loseSelector;
            loseSelector = false;
            boolean rejectAfter = rejectReadsAfterSelector;
            rejectReadsAfterSelector = false;
            return nativeClient.compareAndSet(key, value, version).thenCompose(ignored -> {
                if (rejectAfter) {
                    readsRejected = true;
                    return CompletableFuture.<Void>failedFuture(
                            new IllegalStateException("applied native selector and subsequent read delivery loss"));
                }
                return lose
                        ? CompletableFuture.failedFuture(
                                new IllegalStateException("post-native selector response loss"))
                        : CompletableFuture.<Void>completedFuture(null);
            });
        }
    }
}
