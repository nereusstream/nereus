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

package com.nereusstream.metadata.oxia.v2.retention;

import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.assertThatThrownBy;
import com.nereusstream.domain.bytes.CanonicalBytes;
import com.nereusstream.domain.bytes.Sha256Digest;
import com.nereusstream.domain.identity.TopicBindingId;
import com.nereusstream.metadata.oxia.v2.mutation.AsyncOxiaConditionalClient;
import com.nereusstream.metadata.oxia.v2.mutation.AuthorityRecord;
import com.nereusstream.metadata.oxia.v2.mutation.OxiaConditionalClient;
import com.nereusstream.metadata.spi.retention.ExactMetadataTransactionStoreV1.MutationOutcome;
import com.nereusstream.metadata.spi.retention.ExactMetadataTransactionStoreV1.VersionedValue;
import com.nereusstream.storage.object.control.CanonicalControlMetadataStore;
import com.nereusstream.storage.object.control.ControlMutationOutcome;
import com.nereusstream.storage.object.materialization.M5MaterializationRecordsV1.IdentityEnvelope;
import com.nereusstream.storage.object.read.control.M4ReadControlCodecV1;
import com.nereusstream.storage.object.read.control.M4ReadControlRecordsV1.AdmissionState;
import com.nereusstream.storage.object.read.control.M4ReadControlRecordsV1.BindingIdentity;
import com.nereusstream.storage.object.read.control.M4ReadControlRecordsV1.BindingReadSelector;
import com.nereusstream.storage.object.read.control.M4ReadControlRecordsV1.CapabilityBinding;
import com.nereusstream.storage.object.read.control.M4ReadControlRecordsV1.ProtectionState;
import com.nereusstream.storage.object.read.control.M4ReadControlRecordsV1.SelectorMode;
import com.nereusstream.storage.object.read.control.M4ReadControlRecordsV1.SourceProtection;
import com.nereusstream.storage.object.read.control.M4ReadControlRecordsV1.SourceProtectionIdentity;
import com.nereusstream.storage.object.read.control.M4ReadControlRecordsV1.SourceRetirementBatch;
import com.nereusstream.storage.object.retention.M5BindingAuthorityCodecV1;
import com.nereusstream.storage.object.retention.M5BindingAuthorityControlMetadataStoreV1;
import com.nereusstream.storage.object.retention.M5BindingAuthorityRecordsV1.BatchAuthoritySlotV1;
import com.nereusstream.storage.object.retention.M5BindingAuthorityRecordsV1.BindingRetirementAuthorityV1;
import com.nereusstream.storage.object.retention.M5BindingAuthorityRecordsV1.ReferenceMutationTicketV1;
import com.nereusstream.storage.object.retention.M5BindingRetirementCoordinatorV1;
import com.nereusstream.storage.object.retention.M5RetentionRecordsV1.AuthorityFactV1;
import com.nereusstream.storage.object.retention.M5RetentionRecordsV1.M4ReleaseBindingV1;
import com.nereusstream.storage.object.retention.M5RetentionRecordsV1.ReferenceKindV1;
import com.nereusstream.storage.object.retention.M5RetentionRecordsV1.ReferenceTargetKindV1;
import com.nereusstream.storage.object.retention.M5RetiredBatchHistoryCoordinatorV2;
import com.nereusstream.storage.object.retention.M5RetiredBatchHistoryCoordinatorV2.Outcome;
import com.nereusstream.storage.object.retention.M5RetiredBatchHistoryV2;
import com.nereusstream.storage.object.retention.M5RetiredHistoryWriteBudgetV2;
import io.oxia.client.api.AsyncOxiaClient;
import io.oxia.client.api.OxiaClientBuilder;
import java.io.IOException;
import java.nio.charset.StandardCharsets;
import java.time.Duration;
import java.util.List;
import java.util.Optional;
import java.util.UUID;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.CompletionStage;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicInteger;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.Timeout;

/**
 * Real native metadata; proof facts and response-loss scheduling are explicit fixtures, not live writer authority.
 */
@Timeout(value = 30, unit = TimeUnit.MINUTES)
class M5RetiredHistoryOxiaIntegrationTest {
    static final CapabilityBinding CAPABILITY = new CapabilityBinding(7, digest("m5-capability"));

    @Test
    void continuousNativeRetirementExceedsLifetimeCapAndReconnectRejectsOldIds() throws Exception {
        try (Fixture fixture = Fixture.create(1)) {
            CanonicalBytes activeHole = M4ReadControlCodecV1.encodeBatch(fixture.batch(1));
            for (int id = 2; id <= 1_027; id++) {
                fixture.admit(id);
                var retirement = fixture.retire(id);
                assertThat(fixture.fold(id)).isEqualTo(Outcome.APPLIED_EXACT);
                var exact = fixture.exact();
                var authority = M5BindingAuthorityCodecV1.decodeAuthority(exact.canonicalStoredBytes());
                assertThat(exact.canonicalStoredBytes().length()).isLessThan(2_048);
                assertThat(authority.batchSlots()).singleElement().satisfies(slot -> {
                    assertThat(slot.activationOrdinal()).isEqualTo(1);
                    assertThat(slot.canonicalM4BatchBytes()).contains(activeHole);
                });
                assertThat(authority.retiredHistory().count()).isEqualTo(id - 1);
                assertThat(authority.lastActivationOrdinal()).isEqualTo(id);
                assertThat(fixture.budget.snapshot().unresolvedFolds()).isZero();
                if (id == 2) {
                    fixture.requireRejected(2);
                }
                if (id == 513) {
                    fixture.reconnect();
                    fixture.requireRejected(2);
                }
                if (id % 64 == 0 || id == 1_027) {
                    System.out.printf(
                            java.util.Locale.ROOT,
                            "M5_REAL_OXIA_HISTORY retired=%d selectorBytes=%d durableBytesCharged=%d%n",
                            id - 1,
                            exact.canonicalStoredBytes().length(),
                            fixture.budget.snapshot().chargedDurableBytes());
                }
                if (id == 1_027) {
                    assertThat(await(new M5BindingRetirementCoordinatorV1(fixture.store).retire(retirement)))
                            .isEqualTo(M5BindingRetirementCoordinatorV1.Outcome.EXISTING_TERMINAL);
                }
            }
            fixture.reconnect();
            for (int id : List.of(2, 512, 1_027)) {
                fixture.requireRejected(id);
                var proof = fixture.history()
                        .readProof(
                                fixture.authority().retiredHistory(),
                                fixture.batch(id).batchIdSha256(),
                                fixture::bytes);
                assertThat(proof.tombstone().orElseThrow().fullBatchSha256())
                        .isEqualTo(Sha256Digest.hash(M4ReadControlCodecV1.encodeBatch(fixture.batch(id))));
            }
            fixture.admit(1_028);
            assertThat(fixture.authority().batchSlots())
                    .extracting(BatchAuthoritySlotV1::activationOrdinal)
                    .containsExactly(1L, 1_028L);
            assertThat(fixture.store.supportsAtomicMultiKeyTransactions()).isFalse();
        }
    }

    @Test
    void lostNativePrewriteAndReadResponsesKeepReservationUntilExactRetry() throws Exception {
        try (Fixture fixture = Fixture.create(1, 2)) {
            fixture.retire(2);
            var predecessor = fixture.exact();
            fixture.faults.loseNextNodeResponse = true;
            assertThatThrownBy(() -> fixture.fold(2)).hasRootCauseMessage("injected native read response loss");
            assertThat(fixture.exact()).isEqualTo(predecessor);
            assertThat(fixture.budget.snapshot().unresolvedFolds()).isEqualTo(1);
            assertThat(fixture.faults.lastCreatedKey).contains("/nodes/");
            assertThat(fixture.bytes(fixture.faults.lastCreatedKey)).isPresent();
            long charged = fixture.budget.snapshot().chargedDurableBytes();
            assertThat(charged).isPositive();
            assertThat(fixture.fold(2)).isEqualTo(Outcome.APPLIED_EXACT);
            assertThat(fixture.budget.snapshot().unresolvedFolds()).isZero();
            assertThat(fixture.budget.snapshot().chargedDurableBytes()).isGreaterThan(charged);
            assertThat(await(fixture.coordinator.fold(
                            predecessor, fixture.batch(2).batchIdSha256())))
                    .isEqualTo(Outcome.EXISTING_TERMINAL);
        }
    }

    @Test
    void lostNativeSelectorResponseReconcilesExactTerminalAfterAnotherFold() throws Exception {
        try (Fixture fixture = Fixture.create(1, 2, 3)) {
            fixture.retire(2);
            fixture.retire(3);
            var before = fixture.exact();
            fixture.faults.holdSelectorKey = fixture.selectorKey;
            var pending = fixture.coordinator.fold(before, fixture.batch(2).batchIdSha256());
            await(fixture.faults.selectorApplied);
            try {
                assertThat(fixture.authority().retiredHistory().count()).isEqualTo(1);
                assertThat(fixture.budget.snapshot().unresolvedFolds()).isEqualTo(1);
                assertThat(fixture.fold(3)).isEqualTo(Outcome.APPLIED_EXACT);
                assertThat(fixture.authority().retiredHistory().count()).isEqualTo(2);
            } finally {
                fixture.faults.failedReads.set(1);
                fixture.faults.heldResponse.completeExceptionally(new IOException("injected native CAS response loss"));
            }
            assertThat(await(pending)).isEqualTo(Outcome.EXISTING_TERMINAL);
            assertThat(fixture.budget.snapshot().unresolvedFolds()).isZero();
            assertThat(await(fixture.coordinator.fold(before, fixture.batch(2).batchIdSha256())))
                    .isEqualTo(Outcome.EXISTING_TERMINAL);
        }
    }

    @Test
    void nativeRootChangeBetweenNonmembershipProofAndCasRejectsAdmission() throws Exception {
        try (Fixture fixture = Fixture.create(1, 2, 3)) {
            fixture.retire(2);
            fixture.retire(3);
            assertThat(fixture.fold(2)).isEqualTo(Outcome.APPLIED_EXACT);
            var before = fixture.authority();
            fixture.beforeCanonicalCas = () -> assertThat(fixture.fold(3)).isEqualTo(Outcome.APPLIED_EXACT);
            assertThat(fixture.facade.compareAndSet(
                            fixture.selectorKey,
                            Optional.of(M4ReadControlCodecV1.encodeSelector(before.selectorProjection())),
                            M4ReadControlCodecV1.encodeSelector(
                                    fixture.selector(List.of(fixture.batch(1), fixture.batch(4)), 2))))
                    .isEqualTo(ControlMutationOutcome.DEFINITIVE_CONFLICT);
            assertThat(fixture.authority().slot(fixture.batch(4).batchIdSha256()))
                    .isEmpty();
            assertThat(fixture.authority().retiredHistory().count()).isEqualTo(2);
            fixture.admit(4);
        }
    }

    @Test
    void missingAndCorruptNativeHistoryNeverAdmitAnOldOrNewBatch() throws Exception {
        try (Fixture fixture = Fixture.create(1, 2, 3)) {
            fixture.retire(2);
            fixture.retire(3);
            assertThat(fixture.fold(2)).isEqualTo(Outcome.APPLIED_EXACT);
            var before = fixture.exact();
            var history = fixture.history();
            String key = history.key(fixture.authority().retiredHistory().sha256());
            var exactNode = await(fixture.store.read(key)).orElseThrow();
            // Only this UUID-owned fixture is corrupted/deleted; the history implementation has no delete method.
            assertThat(await(fixture.store.compareAndSet(Optional.of(exactNode), key, bytes("corrupt-history"))))
                    .isEqualTo(MutationOutcome.APPLIED_EXACT);
            assertThatThrownBy(() -> fixture.tryAdmit(4))
                    .isInstanceOf(IllegalStateException.class)
                    .hasMessage("retired history node content address differs");
            assertThatThrownBy(() -> fixture.fold(3))
                    .hasRootCauseMessage("retired history node content address differs");
            assertThat(fixture.exact()).isEqualTo(before);
            await(fixture.client.delete(key));
            for (int id : List.of(2, 4)) {
                assertThatThrownBy(() -> fixture.tryAdmit(id))
                        .isInstanceOf(IllegalStateException.class)
                        .hasMessage("retired history node is missing");
                assertThatThrownBy(() -> await(new M5BindingRetirementCoordinatorV1(fixture.store)
                                .acquireTicket(fixture.ticketRequest(id))))
                        .hasRootCauseMessage("retired history node is missing");
            }
            assertThat(fixture.exact()).isEqualTo(before);
        }
    }

    static final class Fixture implements AutoCloseable {
        final String root;
        final String selectorKey;
        final BindingIdentity binding;
        final M5RetiredHistoryWriteBudgetV2 budget = new M5RetiredHistoryWriteBudgetV2(4, 1_000_000, 128_000_000);
        AsyncOxiaClient client;
        Faults faults;
        Oxia09ExactMetadataTransactionStoreV1 store;
        M5RetiredBatchHistoryCoordinatorV2 coordinator;
        M5BindingAuthorityControlMetadataStoreV1 facade;
        Runnable beforeCanonicalCas;

        Fixture(String root) throws Exception {
            this.root = root;
            this.selectorKey = root + "/selector";
            this.binding = new BindingIdentity(
                    new TopicBindingId(digest(root)), digest(root + "/incarnation"), digest(root + "/storage-epoch"));
            reconnect();
        }

        static Fixture create(int... ids) throws Exception {
            Fixture fixture = new Fixture("/nereus/v2/m5/history/" + UUID.randomUUID());
            try {
                var batches =
                        java.util.Arrays.stream(ids).mapToObj(fixture::batch).toList();
                fixture.create(
                        fixture.selectorKey,
                        M5BindingAuthorityCodecV1.encodeAuthority(
                                M5BindingAuthorityCodecV1.initial(fixture.selector(batches, 1))));
                assertThat(await(new M5BindingRetirementCoordinatorV1(fixture.store)
                                .enrollWriters(new M5BindingRetirementCoordinatorV1.EnrollmentRequest(
                                        fixture.selectorKey,
                                        fixture.exact(),
                                        M5RetentionOxiaIntegrationTest.registry()
                                                .enrollment()))))
                        .isEqualTo(M5BindingRetirementCoordinatorV1.Outcome.APPLIED_EXACT);
                return fixture;
            } catch (Exception | Error failure) {
                fixture.close();
                throw failure;
            }
        }

        void reconnect() throws Exception {
            if (client != null) {
                assertThat(budget.snapshot().unresolvedFolds()).isZero();
                client.close();
            }
            String address = System.getProperty("nereus.m5.retention.oxia.serviceAddress");
            if (address == null || address.isBlank() || "UNSET".equals(address)) {
                throw new IllegalStateException("missing source-locked M5 history Oxia service address");
            }
            var builder = OxiaClientBuilder.create(address);
            var loadedJar = java.nio.file.Path.of(builder.getClass()
                    .getProtectionDomain()
                    .getCodeSource()
                    .getLocation()
                    .toURI());
            assertThat(Sha256Digest.hash(CanonicalBytes.copyOf(java.nio.file.Files.readAllBytes(loadedJar)))
                            .toHex())
                    .isEqualTo("0ca719e6d11bd2ee2c2e7e94b42c6843e60f776bea12f7b5814cff9928e2e4c5");
            client = builder.namespace("default")
                    .requestTimeout(Duration.ofSeconds(10))
                    .asyncClient()
                    .get(30, TimeUnit.SECONDS);
            faults = new Faults(new AsyncOxiaConditionalClient(client));
            store = new Oxia09ExactMetadataTransactionStoreV1(faults);
            coordinator = new M5RetiredBatchHistoryCoordinatorV2(store, budget);
            // Test-only synchronous bridge on the JUnit thread; every value/CAS still goes to the native exact adapter.
            facade = new M5BindingAuthorityControlMetadataStoreV1(
                    new CanonicalControlMetadataStore() {
                        public Optional<CanonicalBytes> get(String key) {
                            return Fixture.this.bytes(key);
                        }

                        public ControlMutationOutcome putIfAbsent(String key, CanonicalBytes value) {
                            return map(await(store.compareAndSet(Optional.empty(), key, value)));
                        }

                        public ControlMutationOutcome compareAndSet(
                                String key, Optional<CanonicalBytes> expected, CanonicalBytes candidate) {
                            var exact = await(store.read(key));
                            if (!exact.map(VersionedValue::canonicalStoredBytes).equals(expected)) {
                                return ControlMutationOutcome.DEFINITIVE_CONFLICT;
                            }
                            Runnable action = beforeCanonicalCas;
                            beforeCanonicalCas = null;
                            if (action != null) {
                                action.run();
                            }
                            return map(await(store.compareAndSet(exact, key, candidate)));
                        }
                    },
                    selectorKey);
        }

        void admit(int id) {
            assertThat(tryAdmit(id)).isEqualTo(ControlMutationOutcome.APPLIED);
        }

        ControlMutationOutcome tryAdmit(int id) {
            var old = authority().selectorProjection();
            var batches = new java.util.ArrayList<>(old.activeBatches());
            batches.add(batch(id));
            return facade.compareAndSet(
                    selectorKey,
                    Optional.of(M4ReadControlCodecV1.encodeSelector(old)),
                    M4ReadControlCodecV1.encodeSelector(selector(batches, old.sourceGeneration() + 1)));
        }

        M5BindingRetirementCoordinatorV1.RetirementRequest retire(int id) {
            var batch = batch(id);
            var source = batch.sources().get(0);
            var proofHead = digest(root + "/release-" + id);
            var protectionBytes = M4ReadControlCodecV1.encodeProtection(new SourceProtection(
                    binding,
                    source,
                    ProtectionState.RELEASED,
                    Optional.of(batch.batchIdSha256()),
                    Optional.of(proofHead)));
            var protection = create(root + "/protection/" + id, protectionBytes);
            var releases = List.of(new M4ReleaseBindingV1(
                    source.sourceIdentitySha256(),
                    source.protectionGeneration(),
                    new AuthorityFactV1(
                            protection.key(), protection.metadataVersion(), protection.canonicalStoredSha256()),
                    protectionBytes,
                    batch.batchIdSha256(),
                    proofHead));
            var retirement = new M5BindingRetirementCoordinatorV1(store);
            assertThat(await(retirement.fence(new M5BindingRetirementCoordinatorV1.FenceRequest(
                            selectorKey, exact(), batch.batchIdSha256(), digest(root + "/fence-" + id), releases))))
                    .isEqualTo(M5BindingRetirementCoordinatorV1.Outcome.APPLIED_EXACT);
            var fenced = exact();
            var selector = authority().selectorProjection();
            var identity = new IdentityEnvelope(
                    digest(root + "/cell"),
                    digest(root + "/provider"),
                    binding,
                    selector.ownerEpoch(),
                    selector.readAdmissionEpoch(),
                    selector.sourceGeneration(),
                    CAPABILITY);
            var proof = M5RetentionOxiaIntegrationTest.proof(
                    store,
                    root + "/proof/" + id,
                    ReferenceTargetKindV1.RETIREMENT_BATCH,
                    batch.batchIdSha256(),
                    fenced,
                    releases,
                    identity);
            var request = new M5BindingRetirementCoordinatorV1.RetirementRequest(selectorKey, fenced, proof);
            assertThat(await(retirement.retire(request)))
                    .isEqualTo(M5BindingRetirementCoordinatorV1.Outcome.APPLIED_EXACT);
            return request;
        }

        void requireRejected(int id) {
            var old = authority();
            var changed = batch(id);
            changed = new SourceRetirementBatch(
                    changed.binding(),
                    changed.batchIdSha256(),
                    digest("changed-payload"),
                    changed.successorSelectorCoreSha256(),
                    changed.transitionSha256(),
                    changed.fallbackSetSha256(),
                    changed.sharedLastFallbackCapableReadAdmissionEpoch(),
                    changed.minimumFirstEpochSummary(),
                    changed.capability(),
                    changed.sources());
            var batches = new java.util.ArrayList<>(old.selectorProjection().activeBatches());
            batches.add(changed);
            var candidate = selector(batches, old.selectorProjection().sourceGeneration() + 1);
            assertThatThrownBy(() -> tryAdmit(id)).hasMessageContaining("historical BatchId");
            // A changed payload with the old digest is not a valid M4 encoding. Check the typed admission path
            // separately, using a proof read from native Oxia, before the M4 encoder can reject that input.
            assertThatThrownBy(() -> M5BindingAuthorityCodecV1.selectorSuccessor(
                            old, candidate, batchId -> history().readProof(old.retiredHistory(), batchId, this::bytes)))
                    .hasMessageContaining("historical BatchId");
            assertThat(await(new M5BindingRetirementCoordinatorV1(store).acquireTicket(ticketRequest(id))))
                    .isEqualTo(M5BindingRetirementCoordinatorV1.Outcome.RETAIN);
        }

        M5BindingRetirementCoordinatorV1.TicketRequest ticketRequest(int id) {
            var ticket = new ReferenceMutationTicketV1(
                    ReferenceTargetKindV1.RETIREMENT_BATCH,
                    batch(id).batchIdSha256(),
                    ReferenceKindV1.AUDIT_GRACE,
                    CAPABILITY,
                    digest(root + "/late-ticket-" + id),
                    digest(root + "/late-external-" + id));
            return new M5BindingRetirementCoordinatorV1.TicketRequest(selectorKey, exact(), ticket);
        }

        SourceRetirementBatch batch(int id) {
            var sources = List.of(new SourceProtectionIdentity(digest(root + "/source-" + id), 1, 1, 1, CAPABILITY));
            var draft = new SourceRetirementBatch(
                    binding,
                    digest("placeholder"),
                    digest(root + "/before-" + id),
                    digest(root + "/after-" + id),
                    digest(root + "/transition-" + id),
                    M4ReadControlCodecV1.calculateFallbackSetSha256(sources),
                    1,
                    1,
                    CAPABILITY,
                    sources);
            return new SourceRetirementBatch(
                    binding,
                    M4ReadControlCodecV1.calculateBatchId(draft),
                    draft.predecessorSelectorCoreSha256(),
                    draft.successorSelectorCoreSha256(),
                    draft.transitionSha256(),
                    draft.fallbackSetSha256(),
                    1,
                    1,
                    CAPABILITY,
                    sources);
        }

        BindingReadSelector selector(List<SourceRetirementBatch> batches, long generation) {
            return new BindingReadSelector(
                    binding,
                    digest(root + "/view-" + generation),
                    11,
                    13,
                    generation,
                    SelectorMode.PREFERRED_ONLY,
                    AdmissionState.ADMITTING,
                    Optional.empty(),
                    CAPABILITY,
                    List.of(),
                    batches);
        }

        VersionedValue create(String key, CanonicalBytes value) {
            assertThat(await(store.compareAndSet(Optional.empty(), key, value)))
                    .isEqualTo(MutationOutcome.APPLIED_EXACT);
            return await(store.read(key)).orElseThrow();
        }

        VersionedValue exact() {
            return await(store.read(selectorKey)).orElseThrow();
        }

        BindingRetirementAuthorityV1 authority() {
            return M5BindingAuthorityCodecV1.decodeAuthority(exact().canonicalStoredBytes());
        }

        Optional<CanonicalBytes> bytes(String key) {
            return await(store.read(key)).map(VersionedValue::canonicalStoredBytes);
        }

        M5RetiredBatchHistoryV2 history() {
            return new M5RetiredBatchHistoryV2(binding);
        }

        Outcome fold(int id) {
            return await(coordinator.fold(exact(), batch(id).batchIdSha256()));
        }

        public void close() throws Exception {
            if (client != null) {
                client.close();
            }
        }
    }

    /** The native operation is delegated first. Only acknowledgement/read delivery is faulted afterward. */
    private static final class Faults implements OxiaConditionalClient {
        final OxiaConditionalClient nativeClient;
        final AtomicInteger failedReads = new AtomicInteger();
        final CompletableFuture<Void> selectorApplied = new CompletableFuture<>();
        final CompletableFuture<Void> heldResponse = new CompletableFuture<>();
        volatile boolean loseNextNodeResponse;
        volatile String holdSelectorKey;
        volatile String lastCreatedKey;

        Faults(OxiaConditionalClient nativeClient) {
            this.nativeClient = nativeClient;
        }

        public CompletionStage<Optional<AuthorityRecord>> read(String key) {
            if (failedReads.getAndUpdate(count -> Math.max(0, count - 1)) > 0) {
                return CompletableFuture.failedFuture(new IOException("injected native read response loss"));
            }
            return nativeClient.read(key);
        }

        public CompletionStage<Void> createIfAbsent(String key, CanonicalBytes value) {
            return nativeClient.createIfAbsent(key, value).thenCompose(ignored -> {
                if (key.contains("/nodes/") && loseNextNodeResponse) {
                    loseNextNodeResponse = false;
                    lastCreatedKey = key;
                    failedReads.set(2);
                    return CompletableFuture.failedFuture(new IOException("injected native create response loss"));
                }
                return CompletableFuture.completedFuture(null);
            });
        }

        public CompletionStage<Void> compareAndSet(String key, CanonicalBytes value, long version) {
            return nativeClient.compareAndSet(key, value, version).thenCompose(ignored -> {
                if (key.equals(holdSelectorKey)) {
                    holdSelectorKey = null;
                    selectorApplied.complete(null);
                    return heldResponse;
                }
                return CompletableFuture.completedFuture(null);
            });
        }
    }

    static <T> T await(CompletionStage<T> stage) {
        try {
            return stage.toCompletableFuture().get(60, TimeUnit.SECONDS);
        } catch (Exception failure) {
            throw new IllegalStateException("native Oxia history operation did not complete exactly", failure);
        }
    }

    private static ControlMutationOutcome map(MutationOutcome outcome) {
        return switch (outcome) {
            case APPLIED_EXACT -> ControlMutationOutcome.APPLIED;
            case PREDECESSOR_UNCHANGED, DEFINITIVE_CONFLICT -> ControlMutationOutcome.DEFINITIVE_CONFLICT;
            case RESPONSE_UNKNOWN -> ControlMutationOutcome.RESPONSE_UNKNOWN;
        };
    }

    static CanonicalBytes bytes(String value) {
        return CanonicalBytes.copyOf(value.getBytes(StandardCharsets.UTF_8));
    }

    static Sha256Digest digest(String value) {
        return Sha256Digest.hash(bytes(value));
    }
}
