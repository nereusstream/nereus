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

package com.nereusstream.storage.object.retention;

import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.assertThatThrownBy;
import com.nereusstream.domain.bytes.CanonicalBytes;
import com.nereusstream.domain.bytes.Sha256Digest;
import com.nereusstream.domain.identity.TopicBindingId;
import com.nereusstream.metadata.spi.model.MetadataVersion;
import com.nereusstream.metadata.spi.retention.ExactMetadataTransactionStoreV1;
import com.nereusstream.metadata.spi.retention.ExactMetadataTransactionStoreV1.VersionedValue;
import com.nereusstream.storage.object.control.CanonicalControlMetadataStore;
import com.nereusstream.storage.object.control.ControlMutationOutcome;
import com.nereusstream.storage.object.read.control.M4ReadControlCodecV1;
import com.nereusstream.storage.object.read.control.M4ReadControlRecordsV1.AdmissionState;
import com.nereusstream.storage.object.read.control.M4ReadControlRecordsV1.BindingIdentity;
import com.nereusstream.storage.object.read.control.M4ReadControlRecordsV1.BindingReadSelector;
import com.nereusstream.storage.object.read.control.M4ReadControlRecordsV1.CapabilityBinding;
import com.nereusstream.storage.object.read.control.M4ReadControlRecordsV1.SelectorMode;
import com.nereusstream.storage.object.read.control.M4ReadControlRecordsV1.SourceProtectionIdentity;
import com.nereusstream.storage.object.read.control.M4ReadControlRecordsV1.SourceRetirementBatch;
import com.nereusstream.storage.object.retention.M5BindingAuthorityRecordsV1.BatchAuthoritySlotV1;
import com.nereusstream.storage.object.retention.M5BindingAuthorityRecordsV1.BindingRetirementAuthorityV1;
import com.nereusstream.storage.object.retention.M5RetentionRecordsV1.BatchMetadataStateV1;
import com.nereusstream.storage.object.retention.M5RetentionRecordsV1.RetiredSourceRetirementBatchTombstoneV1;
import com.nereusstream.storage.object.retention.M5RetiredBatchHistoryCoordinatorV2.Outcome;
import java.nio.ByteBuffer;
import java.nio.charset.StandardCharsets;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.Optional;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.CompletionStage;
import org.junit.jupiter.api.Test;

/** Synthetic tombstones/control store exercise actual history, selector migration and same-key admission code. */
class M5RetiredBatchHistoryV2Test {
    private static final BindingIdentity BINDING =
            new BindingIdentity(new TopicBindingId(digest("binding")), digest("incarnation"), digest("storage-epoch"));
    private static final CapabilityBinding CAPABILITY = new CapabilityBinding(1, digest("capability"));
    private static final String SELECTOR = "/binding/selector";

    @Test
    void membershipAndNonmembershipBindEverySiblingDepthCountKeyAndExactTombstone() {
        Store store = new Store();
        var history = new M5RetiredBatchHistoryV2(BINDING);
        var one = tombstone(batch(1));
        var initial = history.emptyRoot();
        var insertion = history.insert(initial, one, history.readProof(initial, one.batchIdSha256(), store::bytes));
        persist(store, history, insertion);
        var member = history.readProof(insertion.successor(), one.batchIdSha256(), store::bytes);
        assertThat(member.tombstone()).contains(one);
        assertThat(member.siblings()).hasSize(256);
        var absent = history.readProof(insertion.successor(), digest("absent"), store::bytes);
        assertThat(absent.tombstone()).isEmpty();
        assertThatThrownBy(() -> history.verify(initial, one.batchIdSha256(), member))
                .isInstanceOf(IllegalArgumentException.class);
        assertThatThrownBy(() -> history.insert(insertion.successor(), one, member))
                .hasMessageContaining("reuses a BatchId");
        var wrongKey =
                new M5RetiredBatchHistoryV2.Proof(BINDING, digest("wrong-key"), Optional.empty(), absent.siblings());
        assertThatThrownBy(() -> history.verify(insertion.successor(), absent.batchId(), wrongKey))
                .isInstanceOf(IllegalArgumentException.class);
        var siblings = new ArrayList<>(member.siblings());
        siblings.set(0, siblings.get(1));
        assertThatThrownBy(() -> history.verify(
                        insertion.successor(),
                        one.batchIdSha256(),
                        new M5RetiredBatchHistoryV2.Proof(BINDING, one.batchIdSha256(), Optional.of(one), siblings)))
                .isInstanceOf(IllegalArgumentException.class);
        assertThatThrownBy(() -> new M5RetiredBatchHistoryV2.Proof(
                        BINDING,
                        one.batchIdSha256(),
                        Optional.of(one),
                        member.siblings().subList(1, 256)))
                .isInstanceOf(IllegalArgumentException.class);
        assertThatThrownBy(() -> history.verify(
                        new M5RetiredBatchHistoryV2.Root(insertion.successor().sha256(), 2),
                        one.batchIdSha256(),
                        member))
                .isInstanceOf(IllegalArgumentException.class);
        var foreign = new M5RetiredBatchHistoryV2(
                new BindingIdentity(BINDING.bindingId(), digest("other"), BINDING.storageEpochSha256()));
        assertThatThrownBy(() -> foreign.verify(insertion.successor(), one.batchIdSha256(), member))
                .isInstanceOf(IllegalArgumentException.class);
    }

    @Test
    void missingCorruptAndWrongDepthNodesNeverProveAbsence() {
        Store store = new Store();
        var history = new M5RetiredBatchHistoryV2(BINDING);
        var tombstone = tombstone(batch(1));
        var insertion = history.insert(
                history.emptyRoot(),
                tombstone,
                history.readProof(history.emptyRoot(), tombstone.batchIdSha256(), store::bytes));
        persist(store, history, insertion);
        String rootKey = history.key(insertion.successor().sha256());
        var original = store.values.remove(rootKey);
        assertThatThrownBy(() -> history.readProof(insertion.successor(), digest("absent"), store::bytes))
                .hasMessageContaining("missing");
        store.seed(rootKey, insertion.nodes().get(0).bytes());
        assertThatThrownBy(() -> history.readProof(insertion.successor(), tombstone.batchIdSha256(), store::bytes))
                .hasMessageContaining("content address");
        store.values.put(rootKey, original);
        var lower = insertion.nodes().get(255).root();
        assertThatThrownBy(() -> history.readProof(lower, tombstone.batchIdSha256(), store::bytes))
                .hasMessageContaining("depth");
    }

    @Test
    void foldPreservesActiveHoleOrdinalsM4BytesAndPermanentBatchIdRejectionAfterRestart() {
        Fixture fixture = new Fixture(1, 2, 3);
        fixture.retireFixture(2);
        fixture.retireFixture(3);
        var predecessor = fixture.exact();
        var projected = M4ReadControlCodecV1.encodeSelector(fixture.authority().selectorProjection());
        assertThat(fixture.fold(2)).isEqualTo(Outcome.APPLIED_EXACT);
        assertThat(fixture.fold(3)).isEqualTo(Outcome.APPLIED_EXACT);
        var current = fixture.authority();
        assertThat(current.batchSlots()).hasSize(1);
        assertThat(current.batchSlots().get(0).activationOrdinal()).isEqualTo(1);
        assertThat(current.retiredHistory().count()).isEqualTo(2);
        assertThat(current.lastActivationOrdinal()).isEqualTo(3);
        assertThat(fixture.facade.get(SELECTOR)).contains(projected);
        assertThat(fixture.coordinator
                        .fold(predecessor, batch(2).batchIdSha256())
                        .toCompletableFuture()
                        .join())
                .isEqualTo(Outcome.EXISTING_TERMINAL);
        var restarted = new M5BindingAuthorityControlMetadataStoreV1(fixture.store.canonical(), SELECTOR);
        var bad = selector(List.of(batch(1), batch(2)), 2);
        assertThatThrownBy(() -> restarted.compareAndSet(
                        SELECTOR, Optional.of(projected), M4ReadControlCodecV1.encodeSelector(bad)))
                .hasMessageContaining("historical BatchId");
        var valid = selector(List.of(batch(1), batch(4)), 2);
        assertThat(restarted.compareAndSet(
                        SELECTOR, Optional.of(projected), M4ReadControlCodecV1.encodeSelector(valid)))
                .isEqualTo(ControlMutationOutcome.APPLIED);
        assertThat(fixture.authority().batchSlots())
                .extracting(BatchAuthoritySlotV1::activationOrdinal)
                .containsExactly(1L, 4L);
        assertThatThrownBy(() -> M5BindingAuthorityCodecV1.selectorSuccessor(
                        fixture.authority(), selector(List.of(batch(1), batch(4), batch(5)), 3)))
                .hasMessageContaining("requires a proof");
    }

    @Test
    void legacyEnvelopeRoundTripsExactlyAndFoldUpgradesWithoutLosingOriginalTombstones() throws Exception {
        Fixture fixture = new Fixture(1, 2);
        fixture.retireFixture(2);
        CanonicalBytes bytes;
        try (var input = getClass().getResourceAsStream("/retention/m5-history/legacy-m5r1-v1.bin")) {
            bytes = CanonicalBytes.copyOf(
                    java.util.Objects.requireNonNull(input).readAllBytes());
        }
        assertThat(Sha256Digest.hash(bytes).toHex())
                .isEqualTo("5cd63ab14744698e182697640e1ab296b3412047cb31614976e7bfaef086ab02");
        var decoded = M5BindingAuthorityCodecV1.decodeAuthority(bytes);
        assertThat(decoded.wireVersion()).isEqualTo(1);
        assertThat(M5BindingAuthorityCodecV1.encodeAuthority(decoded)).isEqualTo(bytes);
        var old = fixture.store.seed(SELECTOR, bytes);
        assertThat(fixture.fold(2)).isEqualTo(Outcome.APPLIED_EXACT);
        assertThat(fixture.authority().wireVersion()).isEqualTo(2);
        assertThat(fixture.authority().batchSlots().get(0).canonicalM4BatchBytes())
                .isEqualTo(decoded.batchSlots().get(0).canonicalM4BatchBytes());
        assertThat(fixture.store
                        .compareAndSet(Optional.of(old), SELECTOR, bytes)
                        .toCompletableFuture()
                        .join())
                .isEqualTo(ExactMetadataTransactionStoreV1.MutationOutcome.DEFINITIVE_CONFLICT);
    }

    @Test
    void unknownPrewriteOrSelectorResponseRetainsOneBoundedReservationAndRetriesExactly() {
        Fixture fixture = new Fixture(1);
        fixture.retireFixture(1);
        var before = fixture.exact();
        fixture.store.dropNextNode = true;
        assertThat(fixture.fold(1)).isEqualTo(Outcome.RESPONSE_UNKNOWN);
        assertThat(fixture.exact()).isEqualTo(before);
        assertThat(fixture.budget.snapshot().unresolvedFolds()).isEqualTo(1);
        fixture.store.skipNextSelector = true;
        assertThat(fixture.fold(1)).isEqualTo(Outcome.RESPONSE_UNKNOWN);
        assertThat(fixture.exact()).isEqualTo(before);
        long charged = fixture.budget.snapshot().chargedDurableBytes();
        fixture.store.loseNextSelectorResponse = true;
        assertThat(fixture.fold(1)).isEqualTo(Outcome.EXISTING_TERMINAL);
        assertThat(fixture.budget.snapshot().chargedDurableBytes()).isEqualTo(charged);
        assertThat(fixture.budget.snapshot().unresolvedFolds()).isZero();
        assertThat(fixture.coordinator
                        .fold(before, batch(1).batchIdSha256())
                        .toCompletableFuture()
                        .join())
                .isEqualTo(Outcome.EXISTING_TERMINAL);
    }

    @Test
    void immutablePrewriteConflictQuarantinesWithoutRemovingResidentTerminal() {
        Fixture fixture = new Fixture(1);
        fixture.retireFixture(1);
        var before = fixture.exact();
        var current = fixture.authority();
        var history = new M5RetiredBatchHistoryV2(BINDING);
        var proof = history.readProof(current.retiredHistory(), batch(1).batchIdSha256(), fixture.store::bytes);
        var insertion = history.insert(current.retiredHistory(), tombstone(batch(1)), proof);
        fixture.store.seed(
                history.key(insertion.nodes().get(0).root().sha256()), CanonicalBytes.copyOf(new byte[] {9}));
        assertThatThrownBy(() -> fixture.fold(1))
                .hasRootCauseMessage("immutable retired history prewrite conflicts at its content address");
        assertThat(fixture.exact()).isEqualTo(before);
        assertThat(fixture.authority().retiredHistory().count()).isZero();
    }

    @Test
    void concurrentControlUpdateKeepsPrewrittenNodesUnselectedAndRetryUsesItsCurrentRoot() {
        Fixture fixture = new Fixture(1);
        fixture.retireFixture(1);
        var before = fixture.exact();
        fixture.store.beforeNextSelector = () -> {
            var current = fixture.authority();
            fixture.store.seed(
                    SELECTOR,
                    M5BindingAuthorityCodecV1.encodeAuthority(
                            M5BindingAuthorityCodecV1.selectorSuccessor(current, selector(List.of(), 2))));
        };
        assertThat(fixture.fold(1)).isEqualTo(Outcome.RETRY_STALE);
        assertThat(fixture.authority().retiredHistory().count()).isZero();
        assertThat(fixture.authority().batchSlots()).hasSize(1);
        assertThat(fixture.authority().authorityGeneration())
                .isGreaterThan(M5BindingAuthorityCodecV1.decodeAuthority(before.canonicalStoredBytes())
                        .authorityGeneration());
        assertThat(fixture.fold(1)).isEqualTo(Outcome.APPLIED_EXACT);
        assertThat(fixture.authority().retiredHistory().count()).isEqualTo(1);
    }

    @Test
    void capacityRefusesNewWorkButAllowsTheSameUnknownAttemptToReconcile() {
        Fixture fixture = new Fixture(1, 2);
        fixture.retireFixture(1);
        fixture.retireFixture(2);
        var limited = new M5RetiredHistoryWriteBudgetV2(1, 100_000, 100_000);
        var coordinator = new M5RetiredBatchHistoryCoordinatorV2(fixture.store, limited);
        fixture.store.dropNextNode = true;
        assertThat(coordinator
                        .fold(fixture.exact(), batch(1).batchIdSha256())
                        .toCompletableFuture()
                        .join())
                .isEqualTo(Outcome.RESPONSE_UNKNOWN);
        assertThat(coordinator
                        .fold(fixture.exact(), batch(2).batchIdSha256())
                        .toCompletableFuture()
                        .join())
                .isEqualTo(Outcome.RETAIN);
        assertThat(limited.snapshot().unresolvedFolds()).isEqualTo(1);
        assertThat(coordinator
                        .fold(fixture.exact(), batch(1).batchIdSha256())
                        .toCompletableFuture()
                        .join())
                .isEqualTo(Outcome.APPLIED_EXACT);
        limited.addAdmittedHeadroom(100_000);
        assertThat(coordinator
                        .fold(fixture.exact(), batch(2).batchIdSha256())
                        .toCompletableFuture()
                        .join())
                .isEqualTo(Outcome.APPLIED_EXACT);
    }

    @Test
    void missingHistoryStopsControlAdmissionAndHistoricalTicketAdmission() {
        Fixture fixture = new Fixture(1, 2);
        fixture.retireFixture(2);
        fixture.fold(2);
        var authority = fixture.authority();
        var history = new M5RetiredBatchHistoryV2(BINDING);
        var key = history.key(authority.retiredHistory().sha256());
        fixture.store.values.remove(key);
        assertThatThrownBy(() -> fixture.facade.compareAndSet(
                        SELECTOR,
                        Optional.of(M4ReadControlCodecV1.encodeSelector(authority.selectorProjection())),
                        M4ReadControlCodecV1.encodeSelector(selector(List.of(batch(1)), 2))))
                .hasMessageContaining("missing");
        var ticket = new M5BindingAuthorityRecordsV1.ReferenceMutationTicketV1(
                M5RetentionRecordsV1.ReferenceTargetKindV1.RETIREMENT_BATCH,
                batch(2).batchIdSha256(),
                M5RetentionRecordsV1.ReferenceKindV1.AUDIT_GRACE,
                CAPABILITY,
                digest("ticket"),
                digest("external"));
        var request = new M5BindingRetirementCoordinatorV1.TicketRequest(SELECTOR, fixture.exact(), ticket);
        assertThatThrownBy(() -> new M5BindingRetirementCoordinatorV1(fixture.store)
                        .acquireTicket(request)
                        .toCompletableFuture()
                        .join())
                .hasRootCauseMessage("retired history node is missing");
    }

    @Test
    void historyRootChangeBetweenAdmissionProofAndSelectorCasRejectsTheStaleCandidate() {
        Fixture fixture = new Fixture(1, 2, 3);
        fixture.retireFixture(2);
        fixture.retireFixture(3);
        fixture.fold(2);
        var before = fixture.authority();
        var history = new M5RetiredBatchHistoryV2(BINDING);
        var staleProof = history.readProof(before.retiredHistory(), batch(3).batchIdSha256(), fixture.store::bytes);
        fixture.store.beforeRawSelectorCas = () -> assertThat(fixture.fold(3)).isEqualTo(Outcome.APPLIED_EXACT);
        var candidate = selector(List.of(batch(1), batch(4)), 2);
        assertThat(fixture.facade.compareAndSet(
                        SELECTOR,
                        Optional.of(M4ReadControlCodecV1.encodeSelector(before.selectorProjection())),
                        M4ReadControlCodecV1.encodeSelector(candidate)))
                .isEqualTo(ControlMutationOutcome.DEFINITIVE_CONFLICT);
        var after = fixture.authority();
        assertThat(after.slot(batch(4).batchIdSha256())).isEmpty();
        assertThat(after.retiredHistory().count()).isEqualTo(2);
        assertThatThrownBy(() -> M5BindingAuthorityCodecV1.selectorSuccessor(
                        after, selector(List.of(batch(1), batch(3)), 2), ignored -> staleProof))
                .hasMessageContaining("selected root");
        assertThat(fixture.facade.compareAndSet(
                        SELECTOR,
                        Optional.of(M4ReadControlCodecV1.encodeSelector(after.selectorProjection())),
                        M4ReadControlCodecV1.encodeSelector(candidate)))
                .isEqualTo(ControlMutationOutcome.APPLIED);
    }

    @Test
    void lostFoldResponseFollowedByAnotherFoldReconcilesExactHistoricalTombstone() {
        Fixture fixture = new Fixture(1, 2, 3);
        fixture.retireFixture(2);
        fixture.retireFixture(3);
        var before = fixture.exact();
        fixture.store.loseNextSelectorResponse = true;
        fixture.store.afterNextSelector = () -> assertThat(fixture.fold(3)).isEqualTo(Outcome.APPLIED_EXACT);
        assertThat(fixture.coordinator
                        .fold(before, batch(2).batchIdSha256())
                        .toCompletableFuture()
                        .join())
                .isEqualTo(Outcome.EXISTING_TERMINAL);
        assertThat(fixture.authority().retiredHistory().count()).isEqualTo(2);
        assertThat(fixture.authority().batchSlots()).hasSize(1);
        assertThat(fixture.budget.snapshot().unresolvedFolds()).isZero();
    }

    @Test
    void changedPayloadCannotReuseHistoricalBatchIdOrAcquireANewTicket() {
        Fixture fixture = new Fixture(1, 2);
        fixture.retireFixture(2);
        fixture.fold(2);
        var old = batch(2);
        var changed = new SourceRetirementBatch(
                old.binding(),
                old.batchIdSha256(),
                old.predecessorSelectorCoreSha256(),
                digest("different-payload"),
                old.transitionSha256(),
                old.fallbackSetSha256(),
                old.sharedLastFallbackCapableReadAdmissionEpoch(),
                old.minimumFirstEpochSummary(),
                old.capability(),
                old.sources());
        var current = fixture.authority();
        var history = new M5RetiredBatchHistoryV2(BINDING);
        assertThatThrownBy(() -> M5BindingAuthorityCodecV1.selectorSuccessor(
                        current,
                        selector(List.of(batch(1), changed), 2),
                        id -> history.readProof(current.retiredHistory(), id, fixture.store::bytes)))
                .hasMessageContaining("historical BatchId");
        var ticket = new M5BindingAuthorityRecordsV1.ReferenceMutationTicketV1(
                M5RetentionRecordsV1.ReferenceTargetKindV1.RETIREMENT_BATCH,
                old.batchIdSha256(),
                M5RetentionRecordsV1.ReferenceKindV1.AUDIT_GRACE,
                CAPABILITY,
                digest("new-ticket"),
                digest("new-external"));
        assertThat(new M5BindingRetirementCoordinatorV1(fixture.store)
                        .acquireTicket(
                                new M5BindingRetirementCoordinatorV1.TicketRequest(SELECTOR, fixture.exact(), ticket))
                        .toCompletableFuture()
                        .join())
                .isEqualTo(M5BindingRetirementCoordinatorV1.Outcome.RETAIN);
    }

    @Test
    void concurrentReconciliationCannotReleaseReservationBeforeOriginalNativeCallbackDrains() {
        Fixture fixture = new Fixture(1);
        fixture.retireFixture(1);
        var budget = new M5RetiredHistoryWriteBudgetV2(1, 100_000, 1_000_000);
        var coordinator = new M5RetiredBatchHistoryCoordinatorV2(fixture.store, budget);
        var before = fixture.exact();
        fixture.store.holdNextNodeResponse = true;
        var pending = coordinator.fold(before, batch(1).batchIdSha256()).toCompletableFuture();
        try {
            var current = fixture.authority();
            fixture.store.seed(
                    SELECTOR,
                    M5BindingAuthorityCodecV1.encodeAuthority(
                            M5BindingAuthorityCodecV1.selectorSuccessor(current, selector(List.of(), 2))));
            assertThat(coordinator
                            .fold(before, batch(1).batchIdSha256())
                            .toCompletableFuture()
                            .join())
                    .isEqualTo(Outcome.RETRY_STALE);
            assertThat(budget.snapshot().unresolvedFolds()).isEqualTo(1);
            assertThat(coordinator
                            .fold(fixture.exact(), batch(1).batchIdSha256())
                            .toCompletableFuture()
                            .join())
                    .isEqualTo(Outcome.RETAIN);
        } finally {
            fixture.store.heldNodeResponse.complete(ExactMetadataTransactionStoreV1.MutationOutcome.APPLIED_EXACT);
        }
        assertThatThrownBy(pending::join).hasRootCauseMessage("history prewrite has no exact active reservation");
        assertThat(budget.snapshot().unresolvedFolds()).isZero();
        assertThat(coordinator
                        .fold(fixture.exact(), batch(1).batchIdSha256())
                        .toCompletableFuture()
                        .join())
                .isEqualTo(Outcome.APPLIED_EXACT);
    }

    @Test
    void observerCancellationKeepsThePrewriteReservationUntilActualMutationCompletion() {
        Fixture fixture = new Fixture(1);
        fixture.retireFixture(1);
        fixture.store.holdNextNodeResponse = true;
        var pending = fixture.coordinator
                .fold(fixture.exact(), batch(1).batchIdSha256())
                .toCompletableFuture();
        assertThat(pending.cancel(true)).isTrue();
        assertThat(fixture.store.heldNodeResponse.isCancelled()).isFalse();
        assertThat(fixture.budget.snapshot().unresolvedFolds()).isEqualTo(1);
        fixture.store.heldNodeResponse.complete(ExactMetadataTransactionStoreV1.MutationOutcome.APPLIED_EXACT);
        assertThat(fixture.budget.snapshot().unresolvedFolds()).isZero();
        assertThat(fixture.authority().retiredHistory().count()).isEqualTo(1);
        assertThat(pending.isCancelled()).isTrue();
    }

    @Test
    void moreThanLifetimeSlotCapCompletesWithOneActiveHoleAndBoundedResidentSelector() {
        Fixture fixture = new Fixture(1);
        for (int id = 2; id <= 1_027; id++) {
            var current = fixture.authority();
            var next = selector(List.of(batch(1), batch(id)), id);
            assertThat(fixture.facade.compareAndSet(
                            SELECTOR,
                            Optional.of(M4ReadControlCodecV1.encodeSelector(current.selectorProjection())),
                            M4ReadControlCodecV1.encodeSelector(next)))
                    .isEqualTo(ControlMutationOutcome.APPLIED);
            fixture.retireFixture(id);
            assertThat(fixture.fold(id)).isEqualTo(Outcome.APPLIED_EXACT);
            assertThat(fixture.exact().canonicalStoredBytes().length()).isLessThan(2_048);
            assertThat(fixture.authority().batchSlots()).hasSize(1);
        }
        assertThat(fixture.authority().lastActivationOrdinal()).isEqualTo(1_027);
        assertThat(fixture.authority().retiredHistory().count()).isEqualTo(1_026);
        var history = new M5RetiredBatchHistoryV2(BINDING);
        for (int id : List.of(2, 512, 1_027)) {
            assertThat(history.readProof(
                                    fixture.authority().retiredHistory(),
                                    batch(id).batchIdSha256(),
                                    fixture.store::bytes)
                            .tombstone())
                    .contains(tombstone(batch(id)));
        }
        assertThat(fixture.store.transactions).isZero();
    }

    private static void persist(
            Store store, M5RetiredBatchHistoryV2 history, M5RetiredBatchHistoryV2.Insertion insertion) {
        for (var node : insertion.nodes()) {
            store.seed(history.key(node.root().sha256()), node.bytes());
        }
    }

    private static Sha256Digest digest(String value) {
        return Sha256Digest.hash(CanonicalBytes.copyOf(value.getBytes(StandardCharsets.UTF_8)));
    }

    private static SourceRetirementBatch batch(int id) {
        var sources = List.of(new SourceProtectionIdentity(digest("source-" + id), 1, 1, 1, CAPABILITY));
        var draft = new SourceRetirementBatch(
                BINDING,
                digest("placeholder"),
                digest("before-" + id),
                digest("after-" + id),
                digest("transition-" + id),
                M4ReadControlCodecV1.calculateFallbackSetSha256(sources),
                1,
                1,
                CAPABILITY,
                sources);
        return new SourceRetirementBatch(
                BINDING,
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

    private static BindingReadSelector selector(List<SourceRetirementBatch> batches, long generation) {
        return new BindingReadSelector(
                BINDING,
                digest("view-" + generation),
                1,
                1,
                generation,
                SelectorMode.PREFERRED_ONLY,
                AdmissionState.ADMITTING,
                Optional.empty(),
                CAPABILITY,
                List.of(),
                batches);
    }

    private static RetiredSourceRetirementBatchTombstoneV1 tombstone(SourceRetirementBatch batch) {
        return M5RetentionCodecV1.finalizeRetiredBatch(new RetiredSourceRetirementBatchTombstoneV1(
                BatchMetadataStateV1.RETIRED_V1,
                BINDING,
                batch.batchIdSha256(),
                Sha256Digest.hash(M4ReadControlCodecV1.encodeBatch(batch)),
                digest("synthetic-reference-free"),
                new MetadataVersion(CanonicalBytes.copyOf(new byte[] {1})),
                digest("synthetic-predecessor"),
                CAPABILITY,
                digest("placeholder")));
    }

    private static final class Fixture {
        final Store store = new Store();
        final CanonicalControlMetadataStore facade =
                new M5BindingAuthorityControlMetadataStoreV1(store.canonical(), SELECTOR);
        final M5RetiredHistoryWriteBudgetV2 budget = new M5RetiredHistoryWriteBudgetV2(4, 1_000_000, 1_000_000_000);
        final M5RetiredBatchHistoryCoordinatorV2 coordinator = new M5RetiredBatchHistoryCoordinatorV2(store, budget);

        Fixture(int... ids) {
            var batches = Arrays.stream(ids)
                    .mapToObj(M5RetiredBatchHistoryV2Test::batch)
                    .toList();
            store.seed(
                    SELECTOR,
                    M5BindingAuthorityCodecV1.encodeAuthority(M5BindingAuthorityCodecV1.initial(selector(batches, 1))));
        }

        VersionedValue exact() {
            return store.values.get(SELECTOR);
        }

        BindingRetirementAuthorityV1 authority() {
            return M5BindingAuthorityCodecV1.decodeAuthority(exact().canonicalStoredBytes());
        }

        void retireFixture(int id) {
            var current = authority();
            var terminal = tombstone(batch(id));
            var slots = current.batchSlots().stream()
                    .map(slot -> slot.batchIdSha256().equals(terminal.batchIdSha256())
                            ? new BatchAuthoritySlotV1(
                                    slot.activationOrdinal(),
                                    BatchMetadataStateV1.RETIRED_V1,
                                    slot.batchIdSha256(),
                                    slot.fullBatchSha256(),
                                    Optional.empty(),
                                    Optional.of(terminal))
                            : slot)
                    .toList();
            var batches = slots.stream()
                    .filter(slot -> slot.state() == BatchMetadataStateV1.FULL_V1)
                    .map(BatchAuthoritySlotV1::fullBatch)
                    .toList();
            var old = current.selectorProjection();
            var projection = new BindingReadSelector(
                    old.binding(),
                    old.selectedViewSha256(),
                    old.ownerEpoch(),
                    old.readAdmissionEpoch(),
                    old.sourceGeneration(),
                    old.mode(),
                    old.admissionState(),
                    old.fallbackSetSha256(),
                    old.capability(),
                    old.pendingAnchors(),
                    batches);
            store.seed(
                    SELECTOR,
                    M5BindingAuthorityCodecV1.encodeAuthority(M5BindingAuthorityCodecV1.successor(
                            current, current.state(), projection, slots, Optional.empty(), List.of())));
        }

        Outcome fold(int id) {
            return coordinator
                    .fold(exact(), batch(id).batchIdSha256())
                    .toCompletableFuture()
                    .join();
        }
    }

    private static final class Store implements ExactMetadataTransactionStoreV1 {
        final Map<String, VersionedValue> values = new HashMap<>();
        long version;
        int transactions;
        boolean dropNextNode;
        boolean skipNextSelector;
        boolean loseNextSelectorResponse;
        Runnable beforeNextSelector;
        Runnable afterNextSelector;
        Runnable beforeRawSelectorCas;
        boolean holdNextNodeResponse;
        CompletableFuture<MutationOutcome> heldNodeResponse;

        VersionedValue seed(String key, CanonicalBytes bytes) {
            var stored = VersionedValue.of(
                    key,
                    bytes,
                    new MetadataVersion(CanonicalBytes.copyOf(
                            ByteBuffer.allocate(8).putLong(++version).array())));
            values.put(key, stored);
            return stored;
        }

        Optional<CanonicalBytes> bytes(String key) {
            return Optional.ofNullable(values.get(key)).map(VersionedValue::canonicalStoredBytes);
        }

        public CompletionStage<Optional<VersionedValue>> read(String key) {
            return CompletableFuture.completedFuture(Optional.ofNullable(values.get(key)));
        }

        public CompletionStage<MutationOutcome> compareAndSet(
                Optional<VersionedValue> expected, String key, CanonicalBytes candidate) {
            if (key.equals(SELECTOR)) {
                var before = beforeNextSelector;
                beforeNextSelector = null;
                if (before != null) {
                    before.run();
                }
                if (skipNextSelector) {
                    skipNextSelector = false;
                    return CompletableFuture.completedFuture(MutationOutcome.RESPONSE_UNKNOWN);
                }
            } else if (dropNextNode) {
                dropNextNode = false;
                return CompletableFuture.completedFuture(MutationOutcome.RESPONSE_UNKNOWN);
            }
            if (!Optional.ofNullable(values.get(key)).equals(expected)) {
                return CompletableFuture.completedFuture(MutationOutcome.DEFINITIVE_CONFLICT);
            }
            seed(key, candidate);
            if (!key.equals(SELECTOR) && holdNextNodeResponse) {
                holdNextNodeResponse = false;
                heldNodeResponse = new CompletableFuture<>();
                return heldNodeResponse;
            }
            boolean lost = key.equals(SELECTOR) && loseNextSelectorResponse;
            if (key.equals(SELECTOR)) {
                loseNextSelectorResponse = false;
                var after = afterNextSelector;
                afterNextSelector = null;
                if (after != null) {
                    after.run();
                }
            }
            return CompletableFuture.completedFuture(
                    lost ? MutationOutcome.RESPONSE_UNKNOWN : MutationOutcome.APPLIED_EXACT);
        }

        public CompletionStage<TransactionOutcome> conditionalTransaction(ExactTransaction transaction) {
            transactions++;
            throw new AssertionError("history folding must not require a multi-key transaction");
        }

        public boolean supportsAtomicMultiKeyTransactions() {
            return false;
        }

        CanonicalControlMetadataStore canonical() {
            return new CanonicalControlMetadataStore() {
                public Optional<CanonicalBytes> get(String key) {
                    return bytes(key);
                }

                public ControlMutationOutcome putIfAbsent(String key, CanonicalBytes bytes) {
                    if (values.containsKey(key)) {
                        return ControlMutationOutcome.DEFINITIVE_CONFLICT;
                    }
                    seed(key, bytes);
                    return ControlMutationOutcome.APPLIED;
                }

                public ControlMutationOutcome compareAndSet(
                        String key, Optional<CanonicalBytes> expected, CanonicalBytes bytes) {
                    var before = beforeRawSelectorCas;
                    beforeRawSelectorCas = null;
                    if (before != null) {
                        before.run();
                    }
                    if (!Store.this.bytes(key).equals(expected)) {
                        return ControlMutationOutcome.DEFINITIVE_CONFLICT;
                    }
                    seed(key, bytes);
                    return ControlMutationOutcome.APPLIED;
                }
            };
        }
    }
}
