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
import com.nereusstream.domain.bytes.CanonicalUtf8;
import com.nereusstream.domain.bytes.Sha256Digest;
import com.nereusstream.domain.identity.TopicBindingId;
import com.nereusstream.storage.object.control.CanonicalControlMetadataStore;
import com.nereusstream.storage.object.control.ControlMutationOutcome;
import com.nereusstream.storage.object.materialization.M5MaterializationAdmissionV1;
import com.nereusstream.storage.object.materialization.M5MaterializationCodecV1;
import com.nereusstream.storage.object.materialization.M5MaterializationCoordinatorV1;
import com.nereusstream.storage.object.materialization.M5MaterializationKeysV1;
import com.nereusstream.storage.object.materialization.M5MaterializationPlannerV1;
import com.nereusstream.storage.object.materialization.M5MaterializationRecordsV1;
import com.nereusstream.storage.object.materialization.M5MaterializationRecordsV1.MaterializationPlan;
import com.nereusstream.storage.object.materialization.M5PulsarCoverageV2;
import com.nereusstream.storage.object.materialization.M5PulsarFrontiersV2;
import com.nereusstream.storage.object.read.control.M4ReadControlCodecV1;
import com.nereusstream.storage.object.read.control.M4ReadControlKeysV1;
import com.nereusstream.storage.object.read.control.M4ReadControlRecordsV1.AdmissionState;
import com.nereusstream.storage.object.read.control.M4ReadControlRecordsV1.BindingIdentity;
import com.nereusstream.storage.object.read.control.M4ReadControlRecordsV1.BindingReadSelector;
import com.nereusstream.storage.object.read.control.M4ReadControlRecordsV1.CapabilityBinding;
import com.nereusstream.storage.object.read.control.M4ReadControlRecordsV1.SelectorMode;
import java.nio.ByteBuffer;
import java.util.Arrays;
import java.util.HashMap;
import java.util.List;
import java.util.Optional;
import org.junit.jupiter.api.Test;

/** Synthetic control fixtures exercise durable CAS semantics, not native source/owner qualification. */
class M5MaterializationWorkerV1Test {
    private static final BindingIdentity BINDING =
            new BindingIdentity(new TopicBindingId(digest("binding")), digest("incarnation"), digest("storage"));
    private static final String KEY = new M4ReadControlKeysV1(7, BINDING).selector();
    private static final BindingReadSelector INITIAL = selector(1, 1, digest("view"));

    @Test
    void pendingInventoryBindsTheFullPlanAndCancelsOnlyItsOriginalPreparedKeys() {
        var cell = new com.nereusstream.domain.protocol.PulsarProtocolCellIdentity(
                new com.nereusstream.domain.identity.DeploymentId(new com.nereusstream.domain.identity.Id128(1, 2)),
                new com.nereusstream.domain.identity.ReservationDomainId(
                        new com.nereusstream.domain.identity.Id128(3, 4)),
                new com.nereusstream.domain.identity.PulsarCellId(new com.nereusstream.domain.identity.Id128(5, 6)));
        var physical = new com.nereusstream.storage.api.lifecycle.PhysicalNamespaceAuthorityBindingV2(
                new com.nereusstream.storage.api.lifecycle.PhysicalResourceIdV2.Namespace(
                        com.nereusstream.storage.api.lifecycle.PhysicalResourceIdV2.ProviderKind.OBJECT_PROVIDER,
                        CanonicalUtf8.fromString("declared-service"),
                        CanonicalUtf8.fromString("declared-container")),
                new com.nereusstream.storage.api.lifecycle.MetadataNamespaceIdentityV2(
                        new com.nereusstream.domain.identity.Id128(7, 8), 0));
        var profile = new com.nereusstream.storage.object.materialization.M5CellProviderProfileV2(
                physical,
                cell,
                "declared-cell",
                new com.nereusstream.storage.object.materialization.M5CellProviderProfileV2.References(
                        "security", "credentials", "NONE", "quota", "operator", "delete"),
                "declared-adapter",
                digest("config"),
                64L * 1024 * 1024,
                65536,
                1000,
                1024);
        var store = new Store();
        var proposal = coordinator(store)
                .propose(
                        INITIAL,
                        profile.protocolCellSha256(),
                        profile.scopeId().digest(),
                        3,
                        digest("native-worker"),
                        digest("native-namespace"))
                .orElseThrow();
        var plan = plan(proposal);
        var worker = new M5MaterializationWorkerV1(
                proposal.identity(),
                digest("native-worker"),
                digest("native-namespace"),
                Sha256Digest.hash(M4ReadControlCodecV1.encodeSelector(INITIAL)),
                M5MaterializationCodecV1.sourceCutSha256(plan.sourceCut()),
                Sha256Digest.hash(M5MaterializationCodecV1.encodePlan(plan)),
                plan.taskIdSha256());
        var inventory = com.nereusstream.storage.object.materialization.M5PendingOutputInventoryV2.prepare(
                plan, profile, worker);
        assertThat(inventory.outputs()).hasSize(1);
        var encoded = inventory.encode();
        assertThat(com.nereusstream.storage.object.materialization.M5PendingOutputInventoryV2.decode(encoded))
                .isEqualTo(inventory);
        var key = inventory.outputs().get(0);
        var wrong = new com.nereusstream.storage.object.materialization.M5PendingOutputInventoryV2(
                worker,
                profile,
                inventory.state(),
                List.of(new com.nereusstream.storage.object.materialization.M5PendingOutputInventoryV2.Slot(
                        key.role(), key.ordinal(), key.objectKey() + "-wrong")));
        assertThatThrownBy(() -> wrong.requirePlan(plan)).hasMessageContaining("exact complete original plan");
        for (var body : List.of(
                CanonicalBytes.copyOf(Arrays.copyOf(encoded.toByteArray(), encoded.length() - 1)),
                CanonicalBytes.copyOf(Arrays.copyOf(encoded.toByteArray(), encoded.length() + 1)))) {
            assertThatThrownBy(() ->
                            com.nereusstream.storage.object.materialization.M5PendingOutputInventoryV2.decode(body))
                    .isInstanceOf(IllegalArgumentException.class);
        }
        var tasks = new M5MaterializationCoordinatorV1(store, 7, BINDING);
        assertThatThrownBy(() -> tasks.registerPendingOutputs(plan, inventory)).hasMessageContaining("complete stored");
        tasks.register(plan);
        assertThat(tasks.registerPendingOutputs(plan, inventory))
                .isEqualTo(M5MaterializationRecordsV1.PublicationOutcome.APPLIED_EXACT);
        store.loseResponse = true;
        assertThat(tasks.cancelUnstartedOutputs(plan, inventory))
                .isEqualTo(M5MaterializationRecordsV1.PublicationOutcome.EXISTING_EXACT);
        assertThat(tasks.cancelUnstartedOutputs(plan, inventory))
                .isEqualTo(M5MaterializationRecordsV1.PublicationOutcome.EXISTING_EXACT);
        assertThat(tasks.readPendingOutputs(plan.taskIdSha256())).contains(inventory.cancelUnstarted());
        assertThat(tasks.registerPendingOutputs(plan, inventory))
                .isEqualTo(M5MaterializationRecordsV1.PublicationOutcome.CONFLICT);
    }

    @Test
    void completePlanAdmissionRequiresOriginalValuesAndSharedCellReservation() {
        var store = new Store();
        var proposal = proposal(coordinator(store), INITIAL);
        var plan = plan(proposal);
        assertThatThrownBy(() -> proposal.admitPlan(plan)).hasMessageContaining("complete stored plan/source cut");
        var materialization = new M5MaterializationCoordinatorV1(store, 7, BINDING);
        assertThat(materialization.register(plan))
                .isEqualTo(M5MaterializationRecordsV1.PublicationOutcome.APPLIED_EXACT);
        assertThatThrownBy(() -> proposal.admitPlan(plan)).hasMessageContaining("shared Cell charge");
        var admission = new M5MaterializationAdmissionV1(store, digest("cell"));
        admission.install(new M5MaterializationAdmissionV1.Caps(2, 100, 1000, 2, 2, 2, 2));
        admission.reserve(plan, 100, 1);
        var key = new M5MaterializationKeysV1(7, BINDING).plan(plan.taskIdSha256());
        var original = store.values.put(
                key, CanonicalUtf8.fromString("wrong original plan").bytes());
        assertThatThrownBy(() -> proposal.admitPlan(plan)).hasMessageContaining("complete stored plan/source cut");
        store.values.put(key, original);
        store.loseResponse = true;
        var token = proposal.admitPlan(plan).orElseThrow();
        token.requireCurrentPlan(plan);
        var restarted = coordinator(store);
        assertThat(restarted.resumePlan(plan, digest("native-worker"), digest("native-namespace")))
                .get()
                .extracting(M5MaterializationWorkerCoordinatorV1.Admission::worker)
                .isEqualTo(token.worker());
        assertThat(restarted.resumePlan(plan, digest("other-worker"), digest("native-namespace")))
                .isEmpty();
        assertThat(token.worker().planRootSha256())
                .isEqualTo(Sha256Digest.hash(M5MaterializationCodecV1.encodePlan(plan)));
        var verified = new M5MaterializationRecordsV1.MaterializationTask(
                plan.taskIdSha256(),
                M5MaterializationRecordsV1.TaskState.OUTPUT_VERIFIED,
                M5MaterializationCodecV1.sourceCutSha256(plan.sourceCut()),
                plan.outputIdentitySha256(),
                Optional.of(digest("validation")),
                Optional.of(digest("generation")),
                Optional.of(digest("manifest")));
        store.values.put(
                new M5MaterializationKeysV1(7, BINDING).task(plan.taskIdSha256()),
                M5MaterializationCodecV1.encodeTask(verified));
        token.requireCurrentPlan(plan);
        assertThatThrownBy(() -> proposal.admitPlan(plan)).hasMessageContaining("task differs");
        admission.release(plan.taskIdSha256());
        assertThatThrownBy(() -> token.requireCurrentPlan(plan)).hasMessageContaining("shared Cell charge");
        assertThatThrownBy(() -> restarted.resumePlan(plan, digest("native-worker"), digest("native-namespace")))
                .hasMessageContaining("shared Cell charge");
    }

    @Test
    void terminalTaskAndDifferentCellChargeCannotBeUsedForACompletePlanGrant() {
        var store = new Store();
        var proposal = proposal(coordinator(store), INITIAL);
        var plan = plan(proposal);
        new M5MaterializationCoordinatorV1(store, 7, BINDING).register(plan);
        var admission = new M5MaterializationAdmissionV1(store, digest("cell"));
        var caps = new M5MaterializationAdmissionV1.Caps(2, 100, 1000, 2, 2, 2, 2);
        admission.install(caps);
        store.values.put(
                M5MaterializationAdmissionV1.stateKey(digest("cell")),
                M5MaterializationAdmissionV1.encode(new M5MaterializationAdmissionV1.State(
                        2,
                        caps,
                        List.of(new M5MaterializationAdmissionV1.Reservation(
                                plan.taskIdSha256(), 10, 100, 1, 1, 0, 1)))));
        assertThatThrownBy(() -> proposal.admitPlan(plan)).hasMessageContaining("charge differs");
        store.values.put(
                M5MaterializationAdmissionV1.stateKey(digest("cell")),
                M5MaterializationAdmissionV1.encode(new M5MaterializationAdmissionV1.State(1, caps, List.of())));
        admission.reserve(plan, 100, 1);
        var token = proposal.admitPlan(plan).orElseThrow();
        var tasks = new M5MaterializationCoordinatorV1(store, 7, BINDING);
        store.loseResponse = true;
        assertThat(tasks.cancelUnstarted(plan)).isEqualTo(M5MaterializationRecordsV1.PublicationOutcome.EXISTING_EXACT);
        assertThat(tasks.cancelUnstarted(plan)).isEqualTo(M5MaterializationRecordsV1.PublicationOutcome.EXISTING_EXACT);
        assertThatThrownBy(() -> token.requireCurrentPlan(plan)).hasMessageContaining("task differs");
        assertThatThrownBy(() -> proposal.admitPlan(plan)).hasMessageContaining("task differs");
    }

    private static MaterializationPlan plan(M5MaterializationWorkerCoordinatorV1.Proposal proposal) {
        var coverage = M5PulsarCoverageV2.of(
                BINDING,
                INITIAL.selectedViewSha256(),
                List.of(
                        new M5PulsarCoverageV2.ChainLedger(3, false, 1, digest("sealed")),
                        new M5PulsarCoverageV2.ChainLedger(7, true, 1, digest("active"))),
                List.of(new M5PulsarCoverageV2.EntryRange(3, 0, 1)));
        var sources = List.of(new M5MaterializationRecordsV1.SourceExtent(
                M5MaterializationRecordsV1.SourceKind.BOOKKEEPER_LEDGER,
                digest("sealed"),
                M5MaterializationRecordsV1.ProtocolCoverage.pulsar(coverage),
                "bookkeeper-ledger:3",
                9,
                1,
                42,
                42,
                digest("body"),
                Optional.empty(),
                Optional.of(digest("native-source")),
                digest("format"),
                digest("encryption"),
                false,
                false,
                List.of(BINDING.bindingId().digest()),
                Optional.of(digest("entry"))));
        var cut = new M5MaterializationRecordsV1.MaterializationSourceCut(
                proposal.identity(),
                INITIAL,
                Sha256Digest.hash(M4ReadControlCodecV1.encodeSelector(INITIAL)),
                INITIAL.selectedViewSha256(),
                M5MaterializationRecordsV1.ProtocolCoverage.pulsar(coverage),
                -1,
                -1,
                -1,
                -1,
                -1,
                digest("protocol"),
                digest("recovery"),
                digest("policy"),
                digest("format-policy"),
                M5MaterializationCodecV1.calculateSourceSetSha256(sources),
                sources,
                Optional.of(new M5PulsarFrontiersV2(
                        new M5PulsarFrontiersV2.LedgerBoundary(7, 1), Optional.empty(), Optional.empty())));
        return new M5MaterializationPlannerV1()
                .plan(
                        cut,
                        new M5MaterializationPlannerV1.PlanningPolicy(
                                M5MaterializationRecordsV1.PayloadKind.PULSAR_ENTRY_PRESERVING_V1,
                                true,
                                digest("encryption"),
                                digest("compression"),
                                digest("checksum"),
                                List.of(),
                                "declared-cell"));
    }

    @Test
    void legacyMigrationAndRestartKeepAnIndependentMonotonicWorkerEpoch() {
        var store = new Store(M4ReadControlCodecV1.encodeSelector(INITIAL));
        var coordinator = coordinator(store);
        assertThatThrownBy(() -> proposal(coordinator, INITIAL)).hasMessageContaining("migration first");
        store.loseResponse = true;
        assertThat(coordinator.migrateLegacyAuthority()).isTrue();
        var migrated = store.get(KEY).orElseThrow();
        assertThat(M5BindingAuthorityCodecV1.projectSelector(migrated)).isEqualTo(INITIAL);
        assertThat(M5BindingAuthorityCodecV1.decodeAuthority(migrated).wireVersion())
                .isEqualTo(2);
        var first = admit(proposal(coordinator, INITIAL), "first");
        assertThat(first.worker().identity().workerEpoch()).isEqualTo(1);
        var before = store.get(KEY).orElseThrow();
        assertThat(M5BindingAuthorityCodecV1.decodeAuthority(before).wireVersion())
                .isEqualTo(4);
        assertThat(M5BindingAuthorityCodecV1.encodeAuthority(M5BindingAuthorityCodecV1.decodeAuthority(before)))
                .isEqualTo(before);
        var restarted = coordinator(store);
        var next = admit(proposal(restarted, INITIAL), "next");
        assertThat(next.worker().identity().workerEpoch()).isEqualTo(2);
        assertThat(next.worker().identity().ownerEpoch()).isEqualTo(1);
        assertThat(next.worker().identity().storageFence()).isEqualTo(3);
        assertThat(next.isCurrent()).isTrue();
        assertThat(first.isCurrent()).isFalse();
        assertThatThrownBy(first::requireCurrent).hasMessageContaining("stale");
        assertThat(store.get(KEY).orElseThrow().length()).isEqualTo(before.length());
    }

    @Test
    void competingProposalsAndUnappliedUnknownNeverGrantTheLosingTask() {
        var store = new Store();
        var coordinator = coordinator(store);
        var left = proposal(coordinator, INITIAL);
        var right = proposal(coordinator, INITIAL);
        store.dropCas = true;
        assertThat(left.admit(digest("cut"), digest("plan"), digest("left"))).isEmpty();
        store.dropCas = false;
        var winner = admit(left, "left");
        assertThat(right.admit(digest("cut"), digest("plan"), digest("right"))).isEmpty();
        assertThat(winner.isCurrent()).isTrue();
        store.failRead = true;
        assertThatThrownBy(winner::isCurrent).hasMessage("read unknown");
    }

    @Test
    void replacementWorkerWinsTheSameCasAgainstALatePublisherWithIdenticalSelectorProjection() {
        var store = new Store();
        var coordinator = coordinator(store);
        var first = admit(proposal(coordinator, INITIAL), "first");
        var tasks = new M5TaskSelectionCoordinatorV2(store, 7, BINDING);
        var late = tasks.prepare(first.worker().taskIdSha256(), first.worker()).orElseThrow();
        var replacement = admit(proposal(coordinator, INITIAL), "replacement");
        assertThat(M5BindingAuthorityCodecV1.projectSelector(store.get(KEY).orElseThrow()))
                .isEqualTo(INITIAL);
        assertThat(tasks.prepare(first.worker().taskIdSha256(), first.worker())).isEmpty();
        assertThat(select(late, selector(1, 2, digest("late-output"))))
                .isEqualTo(ControlMutationOutcome.DEFINITIVE_CONFLICT);
        var bypass = tasks.prepare(first.worker().taskIdSha256()).orElseThrow();
        assertThatThrownBy(() -> select(bypass, selector(1, 2, digest("bypass"))))
                .hasMessageContaining("admitted worker/selector");
        var fresh = tasks.prepare(replacement.worker().taskIdSha256(), replacement.worker())
                .orElseThrow();
        assertThat(select(fresh, selector(1, 2, digest("selected")))).isEqualTo(ControlMutationOutcome.APPLIED);
        assertThat(tasks.readDecision(replacement.worker().taskIdSha256())
                        .orElseThrow()
                        .selectedOutput())
                .contains(digest("selected"));
        assertThat(tasks.readDecision(first.worker().taskIdSha256())).isEmpty();
    }

    @Test
    void ownerSelectorSuccessorPreservesTheGrantButInvalidatesItsAdmission() {
        var store = new Store();
        var coordinator = coordinator(store);
        var admission = admit(proposal(coordinator, INITIAL), "first");
        var obsolete = proposal(coordinator, INITIAL);
        var before = M5BindingAuthorityCodecV1.decodeAuthority(store.get(KEY).orElseThrow());
        var nextOwner = selector(2, 2, digest("takeover"));
        var next = M5BindingAuthorityCodecV1.selectorSuccessor(before, nextOwner);
        M5BindingAuthorityCodecV1.requireWorkerTransition(before, next);
        store.values.put(KEY, M5BindingAuthorityCodecV1.encodeAuthority(next));
        assertThat(next.materializationWorker()).contains(admission.worker());
        assertThat(admission.isCurrent()).isFalse();
        assertThat(obsolete.admit(digest("cut"), digest("plan"), digest("obsolete")))
                .isEmpty();
        var replacement = admit(proposal(coordinator, nextOwner), "second");
        assertThat(replacement.worker().identity().workerEpoch()).isEqualTo(2);
        assertThat(replacement.worker().identity().ownerEpoch()).isEqualTo(2);
    }

    @Test
    void newWorkerCannotChangeImmutableScopeOrStorageAndWireCannotDowngrade() {
        var store = new Store();
        var coordinator = coordinator(store);
        admit(proposal(coordinator, INITIAL), "first");
        var foreign = coordinator
                .propose(
                        INITIAL,
                        digest("other-cell"),
                        digest("scope"),
                        3,
                        digest("native-worker"),
                        digest("native-namespace"))
                .orElseThrow();
        assertThatThrownBy(() -> admit(foreign, "foreign")).hasMessageContaining("immutable native Cell/storage scope");
        var storage = coordinator
                .propose(
                        INITIAL,
                        digest("cell"),
                        digest("scope"),
                        4,
                        digest("native-worker"),
                        digest("native-namespace"))
                .orElseThrow();
        assertThatThrownBy(() -> admit(storage, "foreign")).hasMessageContaining("immutable native Cell/storage scope");
        var current = M5BindingAuthorityCodecV1.decodeAuthority(store.get(KEY).orElseThrow());
        var legacy = M5BindingAuthorityCodecV1.initial(INITIAL);
        assertThatThrownBy(() -> M5BindingAuthorityCodecV1.requireWorkerTransition(current, legacy))
                .hasMessageContaining("downgrade");
    }

    @Test
    void workerWireRejectsUnknownVersionsTruncationTrailingBytesAndZeroFences() {
        var store = new Store();
        var bytes =
                admit(proposal(coordinator(store), INITIAL), "task").worker().encode();
        assertThat(bytes.length()).isEqualTo(M5MaterializationWorkerV1.BYTES);
        assertThat(M5MaterializationWorkerV1.decode(bytes).encode()).isEqualTo(bytes);
        for (int length = 0; length < bytes.length(); length++) {
            var truncated = CanonicalBytes.copyOf(Arrays.copyOf(bytes.toByteArray(), length));
            assertThatThrownBy(() -> M5MaterializationWorkerV1.decode(truncated))
                    .isInstanceOf(IllegalArgumentException.class);
        }
        var longer = CanonicalBytes.copyOf(Arrays.copyOf(bytes.toByteArray(), bytes.length() + 1));
        assertThatThrownBy(() -> M5MaterializationWorkerV1.decode(longer)).isInstanceOf(IllegalArgumentException.class);
        byte[] unknown = bytes.toByteArray();
        ByteBuffer.wrap(unknown).putInt(4, 2);
        assertThatThrownBy(() -> M5MaterializationWorkerV1.decode(CanonicalBytes.copyOf(unknown)))
                .hasMessageContaining("magic/version");
        byte[] zero = bytes.toByteArray();
        ByteBuffer.wrap(zero).putLong(8 + 5 * 32 + 8, 0);
        assertThatThrownBy(() -> M5MaterializationWorkerV1.decode(CanonicalBytes.copyOf(zero)))
                .hasMessageContaining("identity fence");
    }

    private static M5MaterializationWorkerCoordinatorV1 coordinator(Store store) {
        return new M5MaterializationWorkerCoordinatorV1(store, 7, BINDING);
    }

    private static M5MaterializationWorkerCoordinatorV1.Proposal proposal(
            M5MaterializationWorkerCoordinatorV1 coordinator, BindingReadSelector selector) {
        return coordinator
                .propose(
                        selector,
                        digest("cell"),
                        digest("scope"),
                        3,
                        digest("native-worker"),
                        digest("native-namespace"))
                .orElseThrow();
    }

    private static M5MaterializationWorkerCoordinatorV1.Admission admit(
            M5MaterializationWorkerCoordinatorV1.Proposal proposal, String task) {
        return proposal.admit(digest("cut"), digest("plan"), digest(task)).orElseThrow();
    }

    private static ControlMutationOutcome select(
            M5TaskSelectionCoordinatorV2.Attempt attempt, BindingReadSelector after) {
        var store =
                new M5BindingAuthorityControlMetadataStoreV1(attempt.selectingControl(after.selectedViewSha256()), KEY);
        return store.compareAndSet(
                KEY,
                Optional.of(M4ReadControlCodecV1.encodeSelector(attempt.selector())),
                M4ReadControlCodecV1.encodeSelector(after));
    }

    private static BindingReadSelector selector(long owner, long generation, Sha256Digest view) {
        return new BindingReadSelector(
                BINDING,
                view,
                owner,
                owner,
                generation,
                SelectorMode.PREFERRED_ONLY,
                AdmissionState.ADMITTING,
                Optional.empty(),
                new CapabilityBinding(1, digest("capability")),
                List.of(),
                List.of());
    }

    private static Sha256Digest digest(String value) {
        return Sha256Digest.hash(CanonicalUtf8.fromString(value).bytes());
    }

    private static final class Store implements CanonicalControlMetadataStore {
        private final HashMap<String, CanonicalBytes> values = new HashMap<>();
        private boolean loseResponse;
        private boolean dropCas;
        private boolean failRead;

        private Store() {
            this(M5BindingAuthorityCodecV1.encodeAuthority(M5BindingAuthorityCodecV1.initial(INITIAL)));
        }

        private Store(CanonicalBytes initial) {
            values.put(KEY, initial);
        }

        public Optional<CanonicalBytes> get(String key) {
            if (failRead) {
                throw new IllegalStateException("read unknown");
            }
            return Optional.ofNullable(values.get(key));
        }

        public ControlMutationOutcome putIfAbsent(String key, CanonicalBytes bytes) {
            values.putIfAbsent(key, bytes);
            return ControlMutationOutcome.APPLIED;
        }

        public ControlMutationOutcome compareAndSet(String key, Optional<CanonicalBytes> before, CanonicalBytes after) {
            if (!get(key).equals(before)) {
                return ControlMutationOutcome.DEFINITIVE_CONFLICT;
            }
            if (dropCas) {
                return ControlMutationOutcome.RESPONSE_UNKNOWN;
            }
            values.put(key, after);
            return loseResponse ? ControlMutationOutcome.RESPONSE_UNKNOWN : ControlMutationOutcome.APPLIED;
        }
    }
}
