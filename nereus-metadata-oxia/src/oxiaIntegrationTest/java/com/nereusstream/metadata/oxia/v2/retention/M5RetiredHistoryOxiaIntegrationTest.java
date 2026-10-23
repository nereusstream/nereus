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
import com.nereusstream.metadata.oxia.v2.objectwal.OxiaCanonicalControlMetadataStore;
import com.nereusstream.metadata.spi.retention.ExactMetadataTransactionStoreV1;
import com.nereusstream.metadata.spi.retention.ExactMetadataTransactionStoreV1.MutationOutcome;
import com.nereusstream.metadata.spi.retention.ExactMetadataTransactionStoreV1.VersionedValue;
import com.nereusstream.storage.object.control.CanonicalControlMetadataStore;
import com.nereusstream.storage.object.control.ControlMutationOutcome;
import com.nereusstream.storage.object.materialization.M5MaterializationAdmissionV1;
import com.nereusstream.storage.object.materialization.M5MaterializationCodecV1;
import com.nereusstream.storage.object.materialization.M5MaterializationCoordinatorV1;
import com.nereusstream.storage.object.materialization.M5MaterializationKeysV1;
import com.nereusstream.storage.object.materialization.M5MaterializationPlannerV1;
import com.nereusstream.storage.object.materialization.M5MaterializationRecordsV1;
import com.nereusstream.storage.object.materialization.M5MaterializationRecordsV1.IdentityEnvelope;
import com.nereusstream.storage.object.materialization.M5PulsarCoverageV2;
import com.nereusstream.storage.object.read.control.M4ReadControlCodecV1;
import com.nereusstream.storage.object.read.control.M4ReadControlKeysV1;
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
import com.nereusstream.storage.object.retention.M5BindingAuthorityRecordsV1.BatchAuthoritySlotV1;
import com.nereusstream.storage.object.retention.M5BindingAuthorityRecordsV1.BindingRetirementAuthorityV1;
import com.nereusstream.storage.object.retention.M5BindingAuthorityRecordsV1.ReferenceMutationTicketV1;
import com.nereusstream.storage.object.retention.M5BindingRetirementCoordinatorV1;
import com.nereusstream.storage.object.retention.M5LogicalTrimCoordinatorV1;
import com.nereusstream.storage.object.retention.M5RetentionCodecV1;
import com.nereusstream.storage.object.retention.M5RetentionKeysV1;
import com.nereusstream.storage.object.retention.M5RetentionRecordsV1;
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
    void declaredTypedRetentionRecordsUseActualScopedOxiaAndSurviveLostTrimReply() throws Exception {
        try (Fixture fixture = Fixture.create(1)) {
            var cell = digest(fixture.root + "/typed-retention-cell");
            var route = new OxiaBindingLifecycleMetadataStoreV2(fixture.faults, fixture.root, 7, fixture.binding, cell);
            var stored = await(route.read(fixture.selectorKey)).orElseThrow();
            var fact = new AuthorityFactV1(stored.key(), stored.metadataVersion(), stored.canonicalStoredSha256());
            var snapshot = declaredTypedFloor(
                    fixture.binding,
                    cell,
                    fact,
                    new M5PulsarCoverageV2.Position(91, 0),
                    new M5PulsarCoverageV2.Position(91, 3));
            var keys = new M5RetentionKeysV1(7, fixture.binding);
            String snapshotKey = keys.floorSnapshot(snapshot.snapshotRootSha256());
            assertThat(await(route.compareAndSet(
                            Optional.empty(), snapshotKey, M5RetentionCodecV1.encodeSnapshot(snapshot))))
                    .isEqualTo(MutationOutcome.APPLIED_EXACT);
            assertThat(await(fixture.nativeStore.read(route.nativeKey(snapshotKey)))
                            .orElseThrow()
                            .canonicalStoredBytes())
                    .isEqualTo(M5RetentionCodecV1.encodeSnapshot(snapshot));
            fixture.faults.loseNextTrimResponse = true;
            var applied = await(new M5LogicalTrimCoordinatorV1(route).advance(keys.trimFrontier(), snapshot));
            assertThat(applied.outcome()).isEqualTo(M5LogicalTrimCoordinatorV1.Outcome.APPLIED_EXACT);
            assertThat(fixture.faults.loseNextTrimResponse).isFalse();
            assertThat(fixture.faults.lastMutationKey).isEqualTo(route.nativeKey(fixture.selectorKey));
            var coverage = M5MaterializationRecordsV1.ProtocolCoverage.pulsar(
                    snapshot.pulsarContext().orElseThrow().coverage());
            var rows = java.util.Arrays.stream(ReferenceKindV1.values())
                    .map(kind -> new M5RetentionRecordsV1.ReferenceObservationV1(
                            kind,
                            fact,
                            digest("declared-storage-target"),
                            coverage,
                            M5RetentionRecordsV1.ReferenceDispositionV1.ABSENT,
                            true))
                    .toList();
            var scans = java.util.Arrays.stream(ReferenceKindV1.values())
                    .map(kind -> new M5RetentionRecordsV1.ReferenceScanSummaryV1(kind, 1, 1, 64, true))
                    .toList();
            var proof = M5RetentionCodecV1.finalizeProof(new M5RetentionRecordsV1.ReferenceFreeProofV1(
                    snapshot.identity(),
                    ReferenceTargetKindV1.UNSELECTED_OUTPUT,
                    digest("declared-storage-target"),
                    coverage,
                    fact,
                    fact,
                    fact,
                    snapshot.snapshotRootSha256(),
                    M5RetentionCodecV1.calculateObservationsRoot(rows),
                    List.of(),
                    fact,
                    fact,
                    fact,
                    fact,
                    1,
                    2,
                    scans,
                    rows,
                    digest("placeholder")));
            String proofKey = keys.referenceFreeProof(proof.proofSha256());
            assertThat(await(route.compareAndSet(
                            Optional.empty(), proofKey, M5RetentionCodecV1.encodeReferenceFreeProof(proof))))
                    .isEqualTo(MutationOutcome.APPLIED_EXACT);
            fixture.reconnect();
            var reopened =
                    new OxiaBindingLifecycleMetadataStoreV2(fixture.client, fixture.root, 7, fixture.binding, cell);
            assertThat(M5RetentionCodecV1.decodeSnapshot(
                            await(reopened.read(snapshotKey)).orElseThrow().canonicalStoredBytes()))
                    .isEqualTo(snapshot);
            assertThat(M5RetentionCodecV1.decodeReferenceFreeProof(
                            await(reopened.read(proofKey)).orElseThrow().canonicalStoredBytes()))
                    .isEqualTo(proof);
            assertThat(await(reopened.readLogicalTrimFrontier()).orElseThrow())
                    .isEqualTo(applied.exactFrontier().orElseThrow());
            assertThat(await(new M5LogicalTrimCoordinatorV1(reopened).advance(keys.trimFrontier(), snapshot))
                            .outcome())
                    .isEqualTo(M5LogicalTrimCoordinatorV1.Outcome.EXISTING_EXACT);
            var currentAuthority = await(reopened.read(fixture.selectorKey)).orElseThrow();
            var currentFact = new AuthorityFactV1(
                    currentAuthority.key(),
                    currentAuthority.metadataVersion(),
                    currentAuthority.canonicalStoredSha256());
            var next = declaredTypedFloor(
                    fixture.binding,
                    cell,
                    currentFact,
                    snapshot.minimumPulsarFloor(),
                    new M5PulsarCoverageV2.Position(7, 2));
            await(reopened.compareAndSet(
                    Optional.empty(),
                    keys.floorSnapshot(next.snapshotRootSha256()),
                    M5RetentionCodecV1.encodeSnapshot(next)));
            var advanced = await(new M5LogicalTrimCoordinatorV1(reopened).advance(keys.trimFrontier(), next));
            assertThat(advanced.exactFrontier().orElseThrow().generation()).isEqualTo(2);
            assertThat(advanced.exactFrontier()
                            .orElseThrow()
                            .pulsarContext()
                            .orElseThrow()
                            .newFrontier())
                    .isEqualTo(next.minimumPulsarFloor());
        }
    }

    @Test
    void actualFloorWriterCannotCrossTheFinalInlineTrimScan() throws Exception {
        try (var fixture = Fixture.create(1)) {
            var cell = digest(fixture.root + "/trim-race-cell");
            var route = new OxiaBindingLifecycleMetadataStoreV2(fixture.faults, fixture.root, 7, fixture.binding, cell);
            var keys = new M5RetentionKeysV1(7, fixture.binding);
            var headKey = new M4ReadControlKeysV1(7, fixture.binding).proofHead();
            var head = M4ReadControlCodecV1.encodeHead(
                    new com.nereusstream.storage.object.read.control.M4ReadControlRecordsV1.QuiescenceProofHead(
                            fixture.binding, 1, List.of(), List.of()));
            assertThat(route.rawControlMetadata().putIfAbsent(headKey, head)).isEqualTo(ControlMutationOutcome.APPLIED);
            var before = await(route.read(headKey)).orElseThrow();
            var snapshot = declaredTypedFloor(
                    fixture.binding,
                    cell,
                    new AuthorityFactV1(before.key(), before.metadataVersion(), before.canonicalStoredSha256()),
                    new M5PulsarCoverageV2.Position(91, 0),
                    new M5PulsarCoverageV2.Position(91, 3));
            await(route.compareAndSet(
                    Optional.empty(),
                    keys.floorSnapshot(snapshot.snapshotRootSha256()),
                    M5RetentionCodecV1.encodeSnapshot(snapshot)));
            fixture.faults.holdBeforeInlineTrim = true;
            var advance = new M5LogicalTrimCoordinatorV1(route)
                    .advance(keys.trimFrontier(), snapshot)
                    .toCompletableFuture();
            fixture.faults.trimDispatchReached.get(30, TimeUnit.SECONDS);
            var fenced = await(route.read(fixture.selectorKey)).orElseThrow();
            assertThat(M5BindingAuthorityCodecV1.decodeAuthority(fenced.canonicalStoredBytes())
                            .state())
                    .isEqualTo(
                            com.nereusstream.storage.object.retention.M5BindingAuthorityRecordsV1
                                    .BindingAuthorityStateV1.LOGICAL_TRIM_SCAN_FENCED_V2);
            var changed = M4ReadControlCodecV1.encodeHead(
                    new com.nereusstream.storage.object.read.control.M4ReadControlRecordsV1.QuiescenceProofHead(
                            fixture.binding, 2, List.of(), List.of()));
            assertThat(await(route.compareAndSet(Optional.of(before), headKey, changed)))
                    .isEqualTo(MutationOutcome.DEFINITIVE_CONFLICT);
            assertThat(await(route.read(headKey))).contains(before);
            fixture.faults.trimDispatchReleased.complete(null);
            assertThat(advance.get(30, TimeUnit.SECONDS).outcome())
                    .isEqualTo(M5LogicalTrimCoordinatorV1.Outcome.APPLIED_EXACT);
            assertThat(await(route.readLogicalTrimFrontier())
                            .orElseThrow()
                            .pulsarContext()
                            .orElseThrow()
                            .newFrontier())
                    .isEqualTo(snapshot.minimumPulsarFloor());
            assertThat(await(fixture.nativeStore.read(route.nativeKey(keys.trimFrontier()))))
                    .isEmpty();
        }
    }

    @Test
    void actualFloorWriteUnknownKeepsItsTicketAcrossReconnectWithoutRedispatch() throws Exception {
        try (var fixture = Fixture.create(1)) {
            var route = fixture.route;
            var key = new M4ReadControlKeysV1(7, fixture.binding).proofHead();
            var head = M4ReadControlCodecV1.encodeHead(
                    new com.nereusstream.storage.object.read.control.M4ReadControlRecordsV1.QuiescenceProofHead(
                            fixture.binding, 1, List.of(), List.of()));
            assertThat(route.rawControlMetadata().putIfAbsent(key, head)).isEqualTo(ControlMutationOutcome.APPLIED);
            var before = await(route.read(key)).orElseThrow();
            var next = M4ReadControlCodecV1.encodeHead(
                    new com.nereusstream.storage.object.read.control.M4ReadControlRecordsV1.QuiescenceProofHead(
                            fixture.binding, 2, List.of(), List.of()));
            fixture.faults.loseMutableReplyKey = route.nativeKey(key);
            assertThat(await(route.compareAndSet(Optional.of(before), key, next)))
                    .isEqualTo(MutationOutcome.RESPONSE_UNKNOWN);
            assertThat(fixture.faults.mutableWrites.get()).isEqualTo(1);
            assertThat(M5BindingAuthorityCodecV1.decodeAuthority(await(route.read(fixture.selectorKey))
                                    .orElseThrow()
                                    .canonicalStoredBytes())
                            .trimMutationTickets())
                    .hasSize(1);
            fixture.reconnect();
            await(fixture.route.reconcileLogicalTrimWrites());
            assertThat(M5BindingAuthorityCodecV1.decodeAuthority(await(fixture.route.read(fixture.selectorKey))
                                    .orElseThrow()
                                    .canonicalStoredBytes())
                            .trimMutationTickets())
                    .isEmpty();
            assertThat(await(fixture.route.read(key)).orElseThrow().canonicalStoredBytes())
                    .isEqualTo(next);
            assertThat(fixture.faults.mutableWrites.get()).isZero();
        }
    }

    /** Declared protocol values test real metadata storage only; this is not a native floor/reference producer. */
    private static M5RetentionRecordsV1.RetentionFloorSnapshotV1 declaredTypedFloor(
            BindingIdentity binding,
            Sha256Digest cell,
            AuthorityFactV1 fact,
            M5PulsarCoverageV2.Position prior,
            M5PulsarCoverageV2.Position minimum) {
        var coverage = M5PulsarCoverageV2.of(
                binding,
                digest("declared-storage-chain"),
                List.of(
                        new M5PulsarCoverageV2.ChainLedger(91, false, 3, digest("declared91")),
                        new M5PulsarCoverageV2.ChainLedger(7, false, 4, digest("declared7"))),
                List.of(new M5PulsarCoverageV2.EntryRange(91, 0, 3), new M5PulsarCoverageV2.EntryRange(7, 0, 4)));
        var identity = new M5MaterializationRecordsV1.IdentityEnvelope(
                cell, digest("declared-provider"), binding, 1, 1, 1, CAPABILITY);
        var rows = java.util.Arrays.stream(M5RetentionRecordsV1.FloorClassV1.values())
                .map(kind -> new M5RetentionRecordsV1.RetentionFloorObservationV1(
                        kind,
                        fact,
                        M5MaterializationRecordsV1.PositionDomain.PULSAR_ENTRY,
                        -1,
                        true,
                        true,
                        Optional.of(minimum)))
                .toList();
        return M5RetentionCodecV1.finalizeSnapshot(new M5RetentionRecordsV1.RetentionFloorSnapshotV1(
                identity,
                M5MaterializationRecordsV1.PositionDomain.PULSAR_ENTRY,
                1,
                -1,
                digest("declared-policy"),
                fact,
                fact,
                1,
                4096,
                rows,
                digest("placeholder"),
                Optional.of(new M5RetentionRecordsV1.PulsarFloorContextV2(coverage, prior))));
    }

    @Test
    void completeMaterializationPlanAndSharedCellAdmissionSurviveActualReconnect() throws Exception {
        try (Fixture fixture = Fixture.create(1)) {
            var cell = digest(fixture.root + "/actual-cell-route");
            var once = new java.util.concurrent.atomic.AtomicBoolean(true);
            var nativeClient = new AsyncOxiaConditionalClient(fixture.client);
            OxiaConditionalClient lostPlanReply = new OxiaConditionalClient() {
                public CompletionStage<Optional<AuthorityRecord>> read(String key) {
                    return nativeClient.read(key);
                }

                public CompletionStage<Void> createIfAbsent(String key, CanonicalBytes value) {
                    return nativeClient
                            .createIfAbsent(key, value)
                            .thenCompose(ignored -> key.contains("/plans/") && once.compareAndSet(true, false)
                                    ? CompletableFuture.failedFuture(new java.util.concurrent.TimeoutException(
                                            "actual original plan write reply was lost"))
                                    : CompletableFuture.completedFuture(null));
                }

                public CompletionStage<Void> compareAndSet(String key, CanonicalBytes value, long version) {
                    return nativeClient.compareAndSet(key, value, version);
                }
            };
            var route = new OxiaBindingLifecycleMetadataStoreV2(lostPlanReply, fixture.root, 7, fixture.binding, cell);
            var plan = materializationPlan(fixture.binding, cell, fixture.selector(List.of(), 1));
            var coordinator = new M5MaterializationCoordinatorV1(route.rawControlMetadata(), 7, fixture.binding);
            assertThat(coordinator.register(plan))
                    .isEqualTo(M5MaterializationRecordsV1.PublicationOutcome.APPLIED_EXACT);
            assertThat(once).isFalse();
            var admission = new M5MaterializationAdmissionV1(route.rawControlMetadata(), cell);
            assertThat(admission.install(new M5MaterializationAdmissionV1.Caps(1, 100, 1000, 2, 2, 2, 2)))
                    .isEqualTo(M5MaterializationAdmissionV1.Outcome.APPLIED_EXACT);
            assertThat(admission.reserve(plan, 100, 1)).isEqualTo(M5MaterializationAdmissionV1.Outcome.APPLIED_EXACT);
            fixture.reconnect();
            var reopened =
                    new OxiaBindingLifecycleMetadataStoreV2(fixture.client, fixture.root, 7, fixture.binding, cell);
            assertThat(new M5MaterializationCoordinatorV1(reopened.rawControlMetadata(), 7, fixture.binding)
                            .readPlan(plan.taskIdSha256()))
                    .contains(plan);
            var other = new BindingIdentity(
                    new TopicBindingId(digest(fixture.root + "/sibling")),
                    digest("sibling-incarnation"),
                    digest("sibling-storage"));
            var sibling = new OxiaBindingLifecycleMetadataStoreV2(fixture.client, fixture.root, 7, other, cell);
            var second = new M5MaterializationAdmissionV1(sibling.rawControlMetadata(), cell);
            var otherSelector = new BindingReadSelector(
                    other,
                    digest("sibling-view"),
                    11,
                    13,
                    1,
                    SelectorMode.PREFERRED_ONLY,
                    AdmissionState.ADMITTING,
                    Optional.empty(),
                    CAPABILITY,
                    List.of(),
                    List.of());
            assertThat(second.reserve(materializationPlan(other, cell, otherSelector), 100, 1))
                    .isEqualTo(M5MaterializationAdmissionV1.Outcome.REJECTED_CAP);
            String planKey = new M5MaterializationKeysV1(7, fixture.binding).plan(plan.taskIdSha256());
            var actual = await(reopened.read(planKey)).orElseThrow();
            assertThatThrownBy(
                            () -> reopened.compareAndSet(Optional.of(actual), planKey, actual.canonicalStoredBytes()))
                    .hasMessageContaining("create-only");
            assertThat(second.read().orElseThrow().reservations()).hasSize(1);
            System.out.println("M5_REAL_OXIA_COMPLETE_PLAN actualWriteThenLostReply=true "
                    + "reconnectExactPlan=true siblingSharesCellCapacity=true immutablePlan=true "
                    + "nativeWorkerAuthority=false");
        }
    }

    private static M5MaterializationRecordsV1.MaterializationPlan materializationPlan(
            BindingIdentity binding, Sha256Digest cell, BindingReadSelector selected) {
        var identity = new IdentityEnvelope(
                cell, digest("declared-provider"), binding, selected.ownerEpoch(), 1, 1, CAPABILITY);
        var coverage = new M5MaterializationRecordsV1.ProtocolCoverage(
                M5MaterializationRecordsV1.PositionDomain.KAFKA_OFFSET, 0, 3);
        var source = new M5MaterializationRecordsV1.SourceExtent(
                M5MaterializationRecordsV1.SourceKind.BOOKKEEPER_LEDGER,
                digest("declared-source"),
                coverage,
                "declared-ledger",
                3,
                1,
                1,
                1,
                digest("declared-body"),
                Optional.empty(),
                Optional.of(digest("declared-ledger-id")),
                digest("format"),
                digest("encryption"),
                false,
                false,
                List.of(binding.bindingId().digest()));
        var cut = new M5MaterializationRecordsV1.MaterializationSourceCut(
                identity,
                selected,
                Sha256Digest.hash(M4ReadControlCodecV1.encodeSelector(selected)),
                selected.selectedViewSha256(),
                coverage,
                3,
                3,
                3,
                3,
                0,
                digest("protocol"),
                digest("recovery"),
                digest("policy"),
                digest("output-format"),
                M5MaterializationCodecV1.calculateSourceSetSha256(List.of(source)),
                List.of(source));
        return new M5MaterializationPlannerV1()
                .plan(
                        cut,
                        new M5MaterializationPlannerV1.PlanningPolicy(
                                M5MaterializationRecordsV1.PayloadKind.KAFKA_BATCH_PRESERVING_V1,
                                true,
                                digest("encryption"),
                                digest("compression"),
                                digest("checksum"),
                                List.of(),
                                "declared-cell"));
    }

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
            assertThat(await(fixture.nativeStore.read(fixture.faults.lastCreatedKey)))
                    .isPresent();
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
            fixture.faults.holdSelectorKey = fixture.route.nativeKey(fixture.selectorKey);
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
            var current = fixture.exact();
            assertThatThrownBy(() -> fixture.route.compareAndSet(
                            Optional.of(current), fixture.selectorKey, before.canonicalStoredBytes()))
                    .hasMessageContaining("history cannot roll back");
            assertThat(fixture.exact()).isEqualTo(current);
        }
    }

    @Test
    void nativeRootChangeBetweenNonmembershipProofAndCasRejectsAdmission() throws Exception {
        try (Fixture fixture = Fixture.create(1, 2, 3)) {
            fixture.retire(2);
            fixture.retire(3);
            assertThat(fixture.fold(2)).isEqualTo(Outcome.APPLIED_EXACT);
            var before = fixture.authority();
            fixture.faults.beforeSelectorCas = () -> assertThat(fixture.fold(3)).isEqualTo(Outcome.APPLIED_EXACT);
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
            String nativeKey = fixture.route.nativeKey(key);
            var exactNode = await(fixture.nativeStore.read(nativeKey)).orElseThrow();
            // Only this UUID-owned fixture is corrupted/deleted; the history implementation has no delete method.
            assertThat(await(fixture.nativeStore.compareAndSet(
                            Optional.of(exactNode), nativeKey, bytes("corrupt-history"))))
                    .isEqualTo(MutationOutcome.APPLIED_EXACT);
            assertThatThrownBy(() -> fixture.tryAdmit(4))
                    .hasRootCauseMessage("retired history node content address differs");
            assertThatThrownBy(() -> fixture.fold(3))
                    .hasRootCauseMessage("retired history node content address differs");
            assertThat(fixture.exact()).isEqualTo(before);
            await(fixture.client.delete(nativeKey));
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
        Oxia09ExactMetadataTransactionStoreV1 nativeStore;
        OxiaBindingLifecycleMetadataStoreV2 route;
        ExactMetadataTransactionStoreV1 store;
        M5RetiredBatchHistoryCoordinatorV2 coordinator;
        CanonicalControlMetadataStore facade;

        Fixture(String root) throws Exception {
            this.root = root;
            this.binding = new BindingIdentity(
                    new TopicBindingId(digest(root)), digest(root + "/incarnation"), digest(root + "/storage-epoch"));
            this.selectorKey = new M4ReadControlKeysV1(7, binding).selector();
            reconnect();
        }

        static Fixture create(int... ids) throws Exception {
            Fixture fixture = new Fixture("/nereus/v2/m5/history/" + UUID.randomUUID());
            try {
                var batches =
                        java.util.Arrays.stream(ids).mapToObj(fixture::batch).toList();
                var original = M4ReadControlCodecV1.encodeSelector(fixture.selector(batches, 1));
                var legacy = new OxiaCanonicalControlMetadataStore(fixture.client, fixture.root, 7);
                assertThat(legacy.putIfAbsent(fixture.selectorKey, original)).isEqualTo(ControlMutationOutcome.APPLIED);
                assertThat(await(new M5BindingRetirementCoordinatorV1(fixture.store)
                                .migrateLegacy(new M5BindingRetirementCoordinatorV1.MigrationRequest(
                                        fixture.selectorKey, fixture.exact()))))
                        .isEqualTo(M5BindingRetirementCoordinatorV1.Outcome.APPLIED_EXACT);
                assertThat(fixture.facade.get(fixture.selectorKey)).contains(original);
                assertThat(legacy.get(fixture.selectorKey))
                        .contains(fixture.exact().canonicalStoredBytes());
                assertThat(legacy.compareAndSet(fixture.selectorKey, Optional.of(original), original))
                        .isEqualTo(ControlMutationOutcome.DEFINITIVE_CONFLICT);
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
            nativeStore = new Oxia09ExactMetadataTransactionStoreV1(faults);
            route = new OxiaBindingLifecycleMetadataStoreV2(faults, root, 7, binding);
            coordinator = new M5RetiredBatchHistoryCoordinatorV2(route, budget);
            facade = route.controlMetadata();
            // Only synthetic external proof facts use this fixture dispatcher. All M4/history I/O uses the real route.
            store = new ExactMetadataTransactionStoreV1() {
                public CompletionStage<Optional<VersionedValue>> read(String key) {
                    return proofFixtureKey(key) ? nativeStore.read(key) : route.read(key);
                }

                public CompletionStage<MutationOutcome> compareAndSet(
                        Optional<VersionedValue> predecessor, String key, CanonicalBytes candidate) {
                    return proofFixtureKey(key)
                            ? nativeStore.compareAndSet(predecessor, key, candidate)
                            : route.compareAndSet(predecessor, key, candidate);
                }

                public CompletionStage<TransactionOutcome> conditionalTransaction(ExactTransaction transaction) {
                    throw new AssertionError("native Binding lifecycle must never use a multi-key transaction");
                }

                public boolean supportsAtomicMultiKeyTransactions() {
                    return false;
                }
            };
        }

        private boolean proofFixtureKey(String key) {
            return key.startsWith(root + "/proof/");
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
            var protection = create(
                    new M4ReadControlKeysV1(7, binding)
                            .protection(source.sourceIdentitySha256(), source.protectionGeneration()),
                    protectionBytes);
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
        volatile boolean loseNextTrimResponse;
        volatile String holdSelectorKey;
        volatile String lastCreatedKey;
        volatile String lastMutationKey;
        volatile boolean holdBeforeInlineTrim;
        final CompletableFuture<Void> trimDispatchReached = new CompletableFuture<>();
        final CompletableFuture<Void> trimDispatchReleased = new CompletableFuture<>();
        volatile String loseMutableReplyKey;
        volatile String lostMutableReadKey;
        final AtomicInteger mutableReadsLost = new AtomicInteger();
        final AtomicInteger mutableWrites = new AtomicInteger();
        volatile Runnable beforeSelectorCas;

        Faults(OxiaConditionalClient nativeClient) {
            this.nativeClient = nativeClient;
        }

        public CompletionStage<Optional<AuthorityRecord>> read(String key) {
            if (key.equals(lostMutableReadKey) && mutableReadsLost.getAndUpdate(n -> Math.max(0, n - 1)) > 0) {
                return CompletableFuture.failedFuture(new IOException("lost original mutable reread"));
            }
            if (failedReads.getAndUpdate(count -> Math.max(0, count - 1)) > 0) {
                return CompletableFuture.failedFuture(new IOException("injected native read response loss"));
            }
            return nativeClient.read(key);
        }

        public CompletionStage<Void> createIfAbsent(String key, CanonicalBytes value) {
            return nativeClient.createIfAbsent(key, value).thenCompose(ignored -> {
                if (key.endsWith("/retention/trim-frontier") && loseNextTrimResponse) {
                    loseNextTrimResponse = false;
                    lastCreatedKey = key;
                    return CompletableFuture.failedFuture(new IOException("injected actual trim create response loss"));
                }
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
            Runnable action = key.endsWith("/selector") ? beforeSelectorCas : null;
            if (action != null) {
                beforeSelectorCas = null;
            }
            CompletionStage<Void> before =
                    action == null ? CompletableFuture.completedFuture(null) : CompletableFuture.runAsync(action);
            if (holdBeforeInlineTrim
                    && key.endsWith("/selector")
                    && M5BindingAuthorityCodecV1.isAuthorityValue(value)
                    && M5BindingAuthorityCodecV1.decodeAuthority(value)
                            .logicalTrim()
                            .flatMap(
                                    com.nereusstream.storage.object.retention.M5LogicalTrimRecordsV2.LogicalTrimStateV2
                                            ::frontier)
                            .isPresent()) {
                holdBeforeInlineTrim = false;
                trimDispatchReached.complete(null);
                before = before.thenCompose(ignored -> trimDispatchReleased);
            }
            return before.thenCompose(ignored -> nativeClient.compareAndSet(key, value, version))
                    .thenCompose(ignored -> {
                        if (key.equals(loseMutableReplyKey)) {
                            loseMutableReplyKey = null;
                            mutableWrites.incrementAndGet();
                            lostMutableReadKey = key;
                            mutableReadsLost.set(2);
                            return CompletableFuture.failedFuture(new IOException("lost actual mutable CAS reply"));
                        }
                        if (key.endsWith("/selector")
                                && loseNextTrimResponse
                                && M5BindingAuthorityCodecV1.isAuthorityValue(value)
                                && M5BindingAuthorityCodecV1.decodeAuthority(value)
                                        .logicalTrim()
                                        .flatMap(
                                                com.nereusstream.storage.object.retention.M5LogicalTrimRecordsV2
                                                                .LogicalTrimStateV2::frontier)
                                        .isPresent()
                                && M5BindingAuthorityCodecV1.decodeAuthority(value)
                                                .state()
                                        == com.nereusstream.storage.object.retention.M5BindingAuthorityRecordsV1
                                                .BindingAuthorityStateV1.OPEN_V1) {
                            loseNextTrimResponse = false;
                            lastMutationKey = key;
                            return CompletableFuture.failedFuture(new IOException("lost actual inline trim CAS reply"));
                        }
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

    static CanonicalBytes bytes(String value) {
        return CanonicalBytes.copyOf(value.getBytes(StandardCharsets.UTF_8));
    }

    static Sha256Digest digest(String value) {
        return Sha256Digest.hash(bytes(value));
    }
}
