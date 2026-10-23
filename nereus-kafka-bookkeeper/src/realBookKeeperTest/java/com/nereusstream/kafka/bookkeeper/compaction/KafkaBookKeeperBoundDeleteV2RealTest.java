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
import com.nereusstream.kafka.bookkeeper.compaction.KafkaBookKeeperRunRootsV2RealTest.Fixture;
import com.nereusstream.metadata.oxia.v2.retention.Oxia09ExactMetadataTransactionStoreV1;
import com.nereusstream.metadata.oxia.v2.retention.OxiaPhysicalMetadataNamespaceV2;
import com.nereusstream.metadata.oxia.v2.retention.OxiaQuotaTargetDeleteStoreV2;
import com.nereusstream.metadata.spi.retention.ExactMetadataTransactionStoreV1.VersionedValue;
import com.nereusstream.storage.api.bookkeeper.BookKeeperLedgerIdentity;
import com.nereusstream.storage.api.bookkeeper.RunLedgerAppendRequestV1;
import com.nereusstream.storage.api.bookkeeper.RunLedgerHandleV1;
import com.nereusstream.storage.api.lifecycle.PhysicalResourceIdV2;
import com.nereusstream.storage.bookkeeper.ImmutableRetainedStoragePayload;
import com.nereusstream.storage.bookkeeper.M5BookKeeperDeleteAdapterV1;
import com.nereusstream.storage.bookkeeper.M5BookKeeperDeleteFaultFixtureV2;
import com.nereusstream.storage.object.gc.DeleteEligibilitySnapshotV2;
import com.nereusstream.storage.object.gc.DeleteObservationContextV2;
import com.nereusstream.storage.object.gc.DeleteRecoveryVetoV2;
import com.nereusstream.storage.object.gc.M5GcQuotaCoordinatorV2;
import com.nereusstream.storage.object.gc.M5GcQuotaRecordsV2;
import com.nereusstream.storage.object.gc.M5TargetDeleteAuthorityCodecV1;
import com.nereusstream.storage.object.gc.M5TargetDeleteAuthorityCoordinatorV1;
import com.nereusstream.storage.object.gc.M5TargetDeleteAuthorityStateMachineV1;
import com.nereusstream.storage.object.gc.SyntheticDeleteAuthorityFixturesV2;
import com.nereusstream.storage.object.retention.M5RetentionEvidenceAssemblerV1;
import com.nereusstream.storage.object.retention.M5RetentionEvidenceAssemblerV1.ReferenceProofRequestV1;
import com.nereusstream.storage.object.retention.M5RetentionRecordsV1.AuthorityFactV1;
import com.nereusstream.storage.object.retention.M5RetentionRecordsV1.ReferenceFreeProofV1;
import java.nio.file.Files;
import java.nio.file.Path;
import java.util.List;
import java.util.Optional;
import java.util.UUID;
import java.util.concurrent.CompletionStage;
import java.util.concurrent.TimeUnit;
import java.util.function.BiFunction;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.Timeout;

/** Actual bound route/quota/GC restart; protocol eligibility, grace and dispatch capacity are not certified. */
@Timeout(value = 4, unit = TimeUnit.MINUTES)
class KafkaBookKeeperBoundDeleteV2RealTest {
    @Test
    void epochGraceWaitStartsAtExactMarkAndRestartsForReconstructedClockOrNativeOwner() throws Exception {
        try (var f = new Fixture(8804, "gc-epoch-grace", null)) {
            assertThat(await(f.backend.confirmedDrainProviderMinimumMillis(f.binding)))
                    .isZero();
            var foreign = new com.nereusstream.storage.api.lifecycle.PhysicalNamespaceAuthorityBindingV2(
                    f.binding.physicalNamespace(),
                    new com.nereusstream.storage.api.lifecycle.MetadataNamespaceIdentityV2(
                            new com.nereusstream.domain.identity.Id128(71, 93), 0));
            assertThatThrownBy(() -> await(f.backend.confirmedDrainProviderMinimumMillis(foreign)))
                    .hasRootCauseMessage("confirmed-drain profile namespace changed");
            var handle = sealed(f);
            var resource = resource(f, handle);
            var gc = new KafkaBookKeeperDeleteObservationAuthorityV2(f.source, handle, UUID.randomUUID());
            var route = route(f, gc);
            var beforeQuota = await(route.quota().snapshot()).head();
            assertThat(await(route.quota().reserve(resource))).isEqualTo(M5GcQuotaCoordinatorV2.Result.GRANTED);
            var coordinator = coordinator(f, handle, gc, route);
            var template = SyntheticDeleteAuthorityFixturesV2.phases(resource).get(0);
            var initial = await(f.source.captureExactTarget(handle))
                    .exactTarget()
                    .orElseThrow()
                    .metadataSha256();
            // No synthetic eligibility is installed: this test qualifies the real epoch/clock dependency only.
            var open = await(coordinator.create(M5TargetDeleteAuthorityStateMachineV1.open(
                            template.target(), template.writerEnrollment(), initial)))
                    .observed()
                    .orElseThrow();
            var epoch = await(gc.claim(route, Optional.empty()));
            var context = await(gc.observe(1, Optional.empty()));
            long graceMillis = 750;
            var session = coordinator.prepareEpochGrace(open, context, graceMillis, initial);
            assertThatThrownBy(() -> await(session.requireElapsed())).isInstanceOf(Exception.class);
            await(session.confirmMark());
            assertThatThrownBy(() -> await(session.requireElapsed()))
                    .hasRootCauseMessage("epoch grace duration has not elapsed");
            Thread.sleep(graceMillis + 50);
            var ready = await(session.requireElapsed());
            await(session.confirmMark());
            await(ready.requireCurrent()); // confirming the same original candidate cannot restart its clock
            var marked = await(route.read(resource.authorityKey())).orElseThrow();
            assertThat(M5TargetDeleteAuthorityCodecV1.decodeAuthority(marked.canonicalStoredBytes())
                            .epochGrace())
                    .contains(session.mark());
            var reconstructed = coordinator.prepareEpochGrace(marked, context, graceMillis, initial);
            assertThat(reconstructed.mark().clockDomain())
                    .isNotEqualTo(session.mark().clockDomain());
            await(reconstructed.confirmMark());
            assertThatThrownBy(() -> await(ready.requireCurrent()))
                    .hasRootCauseMessage("epoch grace mark was changed or invalidated by a writer");
            assertThatThrownBy(() -> await(reconstructed.requireElapsed()))
                    .hasRootCauseMessage("epoch grace duration has not elapsed");
            Thread.sleep(graceMillis + 50);
            var reconstructedReady = await(reconstructed.requireElapsed());
            var successorGc = new KafkaBookKeeperDeleteObservationAuthorityV2(f.source, handle, UUID.randomUUID());
            await(successorGc.claim(route, Optional.of(epoch)));
            assertThatThrownBy(() -> await(reconstructedReady.requireCurrent()))
                    .hasRootCauseMessage("native GC owner differs from this coordinator");
            var successorContext = await(successorGc.observe(2, Optional.of(context)));
            var successor = coordinator(f, handle, successorGc, route)
                    .prepareEpochGrace(
                            await(route.read(resource.authorityKey())).orElseThrow(),
                            successorContext,
                            graceMillis,
                            initial);
            await(successor.confirmMark());
            assertThatThrownBy(() -> await(successor.requireElapsed()))
                    .hasRootCauseMessage("epoch grace duration has not elapsed");
            Thread.sleep(graceMillis + 50);
            var successorReady = await(successor.requireElapsed());
            var nativeFacts = new Oxia09ExactMetadataTransactionStoreV1(f.oxia);
            var key = f.binding.authorityRoot() + "/" + resource.authorityKey();
            var original = await(nativeFacts.read(key)).orElseThrow();
            await(nativeFacts.compareAndSet(Optional.of(original), key, original.canonicalStoredBytes()));
            assertThatThrownBy(() -> await(successorReady.requireCurrent()))
                    .hasRootCauseMessage("epoch grace confirmed authority changed");
            assertThat(await(f.source.deleteAuthority(handle).readIntent())).isEmpty();
            var afterQuota = await(route.quota().snapshot()).head();
            assertThat(afterQuota.reservedResources()).isEqualTo(Math.addExact(beforeQuota.reservedResources(), 1));
            assertThat(afterQuota.settledResources()).isEqualTo(beforeQuota.settledResources());
            assertThat(await(route.quota().requireExistingReservation(resource)).settled())
                    .isFalse();
            // Native owner epoch advanced, but no M5 identity-read fence has been installed yet.
            verifyEpochReferenceQualification(
                    f, handle, successorGc, await(successorGc.observe(1, Optional.empty())), graceMillis);
        }
    }

    /** Synthetic protocol statements exercise the new typed time dependency; no physical deletion is admitted here. */
    private static void verifyEpochReferenceQualification(
            Fixture f,
            RunLedgerHandleV1 handle,
            KafkaBookKeeperDeleteObservationAuthorityV2 gc,
            DeleteObservationContextV2 context,
            long graceMillis)
            throws Exception {
        var resource = resource(f, handle);
        var route = route(f, gc);
        var coordinator = coordinator(f, handle, gc, route);
        var model = SyntheticDeleteAuthorityFixturesV2.replacement(resource, 1, facts(f, resource));
        assertThat(model.members()).hasSize(1);
        var member = model.members().get(0);
        var prior = member.physicalReferences();
        var request = epochRequest(prior, context);
        var assembler = epochAssembler(route, prior, context);
        var initial = await(assembler.captureEpochReferences(request));
        var session = coordinator.prepareEpochGrace(
                await(route.read(resource.authorityKey())).orElseThrow(), initial, graceMillis);
        await(session.confirmMark());
        assertThatThrownBy(() -> await(session.requireElapsed()))
                .hasRootCauseMessage("epoch grace duration has not elapsed");
        Thread.sleep(graceMillis + 50);
        var ready = await(session.requireElapsed());
        assertThatThrownBy(() -> await(ready.requireAdmittedCurrent(
                        new PhysicalResourceIdV2.BookKeeperLedger(resource.namespace(), Long.MAX_VALUE), ready.mark())))
                .hasRootCauseMessage("elapsed readiness belongs to another resource or mark");
        var proof = await(assembler.buildEpochReferenceFreeProof(request, ready));
        assertThat(proof.auditGraceDeadlineMillis()).isZero();
        assertThat(proof.observedAuthorityTimeMillis()).isZero();
        var marked = await(route.read(resource.authorityKey())).orElseThrow();
        var snapshot = epochSnapshot(
                model,
                proof,
                M5TargetDeleteAuthorityCodecV1.decodeAuthority(marked.canonicalStoredBytes())
                                .authorityRevision()
                        + 1);
        assertThatThrownBy(() -> coordinator.qualifyEligibility(marked, snapshot))
                .isInstanceOf(IllegalArgumentException.class)
                .hasMessageContaining("original producer readiness");
        var qualified = await(coordinator.qualifyEligibility(marked, snapshot, ready))
                .observed()
                .orElseThrow();
        var fenced = await(coordinator.prepareIdentityRead(
                        qualified, SyntheticDeleteAuthorityFixturesV2.digest("epoch-profile-read"), context, ready))
                .observed()
                .orElseThrow();
        // The actual native owner changes with READ_FENCED still durable. No read admission reopens.
        var recoveryGc = new KafkaBookKeeperDeleteObservationAuthorityV2(f.source, handle, UUID.randomUUID());
        await(recoveryGc.claim(route, await(f.source.deleteAuthority(handle).read())));
        var recoveryContext = await(recoveryGc.observe(2, Optional.of(context)));
        var recoveryRoute = route(f, recoveryGc);
        var recovery = coordinator(f, handle, recoveryGc, recoveryRoute);
        var recoveryRequest = epochRequest(prior, recoveryContext);
        var recoveryAssembler = epochAssembler(recoveryRoute, prior, recoveryContext);
        var recoveryCapture = await(recoveryAssembler.captureEpochReferences(recoveryRequest));
        var fencedAuthority = M5TargetDeleteAuthorityCodecV1.decodeAuthority(fenced.canonicalStoredBytes());
        var missingOwnerFence = new DeleteObservationContextV2(
                2, recoveryContext.coordinatorOwner(), recoveryContext.capability(), Optional.empty());
        assertThatThrownBy(() -> recovery.prepareEpochGrace(
                        fenced, missingOwnerFence, graceMillis, recoveryCapture.rootSha256()))
                .hasMessageContaining("stale/unfenced");
        assertThatThrownBy(() -> M5TargetDeleteAuthorityStateMachineV1.renewReadEpochGrace(
                        fencedAuthority,
                        new com.nereusstream.storage.object.gc.DeleteEpochGraceMarkV1(
                                ready.mark().clockDomain(),
                                recoveryContext,
                                graceMillis,
                                recoveryCapture.rootSha256())))
                .hasMessageContaining("reused clock");
        var renewing = recovery.prepareEpochGrace(fenced, recoveryCapture, graceMillis);
        assertThatThrownBy(() -> await(ready.requireAdmittedCurrent(resource, ready.mark())))
                .hasRootCauseMessage("native GC owner differs from this coordinator");
        await(renewing.confirmMark());
        var pending = await(recoveryRoute.read(resource.authorityKey())).orElseThrow();
        var pendingAuthority = M5TargetDeleteAuthorityCodecV1.decodeAuthority(pending.canonicalStoredBytes());
        assertThat(pendingAuthority.state())
                .isEqualTo(
                        com.nereusstream.storage.object.gc.M5TargetDeleteAuthorityRecordsV1.TargetDeleteAuthorityStateV1
                                .READ_FENCED_V1);
        assertThat(pendingAuthority.recoveryVeto().orElseThrow().reason())
                .isEqualTo(DeleteRecoveryVetoV2.Reason.EPOCH_GRACE_RENEWAL_PENDING);
        assertThat(pendingAuthority.eligibilitySnapshot()).contains(snapshot);
        assertThat(pendingAuthority.readFence().orElseThrow().attemptIdSha256())
                .isEqualTo(M5TargetDeleteAuthorityCodecV1.decodeAuthority(fenced.canonicalStoredBytes())
                        .readFence()
                        .orElseThrow()
                        .attemptIdSha256());
        assertThatThrownBy(() -> await(renewing.requireElapsed()))
                .hasRootCauseMessage("epoch grace duration has not elapsed");
        assertThatThrownBy(() -> recovery.refreshIdentityRead(pending, recoveryContext, snapshot))
                .hasMessageContaining("original producer readiness");
        Thread.sleep(graceMillis + 50);
        var expiredLocal = await(renewing.requireElapsed());
        // Reconstructing the producer loses its clock even though the pending fence and original proof survive.
        var restarted = recovery.prepareEpochGrace(pending, recoveryCapture, graceMillis);
        await(restarted.confirmMark());
        assertThatThrownBy(() -> await(expiredLocal.requireCurrent()))
                .hasRootCauseMessage("epoch grace mark was changed or invalidated by a writer");
        assertThatThrownBy(() -> await(restarted.requireElapsed()))
                .hasRootCauseMessage("epoch grace duration has not elapsed");
        Thread.sleep(graceMillis + 50);
        var recoveryReady = await(restarted.requireElapsed());
        var rescanned = await(recoveryAssembler.buildEpochReferenceFreeProof(recoveryRequest, recoveryReady));
        var restartedPending =
                await(recoveryRoute.read(resource.authorityKey())).orElseThrow();
        var refreshedProof = epochSnapshot(
                model,
                rescanned,
                M5TargetDeleteAuthorityCodecV1.decodeAuthority(restartedPending.canonicalStoredBytes())
                                .authorityRevision()
                        + 1);
        var recoveredFence = await(
                        recovery.refreshIdentityRead(restartedPending, recoveryContext, refreshedProof, recoveryReady))
                .observed()
                .orElseThrow();
        assertThat(M5TargetDeleteAuthorityCodecV1.decodeAuthority(recoveredFence.canonicalStoredBytes())
                        .recoveryVeto())
                .isEmpty();
        var identity = await(new KafkaBookKeeperDeleteIdentityReaderV2(f.source, handle)
                .capture(M5TargetDeleteAuthorityCodecV1.decodeAuthority(recoveredFence.canonicalStoredBytes())));
        var intent = await(recovery.bindDeleteIntent(
                        recoveredFence,
                        identity,
                        SyntheticDeleteAuthorityFixturesV2.digest("epoch-profile-intent"),
                        recoveryReady))
                .observed()
                .orElseThrow();
        assertThatThrownBy(() -> await(recoveryGc.bindIntent(recoveryRoute, intent)))
                .hasRootCauseMessage("epoch grace requires original producer readiness");
        var bound = await(recoveryGc.bindIntent(recoveryRoute, intent, recoveryReady));
        assertThatThrownBy(() -> await(recoveryGc.dispatchBoundDelete(
                        recoveryRoute, f.backend.nativeDeleteCellBudget(), intent, bound)))
                .hasRootCauseMessage("epoch grace requires original producer readiness");
        assertThat(await(f.source.captureExactTarget(handle)).exactTarget()).isPresent();
        // Two consecutive INTENT takeovers cover both the original and an already refreshed history.
        var lastIntent = intent;
        var lastContext = recoveryContext;
        for (int takeover = 0; takeover < 2; takeover++) {
            lastIntent = renewEpochIntent(f, handle, model, prior, lastIntent, lastContext, graceMillis);
            lastContext = M5TargetDeleteAuthorityStateMachineV1.dispatchContext(
                    M5TargetDeleteAuthorityCodecV1.decodeAuthority(lastIntent.canonicalStoredBytes()));
        }
        assertThat(await(f.source.captureExactTarget(handle)).exactTarget()).isPresent();
    }

    private static VersionedValue renewEpochIntent(
            Fixture f,
            RunLedgerHandleV1 handle,
            DeleteEligibilitySnapshotV2 model,
            ReferenceFreeProofV1 prior,
            VersionedValue original,
            DeleteObservationContextV2 previousContext,
            long graceMillis)
            throws Exception {
        var resource = resource(f, handle);
        var gc = new KafkaBookKeeperDeleteObservationAuthorityV2(f.source, handle, UUID.randomUUID());
        var route = route(f, gc);
        await(gc.claim(route, await(f.source.deleteAuthority(handle).read())));
        var context = await(gc.observe(previousContext.observationEpoch() + 1, Optional.of(previousContext)));
        var coordinator = coordinator(f, handle, gc, route);
        var assembler = epochAssembler(route, prior, context);
        var request = epochRequest(prior, context);
        var capture = await(assembler.captureEpochReferences(request));
        var session = coordinator.prepareEpochGrace(original, capture, graceMillis);
        var originalValue = M5TargetDeleteAuthorityCodecV1.decodeAuthority(original.canonicalStoredBytes());
        await(session.confirmMark());
        var pending = await(route.read(resource.authorityKey())).orElseThrow();
        var pendingValue = M5TargetDeleteAuthorityCodecV1.decodeAuthority(pending.canonicalStoredBytes());
        assertThat(pendingValue.deleteIntent()).isEqualTo(originalValue.deleteIntent());
        assertThat(pendingValue.externalIdentity()).isEqualTo(originalValue.externalIdentity());
        assertThat(pendingValue.eligibilitySnapshot()).isEqualTo(originalValue.eligibilitySnapshot());
        assertThatThrownBy(() -> await(session.requireElapsed()))
                .hasRootCauseMessage("epoch grace duration has not elapsed");
        Thread.sleep(graceMillis + 50);
        var ready = await(session.requireElapsed());
        assertThatThrownBy(() -> await(gc.bindIntent(route, pending, ready)))
                .hasRootCauseMessage("epoch grace renewal requires fresh eligibility before dispatch");
        assertThatThrownBy(() -> await(coordinator.requireCurrentDispatch(pending)))
                .hasRootCauseMessage("epoch grace renewal requires fresh eligibility before dispatch");
        assertThatThrownBy(() -> await(coordinator.completeAbsent(pending)))
                .hasRootCauseMessage("epoch grace renewal requires fresh eligibility before dispatch");
        var proof = await(assembler.buildEpochReferenceFreeProof(request, ready));
        var snapshot = epochSnapshot(model, proof, pendingValue.authorityRevision() + 1);
        assertThatThrownBy(() -> coordinator.refreshDispatch(pending, context, snapshot))
                .hasMessageContaining("original producer readiness");
        var refreshed = await(coordinator.refreshDispatch(pending, context, snapshot, ready))
                .observed()
                .orElseThrow();
        var value = M5TargetDeleteAuthorityCodecV1.decodeAuthority(refreshed.canonicalStoredBytes());
        assertThat(value.recoveryVeto()).isEmpty();
        assertThat(value.deleteIntent().orElseThrow().deleteAttemptIdSha256())
                .isEqualTo(originalValue.deleteIntent().orElseThrow().deleteAttemptIdSha256());
        assertThat(value.deleteIntent().orElseThrow().intentAuthorityRevision())
                .isEqualTo(originalValue.deleteIntent().orElseThrow().intentAuthorityRevision());
        await(gc.bindIntent(route, refreshed, ready));
        return refreshed;
    }

    private static ReferenceProofRequestV1 epochRequest(
            ReferenceFreeProofV1 prior, DeleteObservationContextV2 context) {
        return new com.nereusstream.storage.object.retention.M5RetentionEvidenceAssemblerV1.ReferenceProofRequestV1(
                prior.identity(),
                prior.targetKind(),
                prior.targetIdentitySha256(),
                prior.coverage(),
                prior.selectorRoot(),
                prior.manifestRoot(),
                prior.trimRoot(),
                prior.retentionSnapshotRootSha256(),
                prior.m4Releases(),
                prior.ownerFence(),
                prior.workerFence(),
                prior.storageFence(),
                prior.providerFence(),
                0,
                0,
                Optional.of(context));
    }

    private static M5RetentionEvidenceAssemblerV1 epochAssembler(
            OxiaQuotaTargetDeleteStoreV2 route, ReferenceFreeProofV1 prior, DeleteObservationContextV2 context) {
        var floors = new java.util.EnumMap<
                com.nereusstream.storage.object.retention.M5RetentionRecordsV1.FloorClassV1,
                com.nereusstream.storage.object.retention.M5RetentionEvidenceAssemblerV1.FloorAdapterV1>(
                com.nereusstream.storage.object.retention.M5RetentionRecordsV1.FloorClassV1.class);
        for (var kind : com.nereusstream.storage.object.retention.M5RetentionRecordsV1.FloorClassV1.values()) {
            floors.put(
                    kind,
                    ignored -> java.util.concurrent.CompletableFuture.failedFuture(
                            new AssertionError("reference time qualification does not collect protocol floors")));
        }
        var references = new java.util.EnumMap<
                com.nereusstream.storage.object.retention.M5RetentionRecordsV1.ReferenceKindV1,
                com.nereusstream.storage.object.retention.M5RetentionEvidenceAssemblerV1.ReferenceAdapterV1>(
                com.nereusstream.storage.object.retention.M5RetentionRecordsV1.ReferenceKindV1.class);
        for (var kind : com.nereusstream.storage.object.retention.M5RetentionRecordsV1.ReferenceKindV1.values()) {
            var rows =
                    kind == com.nereusstream.storage.object.retention.M5RetentionRecordsV1.ReferenceKindV1.AUDIT_GRACE
                            ? context.authorityFacts().stream()
                                    .distinct()
                                    .sorted(java.util.Comparator.comparing(AuthorityFactV1::key))
                                    .map(
                                            fact -> new com.nereusstream.storage.object.retention.M5RetentionRecordsV1
                                                    .ReferenceObservationV1(
                                                    kind,
                                                    fact,
                                                    prior.targetIdentitySha256(),
                                                    prior.coverage(),
                                                    com.nereusstream.storage.object.retention.M5RetentionRecordsV1
                                                            .ReferenceDispositionV1.ABSENT,
                                                    true))
                                    .toList()
                            : prior.observations().stream()
                                    .filter(row -> row.kind() == kind)
                                    .toList();
            references.put(
                    kind,
                    ignored ->
                            java.util.concurrent.CompletableFuture.completedFuture(new com.nereusstream.storage.object
                                    .retention.M5RetentionEvidenceAssemblerV1.ReferenceAdapterResultV1(
                                    kind,
                                    new com.nereusstream.storage.object.retention.M5RetentionRecordsV1
                                            .ReferenceScanSummaryV1(kind, rows.size(), 1, 1024, true),
                                    rows)));
        }
        return new M5RetentionEvidenceAssemblerV1(route, floors, references);
    }

    private static DeleteEligibilitySnapshotV2 epochSnapshot(
            DeleteEligibilitySnapshotV2 model, ReferenceFreeProofV1 proof, long generation) {
        var member = model.members().get(0);
        return new DeleteEligibilitySnapshotV2(
                model.resource(),
                model.reason(),
                generation,
                model.namespaceAdmission(),
                model.completeMemberInventory(),
                List.of(new com.nereusstream.storage.object.gc.DeleteEligibilitySnapshotV2.MemberSnapshot(
                        member.floors(),
                        proof,
                        member.nativeAuthority(),
                        member.nativeDisposition(),
                        member.replacement(),
                        member.expiry(),
                        member.unpublished())));
    }

    @Test
    void boundNativeClientRequiresActiveQuotaAuthorityAndRejectsRawRoutesBeforeEpochOrIntentMutation()
            throws Exception {
        try (var f = new Fixture(8801, "gc-bound-route", null)) {
            var handle = sealed(f);
            var resource = resource(f, handle);
            assertThat(await(f.source.requireNamespaceBinding())).isEqualTo(f.binding);
            var owner = UUID.randomUUID();
            var gc = new KafkaBookKeeperDeleteObservationAuthorityV2(f.source, handle, owner);
            var facts = facts(f, resource);
            var route = route(f, gc);
            var misplacedRelease = SyntheticDeleteAuthorityFixturesV2.replacement(
                    resource,
                    1,
                    (key, value) -> facts.apply(key.contains("/protections/") ? "/misplaced-m4-release" : key, value));
            assertThatThrownBy(() -> await(gc.requireFreshEligibility(route, misplacedRelease)))
                    .isInstanceOf(IllegalArgumentException.class)
                    .hasMessageContaining("canonical protection key");
            assertThat(await(f.source.deleteAuthority(handle).readIntent())).isEmpty();
            var raw = new OxiaQuotaTargetDeleteStoreV2(
                    f.oxia,
                    new M5GcQuotaRecordsV2.Layout(f.binding.authorityRoot(), f.binding.physicalNamespace()),
                    gc.readOnlyFacts(new Oxia09ExactMetadataTransactionStoreV1(f.oxia)));
            assertThatThrownBy(() -> await(gc.claim(raw, Optional.empty())))
                    .hasRootCauseMessage("native physical namespace route admission is not installed");
            assertThatThrownBy(() -> await(gc.claim(route, Optional.empty())))
                    .hasRootCauseMessage("authority lacks a permanent quota reservation");
            assertThat(await(route.quota().reserve(resource))).isEqualTo(M5GcQuotaCoordinatorV2.Result.GRANTED);
            assertThatThrownBy(() -> await(gc.claim(route, Optional.empty())))
                    .hasRootCauseMessage("reserved native resource has no active authority");
            assertThat(await(f.source.deleteAuthority(handle).read())).isEmpty();
            var coordinator = coordinator(f, handle, gc, route);
            var open = open(coordinator, resource, facts);
            var epoch = await(gc.claim(route, Optional.empty()));
            var observation = await(gc.observe(1, Optional.empty()));
            var intent = intent(f, handle, coordinator, open, observation);
            assertThatThrownBy(() -> await(gc.bindIntent(raw, intent)))
                    .hasRootCauseMessage("native physical namespace route admission is not installed");
            assertThat(await(f.source.deleteAuthority(handle).readIntent())).isEmpty();
            var proof = M5TargetDeleteAuthorityCodecV1.decodeAuthority(intent.canonicalStoredBytes())
                    .eligibilitySnapshot()
                    .orElseThrow()
                    .members()
                    .get(0)
                    .physicalReferences();
            var grace = proof.observations().stream()
                    .filter(row -> row.kind()
                            == com.nereusstream.storage.object.retention.M5RetentionRecordsV1.ReferenceKindV1
                                    .AUDIT_GRACE)
                    .findFirst()
                    .orElseThrow()
                    .authority();
            var semanticFacts = new Oxia09ExactMetadataTransactionStoreV1(f.oxia);
            var originalGrace = await(semanticFacts.read(grace.key())).orElseThrow();
            await(semanticFacts.compareAndSet(
                    Optional.of(originalGrace), grace.key(), originalGrace.canonicalStoredBytes()));
            assertThatThrownBy(() -> await(gc.bindIntent(route, intent)))
                    .hasRootCauseMessage("native intent eligibility authority changed: " + grace.key());
            assertThat(await(f.source.deleteAuthority(handle).readIntent())).isEmpty();
            var refreshedObservation = await(gc.observe(2, Optional.of(observation)));
            var refreshed = await(coordinator.refreshDispatch(
                            intent,
                            refreshedObservation,
                            SyntheticDeleteAuthorityFixturesV2.replacement(
                                    resource,
                                    M5TargetDeleteAuthorityCodecV1.decodeAuthority(intent.canonicalStoredBytes())
                                                    .authorityRevision()
                                            + 1,
                                    facts)))
                    .observed()
                    .orElseThrow();
            var nativeIntent = await(gc.bindIntent(route, refreshed));
            assertThat(nativeIntent.epoch()).isEqualTo(epoch);
            assertThat(nativeIntent.intentAuthoritySha256()).isEqualTo(refreshed.canonicalStoredSha256());
            assertThatThrownBy(() -> await(gc.reconcileBoundCellDeleteAbsence(
                            raw, f.backend.nativeDeleteCellBudget(), refreshed, nativeIntent)))
                    .hasRootCauseMessage("native physical namespace route admission is not installed");
            assertThatThrownBy(() -> await(gc.reconcileBoundCellDeleteAbsence(
                            route, f.backend.nativeDeleteCellBudget(), intent, nativeIntent)))
                    .hasRootCauseMessage("native binding differs from exact M5 delete intent");
            assertThatThrownBy(() -> await(gc.reconcileBoundCellDeleteAbsence(
                            route, f.backend.nativeDeleteCellBudget(), refreshed, nativeIntent)))
                    .hasRootCauseMessage("native Cell reservation is absent");
            assertThat(await(f.backend.nativeDeleteCellBudget().snapshot()).reservations())
                    .isEmpty();
            assertThat(await(route.requireActiveResource(resource))).isEqualTo(f.binding);
            assertThat(await(f.source.captureExactTarget(handle)).exactTarget()).isPresent();
            var refreshedGrace = M5TargetDeleteAuthorityCodecV1.decodeAuthority(refreshed.canonicalStoredBytes())
                    .eligibilitySnapshot()
                    .orElseThrow()
                    .members()
                    .get(0)
                    .physicalReferences()
                    .observations()
                    .stream()
                    .filter(row -> row.kind()
                            == com.nereusstream.storage.object.retention.M5RetentionRecordsV1.ReferenceKindV1
                                    .AUDIT_GRACE)
                    .findFirst()
                    .orElseThrow()
                    .authority();
            var currentGrace = await(semanticFacts.read(refreshedGrace.key())).orElseThrow();
            var heldBefore = await(f.backend.nativeDeleteCellBudget().snapshot());
            await(semanticFacts.compareAndSet(
                    Optional.of(currentGrace), refreshedGrace.key(), currentGrace.canonicalStoredBytes()));
            assertThatThrownBy(() -> await(
                            gc.dispatchBoundDelete(route, f.backend.nativeDeleteCellBudget(), refreshed, nativeIntent)))
                    .hasRootCauseMessage("native intent eligibility authority changed: " + refreshedGrace.key());
            assertThat(await(f.backend.nativeDeleteCellBudget().snapshot())).isEqualTo(heldBefore);
            assertThat(await(f.source.captureExactTarget(handle)).exactTarget()).isPresent();

            var successorEpoch = await(gc.claim(route, Optional.of(epoch)));
            var successorObservation = await(gc.observe(3, Optional.of(refreshedObservation)));
            var next = await(coordinator.refreshDispatch(
                            refreshed,
                            successorObservation,
                            SyntheticDeleteAuthorityFixturesV2.replacement(
                                    resource,
                                    M5TargetDeleteAuthorityCodecV1.decodeAuthority(refreshed.canonicalStoredBytes())
                                                    .authorityRevision()
                                            + 1,
                                    facts)))
                    .observed()
                    .orElseThrow();
            var nextIntent = await(gc.bindIntent(route, next));
            assertThat(nextIntent.epoch()).isEqualTo(successorEpoch);
            try (var faults = new M5BookKeeperDeleteFaultFixtureV2(
                    System.getProperty("nereus.bookkeeper.metadataServiceUri"),
                    f.source.capabilitySnapshot(),
                    f.source.spec(),
                    f.binding)) {
                var faulted = new KafkaBookKeeperDeleteObservationAuthorityV2(
                        f.source, handle, owner, faults.deleteAuthority(handle));
                faults.dropNextDeleteCallback();
                var unknown =
                        await(faulted.dispatchBoundDelete(route, f.backend.nativeDeleteCellBudget(), next, nextIntent));
                assertThat(unknown.deleteResult().outcome())
                        .isEqualTo(M5BookKeeperDeleteAdapterV1.DeleteOutcome.OUTCOME_UNKNOWN);
                assertThat(unknown.reservationRetained()).isTrue();
                var held = await(f.backend.nativeDeleteCellBudget().snapshot());
                assertThat(held.reservations()).hasSize(1);
                assertThat(held.reservations().get(0).terminalUnknown()).isTrue();
                assertThat(await(faulted.reconcileBoundCellDeleteAbsence(
                                        route, f.backend.nativeDeleteCellBudget(), next, nextIntent))
                                .deleteResult()
                                .outcome())
                        .isEqualTo(M5BookKeeperDeleteAdapterV1.DeleteOutcome.EXACT_LEDGER_REMAINS);
                assertThat(await(f.backend.nativeDeleteCellBudget().snapshot())).isEqualTo(held);
                await(faults.deliverDroppedDelete());
                assertThat(await(f.source.captureExactTarget(handle)).exactTarget())
                        .isEmpty();
                assertThat(await(f.backend.nativeDeleteCellBudget().snapshot())).isEqualTo(held);
                Files.write(
                        unknownCheckpoint(),
                        List.of(
                                f.source.spec().encode().toHex(),
                                Long.toString(handle.ledgerIdentity().ledgerId()),
                                next.key(),
                                next.canonicalStoredSha256().toHex(),
                                next.metadataVersion().value().toHex(),
                                Sha256Digest.hash(successorEpoch.encode()).toHex(),
                                Sha256Digest.hash(nextIntent.encode()).toHex(),
                                held.reservations().get(0).operation().toString()));
            }
        }
    }

    @Test
    void writeBeforeServerRestart() throws Exception {
        try (var f = new Fixture(8802, "gc-bound-restart", null)) {
            var nativeQuota = f.backend.nativeDeleteQuota();
            var priorQuota = await(nativeQuota.snapshot());
            await(nativeQuota.expand(Math.max(
                    priorQuota.capacityBytes(),
                    nativeQuota.capacityForResources(Math.addExact(priorQuota.reservedResources(), 1)))));
            var handle = sealed(f);
            var resource = resource(f, handle);
            var gc = new KafkaBookKeeperDeleteObservationAuthorityV2(f.source, handle, UUID.randomUUID());
            var route = route(f, gc);
            var coordinator = coordinator(f, handle, gc, route);
            var open = open(coordinator, resource, facts(f, resource));
            var epoch = await(gc.claim(route, Optional.empty()));
            var observation = await(gc.observe(1, Optional.empty()));
            var intent = intent(f, handle, coordinator, open, observation);
            var nativeIntent = await(gc.bindIntent(route, intent));
            var quotaSnapshot = await(f.backend.nativeDeleteQuota().snapshot());
            Files.write(
                    checkpoint(),
                    List.of(
                            f.source.spec().encode().toHex(),
                            Long.toString(handle.ledgerIdentity().ledgerId()),
                            intent.key(),
                            intent.canonicalStoredSha256().toHex(),
                            intent.metadataVersion().value().toHex(),
                            Sha256Digest.hash(epoch.encode()).toHex(),
                            Sha256Digest.hash(nativeIntent.encode()).toHex(),
                            Sha256Digest.hash(quotaSnapshot.encode()).toHex(),
                            Sha256Digest.hash(await(f.backend
                                                    .nativeDeleteCellBudget()
                                                    .snapshot())
                                            .encode())
                                    .toHex(),
                            Long.toString(quotaSnapshot.reservedResources())));
        }
    }

    @Test
    void readAfterServerRestart() throws Exception {
        var lines = Files.readAllLines(checkpoint());
        assertThat(lines).hasSize(10);
        try (var f = new Fixture(8802, "gc-bound-restart", List.of(lines.get(0)))) {
            var config = f.source.spec().configurations().get(0);
            var handle = new RunLedgerHandleV1(
                    config.providerScopeId(),
                    config.runId(),
                    new BookKeeperLedgerIdentity(Long.parseLong(lines.get(1))),
                    config.configurationDigest());
            var resource = resource(f, handle);
            var nativeAuthority = f.source.deleteAuthority(handle);
            var previous = await(nativeAuthority.read()).orElseThrow();
            var previousIntent = await(nativeAuthority.readIntent()).orElseThrow();
            assertThat(Sha256Digest.hash(previous.encode()).toHex()).isEqualTo(lines.get(5));
            assertThat(Sha256Digest.hash(previousIntent.encode()).toHex()).isEqualTo(lines.get(6));
            var gc = new KafkaBookKeeperDeleteObservationAuthorityV2(f.source, handle, UUID.randomUUID());
            var route = route(f, gc);
            var stored = await(route.read(lines.get(2))).orElseThrow();
            assertThat(stored.canonicalStoredSha256().toHex()).isEqualTo(lines.get(3));
            assertThat(stored.metadataVersion().value().toHex()).isEqualTo(lines.get(4));
            assertThat(previousIntent.intentAuthoritySha256()).isEqualTo(stored.canonicalStoredSha256());
            assertThat(await(route.requireActiveResource(resource))).isEqualTo(f.binding);
            var priorValue = M5TargetDeleteAuthorityCodecV1.decodeAuthority(stored.canonicalStoredBytes());
            var priorObservation = M5TargetDeleteAuthorityStateMachineV1.dispatchContext(priorValue);
            assertThat(priorObservation.coordinatorOwner().valueSha256())
                    .isEqualTo(Sha256Digest.hash(previous.encode()));
            // All three durable records have been read and matched before any GC/M5 mutation.
            var quota = f.backend.nativeDeleteQuota();
            var capacity = await(quota.snapshot());
            assertThat(Sha256Digest.hash(capacity.encode()).toHex()).isEqualTo(lines.get(7));
            assertThat(capacity.reservedResources()).isEqualTo(Long.parseLong(lines.get(9)));
            assertThat(quota.chargedBytes(capacity)).isEqualTo(capacity.capacityBytes());
            var cellBudget = f.backend.nativeDeleteCellBudget();
            var originalBudget = await(cellBudget.snapshot());
            assertThat(Sha256Digest.hash(originalBudget.encode()).toHex()).isEqualTo(lines.get(8));
            assertThat(originalBudget.reservations()).hasSize(1);
            assertThat(originalBudget.reservations().get(0).terminalUnknown()).isTrue();
            var current = await(gc.claim(route, Optional.of(previous)));
            var observation = await(gc.observe(priorObservation.observationEpoch() + 1, Optional.of(priorObservation)));
            var coordinator = coordinator(f, handle, gc, route);
            var refreshed = await(coordinator.refreshDispatch(
                            stored,
                            observation,
                            SyntheticDeleteAuthorityFixturesV2.replacement(
                                    resource, priorValue.authorityRevision() + 1, facts(f, resource))))
                    .observed()
                    .orElseThrow();
            var bound = await(gc.bindIntent(route, refreshed));
            var target =
                    await(f.source.captureExactTarget(handle)).exactTarget().orElseThrow();
            assertThatThrownBy(() -> await(nativeAuthority.deleteExact(cellBudget, previousIntent, target)))
                    .hasRootCauseMessage("native delete owner is fenced");
            // Low-level deletion of this test-owned fixture. Full protocol/grace/Cell admission is still separate.
            assertThatThrownBy(() -> await(nativeAuthority.deleteExact(bound, target)))
                    .hasRootCauseMessage("bound native delete requires Cell budget");
            assertThatThrownBy(() -> await(gc.dispatchBoundDelete(route, cellBudget, refreshed, previousIntent)))
                    .hasRootCauseMessage("native binding differs from exact M5 delete intent");
            assertThatThrownBy(() ->
                            await(gc.reconcileBoundCellDeleteAbsence(route, cellBudget, refreshed, previousIntent)))
                    .hasRootCauseMessage("native binding differs from exact M5 delete intent");
            assertThatThrownBy(() -> await(gc.reconcileBoundCellDeleteAbsence(route, cellBudget, refreshed, bound)))
                    .hasRootCauseMessage("native Cell reservation is absent");
            assertThat(await(cellBudget.snapshot())).isEqualTo(originalBudget);
            var deleted = await(gc.dispatchBoundDelete(route, cellBudget, refreshed, bound));
            assertThat(deleted.deleteResult().outcome())
                    .isEqualTo(M5BookKeeperDeleteAdapterV1.DeleteOutcome.AUTHORITATIVELY_ABSENT);
            assertThat(deleted.reservationRetained()).isFalse();
            assertThat(await(cellBudget.snapshot()).reservations())
                    .containsExactlyElementsOf(originalBudget.reservations());
            var done = await(coordinator.completeAbsent(refreshed)).observed().orElseThrow();
            await(coordinator.compactDone(done));
            assertThat(await(route.quota().settle(resource))).isEqualTo(M5GcQuotaCoordinatorV2.Result.SETTLED);
            assertThatThrownBy(() -> await(gc.claim(route, Optional.of(current))))
                    .hasRootCauseMessage("native resource quota is settled");
            assertThat(await(nativeAuthority.read())).contains(current);
            assertThat(await(quota.snapshot())).isEqualTo(capacity);
            // Native GC records remain charged after native absence, permanent DONE and Oxia quota settlement.
            try (var fresh = new Fixture(8803, "gc-native-capacity", null)) {
                var nextHandle = sealed(fresh);
                var nextResource = resource(fresh, nextHandle);
                var nextGc =
                        new KafkaBookKeeperDeleteObservationAuthorityV2(fresh.source, nextHandle, UUID.randomUUID());
                var nextRoute = route(fresh, nextGc);
                open(coordinator(fresh, nextHandle, nextGc, nextRoute), nextResource, facts(fresh, nextResource));
                assertThatThrownBy(() -> await(nextGc.claim(nextRoute, Optional.empty())))
                        .hasRootCauseMessage("native GC quota exhausted");
                assertThat(await(fresh.source.deleteAuthority(nextHandle).read()))
                        .isEmpty();
                assertThat(await(quota.snapshot())).isEqualTo(capacity);
                long nextCount = Math.addExact(capacity.reservedResources(), 1);
                var expanded = await(quota.expand(quota.capacityForResources(nextCount)));
                assertThat(expanded.reservedResources()).isEqualTo(capacity.reservedResources());
                var admitted = await(nextGc.claim(nextRoute, Optional.empty()));
                var charged = await(quota.snapshot());
                assertThat(charged.reservedResources()).isEqualTo(nextCount);
                assertThat(quota.chargedBytes(charged)).isEqualTo(charged.capacityBytes());
                assertThat(await(nextGc.claim(nextRoute, Optional.empty()))).isEqualTo(admitted);
                assertThat(await(quota.snapshot())).isEqualTo(charged);
            }
            assertThat(await(coordinator.completeAbsent(refreshed)).exactTerminalIsAuthoritative())
                    .isTrue();
        }
        var unknownLines = Files.readAllLines(unknownCheckpoint());
        assertThat(unknownLines).hasSize(8);
        try (var f = new Fixture(8801, "gc-bound-route", List.of(unknownLines.get(0)))) {
            var config = f.source.spec().configurations().get(0);
            var handle = new RunLedgerHandleV1(
                    config.providerScopeId(),
                    config.runId(),
                    new BookKeeperLedgerIdentity(Long.parseLong(unknownLines.get(1))),
                    config.configurationDigest());
            var resource = resource(f, handle);
            var nativeAuthority = f.source.deleteAuthority(handle);
            var epoch = await(nativeAuthority.read()).orElseThrow();
            var nativeIntent = await(nativeAuthority.readIntent()).orElseThrow();
            assertThat(Sha256Digest.hash(epoch.encode()).toHex()).isEqualTo(unknownLines.get(5));
            assertThat(Sha256Digest.hash(nativeIntent.encode()).toHex()).isEqualTo(unknownLines.get(6));
            var recoveryOwner = UUID.randomUUID();
            assertThat(recoveryOwner).isNotEqualTo(epoch.owner());
            var recovery = new KafkaBookKeeperDeleteObservationAuthorityV2(f.source, handle, recoveryOwner);
            var route = route(f, recovery);
            var stored = await(route.read(unknownLines.get(2))).orElseThrow();
            assertThat(stored.canonicalStoredSha256().toHex()).isEqualTo(unknownLines.get(3));
            assertThat(stored.metadataVersion().value().toHex()).isEqualTo(unknownLines.get(4));
            assertThat(nativeIntent.intentAuthoritySha256()).isEqualTo(stored.canonicalStoredSha256());
            assertThat(await(route.requireActiveResource(resource))).isEqualTo(f.binding);
            var budget = f.backend.nativeDeleteCellBudget();
            var held = await(budget.snapshot());
            assertThat(held.reservations()).hasSize(1);
            var retained = held.reservations().get(0);
            assertThat(retained.operation().toString()).isEqualTo(unknownLines.get(7));
            assertThat(retained.resource()).isEqualTo(resource.sha256());
            assertThat(retained.nativeIntent()).isEqualTo(Sha256Digest.hash(nativeIntent.encode()));
            assertThat(retained.terminalUnknown()).isTrue();
            assertThat(await(f.source.captureExactTarget(handle)).exactTarget()).isEmpty();
            var permanent = await(f.source.readFencedLedgerScopeFacts(f.source.spec(), handle));
            assertThat(permanent.scope().fenced()).isTrue();
            assertThat(permanent.handle()).isEqualTo(handle);
            var absent = await(recovery.reconcileBoundCellDeleteAbsence(route, budget, stored, nativeIntent));
            assertThat(absent.deleteResult().outcome())
                    .isEqualTo(M5BookKeeperDeleteAdapterV1.DeleteOutcome.AUTHORITATIVELY_ABSENT);
            assertThat(absent.reservationRetained()).isFalse();
            assertThat(await(budget.snapshot()).reservations()).isEmpty();
            var completionOwner = new KafkaBookKeeperDeleteObservationAuthorityV2(f.source, handle, epoch.owner());
            var completionRoute = route(f, completionOwner);
            var coordinator = coordinator(f, handle, completionOwner, completionRoute);
            var done = await(coordinator.completeAbsent(stored)).observed().orElseThrow();
            await(coordinator.compactDone(done));
            assertThat(await(completionRoute.quota().settle(resource)))
                    .isEqualTo(M5GcQuotaCoordinatorV2.Result.SETTLED);
        }
    }

    private static RunLedgerHandleV1 sealed(Fixture f) throws Exception {
        var id = await(f.session.reserveLedgerIdentity()).exactProof().orElseThrow();
        var handle = await(f.session.createReservedRunLedger(
                        f.source.spec().configurations().get(0), id))
                .exactProof()
                .orElseThrow();
        var payload = ImmutableRetainedStoragePayload.copyOf(new byte[] {4, 5, 6});
        try {
            assertThat(await(f.session.appendExplicitEntry(new RunLedgerAppendRequestV1(handle, 0, payload)))
                            .exactProof())
                    .isPresent();
        } finally {
            payload.release();
        }
        assertThat(await(f.session.closeRunLedger(handle)).exactProof()).isPresent();
        await(f.source.fenceCreates());
        return handle;
    }

    private static PhysicalResourceIdV2.BookKeeperLedger resource(Fixture f, RunLedgerHandleV1 handle) {
        return new PhysicalResourceIdV2.BookKeeperLedger(
                f.binding.physicalNamespace(), handle.ledgerIdentity().ledgerId());
    }

    private static OxiaQuotaTargetDeleteStoreV2 route(Fixture f, KafkaBookKeeperDeleteObservationAuthorityV2 gc) {
        var namespace = await(OxiaPhysicalMetadataNamespaceV2.connect(f.oxia, f.binding.metadataNamespace()));
        return await(namespace.openAuthorityRoute(
                f.backend, gc.readOnlyFacts(new Oxia09ExactMetadataTransactionStoreV1(f.oxia))));
    }

    private static BiFunction<String, CanonicalBytes, AuthorityFactV1> facts(Fixture f, PhysicalResourceIdV2 resource) {
        var facts = new Oxia09ExactMetadataTransactionStoreV1(f.oxia);
        return (suffix, bytes) -> {
            String key = "/native-gc-bound-facts/" + resource.sha256().toHex() + suffix;
            if (await(facts.read(key)).isEmpty()) {
                await(facts.compareAndSet(Optional.empty(), key, bytes));
            }
            var actual = await(facts.read(key)).orElseThrow();
            assertThat(actual.canonicalStoredBytes()).isEqualTo(bytes);
            return new AuthorityFactV1(key, actual.metadataVersion(), actual.canonicalStoredSha256());
        };
    }

    private static M5TargetDeleteAuthorityCoordinatorV1 coordinator(
            Fixture f,
            RunLedgerHandleV1 handle,
            KafkaBookKeeperDeleteObservationAuthorityV2 gc,
            OxiaQuotaTargetDeleteStoreV2 route) {
        return new M5TargetDeleteAuthorityCoordinatorV1(
                route, gc, new KafkaBookKeeperDeleteIdentityReaderV2(f.source, handle));
    }

    private static VersionedValue open(
            M5TargetDeleteAuthorityCoordinatorV1 coordinator,
            PhysicalResourceIdV2 resource,
            BiFunction<String, CanonicalBytes, AuthorityFactV1> facts) {
        var template = SyntheticDeleteAuthorityFixturesV2.phases(resource).get(0);
        return await(coordinator.create(M5TargetDeleteAuthorityStateMachineV1.open(
                        template.target(),
                        template.writerEnrollment(),
                        SyntheticDeleteAuthorityFixturesV2.replacement(resource, 1, facts))))
                .observed()
                .orElseThrow();
    }

    private static VersionedValue intent(
            Fixture f,
            RunLedgerHandleV1 handle,
            M5TargetDeleteAuthorityCoordinatorV1 coordinator,
            VersionedValue open,
            DeleteObservationContextV2 observation) {
        var fenced = await(coordinator.prepareIdentityRead(
                        open, SyntheticDeleteAuthorityFixturesV2.digest("bound-read"), observation))
                .observed()
                .orElseThrow();
        var reader = new KafkaBookKeeperDeleteIdentityReaderV2(f.source, handle);
        var identity =
                await(reader.capture(M5TargetDeleteAuthorityCodecV1.decodeAuthority(fenced.canonicalStoredBytes())));
        return await(coordinator.bindDeleteIntent(
                        fenced, identity, SyntheticDeleteAuthorityFixturesV2.digest("bound-delete")))
                .observed()
                .orElseThrow();
    }

    private static Path checkpoint() {
        return Path.of(System.getProperty("nereus.m5.boundDelete.restartCheckpoint"));
    }

    private static Path unknownCheckpoint() {
        return Path.of(checkpoint() + "-unknown");
    }

    private static <T> T await(CompletionStage<T> stage) {
        try {
            return stage.toCompletableFuture().get(30, TimeUnit.SECONDS);
        } catch (Exception failure) {
            throw new IllegalStateException("bound GC test operation failed", failure);
        }
    }
}
