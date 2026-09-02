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

package com.nereusstream.storage.object.read.control;

import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.assertThatThrownBy;
import com.nereusstream.domain.bytes.CanonicalBytes;
import com.nereusstream.domain.bytes.Sha256Digest;
import com.nereusstream.domain.identity.StorageEpochId;
import com.nereusstream.domain.identity.TopicBindingId;
import com.nereusstream.storage.object.control.CanonicalControlMetadataStore;
import com.nereusstream.storage.object.control.ControlMutationOutcome;
import com.nereusstream.storage.object.read.BindingReadAuthorityV1;
import com.nereusstream.storage.object.read.BindingReadBatchContextV1;
import com.nereusstream.storage.object.read.BindingReadHazardPoolV1;
import com.nereusstream.storage.object.read.BindingReadProtocolV1;
import com.nereusstream.storage.object.read.BindingReadPublicationCellV1;
import com.nereusstream.storage.object.read.BindingReadRouteTableV1;
import com.nereusstream.storage.object.read.control.M4ProofCleanupPlannerV1.EpochInterval;
import com.nereusstream.storage.object.read.control.M4ProofCleanupPlannerV1.ReferenceSnapshot;
import com.nereusstream.storage.object.read.control.M4ReadControlCoordinatorV1.Outcome;
import com.nereusstream.storage.object.read.control.M4ReadControlRecordsV1.AdmissionState;
import com.nereusstream.storage.object.read.control.M4ReadControlRecordsV1.BindingIdentity;
import com.nereusstream.storage.object.read.control.M4ReadControlRecordsV1.BindingReadSelector;
import com.nereusstream.storage.object.read.control.M4ReadControlRecordsV1.CapabilityBinding;
import com.nereusstream.storage.object.read.control.M4ReadControlRecordsV1.CapabilityEvidence;
import com.nereusstream.storage.object.read.control.M4ReadControlRecordsV1.CapabilityKind;
import com.nereusstream.storage.object.read.control.M4ReadControlRecordsV1.CapabilityState;
import com.nereusstream.storage.object.read.control.M4ReadControlRecordsV1.ClosureAnchor;
import com.nereusstream.storage.object.read.control.M4ReadControlRecordsV1.ProtectionState;
import com.nereusstream.storage.object.read.control.M4ReadControlRecordsV1.QuiescenceProofHead;
import com.nereusstream.storage.object.read.control.M4ReadControlRecordsV1.ReadAdmissionEpochTerminalCut;
import com.nereusstream.storage.object.read.control.M4ReadControlRecordsV1.ReadQuiescenceProof;
import com.nereusstream.storage.object.read.control.M4ReadControlRecordsV1.SelectorMode;
import com.nereusstream.storage.object.read.control.M4ReadControlRecordsV1.SourceProtection;
import com.nereusstream.storage.object.read.control.M4ReadControlRecordsV1.SourceProtectionIdentity;
import com.nereusstream.storage.object.read.control.M4ReadControlRecordsV1.SourceRetirementBatch;
import com.nereusstream.storage.object.read.control.M4ReadControlRecordsV1.TerminalKind;
import java.nio.charset.StandardCharsets;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.LinkedHashMap;
import java.util.List;
import java.util.Locale;
import java.util.Map;
import java.util.Optional;
import java.util.concurrent.atomic.AtomicReference;
import org.junit.jupiter.api.Test;

class M4ReadControlCoordinatorV1Test {
    @Test
    void codecsRoundTripAllClosedRecordsAndRejectNonCanonicalBytes() {
        Fixture fixture = new Fixture(CapabilityKind.AUTHORITY_EXPIRY_V1);
        SourceProtectionIdentity source = fixture.source("source-a", 2, 1, 7);
        BindingReadSelector selector = fixture.fallbackSelector(List.of(source), List.of(), List.of(), 1);

        assertThat(M4ReadControlCodecV1.decodeCapability(
                        M4ReadControlCodecV1.encodeCapability(fixture.capabilityEvidence)))
                .isEqualTo(fixture.capabilityEvidence);
        assertThat(M4ReadControlCodecV1.decodeSelector(M4ReadControlCodecV1.encodeSelector(selector)))
                .isEqualTo(selector);
        assertThat(M4ReadControlCodecV1.decodeProtection(
                        M4ReadControlCodecV1.encodeProtection(fixture.protection(source))))
                .isEqualTo(fixture.protection(source));

        CanonicalBytes encoded = M4ReadControlCodecV1.encodeSelector(selector);
        byte[] trailing = Arrays.copyOf(encoded.toByteArray(), encoded.length() + 1);
        assertThatThrownBy(() -> M4ReadControlCodecV1.decodeSelector(CanonicalBytes.copyOf(trailing)))
                .isInstanceOf(IllegalArgumentException.class);
    }

    @Test
    void fusedClosureAtomicallyFreezesBatchClosesEpochAndGrantsPreferredOnlySuccessor() {
        Fixture fixture = new Fixture(CapabilityKind.DURABLE_DRAIN_ONLY_V1);
        SourceProtectionIdentity source = fixture.source("source-a", 3, 1, 7);
        BindingReadSelector predecessor = fixture.fallbackSelector(List.of(source), List.of(), List.of(), 1);
        fixture.install(predecessor, List.of(source));
        fixture.store.nextMode = NextMode.APPLY_BUT_UNKNOWN;

        assertThat(fixture.coordinator.closeFallback(predecessor, digest("preferred-only"), 8, List.of(source)))
                .isEqualTo(Outcome.EXISTING_EXACT);
        BindingReadSelector successor = fixture.coordinator.readSelector().orElseThrow();
        assertThat(successor.mode()).isEqualTo(SelectorMode.PREFERRED_ONLY);
        assertThat(successor.admissionState()).isEqualTo(AdmissionState.ADMITTING);
        assertThat(successor.readAdmissionEpoch()).isEqualTo(2);
        assertThat(successor.sourceGeneration()).isEqualTo(8);
        assertThat(successor.pendingAnchors()).hasSize(1);
        assertThat(successor.activeBatches()).singleElement().satisfies(batch -> {
            assertThat(batch.sharedLastFallbackCapableReadAdmissionEpoch()).isEqualTo(1);
            assertThat(batch.sources()).containsExactly(source);
            assertThat(batch.fallbackSetSha256())
                    .isEqualTo(M4ReadControlCodecV1.calculateFallbackSetSha256(List.of(source)));
        });

        assertThat(fixture.coordinator.closeFallback(predecessor, digest("preferred-only"), 8, List.of(source)))
                .isEqualTo(Outcome.EXISTING_EXACT);
        assertThat(fixture.coordinator.closeFallback(predecessor, digest("different-successor"), 8, List.of(source)))
                .isEqualTo(Outcome.CONFLICT);
    }

    @Test
    void takeoverFallbackIntroductionAndMembershipNeutralUpdateShareExactSelectorCas() {
        Fixture takeover = new Fixture(CapabilityKind.DURABLE_DRAIN_ONLY_V1);
        SourceProtectionIdentity existingSource = takeover.source("existing", 1, 1, 7);
        BindingReadSelector fallback = takeover.fallbackSelector(List.of(existingSource), List.of(), List.of(), 1);
        takeover.install(fallback, List.of(existingSource));

        assertThat(takeover.coordinator.grantTakeover(fallback, digest("owner-two-view"), 2, 8))
                .isEqualTo(Outcome.APPLIED);
        BindingReadSelector afterTakeover = takeover.coordinator.readSelector().orElseThrow();
        assertThat(afterTakeover.ownerEpoch()).isEqualTo(2);
        assertThat(afterTakeover.readAdmissionEpoch()).isEqualTo(2);
        assertThat(afterTakeover.pendingAnchors()).hasSize(1);
        assertThat(takeover.coordinator.closeFallback(fallback, digest("stale-close"), 8, List.of(existingSource)))
                .isEqualTo(Outcome.CONFLICT);

        Fixture introduction = new Fixture(CapabilityKind.DURABLE_DRAIN_ONLY_V1);
        SourceProtectionIdentity newSource = introduction.source("new", 2, 2, 9);
        BindingReadSelector preferred = introduction.preferredSelector(1, 7);
        introduction.install(preferred, List.of(newSource));
        assertThat(introduction.coordinator.introduceFallback(
                        preferred, digest("fallback-view"), 8, List.of(newSource)))
                .isEqualTo(Outcome.APPLIED);
        BindingReadSelector introduced = introduction.coordinator.readSelector().orElseThrow();
        assertThat(introduced.mode()).isEqualTo(SelectorMode.PREFERRED_WITH_FALLBACK);
        assertThat(introduced.readAdmissionEpoch()).isEqualTo(2);
        assertThat(introduced.pendingAnchors()).isEmpty();

        assertThat(introduction.coordinator.updateMembershipNeutralView(
                        introduced, digest("same-membership-view"), 9, List.of(newSource)))
                .isEqualTo(Outcome.APPLIED);
        BindingReadSelector neutral = introduction.coordinator.readSelector().orElseThrow();
        assertThat(neutral.readAdmissionEpoch()).isEqualTo(2);
        assertThat(neutral.fallbackSetSha256()).isEqualTo(introduced.fallbackSetSha256());
        assertThatThrownBy(() -> introduction.coordinator.updateMembershipNeutralView(
                        neutral, digest("missing-membership"), 10, List.of()))
                .isInstanceOf(IllegalArgumentException.class);
    }

    @Test
    void anchorCapacityClosesAdmissionIntoReservedStoppedEnvelopeAndFreshEpochCanResumeAfterPrune() {
        Fixture fixture = new Fixture(CapabilityKind.DURABLE_DRAIN_ONLY_V1);
        SourceProtectionIdentity source = fixture.source("source-a", 1, 1, 4);
        List<ClosureAnchor> anchors = new ArrayList<>();
        for (long epoch = 1; epoch <= M4ReadControlRecordsV1.MAX_PENDING_ANCHORS - 1L; epoch++) {
            anchors.add(fixture.anchor(epoch));
        }
        BindingReadSelector predecessor = fixture.fallbackSelector(
                List.of(source), anchors, List.of(), M4ReadControlRecordsV1.MAX_PENDING_ANCHORS);
        fixture.install(predecessor, List.of(source));

        assertThat(fixture.coordinator.closeFallback(predecessor, digest("preferred-only"), 8, List.of(source)))
                .isEqualTo(Outcome.STOPPED);
        BindingReadSelector stopped = fixture.coordinator.readSelector().orElseThrow();
        assertThat(stopped.admissionState()).isEqualTo(AdmissionState.STOPPED);
        assertThat(stopped.readAdmissionEpoch()).isEqualTo(M4ReadControlRecordsV1.MAX_PENDING_ANCHORS + 1L);
        assertThat(stopped.pendingAnchors()).hasSize(M4ReadControlRecordsV1.MAX_PENDING_ANCHORS);
        assertThat(stopped.activeBatches()).isEmpty();

        ReadAdmissionEpochTerminalCut terminal =
                fixture.terminal(stopped.pendingAnchors().get(0), 11, 1_000);
        fixture.store.put(fixture.keys.terminal(1), M4ReadControlCodecV1.encodeTerminal(terminal));
        assertThat(fixture.coordinator.pruneTerminalBackedAnchors(stopped, 1)).isEqualTo(Outcome.APPLIED);
        BindingReadSelector pruned = fixture.coordinator.readSelector().orElseThrow();
        assertThat(pruned.pendingAnchors()).hasSize(M4ReadControlRecordsV1.MAX_PENDING_ANCHORS - 1);
        assertThat(fixture.coordinator.resumeStopped(pruned, pruned.ownerEpoch() + 1))
                .isEqualTo(Outcome.APPLIED);
        BindingReadSelector resumed = fixture.coordinator.readSelector().orElseThrow();
        assertThat(resumed.admissionState()).isEqualTo(AdmissionState.ADMITTING);
        assertThat(resumed.readAdmissionEpoch()).isEqualTo(pruned.readAdmissionEpoch() + 1);
    }

    @Test
    void terminalCreateAdoptsDifferentValidVariantAndQuarantinesInvalidOccupant() {
        Fixture fixture = new Fixture(CapabilityKind.AUTHORITY_EXPIRY_V1);
        SourceProtectionIdentity source = fixture.source("source-a", 1, 1, 7);
        BindingReadSelector predecessor = fixture.fallbackSelector(List.of(source), List.of(), List.of(), 1);
        fixture.install(predecessor, List.of(source));
        assertThat(fixture.coordinator.closeFallback(predecessor, digest("preferred-only"), 8, List.of(source)))
                .isEqualTo(Outcome.APPLIED);
        ClosureAnchor anchor = fixture.coordinator
                .readSelector()
                .orElseThrow()
                .pendingAnchors()
                .get(0);
        ReadAdmissionEpochTerminalCut planned = fixture.terminal(anchor, 11, 1_000);
        ReadAdmissionEpochTerminalCut expiry = new ReadAdmissionEpochTerminalCut(
                fixture.binding,
                M4ReadControlCodecV1.anchorSha256(anchor),
                anchor.closedReadAdmissionEpoch(),
                anchor.ownerEpoch(),
                12,
                11_150,
                fixture.capability,
                TerminalKind.QUALIFIED_EXPIRY,
                digest("owner-fence"),
                digest("qualified-expiry-evidence"),
                1_000,
                12_000,
                2);
        fixture.store.put(fixture.keys.terminal(1), M4ReadControlCodecV1.encodeTerminal(expiry));

        assertThat(fixture.coordinator.publishTerminal(planned)).isEqualTo(Outcome.ADOPTED_DIFFERENT_VALID_TERMINAL);

        Fixture invalid = new Fixture(CapabilityKind.DURABLE_DRAIN_ONLY_V1);
        SourceProtectionIdentity invalidSource = invalid.source("source", 1, 1, 3);
        BindingReadSelector invalidPredecessor =
                invalid.fallbackSelector(List.of(invalidSource), List.of(), List.of(), 1);
        invalid.install(invalidPredecessor, List.of(invalidSource));
        invalid.coordinator.closeFallback(invalidPredecessor, digest("preferred"), 8, List.of(invalidSource));
        ClosureAnchor invalidAnchor = invalid.coordinator
                .readSelector()
                .orElseThrow()
                .pendingAnchors()
                .get(0);
        ReadAdmissionEpochTerminalCut wrong = new ReadAdmissionEpochTerminalCut(
                invalid.binding,
                digest("wrong-anchor"),
                1,
                1,
                3,
                1_000,
                invalid.capability,
                TerminalKind.PLANNED_DRAIN,
                digest("closed-fence"),
                digest("bad"),
                0,
                0,
                1);
        invalid.store.put(invalid.keys.terminal(1), M4ReadControlCodecV1.encodeTerminal(wrong));
        assertThat(invalid.coordinator.publishTerminal(invalid.terminal(invalidAnchor, 3, 1_000)))
                .isEqualTo(Outcome.QUARANTINED_INVALID_OCCUPANT);
    }

    @Test
    void exactProofAndHazardDrainGateIrreversibleProtectionReleaseAndResponseLoss() {
        Fixture fixture = new Fixture(CapabilityKind.DURABLE_DRAIN_ONLY_V1);
        SourceProtectionIdentity source = fixture.source("source-a", 4, 1, 7);
        BindingReadSelector predecessor = fixture.fallbackSelector(List.of(source), List.of(), List.of(), 1);
        fixture.install(predecessor, List.of(source));
        fixture.coordinator.closeFallback(predecessor, digest("preferred-only"), 8, List.of(source));
        BindingReadSelector successor = fixture.coordinator.readSelector().orElseThrow();
        ClosureAnchor anchor = successor.pendingAnchors().get(0);
        ReadAdmissionEpochTerminalCut terminal = fixture.terminal(anchor, 7, 1_000);
        assertThat(fixture.coordinator.publishTerminal(terminal)).isEqualTo(Outcome.APPLIED);
        assertThat(fixture.coordinator.publishProof(fixture.proof(terminal, 7, 1_000)))
                .isEqualTo(Outcome.APPLIED);

        BindingReadHazardPoolV1 hazards = new BindingReadHazardPoolV1(1, 4);
        BindingReadBatchContextV1 batchContext = new BindingReadBatchContextV1();
        AtomicReference<BindingReadAuthorityV1> authority =
                new AtomicReference<>(fixture.localAuthority(source.fallbackSourceGeneration()));
        assertThat(hazards.tryCapture(authority, batchContext))
                .isEqualTo(BindingReadHazardPoolV1.CaptureOutcome.CAPTURED);
        SourceRetirementBatch batch = successor.activeBatches().get(0);
        assertThat(fixture.coordinator.releaseProtection(batch, source, hazards))
                .isEqualTo(Outcome.RETAIN);

        batchContext.closeNewSourceUse();
        assertThat(batchContext.terminalClearExactLease()).isTrue();
        fixture.store.nextMode = NextMode.APPLY_BUT_UNKNOWN;
        assertThat(fixture.coordinator.releaseProtection(batch, source, hazards))
                .isEqualTo(Outcome.EXISTING_EXACT);
        SourceProtection released = M4ReadControlCodecV1.decodeProtection(fixture.store
                .get(fixture.keys.protection(source.sourceIdentitySha256(), source.protectionGeneration()))
                .orElseThrow());
        assertThat(released.state()).isEqualTo(ProtectionState.RELEASED);
        assertThat(released.releasedByBatchSha256()).contains(batch.batchIdSha256());
    }

    @Test
    void localAdmissionClosesBeforeUnknownSelectorResponseAndOldCapturedGenerationSurvives() {
        Fixture fixture = new Fixture(CapabilityKind.DURABLE_DRAIN_ONLY_V1);
        SourceProtectionIdentity source = fixture.source("source-a", 1, 1, 7);
        BindingReadSelector predecessor = fixture.fallbackSelector(List.of(source), List.of(), List.of(), 1);
        fixture.install(predecessor, List.of(source));
        BindingReadAuthorityV1 predecessorAuthority =
                fixture.authority(predecessor.selectedViewSha256(), predecessor.ownerEpoch(), 1, true, 7);
        BindingReadAuthorityV1 successorAuthority =
                fixture.authority(digest("preferred-only"), predecessor.ownerEpoch(), 2, true, 8);
        BindingReadSelectorRuntimeV1 runtime = new BindingReadSelectorRuntimeV1(
                fixture.binding, fixture.coordinator, predecessor, predecessorAuthority);
        BindingReadHazardPoolV1 hazards = new BindingReadHazardPoolV1(2, 4);
        BindingReadBatchContextV1 oldRead = new BindingReadBatchContextV1();
        assertThat(hazards.tryCapture(runtime.currentAuthority(), oldRead))
                .isEqualTo(BindingReadHazardPoolV1.CaptureOutcome.CAPTURED);

        fixture.store.nextMode = NextMode.UNKNOWN_WITHOUT_APPLY;
        assertThat(runtime.closeFallback(
                        predecessor,
                        predecessorAuthority,
                        successorAuthority,
                        digest("preferred-only"),
                        8,
                        List.of(source)))
                .isEqualTo(Outcome.RETRY_EXACT_PREDECESSOR);
        assertThat(runtime.currentAuthority().get().admitting()).isFalse();
        assertThat(hazards.tryCapture(runtime.currentAuthority(), new BindingReadBatchContextV1()))
                .isEqualTo(BindingReadHazardPoolV1.CaptureOutcome.ADMISSION_CLOSED);
        assertThat(hazards.scan(fixture.binding.bindingId(), 7)).isEqualTo(BindingReadHazardPoolV1.ScanOutcome.PINNED);

        assertThat(runtime.closeFallback(
                        predecessor,
                        predecessorAuthority,
                        successorAuthority,
                        digest("preferred-only"),
                        8,
                        List.of(source)))
                .isEqualTo(Outcome.APPLIED);
        BindingReadBatchContextV1 newRead = new BindingReadBatchContextV1();
        assertThat(hazards.tryCapture(runtime.currentAuthority(), newRead))
                .isEqualTo(BindingReadHazardPoolV1.CaptureOutcome.CAPTURED);
        assertThat(hazards.scan(fixture.binding.bindingId(), 8)).isEqualTo(BindingReadHazardPoolV1.ScanOutcome.PINNED);

        oldRead.closeNewSourceUse();
        assertThat(oldRead.terminalClearExactLease()).isTrue();
        newRead.closeNewSourceUse();
        assertThat(newRead.terminalClearExactLease()).isTrue();
    }

    @Test
    void proofIntervalUsesEachHistoricalCapabilityAndRevocationFailsSafe() {
        Fixture fixture = new Fixture(CapabilityKind.DURABLE_DRAIN_ONLY_V1);
        fixture.coordinator.createCapability(fixture.capabilityEvidence);
        fixture.coordinator.createSelector(fixture.preferredSelector(3, 7));
        CapabilityEvidence secondEvidence = fixture.capabilityEvidence(2, CapabilityKind.AUTHORITY_EXPIRY_V1);
        CapabilityBinding second = fixture.binding(secondEvidence);
        assertThat(fixture.coordinator.createCapability(secondEvidence)).isEqualTo(Outcome.APPLIED);

        ReadAdmissionEpochTerminalCut firstTerminal = fixture.detachedTerminal(1, fixture.capability);
        ReadAdmissionEpochTerminalCut secondTerminal = fixture.detachedTerminal(2, second);
        fixture.store.put(fixture.keys.terminal(1), M4ReadControlCodecV1.encodeTerminal(firstTerminal));
        fixture.store.put(fixture.keys.terminal(2), M4ReadControlCodecV1.encodeTerminal(secondTerminal));
        assertThat(fixture.coordinator.publishProof(fixture.proof(firstTerminal, 1, 1_000)))
                .isEqualTo(Outcome.APPLIED);
        assertThat(fixture.coordinator.publishProof(fixture.proof(secondTerminal, 2, 1_000)))
                .isEqualTo(Outcome.APPLIED);
        QuiescenceProofHead head = fixture.store
                .get(fixture.keys.proofHead())
                .map(M4ReadControlCodecV1::decodeHead)
                .orElseThrow();

        assertThat(fixture.coordinator.verifyInterval(head, 1, 2)).isTrue();
        assertThat(fixture.coordinator.revokeCapability(1)).isEqualTo(Outcome.APPLIED);
        assertThat(fixture.coordinator.verifyInterval(head, 1, 2)).isFalse();
    }

    @Test
    void proofHeadFoldsBoundedContiguousEntriesAndRejectsAGap() {
        Fixture fixture = new Fixture(CapabilityKind.DURABLE_DRAIN_ONLY_V1);
        fixture.coordinator.createCapability(fixture.capabilityEvidence);
        fixture.coordinator.createSelector(fixture.preferredSelector(66, 7));
        for (long epoch = 1; epoch <= M4ReadControlRecordsV1.MAX_PROOF_WINDOW + 1L; epoch++) {
            ReadAdmissionEpochTerminalCut terminal = fixture.detachedTerminal(epoch, fixture.capability);
            fixture.store.put(fixture.keys.terminal(epoch), M4ReadControlCodecV1.encodeTerminal(terminal));
            assertThat(fixture.coordinator.publishProof(fixture.proof(terminal, epoch, 1_000)))
                    .isIn(Outcome.APPLIED, Outcome.EXISTING_EXACT);
        }
        QuiescenceProofHead head = fixture.store
                .get(fixture.keys.proofHead())
                .map(M4ReadControlCodecV1::decodeHead)
                .orElseThrow();
        assertThat(head.folds()).singleElement().satisfies(fold -> {
            assertThat(fold.firstEpoch()).isEqualTo(1);
            assertThat(fold.lastEpoch()).isEqualTo(32);
        });
        assertThat(head.window()).hasSize(33);
        assertThat(fixture.coordinator.verifyInterval(head, 1, 65)).isTrue();

        Fixture gap = new Fixture(CapabilityKind.DURABLE_DRAIN_ONLY_V1);
        gap.coordinator.createCapability(gap.capabilityEvidence);
        gap.coordinator.createSelector(gap.preferredSelector(4, 7));
        for (long epoch : new long[] {1, 3}) {
            ReadAdmissionEpochTerminalCut terminal = gap.detachedTerminal(epoch, gap.capability);
            gap.store.put(gap.keys.terminal(epoch), M4ReadControlCodecV1.encodeTerminal(terminal));
            gap.coordinator.publishProof(gap.proof(terminal, epoch, 1_000));
        }
        QuiescenceProofHead gapHead = gap.store
                .get(gap.keys.proofHead())
                .map(M4ReadControlCodecV1::decodeHead)
                .orElseThrow();
        assertThat(gap.coordinator.verifyInterval(gapHead, 1, 3)).isFalse();

        Fixture selectorless = new Fixture(CapabilityKind.DURABLE_DRAIN_ONLY_V1);
        selectorless.coordinator.createCapability(selectorless.capabilityEvidence);
        ReadAdmissionEpochTerminalCut detached = selectorless.detachedTerminal(1, selectorless.capability);
        selectorless.store.put(selectorless.keys.terminal(1), M4ReadControlCodecV1.encodeTerminal(detached));
        assertThatThrownBy(() -> selectorless.coordinator.publishProof(selectorless.proof(detached, 1, 1_000)))
                .isInstanceOf(IllegalStateException.class)
                .hasMessageContaining("Binding selector");
    }

    @Test
    void proofCapacityStopsSelectorWithoutDroppingOrSkippingAnEpoch() {
        Fixture fixture = new Fixture(CapabilityKind.DURABLE_DRAIN_ONLY_V1);
        assertThat(fixture.coordinator.createCapability(fixture.capabilityEvidence))
                .isEqualTo(Outcome.APPLIED);
        long admittedEpochs = M4ReadControlRecordsV1.MAX_PROOF_FOLDS * (long) M4ReadControlRecordsV1.PROOF_FOLD_ENTRIES
                + M4ReadControlRecordsV1.MAX_PROOF_WINDOW;
        assertThat(fixture.coordinator.createSelector(fixture.preferredSelector(admittedEpochs + 1, 7)))
                .isEqualTo(Outcome.APPLIED);
        long[] latencyNanos = new long[Math.toIntExact(admittedEpochs + 1)];
        long mutationBaseline = fixture.store.mutations;
        long elapsedStart = System.nanoTime();
        for (long epoch = 1; epoch <= admittedEpochs + 1; epoch++) {
            ReadAdmissionEpochTerminalCut terminal = fixture.detachedTerminal(epoch, fixture.capability);
            fixture.store.put(fixture.keys.terminal(epoch), M4ReadControlCodecV1.encodeTerminal(terminal));
            long started = System.nanoTime();
            Outcome outcome = fixture.coordinator.publishProof(fixture.proof(terminal, epoch, 1_000));
            latencyNanos[Math.toIntExact(epoch - 1)] = System.nanoTime() - started;
            if (epoch <= admittedEpochs) {
                assertThat(outcome).isIn(Outcome.APPLIED, Outcome.EXISTING_EXACT);
            } else {
                assertThat(outcome).isEqualTo(Outcome.STOPPED);
            }
        }
        long elapsedNanos = System.nanoTime() - elapsedStart;
        QuiescenceProofHead head = fixture.store
                .get(fixture.keys.proofHead())
                .map(M4ReadControlCodecV1::decodeHead)
                .orElseThrow();
        BindingReadSelector stopped = fixture.coordinator.readSelector().orElseThrow();
        assertThat(head.folds()).hasSize(M4ReadControlRecordsV1.MAX_PROOF_FOLDS);
        assertThat(head.window()).hasSize(M4ReadControlRecordsV1.MAX_PROOF_WINDOW);
        assertThat(head.folds().get(0).firstEpoch()).isOne();
        assertThat(head.window().get(head.window().size() - 1).readAdmissionEpoch())
                .isEqualTo(admittedEpochs);
        assertThat(stopped.admissionState()).isEqualTo(AdmissionState.STOPPED);
        assertThat(stopped.readAdmissionEpoch()).isEqualTo(admittedEpochs + 2);
        assertThat(fixture.store.get(fixture.keys.proof(admittedEpochs + 1))).isPresent();
        Arrays.sort(latencyNanos);
        long p99Nanos = percentile(latencyNanos, 99);
        long measuredThroughput = throughput(admittedEpochs + 1, elapsedNanos);
        assertThat(p99Nanos).isLessThan(10_000_000);
        assertThat(measuredThroughput).isGreaterThan(100);
        System.out.printf(
                Locale.ROOT,
                "M4_METRIC CONTROL_CAPACITY attemptedProofs=%d admittedProofs=%d folds=%d "
                        + "windowEntries=%d stoppedEpoch=%d pendingProofRows=1 elapsedNanos=%d p99Nanos=%d "
                        + "throughputOpsPerSecond=%d metadataMutations=%d proofIntervalEpochCap=%d%n",
                admittedEpochs + 1,
                admittedEpochs,
                head.folds().size(),
                head.window().size(),
                admittedEpochs + 1,
                elapsedNanos,
                p99Nanos,
                measuredThroughput,
                fixture.store.mutations - mutationBaseline,
                M4ReadControlRecordsV1.MAX_PROOF_INTERVAL_EPOCHS);
    }

    @Test
    void pureCleanupPlannerRequiresDurableFoldAndEveryReferenceClassToDisappear() {
        Fixture fixture = new Fixture(CapabilityKind.DURABLE_DRAIN_ONLY_V1);
        assertThat(fixture.coordinator.createCapability(fixture.capabilityEvidence))
                .isEqualTo(Outcome.APPLIED);
        BindingReadSelector selector = fixture.preferredSelector(100, 7);
        assertThat(fixture.coordinator.createSelector(selector)).isEqualTo(Outcome.APPLIED);
        List<ReadAdmissionEpochTerminalCut> terminals = new ArrayList<>();
        List<ReadQuiescenceProof> proofs = new ArrayList<>();
        for (long epoch = 1; epoch <= M4ReadControlRecordsV1.MAX_PROOF_WINDOW + 1L; epoch++) {
            ReadAdmissionEpochTerminalCut terminal = fixture.detachedTerminal(epoch, fixture.capability);
            ReadQuiescenceProof proof = fixture.proof(terminal, epoch, 1_000);
            terminals.add(terminal);
            proofs.add(proof);
            fixture.store.put(fixture.keys.terminal(epoch), M4ReadControlCodecV1.encodeTerminal(terminal));
            assertThat(fixture.coordinator.publishProof(proof)).isIn(Outcome.APPLIED, Outcome.EXISTING_EXACT);
        }
        QuiescenceProofHead head = fixture.store
                .get(fixture.keys.proofHead())
                .map(M4ReadControlCodecV1::decodeHead)
                .orElseThrow();
        ReferenceSnapshot none = references(fixture.binding, 1, List.of(), List.of(), List.of(), List.of());
        M4ProofCleanupPlannerV1.CleanupPlan plan = M4ProofCleanupPlannerV1.plan(
                        selector,
                        head,
                        terminals,
                        proofs,
                        List.of(fixture.capabilityEvidence),
                        none,
                        M4ProofCleanupPlannerV1.MAX_CLEANUP_RECORDS)
                .orElseThrow();
        assertThat(plan.rows()).hasSize(32);
        assertThat(plan.references()).isEqualTo(none);
        assertThat(plan.rows())
                .extracting(row -> row.readAdmissionEpoch())
                .containsExactlyElementsOf(
                        java.util.stream.LongStream.rangeClosed(1, 32).boxed().toList());
        QuiescenceProofHead successor = M4ReadControlCodecV1.decodeHead(plan.successorHeadBytes());
        assertThat(successor.generation()).isEqualTo(head.generation() + 1);
        assertThat(successor.folds()).isEmpty();
        assertThat(successor.window()).hasSize(33);
        ReferenceSnapshot wrongBinding = references(
                new BindingIdentity(
                        new TopicBindingId(digest("other-binding")),
                        digest("other-incarnation"),
                        digest("other-epoch")),
                2,
                List.of(),
                List.of(),
                List.of(),
                List.of());
        assertThatThrownBy(() -> M4ProofCleanupPlannerV1.plan(
                        selector,
                        head,
                        terminals,
                        proofs,
                        List.of(fixture.capabilityEvidence),
                        wrongBinding,
                        M4ProofCleanupPlannerV1.MAX_CLEANUP_RECORDS))
                .isInstanceOf(IllegalArgumentException.class)
                .hasMessageContaining("snapshot Binding");

        List<ReferenceSnapshot> externalBlocks = List.of(
                references(fixture.binding, 2, List.of(new EpochInterval(1, 1)), List.of(), List.of(), List.of()),
                references(fixture.binding, 3, List.of(), List.of(1L), List.of(), List.of()),
                references(fixture.binding, 4, List.of(), List.of(), List.of(1L), List.of()),
                references(fixture.binding, 5, List.of(), List.of(), List.of(), List.of(1L)));
        for (ReferenceSnapshot blocked : externalBlocks) {
            assertThat(M4ProofCleanupPlannerV1.plan(
                            selector,
                            head,
                            terminals,
                            proofs,
                            List.of(fixture.capabilityEvidence),
                            blocked,
                            M4ProofCleanupPlannerV1.MAX_CLEANUP_RECORDS))
                    .isEmpty();
        }

        BindingReadSelector anchorReferenced = new BindingReadSelector(
                selector.binding(),
                selector.selectedViewSha256(),
                selector.ownerEpoch(),
                selector.readAdmissionEpoch(),
                selector.sourceGeneration(),
                selector.mode(),
                selector.admissionState(),
                selector.fallbackSetSha256(),
                selector.capability(),
                List.of(fixture.anchor(1)),
                List.of());
        assertThat(M4ProofCleanupPlannerV1.plan(
                        anchorReferenced,
                        head,
                        terminals,
                        proofs,
                        List.of(fixture.capabilityEvidence),
                        none,
                        M4ProofCleanupPlannerV1.MAX_CLEANUP_RECORDS))
                .isEmpty();

        Fixture batchFixture = new Fixture(CapabilityKind.DURABLE_DRAIN_ONLY_V1);
        SourceProtectionIdentity source = batchFixture.source("cleanup-active-source", 1, 1, 7);
        BindingReadSelector fallback = batchFixture.fallbackSelector(List.of(source), List.of(), List.of(), 1);
        batchFixture.install(fallback, List.of(source));
        assertThat(batchFixture.coordinator.closeFallback(fallback, digest("cleanup-preferred"), 8, List.of(source)))
                .isEqualTo(Outcome.APPLIED);
        BindingReadSelector batchReferenced =
                batchFixture.coordinator.readSelector().orElseThrow();
        assertThat(M4ProofCleanupPlannerV1.plan(
                        batchReferenced,
                        head,
                        terminals,
                        proofs,
                        List.of(fixture.capabilityEvidence),
                        none,
                        M4ProofCleanupPlannerV1.MAX_CLEANUP_RECORDS))
                .isEmpty();
        assertThatThrownBy(() -> M4ProofCleanupPlannerV1.plan(
                        selector,
                        head,
                        terminals,
                        proofs,
                        List.of(fixture.capabilityEvidence.revoke()),
                        none,
                        M4ProofCleanupPlannerV1.MAX_CLEANUP_RECORDS))
                .isInstanceOf(IllegalArgumentException.class)
                .hasMessageContaining("capability");
        System.out.printf(
                Locale.ROOT,
                "M4_METRIC CONTROL_CLEANUP plannedRows=%d terminalRowsRetirable=%d proofRowsRetirable=%d "
                        + "removedFolds=1 successorWindowEntries=%d blockedReferenceKinds=6 "
                        + "referenceGenerationBound=1 publicationFenceShaBound=1 cleanupBatchCap=%d%n",
                plan.rows().size(),
                plan.rows().size(),
                plan.rows().size(),
                successor.window().size(),
                M4ProofCleanupPlannerV1.MAX_CLEANUP_RECORDS);
    }

    private static final class Fixture {
        private final MapStore store = new MapStore();
        private final BindingIdentity binding = new BindingIdentity(
                new TopicBindingId(digest("binding")), digest("incarnation"), digest("storage-epoch"));
        private final CapabilityEvidence capabilityEvidence;
        private final CapabilityBinding capability;
        private final M4ReadControlKeysV1 keys = new M4ReadControlKeysV1(7, binding);
        private final M4ReadControlCoordinatorV1 coordinator = new M4ReadControlCoordinatorV1(store, 7, binding);

        private Fixture(CapabilityKind kind) {
            capabilityEvidence = capabilityEvidence(1, kind);
            capability = binding(capabilityEvidence);
        }

        private CapabilityEvidence capabilityEvidence(long generation, CapabilityKind kind) {
            return new CapabilityEvidence(
                    binding,
                    generation,
                    generation,
                    kind,
                    CapabilityState.ADMITTED,
                    digest("adapter-" + generation),
                    digest("backend-config-" + generation),
                    digest("admission-contract"),
                    digest("verifier"),
                    digest("receipt-identity-" + generation),
                    digest("receipt-" + generation),
                    digest("authority-time"),
                    10_000,
                    kind == CapabilityKind.AUTHORITY_EXPIRY_V1 ? 50 : 0,
                    kind == CapabilityKind.AUTHORITY_EXPIRY_V1 ? 100 : 0);
        }

        private CapabilityBinding binding(CapabilityEvidence evidence) {
            return new CapabilityBinding(
                    evidence.generation(), M4ReadControlCodecV1.capabilityEvidenceSha256(evidence));
        }

        private SourceProtectionIdentity source(String name, long protectionGeneration, long first, long generation) {
            return new SourceProtectionIdentity(digest(name), protectionGeneration, first, generation, capability);
        }

        private SourceProtection protection(SourceProtectionIdentity source) {
            return new SourceProtection(binding, source, ProtectionState.PROTECTED, Optional.empty(), Optional.empty());
        }

        private BindingReadSelector fallbackSelector(
                List<SourceProtectionIdentity> sources,
                List<ClosureAnchor> anchors,
                List<SourceRetirementBatch> batches,
                long epoch) {
            return new BindingReadSelector(
                    binding,
                    digest("fallback-view-" + epoch),
                    1,
                    epoch,
                    7,
                    SelectorMode.PREFERRED_WITH_FALLBACK,
                    AdmissionState.ADMITTING,
                    Optional.of(M4ReadControlCodecV1.calculateFallbackSetSha256(sources)),
                    capability,
                    anchors,
                    batches);
        }

        private BindingReadSelector preferredSelector(long epoch, long sourceGeneration) {
            return new BindingReadSelector(
                    binding,
                    digest("preferred-view-" + epoch),
                    1,
                    epoch,
                    sourceGeneration,
                    SelectorMode.PREFERRED_ONLY,
                    AdmissionState.ADMITTING,
                    Optional.empty(),
                    capability,
                    List.of(),
                    List.of());
        }

        private void install(BindingReadSelector selector, List<SourceProtectionIdentity> sources) {
            assertThat(coordinator.createCapability(capabilityEvidence)).isEqualTo(Outcome.APPLIED);
            for (SourceProtectionIdentity source : sources) {
                assertThat(coordinator.createProtection(protection(source))).isEqualTo(Outcome.APPLIED);
            }
            assertThat(coordinator.createSelector(selector)).isEqualTo(Outcome.APPLIED);
        }

        private ClosureAnchor anchor(long epoch) {
            return new ClosureAnchor(
                    epoch,
                    1,
                    digest("predecessor-" + epoch),
                    digest("successor-" + epoch),
                    digest("transition-" + epoch),
                    capability);
        }

        private ReadAdmissionEpochTerminalCut terminal(ClosureAnchor anchor, long drained, long safeAfter) {
            return new ReadAdmissionEpochTerminalCut(
                    binding,
                    M4ReadControlCodecV1.anchorSha256(anchor),
                    anchor.closedReadAdmissionEpoch(),
                    anchor.ownerEpoch(),
                    drained,
                    safeAfter,
                    anchor.capability(),
                    TerminalKind.PLANNED_DRAIN,
                    digest("admission-closed-fence-" + anchor.closedReadAdmissionEpoch()),
                    digest("planned-drain-" + anchor.closedReadAdmissionEpoch()),
                    0,
                    0,
                    1);
        }

        private ReadAdmissionEpochTerminalCut detachedTerminal(long epoch, CapabilityBinding boundCapability) {
            return new ReadAdmissionEpochTerminalCut(
                    binding,
                    digest("detached-anchor-" + epoch),
                    epoch,
                    1,
                    epoch,
                    1_000,
                    boundCapability,
                    TerminalKind.PLANNED_DRAIN,
                    digest("admission-closed-fence-" + epoch),
                    digest("planned-drain-" + epoch),
                    0,
                    0,
                    1);
        }

        private ReadQuiescenceProof proof(ReadAdmissionEpochTerminalCut terminal, long drainedThrough, long safeAfter) {
            ReadQuiescenceProof draft = new ReadQuiescenceProof(
                    binding,
                    terminal.readAdmissionEpoch(),
                    M4ReadControlCodecV1.terminalSha256(terminal),
                    drainedThrough,
                    safeAfter,
                    terminal.capability(),
                    terminal.kind(),
                    digest("placeholder"));
            return new ReadQuiescenceProof(
                    draft.binding(),
                    draft.readAdmissionEpoch(),
                    draft.terminalCutSha256(),
                    draft.drainedThroughReadViewGeneration(),
                    draft.safeAfterAuthorityTimeMillis(),
                    draft.capability(),
                    draft.kind(),
                    M4ReadControlCodecV1.calculateProofIdentity(draft));
        }

        private BindingReadAuthorityV1 localAuthority(long sourceGeneration) {
            BindingReadPublicationCellV1 cell = new BindingReadPublicationCellV1(
                    sourceGeneration, 10, sourceGeneration, new BindingReadRouteTableV1(List.of()), List.of());
            return new BindingReadAuthorityV1(
                    binding.bindingId(),
                    binding.incarnationSha256(),
                    new StorageEpochId(binding.storageEpochSha256()),
                    BindingReadProtocolV1.KAFKA_OFFSET,
                    digest("local-view"),
                    1,
                    1,
                    true,
                    sourceGeneration,
                    capability.evidenceSha256(),
                    cell);
        }

        private BindingReadAuthorityV1 authority(
                Sha256Digest view, long ownerEpoch, long readAdmissionEpoch, boolean admitting, long sourceGeneration) {
            BindingReadPublicationCellV1 cell = new BindingReadPublicationCellV1(
                    sourceGeneration, 10, sourceGeneration, new BindingReadRouteTableV1(List.of()), List.of());
            return new BindingReadAuthorityV1(
                    binding.bindingId(),
                    binding.incarnationSha256(),
                    new StorageEpochId(binding.storageEpochSha256()),
                    BindingReadProtocolV1.KAFKA_OFFSET,
                    view,
                    ownerEpoch,
                    readAdmissionEpoch,
                    admitting,
                    capability.generation(),
                    capability.evidenceSha256(),
                    cell);
        }
    }

    private enum NextMode {
        NORMAL,
        APPLY_BUT_UNKNOWN,
        UNKNOWN_WITHOUT_APPLY
    }

    private static final class MapStore implements CanonicalControlMetadataStore {
        private final Map<String, CanonicalBytes> values = new LinkedHashMap<>();
        private NextMode nextMode = NextMode.NORMAL;
        private long mutations;

        @Override
        public Optional<CanonicalBytes> get(String key) {
            return Optional.ofNullable(values.get(key));
        }

        @Override
        public ControlMutationOutcome putIfAbsent(String key, CanonicalBytes exactValue) {
            mutations++;
            if (nextMode == NextMode.UNKNOWN_WITHOUT_APPLY) {
                nextMode = NextMode.NORMAL;
                return ControlMutationOutcome.RESPONSE_UNKNOWN;
            }
            CanonicalBytes previous = values.putIfAbsent(key, exactValue);
            if (nextMode == NextMode.APPLY_BUT_UNKNOWN) {
                nextMode = NextMode.NORMAL;
                return ControlMutationOutcome.RESPONSE_UNKNOWN;
            }
            return previous == null ? ControlMutationOutcome.APPLIED : ControlMutationOutcome.DEFINITIVE_CONFLICT;
        }

        @Override
        public ControlMutationOutcome compareAndSet(
                String key, Optional<CanonicalBytes> exactExpected, CanonicalBytes exactCandidate) {
            mutations++;
            if (nextMode == NextMode.UNKNOWN_WITHOUT_APPLY) {
                nextMode = NextMode.NORMAL;
                return ControlMutationOutcome.RESPONSE_UNKNOWN;
            }
            Optional<CanonicalBytes> current = Optional.ofNullable(values.get(key));
            boolean matches = current.equals(exactExpected);
            if (matches) {
                values.put(key, exactCandidate);
            }
            if (nextMode == NextMode.APPLY_BUT_UNKNOWN) {
                nextMode = NextMode.NORMAL;
                return ControlMutationOutcome.RESPONSE_UNKNOWN;
            }
            return matches ? ControlMutationOutcome.APPLIED : ControlMutationOutcome.DEFINITIVE_CONFLICT;
        }

        private void put(String key, CanonicalBytes value) {
            values.put(key, value);
        }
    }

    private static Sha256Digest digest(String value) {
        return Sha256Digest.hash(CanonicalBytes.copyOf(value.getBytes(StandardCharsets.UTF_8)));
    }

    private static ReferenceSnapshot references(
            BindingIdentity binding,
            long generation,
            List<EpochInterval> active,
            List<Long> recovery,
            List<Long> responseLoss,
            List<Long> audit) {
        return new ReferenceSnapshot(
                binding,
                generation,
                digest("cleanup-reference-fence-" + generation),
                active,
                recovery,
                responseLoss,
                audit);
    }

    private static long percentile(long[] sortedNanos, int percentile) {
        int index = Math.floorDiv(Math.addExact(Math.multiplyExact(sortedNanos.length, percentile), 99), 100) - 1;
        return sortedNanos[Math.max(0, Math.min(index, sortedNanos.length - 1))];
    }

    private static long throughput(long operations, long elapsedNanos) {
        return Math.max(1, Math.floorDiv(Math.multiplyExact(operations, 1_000_000_000L), elapsedNanos));
    }
}
