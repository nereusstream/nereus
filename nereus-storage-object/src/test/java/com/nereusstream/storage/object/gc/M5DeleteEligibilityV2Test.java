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

package com.nereusstream.storage.object.gc;

import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.assertThatThrownBy;
import com.nereusstream.domain.bytes.CanonicalBytes;
import com.nereusstream.domain.bytes.CanonicalUtf8;
import com.nereusstream.storage.api.lifecycle.PhysicalResourceIdV2;
import com.nereusstream.storage.object.gc.DeleteEligibilitySnapshotV2.MemberSnapshot;
import com.nereusstream.storage.object.gc.DeleteEligibilitySnapshotV2.NativeDisposition;
import com.nereusstream.storage.object.gc.DeleteEligibilitySnapshotV2.ReclamationReason;
import com.nereusstream.storage.object.gc.DeleteEligibilitySnapshotV2.ReplacementEvidence;
import com.nereusstream.storage.object.gc.DeleteEligibilitySnapshotV2.SemanticTransfer;
import com.nereusstream.storage.object.gc.DeleteEligibilitySnapshotV2.UnpublishedEvidence;
import com.nereusstream.storage.object.gc.M5TargetDeleteAuthorityRecordsV1.PhysicalDeleteTargetV1;
import com.nereusstream.storage.object.gc.M5TargetDeleteAuthorityRecordsV1.ProofBoundWriterClassV1;
import com.nereusstream.storage.object.gc.M5TargetDeleteAuthorityRecordsV1.ProofBoundWriterEnrollmentV1;
import com.nereusstream.storage.object.gc.M5TargetDeleteAuthorityRecordsV1.ProofBoundWriterTicketV1;
import com.nereusstream.storage.object.materialization.M5MaterializationRecordsV1.PositionDomain;
import com.nereusstream.storage.object.materialization.M5MaterializationRecordsV1.ProtocolCoverage;
import com.nereusstream.storage.object.retention.M5RetentionCodecV1;
import com.nereusstream.storage.object.retention.M5RetentionRecordsV1.FloorClassV1;
import com.nereusstream.storage.object.retention.M5RetentionRecordsV1.ReferenceFreeProofV1;
import com.nereusstream.storage.object.retention.M5RetentionRecordsV1.ReferenceKindV1;
import com.nereusstream.storage.object.retention.M5RetentionRecordsV1.ReferenceTargetKindV1;
import java.nio.ByteBuffer;
import java.util.Arrays;
import java.util.List;
import java.util.Optional;
import org.junit.jupiter.api.Test;

class M5DeleteEligibilityV2Test {
    @Test
    void replacementReleasesOldPhysicalEligibilityWhileLogicalMessagesRemainRetained() {
        var snapshot = replacement();
        assertThat(snapshot.members().get(0).floors().minimumSafeFloor()).isZero();
        assertThat(snapshot.members().get(0).physicalReferences().coverage().exclusiveEnd())
                .isEqualTo(100);
        assertThat(DeleteEligibilityCodecV2.decode(DeleteEligibilityCodecV2.encode(snapshot)))
                .isEqualTo(snapshot);
        var authority = M5TargetDeleteAuthorityStateMachineV1.open(target(), enrollment(), snapshot);
        var fenced = M5TargetDeleteAuthorityStateMachineV1.prepareIdentityRead(
                authority, digest("read-attempt"), M5DeleteEligibilityTestFixtures.observation());
        assertThat(fenced.eligibilitySnapshot()).contains(snapshot);
        assertThat(fenced.readFence().orElseThrow().eligibilityRootSha256()).isEqualTo(snapshot.sha256());
        assertThat(fenced.deleteIntent()).isEmpty();
    }

    @Test
    void logicalExpiryRequiresTheEntireTargetToCrossTrimAndEveryFloor() {
        var expired = M5DeleteEligibilityTestFixtures.snapshot(
                resource(), ReclamationReason.LOGICAL_EXPIRY, 1, 100, PositionDomain.KAFKA_OFFSET);
        assertThat(DeleteEligibilityCodecV2.decode(DeleteEligibilityCodecV2.encode(expired)))
                .isEqualTo(expired);
        assertThatThrownBy(() -> M5DeleteEligibilityTestFixtures.snapshot(
                        resource(), ReclamationReason.LOGICAL_EXPIRY, 1, 99, PositionDomain.KAFKA_OFFSET))
                .isInstanceOf(IllegalArgumentException.class)
                .hasMessageContaining("full-range trim");
    }

    @Test
    void unpublishedCleanupNeedsFencedTerminalTaskNoAdoptionAndProtectionClosure() {
        var unpublished = M5DeleteEligibilityTestFixtures.snapshot(
                resource(), ReclamationReason.UNPUBLISHED_ARTIFACT, 1, 0, PositionDomain.KAFKA_OFFSET);
        assertThat(DeleteEligibilityCodecV2.decode(DeleteEligibilityCodecV2.encode(unpublished)))
                .isEqualTo(unpublished);
        MemberSnapshot original = unpublished.members().get(0);
        UnpublishedEvidence proof = original.unpublished().orElseThrow();
        var missing = new UnpublishedEvidence(
                proof.taskAttemptId(),
                proof.terminalTask(),
                proof.fencedTaskOwner(),
                proof.closedAdoptionAndResponseLossPaths(),
                Optional.empty());
        assertThatThrownBy(() -> withMember(
                        unpublished,
                        new MemberSnapshot(
                                original.floors(),
                                original.physicalReferences(),
                                original.nativeAuthority(),
                                original.nativeDisposition(),
                                Optional.empty(),
                                Optional.empty(),
                                Optional.of(missing))))
                .isInstanceOf(IllegalArgumentException.class)
                .hasMessageContaining("protection closure");
    }

    @Test
    void retainBkPolicyStillVetoesAnOtherwiseCompleteReplacement() {
        var snapshot = replacement();
        var original = snapshot.members().get(0);
        assertThatThrownBy(() -> withMember(
                        snapshot,
                        new MemberSnapshot(
                                original.floors(),
                                original.physicalReferences(),
                                original.nativeAuthority(),
                                NativeDisposition.RETAIN,
                                original.replacement(),
                                Optional.empty(),
                                Optional.empty())))
                .isInstanceOf(IllegalArgumentException.class)
                .hasMessageContaining("native-authorized");
    }

    @Test
    void missingRecoverySemanticChangedStateOrPartialCoverageCannotDischargeLogicalObligations() {
        var snapshot = replacement();
        var member = snapshot.members().get(0);
        var proof = member.replacement().orElseThrow();
        var missing = new ReplacementEvidence(
                proof.selectedGeneration(),
                proof.replacementResources(),
                proof.transfers().subList(1, proof.transfers().size()));
        assertThatThrownBy(() -> withReplacement(snapshot, missing))
                .isInstanceOf(IllegalArgumentException.class)
                .hasMessageContaining("omits an applicable");
        SemanticTransfer first = proof.transfers().get(0);
        assertThatThrownBy(() -> new SemanticTransfer(
                        first.aspect(),
                        first.coverage(),
                        first.priorSemanticRoot(),
                        digest("changed"),
                        first.verifierAuthority()))
                .isInstanceOf(IllegalArgumentException.class)
                .hasMessageContaining("changed an applicable");
        var partial = proof.transfers().stream()
                .map(transfer -> new SemanticTransfer(
                        transfer.aspect(),
                        new ProtocolCoverage(PositionDomain.KAFKA_OFFSET, 0, 99),
                        transfer.priorSemanticRoot(),
                        transfer.replacementSemanticRoot(),
                        transfer.verifierAuthority()))
                .toList();
        assertThatThrownBy(() -> withReplacement(
                        snapshot,
                        new ReplacementEvidence(proof.selectedGeneration(), proof.replacementResources(), partial)))
                .isInstanceOf(IllegalArgumentException.class)
                .hasMessageContaining("complete resource range");
    }

    @Test
    void currentSelectionAndPhysicallyDistinctReplacementAreMandatory() {
        var snapshot = replacement();
        var proof = snapshot.members().get(0).replacement().orElseThrow();
        assertThatThrownBy(() -> withReplacement(
                        snapshot,
                        new ReplacementEvidence(
                                M5DeleteEligibilityTestFixtures.fact("/stale-manifest"),
                                proof.replacementResources(),
                                proof.transfers())))
                .isInstanceOf(IllegalArgumentException.class)
                .hasMessageContaining("not selected");
        assertThatThrownBy(() -> withReplacement(
                        snapshot,
                        new ReplacementEvidence(proof.selectedGeneration(), List.of(resource()), proof.transfers())))
                .isInstanceOf(IllegalArgumentException.class)
                .hasMessageContaining("physically distinct");
    }

    @Test
    void publishedReplacementCannotPretendToBeAnUnpublishedSourceWithoutM4Release() {
        var snapshot = replacement();
        var member = snapshot.members().get(0);
        var proof = member.physicalReferences();
        ReferenceFreeProofV1 noRelease = M5RetentionCodecV1.finalizeProof(new ReferenceFreeProofV1(
                proof.identity(),
                ReferenceTargetKindV1.UNSELECTED_OUTPUT,
                proof.targetIdentitySha256(),
                proof.coverage(),
                proof.selectorRoot(),
                proof.manifestRoot(),
                proof.trimRoot(),
                proof.retentionSnapshotRootSha256(),
                proof.observationsRootSha256(),
                List.of(),
                proof.ownerFence(),
                proof.workerFence(),
                proof.storageFence(),
                proof.providerFence(),
                proof.auditGraceDeadlineMillis(),
                proof.observedAuthorityTimeMillis(),
                proof.scanSummaries(),
                proof.observations(),
                digest("placeholder")));
        assertThatThrownBy(() -> withMember(
                        snapshot,
                        new MemberSnapshot(
                                member.floors(),
                                noRelease,
                                member.nativeAuthority(),
                                member.nativeDisposition(),
                                member.replacement(),
                                Optional.empty(),
                                Optional.empty())))
                .isInstanceOf(IllegalArgumentException.class)
                .hasMessageContaining("exact M4 RELEASED");
    }

    @Test
    void allOriginalFloorAndReferenceClassesHaveExplicitScopeAndPulsarHasItsOwnSemantics() {
        for (FloorClassV1 kind : FloorClassV1.values()) {
            assertThat(DeleteEligibilitySnapshotV2.floorScope(kind)).isNotNull();
        }
        for (ReferenceKindV1 kind : ReferenceKindV1.values()) {
            assertThat(DeleteEligibilitySnapshotV2.referenceScope(kind)).isNotNull();
        }
        var pulsar = M5DeleteEligibilityTestFixtures.snapshot(
                resource(), ReclamationReason.REPLACED_REPRESENTATION, 1, 0, PositionDomain.PULSAR_ENTRY);
        assertThat(pulsar.members().get(0).replacement().orElseThrow().transfers())
                .hasSize(4);
        assertThat(DeleteEligibilityCodecV2.decode(DeleteEligibilityCodecV2.encode(pulsar)))
                .isEqualTo(pulsar);
    }

    @Test
    void unqualifiedAuthorityAndPostWriterProofCannotEnterCas1UntilFreshTypedQualification() {
        var unqualified = M5TargetDeleteAuthorityStateMachineV1.open(target(), enrollment(), digest("unassessed"));
        assertThatThrownBy(() -> M5TargetDeleteAuthorityStateMachineV1.prepareIdentityRead(
                        unqualified, digest("attempt"), M5DeleteEligibilityTestFixtures.observation()))
                .isInstanceOf(IllegalStateException.class)
                .hasMessageContaining("complete typed eligibility");
        var qualified = M5TargetDeleteAuthorityStateMachineV1.qualifyEligibility(
                unqualified, M5DeleteEligibilityTestFixtures.replacement(resource(), 2));
        var ticket = new ProofBoundWriterTicketV1(
                ProofBoundWriterClassV1.REPLICA_TOPOLOGY_V1,
                digest("operation"),
                digest("capability"),
                digest("owner"),
                digest("external"),
                qualified.authorityRevision());
        var acquired = M5TargetDeleteAuthorityStateMachineV1.acquireWriterTicket(qualified, ticket);
        assertThat(acquired.eligibilitySnapshot()).isEmpty();
        var reconciled = M5TargetDeleteAuthorityStateMachineV1.completeWriterTicket(
                acquired, ticket.operationIdSha256(), qualified.proofSnapshotDigest());
        assertThat(reconciled.eligibilitySnapshot()).isEmpty();
        assertThatThrownBy(() -> M5TargetDeleteAuthorityStateMachineV1.prepareIdentityRead(
                        reconciled, digest("attempt-2"), M5DeleteEligibilityTestFixtures.observation()))
                .isInstanceOf(IllegalStateException.class);
        assertThatThrownBy(() -> M5TargetDeleteAuthorityStateMachineV1.qualifyEligibility(reconciled, replacement()))
                .isInstanceOf(IllegalArgumentException.class)
                .hasMessageContaining("stale/foreign");
        var refreshed = M5TargetDeleteAuthorityStateMachineV1.qualifyEligibility(
                reconciled,
                M5DeleteEligibilityTestFixtures.replacement(resource(), reconciled.authorityRevision() + 1));
        assertThat(M5TargetDeleteAuthorityStateMachineV1.prepareIdentityRead(
                                refreshed, digest("attempt-3"), M5DeleteEligibilityTestFixtures.observation())
                        .deleteIntent())
                .isEmpty();
        assertThat(refreshed.authorityKey()).isEqualTo(unqualified.authorityKey());
    }

    @Test
    void eligibilityCodecRejectsTrailingTruncatedOversizeUnknownAndNonCanonicalData() {
        byte[] bytes = DeleteEligibilityCodecV2.encode(replacement()).toByteArray();
        assertThatThrownBy(() ->
                        DeleteEligibilityCodecV2.decode(CanonicalBytes.copyOf(Arrays.copyOf(bytes, bytes.length - 1))))
                .isInstanceOf(IllegalArgumentException.class);
        assertThatThrownBy(() ->
                        DeleteEligibilityCodecV2.decode(CanonicalBytes.copyOf(Arrays.copyOf(bytes, bytes.length + 1))))
                .isInstanceOf(IllegalArgumentException.class);
        byte[] changed = bytes.clone();
        ByteBuffer.wrap(changed).putInt(8, Integer.MAX_VALUE);
        assertThatThrownBy(() -> DeleteEligibilityCodecV2.decode(CanonicalBytes.copyOf(changed)))
                .isInstanceOf(IllegalArgumentException.class);
        assertThatThrownBy(() -> DeleteEligibilityCodecV2.decode(
                        CanonicalBytes.copyOf(new byte[DeleteEligibilitySnapshotV2.MAX_SNAPSHOT_BYTES + 1])))
                .isInstanceOf(IllegalArgumentException.class);
    }

    private static DeleteEligibilitySnapshotV2 withReplacement(
            DeleteEligibilitySnapshotV2 snapshot, ReplacementEvidence replacement) {
        var member = snapshot.members().get(0);
        return withMember(
                snapshot,
                new MemberSnapshot(
                        member.floors(),
                        member.physicalReferences(),
                        member.nativeAuthority(),
                        member.nativeDisposition(),
                        Optional.of(replacement),
                        Optional.empty(),
                        Optional.empty()));
    }

    private static DeleteEligibilitySnapshotV2 withMember(DeleteEligibilitySnapshotV2 snapshot, MemberSnapshot member) {
        return new DeleteEligibilitySnapshotV2(
                snapshot.resource(),
                snapshot.reason(),
                snapshot.generation(),
                snapshot.namespaceAdmission(),
                snapshot.completeMemberInventory(),
                List.of(member));
    }

    private static DeleteEligibilitySnapshotV2 replacement() {
        return M5DeleteEligibilityTestFixtures.replacement(resource(), 1);
    }

    private static PhysicalDeleteTargetV1 target() {
        return PhysicalDeleteTargetV1.create(resource());
    }

    private static PhysicalResourceIdV2 resource() {
        return new PhysicalResourceIdV2.BookKeeperLedger(
                new PhysicalResourceIdV2.Namespace(
                        PhysicalResourceIdV2.ProviderKind.BOOKKEEPER,
                        CanonicalUtf8.fromString("old-cluster"),
                        CanonicalUtf8.fromString("namespace")),
                7);
    }

    private static ProofBoundWriterEnrollmentV1 enrollment() {
        return new ProofBoundWriterEnrollmentV1(
                List.of(ProofBoundWriterClassV1.values()),
                digest("capabilities"),
                digest("implementation"),
                digest("policy"));
    }

    private static com.nereusstream.domain.bytes.Sha256Digest digest(String text) {
        return M5DeleteEligibilityTestFixtures.digest(text);
    }
}
