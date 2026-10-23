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
import com.nereusstream.storage.object.materialization.M5PulsarCoverageV2;
import com.nereusstream.storage.object.retention.M5RetentionCodecV1;
import com.nereusstream.storage.object.retention.M5RetentionRecordsV1;
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
    void legacyV2WireBytesRemainExact() {
        var legacy = List.of(
                replacement(),
                M5DeleteEligibilityTestFixtures.snapshot(
                        resource(), ReclamationReason.REPLACED_REPRESENTATION, 1, 0, PositionDomain.PULSAR_ENTRY),
                typedExpiry(new M5PulsarCoverageV2.Position(7, 4)));
        // Captured with the original v2 encoder before adding typed semantic coverage.
        var lengths = List.of(6876, 6250, 6784);
        var digests = List.of(
                "09219bf360eaf38a107f705e904a38f671e4134cae50d16188fca1d41f514734",
                "b98084febf9d337cd5209f1e628f6e785a5f77dedb24177df8f0cdb25f9eb399",
                "c97d490db7563ccbef7539a6b1242bccee129a2f1e97e638f0b51e079b423ceb");
        for (int index = 0; index < legacy.size(); index++) {
            var encoded = DeleteEligibilityCodecV2.encode(legacy.get(index));
            assertThat(ByteBuffer.wrap(encoded.toByteArray()).getInt(4)).isEqualTo(2);
            assertThat(encoded.length()).isEqualTo(lengths.get(index));
            assertThat(com.nereusstream.domain.bytes.Sha256Digest.hash(encoded).toHex())
                    .isEqualTo(digests.get(index));
            assertThat(DeleteEligibilityCodecV2.decode(encoded)).isEqualTo(legacy.get(index));
            assertThat(DeleteEligibilityCodecV2.encode(DeleteEligibilityCodecV2.decode(encoded)))
                    .isEqualTo(encoded);
        }
    }

    @Test
    void typedPulsarReplacementRoundTripsWithStableDigest() {
        var snapshot = typedReplacement();
        var encoded = DeleteEligibilityCodecV2.encode(snapshot);
        assertThat(ByteBuffer.wrap(encoded.toByteArray()).getInt(4)).isEqualTo(3);
        var decoded = DeleteEligibilityCodecV2.decode(encoded);
        assertThat(decoded).isEqualTo(snapshot);
        assertThat(DeleteEligibilityCodecV2.encode(decoded)).isEqualTo(encoded);
        assertThat(decoded.sha256()).isEqualTo(snapshot.sha256());
    }

    @Test
    void typedTransferWireRejectsMalformedAndNoncanonicalForms() {
        var snapshot = typedReplacement();
        var encoded = DeleteEligibilityCodecV2.encode(snapshot).toByteArray();
        var coverage = snapshot.members()
                .get(0)
                .physicalReferences()
                .coverage()
                .pulsarCoverage()
                .orElseThrow()
                .encode()
                .toByteArray();
        int typedOffset = lastIndexOf(encoded, coverage);
        assertThat(typedOffset).isGreaterThan(5);
        var wrongDomain = encoded.clone();
        wrongDomain[typedOffset - 5] = (byte) PositionDomain.KAFKA_OFFSET.ordinal();
        assertThatThrownBy(() -> DeleteEligibilityCodecV2.decode(CanonicalBytes.copyOf(wrongDomain)))
                .isInstanceOf(IllegalArgumentException.class)
                .hasMessageContaining("typed semantic coverage");
        var oversized = encoded.clone();
        ByteBuffer.wrap(oversized).putInt(typedOffset - 4, M5PulsarCoverageV2.MAX_BYTES + 1);
        assertThatThrownBy(() -> DeleteEligibilityCodecV2.decode(CanonicalBytes.copyOf(oversized)))
                .isInstanceOf(IllegalArgumentException.class);
        var wrongVersion = encoded.clone();
        ByteBuffer.wrap(wrongVersion).putInt(4, 4);
        assertThatThrownBy(() -> DeleteEligibilityCodecV2.decode(CanonicalBytes.copyOf(wrongVersion)))
                .isInstanceOf(IllegalArgumentException.class);
        assertThatThrownBy(() -> DeleteEligibilityCodecV2.decode(
                        CanonicalBytes.copyOf(Arrays.copyOf(encoded, encoded.length - 1))))
                .isInstanceOf(IllegalArgumentException.class);
        assertThatThrownBy(() -> DeleteEligibilityCodecV2.decode(
                        CanonicalBytes.copyOf(Arrays.copyOf(encoded, encoded.length + 1))))
                .isInstanceOf(IllegalArgumentException.class);

        // A structurally valid v3 carrying only legacy scalar transfers has no canonical v3 identity.
        var legacy = replacement();
        byte[] noncanonical = DeleteEligibilityCodecV2.encode(legacy).toByteArray();
        for (var transfer : legacy.members().get(0).replacement().orElseThrow().transfers()) {
            var marker = ByteBuffer.allocate(82)
                    .put((byte) transfer.aspect().ordinal())
                    .put((byte) transfer.coverage().domain().ordinal())
                    .putLong(transfer.coverage().inclusiveStart())
                    .putLong(transfer.coverage().exclusiveEnd())
                    .put(transfer.priorSemanticRoot().bytes().toByteArray())
                    .put(transfer.replacementSemanticRoot().bytes().toByteArray())
                    .array();
            int offset = lastIndexOf(noncanonical, marker) + 1;
            assertThat(offset).isGreaterThan(0);
            var withMarker = Arrays.copyOf(noncanonical, noncanonical.length + 1);
            System.arraycopy(noncanonical, offset, withMarker, offset + 1, noncanonical.length - offset);
            withMarker[offset] = 0;
            noncanonical = withMarker;
        }
        ByteBuffer.wrap(noncanonical).putInt(4, 3);
        var scalarV3 = CanonicalBytes.copyOf(noncanonical);
        assertThatThrownBy(() -> DeleteEligibilityCodecV2.decode(scalarV3))
                .isInstanceOf(IllegalArgumentException.class)
                .hasMessageContaining("noncanonical");
    }

    private static int lastIndexOf(byte[] encoded, byte[] needle) {
        for (int index = encoded.length - needle.length; index >= 0; index--) {
            if (Arrays.mismatch(encoded, index, index + needle.length, needle, 0, needle.length) == -1) {
                return index;
            }
        }
        return -1;
    }

    private static DeleteEligibilitySnapshotV2 typedReplacement() {
        var expired = typedExpiry(new M5PulsarCoverageV2.Position(7, 4));
        var member = expired.members().get(0);
        var basis = M5DeleteEligibilityTestFixtures.snapshot(
                        resource(), ReclamationReason.REPLACED_REPRESENTATION, 1, 0, PositionDomain.PULSAR_ENTRY)
                .members()
                .get(0)
                .replacement()
                .orElseThrow();
        var transfers = basis.transfers().stream()
                .map(transfer -> new SemanticTransfer(
                        transfer.aspect(),
                        member.physicalReferences().coverage(),
                        transfer.priorSemanticRoot(),
                        transfer.replacementSemanticRoot(),
                        transfer.verifierAuthority()))
                .toList();
        return new DeleteEligibilitySnapshotV2(
                expired.resource(),
                ReclamationReason.REPLACED_REPRESENTATION,
                expired.generation(),
                expired.namespaceAdmission(),
                expired.completeMemberInventory(),
                List.of(new MemberSnapshot(
                        member.floors(),
                        member.physicalReferences(),
                        member.nativeAuthority(),
                        NativeDisposition.REPLACEMENT_ALLOWED,
                        Optional.of(new ReplacementEvidence(
                                member.physicalReferences().manifestRoot(), basis.replacementResources(), transfers)),
                        Optional.empty(),
                        Optional.empty())));
    }

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
    void typedExpiryRequiresEveryLedgerRangeAndMatchingFloorChain() {
        var expired = typedExpiry(new M5PulsarCoverageV2.Position(7, 4));
        assertThat(DeleteEligibilityCodecV2.decode(DeleteEligibilityCodecV2.encode(expired)))
                .isEqualTo(expired);
        assertThatThrownBy(() -> typedExpiry(new M5PulsarCoverageV2.Position(91, 3)))
                .isInstanceOf(IllegalArgumentException.class)
                .hasMessageContaining("full-range trim");
        assertThatThrownBy(() -> typedExpiry(new M5PulsarCoverageV2.Position(7, 3)))
                .isInstanceOf(IllegalArgumentException.class)
                .hasMessageContaining("full-range trim");
    }

    private static DeleteEligibilitySnapshotV2 typedExpiry(M5PulsarCoverageV2.Position end) {
        var scalar = M5DeleteEligibilityTestFixtures.snapshot(
                resource(), ReclamationReason.LOGICAL_EXPIRY, 1, 100, PositionDomain.PULSAR_ENTRY);
        var member = scalar.members().get(0);
        var basis = member.floors();
        var context = M5PulsarCoverageV2.of(
                basis.identity().binding(),
                digest("typed-expiry-chain"),
                List.of(
                        new M5PulsarCoverageV2.ChainLedger(91, false, 3, digest("typed-91")),
                        new M5PulsarCoverageV2.ChainLedger(7, false, 4, digest("typed-7"))),
                List.of(new M5PulsarCoverageV2.EntryRange(91, 0, 3), new M5PulsarCoverageV2.EntryRange(7, 0, 4)));
        var floorRows = basis.rows().stream()
                .map(row -> new M5RetentionRecordsV1.RetentionFloorObservationV1(
                        row.floorClass(), row.authority(), row.domain(), -1, true, true, Optional.of(end)))
                .toList();
        var prior = new M5PulsarCoverageV2.Position(91, 0);
        var floors = M5RetentionCodecV1.finalizeSnapshot(new M5RetentionRecordsV1.RetentionFloorSnapshotV1(
                basis.identity(),
                basis.domain(),
                basis.generation(),
                -1,
                basis.retentionPolicyRootSha256(),
                basis.ownerFence(),
                basis.storageFence(),
                basis.pageCount(),
                basis.scannedBytes(),
                floorRows,
                digest("placeholder"),
                Optional.of(new M5RetentionRecordsV1.PulsarFloorContextV2(context, prior))));
        var trim = new M5RetentionRecordsV1.BindingTrimFrontierV1(
                floors.identity(),
                floors.domain(),
                -1,
                -1,
                floors.retentionPolicyRootSha256(),
                floors.snapshotRootSha256(),
                floors.ownerFence(),
                floors.storageFence(),
                1,
                floors.identity().capability(),
                Optional.of(new M5RetentionRecordsV1.PulsarTrimContextV2(context, prior, end)));
        var coverage = ProtocolCoverage.pulsar(context);
        var oldProof = member.physicalReferences();
        var references = oldProof.observations().stream()
                .map(row -> new M5RetentionRecordsV1.ReferenceObservationV1(
                        row.kind(),
                        row.authority(),
                        row.targetIdentitySha256(),
                        coverage,
                        row.disposition(),
                        row.enumerationComplete()))
                .toList();
        var trimAuthority =
                M5DeleteEligibilityTestFixtures.fact("/typed-trim", M5RetentionCodecV1.encodeTrimFrontier(trim));
        var proof = M5RetentionCodecV1.finalizeProof(new ReferenceFreeProofV1(
                oldProof.identity(),
                oldProof.targetKind(),
                oldProof.targetIdentitySha256(),
                coverage,
                oldProof.selectorRoot(),
                oldProof.manifestRoot(),
                trimAuthority,
                floors.snapshotRootSha256(),
                M5RetentionCodecV1.calculateObservationsRoot(references),
                oldProof.m4Releases(),
                oldProof.ownerFence(),
                oldProof.workerFence(),
                oldProof.storageFence(),
                oldProof.providerFence(),
                oldProof.auditGraceDeadlineMillis(),
                oldProof.observedAuthorityTimeMillis(),
                oldProof.scanSummaries(),
                references,
                digest("placeholder")));
        return withMember(
                scalar,
                new MemberSnapshot(
                        floors,
                        proof,
                        member.nativeAuthority(),
                        member.nativeDisposition(),
                        Optional.empty(),
                        Optional.of(trim),
                        Optional.empty()));
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
