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

package com.nereusstream.domain.registry.allocator;

import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.assertThatThrownBy;
import com.nereusstream.domain.bytes.CanonicalBytes;
import com.nereusstream.domain.bytes.Sha256Digest;
import com.nereusstream.domain.identity.DeploymentId;
import com.nereusstream.domain.identity.Id128;
import com.nereusstream.domain.identity.PulsarCellId;
import com.nereusstream.domain.identity.ReservationDomainId;
import com.nereusstream.domain.registry.VirtualLedgerSliceAssignmentV1;
import com.nereusstream.domain.registry.VirtualLedgerSliceLifecycleV1;
import com.nereusstream.domain.registry.VirtualLedgerSliceViewV1;
import java.nio.charset.StandardCharsets;
import java.util.List;
import org.junit.jupiter.api.Test;

class AllocatorProtocolV1ProductionTest {
    @Test
    void strictUsesReserveNodeHeadClearWithoutSeparateInstall() {
        VirtualLedgerCellAllocatorStateV1 cell = cell(AllocatorModeV1.STRICT_SERIALIZED);
        ManagedLedgerAllocatorHeadV1 head = head(cell, 7);

        cell = AllocatorProtocolV1.reserve(cell, head, digest("request-1"), 1);
        VirtualLedgerCandidateNodeV1 node =
                AllocatorProtocolV1.strictCandidateFromReservation(cell, head, digest("ledger-1"));
        head = AllocatorProtocolV1.publishStrictReserved(cell, head, node);
        assertThat(AllocatorProtocolV1.publishStrictReserved(cell, head, node)).isSameAs(head);
        cell = AllocatorProtocolV1.clearStrictTerminalReservation(cell, head, node);
        assertThat(AllocatorProtocolV1.clearInstalledReservation(cell, head)).isSameAs(cell);

        assertThat(head.visibleChainHead()).isEqualTo(node.pointer());
        assertThat(head.nextLedgerId()).isEqualTo(cell.sliceStartInclusive() + 1);
        assertThat(cell.reservation()).isEmpty();
    }

    @Test
    void strictFencesStaleCreatorAndForbidsSeparateInstallOrEarlyClear() {
        VirtualLedgerCellAllocatorStateV1 cell = cell(AllocatorModeV1.STRICT_SERIALIZED);
        ManagedLedgerAllocatorHeadV1 head = head(cell, 7);
        cell = AllocatorProtocolV1.reserve(cell, head, digest("strict-fenced"), 1);
        VirtualLedgerCandidateNodeV1 stale =
                AllocatorProtocolV1.strictCandidateFromReservation(cell, head, digest("strict-node"));
        ManagedLedgerAllocatorHeadV1 takenOver = AllocatorProtocolV1.takeover(head, 8);
        VirtualLedgerCellAllocatorStateV1 reserved = cell;

        assertThatThrownBy(() -> AllocatorProtocolV1.publishStrictReserved(reserved, takenOver, stale))
                .isInstanceOfSatisfying(AllocatorProtocolException.class, error -> assertThat(error.code())
                        .isEqualTo(AllocatorProtocolException.Code.OWNER_FENCED));
        assertThatThrownBy(() -> AllocatorProtocolV1.installReservedRange(reserved, head))
                .isInstanceOfSatisfying(AllocatorProtocolException.class, error -> assertThat(error.code())
                        .isEqualTo(AllocatorProtocolException.Code.MODE_MISMATCH));
        assertThatThrownBy(() -> AllocatorProtocolV1.clearInstalledReservation(reserved, head))
                .isInstanceOfSatisfying(AllocatorProtocolException.class, error -> assertThat(error.code())
                        .isEqualTo(AllocatorProtocolException.Code.CANDIDATE_OCCUPANCY_NOT_PROVEN));
    }

    @Test
    void rangeGrantSurvivesOwnerTakeoverAndClearDoesNotGateInstalledUse() {
        VirtualLedgerCellAllocatorStateV1 cell = cell(AllocatorModeV1.RANGE_LEASED);
        ManagedLedgerAllocatorHeadV1 head = head(cell, 10);
        cell = AllocatorProtocolV1.reserve(cell, head, digest("request-range"), 8);

        head = AllocatorProtocolV1.takeover(head, 11);
        head = AllocatorProtocolV1.installReservedRange(cell, head);
        assertThat(AllocatorProtocolV1.installReservedRange(cell, head)).isSameAs(head);
        assertThat(AllocatorProtocolV1.takeover(head, 11)).isSameAs(head);
        VirtualLedgerCellAllocatorStateV1 cleared = AllocatorProtocolV1.clearInstalledReservation(cell, head);
        VirtualLedgerCandidateNodeV1 node = AllocatorProtocolV1.candidate(head, digest("ledger-range"));
        head = AllocatorProtocolV1.publish(head, node);
        assertThat(AllocatorProtocolV1.publish(head, node)).isSameAs(head);

        assertThat(head.ownerEpoch()).isEqualTo(11);
        assertThat(head.rangeEndExclusive() - head.rangeStartInclusive()).isEqualTo(8);
        assertThat(cleared.reservation()).isEmpty();
    }

    @Test
    void rangeUnusedTailCannotBeRegrantedAndTerminalAbandonmentNeverReturnsAllocatorState() {
        VirtualLedgerCellAllocatorStateV1 cell = cell(AllocatorModeV1.RANGE_LEASED);
        ManagedLedgerAllocatorHeadV1 head = head(cell, 10);
        cell = AllocatorProtocolV1.reserve(cell, head, digest("range-one"), 8);
        head = AllocatorProtocolV1.installReservedRange(cell, head);
        VirtualLedgerCellAllocatorStateV1 cleared = AllocatorProtocolV1.clearInstalledReservation(cell, head);
        ManagedLedgerAllocatorHeadV1 installed = head;

        assertThatThrownBy(() -> AllocatorProtocolV1.reserve(cleared, installed, digest("range-two"), 8))
                .isInstanceOfSatisfying(AllocatorProtocolException.class, error -> assertThat(error.code())
                        .isEqualTo(AllocatorProtocolException.Code.RANGE_TAIL_NOT_EXHAUSTED));
        TerminalInstalledRangeAbandonmentV1 terminal = AllocatorProtocolV1.abandonInstalledRangeTerminal(
                cleared, installed, InstalledRangeAbandonmentAuthorityV1.SLICE_RETIREMENT);
        assertThat(terminal.nextLedgerId()).isLessThan(terminal.rangeEndExclusive());
        assertThat(terminal.authority()).isEqualTo(InstalledRangeAbandonmentAuthorityV1.SLICE_RETIREMENT);
        assertThat(TerminalInstalledRangeAbandonmentV1.class.isAssignableFrom(VirtualLedgerCellAllocatorStateV1.class))
                .isFalse();
    }

    @Test
    void takeoverBurnsAtMostTheExactSingleFlightStaleCandidate() {
        VirtualLedgerCellAllocatorStateV1 cell = cell(AllocatorModeV1.RANGE_LEASED);
        ManagedLedgerAllocatorHeadV1 head = head(cell, 10);
        cell = AllocatorProtocolV1.reserve(cell, head, digest("request-range"), 8);
        head = AllocatorProtocolV1.installReservedRange(cell, head);
        VirtualLedgerCandidateNodeV1 stale = AllocatorProtocolV1.candidate(head, digest("stale"));
        head = AllocatorProtocolV1.takeover(head, 11);

        ManagedLedgerAllocatorHeadV1 burned = AllocatorProtocolV1.burnOneStaleCandidate(head, stale);

        assertThat(burned.nextLedgerId()).isEqualTo(head.nextLedgerId() + 1);
        assertThat(burned.visibleChainHead()).isEqualTo(head.visibleChainHead());
        assertThat(AllocatorProtocolV1.burnOneStaleCandidate(burned, stale)).isSameAs(burned);
    }

    @Test
    void exactSliceBoundaryExhaustsAndNeverWrapsOrReuses() {
        VirtualLedgerCellAllocatorStateV1 cell = cell(AllocatorModeV1.RANGE_LEASED);
        cell = new VirtualLedgerCellAllocatorStateV1(
                cell.mode(),
                cell.allocatorProtocolVersion(),
                cell.ledgerIdCompatibilityNamespaceId(),
                cell.sliceAssignmentId(),
                cell.sliceStartInclusive(),
                cell.sliceEndInclusive(),
                cell.sliceEndInclusive() - 1,
                cell.nextGrantId(),
                cell.reservation());
        ManagedLedgerAllocatorHeadV1 head =
                ManagedLedgerAllocatorHeadV1.initial(incarnation(), 1, cell.nextSliceLedgerId());
        VirtualLedgerCellAllocatorStateV1 exhausted = AllocatorProtocolV1.reserve(cell, head, digest("last-two"), 2);

        assertThat(exhausted.exhausted()).isTrue();
        assertThatThrownBy(() -> AllocatorProtocolV1.reserve(
                        new VirtualLedgerCellAllocatorStateV1(
                                exhausted.mode(),
                                exhausted.allocatorProtocolVersion(),
                                exhausted.ledgerIdCompatibilityNamespaceId(),
                                exhausted.sliceAssignmentId(),
                                exhausted.sliceStartInclusive(),
                                exhausted.sliceEndInclusive(),
                                exhausted.nextSliceLedgerId(),
                                exhausted.nextGrantId(),
                                java.util.Optional.empty()),
                        head,
                        digest("after-end"),
                        2))
                .isInstanceOfSatisfying(AllocatorProtocolException.class, error -> assertThat(error.code())
                        .isEqualTo(AllocatorProtocolException.Code.SLICE_EXHAUSTED));
    }

    @Test
    void retiringOrDriftedSliceFailsClosed() {
        VirtualLedgerCellAllocatorStateV1 cell = cell(AllocatorModeV1.RANGE_LEASED);
        VirtualLedgerSliceAssignmentV1 retiring = assignment().withLifecycle(VirtualLedgerSliceLifecycleV1.RETIRING);
        VirtualLedgerSliceViewV1 view = new VirtualLedgerSliceViewV1(namespace(), 2, retiring);
        assertThatThrownBy(() -> AllocatorProtocolV1.requireCurrentActiveSlice(cell, view))
                .isInstanceOfSatisfying(AllocatorProtocolException.class, error -> assertThat(error.code())
                        .isEqualTo(AllocatorProtocolException.Code.SLICE_NOT_ACTIVE));
    }

    @Test
    void wireRoundTripsExactFixedWidthsAndRejectsReservedCorruption() {
        VirtualLedgerCellAllocatorStateV1 cell = cell(AllocatorModeV1.RANGE_LEASED);
        ManagedLedgerAllocatorHeadV1 head = head(cell, 7);
        cell = AllocatorProtocolV1.reserve(cell, head, digest("request"), 8);
        head = AllocatorProtocolV1.installReservedRange(cell, head);
        VirtualLedgerCandidateNodeV1 node = AllocatorProtocolV1.candidate(head, digest("descriptor"));

        assertThat(AllocatorWireV1.encodeCell(cell).length()).isEqualTo(AllocatorWireV1.CELL_BYTES);
        assertThat(AllocatorWireV1.decodeCell(AllocatorWireV1.encodeCell(cell))).isEqualTo(cell);
        assertThat(AllocatorWireV1.encodeHead(head).length()).isEqualTo(AllocatorWireV1.HEAD_BYTES);
        assertThat(AllocatorWireV1.decodeHead(AllocatorWireV1.encodeHead(head))).isEqualTo(head);
        assertThat(AllocatorWireV1.encodeNode(node).length()).isEqualTo(AllocatorWireV1.NODE_BYTES);
        assertThat(AllocatorWireV1.decodeNode(AllocatorWireV1.encodeNode(node))).isEqualTo(node);
        assertThat(node.nodeId().toHex()).isEqualTo("c9e5c45effda5baa42631dd25e6edf79e37fc5414f44d7c839bfc53207ddf231");
        assertThat(node.nodeDigest().toHex())
                .isEqualTo("d2800feb75cb35c44d44081665527794695489d64b1112c8886e099fab522e77");

        byte[] corrupted = AllocatorWireV1.encodeHead(head).toByteArray();
        corrupted[corrupted.length - 1] = 1;
        assertThatThrownBy(() -> AllocatorWireV1.decodeHead(CanonicalBytes.copyOf(corrupted)))
                .isInstanceOfSatisfying(AllocatorProtocolException.class, error -> assertThat(error.code())
                        .isEqualTo(AllocatorProtocolException.Code.NON_CANONICAL_WIRE));
        assertThatThrownBy(() ->
                        new ManagedLedgerAllocatorHeadV1(1, incarnation(), 1, ChainPointerV1.absent(), 1, 1, 2, 1))
                .isInstanceOfSatisfying(AllocatorProtocolException.class, error -> assertThat(error.code())
                        .isEqualTo(AllocatorProtocolException.Code.HEAD_GEOMETRY));
    }

    @Test
    void completeEvidenceEntryMatrixContains10kAnd100kAtAllFrozenLatencies() {
        List<AllocatorEvidenceWorkloadV1> workloads =
                AllocatorEvidenceWorkloadV1.completeMatrix(AllocatorModeV1.RANGE_LEASED, 64, 4);
        assertThat(workloads).hasSize(8);
        assertThat(workloads)
                .extracting(AllocatorEvidenceWorkloadV1::activeManagedLedgers)
                .containsOnly(10_000, 100_000);
        assertThat(workloads)
                .extracting(AllocatorEvidenceWorkloadV1::metadataLatencyP99Millis)
                .containsOnly(1, 5, 10, 25);
        assertThat(AllocatorDeterministicEvidenceHarnessV1.completePlan(AllocatorModeV1.RANGE_LEASED, 64, 4))
                .hasSize(8 * AllocatorDeterministicEvidenceHarnessV1.FaultCut.values().length)
                .extracting(AllocatorDeterministicEvidenceHarnessV1.Scenario::faultCut)
                .containsExactlyInAnyOrderElementsOf(
                        java.util.Arrays.stream(AllocatorDeterministicEvidenceHarnessV1.FaultCut.values())
                                .flatMap(cut -> java.util.stream.Stream.generate(() -> cut)
                                        .limit(8))
                                .toList());
    }

    @Test
    void selectionReceiptHasNoCallerAggregateOrBooleanConstructionPath() {
        AllocatorEvidenceSourceArtifactsV1 absentArtifacts = new AllocatorEvidenceSourceArtifactsV1(
                java.nio.file.Path.of("absent-client"),
                java.nio.file.Path.of("absent-evidence"),
                java.nio.file.Path.of("absent-domain"),
                java.nio.file.Path.of("absent-spi"),
                java.nio.file.Path.of("absent-oxia"),
                java.nio.file.Path.of("absent-locks"),
                java.nio.file.Path.of("absent-executor"));
        assertThatThrownBy(() -> AllocatorSelectionReceiptV1.evaluateCanonicalAttachments(List.of(), absentArtifacts))
                .isInstanceOfSatisfying(AllocatorProtocolException.class, error -> assertThat(error.code())
                        .isEqualTo(AllocatorProtocolException.Code.SELECTION_NOT_ELIGIBLE));
        assertThat(AllocatorSelectionReceiptV1.class.getConstructors()).isEmpty();
        assertThat(java.util.Arrays.stream(AllocatorSelectionReceiptV1.class.getMethods())
                        .flatMap(method -> java.util.Arrays.stream(method.getParameterTypes()))
                        .filter(type -> type == AllocatorNativeRelativeMetricsV1.class || type == boolean.class))
                .isEmpty();
    }

    private static VirtualLedgerCellAllocatorStateV1 cell(AllocatorModeV1 mode) {
        return VirtualLedgerCellAllocatorStateV1.initial(mode, assignment());
    }

    private static ManagedLedgerAllocatorHeadV1 head(VirtualLedgerCellAllocatorStateV1 cell, long ownerEpoch) {
        return ManagedLedgerAllocatorHeadV1.initial(incarnation(), ownerEpoch, cell.nextSliceLedgerId());
    }

    private static ManagedLedgerIncarnationIdV1 incarnation() {
        return new ManagedLedgerIncarnationIdV1(digest("managed-ledger-incarnation"));
    }

    private static VirtualLedgerSliceAssignmentV1 assignment() {
        return VirtualLedgerSliceAssignmentV1.create(
                new DeploymentId(new Id128(1, 2)),
                new ReservationDomainId(new Id128(3, 4)),
                new PulsarCellId(new Id128(5, 6)),
                namespace(),
                VirtualLedgerSliceAssignmentV1.RESERVED_START_INCLUSIVE,
                VirtualLedgerSliceLifecycleV1.ACTIVE);
    }

    private static Sha256Digest namespace() {
        return digest("compatibility-namespace");
    }

    private static Sha256Digest digest(String value) {
        return Sha256Digest.hash(CanonicalBytes.copyOf(value.getBytes(StandardCharsets.UTF_8)));
    }
}
