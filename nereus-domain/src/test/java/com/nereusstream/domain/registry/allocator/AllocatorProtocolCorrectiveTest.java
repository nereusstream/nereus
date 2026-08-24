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
import java.nio.charset.StandardCharsets;
import org.junit.jupiter.api.Test;

class AllocatorProtocolCorrectiveTest {
    @Test
    void strictStaleCreatorIsOwnerFencedAndCannotInstallOrClearEarly() {
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
    void strictTakeoverBurnsExactReservedStaleNodeThenClearsWithoutPublishingIt() {
        VirtualLedgerCellAllocatorStateV1 cell = cell(AllocatorModeV1.STRICT_SERIALIZED);
        ManagedLedgerAllocatorHeadV1 original = head(cell, 7);
        cell = AllocatorProtocolV1.reserve(cell, original, digest("strict-reserve"), 1);
        VirtualLedgerCandidateNodeV1 stale =
                AllocatorProtocolV1.strictCandidateFromReservation(cell, original, digest("strict-stale"));
        ManagedLedgerAllocatorHeadV1 takenOver = AllocatorProtocolV1.takeover(original, 8);

        ManagedLedgerAllocatorHeadV1 burned =
                AllocatorProtocolV1.burnStrictReservedStaleCandidate(cell, takenOver, stale);
        assertThat(burned.visibleChainHead()).isEqualTo(original.visibleChainHead());
        assertThat(burned.nextLedgerId()).isEqualTo(stale.ledgerId() + 1);
        assertThat(AllocatorProtocolV1.burnStrictReservedStaleCandidate(cell, burned, stale))
                .isSameAs(burned);
        assertThat(AllocatorProtocolV1.clearStrictTerminalReservation(cell, burned, stale)
                        .reservation())
                .isEmpty();

        VirtualLedgerCandidateNodeV1 currentOwner = AllocatorWireV1.createNode(
                stale.managedLedgerIncarnation(),
                stale.ledgerId(),
                stale.grantId(),
                takenOver.ownerEpoch(),
                stale.expectedPredecessor(),
                digest("not-stale"));
        VirtualLedgerCellAllocatorStateV1 reserved = cell;
        assertThatThrownBy(
                        () -> AllocatorProtocolV1.burnStrictReservedStaleCandidate(reserved, takenOver, currentOwner))
                .isInstanceOfSatisfying(AllocatorProtocolException.class, error -> assertThat(error.code())
                        .isEqualTo(AllocatorProtocolException.Code.STALE_CANDIDATE_REQUIRED));
        assertThatThrownBy(() -> AllocatorProtocolV1.clearStrictTerminalReservation(reserved, burned, currentOwner))
                .isInstanceOfSatisfying(AllocatorProtocolException.class, error -> assertThat(error.code())
                        .isEqualTo(AllocatorProtocolException.Code.CANDIDATE_OCCUPANCY_NOT_PROVEN));
    }

    @Test
    void rangeUnusedTailCannotBeRegrantedAndAbandonmentIsTerminalAccountingOnly() {
        VirtualLedgerCellAllocatorStateV1 cell = cell(AllocatorModeV1.RANGE_LEASED);
        ManagedLedgerAllocatorHeadV1 head = head(cell, 10);
        cell = AllocatorProtocolV1.reserve(cell, head, digest("range-one"), 16);
        head = AllocatorProtocolV1.installReservedRange(cell, head);
        VirtualLedgerCellAllocatorStateV1 cleared = AllocatorProtocolV1.clearInstalledReservation(cell, head);
        ManagedLedgerAllocatorHeadV1 installed = head;

        assertThatThrownBy(() -> AllocatorProtocolV1.reserve(cleared, installed, digest("range-two"), 16))
                .isInstanceOfSatisfying(AllocatorProtocolException.class, error -> assertThat(error.code())
                        .isEqualTo(AllocatorProtocolException.Code.RANGE_TAIL_NOT_EXHAUSTED));
        TerminalInstalledRangeAbandonmentV1 terminal = AllocatorProtocolV1.abandonInstalledRangeTerminal(
                cleared, installed, InstalledRangeAbandonmentAuthorityV1.SLICE_RETIREMENT);
        assertThat(terminal.nextLedgerId()).isLessThan(terminal.rangeEndExclusive());
    }

    @Test
    void rangeReserveContinuesOnlyAfterInstalledTailIsActuallyExhausted() {
        VirtualLedgerCellAllocatorStateV1 cell = cell(AllocatorModeV1.RANGE_LEASED);
        ManagedLedgerAllocatorHeadV1 head = head(cell, 10);
        cell = AllocatorProtocolV1.reserve(cell, head, digest("range-one"), 16);
        head = AllocatorProtocolV1.installReservedRange(cell, head);
        VirtualLedgerCellAllocatorStateV1 cleared = AllocatorProtocolV1.clearInstalledReservation(cell, head);
        for (int index = 0; index < 16; index++) {
            head = AllocatorProtocolV1.publish(head, AllocatorProtocolV1.candidate(head, digest("node-" + index)));
        }

        VirtualLedgerCellAllocatorStateV1 successor =
                AllocatorProtocolV1.reserve(cleared, head, digest("range-two"), 16);
        assertThat(successor.reservation()).isPresent();
        assertThat(successor.nextSliceLedgerId()).isEqualTo(cleared.nextSliceLedgerId() + 16);
    }

    @Test
    void headOutsideReservedIntervalOrConsumedCellPrefixFailsClosed() {
        assertThatThrownBy(() ->
                        new ManagedLedgerAllocatorHeadV1(1, incarnation(), 1, ChainPointerV1.absent(), 1, 1, 2, 1))
                .isInstanceOfSatisfying(AllocatorProtocolException.class, error -> assertThat(error.code())
                        .isEqualTo(AllocatorProtocolException.Code.HEAD_GEOMETRY));

        VirtualLedgerCellAllocatorStateV1 cell = cell(AllocatorModeV1.RANGE_LEASED);
        ManagedLedgerAllocatorHeadV1 future =
                ManagedLedgerAllocatorHeadV1.initial(incarnation(), 1, cell.nextSliceLedgerId() + 1);
        assertThatThrownBy(() -> AllocatorProtocolV1.requireHeadWithinConsumedSlicePrefix(cell, future))
                .isInstanceOfSatisfying(AllocatorProtocolException.class, error -> assertThat(error.code())
                        .isEqualTo(AllocatorProtocolException.Code.HEAD_GEOMETRY));
    }

    private static VirtualLedgerCellAllocatorStateV1 cell(AllocatorModeV1 mode) {
        return VirtualLedgerCellAllocatorStateV1.initial(mode, assignment());
    }

    private static ManagedLedgerAllocatorHeadV1 head(VirtualLedgerCellAllocatorStateV1 cell, long ownerEpoch) {
        return ManagedLedgerAllocatorHeadV1.initial(incarnation(), ownerEpoch, cell.nextSliceLedgerId());
    }

    private static VirtualLedgerSliceAssignmentV1 assignment() {
        return VirtualLedgerSliceAssignmentV1.create(
                new DeploymentId(new Id128(1, 2)),
                new ReservationDomainId(new Id128(3, 4)),
                new PulsarCellId(new Id128(5, 6)),
                digest("namespace"),
                VirtualLedgerSliceAssignmentV1.RESERVED_START_INCLUSIVE,
                VirtualLedgerSliceLifecycleV1.ACTIVE);
    }

    private static ManagedLedgerIncarnationIdV1 incarnation() {
        return new ManagedLedgerIncarnationIdV1(digest("incarnation"));
    }

    private static Sha256Digest digest(String value) {
        return Sha256Digest.hash(CanonicalBytes.copyOf(value.getBytes(StandardCharsets.UTF_8)));
    }
}
