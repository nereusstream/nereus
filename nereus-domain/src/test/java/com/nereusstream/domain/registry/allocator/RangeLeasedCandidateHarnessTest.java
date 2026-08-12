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
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;

class RangeLeasedCandidateHarnessTest {
    private AllocatorCandidateHarness harness;

    @BeforeEach
    void setUp() {
        harness = new AllocatorCandidateHarness(AllocatorCandidateHarness.Mode.RANGE_LEASED, 1000, 91, 10);
    }

    @Test
    void grantBelongsToManagedLedgerIncarnationNotOwnerEpoch() {
        harness.reserve(8, AllocatorCandidateHarness.Cut.NONE);
        long grantId = harness.reserved().orElseThrow().grantId();
        harness.takeover(11);
        assertThat(harness.reconcileInstall()).isEqualTo(AllocatorCandidateHarness.Outcome.APPLIED_EXACT);
        assertThat(harness.head().grantId()).isEqualTo(grantId);
        assertThat(harness.head().ownerEpoch()).isEqualTo(11);
    }

    @Test
    void installedRangeSurvivesOwnerTakeover() {
        installRange();
        long end = harness.head().rangeEndExclusive();
        harness.takeover(11);
        assertThat(harness.head().rangeEndExclusive()).isEqualTo(end);
        assertThat(harness.createCandidate("new-owner").ledgerId()).isEqualTo(1000);
    }

    @Test
    void staleOwnerCandidateBurnsExactlyOneId() {
        installRange();
        var stale = harness.createCandidate("stale-owner");
        harness.takeover(11);
        assertThat(harness.burnStaleCandidate(stale.ledgerId()))
                .isEqualTo(AllocatorCandidateHarness.Outcome.APPLIED_EXACT);
        assertThat(harness.head().nextLedgerId()).isEqualTo(1001);
        assertThat(harness.head().rangeEndExclusive()).isEqualTo(1008);
    }

    @Test
    void newOwnerNeverAdoptsStaleOwnerCandidate() {
        installRange();
        var stale = harness.createCandidate("stale-owner");
        harness.takeover(11);
        assertThat(harness.publishCandidate(stale, AllocatorCandidateHarness.Cut.NONE))
                .isEqualTo(AllocatorCandidateHarness.Outcome.DEFINITIVE_CONFLICT);
    }

    @Test
    void installResponseLossConvergesToSameGrant() {
        harness.reserve(8, AllocatorCandidateHarness.Cut.NONE);
        assertThat(harness.installReserved(AllocatorCandidateHarness.Cut.APPLIED_RESPONSE_LOST))
                .isEqualTo(AllocatorCandidateHarness.Outcome.INDETERMINATE);
        assertThat(harness.reconcileInstall()).isEqualTo(AllocatorCandidateHarness.Outcome.EXISTING_EXACT);
    }

    @Test
    void clearResponseLossIsExactAndIdempotent() {
        installRange();
        assertThat(harness.clearReserved(AllocatorCandidateHarness.Cut.APPLIED_RESPONSE_LOST))
                .isEqualTo(AllocatorCandidateHarness.Outcome.INDETERMINATE);
        assertThat(harness.clearReserved(AllocatorCandidateHarness.Cut.NONE))
                .isEqualTo(AllocatorCandidateHarness.Outcome.EXISTING_EXACT);
    }

    @Test
    void rangeExhaustionFailsWithoutReusingAnId() {
        harness = new AllocatorCandidateHarness(AllocatorCandidateHarness.Mode.RANGE_LEASED, 1000, 91, 10);
        harness.reserve(1, AllocatorCandidateHarness.Cut.NONE);
        harness.installReserved(AllocatorCandidateHarness.Cut.NONE);
        var candidate = harness.createCandidate("only");
        harness.publishCandidate(candidate, AllocatorCandidateHarness.Cut.NONE);
        assertThatThrownBy(() -> harness.createCandidate("reuse")).isInstanceOf(IllegalStateException.class);
    }

    @Test
    void nonIncreasingTakeoverEpochFailsClosed() {
        assertThat(harness.takeover(10)).isEqualTo(AllocatorCandidateHarness.Outcome.DEFINITIVE_CONFLICT);
    }

    private void installRange() {
        harness.reserve(8, AllocatorCandidateHarness.Cut.NONE);
        harness.installReserved(AllocatorCandidateHarness.Cut.NONE);
    }
}
