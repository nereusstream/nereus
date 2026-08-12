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
import com.nereusstream.domain.receipt.VirtualLedgerReceiptV1;
import com.nereusstream.domain.receipt.VirtualLedgerReceiptV1.ReceiptKind;
import java.util.List;
import org.junit.jupiter.api.Test;

class AllocatorEvidenceProtocolHarnessTest {
    @Test
    void strictCandidateUsesFourDistinctSuccessfulCuts() {
        var harness = strict();
        assertThat(harness.reserve(1, AllocatorCandidateHarness.Cut.NONE))
                .isEqualTo(AllocatorCandidateHarness.Outcome.APPLIED_EXACT);
        assertThat(harness.installReserved(AllocatorCandidateHarness.Cut.NONE))
                .isEqualTo(AllocatorCandidateHarness.Outcome.APPLIED_EXACT);
        var candidate = harness.createCandidate("node-a");
        assertThat(harness.publishCandidate(candidate, AllocatorCandidateHarness.Cut.NONE))
                .isEqualTo(AllocatorCandidateHarness.Outcome.APPLIED_EXACT);
        assertThat(harness.clearReserved(AllocatorCandidateHarness.Cut.NONE))
                .isEqualTo(AllocatorCandidateHarness.Outcome.APPLIED_EXACT);
    }

    @Test
    void reserveResponseLossConvergesByExactReread() {
        var harness = strict();
        assertThat(harness.reserve(1, AllocatorCandidateHarness.Cut.APPLIED_RESPONSE_LOST))
                .isEqualTo(AllocatorCandidateHarness.Outcome.INDETERMINATE);
        assertThat(harness.reserve(1, AllocatorCandidateHarness.Cut.NONE))
                .isEqualTo(AllocatorCandidateHarness.Outcome.EXISTING_EXACT);
    }

    @Test
    void absentResponseLossDoesNotInventAReservation() {
        var harness = strict();
        assertThat(harness.reserve(1, AllocatorCandidateHarness.Cut.NOT_APPLIED_RESPONSE_LOST))
                .isEqualTo(AllocatorCandidateHarness.Outcome.INDETERMINATE);
        assertThat(harness.reserved()).isEmpty();
    }

    @Test
    void nodeAndHeadResponseLossConvergeWithoutSecondLedgerId() {
        var harness = strict();
        harness.reserve(1, AllocatorCandidateHarness.Cut.NONE);
        harness.installReserved(AllocatorCandidateHarness.Cut.NONE);
        var candidate = harness.createCandidate("node-a");
        assertThat(harness.publishCandidate(candidate, AllocatorCandidateHarness.Cut.APPLIED_RESPONSE_LOST))
                .isEqualTo(AllocatorCandidateHarness.Outcome.INDETERMINATE);
        assertThat(harness.reconcilePublished(candidate)).isEqualTo(AllocatorCandidateHarness.Outcome.EXISTING_EXACT);
        assertThat(harness.head().nextLedgerId()).isEqualTo(101);
    }

    @Test
    void clearIsBackgroundButBlocksTheNextCellGrant() {
        var harness = strict();
        harness.reserve(1, AllocatorCandidateHarness.Cut.NONE);
        harness.installReserved(AllocatorCandidateHarness.Cut.NONE);
        assertThat(harness.reserve(1, AllocatorCandidateHarness.Cut.NONE))
                .isEqualTo(AllocatorCandidateHarness.Outcome.DEFINITIVE_CONFLICT);
        harness.clearReserved(AllocatorCandidateHarness.Cut.NONE);
        assertThat(harness.reserve(1, AllocatorCandidateHarness.Cut.NONE))
                .isEqualTo(AllocatorCandidateHarness.Outcome.APPLIED_EXACT);
    }

    @Test
    void receiptKindDerivesPermanentNonSelection() {
        assertThat(ReceiptKind.HARNESS_CONFORMANCE_ONLY).isNotEqualTo(ReceiptKind.REGISTRY_CONFORMANCE);
        assertThat(VirtualLedgerReceiptV1.SCHEMA).isEqualTo("NEREUS_VIRTUAL_LEDGER_RECEIPT_V1");
        assertThat(List.of(ReceiptKind.values()))
                .containsExactly(ReceiptKind.REGISTRY_CONFORMANCE, ReceiptKind.HARNESS_CONFORMANCE_ONLY);
    }

    private static AllocatorCandidateHarness strict() {
        return new AllocatorCandidateHarness(AllocatorCandidateHarness.Mode.STRICT_SERIALIZED, 100, 7, 1);
    }
}
