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
import com.nereusstream.domain.bytes.Sha256Digest;
import com.nereusstream.storage.api.bookkeeper.CellProviderScopeId;
import com.nereusstream.storage.object.gc.M5PhysicalGcCellAdmissionV1.AdmissionOutcome;
import com.nereusstream.storage.object.gc.M5PhysicalGcCellAdmissionV1.CellBudgetEnvelopeV1;
import com.nereusstream.storage.object.gc.M5PhysicalGcCellAdmissionV1.CellBudgetStateV1;
import com.nereusstream.storage.object.gc.M5PhysicalGcCellAdmissionV1.ResourceVectorV1;
import org.junit.jupiter.api.Test;

class M5PhysicalGcCellAdmissionV1Test {
    @Test
    void admitsOnlyWithinOneCellsHardReservationEnvelope() {
        CellProviderScopeId cell = cell(1);
        CellBudgetEnvelopeV1 envelope =
                new CellBudgetEnvelopeV1(cell, ResourceVectorV1.uniform(2), ResourceVectorV1.uniform(10));

        var admitted = M5PhysicalGcCellAdmissionV1.admit(
                envelope,
                new CellBudgetStateV1(cell, ResourceVectorV1.uniform(4)),
                new CellBudgetStateV1(cell, ResourceVectorV1.uniform(6)));

        assertThat(admitted.outcome()).isEqualTo(AdmissionOutcome.ADMITTED);
        assertThat(admitted.updatedState().orElseThrow().usage().inventory().candidateCount())
                .isEqualTo(10);
        assertThat(admitted.updatedState().orElseThrow().usage().queues().oldestPendingAgeMillis())
                .isEqualTo(6);
        assertThat(admitted.updatedState().orElseThrow().usage().memory().perTargetBufferBytes())
                .isEqualTo(6);
    }

    @Test
    void anyCountByteQueueConcurrencyRateMemoryScannerOrQuarantineCapFailsClosed() {
        CellProviderScopeId cell = cell(1);
        CellBudgetEnvelopeV1 envelope =
                new CellBudgetEnvelopeV1(cell, ResourceVectorV1.zero(), ResourceVectorV1.uniform(10));
        ResourceVectorV1 current = ResourceVectorV1.uniform(10);
        ResourceVectorV1 requested = new ResourceVectorV1(
                new M5PhysicalGcCellAdmissionV1.InventoryUsageV1(1, 0, 0, 0, 0, 0),
                M5PhysicalGcCellAdmissionV1.QueueUsageV1.uniform(0),
                M5PhysicalGcCellAdmissionV1.ConcurrencyUsageV1.uniform(0),
                M5PhysicalGcCellAdmissionV1.RateUsageV1.uniform(0),
                M5PhysicalGcCellAdmissionV1.MemoryUsageV1.uniform(0),
                M5PhysicalGcCellAdmissionV1.ScannerUsageV1.uniform(0),
                M5PhysicalGcCellAdmissionV1.QuarantineUsageV1.uniform(0));

        assertThat(M5PhysicalGcCellAdmissionV1.admit(
                                envelope, new CellBudgetStateV1(cell, current), new CellBudgetStateV1(cell, requested))
                        .outcome())
                .isEqualTo(AdmissionOutcome.CELL_HARD_LIMIT_REACHED);
    }

    @Test
    void arithmeticOverflowFailsClosedInsteadOfWrappingBelowLimit() {
        CellProviderScopeId cell = cell(1);
        CellBudgetEnvelopeV1 envelope =
                new CellBudgetEnvelopeV1(cell, ResourceVectorV1.zero(), ResourceVectorV1.uniform(Long.MAX_VALUE));

        assertThat(M5PhysicalGcCellAdmissionV1.admit(
                                envelope,
                                new CellBudgetStateV1(cell, ResourceVectorV1.uniform(Long.MAX_VALUE)),
                                new CellBudgetStateV1(cell, ResourceVectorV1.uniform(1)))
                        .outcome())
                .isEqualTo(AdmissionOutcome.CELL_HARD_LIMIT_REACHED);
    }

    @Test
    void aCellCannotBorrowAnotherCellsIdentityOrCapacity() {
        CellProviderScopeId first = cell(1);
        CellProviderScopeId second = cell(2);
        CellBudgetEnvelopeV1 firstEnvelope =
                new CellBudgetEnvelopeV1(first, ResourceVectorV1.uniform(2), ResourceVectorV1.uniform(5));
        CellBudgetEnvelopeV1 secondEnvelope =
                new CellBudgetEnvelopeV1(second, ResourceVectorV1.uniform(2), ResourceVectorV1.uniform(5));

        assertThat(M5PhysicalGcCellAdmissionV1.admit(
                                firstEnvelope,
                                new CellBudgetStateV1(first, ResourceVectorV1.uniform(5)),
                                new CellBudgetStateV1(second, ResourceVectorV1.zero()))
                        .outcome())
                .isEqualTo(AdmissionOutcome.CELL_IDENTITY_MISMATCH);
        assertThat(M5PhysicalGcCellAdmissionV1.admit(
                                firstEnvelope,
                                new CellBudgetStateV1(first, ResourceVectorV1.uniform(5)),
                                new CellBudgetStateV1(first, ResourceVectorV1.uniform(1)))
                        .outcome())
                .isEqualTo(AdmissionOutcome.CELL_HARD_LIMIT_REACHED);
        assertThat(M5PhysicalGcCellAdmissionV1.admit(
                                secondEnvelope,
                                new CellBudgetStateV1(second, ResourceVectorV1.zero()),
                                new CellBudgetStateV1(second, ResourceVectorV1.uniform(1)))
                        .outcome())
                .isEqualTo(AdmissionOutcome.ADMITTED);
    }

    @Test
    void reservedMinimumCannotExceedTheSameCellsHardLimit() {
        assertThatThrownBy(() ->
                        new CellBudgetEnvelopeV1(cell(1), ResourceVectorV1.uniform(11), ResourceVectorV1.uniform(10)))
                .isInstanceOf(IllegalArgumentException.class)
                .hasMessageContaining("reserved minimum");
    }

    private static CellProviderScopeId cell(int lastByte) {
        byte[] bytes = new byte[Sha256Digest.LENGTH];
        bytes[bytes.length - 1] = (byte) lastByte;
        return new CellProviderScopeId(Sha256Digest.copyOf(bytes));
    }
}
