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

import com.nereusstream.domain.bytes.Sha256Digest;
import com.nereusstream.domain.registry.VirtualLedgerSliceAssignmentV1;
import java.util.Objects;
import java.util.Optional;

/** Exact Cell-wide allocator state; a reservation blocks the next grant until exact clear. */
public record VirtualLedgerCellAllocatorStateV1(
        AllocatorModeV1 mode,
        int allocatorProtocolVersion,
        Sha256Digest ledgerIdCompatibilityNamespaceId,
        Sha256Digest sliceAssignmentId,
        long sliceStartInclusive,
        long sliceEndInclusive,
        long nextSliceLedgerId,
        long nextGrantId,
        Optional<CellAllocatorReservationV1> reservation) {
    public static final int PROTOCOL_VERSION = 1;

    public VirtualLedgerCellAllocatorStateV1 {
        Objects.requireNonNull(mode, "mode");
        Objects.requireNonNull(ledgerIdCompatibilityNamespaceId, "ledgerIdCompatibilityNamespaceId");
        Objects.requireNonNull(sliceAssignmentId, "sliceAssignmentId");
        Objects.requireNonNull(reservation, "reservation");
        if (allocatorProtocolVersion != PROTOCOL_VERSION) {
            throw failure(AllocatorProtocolException.Code.PROTOCOL_VERSION, "allocator protocol version must be 1");
        }
        if (ledgerIdCompatibilityNamespaceId.isZero() || sliceAssignmentId.isZero()) {
            throw new IllegalArgumentException("allocator namespace and slice assignment IDs must be non-zero");
        }
        if (sliceStartInclusive < VirtualLedgerSliceAssignmentV1.RESERVED_START_INCLUSIVE
                || sliceEndInclusive > VirtualLedgerSliceAssignmentV1.RESERVED_END_INCLUSIVE
                || sliceEndInclusive - sliceStartInclusive != VirtualLedgerSliceAssignmentV1.SLICE_SIZE - 1
                || (sliceStartInclusive - VirtualLedgerSliceAssignmentV1.RESERVED_START_INCLUSIVE)
                                % VirtualLedgerSliceAssignmentV1.SLICE_SIZE
                        != 0) {
            throw new IllegalArgumentException("allocator state must own one exact aligned Registry slice");
        }
        if (nextSliceLedgerId < sliceStartInclusive || nextSliceLedgerId > sliceEndInclusive + 1 || nextGrantId <= 0) {
            throw new IllegalArgumentException("allocator cursor/grant ID is outside its legal bounds");
        }
        reservation.ifPresent(value -> {
            if (value.grantId() >= nextGrantId
                    || value.rangeStartInclusive() < sliceStartInclusive
                    || value.rangeEndExclusive() > sliceEndInclusive + 1
                    || value.rangeEndExclusive() > nextSliceLedgerId) {
                throw new IllegalArgumentException("reservation lies outside the consumed slice prefix");
            }
        });
    }

    public static VirtualLedgerCellAllocatorStateV1 initial(
            AllocatorModeV1 mode, VirtualLedgerSliceAssignmentV1 assignment) {
        Objects.requireNonNull(assignment, "assignment");
        return new VirtualLedgerCellAllocatorStateV1(
                mode,
                PROTOCOL_VERSION,
                assignment.ledgerIdCompatibilityNamespaceId(),
                assignment.sliceAssignmentId(),
                assignment.startInclusive(),
                assignment.endInclusive(),
                assignment.startInclusive(),
                1,
                Optional.empty());
    }

    public boolean exhausted() {
        return nextSliceLedgerId == sliceEndInclusive + 1;
    }

    private static AllocatorProtocolException failure(AllocatorProtocolException.Code code, String message) {
        return new AllocatorProtocolException(code, message);
    }
}
