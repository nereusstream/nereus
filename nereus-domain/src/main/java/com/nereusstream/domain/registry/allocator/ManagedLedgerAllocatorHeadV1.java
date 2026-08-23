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

import com.nereusstream.domain.registry.VirtualLedgerSliceAssignmentV1;
import java.util.Objects;

/** Owner-fenced ManagedLedger allocation cursor and explicit Ledger Chain head. */
public record ManagedLedgerAllocatorHeadV1(
        int allocatorProtocolVersion,
        ManagedLedgerIncarnationIdV1 managedLedgerIncarnation,
        long ownerEpoch,
        ChainPointerV1 visibleChainHead,
        long grantId,
        long rangeStartInclusive,
        long rangeEndExclusive,
        long nextLedgerId) {
    public ManagedLedgerAllocatorHeadV1 {
        Objects.requireNonNull(managedLedgerIncarnation, "managedLedgerIncarnation");
        Objects.requireNonNull(visibleChainHead, "visibleChainHead");
        if (allocatorProtocolVersion != VirtualLedgerCellAllocatorStateV1.PROTOCOL_VERSION) {
            throw new AllocatorProtocolException(
                    AllocatorProtocolException.Code.PROTOCOL_VERSION, "allocator protocol version must be 1");
        }
        if (ownerEpoch <= 0 || grantId < 0 || nextLedgerId < 0) {
            throw new IllegalArgumentException("head owner/grant/cursor is invalid");
        }
        new AllocatorHeadStateV1(visibleChainHead, grantId, rangeStartInclusive, rangeEndExclusive, nextLedgerId);
        if (nextLedgerId < VirtualLedgerSliceAssignmentV1.RESERVED_START_INCLUSIVE
                || nextLedgerId > VirtualLedgerSliceAssignmentV1.RESERVED_END_INCLUSIVE + 1
                || (grantId != 0
                        && (rangeStartInclusive < VirtualLedgerSliceAssignmentV1.RESERVED_START_INCLUSIVE
                                || rangeEndExclusive > VirtualLedgerSliceAssignmentV1.RESERVED_END_INCLUSIVE + 1))) {
            throw new AllocatorProtocolException(
                    AllocatorProtocolException.Code.HEAD_GEOMETRY,
                    "allocator Head lies outside the reserved virtual-ledger interval");
        }
    }

    public static ManagedLedgerAllocatorHeadV1 initial(
            ManagedLedgerIncarnationIdV1 incarnation, long ownerEpoch, long initialSliceLedgerId) {
        return new ManagedLedgerAllocatorHeadV1(
                VirtualLedgerCellAllocatorStateV1.PROTOCOL_VERSION,
                incarnation,
                ownerEpoch,
                ChainPointerV1.absent(),
                0,
                0,
                0,
                initialSliceLedgerId);
    }

    public AllocatorHeadStateV1 allocationState() {
        return new AllocatorHeadStateV1(
                visibleChainHead, grantId, rangeStartInclusive, rangeEndExclusive, nextLedgerId);
    }
}
