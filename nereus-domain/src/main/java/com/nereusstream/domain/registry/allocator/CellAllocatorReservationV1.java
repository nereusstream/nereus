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
import java.util.Objects;

/** One Cell-wide RESERVED fact permanently bound to a ManagedLedger incarnation and grant. */
public record CellAllocatorReservationV1(
        ManagedLedgerIncarnationIdV1 managedLedgerIncarnation,
        long grantId,
        long rangeStartInclusive,
        long rangeEndExclusive,
        Sha256Digest requestId,
        AllocatorHeadStateV1 expectedAllocationState) {
    public CellAllocatorReservationV1 {
        Objects.requireNonNull(managedLedgerIncarnation, "managedLedgerIncarnation");
        Objects.requireNonNull(requestId, "requestId");
        Objects.requireNonNull(expectedAllocationState, "expectedAllocationState");
        if (grantId <= 0 || rangeStartInclusive <= 0 || rangeEndExclusive <= rangeStartInclusive) {
            throw new IllegalArgumentException("reservation grant/range is invalid");
        }
        if (requestId.isZero()) {
            throw new IllegalArgumentException("reservation request ID must be non-zero");
        }
    }
}
