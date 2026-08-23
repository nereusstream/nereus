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

/** Terminal accounting fact for an unused RANGE tail. It is deliberately not accepted by reserve. */
public record TerminalInstalledRangeAbandonmentV1(
        Sha256Digest ledgerIdCompatibilityNamespaceId,
        Sha256Digest sliceAssignmentId,
        ManagedLedgerIncarnationIdV1 managedLedgerIncarnation,
        long grantId,
        long rangeStartInclusive,
        long rangeEndExclusive,
        long nextLedgerId,
        long ownerEpoch,
        InstalledRangeAbandonmentAuthorityV1 authority) {
    public TerminalInstalledRangeAbandonmentV1 {
        Objects.requireNonNull(ledgerIdCompatibilityNamespaceId, "ledgerIdCompatibilityNamespaceId");
        Objects.requireNonNull(sliceAssignmentId, "sliceAssignmentId");
        Objects.requireNonNull(managedLedgerIncarnation, "managedLedgerIncarnation");
        Objects.requireNonNull(authority, "authority");
        if (ledgerIdCompatibilityNamespaceId.isZero()
                || sliceAssignmentId.isZero()
                || grantId <= 0
                || rangeStartInclusive <= 0
                || nextLedgerId < rangeStartInclusive
                || nextLedgerId >= rangeEndExclusive
                || ownerEpoch <= 0) {
            throw new IllegalArgumentException("terminal RANGE abandonment must bind one non-empty installed tail");
        }
    }
}
