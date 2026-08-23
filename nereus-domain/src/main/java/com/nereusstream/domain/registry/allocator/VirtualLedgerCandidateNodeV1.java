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

/** Immutable candidate Ledger Chain node, published only by an exact Head CAS. */
public record VirtualLedgerCandidateNodeV1(
        int allocatorProtocolVersion,
        ManagedLedgerIncarnationIdV1 managedLedgerIncarnation,
        long ledgerId,
        long grantId,
        long creatorOwnerEpoch,
        ChainPointerV1 expectedPredecessor,
        Sha256Digest ledgerDescriptorDigest,
        Sha256Digest nodeId,
        Sha256Digest nodeDigest) {
    public VirtualLedgerCandidateNodeV1 {
        Objects.requireNonNull(managedLedgerIncarnation, "managedLedgerIncarnation");
        Objects.requireNonNull(expectedPredecessor, "expectedPredecessor");
        Objects.requireNonNull(ledgerDescriptorDigest, "ledgerDescriptorDigest");
        Objects.requireNonNull(nodeId, "nodeId");
        Objects.requireNonNull(nodeDigest, "nodeDigest");
        if (allocatorProtocolVersion != VirtualLedgerCellAllocatorStateV1.PROTOCOL_VERSION) {
            throw new AllocatorProtocolException(
                    AllocatorProtocolException.Code.PROTOCOL_VERSION, "allocator protocol version must be 1");
        }
        if (ledgerId <= 0 || grantId <= 0 || creatorOwnerEpoch <= 0) {
            throw new IllegalArgumentException("candidate ledger/grant/owner identity is invalid");
        }
        if (ledgerDescriptorDigest.isZero() || nodeId.isZero() || nodeDigest.isZero()) {
            throw new IllegalArgumentException("candidate descriptor/node identities must be non-zero");
        }
    }

    public ChainPointerV1 pointer() {
        return new ChainPointerV1(nodeId, nodeDigest);
    }
}
