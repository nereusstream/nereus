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

package com.nereusstream.metadata.spi.allocator;

import com.nereusstream.domain.bytes.Sha256Digest;
import com.nereusstream.domain.registry.allocator.ManagedLedgerAllocatorHeadV1;
import com.nereusstream.domain.registry.allocator.ManagedLedgerIncarnationIdV1;
import com.nereusstream.domain.registry.allocator.VirtualLedgerCandidateNodeV1;
import com.nereusstream.domain.registry.allocator.VirtualLedgerCellAllocatorStateV1;
import com.nereusstream.metadata.spi.model.ConditionalCasResult;
import com.nereusstream.metadata.spi.model.CreateMutationResult;
import java.util.Optional;
import java.util.concurrent.CompletionStage;

/** Exact-key, exact-value, no-delete metadata authority for the production virtual-ledger allocator. */
public interface PulsarVirtualLedgerAllocatorStore {
    CompletionStage<Optional<VersionedAllocatorCellStateV1>> readCell(
            Sha256Digest namespaceId, Sha256Digest sliceAssignmentId);

    CompletionStage<CreateMutationResult<VersionedAllocatorCellStateV1>> createCell(
            VirtualLedgerCellAllocatorStateV1 candidate);

    CompletionStage<ConditionalCasResult<VersionedAllocatorCellStateV1>> compareAndSetCell(
            VersionedAllocatorCellStateV1 exactPredecessor, VirtualLedgerCellAllocatorStateV1 candidate);

    CompletionStage<Optional<VersionedManagedLedgerAllocatorHeadV1>> readHead(
            Sha256Digest namespaceId,
            Sha256Digest sliceAssignmentId,
            ManagedLedgerIncarnationIdV1 managedLedgerIncarnation);

    CompletionStage<CreateMutationResult<VersionedManagedLedgerAllocatorHeadV1>> createHead(
            Sha256Digest namespaceId, Sha256Digest sliceAssignmentId, ManagedLedgerAllocatorHeadV1 candidate);

    CompletionStage<ConditionalCasResult<VersionedManagedLedgerAllocatorHeadV1>> compareAndSetHead(
            Sha256Digest namespaceId,
            Sha256Digest sliceAssignmentId,
            VersionedManagedLedgerAllocatorHeadV1 exactPredecessor,
            ManagedLedgerAllocatorHeadV1 candidate);

    CompletionStage<Optional<VersionedVirtualLedgerCandidateNodeV1>> readNode(
            Sha256Digest namespaceId,
            Sha256Digest sliceAssignmentId,
            ManagedLedgerIncarnationIdV1 managedLedgerIncarnation,
            long ledgerId);

    CompletionStage<CreateMutationResult<VersionedVirtualLedgerCandidateNodeV1>> createNode(
            Sha256Digest namespaceId, Sha256Digest sliceAssignmentId, VirtualLedgerCandidateNodeV1 candidate);
}
