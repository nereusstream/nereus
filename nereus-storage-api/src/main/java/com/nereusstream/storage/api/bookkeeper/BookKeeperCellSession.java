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

package com.nereusstream.storage.api.bookkeeper;

import java.util.concurrent.CompletionStage;

/**
 * One independently drainable provider session for one Cell Provider Scope.
 *
 * <p>A method that throws before returning a stage did not accept the operation. Once a stage is returned, the session
 * owns the operation, its permits, and (for append) one retained payload reference through terminal reconciliation.
 * Cancelling or timing out an observer is not terminal reconciliation.
 */
public interface BookKeeperCellSession {
    CellProviderScopeId providerScopeId();

    BookKeeperCapabilitySnapshotV1 capabilitySnapshot();

    CompletionStage<ProviderMutationResultV1<RunLedgerHandleV1>> createRunLedger(
            RunLedgerConfigurationV1 configuration);

    /**
     * Allocates one never-reused native ledger ID without creating a ledger. The caller must durably record an exact
     * successful result in its task inventory before createReservedRunLedger; an unknown allocation burns only an ID.
     */
    default CompletionStage<ProviderMutationResultV1<BookKeeperLedgerIdentity>> reserveLedgerIdentity() {
        throw new UnsupportedOperationException("native ledger identity reservation is not implemented");
    }

    /** Creates only the natively reserved, durably inventoried ID; retries may never allocate a different ID. */
    default CompletionStage<ProviderMutationResultV1<RunLedgerHandleV1>> createReservedRunLedger(
            RunLedgerConfigurationV1 configuration, BookKeeperLedgerIdentity reservedIdentity) {
        throw new UnsupportedOperationException("reserved ledger creation is not implemented");
    }

    CompletionStage<RunLedgerOpenResultV1> openRunLedger(RunLedgerHandleV1 expectedHandle);

    CompletionStage<ProviderMutationResultV1<AppendQuorumProofV1>> appendExplicitEntry(
            RunLedgerAppendRequestV1 request);

    CompletionStage<RunLedgerReadResultV1> readExactEntry(RunLedgerHandleV1 handle, long entryId);

    CompletionStage<ProviderMutationResultV1<RunLedgerRecoveryProofV1>> fenceAndRecoverRunLedger(
            RunLedgerHandleV1 handle);

    CompletionStage<ProviderMutationResultV1<RunLedgerCloseProofV1>> closeRunLedger(RunLedgerHandleV1 handle);

    /** Stops new admission and completes after no accepted operation, payload, permit, or resolver remains. */
    CompletionStage<Void> drain();

    /** Drains and then closes only this Cell session; a borrowed transport is outside its authority. */
    CompletionStage<Void> closeAsync();
}
