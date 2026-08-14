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

import com.nereusstream.domain.bytes.Sha256Digest;
import java.util.Objects;

/** Exact run-ledger configuration selected from an admitted capability snapshot. */
public record RunLedgerConfigurationV1(
        CellProviderScopeId providerScopeId,
        StorageRunId runId,
        int ensembleSize,
        int writeQuorumSize,
        int ackQuorumSize,
        BookKeeperDigestTypeV1 digestType,
        Sha256Digest configurationDigest) {
    public RunLedgerConfigurationV1 {
        Objects.requireNonNull(providerScopeId, "providerScopeId");
        Objects.requireNonNull(runId, "runId");
        if (ackQuorumSize <= 0 || writeQuorumSize < ackQuorumSize || ensembleSize < writeQuorumSize) {
            throw new IllegalArgumentException("quorum sizes must satisfy 0 < ack <= write <= ensemble");
        }
        Objects.requireNonNull(digestType, "digestType");
        Objects.requireNonNull(configurationDigest, "configurationDigest");
        if (configurationDigest.isZero()) {
            throw new IllegalArgumentException("configuration digest must be non-zero");
        }
    }

    public static RunLedgerConfigurationV1 from(BookKeeperCapabilitySnapshotV1 capabilitySnapshot, StorageRunId runId) {
        Objects.requireNonNull(capabilitySnapshot, "capabilitySnapshot");
        return new RunLedgerConfigurationV1(
                capabilitySnapshot.providerScopeId(),
                runId,
                capabilitySnapshot.ensembleSize(),
                capabilitySnapshot.writeQuorumSize(),
                capabilitySnapshot.ackQuorumSize(),
                capabilitySnapshot.digestType(),
                capabilitySnapshot.configurationDigest());
    }
}
