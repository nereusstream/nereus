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

/** Exact ledger handle identity without leaking a BookKeeper SDK type. */
public record RunLedgerHandleV1(
        CellProviderScopeId providerScopeId,
        StorageRunId runId,
        BookKeeperLedgerIdentity ledgerIdentity,
        Sha256Digest configurationDigest) {
    public RunLedgerHandleV1 {
        Objects.requireNonNull(providerScopeId, "providerScopeId");
        Objects.requireNonNull(runId, "runId");
        Objects.requireNonNull(ledgerIdentity, "ledgerIdentity");
        Objects.requireNonNull(configurationDigest, "configurationDigest");
        if (configurationDigest.isZero()) {
            throw new IllegalArgumentException("configuration digest must be non-zero");
        }
    }
}
