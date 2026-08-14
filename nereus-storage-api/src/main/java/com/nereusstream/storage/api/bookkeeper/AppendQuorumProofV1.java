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

/** Exact ledger/entry/payload identity and quorum evidence for APPLIED_EXACT. */
public record AppendQuorumProofV1(
        RunLedgerHandleV1 handle, long entryId, int payloadBytes, Sha256Digest payloadSha256, int acknowledgedBookies) {
    public AppendQuorumProofV1 {
        Objects.requireNonNull(handle, "handle");
        if (entryId < 0 || payloadBytes <= 0 || acknowledgedBookies <= 0) {
            throw new IllegalArgumentException("entry ID, payload bytes, and acknowledged bookies are invalid");
        }
        Objects.requireNonNull(payloadSha256, "payloadSha256");
        if (payloadSha256.isZero()) {
            throw new IllegalArgumentException("payload digest must be non-zero");
        }
    }
}
