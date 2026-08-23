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

/** Runtime activation obtained only from a complete selection receipt at the exact running source. */
public final class AllocatorActivationV1 {
    private final AllocatorModeV1 selectedMode;
    private final int allocatorProtocolVersion;
    private final long selectedRangeSize;
    private final Sha256Digest evidenceReceiptSha256;
    private final String exactSourceCommit;

    private AllocatorActivationV1(
            AllocatorModeV1 selectedMode,
            int allocatorProtocolVersion,
            long selectedRangeSize,
            Sha256Digest evidenceReceiptSha256,
            String exactSourceCommit) {
        this.selectedMode = Objects.requireNonNull(selectedMode, "selectedMode");
        this.allocatorProtocolVersion = allocatorProtocolVersion;
        this.selectedRangeSize = selectedRangeSize;
        this.evidenceReceiptSha256 = Objects.requireNonNull(evidenceReceiptSha256, "evidenceReceiptSha256");
        this.exactSourceCommit = Objects.requireNonNull(exactSourceCommit, "exactSourceCommit");
    }

    static AllocatorActivationV1 fromReceipt(AllocatorSelectionReceiptV1 receipt, String exactRunningSourceCommit) {
        Objects.requireNonNull(receipt, "receipt");
        if (!receipt.nereusSourceCommit().equals(exactRunningSourceCommit)) {
            throw new AllocatorProtocolException(
                    AllocatorProtocolException.Code.SOURCE_MISMATCH,
                    "allocator selection receipt does not bind the exact running Nereus source");
        }
        return new AllocatorActivationV1(
                receipt.selectedMode(),
                receipt.allocatorProtocolVersion(),
                receipt.selectedRangeSize(),
                receipt.evidenceReceiptSha256(),
                exactRunningSourceCommit);
    }

    public AllocatorModeV1 selectedMode() {
        return selectedMode;
    }

    public int allocatorProtocolVersion() {
        return allocatorProtocolVersion;
    }

    public long selectedRangeSize() {
        return selectedRangeSize;
    }

    public Sha256Digest evidenceReceiptSha256() {
        return evidenceReceiptSha256;
    }

    public String exactSourceCommit() {
        return exactSourceCommit;
    }
}
