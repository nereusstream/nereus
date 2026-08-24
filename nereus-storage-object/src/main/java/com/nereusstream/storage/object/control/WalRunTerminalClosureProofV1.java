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

package com.nereusstream.storage.object.control;

import com.nereusstream.domain.bytes.Sha256Digest;
import java.util.Objects;

/** Unforgeable outside this package; issued only after stopped runtime and exact physical Seal closure verification. */
public final class WalRunTerminalClosureProofV1 {
    private final WalRunReference root;
    private final String sealKey;
    private final Sha256Digest sealSha256;
    private final LaneSequenceVector terminalSequence;
    private final String finalPhysicalCheckpointHeadKey;
    private final Sha256Digest finalPhysicalCheckpointHeadSha256;
    private final long aggregateExtentCount;
    private final long aggregateCanonicalBodyBytes;

    WalRunTerminalClosureProofV1(String sealKey, WalRunSealRecord seal) {
        this.root = Objects.requireNonNull(seal, "seal").root();
        this.sealKey = Objects.requireNonNull(sealKey, "sealKey");
        WalRunControlKeys.requireSealKey(sealKey, root.shardId(), root.shardRunEpoch());
        this.sealSha256 = WalRunControlCodec.sealSha256(seal);
        this.terminalSequence = seal.terminalSequence();
        this.finalPhysicalCheckpointHeadKey = seal.finalCheckpointHeadKey();
        this.finalPhysicalCheckpointHeadSha256 = seal.finalCheckpointHeadSha256();
        this.aggregateExtentCount = seal.aggregateExtentCount();
        this.aggregateCanonicalBodyBytes = seal.aggregateCanonicalBodyBytes();
    }

    public WalRunReference root() {
        return root;
    }

    public String sealKey() {
        return sealKey;
    }

    public Sha256Digest sealSha256() {
        return sealSha256;
    }

    public LaneSequenceVector terminalSequence() {
        return terminalSequence;
    }

    public String finalPhysicalCheckpointHeadKey() {
        return finalPhysicalCheckpointHeadKey;
    }

    public Sha256Digest finalPhysicalCheckpointHeadSha256() {
        return finalPhysicalCheckpointHeadSha256;
    }

    public long aggregateExtentCount() {
        return aggregateExtentCount;
    }

    public long aggregateCanonicalBodyBytes() {
        return aggregateCanonicalBodyBytes;
    }
}
