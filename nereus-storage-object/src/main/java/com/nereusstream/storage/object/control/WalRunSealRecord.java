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
import java.nio.charset.StandardCharsets;
import java.util.Objects;

/** Immutable physical-only closure record for one reconciled WalRun. */
public record WalRunSealRecord(
        WalRunReference root,
        LaneSequenceVector terminalSequence,
        String finalCheckpointHeadKey,
        Sha256Digest finalCheckpointHeadSha256,
        long aggregateExtentCount,
        long aggregateCanonicalBodyBytes) {
    public WalRunSealRecord {
        Objects.requireNonNull(root, "root");
        Objects.requireNonNull(terminalSequence, "terminalSequence");
        Objects.requireNonNull(finalCheckpointHeadKey, "finalCheckpointHeadKey");
        Objects.requireNonNull(finalCheckpointHeadSha256, "finalCheckpointHeadSha256");
        int keyLength = finalCheckpointHeadKey.getBytes(StandardCharsets.UTF_8).length;
        if (keyLength == 0
                || keyLength > WalRunReference.MAX_METADATA_KEY_BYTES
                || finalCheckpointHeadKey.indexOf('\0') >= 0) {
            throw new IllegalArgumentException("checkpoint head key must be non-empty bounded UTF-8 without NUL");
        }
        if (finalCheckpointHeadSha256.isZero()) {
            throw new IllegalArgumentException("checkpoint head SHA-256 must be non-zero");
        }
        if (aggregateExtentCount < 0 || aggregateCanonicalBodyBytes < 0) {
            throw new IllegalArgumentException("Seal aggregate completeness facts must be non-negative");
        }
        WalRunControlKeys.requireCheckpointHeadKey(finalCheckpointHeadKey, root.shardId(), root.shardRunEpoch());
    }
}
