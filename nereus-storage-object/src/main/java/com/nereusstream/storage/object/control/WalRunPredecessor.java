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
import java.util.Optional;

/** Exact predecessor Root and Seal identities stored in a successor Root. */
public record WalRunPredecessor(
        WalRunReference root,
        String sealKey,
        Sha256Digest sealSha256,
        Optional<TerminalProtocolCheckpointBindingV1> terminalProtocolCheckpoint) {
    public WalRunPredecessor {
        Objects.requireNonNull(root, "root");
        Objects.requireNonNull(sealKey, "sealKey");
        Objects.requireNonNull(sealSha256, "sealSha256");
        Objects.requireNonNull(terminalProtocolCheckpoint, "terminalProtocolCheckpoint");
        int keyLength = sealKey.getBytes(StandardCharsets.UTF_8).length;
        if (keyLength == 0 || keyLength > WalRunReference.MAX_METADATA_KEY_BYTES || sealKey.indexOf('\0') >= 0) {
            throw new IllegalArgumentException("sealKey must be non-empty bounded UTF-8 without NUL");
        }
        if (sealSha256.isZero()) {
            throw new IllegalArgumentException("Seal SHA-256 must be non-zero");
        }
        WalRunControlKeys.requireSealKey(sealKey, root.shardId(), root.shardRunEpoch());
    }
}
