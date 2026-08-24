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
import com.nereusstream.domain.protocol.ProtocolKindV1;
import java.nio.charset.StandardCharsets;
import java.util.Objects;

/** Protocol-neutral exact terminal checkpoint Head identity bound by a successor Root. */
public record TerminalProtocolCheckpointBindingV1(
        ProtocolKindV1 protocolKind, String terminalHeadKey, Sha256Digest terminalHeadValueSha256) {
    public TerminalProtocolCheckpointBindingV1 {
        Objects.requireNonNull(protocolKind, "protocolKind");
        Objects.requireNonNull(terminalHeadKey, "terminalHeadKey");
        Objects.requireNonNull(terminalHeadValueSha256, "terminalHeadValueSha256");
        int keyBytes = terminalHeadKey.getBytes(StandardCharsets.UTF_8).length;
        if (keyBytes == 0 || keyBytes > WalRunReference.MAX_METADATA_KEY_BYTES || terminalHeadKey.indexOf('\0') >= 0) {
            throw new IllegalArgumentException("terminal protocol Head key must be non-empty bounded UTF-8");
        }
        if (terminalHeadValueSha256.isZero()) {
            throw new IllegalArgumentException("terminal protocol Head SHA-256 must be non-zero");
        }
    }
}
