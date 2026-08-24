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

import com.nereusstream.domain.bytes.CanonicalBytes;
import java.util.Objects;

/** Compact closed proof field carried by one physical extent row. */
public record ProviderVersionProof(ProviderProofMode mode, CanonicalBytes canonicalVersionToken) {
    public ProviderVersionProof {
        Objects.requireNonNull(mode, "mode");
        Objects.requireNonNull(canonicalVersionToken, "canonicalVersionToken");
        if (mode == ProviderProofMode.NONE && !canonicalVersionToken.isEmpty()) {
            throw new IllegalArgumentException("NONE proof must carry no version-token bytes");
        }
        if (mode != ProviderProofMode.NONE && canonicalVersionToken.isEmpty()) {
            throw new IllegalArgumentException("version-bound proof must carry a token");
        }
    }

    public static ProviderVersionProof none() {
        return new ProviderVersionProof(ProviderProofMode.NONE, CanonicalBytes.empty());
    }
}
