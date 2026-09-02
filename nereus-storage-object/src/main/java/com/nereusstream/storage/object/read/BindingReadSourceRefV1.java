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

package com.nereusstream.storage.object.read;

import com.nereusstream.domain.bytes.Sha256Digest;
import java.util.Objects;

/** Exact immutable physical source identity captured by one read-view generation. */
public record BindingReadSourceRefV1(
        SourceKind kind,
        Sha256Digest sourceIdentity,
        Sha256Digest sourceVersion,
        Sha256Digest semanticIdentity,
        long protectionGeneration) {
    public enum SourceKind {
        BOOKKEEPER,
        OBJECT
    }

    public BindingReadSourceRefV1 {
        Objects.requireNonNull(kind, "kind");
        Objects.requireNonNull(sourceIdentity, "sourceIdentity");
        Objects.requireNonNull(sourceVersion, "sourceVersion");
        Objects.requireNonNull(semanticIdentity, "semanticIdentity");
        if (sourceIdentity.isZero()
                || sourceVersion.isZero()
                || semanticIdentity.isZero()
                || protectionGeneration < 0) {
            throw new IllegalArgumentException("read source identity is outside its exact domain");
        }
    }

    public boolean semanticallyEquivalentTo(BindingReadSourceRefV1 other) {
        return other != null && semanticIdentity.equals(other.semanticIdentity);
    }
}
