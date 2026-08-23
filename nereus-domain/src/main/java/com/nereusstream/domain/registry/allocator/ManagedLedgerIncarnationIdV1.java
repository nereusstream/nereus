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

/** Immutable ManagedLedger incarnation identity; broker owner epochs are deliberately separate. */
public record ManagedLedgerIncarnationIdV1(Sha256Digest value) {
    public ManagedLedgerIncarnationIdV1 {
        Objects.requireNonNull(value, "value");
        if (value.isZero()) {
            throw new IllegalArgumentException("ManagedLedger incarnation identity must be non-zero");
        }
    }
}
