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

package com.nereusstream.storage.object.provider;

import com.nereusstream.domain.bytes.CanonicalBytes;
import java.util.Objects;
import java.util.Optional;

/** Exact provider mutation result with optional immutable version token. */
public record ProviderObjectResult(ProviderObjectOutcome outcome, Optional<CanonicalBytes> versionToken) {
    public ProviderObjectResult {
        Objects.requireNonNull(outcome, "outcome");
        Objects.requireNonNull(versionToken, "versionToken");
        versionToken = versionToken.map(value -> CanonicalBytes.copyOf(value.toByteArray()));
        if (versionToken.isPresent()
                && outcome != ProviderObjectOutcome.APPLIED_EXACT
                && outcome != ProviderObjectOutcome.EXISTING_EXACT) {
            throw new IllegalArgumentException("only exact success may carry a version token");
        }
    }

    public static ProviderObjectResult outcome(ProviderObjectOutcome outcome) {
        return new ProviderObjectResult(outcome, Optional.empty());
    }
}
