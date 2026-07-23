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

package com.nereusstream.api;

import java.util.Objects;

/** Public request for an exact semantic read view and entry-boundary policy. */
public record ReadRequest(
        long startOffset,
        ReadView view,
        ReadBoundaryMode boundaryMode,
        FirstEntryPolicy firstEntryPolicy,
        ReadOptions options) {
    public ReadRequest {
        Objects.requireNonNull(view, "view");
        Objects.requireNonNull(boundaryMode, "boundaryMode");
        Objects.requireNonNull(firstEntryPolicy, "firstEntryPolicy");
        Objects.requireNonNull(options, "options");
        if (startOffset < 0) {
            throw new IllegalArgumentException("startOffset must be non-negative");
        }
    }

    /** Returns true only for the behavior represented by the original {@link StreamStorage#read} method. */
    public boolean isLegacyEquivalent() {
        return view == ReadView.COMMITTED
                && boundaryMode == ReadBoundaryMode.EXACT_START
                && firstEntryPolicy == FirstEntryPolicy.LEGACY_STRICT_LIMIT;
    }
}
