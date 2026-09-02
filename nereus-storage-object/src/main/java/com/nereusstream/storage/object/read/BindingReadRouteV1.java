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

import java.util.Objects;

/** One already-authorized protocol-position interval in a generation-tagged source map. */
public record BindingReadRouteV1(
        long startInclusive,
        long endExclusive,
        BindingReadSourceRefV1 primary,
        BindingReadSourceRefV1 fallback,
        int fallbackFailureMask,
        SourcePurity purity) {
    public enum SourcePurity {
        KAFKA_APPEND_UNIT,
        PULSAR_ENTRY,
        PULSAR_WHOLE_REQUEST
    }

    public enum FailureClass {
        MISSING(1),
        UNAVAILABLE(1 << 1),
        CORRUPT_OR_FORMAT(1 << 2),
        NOT_ELIGIBLE(1 << 3);

        private final int mask;

        FailureClass(int mask) {
            this.mask = mask;
        }

        public int mask() {
            return mask;
        }
    }

    public BindingReadRouteV1 {
        Objects.requireNonNull(primary, "primary");
        Objects.requireNonNull(purity, "purity");
        if (startInclusive < 0 || endExclusive <= startInclusive) {
            throw new IllegalArgumentException("read route interval is empty or outside the position domain");
        }
        int knownMask = FailureClass.MISSING.mask
                | FailureClass.UNAVAILABLE.mask
                | FailureClass.CORRUPT_OR_FORMAT.mask
                | FailureClass.NOT_ELIGIBLE.mask;
        if ((fallbackFailureMask & ~knownMask) != 0
                || fallback == null && fallbackFailureMask != 0
                || fallback != null && fallbackFailureMask == 0
                || fallback != null && !primary.semanticallyEquivalentTo(fallback)
                || (fallbackFailureMask & FailureClass.NOT_ELIGIBLE.mask) != 0) {
            throw new IllegalArgumentException("fallback identity or closed failure mask is invalid");
        }
    }

    public boolean allowsFallback(FailureClass failure) {
        return fallback != null && (fallbackFailureMask & Objects.requireNonNull(failure, "failure").mask) != 0;
    }
}
