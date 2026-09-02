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

import com.nereusstream.storage.object.read.BindingReadRouteV1.FailureClass;
import com.nereusstream.storage.object.read.BindingReadRouteV1.SourcePurity;
import java.util.Objects;

/** One exact virtual-ledger range; Pulsar positions are never flattened into Kafka offsets. */
public record PulsarBindingReadRouteV1(
        long virtualLedgerId,
        long startEntryIdInclusive,
        long endEntryIdExclusive,
        BindingReadSourceRefV1 primary,
        BindingReadSourceRefV1 fallback,
        int fallbackFailureMask,
        SourcePurity purity) {
    public PulsarBindingReadRouteV1 {
        Objects.requireNonNull(primary, "primary");
        Objects.requireNonNull(purity, "purity");
        if (virtualLedgerId <= 0 || startEntryIdInclusive < 0 || endEntryIdExclusive <= startEntryIdInclusive) {
            throw new IllegalArgumentException("Pulsar route position is outside its typed ledger/entry domain");
        }
        if (purity != SourcePurity.PULSAR_ENTRY && purity != SourcePurity.PULSAR_WHOLE_REQUEST) {
            throw new IllegalArgumentException("Pulsar route has a non-Pulsar source-purity unit");
        }
        int knownMask = FailureClass.MISSING.mask()
                | FailureClass.UNAVAILABLE.mask()
                | FailureClass.CORRUPT_OR_FORMAT.mask()
                | FailureClass.NOT_ELIGIBLE.mask();
        if ((fallbackFailureMask & ~knownMask) != 0
                || fallback == null && fallbackFailureMask != 0
                || fallback != null && fallbackFailureMask == 0
                || fallback != null && !primary.semanticallyEquivalentTo(fallback)
                || (fallbackFailureMask & FailureClass.NOT_ELIGIBLE.mask()) != 0) {
            throw new IllegalArgumentException("Pulsar fallback identity or closed failure mask is invalid");
        }
    }

    public boolean allowsFallback(FailureClass failure) {
        return fallback != null
                && (fallbackFailureMask
                                & Objects.requireNonNull(failure, "failure").mask())
                        != 0;
    }
}
