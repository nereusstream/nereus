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

import java.util.Objects;

/** Allocation state compared by RANGE reservation; owner epoch is intentionally excluded. */
public record AllocatorHeadStateV1(
        ChainPointerV1 visibleChainHead,
        long priorGrantId,
        long priorRangeStartInclusive,
        long priorRangeEndExclusive,
        long nextLedgerId) {
    public AllocatorHeadStateV1 {
        Objects.requireNonNull(visibleChainHead, "visibleChainHead");
        if (priorGrantId < 0 || nextLedgerId < 0) {
            throw new IllegalArgumentException("allocator head state contains a negative unsigned value");
        }
        if (priorGrantId == 0) {
            if (priorRangeStartInclusive != 0 || priorRangeEndExclusive != 0) {
                throw new IllegalArgumentException("head without a grant must have an empty range");
            }
        } else if (priorRangeStartInclusive <= 0
                || priorRangeEndExclusive <= priorRangeStartInclusive
                || nextLedgerId < priorRangeStartInclusive
                || nextLedgerId > priorRangeEndExclusive) {
            throw new IllegalArgumentException("installed grant range/cursor is invalid");
        }
    }
}
