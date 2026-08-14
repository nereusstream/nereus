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

package com.nereusstream.kafka.bookkeeper.nbke2;

import java.util.List;
import java.util.Objects;

/** Immutable packed, ordered, gap-free locator block. */
public record Nbke2RangeIndexBlockV1(
        Nbke2RunBindingV1 runBinding,
        long anchorOffset,
        long anchorEntryId,
        long coveredThroughOffset,
        long firstDataEntryId,
        long lastDataEntryId,
        long predecessorBlockEntryId,
        long successorDataEntryId,
        List<Nbke2BatchLocatorV1> locators)
        implements Nbke2FrameV1 {
    public Nbke2RangeIndexBlockV1 {
        Objects.requireNonNull(runBinding, "runBinding");
        Objects.requireNonNull(locators, "locators");
        locators = List.copyOf(locators);
        if (anchorOffset < 0
                || anchorEntryId < 0
                || coveredThroughOffset <= anchorOffset
                || firstDataEntryId < 0
                || lastDataEntryId < firstDataEntryId
                || predecessorBlockEntryId < -1
                || successorDataEntryId <= lastDataEntryId) {
            throw new IllegalArgumentException("range-index bounds are outside the NBKE2 v1 domain");
        }
        if (locators.isEmpty() || locators.size() > Nbke2ConstantsV1.FORMAT_MAX_LOCATOR_COUNT) {
            throw new IllegalArgumentException("locator count is outside the NBKE2 v1 domain");
        }
        long expectedOffset = anchorOffset;
        long expectedEntryId = firstDataEntryId;
        long previousGroupDelta = -1;
        for (Nbke2BatchLocatorV1 locator : locators) {
            long offset = Math.addExact(anchorOffset, locator.baseOffsetDelta());
            long entryId = Math.addExact(anchorEntryId, locator.entryIdDelta());
            if (offset != expectedOffset || entryId != expectedEntryId) {
                throw new IllegalArgumentException("locator rows are not gap-free in offset/entry order");
            }
            if (locator.appendGroupDelta() < previousGroupDelta) {
                throw new IllegalArgumentException("locator append-group deltas regress");
            }
            expectedOffset = Math.addExact(offset, locator.logicalOffsetCount());
            expectedEntryId = Math.incrementExact(entryId);
            previousGroupDelta = locator.appendGroupDelta();
        }
        if (expectedOffset != coveredThroughOffset || expectedEntryId - 1 != lastDataEntryId) {
            throw new IllegalArgumentException("locator rows do not match declared terminal bounds");
        }
    }

    @Override
    public Nbke2FrameTypeV1 frameType() {
        return Nbke2FrameTypeV1.RANGE_INDEX_BLOCK;
    }
}
