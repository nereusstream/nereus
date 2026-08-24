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
import java.util.Comparator;
import java.util.List;
import java.util.Objects;
import java.util.Optional;

/** One immutable page in the single run-wide physical checkpoint chain. */
public record WalRunCheckpointPageV1(
        Sha256Digest rootSha256,
        long pageOrdinal,
        Optional<Sha256Digest> predecessorPageSha256,
        List<ProviderResolvedExtentRowV1> extents,
        LaneSequenceVector coveredThrough) {
    private static final Comparator<ProviderResolvedExtentRowV1> ROW_ORDER = Comparator.comparingInt(
                    (ProviderResolvedExtentRowV1 value) -> value.laneId().code())
            .thenComparingLong(ProviderResolvedExtentRowV1::laneSequence);

    public WalRunCheckpointPageV1 {
        Objects.requireNonNull(rootSha256, "rootSha256");
        Objects.requireNonNull(predecessorPageSha256, "predecessorPageSha256");
        Objects.requireNonNull(extents, "extents");
        Objects.requireNonNull(coveredThrough, "coveredThrough");
        extents = List.copyOf(extents);
        if (rootSha256.isZero() || pageOrdinal < 0 || extents.isEmpty()) {
            throw new IllegalArgumentException("checkpoint page identity and rows must be present");
        }
        if (extents.size() > WalCheckpointPolicy.FORMAT_MAX_ROWS_PER_PAGE) {
            throw new IllegalArgumentException("checkpoint page row count exceeds the format cap");
        }
        if (pageOrdinal == 0 != predecessorPageSha256.isEmpty()) {
            throw new IllegalArgumentException("only page zero may omit the predecessor digest");
        }
        for (int index = 1; index < extents.size(); index++) {
            if (ROW_ORDER.compare(extents.get(index - 1), extents.get(index)) >= 0) {
                throw new IllegalArgumentException("checkpoint rows must be unique and ordered by lane/sequence");
            }
        }
        for (ProviderResolvedExtentRowV1 extent : extents) {
            if (coveredThrough.get(extent.laneId()) < extent.laneSequence()) {
                throw new IllegalArgumentException("covered-through vector does not include a page row");
            }
        }
    }
}
