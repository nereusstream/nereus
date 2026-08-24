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

import java.util.ArrayList;
import java.util.List;

/** Implemented segmented C2 candidate. It is deliberately not promotion-eligible without an independent receipt. */
public final class SegmentedObjectLayoutC2 {
    public boolean promotionEligible() {
        return false;
    }

    public List<Segment> plan(long bodyLength, long maximumSegmentBytes) {
        if (bodyLength <= 0 || maximumSegmentBytes <= 0) {
            throw new IllegalArgumentException("body and segment caps must be positive");
        }
        ArrayList<Segment> segments = new ArrayList<>();
        long offset = 0;
        int ordinal = 0;
        while (offset < bodyLength) {
            long length = Math.min(maximumSegmentBytes, Math.subtractExact(bodyLength, offset));
            segments.add(new Segment(ordinal, offset, length));
            offset = Math.addExact(offset, length);
            ordinal = Math.incrementExact(ordinal);
        }
        return List.copyOf(segments);
    }

    public record Segment(int ordinal, long offset, long length) {
        public Segment {
            if (ordinal < 0 || offset < 0 || length <= 0) {
                throw new IllegalArgumentException("segment is invalid");
            }
        }
    }
}
