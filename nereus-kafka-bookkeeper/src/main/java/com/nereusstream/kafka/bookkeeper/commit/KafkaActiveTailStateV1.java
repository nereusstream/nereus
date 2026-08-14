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

package com.nereusstream.kafka.bookkeeper.commit;

import java.util.ArrayList;
import java.util.List;
import java.util.Objects;

/** Immutable owner-local active-run locator sequence; K6 replaces its lookup representation with a packed index. */
public record KafkaActiveTailStateV1(
        long startOffset, long endOffsetExclusive, List<KafkaBookKeeperActiveTailLocatorV1> locators) {
    public KafkaActiveTailStateV1 {
        locators = List.copyOf(Objects.requireNonNull(locators, "locators"));
        if (startOffset < 0 || endOffsetExclusive < startOffset) {
            throw new IllegalArgumentException("active-tail range is invalid");
        }
        long next = startOffset;
        for (KafkaBookKeeperActiveTailLocatorV1 locator : locators) {
            if (locator.startOffset() != next) {
                throw new IllegalArgumentException("active-tail locators are not contiguous");
            }
            next = locator.endOffsetExclusive();
        }
        if (next != endOffsetExclusive) {
            throw new IllegalArgumentException("active-tail locator coverage differs from its end");
        }
    }

    public static KafkaActiveTailStateV1 empty(long startOffset) {
        return new KafkaActiveTailStateV1(startOffset, startOffset, List.of());
    }

    public KafkaActiveTailStateV1 append(KafkaBookKeeperActiveTailLocatorV1 locator) {
        Objects.requireNonNull(locator, "locator");
        if (locator.startOffset() != endOffsetExclusive) {
            throw new IllegalArgumentException("active-tail locator does not begin at the current end");
        }
        List<KafkaBookKeeperActiveTailLocatorV1> replacement = new ArrayList<>(locators);
        replacement.add(locator);
        return new KafkaActiveTailStateV1(startOffset, locator.endOffsetExclusive(), replacement);
    }
}
