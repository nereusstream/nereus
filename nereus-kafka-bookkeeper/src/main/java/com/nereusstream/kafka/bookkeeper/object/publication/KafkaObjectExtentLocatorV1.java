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

package com.nereusstream.kafka.bookkeeper.object.publication;

import java.util.Objects;

/** Compact complete-commit-set locator installed before Readable/Durable publication. */
public record KafkaObjectExtentLocatorV1(
        KafkaObjectBindingKeyV1 binding,
        long startOffset,
        long endOffsetExclusive,
        KafkaObjectExtentIdentityV1 extent,
        int firstDirectoryRow,
        int directoryRowCount) {
    public KafkaObjectExtentLocatorV1 {
        Objects.requireNonNull(binding, "binding");
        Objects.requireNonNull(extent, "extent");
        if (startOffset < 0
                || endOffsetExclusive <= startOffset
                || firstDirectoryRow < 0
                || directoryRowCount <= 0
                || Math.addExact(firstDirectoryRow, directoryRowCount) > 65_536) {
            throw new IllegalArgumentException("Kafka Object locator is outside its compact range/span domain");
        }
    }
}
