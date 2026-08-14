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

/** One published complete RecordBatch locator within an atomic active-tail append group. */
public record KafkaBookKeeperDataLocatorV1(
        long startOffset, long endOffsetExclusive, long entryId, int memberOrdinal, long rawAssignedRecordBatchBytes) {
    public KafkaBookKeeperDataLocatorV1 {
        if (startOffset < 0
                || endOffsetExclusive <= startOffset
                || entryId <= 0
                || memberOrdinal < 0
                || rawAssignedRecordBatchBytes <= 0) {
            throw new IllegalArgumentException("BookKeeper DATA locator is outside its domain");
        }
    }
}
