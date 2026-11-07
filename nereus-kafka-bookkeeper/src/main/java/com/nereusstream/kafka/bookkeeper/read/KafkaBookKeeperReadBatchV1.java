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

package com.nereusstream.kafka.bookkeeper.read;

import com.nereusstream.domain.bytes.CanonicalBytes;
import java.util.Objects;
import java.util.OptionalLong;

/** One validated complete RecordBatch. An Object part has no BookKeeper entry ID. */
public record KafkaBookKeeperReadBatchV1(
        long startOffset,
        long endOffsetExclusive,
        OptionalLong bookKeeperEntryId,
        CanonicalBytes rawAssignedRecordBatch) {
    public KafkaBookKeeperReadBatchV1(
            long startOffset, long endOffsetExclusive, long entryId, CanonicalBytes rawAssignedRecordBatch) {
        this(startOffset, endOffsetExclusive, OptionalLong.of(entryId), rawAssignedRecordBatch);
    }

    public KafkaBookKeeperReadBatchV1 {
        Objects.requireNonNull(bookKeeperEntryId, "bookKeeperEntryId");
        Objects.requireNonNull(rawAssignedRecordBatch, "rawAssignedRecordBatch");
        if (startOffset < 0
                || endOffsetExclusive <= startOffset
                || bookKeeperEntryId.orElse(0) < 0
                || rawAssignedRecordBatch.isEmpty()) {
            throw new IllegalArgumentException("read batch is outside its domain");
        }
    }

    /** Actual first BookKeeper carrier entry, including KBSC entry zero. */
    public long entryId() {
        return bookKeeperEntryId.orElseThrow();
    }
}
