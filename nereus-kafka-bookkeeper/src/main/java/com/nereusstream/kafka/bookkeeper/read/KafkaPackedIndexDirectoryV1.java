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

import com.nereusstream.kafka.bookkeeper.nbke2.Nbke2ConstantsV1;
import com.nereusstream.kafka.bookkeeper.nbke2.Nbke2IndexDirectoryEntryV1;
import java.util.List;
import java.util.Objects;
import java.util.Optional;

/** Primitive-array sealed-run footer directory supporting coverage-aware floor plus successor. */
public final class KafkaPackedIndexDirectoryV1 {
    private final long[] entryIds;
    private final long[] startOffsets;
    private final long[] coveredThroughOffsets;

    public KafkaPackedIndexDirectoryV1(List<Nbke2IndexDirectoryEntryV1> entries) {
        entries = List.copyOf(Objects.requireNonNull(entries, "entries"));
        if (entries.isEmpty() || entries.size() > Nbke2ConstantsV1.FORMAT_MAX_INDEX_DIRECTORY_COUNT) {
            throw new IllegalArgumentException("range-index directory count is outside the NBKE2 v1 cap");
        }
        entryIds = new long[entries.size()];
        startOffsets = new long[entries.size()];
        coveredThroughOffsets = new long[entries.size()];
        long previousEntry = -1;
        long previousStart = -1;
        long previousCovered = -1;
        for (int index = 0; index < entries.size(); index++) {
            Nbke2IndexDirectoryEntryV1 entry = entries.get(index);
            if (entry.indexBlockEntryId() <= previousEntry
                    || entry.blockStartOffset() <= previousStart
                    || entry.blockStartOffset() < previousCovered
                    || entry.blockCoveredThroughOffset() <= previousCovered) {
                throw new IllegalArgumentException("range-index directory regresses physical or logical order");
            }
            entryIds[index] = entry.indexBlockEntryId();
            startOffsets[index] = entry.blockStartOffset();
            coveredThroughOffsets[index] = entry.blockCoveredThroughOffset();
            previousEntry = entry.indexBlockEntryId();
            previousStart = entry.blockStartOffset();
            previousCovered = entry.blockCoveredThroughOffset();
        }
    }

    public int size() {
        return entryIds.length;
    }

    public long startOffset() {
        return startOffsets[0];
    }

    public long coveredThroughOffset() {
        return coveredThroughOffsets[coveredThroughOffsets.length - 1];
    }

    public Optional<KafkaIndexBlockPointerV1> floorOrSuccessor(long requestedOffset) {
        if (requestedOffset < 0) {
            throw new IllegalArgumentException("requested offset must be non-negative");
        }
        int low = 0;
        int high = startOffsets.length - 1;
        int floor = -1;
        while (low <= high) {
            int middle = (low + high) >>> 1;
            if (startOffsets[middle] <= requestedOffset) {
                floor = middle;
                low = middle + 1;
            } else {
                high = middle - 1;
            }
        }
        if (floor >= 0 && requestedOffset < coveredThroughOffsets[floor]) {
            return Optional.of(pointer(floor));
        }
        int successor = floor + 1;
        return successor < entryIds.length ? Optional.of(pointer(successor)) : Optional.empty();
    }

    public Optional<KafkaIndexBlockPointerV1> successor(int ordinal) {
        if (ordinal < 0 || ordinal >= entryIds.length) {
            throw new IllegalArgumentException("directory ordinal is outside the packed index");
        }
        return ordinal + 1 < entryIds.length ? Optional.of(pointer(ordinal + 1)) : Optional.empty();
    }

    public Optional<KafkaIndexBlockPointerV1> findByEntryId(long indexBlockEntryId) {
        if (indexBlockEntryId < 0) {
            throw new IllegalArgumentException("range-index block entry ID must be non-negative");
        }
        int low = 0;
        int high = entryIds.length - 1;
        while (low <= high) {
            int middle = (low + high) >>> 1;
            long candidate = entryIds[middle];
            if (candidate == indexBlockEntryId) {
                return Optional.of(pointer(middle));
            }
            if (candidate < indexBlockEntryId) {
                low = middle + 1;
            } else {
                high = middle - 1;
            }
        }
        return Optional.empty();
    }

    private KafkaIndexBlockPointerV1 pointer(int ordinal) {
        return new KafkaIndexBlockPointerV1(
                ordinal, entryIds[ordinal], startOffsets[ordinal], coveredThroughOffsets[ordinal]);
    }
}
