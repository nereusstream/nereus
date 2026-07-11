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

package com.nereusstream.objectstore.wal;

import java.util.List;
import java.util.Objects;

public record EntryIndex(
        int entryCount,
        int recordCount,
        List<EntryIndexItem> entries) {
    public EntryIndex {
        entries = List.copyOf(Objects.requireNonNull(entries, "entries"));
        if (entryCount != entries.size() || entryCount <= 0 || recordCount <= 0) {
            throw new IllegalArgumentException("entry index counts are invalid");
        }
        long expectedBaseOffset = 0;
        long sumRecords = 0;
        for (int i = 0; i < entries.size(); i++) {
            EntryIndexItem item = entries.get(i);
            if (item.entryOrdinal() != i) {
                throw new IllegalArgumentException("entry ordinals must be zero-based and contiguous");
            }
            if (item.relativeBaseOffset() != expectedBaseOffset) {
                throw new IllegalArgumentException("entry relative offsets must be contiguous");
            }
            expectedBaseOffset = Math.addExact(expectedBaseOffset, item.recordCount());
            sumRecords = Math.addExact(sumRecords, item.recordCount());
        }
        if (sumRecords != recordCount) {
            throw new IllegalArgumentException("recordCount must equal sum of entry record counts");
        }
    }
}
