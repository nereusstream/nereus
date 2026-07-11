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

import com.nereusstream.api.ApiLimits;
import com.nereusstream.api.MetadataCanonicalizer;
import com.nereusstream.objectstore.RangeChecks;
import java.util.Map;

public record EntryIndexItem(
        int entryOrdinal,
        long relativeBaseOffset,
        int recordCount,
        long payloadOffset,
        long payloadLength,
        long eventTimeMillis,
        Map<String, String> attributes) {
    public EntryIndexItem {
        attributes = MetadataCanonicalizer.canonicalStringMap(
                attributes,
                ApiLimits.MAX_ENTRY_ATTRIBUTES_ENCODED_BYTES,
                "attributes");
        RangeChecks.requireNonNegativeNonOverflowingRange(payloadOffset, payloadLength, "entry payload");
        if (entryOrdinal < 0 || relativeBaseOffset < 0 || recordCount <= 0 || eventTimeMillis < 0) {
            throw new IllegalArgumentException("entry index item numeric fields are invalid");
        }
    }
}
