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

package io.nereus.api;

import java.util.Objects;

/** Half-open stream offset range: [startOffset, endOffset). */
public record OffsetRange(long startOffset, long endOffset) {
    public OffsetRange {
        if (startOffset < 0) {
            throw new IllegalArgumentException("startOffset must be non-negative");
        }
        if (endOffset < startOffset) {
            throw new IllegalArgumentException("endOffset must be >= startOffset");
        }
    }

    public long recordCount() {
        return endOffset - startOffset;
    }

    public boolean isEmpty() {
        return startOffset == endOffset;
    }

    public boolean contains(long offset) {
        return offset >= startOffset && offset < endOffset;
    }

    public boolean overlaps(OffsetRange other) {
        Objects.requireNonNull(other, "other");
        return startOffset < other.endOffset() && other.startOffset() < endOffset;
    }
}
