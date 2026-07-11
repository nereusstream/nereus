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

package com.nereusstream.api;

import java.util.List;
import java.util.Objects;

/** Object ranges selected by the resolver for a logical offset. */
public record ResolveResult(
        StreamId streamId,
        long requestedOffset,
        List<ResolvedObjectRange> ranges,
        long resolvedEndOffset,
        long metadataVersion) {
    public ResolveResult {
        Objects.requireNonNull(streamId, "streamId");
        ranges = List.copyOf(ranges);
        if (requestedOffset < 0 || resolvedEndOffset < requestedOffset || metadataVersion < 0) {
            throw new IllegalArgumentException("invalid resolve offsets or metadata version");
        }
    }
}
