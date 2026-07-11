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

/** Payload batches returned by a read call. */
public record ReadResult(
        StreamId streamId,
        long requestedOffset,
        long nextOffset,
        List<ReadBatch> batches,
        boolean endOfStream) {
    public ReadResult {
        Objects.requireNonNull(streamId, "streamId");
        batches = List.copyOf(batches);
        if (requestedOffset < 0 || nextOffset < requestedOffset) {
            throw new IllegalArgumentException("invalid read offsets");
        }
    }
}
