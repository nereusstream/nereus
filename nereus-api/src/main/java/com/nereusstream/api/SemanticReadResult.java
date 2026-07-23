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

/** A read result paired with the semantic view and exclusive source coverage consumed. */
public record SemanticReadResult(
        ReadView view,
        ReadResult result,
        long sourceCoverageEndOffset) {
    public SemanticReadResult {
        Objects.requireNonNull(view, "view");
        Objects.requireNonNull(result, "result");
        if (sourceCoverageEndOffset < result.nextOffset()) {
            throw new IllegalArgumentException("source coverage cannot precede the returned cursor");
        }
        if (view == ReadView.COMMITTED && sourceCoverageEndOffset != result.nextOffset()) {
            throw new IllegalArgumentException("COMMITTED source coverage must equal result.nextOffset");
        }
    }

    /** Creates and validates a result against the exact request that produced it. */
    public static SemanticReadResult forRequest(
            ReadRequest request,
            ReadResult result,
            long sourceCoverageEndOffset) {
        Objects.requireNonNull(request, "request");
        Objects.requireNonNull(result, "result");
        if (result.requestedOffset() != request.startOffset()) {
            throw new IllegalArgumentException("result requestedOffset must equal request startOffset");
        }
        validateBatches(request, result);
        return new SemanticReadResult(request.view(), result, sourceCoverageEndOffset);
    }

    private static void validateBatches(ReadRequest request, ReadResult result) {
        List<ReadBatch> batches = result.batches();
        if (batches.isEmpty()) {
            if (result.nextOffset() != request.startOffset()) {
                throw new IllegalArgumentException("empty result must not advance the returned cursor");
            }
            return;
        }
        OffsetRange first = batches.get(0).range();
        if (request.boundaryMode() == ReadBoundaryMode.EXACT_START
                && (first.startOffset() < request.startOffset()
                        || (request.view() == ReadView.COMMITTED
                                && first.startOffset() != request.startOffset()))) {
            throw new IllegalArgumentException("EXACT_START result must begin at the requested offset");
        }
        if (request.boundaryMode() == ReadBoundaryMode.CONTAINING_ENTRY
                && !first.contains(request.startOffset())
                && (request.view() == ReadView.COMMITTED
                        || first.startOffset() < request.startOffset())) {
            throw new IllegalArgumentException("CONTAINING_ENTRY result must contain the requested offset");
        }
        long previousEnd = first.endOffset();
        for (int index = 1; index < batches.size(); index++) {
            OffsetRange current = batches.get(index).range();
            if (request.view() == ReadView.COMMITTED && current.startOffset() != previousEnd) {
                throw new IllegalArgumentException("COMMITTED result batches must be dense");
            }
            if (request.view() == ReadView.TOPIC_COMPACTED && current.startOffset() < previousEnd) {
                throw new IllegalArgumentException("TOPIC_COMPACTED result batches must not overlap");
            }
            previousEnd = current.endOffset();
        }
        if (result.nextOffset() != previousEnd) {
            throw new IllegalArgumentException("result nextOffset must equal the last batch endOffset");
        }
    }
}
