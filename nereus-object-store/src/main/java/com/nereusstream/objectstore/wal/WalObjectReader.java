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

import com.nereusstream.api.ReadBatch;
import com.nereusstream.api.ErrorCode;
import com.nereusstream.api.FirstEntryPolicy;
import com.nereusstream.api.NereusException;
import com.nereusstream.api.ReadBoundaryMode;
import com.nereusstream.api.ReadOptions;
import com.nereusstream.api.ReadRequest;
import com.nereusstream.api.ResolvedObjectRange;
import java.util.List;
import java.util.concurrent.CompletableFuture;

public interface WalObjectReader {
    /**
     * Reads resolved ranges sequentially in list order and reports exact accounting for every slice that was
     * actually downloaded and verified.
     */
    CompletableFuture<WalReadResult> readWithStats(
            long startOffset,
            List<ResolvedObjectRange> ranges,
            ReadOptions options);

    /** Request-aware read; old implementations fail closed for new boundary or limit semantics. */
    default CompletableFuture<WalReadResult> readWithStats(
            ReadRequest request,
            List<ResolvedObjectRange> ranges) {
        if (request.boundaryMode() != ReadBoundaryMode.EXACT_START
                || request.firstEntryPolicy() != FirstEntryPolicy.LEGACY_STRICT_LIMIT) {
            return NereusException.failedFuture(
                    ErrorCode.UNSUPPORTED_READ_SEMANTICS,
                    false,
                    "WAL reader does not support ranged-entry semantics");
        }
        return readWithStats(request.startOffset(), ranges, request.options());
    }

    default CompletableFuture<List<ReadBatch>> read(
            long startOffset,
            List<ResolvedObjectRange> ranges,
            ReadOptions options) {
        return readWithStats(startOffset, ranges, options).thenApply(WalReadResult::batches);
    }
}
