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

package com.nereusstream.core.read;

import com.nereusstream.api.StreamId;
import com.nereusstream.core.recovery.GenerationZeroRepairScanner;
import java.time.Duration;
import java.util.Objects;
import java.util.concurrent.CompletableFuture;

/** Protected live-tail repair used when a WAL_DURABLE append is visible in the head before its gen-0 index. */
public final class ReadAfterStableCommitRepair implements GenerationIndexRepairer {
    private final GenerationZeroRepairScanner scanner;

    public ReadAfterStableCommitRepair(GenerationZeroRepairScanner scanner) {
        this.scanner = Objects.requireNonNull(scanner, "scanner");
    }

    @Override
    public CompletableFuture<GenerationIndexRepairResult> repair(
            StreamId streamId, long targetOffset, Duration timeout) {
        return scanner.repairCovering(streamId, targetOffset, timeout)
                .thenApply(result -> result.trimmed()
                        ? GenerationIndexRepairResult.trimmed(
                                result.streamId(), result.targetOffset())
                        : GenerationIndexRepairResult.live(
                                result.streamId(),
                                result.targetOffset(),
                                result.scannedCommits()));
    }
}
