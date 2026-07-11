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

package com.nereusstream.metadata.oxia;

import com.nereusstream.api.StreamId;
import java.util.Objects;
import java.util.Optional;

/** Result of one scan-bounded page of generation-zero derived-index repair. */
public record DerivedIndexRepairResult(
        StreamId streamId,
        long repairedFromOffset,
        long repairedToOffset,
        int scannedRecords,
        int repairedRecords,
        boolean targetCovered,
        boolean repairBudgetExhausted,
        Optional<DerivedIndexRepairCursor> continuation,
        long observedCommitVersion) {
    public DerivedIndexRepairResult {
        Objects.requireNonNull(streamId, "streamId");
        continuation = Objects.requireNonNull(continuation, "continuation");
        if (repairedFromOffset < 0 || repairedToOffset < 0 || repairedToOffset < repairedFromOffset
                || scannedRecords < 0 || repairedRecords < 0 || repairedRecords > scannedRecords
                || observedCommitVersion < 0) {
            throw new IllegalArgumentException("repair result numeric fields must be non-negative and ordered");
        }
        if (repairBudgetExhausted != continuation.isPresent()) {
            throw new IllegalArgumentException("budget exhaustion and continuation presence must match");
        }
        if (targetCovered && continuation.isPresent()) {
            throw new IllegalArgumentException("target-covered repair cannot carry a continuation");
        }
        if (!targetCovered && !repairBudgetExhausted) {
            throw new IllegalArgumentException("repair must cover the target or return an exhausted continuation");
        }
        if (continuation.isPresent()) {
            DerivedIndexRepairCursor cursor = continuation.orElseThrow();
            if (!cursor.streamId().equals(streamId)
                    || cursor.observedCommitVersion() != observedCommitVersion) {
                throw new IllegalArgumentException("repair continuation must match result stream and head version");
            }
        }
    }
}
