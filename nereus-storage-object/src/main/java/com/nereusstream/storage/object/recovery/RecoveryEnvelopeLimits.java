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

package com.nereusstream.storage.object.recovery;

/** Hard cumulative recovery limits. Concurrent work charges the same envelope and never multiplies it. */
public record RecoveryEnvelopeLimits(
        int maxLiveRoots,
        int maxPredecessorRuns,
        int maxListPages,
        long maxListedKeys,
        long maxListedKeyBytes,
        int maxHeadRequests,
        int maxRangeGetRequests,
        int maxFullGetRequests,
        long maxCanonicalBodyBytes,
        long maxDecodedContexts,
        long maxDecodedFrames,
        long maxDecodedCommitSets,
        long maxWorkingMemoryBytes,
        int maxConcurrency,
        int maxRetryAttempts,
        long maxWallTimeNanos) {
    public RecoveryEnvelopeLimits {
        if (maxLiveRoots <= 0
                || maxPredecessorRuns < 0
                || maxListPages <= 0
                || maxListedKeys <= 0
                || maxListedKeyBytes <= 0
                || maxHeadRequests < 0
                || maxRangeGetRequests <= 0
                || maxFullGetRequests < 0
                || maxCanonicalBodyBytes <= 0
                || maxDecodedContexts <= 0
                || maxDecodedFrames <= 0
                || maxDecodedCommitSets <= 0
                || maxWorkingMemoryBytes <= 0
                || maxConcurrency <= 0
                || maxRetryAttempts < 0
                || maxWallTimeNanos <= 0) {
            throw new IllegalArgumentException("recovery envelope limits are invalid");
        }
    }
}
