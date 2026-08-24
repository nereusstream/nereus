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

package com.nereusstream.storage.object.kms;

/** Run-scoped KMS cache identity. Shard epochs are immutable and never reused. */
public record RunKeyCacheIdentity(int shardId, long shardRunEpoch) {
    public RunKeyCacheIdentity {
        if (shardId < 0 || shardRunEpoch < 0) {
            throw new IllegalArgumentException("run-key cache identity must be non-negative");
        }
    }
}
