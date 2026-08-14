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

package com.nereusstream.kafka.bookkeeper.pipeline;

/** Atomic capacity counters used for deterministic admission and leak checks. */
public record KafkaAppendCapacitySnapshotV1(long groups, long entries, long bytes) {
    public KafkaAppendCapacitySnapshotV1 {
        if (groups < 0 || entries < 0 || bytes < 0) {
            throw new IllegalArgumentException("pipeline capacity counters must be non-negative");
        }
    }
}
