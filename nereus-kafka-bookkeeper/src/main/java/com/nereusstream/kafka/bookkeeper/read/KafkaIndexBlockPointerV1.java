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

package com.nereusstream.kafka.bookkeeper.read;

/** Ephemeral primitive directory lookup result for one persisted range-index block. */
public record KafkaIndexBlockPointerV1(
        int ordinal, long indexBlockEntryId, long startOffset, long coveredThroughOffset) {
    public KafkaIndexBlockPointerV1 {
        if (ordinal < 0 || indexBlockEntryId < 0 || startOffset < 0 || coveredThroughOffset <= startOffset) {
            throw new IllegalArgumentException("range-index block pointer is outside its domain");
        }
    }

    public boolean covers(long offset) {
        return offset >= startOffset && offset < coveredThroughOffset;
    }
}
