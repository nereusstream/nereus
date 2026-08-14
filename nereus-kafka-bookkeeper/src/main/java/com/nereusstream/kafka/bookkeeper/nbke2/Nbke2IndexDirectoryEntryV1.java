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

package com.nereusstream.kafka.bookkeeper.nbke2;

/** Fixed 24-byte footer directory row for one authenticated range-index block. */
public record Nbke2IndexDirectoryEntryV1(
        long indexBlockEntryId, long blockStartOffset, long blockCoveredThroughOffset) {
    public Nbke2IndexDirectoryEntryV1 {
        if (indexBlockEntryId < 0 || blockStartOffset < 0 || blockCoveredThroughOffset <= blockStartOffset) {
            throw new IllegalArgumentException("index-directory entry is outside the NBKE2 v1 domain");
        }
    }
}
