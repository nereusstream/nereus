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

import java.util.List;
import java.util.Objects;

/** Terminal logical/physical bounds and ordered index directory for one sealed run. */
public record Nbke2RunFooterV1(
        Nbke2RunBindingV1 runBinding,
        long kafkaEndOffsetExclusive,
        long lastPhysicalEntryIdExclusive,
        long latestIndexBlockEntryId,
        long protocolCheckpointEntryId,
        long sealOwnerEpoch,
        List<Nbke2IndexDirectoryEntryV1> indexDirectory)
        implements Nbke2FrameV1 {
    public Nbke2RunFooterV1 {
        Objects.requireNonNull(runBinding, "runBinding");
        Objects.requireNonNull(indexDirectory, "indexDirectory");
        indexDirectory = List.copyOf(indexDirectory);
        if (kafkaEndOffsetExclusive < 0
                || lastPhysicalEntryIdExclusive <= 0
                || latestIndexBlockEntryId < -1
                || protocolCheckpointEntryId < -1
                || sealOwnerEpoch <= 0) {
            throw new IllegalArgumentException("footer bounds/fence are outside the NBKE2 v1 domain");
        }
        if (indexDirectory.size() > Nbke2ConstantsV1.FORMAT_MAX_INDEX_DIRECTORY_COUNT) {
            throw new IllegalArgumentException("footer index-directory count exceeds the persisted v1 cap");
        }
        if (indexDirectory.isEmpty() != (latestIndexBlockEntryId == -1)) {
            throw new IllegalArgumentException("latest index-block identity and directory presence disagree");
        }
        long previousEntry = -1;
        long previousEnd = -1;
        for (Nbke2IndexDirectoryEntryV1 entry : indexDirectory) {
            if (entry.indexBlockEntryId() <= previousEntry
                    || previousEnd >= 0 && entry.blockStartOffset() != previousEnd) {
                throw new IllegalArgumentException("footer directory is unordered or has a logical gap");
            }
            previousEntry = entry.indexBlockEntryId();
            previousEnd = entry.blockCoveredThroughOffset();
        }
        if (!indexDirectory.isEmpty()
                && (latestIndexBlockEntryId != previousEntry || kafkaEndOffsetExclusive != previousEnd)) {
            throw new IllegalArgumentException("footer terminal bounds do not match the directory");
        }
    }

    @Override
    public Nbke2FrameTypeV1 frameType() {
        return Nbke2FrameTypeV1.RUN_FOOTER;
    }
}
