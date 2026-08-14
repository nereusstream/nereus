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

import com.nereusstream.kafka.bookkeeper.nbke2.Nbke2RunBindingV1;
import com.nereusstream.storage.api.bookkeeper.RunLedgerHandleV1;
import java.util.Objects;
import java.util.Optional;

/** One coarse read-run descriptor with either an owner-local active index or a sealed footer directory. */
public record KafkaBookKeeperReadRunV1(
        Nbke2RunBindingV1 runBinding,
        RunLedgerHandleV1 handle,
        long startOffset,
        long endOffsetExclusive,
        long sourceGeneration,
        Optional<KafkaPackedBatchLocatorIndexV1> activeIndex,
        Optional<KafkaPackedIndexDirectoryV1> sealedDirectory) {
    public KafkaBookKeeperReadRunV1 {
        Objects.requireNonNull(runBinding, "runBinding");
        Objects.requireNonNull(handle, "handle");
        activeIndex = Objects.requireNonNull(activeIndex, "activeIndex");
        sealedDirectory = Objects.requireNonNull(sealedDirectory, "sealedDirectory");
        if (startOffset < 0
                || endOffsetExclusive <= startOffset
                || sourceGeneration < 0
                || activeIndex.isPresent() == sealedDirectory.isPresent()
                || !runBinding.providerScopeId().equals(handle.providerScopeId())
                || !runBinding.runId().equals(handle.runId())) {
            throw new IllegalArgumentException("read run identity/range/source kind is invalid");
        }
        if (activeIndex.isPresent()) {
            KafkaPackedBatchLocatorIndexV1 index = activeIndex.orElseThrow();
            if (index.startOffset() != startOffset || index.coveredThroughOffset() != endOffsetExclusive) {
                throw new IllegalArgumentException("active packed index differs from its run coverage");
            }
        } else {
            KafkaPackedIndexDirectoryV1 directory = sealedDirectory.orElseThrow();
            if (directory.startOffset() < startOffset || directory.coveredThroughOffset() > endOffsetExclusive) {
                throw new IllegalArgumentException("sealed index directory escapes its run coverage");
            }
        }
    }

    public boolean active() {
        return activeIndex.isPresent();
    }
}
