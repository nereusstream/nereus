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

import com.nereusstream.domain.bytes.CanonicalBytes;
import java.util.Objects;

/** Compatible Kafka protocol-state vector and its three closed canonical sections. */
public record Nbke2ProtocolCheckpointV1(
        Nbke2RunBindingV1 runBinding,
        long rangeIndexCoveredThrough,
        long producerStateCoveredThrough,
        long transactionIndexCoveredThrough,
        long leaderEpochCoveredThrough,
        CanonicalBytes producerState,
        CanonicalBytes transactionIndex,
        CanonicalBytes leaderEpochIndex)
        implements Nbke2FrameV1 {
    public Nbke2ProtocolCheckpointV1 {
        Objects.requireNonNull(runBinding, "runBinding");
        Objects.requireNonNull(producerState, "producerState");
        Objects.requireNonNull(transactionIndex, "transactionIndex");
        Objects.requireNonNull(leaderEpochIndex, "leaderEpochIndex");
        if (rangeIndexCoveredThrough < 0
                || producerStateCoveredThrough < 0
                || transactionIndexCoveredThrough < 0
                || leaderEpochCoveredThrough < 0) {
            throw new IllegalArgumentException("checkpoint covered-through components must be non-negative");
        }
        for (CanonicalBytes section : new CanonicalBytes[] {producerState, transactionIndex, leaderEpochIndex}) {
            if (section.length() > Nbke2ConstantsV1.FORMAT_MAX_CHECKPOINT_SECTION_BYTES) {
                throw new IllegalArgumentException("checkpoint section exceeds its persisted v1 cap");
            }
        }
    }

    public long recoveryCoveredThrough() {
        return Math.min(
                Math.min(rangeIndexCoveredThrough, producerStateCoveredThrough),
                Math.min(transactionIndexCoveredThrough, leaderEpochCoveredThrough));
    }

    @Override
    public Nbke2FrameTypeV1 frameType() {
        return Nbke2FrameTypeV1.PROTOCOL_CHECKPOINT;
    }
}
