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

package com.nereusstream.kafka.bookkeeper.checkpoint;

import com.nereusstream.kafka.bookkeeper.nbke2.Nbke2ProtocolCheckpointV1;
import com.nereusstream.kafka.bookkeeper.nbke2.Nbke2RunBindingV1;
import java.util.Objects;

/** Profile-neutral compatible coverage vector bound to one exact physical run identity. */
public record KafkaRecoveryCheckpointVectorV1(
        Nbke2RunBindingV1 runBinding,
        long rangeIndexCoveredThrough,
        long producerStateCoveredThrough,
        long transactionIndexCoveredThrough,
        long leaderEpochCoveredThrough) {
    public KafkaRecoveryCheckpointVectorV1 {
        Objects.requireNonNull(runBinding, "runBinding");
        if (rangeIndexCoveredThrough < 0
                || producerStateCoveredThrough < 0
                || transactionIndexCoveredThrough < 0
                || leaderEpochCoveredThrough < 0) {
            throw new IllegalArgumentException("checkpoint coverage is outside the Kafka offset domain");
        }
    }

    public static KafkaRecoveryCheckpointVectorV1 from(Nbke2ProtocolCheckpointV1 checkpoint) {
        Objects.requireNonNull(checkpoint, "checkpoint");
        return new KafkaRecoveryCheckpointVectorV1(
                checkpoint.runBinding(),
                checkpoint.rangeIndexCoveredThrough(),
                checkpoint.producerStateCoveredThrough(),
                checkpoint.transactionIndexCoveredThrough(),
                checkpoint.leaderEpochCoveredThrough());
    }

    public long recoveryCoveredThrough() {
        return Math.min(
                Math.min(rangeIndexCoveredThrough, producerStateCoveredThrough),
                Math.min(transactionIndexCoveredThrough, leaderEpochCoveredThrough));
    }

    public boolean isAlignedCompoundCheckpoint() {
        return rangeIndexCoveredThrough == producerStateCoveredThrough
                && rangeIndexCoveredThrough == transactionIndexCoveredThrough
                && rangeIndexCoveredThrough == leaderEpochCoveredThrough;
    }

    public boolean doesNotRegress(KafkaRecoveryCheckpointVectorV1 previous) {
        Objects.requireNonNull(previous, "previous");
        return runBinding.equals(previous.runBinding)
                && rangeIndexCoveredThrough >= previous.rangeIndexCoveredThrough
                && producerStateCoveredThrough >= previous.producerStateCoveredThrough
                && transactionIndexCoveredThrough >= previous.transactionIndexCoveredThrough
                && leaderEpochCoveredThrough >= previous.leaderEpochCoveredThrough;
    }
}
