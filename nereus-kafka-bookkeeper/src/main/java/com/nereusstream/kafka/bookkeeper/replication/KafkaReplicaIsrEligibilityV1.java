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

package com.nereusstream.kafka.bookkeeper.replication;

/** Complete closed ISR/HW observation-eligibility tuple and measured lag. */
public record KafkaReplicaIsrEligibilityV1(
        boolean journalDurableThroughObserved,
        boolean applyLagOffsetsWithinBound,
        boolean unappliedBytesWithinBound,
        boolean unappliedAgeWithinBound,
        boolean recoverableSourceCoversUnapplied,
        long applyLagOffsets,
        long unappliedBytes,
        long unappliedAgeNanos) {
    public KafkaReplicaIsrEligibilityV1 {
        if (applyLagOffsets < 0 || unappliedBytes < 0 || unappliedAgeNanos < 0) {
            throw new IllegalArgumentException("replica eligibility metrics must be non-negative");
        }
    }

    public boolean eligible() {
        return journalDurableThroughObserved
                && applyLagOffsetsWithinBound
                && unappliedBytesWithinBound
                && unappliedAgeWithinBound
                && recoverableSourceCoversUnapplied;
    }
}
