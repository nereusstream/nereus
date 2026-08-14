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

/** Hard Observed/Applied eligibility inputs; K9 selects versioned defaults from evidence. */
public record KafkaReplicaEligibilityBoundsV1(
        long maximumApplyLagOffsets, long maximumUnappliedBytes, long maximumUnappliedNanos) {
    public KafkaReplicaEligibilityBoundsV1 {
        if (maximumApplyLagOffsets < 0 || maximumUnappliedBytes < 0 || maximumUnappliedNanos < 0) {
            throw new IllegalArgumentException("replica eligibility bounds must be non-negative");
        }
    }
}
