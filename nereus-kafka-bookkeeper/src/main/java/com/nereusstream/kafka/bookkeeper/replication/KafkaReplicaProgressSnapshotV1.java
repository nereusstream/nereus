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

import com.nereusstream.domain.bytes.Sha256Digest;
import com.nereusstream.kafka.bookkeeper.protocol.KafkaPartitionFenceV1;
import java.util.Objects;
import java.util.Optional;

/** Separate Observed/Applied progress plus the currently evaluated ISR eligibility. */
public record KafkaReplicaProgressSnapshotV1(
        int replicaId,
        KafkaPartitionFenceV1 fence,
        long observedEndOffset,
        long appliedEndOffset,
        long observedStateVersion,
        long appliedStateVersion,
        Optional<Sha256Digest> observedDescriptorDigest,
        KafkaReplicaJournalHealthV1 journalHealth,
        boolean canAdvanceObserved,
        KafkaReplicaIsrEligibilityV1 isrEligibility) {
    public KafkaReplicaProgressSnapshotV1 {
        Objects.requireNonNull(fence, "fence");
        Objects.requireNonNull(observedDescriptorDigest, "observedDescriptorDigest");
        Objects.requireNonNull(journalHealth, "journalHealth");
        Objects.requireNonNull(isrEligibility, "isrEligibility");
        if (replicaId < 0
                || appliedEndOffset < 0
                || observedEndOffset < appliedEndOffset
                || observedStateVersion < appliedStateVersion
                || appliedStateVersion < 0) {
            throw new IllegalArgumentException("replica progress frontiers or versions are inconsistent");
        }
    }
}
