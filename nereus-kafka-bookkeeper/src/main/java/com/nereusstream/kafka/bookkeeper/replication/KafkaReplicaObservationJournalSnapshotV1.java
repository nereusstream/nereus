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
import java.util.List;
import java.util.Objects;

/** Highest contiguous surviving journal prefix and exact append-chain continuation. */
public record KafkaReplicaObservationJournalSnapshotV1(
        KafkaReplicaJournalHealthV1 health,
        List<KafkaReplicaObservationRecordV1> records,
        long durableThroughOffset,
        long nextOrdinal,
        Sha256Digest predecessorRecordDigest,
        long encodedBytes) {
    public KafkaReplicaObservationJournalSnapshotV1 {
        Objects.requireNonNull(health, "health");
        records = List.copyOf(Objects.requireNonNull(records, "records"));
        Objects.requireNonNull(predecessorRecordDigest, "predecessorRecordDigest");
        if (durableThroughOffset < 0
                || nextOrdinal < 0
                || nextOrdinal != records.size()
                || encodedBytes < 0
                || nextOrdinal == 0 && !predecessorRecordDigest.isZero()
                || nextOrdinal > 0 && predecessorRecordDigest.isZero()) {
            throw new IllegalArgumentException("observation-journal snapshot is internally inconsistent");
        }
    }

    public boolean acceptsAppend() {
        return health == KafkaReplicaJournalHealthV1.HEALTHY;
    }
}
