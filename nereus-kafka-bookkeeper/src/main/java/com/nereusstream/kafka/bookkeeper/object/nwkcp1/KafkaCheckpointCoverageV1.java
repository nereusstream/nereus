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

package com.nereusstream.kafka.bookkeeper.object.nwkcp1;

import com.nereusstream.domain.identity.KafkaTopicId;
import com.nereusstream.domain.identity.StorageEpochId;
import com.nereusstream.domain.identity.TopicBindingId;
import com.nereusstream.kafka.bookkeeper.checkpoint.KafkaRecoveryCheckpointVectorV1;
import java.util.Objects;

/** One partition component of a Root-bound Kafka protocol checkpoint Head vector. */
public record KafkaCheckpointCoverageV1(
        TopicBindingId bindingId,
        KafkaTopicId topicId,
        int partitionId,
        StorageEpochId storageEpochId,
        long ownerEpoch,
        int kafkaLeaderEpoch,
        long rangeIndexCoveredThrough,
        long producerStateCoveredThrough,
        long transactionIndexCoveredThrough,
        long leaderEpochCoveredThrough)
        implements Comparable<KafkaCheckpointCoverageV1> {
    public KafkaCheckpointCoverageV1 {
        Objects.requireNonNull(bindingId, "bindingId");
        Objects.requireNonNull(topicId, "topicId");
        Objects.requireNonNull(storageEpochId, "storageEpochId");
        if (bindingId.digest().isZero()
                || storageEpochId.digest().isZero()
                || partitionId < 0
                || ownerEpoch <= 0
                || kafkaLeaderEpoch < 0
                || rangeIndexCoveredThrough < 0
                || producerStateCoveredThrough < 0
                || transactionIndexCoveredThrough < 0
                || leaderEpochCoveredThrough < 0) {
            throw new IllegalArgumentException("checkpoint Head coverage is outside the v1 domain");
        }
    }

    public static KafkaCheckpointCoverageV1 from(KafkaRecoveryCheckpointVectorV1 vector) {
        Objects.requireNonNull(vector, "vector");
        var binding = vector.runBinding();
        return new KafkaCheckpointCoverageV1(
                binding.bindingId(),
                binding.topicIncarnation().topicId(),
                binding.partitionId(),
                binding.storageEpochId(),
                binding.creatorOwnerEpoch(),
                binding.kafkaLeaderEpoch(),
                vector.rangeIndexCoveredThrough(),
                vector.producerStateCoveredThrough(),
                vector.transactionIndexCoveredThrough(),
                vector.leaderEpochCoveredThrough());
    }

    public boolean sameCheckpointContext(KafkaCheckpointCoverageV1 other) {
        Objects.requireNonNull(other, "other");
        return bindingId.equals(other.bindingId)
                && topicId.equals(other.topicId)
                && partitionId == other.partitionId
                && storageEpochId.equals(other.storageEpochId)
                && ownerEpoch == other.ownerEpoch
                && kafkaLeaderEpoch == other.kafkaLeaderEpoch;
    }

    public boolean doesNotRegress(KafkaCheckpointCoverageV1 previous) {
        Objects.requireNonNull(previous, "previous");
        return sameCheckpointContext(previous)
                && rangeIndexCoveredThrough >= previous.rangeIndexCoveredThrough
                && producerStateCoveredThrough >= previous.producerStateCoveredThrough
                && transactionIndexCoveredThrough >= previous.transactionIndexCoveredThrough
                && leaderEpochCoveredThrough >= previous.leaderEpochCoveredThrough;
    }

    @Override
    public int compareTo(KafkaCheckpointCoverageV1 other) {
        int result =
                bindingId.digest().toHex().compareTo(other.bindingId.digest().toHex());
        if (result == 0) {
            result = topicId.value().toHex().compareTo(other.topicId.value().toHex());
        }
        return result == 0 ? Integer.compare(partitionId, other.partitionId) : result;
    }
}
