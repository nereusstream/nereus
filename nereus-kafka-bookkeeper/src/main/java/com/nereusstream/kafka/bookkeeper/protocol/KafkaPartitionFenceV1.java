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

package com.nereusstream.kafka.bookkeeper.protocol;

import com.nereusstream.domain.identity.StorageEpochId;
import com.nereusstream.domain.identity.TopicBindingId;
import com.nereusstream.domain.protocol.KafkaTopicIncarnationIdentity;
import java.util.Objects;

/** Exact independent publication fences for one Kafka partition state root. */
public record KafkaPartitionFenceV1(
        TopicBindingId bindingId,
        KafkaTopicIncarnationIdentity topicIncarnation,
        int partitionId,
        long bindingGeneration,
        StorageEpochId storageEpochId,
        long ownerEpoch,
        int kafkaLeaderEpoch) {
    public KafkaPartitionFenceV1 {
        Objects.requireNonNull(bindingId, "bindingId");
        Objects.requireNonNull(topicIncarnation, "topicIncarnation");
        Objects.requireNonNull(storageEpochId, "storageEpochId");
        if (bindingId.digest().isZero() || storageEpochId.digest().isZero()) {
            throw new IllegalArgumentException("binding and storage epoch identities must be non-zero");
        }
        if (partitionId < 0 || bindingGeneration <= 0 || ownerEpoch <= 0 || kafkaLeaderEpoch < 0) {
            throw new IllegalArgumentException("partition and fence generations are outside their domains");
        }
    }

    public boolean samePartitionIdentity(KafkaPartitionFenceV1 other) {
        Objects.requireNonNull(other, "other");
        return bindingId.equals(other.bindingId)
                && topicIncarnation.equals(other.topicIncarnation)
                && partitionId == other.partitionId;
    }
}
