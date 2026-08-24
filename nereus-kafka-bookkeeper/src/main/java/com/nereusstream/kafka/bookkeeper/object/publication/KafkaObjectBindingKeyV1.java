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

package com.nereusstream.kafka.bookkeeper.object.publication;

import com.nereusstream.domain.identity.KafkaTopicId;
import com.nereusstream.domain.identity.StorageEpochId;
import com.nereusstream.domain.identity.TopicBindingId;
import java.util.Objects;

/** Durable logical isolation key for one Kafka Object-WAL completion tracker and read view. */
public record KafkaObjectBindingKeyV1(
        TopicBindingId bindingId, KafkaTopicId topicId, int partitionId, StorageEpochId storageEpochId) {
    public KafkaObjectBindingKeyV1 {
        Objects.requireNonNull(bindingId, "bindingId");
        Objects.requireNonNull(topicId, "topicId");
        Objects.requireNonNull(storageEpochId, "storageEpochId");
        if (bindingId.digest().isZero() || storageEpochId.digest().isZero() || partitionId < 0) {
            throw new IllegalArgumentException("Kafka Object binding key is outside its domain");
        }
    }
}
