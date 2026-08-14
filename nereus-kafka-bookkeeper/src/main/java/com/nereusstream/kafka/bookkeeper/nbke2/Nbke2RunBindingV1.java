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

import com.nereusstream.domain.identity.StorageEpochId;
import com.nereusstream.domain.identity.TopicBindingId;
import com.nereusstream.domain.protocol.KafkaTopicIncarnationIdentity;
import com.nereusstream.storage.api.bookkeeper.CellProviderScopeId;
import com.nereusstream.storage.api.bookkeeper.StorageRunId;
import java.util.Objects;

/** Identity/fence tuple repeated by every frame in one run. */
public record Nbke2RunBindingV1(
        TopicBindingId bindingId,
        KafkaTopicIncarnationIdentity topicIncarnation,
        int partitionId,
        StorageEpochId storageEpochId,
        long creatorOwnerEpoch,
        int kafkaLeaderEpoch,
        CellProviderScopeId providerScopeId,
        StorageRunId runId) {
    public Nbke2RunBindingV1 {
        Objects.requireNonNull(bindingId, "bindingId");
        Objects.requireNonNull(topicIncarnation, "topicIncarnation");
        Objects.requireNonNull(storageEpochId, "storageEpochId");
        Objects.requireNonNull(providerScopeId, "providerScopeId");
        Objects.requireNonNull(runId, "runId");
        if (partitionId < 0 || creatorOwnerEpoch <= 0 || kafkaLeaderEpoch < 0) {
            throw new IllegalArgumentException("partition and epochs are outside the NBKE2 v1 domain");
        }
        int topicNameBytes = topicIncarnation.topicName().bytes().length();
        if (topicNameBytes <= 0 || topicNameBytes > Nbke2ConstantsV1.FORMAT_MAX_TOPIC_NAME_BYTES) {
            throw new IllegalArgumentException("topic name length is outside the NBKE2 v1 domain");
        }
    }
}
