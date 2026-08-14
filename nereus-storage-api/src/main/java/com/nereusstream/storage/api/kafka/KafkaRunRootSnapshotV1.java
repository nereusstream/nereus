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

package com.nereusstream.storage.api.kafka;

import com.nereusstream.domain.identity.StorageEpochId;
import com.nereusstream.domain.identity.TopicBindingId;
import com.nereusstream.domain.protocol.KafkaTopicIncarnationIdentity;
import com.nereusstream.storage.api.bookkeeper.BookKeeperLedgerIdentity;
import com.nereusstream.storage.api.bookkeeper.CellProviderScopeId;
import com.nereusstream.storage.api.bookkeeper.StorageRunId;
import java.util.Objects;
import java.util.Optional;
import java.util.OptionalLong;

/** Immutable low-frequency root authority for one Kafka partition BookKeeper run. */
public record KafkaRunRootSnapshotV1(
        TopicBindingId bindingId,
        KafkaTopicIncarnationIdentity topicIncarnation,
        int partitionId,
        StorageEpochId storageEpochId,
        long creatorOwnerEpoch,
        int kafkaLeaderEpoch,
        CellProviderScopeId providerScopeId,
        StorageRunId runId,
        BookKeeperLedgerIdentity ledgerIdentity,
        long kafkaStartOffset,
        OptionalLong kafkaEndOffsetExclusive,
        KafkaRunRootStateV1 state,
        Optional<StorageRunId> predecessorRunId) {
    public KafkaRunRootSnapshotV1 {
        Objects.requireNonNull(bindingId, "bindingId");
        Objects.requireNonNull(topicIncarnation, "topicIncarnation");
        Objects.requireNonNull(storageEpochId, "storageEpochId");
        Objects.requireNonNull(providerScopeId, "providerScopeId");
        Objects.requireNonNull(runId, "runId");
        Objects.requireNonNull(ledgerIdentity, "ledgerIdentity");
        Objects.requireNonNull(kafkaEndOffsetExclusive, "kafkaEndOffsetExclusive");
        Objects.requireNonNull(state, "state");
        Objects.requireNonNull(predecessorRunId, "predecessorRunId");
        if (partitionId < 0 || creatorOwnerEpoch <= 0 || kafkaLeaderEpoch < 0 || kafkaStartOffset < 0) {
            throw new IllegalArgumentException("partition, epochs, and start offset are outside their domains");
        }
        boolean sealed = state == KafkaRunRootStateV1.SEALED;
        if (sealed != kafkaEndOffsetExclusive.isPresent()) {
            throw new IllegalArgumentException("exactly SEALED roots carry an end offset");
        }
        if (sealed && kafkaEndOffsetExclusive.getAsLong() < kafkaStartOffset) {
            throw new IllegalArgumentException("sealed end offset precedes the run start");
        }
        if (predecessorRunId.filter(runId::equals).isPresent()) {
            throw new IllegalArgumentException("a run cannot be its own predecessor");
        }
    }
}
