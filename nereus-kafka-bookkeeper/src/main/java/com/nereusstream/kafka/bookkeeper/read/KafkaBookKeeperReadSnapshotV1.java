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

package com.nereusstream.kafka.bookkeeper.read;

import com.nereusstream.kafka.bookkeeper.commit.KafkaBookKeeperActiveTailLocatorV1;
import com.nereusstream.kafka.bookkeeper.commit.KafkaCoherentProtocolSnapshotV1;
import com.nereusstream.kafka.bookkeeper.commit.KafkaTransactionStateV1;
import com.nereusstream.kafka.bookkeeper.nbke2.Nbke2RunBindingV1;
import com.nereusstream.kafka.bookkeeper.protocol.KafkaPartitionFenceV1;
import com.nereusstream.kafka.bookkeeper.protocol.KafkaPartitionProtocolStateV1;
import java.util.List;
import java.util.Objects;
import java.util.Optional;

/** One coherent K1 root plus the exact run/transaction views used by a K6 read request. */
public record KafkaBookKeeperReadSnapshotV1(
        KafkaPartitionProtocolStateV1 root,
        KafkaBookKeeperRunTableV1 runTable,
        KafkaTransactionStateV1 transactionState) {
    public KafkaBookKeeperReadSnapshotV1 {
        Objects.requireNonNull(root, "root");
        Objects.requireNonNull(runTable, "runTable");
        Objects.requireNonNull(transactionState, "transactionState");
        KafkaPartitionFenceV1 fence = root.fence();
        for (KafkaBookKeeperReadRunV1 run : runTable.runs()) {
            Nbke2RunBindingV1 binding = run.runBinding();
            if (!binding.bindingId().equals(fence.bindingId())
                    || !binding.topicIncarnation().equals(fence.topicIncarnation())
                    || binding.partitionId() != fence.partitionId()
                    || !binding.storageEpochId().equals(fence.storageEpochId())
                    || binding.creatorOwnerEpoch() > fence.ownerEpoch()
                    || binding.kafkaLeaderEpoch() > fence.kafkaLeaderEpoch()
                    || run.endOffsetExclusive() > root.frontiers().readableEndOffset()) {
                throw new IllegalArgumentException("read run escapes the coherent partition identity/frontier");
            }
            if (run.active()
                    && run.sourceGeneration() != root.references().activeTail().generation()) {
                throw new IllegalArgumentException("active read run differs from the coherent active-tail generation");
            }
        }
    }

    public static KafkaBookKeeperReadSnapshotV1 fromActive(KafkaCoherentProtocolSnapshotV1 snapshot) {
        Objects.requireNonNull(snapshot, "snapshot");
        List<KafkaBookKeeperActiveTailLocatorV1> groups = snapshot.activeTail().locators();
        if (groups.isEmpty()) {
            return new KafkaBookKeeperReadSnapshotV1(
                    snapshot.root(), new KafkaBookKeeperRunTableV1(List.of()), snapshot.transactionState());
        }
        KafkaBookKeeperActiveTailLocatorV1 first = groups.get(0);
        for (KafkaBookKeeperActiveTailLocatorV1 group : groups) {
            if (!group.runBinding().equals(first.runBinding())
                    || !group.handle().equals(first.handle())) {
                throw new IllegalArgumentException("active-tail groups change run identity or handle");
            }
        }
        KafkaPackedBatchLocatorIndexV1 index = KafkaPackedBatchLocatorIndexV1.fromActiveTail(snapshot.activeTail());
        KafkaBookKeeperReadRunV1 run = new KafkaBookKeeperReadRunV1(
                first.runBinding(),
                first.handle(),
                snapshot.activeTail().startOffset(),
                snapshot.activeTail().endOffsetExclusive(),
                snapshot.root().references().activeTail().generation(),
                Optional.of(index),
                Optional.empty());
        return new KafkaBookKeeperReadSnapshotV1(
                snapshot.root(), new KafkaBookKeeperRunTableV1(List.of(run)), snapshot.transactionState());
    }
}
