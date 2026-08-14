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

package com.nereusstream.kafka.bookkeeper.run;

import com.nereusstream.kafka.bookkeeper.nbke2.Nbke2RunBindingV1;
import com.nereusstream.storage.api.bookkeeper.RunLedgerHandleV1;
import com.nereusstream.storage.api.kafka.KafkaRunRootSnapshotV1;
import java.util.Objects;
import java.util.OptionalLong;

/** Coherent owner-local view of one K3 run and its durable low-frequency root. */
public record KafkaBookKeeperRunSnapshotV1(
        Nbke2RunBindingV1 runBinding,
        RunLedgerHandleV1 handle,
        KafkaRunRootSnapshotV1 root,
        KafkaBookKeeperRunStateV1 state,
        long nextEntryId,
        int pendingOperations,
        OptionalLong latestProtocolCheckpointEntryId) {
    public KafkaBookKeeperRunSnapshotV1 {
        Objects.requireNonNull(runBinding, "runBinding");
        Objects.requireNonNull(handle, "handle");
        Objects.requireNonNull(root, "root");
        Objects.requireNonNull(state, "state");
        Objects.requireNonNull(latestProtocolCheckpointEntryId, "latestProtocolCheckpointEntryId");
        if (nextEntryId <= 0 || pendingOperations < 0) {
            throw new IllegalArgumentException("run snapshot counters are outside their domains");
        }
    }
}
