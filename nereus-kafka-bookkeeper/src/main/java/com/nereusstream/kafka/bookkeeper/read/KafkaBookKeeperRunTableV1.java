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

import com.nereusstream.storage.api.bookkeeper.StorageRunId;
import java.util.List;
import java.util.Objects;
import java.util.Optional;

/** Coarse ordered run table; per-RecordBatch lookup remains in packed primitive indexes. */
public record KafkaBookKeeperRunTableV1(List<KafkaBookKeeperReadRunV1> runs) {
    public KafkaBookKeeperRunTableV1 {
        runs = List.copyOf(Objects.requireNonNull(runs, "runs"));
        if (!runs.isEmpty()) {
            KafkaBookKeeperReadRunV1 first = runs.get(0);
            long previousStart = -1;
            long previousEnd = -1;
            for (KafkaBookKeeperReadRunV1 run : runs) {
                if (run.startOffset() <= previousStart
                        || run.startOffset() < previousEnd
                        || !run.runBinding()
                                .bindingId()
                                .equals(first.runBinding().bindingId())
                        || !run.runBinding()
                                .topicIncarnation()
                                .equals(first.runBinding().topicIncarnation())
                        || run.runBinding().partitionId() != first.runBinding().partitionId()
                        || !run.runBinding()
                                .storageEpochId()
                                .equals(first.runBinding().storageEpochId())
                        || !run.runBinding()
                                .providerScopeId()
                                .equals(first.runBinding().providerScopeId())) {
                    throw new IllegalArgumentException(
                            "read runs overlap, regress, or change partition/provider identity");
                }
                previousStart = run.startOffset();
                previousEnd = run.endOffsetExclusive();
            }
        }
    }

    public Optional<KafkaBookKeeperReadRunV1> floorOrSuccessor(long requestedOffset) {
        if (requestedOffset < 0) {
            throw new IllegalArgumentException("requested offset must be non-negative");
        }
        int low = 0;
        int high = runs.size() - 1;
        int floor = -1;
        while (low <= high) {
            int middle = (low + high) >>> 1;
            if (runs.get(middle).startOffset() <= requestedOffset) {
                floor = middle;
                low = middle + 1;
            } else {
                high = middle - 1;
            }
        }
        if (floor >= 0 && requestedOffset < runs.get(floor).endOffsetExclusive()) {
            return Optional.of(runs.get(floor));
        }
        int successor = floor + 1;
        return successor < runs.size() ? Optional.of(runs.get(successor)) : Optional.empty();
    }

    public Optional<KafkaBookKeeperReadRunV1> successor(KafkaBookKeeperReadRunV1 current) {
        int index = runs.indexOf(Objects.requireNonNull(current, "current"));
        if (index < 0) {
            throw new IllegalArgumentException("current run is not in the read table");
        }
        return index + 1 < runs.size() ? Optional.of(runs.get(index + 1)) : Optional.empty();
    }

    public Optional<KafkaBookKeeperReadRunV1> find(StorageRunId runId) {
        Objects.requireNonNull(runId, "runId");
        return runs.stream()
                .filter(run -> run.runBinding().runId().equals(runId))
                .findFirst();
    }
}
