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

package com.nereusstream.kafka.bookkeeper.checkpoint;

import com.nereusstream.kafka.bookkeeper.nbke2.Nbke2ConstantsV1;
import com.nereusstream.kafka.bookkeeper.read.KafkaBookKeeperReadRunV1;
import com.nereusstream.storage.api.bookkeeper.StorageRunId;
import java.util.ArrayList;
import java.util.List;
import java.util.Objects;

/** Durable locators for an already protected crash run that cannot accept a native footer. */
public record KafkaCheckpointReadIndexV1(StorageRunId runId, List<Row> rows) {
    public record Row(long startOffset, long endOffsetExclusive, long entryId, long groupOrdinal, long payloadBytes) {}

    public KafkaCheckpointReadIndexV1 {
        Objects.requireNonNull(runId, "runId");
        rows = List.copyOf(rows);
        if (rows.isEmpty() || rows.size() > Nbke2ConstantsV1.FORMAT_MAX_LOCATOR_COUNT) {
            throw new IllegalArgumentException("checkpoint source locator count exceeds its persisted bound");
        }
        long end = rows.get(0).startOffset();
        long entry = 0;
        long group = -1;
        for (var row : rows) {
            if (row.startOffset() != end
                    || row.endOffsetExclusive() <= end
                    || row.entryId() <= entry
                    || row.groupOrdinal() < group
                    || row.payloadBytes() <= 0) {
                throw new IllegalArgumentException("checkpoint source locators regress or contain a gap");
            }
            end = row.endOffsetExclusive();
            entry = row.entryId();
            group = row.groupOrdinal();
        }
    }

    public static KafkaCheckpointReadIndexV1 from(KafkaBookKeeperReadRunV1 run) {
        var index = run.activeIndex().orElseThrow();
        var rows = new ArrayList<Row>();
        for (int ordinal = 0; ordinal < index.size(); ordinal++) {
            var row = index.at(ordinal);
            rows.add(new Row(
                    row.startOffset(),
                    row.endOffsetExclusive(),
                    row.entryId(),
                    row.appendGroupOrdinal(),
                    row.rawPayloadBytes()));
        }
        return new KafkaCheckpointReadIndexV1(run.runBinding().runId(), rows);
    }
}
