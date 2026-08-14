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

package com.nereusstream.kafka.bookkeeper.commit;

import com.nereusstream.kafka.bookkeeper.protocol.KafkaPartitionFenceV1;
import java.util.List;
import java.util.Objects;

/** Exact native producer/transaction delta vector bound to one later-assigned append range. */
public record KafkaProtocolAppendPlanV1(KafkaPartitionFenceV1 expectedFence, List<KafkaProtocolBatchDeltaV1> batches) {
    public KafkaProtocolAppendPlanV1 {
        Objects.requireNonNull(expectedFence, "expectedFence");
        batches = List.copyOf(Objects.requireNonNull(batches, "batches"));
        if (batches.isEmpty()) {
            throw new IllegalArgumentException("protocol append plan must contain at least one batch");
        }
        long total = 0;
        try {
            for (KafkaProtocolBatchDeltaV1 batch : batches) {
                total = Math.addExact(total, batch.logicalOffsetCount());
            }
        } catch (ArithmeticException failure) {
            throw new IllegalArgumentException("protocol append offset coverage overflows", failure);
        }
    }

    public long logicalOffsetCount() {
        long total = 0;
        for (KafkaProtocolBatchDeltaV1 batch : batches) {
            total = Math.addExact(total, batch.logicalOffsetCount());
        }
        return total;
    }
}
