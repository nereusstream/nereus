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
import java.util.ArrayList;
import java.util.List;
import java.util.Objects;

/** Hidden ordered producer/transaction delta installed after offset assignment and before storage I/O. */
public record KafkaSpeculativeCommitV1(
        long startOffset,
        long endOffsetExclusive,
        KafkaPartitionFenceV1 expectedFence,
        List<KafkaAssignedProtocolBatchV1> batches) {
    public KafkaSpeculativeCommitV1 {
        Objects.requireNonNull(expectedFence, "expectedFence");
        batches = List.copyOf(Objects.requireNonNull(batches, "batches"));
        if (startOffset < 0 || endOffsetExclusive <= startOffset || batches.isEmpty()) {
            throw new IllegalArgumentException("speculative commit range is empty or invalid");
        }
        long next = startOffset;
        for (KafkaAssignedProtocolBatchV1 batch : batches) {
            if (batch.startOffset() != next) {
                throw new IllegalArgumentException("speculative batch coverage is not contiguous");
            }
            next = batch.endOffsetExclusive();
        }
        if (next != endOffsetExclusive) {
            throw new IllegalArgumentException("speculative batch coverage differs from the commit range");
        }
    }

    public static KafkaSpeculativeCommitV1 assign(
            KafkaProtocolAppendPlanV1 plan, long startOffset, long endOffsetExclusive) {
        Objects.requireNonNull(plan, "plan");
        if (Math.addExact(startOffset, plan.logicalOffsetCount()) != endOffsetExclusive) {
            throw new IllegalArgumentException("assigned append range differs from the protocol plan");
        }
        List<KafkaAssignedProtocolBatchV1> assigned =
                new ArrayList<>(plan.batches().size());
        long next = startOffset;
        for (KafkaProtocolBatchDeltaV1 batch : plan.batches()) {
            long end = Math.addExact(next, batch.logicalOffsetCount());
            assigned.add(new KafkaAssignedProtocolBatchV1(next, end, batch));
            next = end;
        }
        return new KafkaSpeculativeCommitV1(startOffset, endOffsetExclusive, plan.expectedFence(), assigned);
    }
}
