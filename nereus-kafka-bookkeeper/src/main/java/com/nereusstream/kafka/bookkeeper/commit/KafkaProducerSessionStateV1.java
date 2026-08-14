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

import java.util.ArrayList;
import java.util.List;
import java.util.Objects;

/** Immutable committed producer epoch/sequence plus the bounded native duplicate result window. */
public record KafkaProducerSessionStateV1(
        long producerId,
        short producerEpoch,
        int lastSequence,
        long lastOffset,
        List<KafkaProducerBatchResultV1> recentBatches) {
    public static final int MAX_RECENT_BATCHES = 5;

    public KafkaProducerSessionStateV1 {
        recentBatches = List.copyOf(Objects.requireNonNull(recentBatches, "recentBatches"));
        if (producerId < 0
                || producerEpoch < 0
                || lastSequence < 0
                || lastOffset < 0
                || recentBatches.isEmpty()
                || recentBatches.size() > MAX_RECENT_BATCHES) {
            throw new IllegalArgumentException("committed producer session is outside its bounded domain");
        }
        long previousEnd = -1;
        for (KafkaProducerBatchResultV1 batch : recentBatches) {
            if (batch.identity().producerId() != producerId || batch.startOffset() < previousEnd) {
                throw new IllegalArgumentException("recent producer results change producer or offset order");
            }
            previousEnd = batch.endOffsetExclusive();
        }
        KafkaProducerBatchResultV1 last = recentBatches.get(recentBatches.size() - 1);
        if (last.identity().producerEpoch() != producerEpoch
                || last.identity().lastSequence() != lastSequence
                || last.endOffsetExclusive() - 1 != lastOffset) {
            throw new IllegalArgumentException("producer session tail differs from its recent result tail");
        }
    }

    public static KafkaProducerSessionStateV1 first(KafkaProducerBatchResultV1 result) {
        KafkaBatchDuplicateIdentityV1 identity = result.identity();
        if (identity.baseSequence() != 0) {
            throw new IllegalArgumentException("a new producer epoch must begin at sequence zero");
        }
        return new KafkaProducerSessionStateV1(
                identity.producerId(),
                identity.producerEpoch(),
                identity.lastSequence(),
                result.endOffsetExclusive() - 1,
                List.of(result));
    }

    public KafkaProducerSessionStateV1 append(KafkaProducerBatchResultV1 result) {
        KafkaBatchDuplicateIdentityV1 identity = result.identity();
        if (identity.producerId() != producerId || result.startOffset() <= lastOffset) {
            throw new IllegalArgumentException("producer result changes identity or regresses offsets");
        }
        if (identity.producerEpoch() < producerEpoch) {
            throw new IllegalArgumentException("producer epoch regresses");
        }
        if (identity.producerEpoch() > producerEpoch) {
            return first(result);
        }
        int expectedSequence = lastSequence == Integer.MAX_VALUE ? 0 : lastSequence + 1;
        if (identity.baseSequence() != expectedSequence) {
            throw new IllegalArgumentException("producer sequence is not the next speculative/committed sequence");
        }
        List<KafkaProducerBatchResultV1> recent = new ArrayList<>(recentBatches);
        recent.add(result);
        if (recent.size() > MAX_RECENT_BATCHES) {
            recent.remove(0);
        }
        return new KafkaProducerSessionStateV1(
                producerId, producerEpoch, identity.lastSequence(), result.endOffsetExclusive() - 1, recent);
    }
}
