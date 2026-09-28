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
        List<KafkaProducerBatchResultV1> recentBatches,
        int coordinatorEpoch,
        long lastMarkerOffset,
        long lastTimestamp) {
    public static final int MAX_RECENT_BATCHES = 5;

    public KafkaProducerSessionStateV1 {
        recentBatches = List.copyOf(Objects.requireNonNull(recentBatches, "recentBatches"));
        if (producerId < 0
                || producerEpoch < 0
                || lastSequence < -1
                || lastOffset < -1
                || coordinatorEpoch < -1
                || lastMarkerOffset < -1
                || (coordinatorEpoch == -1) != (lastMarkerOffset == -1)
                || recentBatches.isEmpty() != (lastSequence == -1 && lastOffset == -1)
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
        if (!recentBatches.isEmpty()) {
            KafkaProducerBatchResultV1 last = recentBatches.get(recentBatches.size() - 1);
            if (last.identity().producerEpoch() != producerEpoch
                    || last.identity().lastSequence() != lastSequence
                    || last.endOffsetExclusive() - 1 != lastOffset) {
                throw new IllegalArgumentException("producer session tail differs from its recent result tail");
            }
        }
    }

    public KafkaProducerSessionStateV1(
            long producerId,
            short producerEpoch,
            int lastSequence,
            long lastOffset,
            List<KafkaProducerBatchResultV1> recentBatches,
            int coordinatorEpoch,
            long lastMarkerOffset) {
        this(
                producerId,
                producerEpoch,
                lastSequence,
                lastOffset,
                recentBatches,
                coordinatorEpoch,
                lastMarkerOffset,
                -1);
    }

    public KafkaProducerSessionStateV1(
            long producerId,
            short producerEpoch,
            int lastSequence,
            long lastOffset,
            List<KafkaProducerBatchResultV1> recentBatches) {
        this(producerId, producerEpoch, lastSequence, lastOffset, recentBatches, -1, -1);
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
                List.of(result),
                -1,
                -1,
                result.maxTimestamp());
    }

    public KafkaProducerSessionStateV1 append(KafkaProducerBatchResultV1 result) {
        KafkaBatchDuplicateIdentityV1 identity = result.identity();
        if (identity.producerId() != producerId || result.startOffset() <= Math.max(lastOffset, lastMarkerOffset)) {
            throw new IllegalArgumentException("producer result changes identity or regresses offsets");
        }
        if (identity.producerEpoch() < producerEpoch) {
            throw new IllegalArgumentException("producer epoch regresses");
        }
        if (identity.producerEpoch() > producerEpoch) {
            var first = first(result);
            return new KafkaProducerSessionStateV1(
                    first.producerId(),
                    first.producerEpoch(),
                    first.lastSequence(),
                    first.lastOffset(),
                    first.recentBatches(),
                    coordinatorEpoch,
                    lastMarkerOffset,
                    result.maxTimestamp());
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
                producerId,
                producerEpoch,
                identity.lastSequence(),
                result.endOffsetExclusive() - 1,
                recent,
                coordinatorEpoch,
                lastMarkerOffset,
                result.maxTimestamp());
    }

    public KafkaProducerSessionStateV1 marker(short epoch, int coordinator, long offset) {
        return marker(epoch, coordinator, offset, -1);
    }

    public KafkaProducerSessionStateV1 marker(short epoch, int coordinator, long offset, long timestamp) {
        if (epoch < producerEpoch
                || coordinator < coordinatorEpoch
                || offset <= Math.max(lastOffset, lastMarkerOffset)) {
            throw new IllegalArgumentException("transaction marker producer or coordinator epoch regresses");
        }
        return epoch == producerEpoch
                ? new KafkaProducerSessionStateV1(
                        producerId, epoch, lastSequence, lastOffset, recentBatches, coordinator, offset, timestamp)
                : markerOnly(producerId, epoch, coordinator, offset, timestamp);
    }

    public static KafkaProducerSessionStateV1 markerOnly(long producerId, short epoch, int coordinator, long offset) {
        return markerOnly(producerId, epoch, coordinator, offset, -1);
    }

    public static KafkaProducerSessionStateV1 markerOnly(
            long producerId, short epoch, int coordinator, long offset, long timestamp) {
        return new KafkaProducerSessionStateV1(producerId, epoch, -1, -1, List.of(), coordinator, offset, timestamp);
    }
}
