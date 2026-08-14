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

package com.nereusstream.kafka.bookkeeper.protocol;

/** Checked half-open Kafka protocol and storage frontiers for one partition. */
public record KafkaPartitionFrontiersV1(
        long trimStartOffset,
        long allocatedEndOffset,
        long durableEndOffset,
        long readableEndOffset,
        long highWatermark,
        long lastStableOffset) {
    public KafkaPartitionFrontiersV1 {
        if (trimStartOffset < 0
                || trimStartOffset > lastStableOffset
                || lastStableOffset > highWatermark
                || highWatermark > readableEndOffset
                || readableEndOffset > durableEndOffset
                || durableEndOffset > allocatedEndOffset) {
            throw new IllegalArgumentException(
                    "frontiers must satisfy trim <= LSO <= HW <= readable <= durable <= allocated");
        }
    }

    public boolean noRegressionFrom(KafkaPartitionFrontiersV1 previous) {
        return trimStartOffset >= previous.trimStartOffset
                && allocatedEndOffset >= previous.allocatedEndOffset
                && durableEndOffset >= previous.durableEndOffset
                && readableEndOffset >= previous.readableEndOffset
                && highWatermark >= previous.highWatermark
                && lastStableOffset >= previous.lastStableOffset;
    }
}
