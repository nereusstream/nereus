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

package com.nereusstream.kafka.partition;

import java.time.Duration;
import java.util.Objects;
import java.util.OptionalLong;

/** One leader-fenced ListOffsets request with hard committed-tail scan limits. */
public record KafkaListOffsetsRequest(
        KafkaListOffsetQuery query,
        OptionalLong targetTimestampMillis,
        int expectedLeaderEpoch,
        long maxScanRecords,
        long maxScanBytes,
        int readTargetBytes,
        int hardMaxReadBytes,
        int maxReadOperations,
        Duration timeout) {
    public KafkaListOffsetsRequest {
        Objects.requireNonNull(query, "query");
        Objects.requireNonNull(targetTimestampMillis, "targetTimestampMillis");
        Objects.requireNonNull(timeout, "timeout");
        boolean timestampQuery = query == KafkaListOffsetQuery.TIMESTAMP;
        if (timestampQuery != targetTimestampMillis.isPresent()) {
            throw new IllegalArgumentException("only TIMESTAMP queries carry a target timestamp");
        }
        if (targetTimestampMillis.isPresent() && targetTimestampMillis.orElseThrow() < 0) {
            throw new IllegalArgumentException("targetTimestampMillis must be non-negative");
        }
        if (expectedLeaderEpoch < 0
                || maxScanRecords <= 0
                || maxScanBytes <= 0
                || readTargetBytes <= 0
                || hardMaxReadBytes < readTargetBytes
                || maxReadOperations <= 0) {
            throw new IllegalArgumentException("invalid Kafka ListOffsets scan bounds");
        }
        try {
            if (timeout.isZero() || timeout.isNegative() || timeout.toMillis() <= 0 || timeout.toNanos() <= 0) {
                throw new IllegalArgumentException("Kafka ListOffsets timeout must be positive");
            }
        } catch (ArithmeticException failure) {
            throw new IllegalArgumentException("Kafka ListOffsets timeout is outside nanosecond range", failure);
        }
    }
}
