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

package com.nereusstream.kafka.bookkeeper.admission;

import java.util.Objects;

/** Mandatory simultaneous entry, byte, and elapsed-time recovery envelope. */
public record KafkaBookKeeperRecoveryEnvelopeV1(
        long maximumEntries, long maximumEncodedBytes, long maximumElapsedNanos) {
    public KafkaBookKeeperRecoveryEnvelopeV1 {
        if (maximumEntries <= 0 || maximumEncodedBytes <= 0 || maximumElapsedNanos <= 0) {
            throw new IllegalArgumentException("all recovery envelope dimensions must be positive");
        }
    }

    public KafkaBookKeeperRecoveryEnvelopeV1 loweredBy(KafkaBookKeeperRecoveryEnvelopeV1 requestedLowerBound) {
        Objects.requireNonNull(requestedLowerBound, "requestedLowerBound");
        if (requestedLowerBound.maximumEntries > maximumEntries
                || requestedLowerBound.maximumEncodedBytes > maximumEncodedBytes
                || requestedLowerBound.maximumElapsedNanos > maximumElapsedNanos) {
            throw new IllegalArgumentException("a lower authority cannot enlarge a recovery envelope dimension");
        }
        return requestedLowerBound;
    }

    public KafkaBookKeeperRecoveryStatusV1 classify(KafkaBookKeeperRecoveryProgressV1 progress) {
        Objects.requireNonNull(progress, "progress");
        if (progress.entries() > maximumEntries) {
            return KafkaBookKeeperRecoveryStatusV1.ENTRY_COUNT_EXCEEDED;
        }
        if (progress.encodedBytes() > maximumEncodedBytes) {
            return KafkaBookKeeperRecoveryStatusV1.ENCODED_BYTES_EXCEEDED;
        }
        if (progress.elapsedNanos() > maximumElapsedNanos) {
            return KafkaBookKeeperRecoveryStatusV1.ELAPSED_NANOS_EXCEEDED;
        }
        return KafkaBookKeeperRecoveryStatusV1.WITHIN_ENVELOPE;
    }
}
