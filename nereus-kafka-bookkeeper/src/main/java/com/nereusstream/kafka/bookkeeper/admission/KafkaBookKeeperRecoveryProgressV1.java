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

/** Checked cumulative progress through an unchecked recovery tail. */
public record KafkaBookKeeperRecoveryProgressV1(long entries, long encodedBytes, long elapsedNanos) {
    public static final KafkaBookKeeperRecoveryProgressV1 ZERO = new KafkaBookKeeperRecoveryProgressV1(0, 0, 0);

    public KafkaBookKeeperRecoveryProgressV1 {
        if (entries < 0 || encodedBytes < 0 || elapsedNanos < 0) {
            throw new IllegalArgumentException("recovery progress dimensions must be non-negative");
        }
    }

    public KafkaBookKeeperRecoveryProgressV1 advance(
            long additionalEntries, long additionalEncodedBytes, long additionalElapsedNanos) {
        if (additionalEntries < 0 || additionalEncodedBytes < 0 || additionalElapsedNanos < 0) {
            throw new IllegalArgumentException("recovery progress deltas must be non-negative");
        }
        return new KafkaBookKeeperRecoveryProgressV1(
                Math.addExact(entries, additionalEntries),
                Math.addExact(encodedBytes, additionalEncodedBytes),
                Math.addExact(elapsedNanos, additionalElapsedNanos));
    }
}
