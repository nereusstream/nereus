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

package com.nereusstream.kafka.fetch;

import java.util.Objects;

/** Frozen terminal facts for a stock-compatible whole-request Fetch read wave. */
public record KafkaFetchWaveResult<T>(
        T response,
        int responseBytes,
        boolean timedOut,
        int readAttempts) {
    public KafkaFetchWaveResult {
        Objects.requireNonNull(response, "response");
        if (responseBytes < 0 || readAttempts <= 0) {
            throw new IllegalArgumentException("invalid Kafka Fetch wave result");
        }
    }
}
