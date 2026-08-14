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

import java.util.Objects;

/** Closed publication outcome plus the exact state observed after the attempt. */
public record KafkaPartitionPublicationResultV1(
        KafkaPartitionPublicationOutcomeV1 outcome, KafkaPartitionProtocolStateV1 observedState) {
    public KafkaPartitionPublicationResultV1 {
        Objects.requireNonNull(outcome, "outcome");
        Objects.requireNonNull(observedState, "observedState");
    }

    public boolean published() {
        return outcome == KafkaPartitionPublicationOutcomeV1.PUBLISHED
                || outcome == KafkaPartitionPublicationOutcomeV1.PUBLISHED_NOTIFICATION_FAILED;
    }
}
