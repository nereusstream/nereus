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

package com.nereusstream.kafka.bookkeeper.checkpoint;

import java.util.Objects;

/** One exactly published profile-specific carrier of a profile-neutral checkpoint state. */
public record KafkaProtocolCheckpointPublicationV1(long physicalIdentity, KafkaProtocolCheckpointStateV1 state) {
    public KafkaProtocolCheckpointPublicationV1 {
        Objects.requireNonNull(state, "state");
        if (physicalIdentity < 0) {
            throw new IllegalArgumentException("checkpoint physical identity must be non-negative");
        }
    }
}
