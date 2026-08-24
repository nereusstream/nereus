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

package com.nereusstream.kafka.bookkeeper.object.publication;

import com.nereusstream.domain.bytes.Sha256Digest;
import java.util.Objects;

/** Structured NWG1 extent identity retained by Kafka active-tail locators. */
public record KafkaObjectExtentIdentityV1(
        Sha256Digest walRunRootSha,
        int laneId,
        long laneSequence,
        long directoryPrefixEnd,
        long bodyLength,
        Sha256Digest bodySha) {
    public KafkaObjectExtentIdentityV1 {
        Objects.requireNonNull(walRunRootSha, "walRunRootSha");
        Objects.requireNonNull(bodySha, "bodySha");
        if (walRunRootSha.isZero()
                || bodySha.isZero()
                || laneId < 0
                || laneId > 2
                || laneSequence < 0
                || directoryPrefixEnd < 256
                || directoryPrefixEnd > bodyLength
                || bodyLength <= 0) {
            throw new IllegalArgumentException("Kafka Object extent identity is outside the NWG1 v1 domain");
        }
    }
}
