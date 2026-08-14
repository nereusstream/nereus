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

package com.nereusstream.kafka.bookkeeper.replication;

import com.nereusstream.domain.bytes.Sha256Digest;
import java.util.Objects;

/** Exact local sync proof returned only after the observation record is durable. */
public record KafkaReplicaJournalAppendProofV1(long ordinal, int encodedBytes, Sha256Digest recordDigest) {
    public KafkaReplicaJournalAppendProofV1 {
        Objects.requireNonNull(recordDigest, "recordDigest");
        if (ordinal < 0 || encodedBytes <= 0 || recordDigest.isZero()) {
            throw new IllegalArgumentException("journal append proof is outside its exact identity domain");
        }
    }
}
