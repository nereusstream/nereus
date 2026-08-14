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

/** One predecessor-chained descriptor with a nondecreasing epoch-nanosecond observation time. */
public record KafkaReplicaObservationRecordV1(
        long ordinal,
        Sha256Digest predecessorRecordDigest,
        long observedAtNanos,
        KafkaReplicaCommitDescriptorV1 descriptor) {
    public KafkaReplicaObservationRecordV1 {
        Objects.requireNonNull(predecessorRecordDigest, "predecessorRecordDigest");
        Objects.requireNonNull(descriptor, "descriptor");
        if (ordinal < 0
                || observedAtNanos < 0
                || ordinal == 0 && !predecessorRecordDigest.isZero()
                || ordinal > 0 && predecessorRecordDigest.isZero()) {
            throw new IllegalArgumentException("observation record ordinal/predecessor/time is inconsistent");
        }
    }
}
