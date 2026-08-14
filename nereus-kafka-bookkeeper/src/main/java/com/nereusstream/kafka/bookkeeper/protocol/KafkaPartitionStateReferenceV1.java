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

import com.nereusstream.domain.bytes.Sha256Digest;
import java.util.Objects;

/** Immutable generation/content identity for one large partition-state view. */
public record KafkaPartitionStateReferenceV1(long generation, Sha256Digest contentDigest) {
    public KafkaPartitionStateReferenceV1 {
        Objects.requireNonNull(contentDigest, "contentDigest");
        if (generation < 0 || contentDigest.isZero()) {
            throw new IllegalArgumentException("state reference generation/digest is outside its domain");
        }
    }

    public boolean doesNotRegress(KafkaPartitionStateReferenceV1 previous) {
        Objects.requireNonNull(previous, "previous");
        return generation > previous.generation
                || generation == previous.generation && contentDigest.equals(previous.contentDigest);
    }
}
