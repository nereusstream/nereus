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

package com.nereusstream.kafka.bookkeeper.commit;

import com.nereusstream.domain.bytes.CanonicalBytes;
import com.nereusstream.domain.bytes.Sha256Digest;
import com.nereusstream.kafka.bookkeeper.protocol.KafkaPartitionStateReferenceV1;
import java.util.HashMap;
import java.util.Map;
import java.util.Objects;

/** Owner-local immutable component repository; K1 roots expose objects only by exact generation/content identity. */
final class KafkaProtocolStateRepositoryV1 {
    private final Map<KafkaPartitionStateReferenceV1, Stored> values = new HashMap<>();

    synchronized KafkaPartitionStateReferenceV1 store(long generation, CanonicalBytes canonical, Object value) {
        Objects.requireNonNull(canonical, "canonical");
        Objects.requireNonNull(value, "value");
        KafkaPartitionStateReferenceV1 reference =
                new KafkaPartitionStateReferenceV1(generation, Sha256Digest.hash(canonical));
        Stored candidate = new Stored(canonical, value);
        Stored previous = values.putIfAbsent(reference, candidate);
        if (previous != null && !previous.equals(candidate)) {
            throw new IllegalStateException("protocol component reference collision");
        }
        return reference;
    }

    synchronized <T> T resolve(KafkaPartitionStateReferenceV1 reference, Class<T> type) {
        Stored stored = values.get(Objects.requireNonNull(reference, "reference"));
        if (stored == null || !type.isInstance(stored.value())) {
            throw new IllegalStateException("coherent root names an unavailable or wrongly typed protocol component");
        }
        return type.cast(stored.value());
    }

    private record Stored(CanonicalBytes canonical, Object value) {}
}
