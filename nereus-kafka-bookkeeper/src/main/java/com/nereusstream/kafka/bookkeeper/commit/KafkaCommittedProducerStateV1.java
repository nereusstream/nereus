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

import java.util.Collections;
import java.util.NavigableMap;
import java.util.Objects;
import java.util.Optional;
import java.util.TreeMap;

/** Immutable partition producer table used for native duplicate/sequence recovery. */
public record KafkaCommittedProducerStateV1(NavigableMap<Long, KafkaProducerSessionStateV1> producers) {
    public KafkaCommittedProducerStateV1 {
        producers = Collections.unmodifiableNavigableMap(new TreeMap<>(Objects.requireNonNull(producers, "producers")));
        producers.forEach((producerId, state) -> {
            if (producerId != state.producerId()) {
                throw new IllegalArgumentException("producer table key differs from its session identity");
            }
        });
    }

    public static KafkaCommittedProducerStateV1 empty() {
        return new KafkaCommittedProducerStateV1(new TreeMap<>());
    }

    public KafkaCommittedProducerStateV1 apply(KafkaSpeculativeCommitV1 commit) {
        TreeMap<Long, KafkaProducerSessionStateV1> replacement = new TreeMap<>(producers);
        for (KafkaAssignedProtocolBatchV1 batch : commit.batches()) {
            batch.delta().duplicateIdentity().ifPresent(identity -> {
                KafkaProducerBatchResultV1 result =
                        new KafkaProducerBatchResultV1(identity, batch.startOffset(), batch.endOffsetExclusive());
                replacement.compute(
                        identity.producerId(),
                        (producerId, previous) ->
                                previous == null ? KafkaProducerSessionStateV1.first(result) : previous.append(result));
            });
        }
        return new KafkaCommittedProducerStateV1(replacement);
    }

    public Optional<KafkaProducerBatchResultV1> findDuplicate(KafkaBatchDuplicateIdentityV1 identity) {
        Objects.requireNonNull(identity, "identity");
        KafkaProducerSessionStateV1 producer = producers.get(identity.producerId());
        if (producer == null) {
            return Optional.empty();
        }
        return producer.recentBatches().stream()
                .filter(batch -> batch.identity().equals(identity))
                .findFirst();
    }
}
