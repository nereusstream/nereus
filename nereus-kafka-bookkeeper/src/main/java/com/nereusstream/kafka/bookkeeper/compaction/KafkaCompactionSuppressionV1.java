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

package com.nereusstream.kafka.bookkeeper.compaction;

import com.nereusstream.domain.bytes.Sha256Digest;
import com.nereusstream.kafka.bookkeeper.compaction.KafkaCompactionRecordsV1.DispositionRow;
import java.util.List;
import java.util.Objects;
import java.util.Set;
import java.util.stream.Collectors;

/** Immutable preferred-generation suppression applied to every raw predecessor fallback read. */
public final class KafkaCompactionSuppressionV1 {
    private final List<DispositionRow> dispositions;
    private final Set<Long> suppressedOffsets;
    private final Sha256Digest rootSha256;

    public KafkaCompactionSuppressionV1(List<DispositionRow> dispositions) {
        this.dispositions = List.copyOf(Objects.requireNonNull(dispositions, "dispositions"));
        if (this.dispositions.stream().map(DispositionRow::offset).distinct().count() != this.dispositions.size()) {
            throw new IllegalArgumentException("M5-B fallback suppression has duplicate offsets");
        }
        this.suppressedOffsets = this.dispositions.stream()
                .filter(value -> !value.retained())
                .map(DispositionRow::offset)
                .collect(Collectors.toUnmodifiableSet());
        this.rootSha256 = KafkaCompactionCanonicalV1.suppressionRoot(this.dispositions);
    }

    public Sha256Digest rootSha256() {
        return rootSha256;
    }

    public boolean allowFromFallback(long offset) {
        if (offset < 0) {
            throw new IllegalArgumentException("Kafka fallback offset is negative");
        }
        return !suppressedOffsets.contains(offset);
    }

    public <T> List<T> filterFallback(List<T> values, java.util.function.ToLongFunction<T> offset) {
        Objects.requireNonNull(values, "values");
        Objects.requireNonNull(offset, "offset");
        return values.stream()
                .filter(value -> allowFromFallback(offset.applyAsLong(value)))
                .toList();
    }
}
