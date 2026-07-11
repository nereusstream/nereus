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

package io.nereus.core.read;

import io.nereus.api.StreamId;
import io.nereus.api.ErrorCode;
import io.nereus.api.NereusException;
import io.nereus.metadata.oxia.records.OffsetIndexRecord;
import java.time.Clock;
import java.time.Duration;
import java.util.ArrayList;
import java.util.Comparator;
import java.util.LinkedHashMap;
import java.util.List;
import java.util.Map;
import java.util.Objects;
import java.util.Optional;
import java.util.concurrent.ConcurrentHashMap;

/** Positive-only offset-index cache. Empty scans and EOF are never stored. */
final class OffsetIndexCache {
    private static final Comparator<OffsetIndexRecord> ORDER = Comparator
            .comparingLong(OffsetIndexRecord::offsetEnd)
            .thenComparingLong(OffsetIndexRecord::generation);

    private final boolean enabled;
    private final long ttlMillis;
    private final Clock clock;
    private final ConcurrentHashMap<StreamId, CacheEntry> entries = new ConcurrentHashMap<>();

    OffsetIndexCache(boolean enabled, Duration ttl, Clock clock) {
        this.enabled = enabled;
        this.ttlMillis = durationMillis(Objects.requireNonNull(ttl, "ttl"));
        this.clock = Objects.requireNonNull(clock, "clock");
    }

    Optional<List<OffsetIndexRecord>> lookup(
            StreamId streamId,
            long targetOffset,
            long currentTrimOffset) {
        if (!enabled) {
            return Optional.empty();
        }
        CacheEntry entry = entries.get(streamId);
        if (entry == null) {
            return Optional.empty();
        }
        long age = clock.millis() - entry.createdAtMillis();
        if (age < 0 || age >= ttlMillis || targetOffset < entry.trimOffset()
                || targetOffset < currentTrimOffset) {
            entries.remove(streamId, entry);
            return Optional.empty();
        }
        boolean covers = entry.records().stream().anyMatch(record ->
                !record.tombstoned()
                        && record.offsetStart() <= targetOffset
                        && targetOffset < record.offsetEnd());
        return covers ? Optional.of(entry.records()) : Optional.empty();
    }

    void putPositive(
            StreamId streamId,
            List<OffsetIndexRecord> records,
            long trimOffset) {
        if (!enabled || records.isEmpty()) {
            return;
        }
        List<OffsetIndexRecord> positive = records.stream()
                .filter(record -> record.streamId().equals(streamId.value()))
                .sorted(ORDER)
                .toList();
        if (positive.isEmpty()) {
            return;
        }
        entries.compute(streamId, (ignored, existing) -> {
            Map<IndexIdentity, OffsetIndexRecord> merged = new LinkedHashMap<>();
            if (existing != null) {
                existing.records().forEach(record -> merged.put(IndexIdentity.of(record), record));
            }
            positive.forEach(record -> {
                IndexIdentity identity = IndexIdentity.of(record);
                OffsetIndexRecord previous = merged.put(identity, record);
                if (previous != null && !previous.equals(record)) {
                    throw new NereusException(
                            ErrorCode.METADATA_INVARIANT_VIOLATION,
                            false,
                            "offset-index cache observed conflicting bytes for one durable key");
                }
            });
            List<OffsetIndexRecord> ordered = new ArrayList<>(merged.values());
            ordered.sort(ORDER);
            long metadataVersion = ordered.stream()
                    .mapToLong(OffsetIndexRecord::metadataVersion)
                    .max()
                    .orElse(0);
            return new CacheEntry(List.copyOf(ordered), trimOffset, clock.millis(), metadataVersion);
        });
    }

    void invalidate(StreamId streamId) {
        entries.remove(streamId);
    }

    void invalidateFromWatch(StreamId streamId, long metadataVersion) {
        entries.computeIfPresent(streamId, (ignored, entry) ->
                metadataVersion >= entry.metadataVersion() ? null : entry);
    }

    void clear() {
        entries.clear();
    }

    private static long durationMillis(Duration duration) {
        try {
            return duration.toMillis();
        } catch (ArithmeticException e) {
            return Long.MAX_VALUE;
        }
    }

    private record CacheEntry(
            List<OffsetIndexRecord> records,
            long trimOffset,
            long createdAtMillis,
            long metadataVersion) {
    }

    private record IndexIdentity(long offsetEnd, long generation) {
        private static IndexIdentity of(OffsetIndexRecord record) {
            return new IndexIdentity(record.offsetEnd(), record.generation());
        }
    }
}
