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

package com.nereusstream.core.read;

import com.nereusstream.api.StreamId;
import com.nereusstream.api.ErrorCode;
import com.nereusstream.api.NereusException;
import com.nereusstream.metadata.oxia.OffsetIndexEntry;
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
    private static final Comparator<OffsetIndexEntry> ORDER = Comparator
            .comparingLong((OffsetIndexEntry record) -> record.range().endOffset())
            .thenComparingLong(OffsetIndexEntry::generation);

    private final boolean enabled;
    private final long ttlMillis;
    private final Clock clock;
    private final int maxStreams;
    private final int maxRecordsPerStream;
    private final ConcurrentHashMap<StreamId, CacheEntry> entries = new ConcurrentHashMap<>();

    OffsetIndexCache(boolean enabled, Duration ttl, Clock clock) {
        this(enabled, ttl, clock, Integer.MAX_VALUE, Integer.MAX_VALUE);
    }

    OffsetIndexCache(
            boolean enabled,
            Duration ttl,
            Clock clock,
            int maxStreams,
            int maxRecordsPerStream) {
        this.enabled = enabled;
        this.ttlMillis = durationMillis(Objects.requireNonNull(ttl, "ttl"));
        this.clock = Objects.requireNonNull(clock, "clock");
        if (maxStreams <= 0 || maxRecordsPerStream <= 0) {
            throw new IllegalArgumentException("cache limits must be positive");
        }
        this.maxStreams = maxStreams;
        this.maxRecordsPerStream = maxRecordsPerStream;
    }

    Optional<List<OffsetIndexEntry>> lookup(
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
                        && record.range().startOffset() <= targetOffset
                        && targetOffset < record.range().endOffset());
        return covers ? Optional.of(entry.records()) : Optional.empty();
    }

    void putPositive(
            StreamId streamId,
            List<OffsetIndexEntry> records,
            long trimOffset) {
        if (!enabled || records.isEmpty()) {
            return;
        }
        List<OffsetIndexEntry> positive = records.stream()
                .filter(record -> record.streamId().equals(streamId))
                .sorted(ORDER)
                .toList();
        if (positive.isEmpty()) {
            return;
        }
        long now = clock.millis();
        entries.compute(streamId, (ignored, existing) -> {
            Map<IndexIdentity, OffsetIndexEntry> merged = new LinkedHashMap<>();
            positive.forEach(record -> {
                IndexIdentity identity = IndexIdentity.of(record);
                OffsetIndexEntry previous = merged.put(identity, record);
                if (previous != null && !previous.equals(record)) {
                    throw new NereusException(
                            ErrorCode.METADATA_INVARIANT_VIOLATION,
                            false,
                            "offset-index cache observed conflicting bytes for one durable key");
                }
            });
            if (existing != null) {
                existing.records().forEach(record -> {
                    IndexIdentity identity = IndexIdentity.of(record);
                    OffsetIndexEntry current = merged.get(identity);
                    if (current != null && !current.equals(record)) {
                        throw new NereusException(
                                ErrorCode.METADATA_INVARIANT_VIOLATION,
                                false,
                                "offset-index cache observed conflicting bytes for one durable key");
                    }
                    merged.putIfAbsent(identity, record);
                });
            }
            List<OffsetIndexEntry> selected = merged.values().stream()
                    .limit(maxRecordsPerStream)
                    .toList();
            List<OffsetIndexEntry> ordered = new ArrayList<>(selected);
            ordered.sort(ORDER);
            long metadataVersion = ordered.stream()
                    .mapToLong(OffsetIndexEntry::metadataVersion)
                    .max()
                    .orElse(0);
            return new CacheEntry(List.copyOf(ordered), trimOffset, now, metadataVersion);
        });
        enforceStreamLimit(streamId);
    }

    private void enforceStreamLimit(StreamId protectedStream) {
        while (entries.size() > maxStreams) {
            Optional<Map.Entry<StreamId, CacheEntry>> oldest = entries.entrySet().stream()
                    .filter(entry -> !entry.getKey().equals(protectedStream))
                    .min(Comparator.comparingLong(entry -> entry.getValue().createdAtMillis()));
            if (oldest.isEmpty()) {
                return;
            }
            Map.Entry<StreamId, CacheEntry> entry = oldest.orElseThrow();
            entries.remove(entry.getKey(), entry.getValue());
        }
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
            List<OffsetIndexEntry> records,
            long trimOffset,
            long createdAtMillis,
            long metadataVersion) {
    }

    private record IndexIdentity(long offsetEnd, long generation) {
        private static IndexIdentity of(OffsetIndexEntry record) {
            return new IndexIdentity(record.range().endOffset(), record.generation());
        }
    }
}
