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

import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.assertThatThrownBy;

import com.nereusstream.api.ErrorCode;
import com.nereusstream.api.NereusException;
import com.nereusstream.api.StreamId;
import com.nereusstream.metadata.oxia.CommitSliceRequest;
import com.nereusstream.metadata.oxia.OffsetIndexEntry;
import com.nereusstream.metadata.oxia.records.EntryIndexReferenceRecord;
import com.nereusstream.metadata.oxia.records.OffsetIndexRecord;
import java.time.Clock;
import java.time.Duration;
import java.time.Instant;
import java.time.ZoneId;
import java.time.ZoneOffset;
import java.util.List;
import org.junit.jupiter.api.Test;

class OffsetIndexCacheTest {
    private static final StreamId STREAM_ID = new StreamId("stream");

    @Test
    void cachesOnlyPositiveCoverageAndExpiresAtTtl() {
        MutableClock clock = new MutableClock();
        OffsetIndexCache cache = new OffsetIndexCache(true, Duration.ofSeconds(5), clock);

        cache.putPositive(STREAM_ID, List.of(), 0);
        assertThat(cache.lookup(STREAM_ID, 0, 0)).isEmpty();

        cache.putPositive(STREAM_ID, List.of(index(1, 10)), 0);
        assertThat(cache.lookup(STREAM_ID, 0, 0)).isPresent();
        assertThat(cache.lookup(STREAM_ID, 1, 0)).isEmpty();

        clock.advance(Duration.ofSeconds(5));
        assertThat(cache.lookup(STREAM_ID, 0, 0)).isEmpty();
    }

    @Test
    void lowerVersionWatchCannotEvictNewerDataButEqualVersionDoes() {
        MutableClock clock = new MutableClock();
        OffsetIndexCache cache = new OffsetIndexCache(true, Duration.ofSeconds(5), clock);
        cache.putPositive(STREAM_ID, List.of(index(1, 10)), 0);

        cache.invalidateFromWatch(STREAM_ID, 9);
        assertThat(cache.lookup(STREAM_ID, 0, 0)).isPresent();

        cache.invalidateFromWatch(STREAM_ID, 10);
        assertThat(cache.lookup(STREAM_ID, 0, 0)).isEmpty();
    }

    @Test
    void conflictingBytesForOneOffsetGenerationKeyAreRejected() {
        OffsetIndexCache cache = new OffsetIndexCache(
                true, Duration.ofSeconds(5), new MutableClock());
        cache.putPositive(STREAM_ID, List.of(index(1, 10)), 0);

        assertThatThrownBy(() -> cache.putPositive(STREAM_ID, List.of(index(2, 11)), 0))
                .isInstanceOfSatisfying(NereusException.class, error ->
                        assertThat(error.code()).isEqualTo(ErrorCode.METADATA_INVARIANT_VIOLATION));
    }

    @Test
    void boundsRecordsPerStreamAndEvictsOldestStream() {
        MutableClock clock = new MutableClock();
        OffsetIndexCache cache = new OffsetIndexCache(
                true, Duration.ofMinutes(1), clock, 1, 2);
        StreamId another = new StreamId("another");

        cache.putPositive(STREAM_ID, List.of(
                index(STREAM_ID, 0, 1, 10),
                index(STREAM_ID, 1, 2, 11),
                index(STREAM_ID, 2, 3, 12)), 0);

        assertThat(cache.lookup(STREAM_ID, 0, 0).orElseThrow()).hasSize(2);
        assertThat(cache.lookup(STREAM_ID, 2, 0)).isEmpty();

        clock.advance(Duration.ofMillis(1));
        cache.putPositive(another, List.of(index(another, 0, 1, 20)), 0);

        assertThat(cache.lookup(STREAM_ID, 0, 0)).isEmpty();
        assertThat(cache.lookup(another, 0, 0)).isPresent();
    }

    private static OffsetIndexEntry index(long logicalBytes, long metadataVersion) {
        return OffsetIndexEntry.fromLegacy(new OffsetIndexRecord(
                STREAM_ID.value(),
                0,
                1,
                0,
                logicalBytes,
                "object",
                "object-key",
                "slice",
                "MULTI_STREAM_WAL_OBJECT",
                "WAL_OBJECT_V1",
                "OPAQUE_SLICE",
                "OPAQUE_RECORD_BATCH",
                100,
                1,
                1,
                1,
                logicalBytes,
                List.of(),
                new EntryIndexReferenceRecord(
                        "OBJECT_FOOTER",
                        "",
                        "",
                        new byte[0],
                        200,
                        10,
                        "CRC32C",
                        "11111111"),
                CommitSliceRequest.emptyProjectionIdentity(),
                "CRC32C",
                "22222222",
                1,
                1,
                1,
                false,
                metadataVersion));
    }

    private static OffsetIndexEntry index(
            StreamId streamId,
            long startOffset,
            long endOffset,
            long metadataVersion) {
        String suffix = Long.toString(endOffset);
        return OffsetIndexEntry.fromLegacy(new OffsetIndexRecord(
                streamId.value(),
                startOffset,
                endOffset,
                0,
                endOffset,
                "object-" + suffix,
                "object-key-" + suffix,
                "slice-" + suffix,
                "MULTI_STREAM_WAL_OBJECT",
                "WAL_OBJECT_V1",
                "OPAQUE_SLICE",
                "OPAQUE_RECORD_BATCH",
                100 + startOffset,
                1,
                1,
                1,
                1,
                List.of(),
                new EntryIndexReferenceRecord(
                        "OBJECT_FOOTER",
                        "",
                        "",
                        new byte[0],
                        200 + startOffset,
                        10,
                        "CRC32C",
                        "11111111"),
                CommitSliceRequest.emptyProjectionIdentity(),
                "CRC32C",
                "22222222",
                1,
                1,
                endOffset,
                false,
                metadataVersion));
    }

    private static final class MutableClock extends Clock {
        private Instant instant = Instant.parse("2026-07-11T00:00:00Z");

        void advance(Duration duration) {
            instant = instant.plus(duration);
        }

        @Override
        public ZoneId getZone() {
            return ZoneOffset.UTC;
        }

        @Override
        public Clock withZone(ZoneId zone) {
            return this;
        }

        @Override
        public Instant instant() {
            return instant;
        }
    }
}
