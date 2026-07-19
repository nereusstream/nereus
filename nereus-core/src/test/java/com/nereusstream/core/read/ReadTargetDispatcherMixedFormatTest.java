/* Licensed under the Apache License, Version 2.0 */
package com.nereusstream.core.read;

import static org.assertj.core.api.Assertions.assertThat;

import com.nereusstream.api.OffsetRange;
import com.nereusstream.api.ObjectType;
import com.nereusstream.api.PayloadFormat;
import com.nereusstream.api.PhysicalReadResult;
import com.nereusstream.api.ReadBatch;
import com.nereusstream.api.ReadIsolation;
import com.nereusstream.api.ReadOptions;
import com.nereusstream.api.ResolvedRange;
import com.nereusstream.api.StreamId;
import com.nereusstream.api.target.ObjectSliceReadTarget;
import com.nereusstream.objectstore.wal.WalReadResult;
import java.time.Duration;
import java.util.ArrayList;
import java.util.List;
import java.util.Optional;
import java.util.concurrent.CompletableFuture;
import org.junit.jupiter.api.Test;

class ReadTargetDispatcherMixedFormatTest {
    @Test
    void groupsOnlyAdjacentRangesWithTheSameExactReaderKey() {
        RecordingReader wal = new RecordingReader(ReadTargetReaderRegistryTest.key(
                ObjectType.MULTI_STREAM_WAL_OBJECT, "WAL_OBJECT_V1"));
        RecordingReader compacted = new RecordingReader(ReadTargetReaderRegistryTest.key(
                ObjectType.STREAM_COMPACTED_OBJECT, "NEREUS_COMPACTED_PARQUET_V1"));
        ReadTargetDispatcher dispatcher = new ReadTargetDispatcher(
                new ReadTargetReaderRegistry(List.of(wal, compacted)));
        List<ResolvedRange> ranges = List.of(
                range(0, ObjectType.MULTI_STREAM_WAL_OBJECT, "WAL_OBJECT_V1"),
                range(1, ObjectType.MULTI_STREAM_WAL_OBJECT, "WAL_OBJECT_V1"),
                range(2, ObjectType.STREAM_COMPACTED_OBJECT, "NEREUS_COMPACTED_PARQUET_V1"),
                range(3, ObjectType.MULTI_STREAM_WAL_OBJECT, "WAL_OBJECT_V1"));

        PhysicalReadResult result = dispatcher.read(
                        new StreamId("stream"),
                        0,
                        ranges,
                        new ReadOptions(10, 10, ReadIsolation.COMMITTED, Duration.ofSeconds(1)))
                .join();

        assertThat(result.batches()).hasSize(4);
        assertThat(wal.runSizes).containsExactly(2, 1);
        assertThat(compacted.runSizes).containsExactly(1);
    }

    private static ResolvedRange range(long offset, ObjectType type, String format) {
        return new ResolvedRange(
                new OffsetRange(offset, offset + 1),
                type == ObjectType.MULTI_STREAM_WAL_OBJECT ? 0 : 2,
                ReadTargetReaderRegistryTest.target(type, format),
                PayloadFormat.OPAQUE_RECORD_BATCH,
                1,
                1,
                1,
                List.of(),
                Optional.empty(),
                1);
    }

    private static final class RecordingReader implements ReadTargetReader {
        private final ReadTargetReaderKey key;
        private final List<Integer> runSizes = new ArrayList<>();

        private RecordingReader(ReadTargetReaderKey key) {
            this.key = key;
        }

        @Override
        public ReadTargetReaderKey key() {
            return key;
        }

        @Override
        public long reservationBytes(ResolvedRange range) {
            return 1;
        }

        @Override
        public CompletableFuture<WalReadResult> readWithStats(
                StreamId streamId,
                long startOffset,
                List<ResolvedRange> ranges,
                ReadOptions options) {
            runSizes.add(ranges.size());
            List<ReadBatch> batches = ranges.stream().map(range -> {
                ObjectSliceReadTarget target = (ObjectSliceReadTarget) range.readTarget();
                return new ReadBatch(
                        range.offsetRange(),
                        range.payloadFormat(),
                        new byte[] {1},
                        range.schemaRefs(),
                        target.entryIndexRef(),
                        range.projectionRef(),
                        target.objectId(),
                        target.objectOffset(),
                        target.objectLength());
            }).toList();
            return CompletableFuture.completedFuture(new WalReadResult(batches, List.of()));
        }
    }
}
