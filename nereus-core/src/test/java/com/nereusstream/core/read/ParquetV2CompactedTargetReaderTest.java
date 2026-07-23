/* Licensed under the Apache License, Version 2.0 */
package com.nereusstream.core.read;

import static org.assertj.core.api.Assertions.assertThat;

import com.nereusstream.api.Checksum;
import com.nereusstream.api.ChecksumType;
import com.nereusstream.api.EntryIndexLocation;
import com.nereusstream.api.EntryIndexRef;
import com.nereusstream.api.FirstEntryPolicy;
import com.nereusstream.api.ObjectType;
import com.nereusstream.api.OffsetRange;
import com.nereusstream.api.PayloadFormat;
import com.nereusstream.api.PhysicalReadResult;
import com.nereusstream.api.ReadBoundaryMode;
import com.nereusstream.api.ReadIsolation;
import com.nereusstream.api.ReadOptions;
import com.nereusstream.api.ReadRequest;
import com.nereusstream.api.ReadResult;
import com.nereusstream.api.ReadView;
import com.nereusstream.api.ResolvedRange;
import com.nereusstream.api.SemanticReadResult;
import com.nereusstream.api.StreamId;
import com.nereusstream.api.target.ObjectSliceReadTarget;
import com.nereusstream.objectstore.Crc32cChecksums;
import com.nereusstream.objectstore.compacted.CompactedObjectFormatV2;
import com.nereusstream.objectstore.compacted.KafkaCompactionDispositionV2;
import com.nereusstream.objectstore.compacted.KafkaCompactionKeyEncodingV2;
import com.nereusstream.objectstore.compacted.KafkaTopicCompactedFormatSpecV2;
import com.nereusstream.objectstore.compacted.KafkaTopicCompactedObjectReadResult;
import com.nereusstream.objectstore.compacted.KafkaTopicCompactedObjectRow;
import com.nereusstream.objectstore.compacted.RangedCompactedObjectMetadata;
import com.nereusstream.objectstore.compacted.RangedCompactedObjectReadResult;
import com.nereusstream.objectstore.compacted.RangedCompactedObjectRow;
import java.nio.ByteBuffer;
import java.time.Duration;
import java.util.List;
import java.util.Optional;
import java.util.OptionalLong;
import org.junit.jupiter.api.Test;

class ParquetV2CompactedTargetReaderTest {
    @Test
    void ncp2ContainingReadReturnsWholeRangedEntryAndExactCoverage() {
        StreamId streamId = new StreamId("s-ncp2-core");
        OffsetRange coverage = new OffsetRange(10, 13);
        ObjectSliceReadTarget target = target(streamId, coverage, ReadView.COMMITTED);
        ResolvedRange range = resolved(coverage, target, 3, 1, 3);
        byte[] payload = "abc".getBytes(java.nio.charset.StandardCharsets.UTF_8);
        RangedCompactedObjectRow row = new RangedCompactedObjectRow(
                10, 3, 0, ByteBuffer.wrap(payload), crc(payload), OptionalLong.empty());
        ParquetV2CompactedTargetReader reader = new ParquetV2CompactedTargetReader(
                ignored -> java.util.concurrent.CompletableFuture.completedFuture(
                        new RangedCompactedObjectReadResult(
                                metadata(streamId, coverage, ReadView.COMMITTED, 3, 3, 1, 3),
                                List.of(row), 13, 100, 10)),
                ignored -> java.util.concurrent.CompletableFuture.failedFuture(
                        new AssertionError("NTC2 reader must not run")));

        PhysicalReadResult result = reader.readPhysicalWithStats(
                        streamId,
                        request(11, ReadView.COMMITTED, ReadBoundaryMode.CONTAINING_ENTRY),
                        List.of(range))
                .join();

        assertThat(result.batches()).hasSize(1);
        assertThat(result.batches().get(0).range()).isEqualTo(coverage);
        assertThat(result.sourceCoverageEndOffset()).hasValue(13);
        assertThat(ReadTargetReaderKey.from(target)).isEqualTo(ParquetV2CompactedTargetReader.NCP2_KEY);
    }

    @Test
    void ntc2SparseHoleAdvancesCoverageWithoutPretendingTheFirstRowStartsAtRequest() {
        StreamId streamId = new StreamId("s-ntc2-core");
        OffsetRange coverage = new OffsetRange(20, 30);
        ObjectSliceReadTarget target = target(streamId, coverage, ReadView.TOPIC_COMPACTED);
        ResolvedRange range = resolved(coverage, target, 10, 1, 1);
        byte[] payload = new byte[] {7};
        KafkaTopicCompactedObjectRow row = new KafkaTopicCompactedObjectRow(
                23, 1, KafkaCompactionDispositionV2.RETAIN_UNKEYED,
                KafkaCompactionKeyEncodingV2.nullKey(23), ByteBuffer.wrap(payload), crc(payload),
                23, 0, sha256('9'), OptionalLong.empty());
        ParquetV2CompactedTargetReader reader = new ParquetV2CompactedTargetReader(
                ignored -> java.util.concurrent.CompletableFuture.failedFuture(
                        new AssertionError("NCP2 reader must not run")),
                ignored -> java.util.concurrent.CompletableFuture.completedFuture(
                        new KafkaTopicCompactedObjectReadResult(
                                metadata(streamId, coverage, ReadView.TOPIC_COMPACTED, 10, 1, 1, 1),
                                List.of(row), 30, 100, 10)));
        ReadRequest request = request(21, ReadView.TOPIC_COMPACTED, ReadBoundaryMode.EXACT_START);

        PhysicalReadResult physical = new ReadTargetDispatcher(
                        new ReadTargetReaderRegistry(List.of(reader)))
                .read(streamId, request, List.of(range))
                .join();

        assertThat(physical.batches()).hasSize(1);
        assertThat(physical.batches().get(0).range()).isEqualTo(new OffsetRange(23, 24));
        assertThat(physical.sourceCoverageEndOffset()).hasValue(30);
        ReadResult readResult = new ReadResult(streamId, 21, 24, physical.batches(), false);
        SemanticReadResult semantic = SemanticReadResult.forRequest(request, readResult, 30);
        assertThat(semantic.sourceCoverageEndOffset()).isEqualTo(30);
        assertThat(ReadTargetReaderKey.from(target)).isEqualTo(ParquetV2CompactedTargetReader.NTC2_KEY);
    }

    private static ReadRequest request(long start, ReadView view, ReadBoundaryMode boundary) {
        return new ReadRequest(
                start, view, boundary, FirstEntryPolicy.ALLOW_FIRST_ENTRY_OVERFLOW,
                new ReadOptions(100, 100, ReadIsolation.COMMITTED, Duration.ofSeconds(10)));
    }

    private static ObjectSliceReadTarget target(StreamId streamId, OffsetRange coverage, ReadView view) {
        Checksum content = sha256('a');
        com.nereusstream.api.ObjectKey key = CompactedObjectFormatV2.objectKey(
                "test-cluster", view, streamId, coverage, content, "a".repeat(26));
        com.nereusstream.api.ObjectId objectId = CompactedObjectFormatV2.objectId(key);
        EntryIndexRef footer = new EntryIndexRef(
                EntryIndexLocation.OBJECT_FOOTER,
                Optional.of(objectId),
                Optional.of(key),
                Optional.empty(),
                90,
                10,
                new Checksum(ChecksumType.CRC32C, "00000001"));
        return new ObjectSliceReadTarget(
                1, objectId, key, ObjectType.STREAM_COMPACTED_OBJECT,
                CompactedObjectFormatV2.physicalFormat(view), CompactedObjectFormatV2.KAFKA_LOGICAL_FORMAT,
                coverage.startOffset() + "-" + coverage.endOffset(), 0, 100,
                new Checksum(ChecksumType.CRC32C, "00000002"), footer);
    }

    private static ResolvedRange resolved(
            OffsetRange coverage,
            ObjectSliceReadTarget target,
            int records,
            int entries,
            long logicalBytes) {
        return new ResolvedRange(
                coverage, 2, target, PayloadFormat.KAFKA_RECORD_BATCH,
                records, entries, logicalBytes, List.of(), Optional.empty(), 1);
    }

    private static RangedCompactedObjectMetadata metadata(
            StreamId streamId,
            OffsetRange coverage,
            ReadView view,
            long sourceRecords,
            long outputRecords,
            int entries,
            long logicalBytes) {
        return new RangedCompactedObjectMetadata(
                view, streamId, coverage, sha256('1'), sha256('2'), "a".repeat(26),
                PayloadFormat.KAFKA_RECORD_BATCH, CompactedObjectFormatV2.KAFKA_LOGICAL_FORMAT,
                CompactedObjectFormatV2.RANGE_MODEL, sourceRecords, outputRecords, entries,
                logicalBytes, 100 + logicalBytes, "test", "UNCOMPRESSED", 2,
                view == ReadView.COMMITTED
                        ? Optional.empty()
                        : Optional.of(new KafkaTopicCompactedFormatSpecV2(
                                "latest", 1, CompactedObjectFormatV2.KAFKA_KEY_CODEC,
                                CompactedObjectFormatV2.KAFKA_REWRITE_CODEC, sha256('3'), 1, entries)));
    }

    private static Checksum sha256(char value) {
        return new Checksum(ChecksumType.SHA256, Character.toString(value).repeat(64));
    }

    private static int crc(byte[] payload) {
        return Crc32cChecksums.intValue(Crc32cChecksums.checksum(payload));
    }
}
