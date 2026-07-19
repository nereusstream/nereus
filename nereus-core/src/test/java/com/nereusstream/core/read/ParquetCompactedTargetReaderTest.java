/* Licensed under the Apache License, Version 2.0 */
package com.nereusstream.core.read;

import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.assertThatThrownBy;

import com.nereusstream.api.ErrorCode;
import com.nereusstream.api.NereusException;
import com.nereusstream.api.ObjectType;
import com.nereusstream.api.OffsetRange;
import com.nereusstream.api.PayloadFormat;
import com.nereusstream.api.ProjectionRef;
import com.nereusstream.api.ProjectionType;
import com.nereusstream.api.ReadIsolation;
import com.nereusstream.api.ReadOptions;
import com.nereusstream.api.ReadView;
import com.nereusstream.api.ResolvedRange;
import com.nereusstream.api.StreamId;
import com.nereusstream.api.target.ObjectSliceReadTarget;
import com.nereusstream.metadata.oxia.codec.ReadTargetCodecRegistry;
import com.nereusstream.objectstore.Crc32cChecksums;
import com.nereusstream.objectstore.PutObjectOptions;
import com.nereusstream.objectstore.compacted.CompactedObjectFormatV1;
import com.nereusstream.objectstore.compacted.CompactedObjectRow;
import com.nereusstream.objectstore.compacted.CompactedObjectWriteRequest;
import com.nereusstream.objectstore.compacted.CompactedObjectWriteResult;
import com.nereusstream.objectstore.compacted.ParquetCompactedObjectReader;
import com.nereusstream.objectstore.compacted.ParquetCompactedObjectWriter;
import com.nereusstream.objectstore.staging.StagingFileManager;
import com.nereusstream.objectstore.testing.LocalFileObjectStore;
import com.nereusstream.objectstore.wal.WalReadResult;
import java.nio.ByteBuffer;
import java.nio.charset.StandardCharsets;
import java.nio.file.Files;
import java.nio.file.Path;
import java.nio.file.attribute.PosixFilePermissions;
import java.time.Duration;
import java.util.List;
import java.util.Map;
import java.util.Optional;
import java.util.OptionalInt;
import java.util.OptionalLong;
import java.util.concurrent.Flow;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.io.TempDir;

class ParquetCompactedTargetReaderTest {
    @TempDir
    Path temporaryDirectory;

    @Test
    void mapsDenseNcp1RowsToExactBatchesAndRejectsCrossStreamSelection() throws Exception {
        StreamId streamId = new StreamId("stream-core-ncp1");
        List<byte[]> payloads = List.of(utf8("zero"), utf8("one"), utf8("two"), utf8("three"));
        long logicalBytes = payloads.stream().mapToLong(bytes -> bytes.length).sum();
        CompactedObjectWriteRequest writeRequest = request(streamId, logicalBytes);
        try (StagingFileManager staging = staging();
                LocalFileObjectStore store =
                        new LocalFileObjectStore(temporaryDirectory.resolve("objects"));
                CompactedObjectWriteResult written = new ParquetCompactedObjectWriter(staging, Runnable::run)
                        .write(writeRequest, publisher(List.of(
                                row(40, payloads.get(0)),
                                row(41, payloads.get(1)),
                                row(42, payloads.get(2)),
                                row(43, payloads.get(3)))))
                        .join()) {
            store.putObject(
                            written.objectKey(),
                            written.stagingFile(),
                            new PutObjectOptions(
                                    "application/vnd.apache.parquet",
                                    written.storageCrc32c(),
                                    true,
                                    Map.of(),
                                    Duration.ofSeconds(10)))
                    .join();
            ObjectSliceReadTarget target = new ObjectSliceReadTarget(
                    1,
                    written.objectId(),
                    written.objectKey(),
                    ObjectType.STREAM_COMPACTED_OBJECT,
                    written.physicalFormat(),
                    writeRequest.logicalFormat(),
                    "40-44",
                    0,
                    written.objectLength(),
                    written.storageCrc32c(),
                    written.entryIndexRef());
            ResolvedRange range = new ResolvedRange(
                    writeRequest.sourceCoverage(),
                    2,
                    target,
                    writeRequest.payloadFormat(),
                    4,
                    4,
                    logicalBytes,
                    List.of(),
                    Optional.of(new ProjectionRef(
                            ProjectionType.VIRTUAL_LEDGER, "nereus-ml-v1.core-test")),
                    9);
            ParquetCompactedTargetReader reader = new ParquetCompactedTargetReader(
                    new ParquetCompactedObjectReader(store, Runnable::run));

            WalReadResult read = reader.readWithStats(
                            streamId,
                            41,
                            List.of(range),
                            new ReadOptions(
                                    2,
                                    payloads.get(1).length + payloads.get(2).length,
                                    ReadIsolation.COMMITTED,
                                    Duration.ofSeconds(10)))
                    .join();
            assertThat(read.batches()).extracting(batch -> batch.range().startOffset())
                    .containsExactly(41L, 42L);
            assertThat(read.batches()).extracting(batch -> new String(
                            batch.payload(), StandardCharsets.UTF_8))
                    .containsExactly("one", "two");
            assertThat(read.batches()).allSatisfy(batch -> {
                assertThat(batch.sourceObjectId()).isEqualTo(written.objectId());
                assertThat(batch.sourceObjectOffset()).isZero();
                assertThat(batch.sourceObjectLength()).isEqualTo(written.objectLength());
                assertThat(batch.projectionRef()).isEmpty();
            });
            assertThat(read.sliceStats()).singleElement().satisfies(stats -> {
                assertThat(stats.fullSlicePayloadBytes()).isEqualTo(written.objectLength());
                assertThat(stats.entryIndexBytes()).isEqualTo(written.entryIndexRef().length());
                assertThat(stats.returnedPayloadBytes())
                        .isEqualTo(payloads.get(1).length + payloads.get(2).length);
            });
            assertThat(reader.reservationBytes(range)).isEqualTo(
                    written.objectLength() + written.entryIndexRef().length());
            assertThat(ReadTargetCodecRegistry.phase15().encode(target).targetVersion()).isEqualTo(1);

            assertThatThrownBy(() -> reader.readWithStats(
                                    new StreamId("another-stream"),
                                    40,
                                    List.of(range),
                                    new ReadOptions(
                                            4,
                                            1 << 20,
                                            ReadIsolation.COMMITTED,
                                            Duration.ofSeconds(10)))
                            .join())
                    .satisfies(failure -> assertThat(findNereus(failure).code())
                            .isEqualTo(ErrorCode.OBJECT_CHECKSUM_MISMATCH));
        }
    }

    @Test
    void readsCompressibleLogicalPayloadLargerThanPhysicalParquetIo() throws Exception {
        StreamId streamId = new StreamId("stream-core-ncp1-compressed");
        byte[] payload = new byte[256 * 1024];
        java.util.Arrays.fill(payload, (byte) 'z');
        long logicalBytes = Math.multiplyExact(payload.length, 4L);
        CompactedObjectWriteRequest writeRequest = request(streamId, logicalBytes);
        try (StagingFileManager staging = staging();
                LocalFileObjectStore store =
                        new LocalFileObjectStore(temporaryDirectory.resolve("compressed-objects"));
                CompactedObjectWriteResult written = new ParquetCompactedObjectWriter(staging, Runnable::run)
                        .write(writeRequest, publisher(List.of(
                                row(40, payload),
                                row(41, payload),
                                row(42, payload),
                                row(43, payload))))
                        .join()) {
            store.putObject(
                            written.objectKey(),
                            written.stagingFile(),
                            new PutObjectOptions(
                                    "application/vnd.apache.parquet",
                                    written.storageCrc32c(),
                                    true,
                                    Map.of(),
                                    Duration.ofSeconds(10)))
                    .join();
            ObjectSliceReadTarget target = new ObjectSliceReadTarget(
                    1,
                    written.objectId(),
                    written.objectKey(),
                    ObjectType.STREAM_COMPACTED_OBJECT,
                    written.physicalFormat(),
                    writeRequest.logicalFormat(),
                    "40-44",
                    0,
                    written.objectLength(),
                    written.storageCrc32c(),
                    written.entryIndexRef());
            ResolvedRange range = new ResolvedRange(
                    writeRequest.sourceCoverage(),
                    2,
                    target,
                    writeRequest.payloadFormat(),
                    4,
                    4,
                    logicalBytes,
                    List.of(),
                    Optional.of(new ProjectionRef(
                            ProjectionType.VIRTUAL_LEDGER, "nereus-ml-v1.core-compressed")),
                    9);
            ParquetCompactedTargetReader reader = new ParquetCompactedTargetReader(
                    new ParquetCompactedObjectReader(store, Runnable::run));

            WalReadResult read = reader.readWithStats(
                            streamId,
                            40,
                            List.of(range),
                            new ReadOptions(
                                    2,
                                    payload.length * 2,
                                    ReadIsolation.COMMITTED,
                                    Duration.ofSeconds(10)))
                    .join();

            assertThat(read.batches()).hasSize(2).allSatisfy(batch ->
                    assertThat(batch.payload()).containsExactly(payload));
            assertThat(read.sliceStats()).singleElement().satisfies(stats -> {
                assertThat(stats.fullSlicePayloadBytes())
                        .isEqualTo(written.objectLength());
                assertThat(stats.entryIndexBytes())
                        .isEqualTo(written.entryIndexRef().length());
                assertThat(stats.returnedPayloadBytes())
                        .isEqualTo(payload.length * 2L);
                assertThat(stats.physicalBytesRead())
                        .isLessThan(stats.returnedPayloadBytes());
                assertThat(stats.amplificationBytes()).isZero();
                assertThat(stats.compressionSavingsBytes())
                        .isEqualTo(stats.returnedPayloadBytes()
                                - stats.physicalBytesRead());
            });
        }
    }

    private StagingFileManager staging() throws Exception {
        Path directory = Files.createDirectory(temporaryDirectory.resolve("staging"));
        Files.setPosixFilePermissions(directory, PosixFilePermissions.fromString("rwx------"));
        return new StagingFileManager(
                directory,
                32L << 20,
                StagingFileManager.MIN_UPLOAD_CHUNK_BYTES,
                Duration.ofHours(1),
                Runnable::run);
    }

    private static CompactedObjectWriteRequest request(StreamId streamId, long logicalBytes) {
        return new CompactedObjectWriteRequest(
                "test-cluster",
                ReadView.COMMITTED,
                streamId,
                new OffsetRange(40, 44),
                "c".repeat(26),
                new com.nereusstream.api.Checksum(
                        com.nereusstream.api.ChecksumType.SHA256, "1".repeat(64)),
                new com.nereusstream.api.Checksum(
                        com.nereusstream.api.ChecksumType.SHA256, "2".repeat(64)),
                PayloadFormat.PULSAR_ENTRY_BATCH,
                PayloadFormat.PULSAR_ENTRY_BATCH.name(),
                Optional.of(new com.nereusstream.api.Checksum(
                        com.nereusstream.api.ChecksumType.SHA256, "3".repeat(64))),
                4,
                4,
                4,
                logicalBytes,
                List.of(),
                100,
                100 + logicalBytes,
                2,
                "ZSTD",
                "nereus-core-test",
                Optional.empty());
    }

    private static CompactedObjectRow row(long offset, byte[] payload) {
        return new CompactedObjectRow(
                offset,
                ByteBuffer.wrap(payload),
                Crc32cChecksums.intValue(Crc32cChecksums.checksum(payload)),
                OptionalLong.empty(),
                OptionalLong.empty(),
                Optional.empty(),
                Optional.empty(),
                Optional.empty(),
                OptionalLong.empty(),
                OptionalLong.empty(),
                OptionalInt.empty(),
                OptionalInt.empty(),
                Optional.empty());
    }

    private static Flow.Publisher<CompactedObjectRow> publisher(List<CompactedObjectRow> rows) {
        return subscriber -> subscriber.onSubscribe(new Flow.Subscription() {
            private int index;
            private boolean complete;

            @Override
            public void request(long count) {
                if (complete) {
                    return;
                }
                subscriber.onNext(rows.get(index++));
                if (index == rows.size()) {
                    complete = true;
                    subscriber.onComplete();
                }
            }

            @Override
            public void cancel() {
                complete = true;
            }
        });
    }

    private static byte[] utf8(String value) {
        return value.getBytes(StandardCharsets.UTF_8);
    }

    private static NereusException findNereus(Throwable supplied) {
        Throwable current = supplied;
        while (current != null && !(current instanceof NereusException)) {
            current = current.getCause();
        }
        assertThat(current).isInstanceOf(NereusException.class);
        return (NereusException) current;
    }
}
