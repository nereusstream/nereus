/* Licensed under the Apache License, Version 2.0 */
package com.nereusstream.objectstore.compacted;

import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.assertThatThrownBy;

import com.nereusstream.api.Checksum;
import com.nereusstream.api.ChecksumType;
import com.nereusstream.api.ErrorCode;
import com.nereusstream.api.FirstEntryPolicy;
import com.nereusstream.api.NereusException;
import com.nereusstream.api.ObjectType;
import com.nereusstream.api.OffsetRange;
import com.nereusstream.api.PayloadFormat;
import com.nereusstream.api.ReadBoundaryMode;
import com.nereusstream.api.ReadView;
import com.nereusstream.api.StreamId;
import com.nereusstream.api.target.ObjectSliceReadTarget;
import com.nereusstream.objectstore.Crc32cChecksums;
import com.nereusstream.objectstore.PutObjectOptions;
import com.nereusstream.objectstore.staging.StagingFileManager;
import com.nereusstream.objectstore.testing.LocalFileObjectStore;
import java.nio.ByteBuffer;
import java.nio.file.Path;
import java.time.Duration;
import java.util.HexFormat;
import java.util.List;
import java.util.Map;
import java.util.OptionalLong;
import java.util.concurrent.Flow;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.io.TempDir;

class Ncp2Ntc2GoldenAndCorruptionTest {
    @TempDir
    Path temporaryDirectory;

    @Test
    void ncp2RoundTripsMixedRangesWithExactBoundariesAndFrozenBytes() throws Exception {
        try (StagingFileManager staging = CompactedParquetTestSupport.staging(temporaryDirectory, 64L << 20);
                LocalFileObjectStore store = new LocalFileObjectStore(temporaryDirectory.resolve("objects"))) {
            RangedCompactedObjectWriteRequest request = ncp2Request();
            List<RangedCompactedObjectRow> rows = List.of(
                    ncp2Row(10, 3, 0, "abc"),
                    ncp2Row(13, 1, 1, "de"),
                    ncp2Row(14, 2, 2, "f"));
            ParquetRangedCompactedObjectWriter writer =
                    new ParquetRangedCompactedObjectWriter(staging, Runnable::run);
            ParquetRangedCompactedObjectReader reader =
                    new ParquetRangedCompactedObjectReader(store, Runnable::run);
            RangedCompactedObjectVerifier verifier = new RangedCompactedObjectVerifier(
                    store, reader, new ParquetKafkaTopicCompactedReader(store, Runnable::run));
            try (RangedCompactedObjectWriteResult written = writer.write(request, publisher(rows)).join()) {
                upload(store, written);
                ObjectSliceReadTarget target = target(request, written);
                RangedCompactedObjectVerificationRequest verification =
                        RangedCompactedObjectVerificationRequest.from(
                                request, written, Duration.ofSeconds(20));
                verifier.verifyExact(verification, request).join();
                assertThat(written.contentSha256().value())
                        .isEqualTo("671ac184f5b1fbf898329cd868f88d53a569e229cfeb451ebdb4c618b5591532");
                assertThat(CompactedObjectFormatV2.COMMITTED_SCHEMA.toString()).isEqualTo("""
                        message nereus_committed_generation_v2 {
                          required int64 stream_offset_start;
                          required int32 record_count;
                          required int32 entry_ordinal;
                          required binary payload;
                          required int32 payload_crc32c;
                          optional int64 event_time_millis;
                        }
                        """);

                RangedCompactedObjectReadResult full = reader.read(readRequest(
                                request, target, 10, ReadBoundaryMode.EXACT_START,
                                FirstEntryPolicy.LEGACY_STRICT_LIMIT, 6, 6))
                        .join();
                assertThat(full.rows()).extracting(RangedCompactedObjectRow::recordCount)
                        .containsExactly(3, 1, 2);
                assertThat(full.sourceCoverageEndOffset()).isEqualTo(16);

                assertThatThrownBy(() -> reader.read(readRequest(
                                request, target, 11, ReadBoundaryMode.EXACT_START,
                                FirstEntryPolicy.ALLOW_FIRST_ENTRY_OVERFLOW, 1, 1)).join())
                        .hasCauseInstanceOf(NereusException.class)
                        .cause()
                        .extracting(error -> ((NereusException) error).code())
                        .isEqualTo(ErrorCode.OFFSET_NOT_AVAILABLE);

                RangedCompactedObjectReadResult containing = reader.read(readRequest(
                                request, target, 11, ReadBoundaryMode.CONTAINING_ENTRY,
                                FirstEntryPolicy.ALLOW_FIRST_ENTRY_OVERFLOW, 1, 1))
                        .join();
                assertThat(containing.rows()).hasSize(1);
                assertThat(containing.rows().get(0).streamOffsetStart()).isEqualTo(10);
                assertThat(containing.sourceCoverageEndOffset()).isEqualTo(13);

                assertThatThrownBy(() -> reader.read(readRequest(
                                request, target, 10, ReadBoundaryMode.EXACT_START,
                                FirstEntryPolicy.LEGACY_STRICT_LIMIT, 2, 2)).join())
                        .hasCauseInstanceOf(NereusException.class)
                        .cause()
                        .extracting(error -> ((NereusException) error).code())
                        .isEqualTo(ErrorCode.READ_LIMIT_TOO_SMALL);
            }
        }
    }

    @Test
    void ntc2RoundTripsSparseKeyTagsTombstoneAndControlWithFrozenBytes() throws Exception {
        try (StagingFileManager staging = CompactedParquetTestSupport.staging(temporaryDirectory, 64L << 20);
                LocalFileObjectStore store = new LocalFileObjectStore(temporaryDirectory.resolve("objects"))) {
            KafkaTopicCompactedObjectWriteRequest request = ntc2Request();
            List<KafkaTopicCompactedObjectRow> rows = List.of(
                    ntc2Row(20, KafkaCompactionDispositionV2.RETAIN_VALUE,
                            KafkaCompactionKeyEncodingV2.keyed(ByteBuffer.allocate(0)), "a", 20, 0),
                    ntc2Row(23, KafkaCompactionDispositionV2.RETAIN_UNKEYED,
                            KafkaCompactionKeyEncodingV2.nullKey(23), "b", 22, 1),
                    ntc2Row(27, KafkaCompactionDispositionV2.RETAIN_TOMBSTONE,
                            KafkaCompactionKeyEncodingV2.keyed(ByteBuffer.wrap(new byte[] {1})), "", 25, 2),
                    ntc2Row(29, KafkaCompactionDispositionV2.RETAIN_CONTROL,
                            KafkaCompactionKeyEncodingV2.control(29), "c", 29, 0));
            ParquetKafkaTopicCompactedWriter writer =
                    new ParquetKafkaTopicCompactedWriter(staging, Runnable::run);
            ParquetKafkaTopicCompactedReader reader =
                    new ParquetKafkaTopicCompactedReader(store, Runnable::run);
            RangedCompactedObjectVerifier verifier = new RangedCompactedObjectVerifier(
                    store, new ParquetRangedCompactedObjectReader(store, Runnable::run), reader);
            try (RangedCompactedObjectWriteResult written = writer.write(request, publisher(rows)).join()) {
                upload(store, written);
                ObjectSliceReadTarget target = target(request, written);
                verifier.verifyExact(
                                RangedCompactedObjectVerificationRequest.from(
                                        request, written, Duration.ofSeconds(20)),
                                request)
                        .join();
                assertThat(written.contentSha256().value())
                        .isEqualTo("367da6663bb4e8d6e83e942277b3a250b86ec13f4f4a5863235aed32157bd2e8");
                KafkaTopicCompactedObjectReadResult read = reader.read(new KafkaTopicCompactedObjectReadRequest(
                                request.streamId(), request.sourceCoverage(), 21, target,
                                ReadBoundaryMode.EXACT_START, FirstEntryPolicy.ALLOW_FIRST_ENTRY_OVERFLOW,
                                10, 10, Duration.ofSeconds(20)))
                        .join();
                assertThat(read.rows()).extracting(KafkaTopicCompactedObjectRow::streamOffsetStart)
                        .containsExactly(23L, 27L, 29L);
                assertThat(read.rows()).extracting(KafkaTopicCompactedObjectRow::disposition)
                        .containsExactly(
                                KafkaCompactionDispositionV2.RETAIN_UNKEYED,
                                KafkaCompactionDispositionV2.RETAIN_TOMBSTONE,
                                KafkaCompactionDispositionV2.RETAIN_CONTROL);
                assertThat(read.sourceCoverageEndOffset()).isEqualTo(30);
            }
        }
    }

    @Test
    void exactMetadataRegistriesRejectUnknownNonCanonicalAndCrossVersionValues() {
        Map<String, String> ncp2 = CompactedObjectFormatV2.metadata(ncp2Request());
        assertThat(CompactedObjectFormatV2.parseMetadata(ncp2).view()).isEqualTo(ReadView.COMMITTED);

        Map<String, String> unknown = new java.util.HashMap<>(ncp2);
        unknown.put("nereus.future.field", "1");
        assertThatThrownBy(() -> CompactedObjectFormatV2.parseMetadata(unknown))
                .isInstanceOf(CompactedObjectFormatException.class)
                .hasMessageContaining("unknown");

        Map<String, String> nonCanonical = new java.util.HashMap<>(ncp2);
        nonCanonical.put("nereus.entry.count", "03");
        assertThatThrownBy(() -> CompactedObjectFormatV2.parseMetadata(nonCanonical))
                .isInstanceOf(CompactedObjectFormatException.class)
                .hasMessageContaining("canonical");

        Map<String, String> v1 = new java.util.HashMap<>(ncp2);
        v1.put("nereus.format", CompactedObjectFormatV1.COMMITTED_FORMAT_ID);
        v1.put("nereus.format.version", "1");
        assertThatThrownBy(() -> CompactedObjectFormatV2.parseMetadata(v1))
                .isInstanceOf(CompactedObjectFormatException.class);
    }

    @Test
    void kck2AndDispositionWireValuesAreClosed() {
        assertThat(bytes(KafkaCompactionKeyEncodingV2.keyed(ByteBuffer.allocate(0))))
                .containsExactly(0x01);
        assertThat(HexFormat.of().formatHex(bytes(KafkaCompactionKeyEncodingV2.nullKey(9))))
                .isEqualTo("020000000000000009");
        assertThat(HexFormat.of().formatHex(bytes(KafkaCompactionKeyEncodingV2.control(9))))
                .isEqualTo("030000000000000009");
        assertThatThrownBy(() -> KafkaCompactionDispositionV2.fromWireId(5))
                .isInstanceOf(CompactedObjectFormatException.class);
        assertThatThrownBy(() -> KafkaCompactionKeyEncodingV2.validateForRow(
                        KafkaCompactionKeyEncodingV2.nullKey(10),
                        9,
                        KafkaCompactionDispositionV2.RETAIN_UNKEYED))
                .isInstanceOf(CompactedObjectFormatException.class);
    }

    @Test
    void writersRejectBadCrcDenseGapsAndSparseReorderingBeforePublication() throws Exception {
        try (StagingFileManager staging = CompactedParquetTestSupport.staging(temporaryDirectory, 64L << 20)) {
            ParquetRangedCompactedObjectWriter ncp2Writer =
                    new ParquetRangedCompactedObjectWriter(staging, Runnable::run);
            RangedCompactedObjectRow badCrc = new RangedCompactedObjectRow(
                    10,
                    3,
                    0,
                    ByteBuffer.wrap(new byte[] {1}),
                    0,
                    OptionalLong.empty());
            assertFormatFailure(() -> ncp2Writer.write(ncp2Request(), publisher(List.of(badCrc))).join());

            List<RangedCompactedObjectRow> denseGap = List.of(
                    ncp2Row(10, 3, 0, "abc"),
                    ncp2Row(14, 1, 1, "de"),
                    ncp2Row(15, 1, 2, "f"));
            assertFormatFailure(() -> ncp2Writer.write(ncp2Request(), publisher(denseGap)).join());

            ParquetKafkaTopicCompactedWriter ntc2Writer =
                    new ParquetKafkaTopicCompactedWriter(staging, Runnable::run);
            List<KafkaTopicCompactedObjectRow> reordered = List.of(
                    ntc2Row(23, KafkaCompactionDispositionV2.RETAIN_UNKEYED,
                            KafkaCompactionKeyEncodingV2.nullKey(23), "a", 23, 0),
                    ntc2Row(20, KafkaCompactionDispositionV2.RETAIN_VALUE,
                            KafkaCompactionKeyEncodingV2.keyed(ByteBuffer.wrap(new byte[] {1})),
                            "b", 20, 0));
            assertFormatFailure(() -> ntc2Writer.write(ntc2Request(), publisher(reordered)).join());
        }
    }

    @Test
    void verifierRejectsWrongFrozenContentDigest() throws Exception {
        try (StagingFileManager staging = CompactedParquetTestSupport.staging(temporaryDirectory, 64L << 20);
                LocalFileObjectStore store = new LocalFileObjectStore(temporaryDirectory.resolve("verify-objects"))) {
            RangedCompactedObjectWriteRequest request = ncp2Request();
            ParquetRangedCompactedObjectWriter writer =
                    new ParquetRangedCompactedObjectWriter(staging, Runnable::run);
            ParquetRangedCompactedObjectReader reader =
                    new ParquetRangedCompactedObjectReader(store, Runnable::run);
            RangedCompactedObjectVerifier verifier = new RangedCompactedObjectVerifier(
                    store, reader, new ParquetKafkaTopicCompactedReader(store, Runnable::run));
            List<RangedCompactedObjectRow> rows = List.of(
                    ncp2Row(10, 3, 0, "abc"),
                    ncp2Row(13, 1, 1, "de"),
                    ncp2Row(14, 2, 2, "f"));
            try (RangedCompactedObjectWriteResult written = writer.write(request, publisher(rows)).join()) {
                upload(store, written);
                RangedCompactedObjectVerificationRequest valid =
                        RangedCompactedObjectVerificationRequest.from(
                                request, written, Duration.ofSeconds(20));
                RangedCompactedObjectVerificationRequest wrongDigest =
                        new RangedCompactedObjectVerificationRequest(
                                valid.streamId(),
                                valid.view(),
                                valid.sourceCoverage(),
                                valid.target(),
                                valid.payloadFormat(),
                                valid.storageCrc32c(),
                                sha256('f'),
                                valid.timeout());

                assertFormatFailure(() -> verifier.verify(wrongDigest).join());
            }
        }
    }

    private static RangedCompactedObjectWriteRequest ncp2Request() {
        return new RangedCompactedObjectWriteRequest(
                "test-cluster", new StreamId("s-ncp2"), new OffsetRange(10, 16), "a".repeat(26),
                sha256('1'), sha256('2'), PayloadFormat.KAFKA_RECORD_BATCH,
                CompactedObjectFormatV2.KAFKA_LOGICAL_FORMAT, 6, 3, 6, 106, 2,
                "UNCOMPRESSED", "nereus-test-build");
    }

    private static KafkaTopicCompactedObjectWriteRequest ntc2Request() {
        return new KafkaTopicCompactedObjectWriteRequest(
                "test-cluster", new StreamId("s-ntc2"), new OffsetRange(20, 30), "b".repeat(26),
                sha256('3'), sha256('4'), 4, 4, 3, 203, 2, "UNCOMPRESSED", "nereus-test-build",
                new KafkaTopicCompactedFormatSpecV2(
                        "kafka-latest-key", 1, CompactedObjectFormatV2.KAFKA_KEY_CODEC,
                        CompactedObjectFormatV2.KAFKA_REWRITE_CODEC, sha256('5'), 4, 4));
    }

    private static RangedCompactedObjectRow ncp2Row(
            long offset, int records, int ordinal, String payload) {
        byte[] bytes = payload.getBytes(java.nio.charset.StandardCharsets.UTF_8);
        return new RangedCompactedObjectRow(
                offset, records, ordinal, ByteBuffer.wrap(bytes), crc(bytes), OptionalLong.empty());
    }

    private static KafkaTopicCompactedObjectRow ntc2Row(
            long offset,
            KafkaCompactionDispositionV2 disposition,
            ByteBuffer key,
            String payload,
            long sourceBase,
            int sourceIndex) {
        byte[] bytes = payload.getBytes(java.nio.charset.StandardCharsets.UTF_8);
        return new KafkaTopicCompactedObjectRow(
                offset, 1, disposition, key, ByteBuffer.wrap(bytes), crc(bytes), sourceBase, sourceIndex,
                sha256((char) ('a' + sourceIndex)), OptionalLong.empty());
    }

    private static RangedCompactedObjectReadRequest readRequest(
            RangedCompactedObjectWriteRequest request,
            ObjectSliceReadTarget target,
            long offset,
            ReadBoundaryMode boundary,
            FirstEntryPolicy firstPolicy,
            int maxRecords,
            int maxBytes) {
        return new RangedCompactedObjectReadRequest(
                request.streamId(), request.sourceCoverage(), offset, target, request.payloadFormat(),
                boundary, firstPolicy, maxRecords, maxBytes, Duration.ofSeconds(20));
    }

    private static ObjectSliceReadTarget target(
            RangedCompactedObjectWriteRequest request,
            RangedCompactedObjectWriteResult result) {
        return target(request.streamId(), request.sourceCoverage(), request.logicalFormat(), result);
    }

    private static ObjectSliceReadTarget target(
            KafkaTopicCompactedObjectWriteRequest request,
            RangedCompactedObjectWriteResult result) {
        return target(
                request.streamId(), request.sourceCoverage(), CompactedObjectFormatV2.KAFKA_LOGICAL_FORMAT, result);
    }

    private static ObjectSliceReadTarget target(
            StreamId streamId,
            OffsetRange coverage,
            String logicalFormat,
            RangedCompactedObjectWriteResult result) {
        return new ObjectSliceReadTarget(
                1, result.objectId(), result.objectKey(), ObjectType.STREAM_COMPACTED_OBJECT,
                result.physicalFormat(), logicalFormat,
                coverage.startOffset() + "-" + coverage.endOffset(), 0, result.objectLength(),
                result.storageCrc32c(), result.entryIndexRef());
    }

    private static void upload(LocalFileObjectStore store, RangedCompactedObjectWriteResult result) {
        store.putObject(
                        result.objectKey(),
                        result.stagingFile(),
                        new PutObjectOptions(
                                "application/vnd.apache.parquet", result.storageCrc32c(), true,
                                Map.of(), Duration.ofSeconds(20)))
                .join();
    }

    private static <T> Flow.Publisher<T> publisher(List<T> values) {
        List<T> immutable = List.copyOf(values);
        return subscriber -> subscriber.onSubscribe(new Flow.Subscription() {
            private int index;
            private boolean done;

            @Override
            public void request(long count) {
                if (done) {
                    return;
                }
                if (count <= 0) {
                    done = true;
                    subscriber.onError(new IllegalArgumentException("non-positive demand"));
                    return;
                }
                long emitted = 0;
                while (!done && emitted < count && index < immutable.size()) {
                    subscriber.onNext(immutable.get(index++));
                    emitted++;
                }
                if (!done && index == immutable.size()) {
                    done = true;
                    subscriber.onComplete();
                }
            }

            @Override
            public void cancel() {
                done = true;
            }
        });
    }

    private static Checksum sha256(char value) {
        return new Checksum(ChecksumType.SHA256, Character.toString(value).repeat(64));
    }

    private static int crc(byte[] value) {
        return Crc32cChecksums.intValue(Crc32cChecksums.checksum(value));
    }

    private static byte[] bytes(ByteBuffer value) {
        ByteBuffer copy = value.asReadOnlyBuffer();
        byte[] bytes = new byte[copy.remaining()];
        copy.get(bytes);
        return bytes;
    }

    private static void assertFormatFailure(Runnable operation) {
        assertThatThrownBy(operation::run)
                .hasRootCauseInstanceOf(CompactedObjectFormatException.class);
    }
}
