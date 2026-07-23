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

package com.nereusstream.objectstore.wal;

import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.assertThatThrownBy;

import com.nereusstream.api.AppendBatch;
import com.nereusstream.api.AppendEntry;
import com.nereusstream.api.Checksum;
import com.nereusstream.api.ChecksumType;
import com.nereusstream.api.EntryIndexLocation;
import com.nereusstream.api.EntryIndexRef;
import com.nereusstream.api.ErrorCode;
import com.nereusstream.api.FirstEntryPolicy;
import com.nereusstream.api.NereusException;
import com.nereusstream.api.ObjectId;
import com.nereusstream.api.ObjectKey;
import com.nereusstream.api.ObjectType;
import com.nereusstream.api.OffsetRange;
import com.nereusstream.api.PayloadFormat;
import com.nereusstream.api.ReadBatch;
import com.nereusstream.api.ReadBoundaryMode;
import com.nereusstream.api.ReadIsolation;
import com.nereusstream.api.ReadOptions;
import com.nereusstream.api.ReadRequest;
import com.nereusstream.api.ReadView;
import com.nereusstream.api.ResolvedObjectRange;
import com.nereusstream.api.SchemaRef;
import com.nereusstream.api.StreamId;
import com.nereusstream.objectstore.Crc32cChecksums;
import com.nereusstream.objectstore.HeadObjectOptions;
import com.nereusstream.objectstore.ObjectStore;
import com.nereusstream.objectstore.PutObjectOptions;
import com.nereusstream.objectstore.PutObjectResult;
import com.nereusstream.objectstore.RangeReadOptions;
import com.nereusstream.objectstore.RangeReadResult;
import com.nereusstream.objectstore.testing.LocalFileObjectStore;
import java.nio.ByteBuffer;
import java.nio.ByteOrder;
import java.nio.charset.StandardCharsets;
import java.nio.file.Path;
import java.time.Clock;
import java.time.Duration;
import java.time.Instant;
import java.time.ZoneOffset;
import java.util.ArrayList;
import java.util.HexFormat;
import java.util.List;
import java.util.Map;
import java.util.Optional;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.CompletionException;
import java.util.concurrent.atomic.AtomicInteger;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.io.TempDir;

class WalObjectWriterReaderTest {
    private static final Clock FIXED_CLOCK = Clock.fixed(Instant.parse("2026-07-08T01:02:03Z"), ZoneOffset.UTC);
    private static final String RUN_HASH = "runhash";

    @TempDir
    Path root;

    @Test
    void writeOneSliceWalObjectAndReadItBack() {
        LocalFileObjectStore store = new LocalFileObjectStore(root);
        WalWriteResult result = writer(store).write(new WalWriteRequest(
                "tenant/ns",
                "writer",
                RUN_HASH,
                7,
                List.of(new WalStreamSliceInput(new StreamId("stream-a"), batch("a", "b"))),
                options(false, 1 << 20)))
                .join();

        assertThat(result.objectKey().value()).contains("/wal/2026/07/08/");
        assertThat(result.objectChecksum()).isNotEqualTo(result.storageChecksum());
        WalObjectLayout.DecodedObject decoded = decodeStoredObject(store, result);
        assertThat(decoded.objectId()).isEqualTo(result.objectId());
        assertThat(decoded.slices()).hasSize(1);

        WalReadResult readResult = reader(store).readWithStats(
                0,
                List.of(resolved(result.slices().get(0), result, 0)),
                readOptions(10, 100))
                .join();
        List<ReadBatch> batches = readResult.batches();

        assertThat(batches).hasSize(2);
        assertThat(new String(batches.get(0).payload(), StandardCharsets.UTF_8)).isEqualTo("a");
        assertThat(new String(batches.get(1).payload(), StandardCharsets.UTF_8)).isEqualTo("b");
        assertThat(batches.get(0).range()).isEqualTo(new OffsetRange(0, 1));
        assertThat(batches.get(1).range()).isEqualTo(new OffsetRange(1, 2));
        assertThat(batches.get(0).sourceObjectOffset()).isEqualTo(result.slices().get(0).objectOffset());
        assertThat(batches.get(0).sourceObjectLength()).isEqualTo(1);
        assertThat(batches.get(1).sourceObjectOffset()).isEqualTo(result.slices().get(0).objectOffset() + 1);
        assertThat(batches.get(1).sourceObjectLength()).isEqualTo(1);
        assertThat(readResult.sliceStats()).singleElement().satisfies(stats -> {
            assertThat(stats.objectId()).isEqualTo(result.objectId());
            assertThat(stats.objectOffset()).isEqualTo(result.slices().getFirst().objectOffset());
            assertThat(stats.fullSlicePayloadBytes()).isEqualTo(2);
            assertThat(stats.entryIndexBytes()).isPositive();
            assertThat(stats.returnedPayloadBytes()).isEqualTo(2);
            assertThat(stats.amplificationBytes()).isEqualTo(stats.entryIndexBytes());
        });
    }

    @Test
    void rangedKafkaEntriesHonorExactContainingAndFirstOverflowPolicies() {
        LocalFileObjectStore store = new LocalFileObjectStore(root);
        WalWriteResult result = writer(store).write(new WalWriteRequest(
                "cluster",
                "writer",
                RUN_HASH,
                7,
                List.of(new WalStreamSliceInput(
                        new StreamId("stream-a"),
                        rangedKafkaBatch(List.of("aaa", "bb"), List.of(3, 2)))),
                options(false, 1 << 20))).join();
        ResolvedObjectRange range = resolved(result.slices().get(0), result, 10);

        WalReadResult exact = reader(store).readWithStats(
                request(10, ReadBoundaryMode.EXACT_START,
                        FirstEntryPolicy.LEGACY_STRICT_LIMIT, 10, 100),
                List.of(range)).join();
        assertThat(exact.batches()).extracting(ReadBatch::range)
                .containsExactly(new OffsetRange(10, 13), new OffsetRange(13, 15));
        assertThat(exact.batches()).extracting(ReadBatch::payloadFormat)
                .containsOnly(PayloadFormat.KAFKA_RECORD_BATCH);

        assertCode(() -> reader(store).readWithStats(
                        request(11, ReadBoundaryMode.EXACT_START,
                                FirstEntryPolicy.LEGACY_STRICT_LIMIT, 10, 100),
                        List.of(range)).join(),
                ErrorCode.OFFSET_NOT_AVAILABLE);

        WalReadResult containingFirst = reader(store).readWithStats(
                request(11, ReadBoundaryMode.CONTAINING_ENTRY,
                        FirstEntryPolicy.LEGACY_STRICT_LIMIT, 10, 100),
                List.of(range)).join();
        assertThat(containingFirst.batches()).extracting(ReadBatch::range)
                .containsExactly(new OffsetRange(10, 13), new OffsetRange(13, 15));
        WalReadResult containingLast = reader(store).readWithStats(
                request(14, ReadBoundaryMode.CONTAINING_ENTRY,
                        FirstEntryPolicy.LEGACY_STRICT_LIMIT, 10, 100),
                List.of(range)).join();
        assertThat(containingLast.batches()).extracting(ReadBatch::range)
                .containsExactly(new OffsetRange(13, 15));

        WalReadResult overflow = reader(store).readWithStats(
                request(11, ReadBoundaryMode.CONTAINING_ENTRY,
                        FirstEntryPolicy.ALLOW_FIRST_ENTRY_OVERFLOW, 1, 1),
                List.of(range)).join();
        assertThat(overflow.batches()).singleElement().satisfies(batch -> {
            assertThat(batch.range()).isEqualTo(new OffsetRange(10, 13));
            assertThat(new String(batch.payload(), StandardCharsets.UTF_8)).isEqualTo("aaa");
        });

        WalReadResult strictRecordLimit = reader(store).readWithStats(
                request(10, ReadBoundaryMode.EXACT_START,
                        FirstEntryPolicy.LEGACY_STRICT_LIMIT, 1, 100),
                List.of(range)).join();
        assertThat(strictRecordLimit.batches()).isEmpty();
        assertCode(() -> reader(store).readWithStats(
                        request(10, ReadBoundaryMode.EXACT_START,
                                FirstEntryPolicy.LEGACY_STRICT_LIMIT, 10, 1),
                        List.of(range)).join(),
                ErrorCode.READ_LIMIT_TOO_SMALL);
    }

    @Test
    void writeMultiSliceWalObjectAndReadEachSliceByRange() {
        LocalFileObjectStore store = new LocalFileObjectStore(root);
        WalWriteResult result = writer(store).write(new WalWriteRequest(
                "cluster",
                "writer",
                RUN_HASH,
                7,
                List.of(
                        new WalStreamSliceInput(new StreamId("stream-b"), batch("b1", "b2")),
                        new WalStreamSliceInput(new StreamId("stream-a"), batch("a1", "a2"))),
                options(false, 1 << 20)))
                .join();

        assertThat(result.slices()).extracting(slice -> slice.streamId().value())
                .containsExactly("stream-a", "stream-b");
        List<ReadBatch> streamA = reader(store).read(
                10,
                List.of(resolved(result.slices().get(0), result, 10)),
                readOptions(10, 100))
                .join();
        List<ReadBatch> streamB = reader(store).read(
                20,
                List.of(resolved(result.slices().get(1), result, 20)),
                readOptions(10, 100))
                .join();

        assertThat(streamA).extracting(batch -> new String(batch.payload(), StandardCharsets.UTF_8))
                .containsExactly("a1", "a2");
        assertThat(streamB).extracting(batch -> new String(batch.payload(), StandardCharsets.UTF_8))
                .containsExactly("b1", "b2");
    }

    @Test
    void sizingFailureAndForceSingleStreamGuardHappenBeforeUpload() {
        CountingObjectStore store = new CountingObjectStore(new LocalFileObjectStore(root));
        WalObjectWriter writer = writer(store);

        assertCode(() -> writer.write(new WalWriteRequest(
                        "cluster",
                        "writer",
                        RUN_HASH,
                        7,
                        List.of(new WalStreamSliceInput(new StreamId("stream-a"), batch("payload"))),
                        options(false, 64)))
                .join(), ErrorCode.INVALID_ARGUMENT);
        assertCode(() -> writer.write(new WalWriteRequest(
                        "cluster",
                        "writer",
                        RUN_HASH,
                        7,
                        List.of(
                                new WalStreamSliceInput(new StreamId("stream-a"), batch("a")),
                                new WalStreamSliceInput(new StreamId("stream-b"), batch("b"))),
                        options(true, 1 << 20)))
                .join(), ErrorCode.INVALID_ARGUMENT);
        assertCode(() -> writer.write(new WalWriteRequest(
                        "cluster",
                        "writer",
                        RUN_HASH,
                        7,
                        List.of(new WalStreamSliceInput(new StreamId("stream-a"), batch("payload"))),
                        new WalWriteOptions(
                                CompressionType.ZSTD,
                                1024,
                                1 << 20,
                                Duration.ofSeconds(1),
                                false)))
                .join(), ErrorCode.UNSUPPORTED_FORMAT);

        assertThat(store.puts()).isZero();
    }

    @Test
    void uploadTimeoutPropagatesAndTargetObjectSizeIsAdvisory() {
        RecordingObjectStore store = new RecordingObjectStore(new LocalFileObjectStore(root));
        Duration timeout = Duration.ofMillis(1234);

        WalWriteResult result = writer(store).write(new WalWriteRequest(
                "cluster",
                "writer",
                RUN_HASH,
                7,
                List.of(new WalStreamSliceInput(new StreamId("stream-a"), batch("payload"))),
                new WalWriteOptions(
                        CompressionType.NONE,
                        1,
                        1 << 20,
                        timeout,
                        false)))
                .join();

        assertThat(result.objectLength()).isGreaterThan(1);
        assertThat(store.lastPutOptions().timeout()).isEqualTo(timeout);
    }

    @Test
    void payloadAndEntryIndexReadsShareOneDecreasingDeadline() {
        LocalFileObjectStore local = new LocalFileObjectStore(root);
        WalWriteResult result = writer(local).write(new WalWriteRequest(
                "cluster",
                "writer",
                RUN_HASH,
                7,
                List.of(new WalStreamSliceInput(new StreamId("stream-a"), batch("a"))),
                options(false, 1 << 20)))
                .join();
        TimeoutRecordingObjectStore recording = new TimeoutRecordingObjectStore(local);

        new DefaultWalObjectReader(recording).read(
                0,
                List.of(resolved(result.slices().getFirst(), result, 0)),
                new ReadOptions(10, 100, ReadIsolation.COMMITTED, Duration.ofSeconds(1)))
                .join();

        assertThat(recording.timeouts()).hasSize(2);
        assertThat(recording.timeouts().get(1)).isLessThan(recording.timeouts().get(0));
    }

    @Test
    void storageChecksumMismatchFailsBeforeMetadataCommit() {
        ObjectStore store = new WrongChecksumObjectStore(new LocalFileObjectStore(root));

        assertCode(() -> writer(store).write(new WalWriteRequest(
                        "cluster",
                        "writer",
                        RUN_HASH,
                        7,
                        List.of(new WalStreamSliceInput(new StreamId("stream-a"), batch("payload"))),
                        options(false, 1 << 20)))
                .join(), ErrorCode.OBJECT_CHECKSUM_MISMATCH);
    }

    @Test
    void corruptedSliceOrEntryIndexChecksumFailsRead() {
        LocalFileObjectStore store = new LocalFileObjectStore(root);
        WalWriteResult result = writer(store).write(new WalWriteRequest(
                "cluster",
                "writer",
                RUN_HASH,
                7,
                List.of(new WalStreamSliceInput(new StreamId("stream-a"), batch("a", "b"))),
                options(false, 1 << 20)))
                .join();
        WrittenStreamSlice slice = result.slices().get(0);
        ResolvedObjectRange corrupted = new ResolvedObjectRange(
                new OffsetRange(0, slice.recordCount()),
                0,
                result.objectId(),
                result.objectKey(),
                ObjectType.MULTI_STREAM_WAL_OBJECT,
                slice.objectOffset(),
                slice.objectLength(),
                new Checksum(ChecksumType.CRC32C, "00000000"),
                slice.payloadFormat(),
                slice.schemaRefs(),
                slice.entryIndexRef(),
                Optional.empty(),
                1);

        assertCode(() -> reader(store).read(0, List.of(corrupted), readOptions(10, 100)).join(),
                ErrorCode.OBJECT_CHECKSUM_MISMATCH);
    }

    @Test
    void readLimitStopsAfterReturnedDataButStillAllowsZeroByteEntries() {
        LocalFileObjectStore store = new LocalFileObjectStore(root);
        DefaultWalObjectWriter writer = writer(store);
        WalWriteResult one = writer.write(new WalWriteRequest(
                "cluster",
                "writer",
                RUN_HASH,
                7,
                List.of(new WalStreamSliceInput(new StreamId("stream-a"), batch("a"))),
                options(false, 1 << 20)))
                .join();
        WalWriteResult positiveAfterBudget = writer.write(new WalWriteRequest(
                "cluster",
                "writer",
                RUN_HASH,
                7,
                List.of(new WalStreamSliceInput(new StreamId("stream-a"), batch("b"))),
                options(false, 1 << 20)))
                .join();
        WalWriteResult zeroAfterBudget = writer.write(new WalWriteRequest(
                "cluster",
                "writer",
                RUN_HASH,
                7,
                List.of(new WalStreamSliceInput(new StreamId("stream-a"), batch(""))),
                options(false, 1 << 20)))
                .join();

        List<ReadBatch> stoppedBeforePositive = reader(store).read(
                0,
                List.of(
                        resolved(one.slices().get(0), one, 0),
                        resolved(positiveAfterBudget.slices().get(0), positiveAfterBudget, 1)),
                readOptions(10, 1))
                .join();

        assertThat(stoppedBeforePositive).hasSize(1);
        assertThat(new String(stoppedBeforePositive.get(0).payload(), StandardCharsets.UTF_8)).isEqualTo("a");

        List<ReadBatch> includesZeroByte = reader(store).read(
                0,
                List.of(
                        resolved(one.slices().get(0), one, 0),
                        resolved(zeroAfterBudget.slices().get(0), zeroAfterBudget, 1)),
                readOptions(10, 1))
                .join();

        assertThat(includesZeroByte).hasSize(2);
        assertThat(includesZeroByte.get(0).range()).isEqualTo(new OffsetRange(0, 1));
        assertThat(includesZeroByte.get(0).payload()).isEqualTo("a".getBytes(StandardCharsets.UTF_8));
        assertThat(includesZeroByte.get(1).range()).isEqualTo(new OffsetRange(1, 2));
        assertThat(includesZeroByte.get(1).payload()).isEmpty();
    }

    @Test
    void readZeroByteEntryAfterExactByteBudgetWithinSameSlice() {
        LocalFileObjectStore store = new LocalFileObjectStore(root);
        WalWriteResult result = writer(store).write(new WalWriteRequest(
                "cluster",
                "writer",
                RUN_HASH,
                7,
                List.of(new WalStreamSliceInput(new StreamId("stream-a"), batch("a", "", "b"))),
                options(false, 1 << 20)))
                .join();

        List<ReadBatch> batches = reader(store).read(
                0,
                List.of(resolved(result.slices().get(0), result, 0)),
                readOptions(10, 1))
                .join();

        assertThat(batches).hasSize(2);
        assertThat(batches.get(0).payload()).isEqualTo("a".getBytes(StandardCharsets.UTF_8));
        assertThat(batches.get(1).payload()).isEmpty();
    }

    @Test
    void unsupportedEntryIndexLocationsFailBeforeObjectIo() {
        LocalFileObjectStore local = new LocalFileObjectStore(root);
        CountingObjectStore counting = new CountingObjectStore(local);
        WalWriteResult result = writer(counting).write(new WalWriteRequest(
                "cluster",
                "writer",
                RUN_HASH,
                7,
                List.of(new WalStreamSliceInput(new StreamId("stream-a"), batch("payload"))),
                options(false, 1 << 20)))
                .join();
        WrittenStreamSlice slice = result.slices().get(0);
        int readsBefore = counting.reads();
        EntryIndexRef inline = new EntryIndexRef(
                EntryIndexLocation.INLINE,
                Optional.empty(),
                Optional.empty(),
                Optional.of(new byte[] {1}),
                0,
                0,
                Crc32cChecksums.checksum(new byte[] {1}));
        EntryIndexRef indexObject = new EntryIndexRef(
                EntryIndexLocation.INDEX_OBJECT,
                Optional.of(new ObjectId("idx")),
                Optional.of(new ObjectKey("idx")),
                Optional.empty(),
                0,
                1,
                Crc32cChecksums.checksum(new byte[] {1}));

        assertCode(() -> reader(counting).read(
                        0,
                        List.of(resolved(slice, result, 0, inline)),
                        readOptions(10, 100))
                .join(), ErrorCode.UNSUPPORTED_FORMAT);
        assertCode(() -> reader(counting).read(
                        0,
                        List.of(resolved(slice, result, 0, indexObject)),
                        readOptions(10, 100))
                .join(), ErrorCode.UNSUPPORTED_FORMAT);
        assertThat(counting.reads()).isEqualTo(readsBefore);
    }

    @Test
    void entryIndexEncodingHasStableGoldenBytes() {
        EntryIndex index = new EntryIndex(
                1,
                1,
                List.of(new EntryIndexItem(0, 0, 1, 0, 3, 11, Map.of("k", "v"))));

        assertThat(HexFormat.of().formatHex(EntryIndexEncoder.encode(index)))
                .isEqualTo("010000000100000000000000000000000000000001000000000000000000000003000000000000000b0000000000000001000000010000006b0100000076");
    }

    @Test
    void entryIndexDecoderRejectsInvalidCountsAsCorruptFormat() {
        byte[] corruptCounts = ByteBuffer.allocate(Integer.BYTES * 2)
                .order(ByteOrder.LITTLE_ENDIAN)
                .putInt(-1)
                .putInt(1)
                .array();

        assertCode(() -> EntryIndexDecoder.decode(corruptCounts, 0, 0, Long.MAX_VALUE),
                ErrorCode.UNSUPPORTED_FORMAT);
    }

    @Test
    void entryIndexDecoderWrapsChecksumConsistentInvalidItemsAsUnsupportedFormat() {
        WalBinary.Writer writer = new WalBinary.Writer();
        writer.int32(1);
        writer.int32(1);
        writer.int32(0);
        writer.int64(0);
        writer.int32(-1);
        writer.int64(0);
        writer.int64(1);
        writer.int64(10);
        writer.int32(0);

        assertCode(() -> EntryIndexDecoder.decode(writer.toByteArray(), 1, 0, Long.MAX_VALUE),
                ErrorCode.UNSUPPORTED_FORMAT);
    }

    @Test
    void layoutDecoderRejectsCorruptHeaderAndObjectChecksum() {
        LocalFileObjectStore store = new LocalFileObjectStore(root);
        WalWriteResult result = writer(store).write(new WalWriteRequest(
                "cluster",
                "writer",
                RUN_HASH,
                7,
                List.of(new WalStreamSliceInput(new StreamId("stream-a"), batch("payload"))),
                options(false, 1 << 20)))
                .join();
        byte[] bytes = storedBytes(store, result);

        byte[] badMagic = bytes.clone();
        badMagic[0] = 'X';
        assertCode(() -> WalObjectLayout.decode(badMagic), ErrorCode.UNSUPPORTED_FORMAT);

        byte[] badPayload = bytes.clone();
        badPayload[badPayload.length - 1] ^= 1;
        assertCode(() -> WalObjectLayout.decode(badPayload), ErrorCode.UNSUPPORTED_FORMAT);
    }

    @Test
    void layoutDecoderRejectsChecksumConsistentDescriptorOutOfBounds() {
        ObjectId objectId = new ObjectId("object-with-bad-descriptor");
        byte[] payload = "x".getBytes(StandardCharsets.UTF_8);
        byte[] entryIndexBytes = EntryIndexEncoder.encode(new EntryIndex(
                1,
                1,
                List.of(new EntryIndexItem(0, 0, 1, 0, 1, 10, Map.of()))));
        StreamSliceDescriptor badDescriptor = new StreamSliceDescriptor(
                0,
                new StreamId("stream-a"),
                "slice-a",
                7,
                0,
                1,
                1,
                1,
                1_000_000,
                1,
                1_000_001,
                entryIndexBytes.length,
                Crc32cChecksums.checksum(payload, entryIndexBytes),
                PayloadFormat.OPAQUE_RECORD_BATCH,
                10,
                10,
                List.of());
        byte[] headerPayload = WalObjectLayout.encodeWalObjectHeader(
                objectId,
                "cluster",
                "writer",
                RUN_HASH,
                7,
                "test-writer-1",
                FIXED_CLOCK.instant().toEpochMilli(),
                CompressionType.NONE,
                1,
                1,
                10,
                10);
        byte[] directoryPayload = WalObjectLayout.encodeSliceDirectory(List.of(badDescriptor));
        byte[] footerPayload = WalObjectLayout.encodeFooter(objectId, List.of(badDescriptor));
        long footerOffset = WalObjectLayout.COMMON_HEADER_LENGTH
                + WalObjectLayout.sectionEncodedLength(headerPayload)
                + WalObjectLayout.sectionEncodedLength(directoryPayload)
                + WalObjectLayout.sectionEncodedLength(payload)
                + WalObjectLayout.sectionEncodedLength(entryIndexBytes);
        WalObjectLayout.EncodedObject encoded = WalObjectLayout.encodeObject(
                List.of(
                        new WalObjectLayout.Section(WalObjectLayout.SECTION_WAL_OBJECT_HEADER, headerPayload),
                        new WalObjectLayout.Section(WalObjectLayout.SECTION_STREAM_SLICE_DIRECTORY, directoryPayload),
                        new WalObjectLayout.Section(WalObjectLayout.SECTION_PAYLOAD_BLOCK, payload),
                        new WalObjectLayout.Section(WalObjectLayout.SECTION_ENTRY_INDEX, entryIndexBytes),
                        new WalObjectLayout.Section(WalObjectLayout.SECTION_FOOTER, footerPayload)),
                footerOffset,
                WalObjectLayout.sectionEncodedLength(footerPayload));

        assertCode(() -> WalObjectLayout.decode(encoded.bytes()), ErrorCode.UNSUPPORTED_FORMAT);
    }

    @Test
    void layoutDecoderWrapsChecksumConsistentInvalidDescriptorMetadataAsUnsupportedFormat() {
        ObjectId objectId = new ObjectId("object-with-bad-descriptor-metadata");
        byte[] payload = "x".getBytes(StandardCharsets.UTF_8);
        byte[] entryIndexBytes = EntryIndexEncoder.encode(new EntryIndex(
                1,
                1,
                List.of(new EntryIndexItem(0, 0, 1, 0, 1, 10, Map.of()))));
        StreamSliceDescriptor descriptor = new StreamSliceDescriptor(
                0,
                new StreamId("stream-a"),
                "slice-a",
                7,
                0,
                1,
                1,
                1,
                0,
                1,
                1,
                entryIndexBytes.length,
                Crc32cChecksums.checksum(payload, entryIndexBytes),
                PayloadFormat.OPAQUE_RECORD_BATCH,
                10,
                10,
                List.of());
        byte[] headerPayload = WalObjectLayout.encodeWalObjectHeader(
                objectId,
                "cluster",
                "writer",
                RUN_HASH,
                7,
                "test-writer-1",
                FIXED_CLOCK.instant().toEpochMilli(),
                CompressionType.NONE,
                1,
                1,
                10,
                10);
        byte[] directoryPayload = WalObjectLayout.encodeSliceDirectory(List.of(descriptor));
        byte[] footerPayload = footerWithBadChecksumType(objectId, descriptor);
        long footerOffset = WalObjectLayout.COMMON_HEADER_LENGTH
                + WalObjectLayout.sectionEncodedLength(headerPayload)
                + WalObjectLayout.sectionEncodedLength(directoryPayload)
                + WalObjectLayout.sectionEncodedLength(payload)
                + WalObjectLayout.sectionEncodedLength(entryIndexBytes);
        WalObjectLayout.EncodedObject encoded = WalObjectLayout.encodeObject(
                List.of(
                        new WalObjectLayout.Section(WalObjectLayout.SECTION_WAL_OBJECT_HEADER, headerPayload),
                        new WalObjectLayout.Section(WalObjectLayout.SECTION_STREAM_SLICE_DIRECTORY, directoryPayload),
                        new WalObjectLayout.Section(WalObjectLayout.SECTION_PAYLOAD_BLOCK, payload),
                        new WalObjectLayout.Section(WalObjectLayout.SECTION_ENTRY_INDEX, entryIndexBytes),
                        new WalObjectLayout.Section(WalObjectLayout.SECTION_FOOTER, footerPayload)),
                footerOffset,
                WalObjectLayout.sectionEncodedLength(footerPayload));

        assertCode(() -> WalObjectLayout.decode(encoded.bytes()), ErrorCode.UNSUPPORTED_FORMAT);
    }

    @Test
    void readResourceGuardRejectsBeforeObjectIo() {
        LocalFileObjectStore local = new LocalFileObjectStore(root);
        CountingObjectStore counting = new CountingObjectStore(local);
        WalWriteResult result = writer(counting).write(new WalWriteRequest(
                "cluster",
                "writer",
                RUN_HASH,
                7,
                List.of(new WalStreamSliceInput(new StreamId("stream-a"), batch("payload"))),
                options(false, 1 << 20)))
                .join();
        int readsBefore = counting.reads();
        WalObjectReader guarded = new DefaultWalObjectReader(
                counting,
                bytes -> {
                    throw new NereusException(ErrorCode.BACKPRESSURE_REJECTED, true, "no read budget");
                },
                WalReadObserver.noop());

        assertCode(() -> guarded.read(
                        0,
                        List.of(resolved(result.slices().get(0), result, 0)),
                        readOptions(10, 100))
                .join(), ErrorCode.BACKPRESSURE_REJECTED);
        assertThat(counting.reads()).isEqualTo(readsBefore);
    }

    private DefaultWalObjectWriter writer(ObjectStore store) {
        return new DefaultWalObjectWriter(store, "test-writer-1", FIXED_CLOCK);
    }

    private DefaultWalObjectReader reader(ObjectStore store) {
        return new DefaultWalObjectReader(store);
    }

    private WalWriteOptions options(boolean forceSingleStreamObject, int maxObjectBytes) {
        return new WalWriteOptions(
                CompressionType.NONE,
                Math.min(1024, maxObjectBytes),
                maxObjectBytes,
                Duration.ofSeconds(1),
                forceSingleStreamObject);
    }

    private AppendBatch batch(String... payloads) {
        List<AppendEntry> entries = new ArrayList<>();
        for (int i = 0; i < payloads.length; i++) {
            entries.add(new AppendEntry(
                    payloads[i].getBytes(StandardCharsets.UTF_8),
                    1,
                    10 + i,
                    Map.of("entry", Integer.toString(i))));
        }
        return new AppendBatch(
                PayloadFormat.OPAQUE_RECORD_BATCH,
                entries,
                entries.size(),
                entries.size(),
                10,
                10 + entries.size(),
                List.of(new SchemaRef("ns", "schema", 1)),
                Map.of(),
                Optional.empty());
    }

    private AppendBatch rangedKafkaBatch(List<String> payloads, List<Integer> recordCounts) {
        List<AppendEntry> entries = new ArrayList<>();
        int records = 0;
        for (int index = 0; index < payloads.size(); index++) {
            int recordCount = recordCounts.get(index);
            records = Math.addExact(records, recordCount);
            entries.add(new AppendEntry(
                    payloads.get(index).getBytes(StandardCharsets.UTF_8),
                    recordCount,
                    10 + index,
                    Map.of("entry", Integer.toString(index))));
        }
        return new AppendBatch(
                PayloadFormat.KAFKA_RECORD_BATCH,
                entries,
                records,
                entries.size(),
                10,
                10 + entries.size(),
                List.of(new SchemaRef("ns", "kafka", 1)),
                Map.of(),
                Optional.empty());
    }

    private ReadRequest request(
            long startOffset,
            ReadBoundaryMode boundaryMode,
            FirstEntryPolicy firstEntryPolicy,
            int maxRecords,
            int maxBytes) {
        return new ReadRequest(
                startOffset,
                ReadView.COMMITTED,
                boundaryMode,
                firstEntryPolicy,
                readOptions(maxRecords, maxBytes));
    }

    private ReadOptions readOptions(int maxRecords, int maxBytes) {
        return new ReadOptions(maxRecords, maxBytes, ReadIsolation.COMMITTED, Duration.ofSeconds(1));
    }

    private ResolvedObjectRange resolved(WrittenStreamSlice slice, WalWriteResult result, long startOffset) {
        return resolved(slice, result, startOffset, slice.entryIndexRef());
    }

    private ResolvedObjectRange resolved(
            WrittenStreamSlice slice,
            WalWriteResult result,
            long startOffset,
            EntryIndexRef entryIndexRef) {
        return new ResolvedObjectRange(
                new OffsetRange(startOffset, startOffset + slice.recordCount()),
                0,
                result.objectId(),
                result.objectKey(),
                ObjectType.MULTI_STREAM_WAL_OBJECT,
                slice.objectOffset(),
                slice.objectLength(),
                slice.sliceChecksum(),
                slice.payloadFormat(),
                slice.schemaRefs(),
                entryIndexRef,
                Optional.empty(),
                1);
    }

    private WalObjectLayout.DecodedObject decodeStoredObject(LocalFileObjectStore store, WalWriteResult result) {
        return WalObjectLayout.decode(storedBytes(store, result));
    }

    private byte[] storedBytes(ObjectStore store, WalWriteResult result) {
        RangeReadResult range = store.readRange(
                        result.objectKey(),
                        0,
                        result.objectLength(),
                        new RangeReadOptions(Optional.empty(), Duration.ofSeconds(1)))
                .join();
        ByteBuffer buffer = range.payload();
        byte[] bytes = new byte[buffer.remaining()];
        buffer.get(bytes);
        return bytes;
    }

    private byte[] footerWithBadChecksumType(ObjectId objectId, StreamSliceDescriptor descriptor) {
        WalBinary.Writer writer = new WalBinary.Writer();
        writer.int32(0);
        writer.string(objectId.value());
        writer.int32(1);
        writer.int64(0);
        writer.int64(0);
        writer.int32(descriptor.sliceOrdinal());
        writer.string(descriptor.streamId().value());
        writer.string(descriptor.sliceId());
        writer.int64(descriptor.writerEpoch());
        writer.int64(descriptor.relativeBaseOffset());
        writer.int32(descriptor.entryCount());
        writer.int32(descriptor.recordCount());
        writer.int64(descriptor.logicalBytes());
        writer.int64(descriptor.payloadOffset());
        writer.int64(descriptor.payloadLength());
        writer.int64(descriptor.entryIndexOffset());
        writer.int64(descriptor.entryIndexLength());
        writer.string("NOT_A_CHECKSUM");
        writer.string(descriptor.checksum().value());
        writer.string(descriptor.payloadFormat().name());
        writer.int64(descriptor.minEventTimeMillis());
        writer.int64(descriptor.maxEventTimeMillis());
        writer.int32(descriptor.schemaRefs().size());
        for (SchemaRef schemaRef : descriptor.schemaRefs()) {
            writer.string(schemaRef.namespace());
            writer.string(schemaRef.id());
            writer.int64(schemaRef.version());
        }
        byte[] payload = writer.toByteArray();
        int checksum = Crc32cChecksums.intValue(Crc32cChecksums.checksum(payload));
        ByteBuffer.wrap(payload, 0, Integer.BYTES).order(ByteOrder.LITTLE_ENDIAN).putInt(checksum);
        return payload;
    }

    private void assertCode(Runnable runnable, ErrorCode code) {
        assertThatThrownBy(runnable::run)
                .satisfies(throwable -> {
                    Throwable current = throwable;
                    if (current instanceof CompletionException) {
                        current = current.getCause();
                    }
                    assertThat(current)
                            .isInstanceOfSatisfying(NereusException.class, exception ->
                                    assertThat(exception.code()).isEqualTo(code));
                });
    }

    private static final class CountingObjectStore implements ObjectStore {
        private final ObjectStore delegate;
        private final AtomicInteger puts = new AtomicInteger();
        private final AtomicInteger reads = new AtomicInteger();

        private CountingObjectStore(ObjectStore delegate) {
            this.delegate = delegate;
        }

        int puts() {
            return puts.get();
        }

        int reads() {
            return reads.get();
        }

        @Override
        public CompletableFuture<PutObjectResult> putObject(com.nereusstream.api.ObjectKey key, ByteBuffer payload, PutObjectOptions options) {
            puts.incrementAndGet();
            return delegate.putObject(key, payload, options);
        }

        @Override
        public CompletableFuture<RangeReadResult> readRange(com.nereusstream.api.ObjectKey key, long offset, long length, RangeReadOptions options) {
            reads.incrementAndGet();
            return delegate.readRange(key, offset, length, options);
        }

        @Override
        public CompletableFuture<com.nereusstream.objectstore.HeadObjectResult> headObject(com.nereusstream.api.ObjectKey key, HeadObjectOptions options) {
            return delegate.headObject(key, options);
        }

        @Override
        public void close() {
            delegate.close();
        }
    }

    private static final class RecordingObjectStore implements ObjectStore {
        private final ObjectStore delegate;
        private PutObjectOptions lastPutOptions;

        private RecordingObjectStore(ObjectStore delegate) {
            this.delegate = delegate;
        }

        PutObjectOptions lastPutOptions() {
            return lastPutOptions;
        }

        @Override
        public CompletableFuture<PutObjectResult> putObject(com.nereusstream.api.ObjectKey key, ByteBuffer payload, PutObjectOptions options) {
            lastPutOptions = options;
            return delegate.putObject(key, payload, options);
        }

        @Override
        public CompletableFuture<RangeReadResult> readRange(com.nereusstream.api.ObjectKey key, long offset, long length, RangeReadOptions options) {
            return delegate.readRange(key, offset, length, options);
        }

        @Override
        public CompletableFuture<com.nereusstream.objectstore.HeadObjectResult> headObject(com.nereusstream.api.ObjectKey key, HeadObjectOptions options) {
            return delegate.headObject(key, options);
        }

        @Override
        public void close() {
            delegate.close();
        }
    }

    private static final class TimeoutRecordingObjectStore implements ObjectStore {
        private final ObjectStore delegate;
        private final List<Duration> timeouts = new ArrayList<>();

        private TimeoutRecordingObjectStore(ObjectStore delegate) {
            this.delegate = delegate;
        }

        List<Duration> timeouts() {
            return List.copyOf(timeouts);
        }

        @Override
        public CompletableFuture<PutObjectResult> putObject(
                com.nereusstream.api.ObjectKey key,
                ByteBuffer payload,
                PutObjectOptions options) {
            return delegate.putObject(key, payload, options);
        }

        @Override
        public CompletableFuture<RangeReadResult> readRange(
                com.nereusstream.api.ObjectKey key,
                long offset,
                long length,
                RangeReadOptions options) {
            timeouts.add(options.timeout());
            if (timeouts.size() == 1) {
                try {
                    Thread.sleep(20);
                } catch (InterruptedException e) {
                    Thread.currentThread().interrupt();
                    return CompletableFuture.failedFuture(e);
                }
            }
            return delegate.readRange(key, offset, length, options);
        }

        @Override
        public CompletableFuture<com.nereusstream.objectstore.HeadObjectResult> headObject(
                com.nereusstream.api.ObjectKey key,
                HeadObjectOptions options) {
            return delegate.headObject(key, options);
        }

        @Override
        public void close() {
            delegate.close();
        }
    }

    private static final class WrongChecksumObjectStore implements ObjectStore {
        private final ObjectStore delegate;

        private WrongChecksumObjectStore(ObjectStore delegate) {
            this.delegate = delegate;
        }

        @Override
        public CompletableFuture<PutObjectResult> putObject(com.nereusstream.api.ObjectKey key, ByteBuffer payload, PutObjectOptions options) {
            return delegate.putObject(key, payload, options)
                    .thenApply(result -> new PutObjectResult(
                            result.key(),
                            result.objectLength(),
                            new Checksum(ChecksumType.CRC32C, "ffffffff"),
                            result.etag()));
        }

        @Override
        public CompletableFuture<RangeReadResult> readRange(com.nereusstream.api.ObjectKey key, long offset, long length, RangeReadOptions options) {
            return delegate.readRange(key, offset, length, options);
        }

        @Override
        public CompletableFuture<com.nereusstream.objectstore.HeadObjectResult> headObject(com.nereusstream.api.ObjectKey key, HeadObjectOptions options) {
            return delegate.headObject(key, options);
        }

        @Override
        public void close() {
            delegate.close();
        }
    }
}
