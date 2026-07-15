/* Licensed under the Apache License, Version 2.0 */
package com.nereusstream.managedledger.entry;

import static org.assertj.core.api.Assertions.assertThat;

import com.nereusstream.api.Checksum;
import com.nereusstream.api.ChecksumType;
import com.nereusstream.api.ObjectType;
import com.nereusstream.api.OffsetRange;
import com.nereusstream.api.PayloadFormat;
import com.nereusstream.api.ProjectionRef;
import com.nereusstream.api.ProjectionType;
import com.nereusstream.api.ReadBatch;
import com.nereusstream.api.ReadIsolation;
import com.nereusstream.api.ReadOptions;
import com.nereusstream.api.ReadResult;
import com.nereusstream.api.ReadView;
import com.nereusstream.api.ResolvedRange;
import com.nereusstream.api.StreamId;
import com.nereusstream.api.target.ObjectSliceReadTarget;
import com.nereusstream.core.read.ParquetCompactedTargetReader;
import com.nereusstream.managedledger.projection.VirtualLedgerProjection;
import com.nereusstream.objectstore.Crc32cChecksums;
import com.nereusstream.objectstore.PutObjectOptions;
import com.nereusstream.objectstore.compacted.CompactedObjectRow;
import com.nereusstream.objectstore.compacted.CompactedObjectVerificationRequest;
import com.nereusstream.objectstore.compacted.CompactedObjectWriteRequest;
import com.nereusstream.objectstore.compacted.CompactedObjectWriteResult;
import com.nereusstream.objectstore.compacted.ParquetCompactedObjectReader;
import com.nereusstream.objectstore.compacted.ParquetCompactedObjectWriter;
import com.nereusstream.objectstore.staging.StagingFileManager;
import com.nereusstream.objectstore.testing.LocalFileObjectStore;
import com.nereusstream.objectstore.wal.WalReadResult;
import io.netty.buffer.ByteBuf;
import io.netty.buffer.Unpooled;
import java.nio.ByteBuffer;
import java.nio.charset.StandardCharsets;
import java.nio.file.Files;
import java.nio.file.Path;
import java.nio.file.attribute.PosixFilePermissions;
import java.time.Duration;
import java.util.ArrayList;
import java.util.List;
import java.util.Map;
import java.util.Optional;
import java.util.OptionalInt;
import java.util.OptionalLong;
import java.util.concurrent.Flow;
import org.apache.bookkeeper.mledger.Entry;
import org.apache.bookkeeper.mledger.Position;
import org.apache.bookkeeper.mledger.PositionFactory;
import org.apache.pulsar.client.api.MessageId;
import org.apache.pulsar.client.impl.BatchMessageIdImpl;
import org.apache.pulsar.client.impl.MessageIdImpl;
import org.apache.pulsar.common.api.proto.CompressionType;
import org.apache.pulsar.common.api.proto.MessageMetadata;
import org.apache.pulsar.common.api.proto.SingleMessageMetadata;
import org.apache.pulsar.common.compression.CompressionCodec;
import org.apache.pulsar.common.compression.CompressionCodecProvider;
import org.apache.pulsar.common.protocol.Commands;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.io.TempDir;

/** Cross-layer proof that lossless materialization never parses or rewrites Pulsar Entry bytes. */
class PulsarEntryOpaqueRoundTripTest {
    private static final StreamId STREAM_ID = new StreamId("pulsar-entry-round-trip");
    private static final OffsetRange COVERAGE = new OffsetRange(40, 42);
    private static final long VIRTUAL_LEDGER_ID = VirtualLedgerProjection.MIN_VIRTUAL_LEDGER_ID + 17;
    private static final ProjectionRef PROJECTION = new ProjectionRef(
            ProjectionType.VIRTUAL_LEDGER, "nereus-ml-v1.round-trip");

    @TempDir
    Path temporaryDirectory;

    @Test
    void preservesUnbatchedAndCompressedBatchBytesPropertiesOrderingKeyAndMiddleBatchMessageId()
            throws Exception {
        byte[] unbatched = unbatchedEntry();
        byte[] compressedBatch = compressedBatchEntry();
        List<byte[]> source = List.of(unbatched, compressedBatch);
        long logicalBytes = source.stream().mapToLong(bytes -> bytes.length).sum();
        CompactedObjectWriteRequest request = request(logicalBytes);

        Path stagingDirectory = Files.createDirectory(temporaryDirectory.resolve("staging"));
        Files.setPosixFilePermissions(
                stagingDirectory, PosixFilePermissions.fromString("rwx------"));
        try (StagingFileManager staging = new StagingFileManager(
                        stagingDirectory,
                        32L << 20,
                        StagingFileManager.MIN_UPLOAD_CHUNK_BYTES,
                        Duration.ofHours(1),
                        Runnable::run);
                LocalFileObjectStore objectStore =
                        new LocalFileObjectStore(temporaryDirectory.resolve("objects"));
                CompactedObjectWriteResult written = new ParquetCompactedObjectWriter(
                                staging, Runnable::run)
                        .write(request, publisher(List.of(
                                row(40, unbatched), row(41, compressedBatch))))
                        .join()) {
            objectStore.putObject(
                            written.objectKey(),
                            written.stagingFile(),
                            new PutObjectOptions(
                                    "application/vnd.apache.parquet",
                                    written.storageCrc32c(),
                                    true,
                                    Map.of(),
                                    Duration.ofSeconds(10)))
                    .join();
            ObjectSliceReadTarget target = CompactedObjectVerificationRequest.from(
                            request, written, Duration.ofSeconds(10))
                    .target();
            ResolvedRange range = new ResolvedRange(
                    COVERAGE,
                    3,
                    target,
                    PayloadFormat.OPAQUE_RECORD_BATCH,
                    2,
                    2,
                    logicalBytes,
                    List.of(),
                    Optional.of(PROJECTION),
                    19);
            WalReadResult materialized = new ParquetCompactedTargetReader(
                            new ParquetCompactedObjectReader(objectStore, Runnable::run))
                    .readWithStats(
                            STREAM_ID,
                            COVERAGE.startOffset(),
                            List.of(range),
                            new ReadOptions(
                                    2,
                                    1 << 20,
                                    ReadIsolation.COMMITTED,
                                    Duration.ofSeconds(10)))
                    .join();

            assertThat(materialized.batches()).hasSize(2).allSatisfy(batch ->
                    assertThat(batch.projectionRef()).isEmpty());
            PulsarEntryCodec codec = new PulsarEntryCodec(1 << 20);
            List<Entry> recovered = decode(codec, materialized.batches());
            try {
                assertThat(recovered.get(0).getData()).containsExactly(unbatched);
                assertThat(recovered.get(1).getData()).containsExactly(compressedBatch);

                MessageMetadata ordinaryMetadata = recovered.get(0).getMessageMetadata();
                assertThat(ordinaryMetadata.getPropertyAt(0).getKey()).isEqualTo("ordinary-property");
                assertThat(ordinaryMetadata.getPropertyAt(0).getValue()).isEqualTo("ordinary-value");
                assertThat(ordinaryMetadata.getOrderingKey())
                        .containsExactly("ordinary-order".getBytes(StandardCharsets.UTF_8));

                SingleMessageMetadata middle = middleBatchMetadata(recovered.get(1).getData());
                assertThat(middle.getPartitionKey()).isEqualTo("batch-key-1");
                assertThat(middle.getPropertyAt(0).getKey()).isEqualTo("batch-property");
                assertThat(middle.getPropertyAt(0).getValue()).isEqualTo("value-1");
                assertThat(middle.getOrderingKey())
                        .containsExactly("batch-order-1".getBytes(StandardCharsets.UTF_8));

                Position originalPosition = position(41);
                Position recoveredPosition = recovered.get(1).getPosition();
                BatchMessageIdImpl originalMiddle = new BatchMessageIdImpl(
                        originalPosition.getLedgerId(),
                        originalPosition.getEntryId(),
                        2,
                        1,
                        3,
                        null);
                BatchMessageIdImpl recoveredMiddle = new BatchMessageIdImpl(
                        recoveredPosition.getLedgerId(),
                        recoveredPosition.getEntryId(),
                        2,
                        1,
                        3,
                        null);
                assertThat(recoveredMiddle.toByteArray())
                        .containsExactly(originalMiddle.toByteArray());
                MessageId decodedId = MessageIdImpl.fromByteArray(recoveredMiddle.toByteArray());
                assertThat(decodedId).isEqualTo(originalMiddle);
            } finally {
                recovered.forEach(Entry::release);
            }
        }
    }

    private static List<Entry> decode(PulsarEntryCodec codec, List<ReadBatch> batches) {
        List<Entry> entries = new ArrayList<>();
        for (ReadBatch batch : batches) {
            long offset = batch.range().startOffset();
            entries.add(codec.decode(
                    position(offset),
                    new ReadResult(
                            STREAM_ID,
                            offset,
                            offset + 1,
                            List.of(batch),
                            false)));
        }
        return entries;
    }

    private static Position position(long offset) {
        return PositionFactory.create(VIRTUAL_LEDGER_ID, offset);
    }

    private static CompactedObjectWriteRequest request(long logicalBytes) {
        return new CompactedObjectWriteRequest(
                "test-cluster",
                ReadView.COMMITTED,
                STREAM_ID,
                COVERAGE,
                "a".repeat(26),
                sha256('1'),
                sha256('2'),
                PayloadFormat.OPAQUE_RECORD_BATCH,
                PayloadFormat.OPAQUE_RECORD_BATCH.name(),
                Optional.of(sha256('3')),
                2,
                2,
                2,
                logicalBytes,
                List.of(),
                100,
                100 + logicalBytes,
                2,
                "ZSTD",
                "nereus-pulsar-round-trip-test",
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

    private static byte[] unbatchedEntry() {
        MessageMetadata metadata = new MessageMetadata()
                .setProducerName("ordinary-producer")
                .setSequenceId(7)
                .setPublishTime(1_234)
                .setEventTime(1_235)
                .setPartitionKey("ordinary-key")
                .setOrderingKey("ordinary-order".getBytes(StandardCharsets.UTF_8));
        metadata.addProperty()
                .setKey("ordinary-property")
                .setValue("ordinary-value");
        ByteBuf payload = Unpooled.wrappedBuffer("ordinary-payload".getBytes(StandardCharsets.UTF_8));
        ByteBuf serialized = Commands.serializeMetadataAndPayload(
                Commands.ChecksumType.Crc32c, metadata, payload);
        try {
            return bytes(serialized);
        } finally {
            serialized.release();
            payload.release();
        }
    }

    private static byte[] compressedBatchEntry() {
        ByteBuf uncompressed = Unpooled.buffer();
        for (int index = 0; index < 3; index++) {
            SingleMessageMetadata metadata = new SingleMessageMetadata()
                    .setPartitionKey("batch-key-" + index)
                    .setOrderingKey(("batch-order-" + index).getBytes(StandardCharsets.UTF_8))
                    .setEventTime(2_000 + index)
                    .setSequenceId(20 + index);
            metadata.addProperty()
                    .setKey("batch-property")
                    .setValue("value-" + index);
            ByteBuf payload = Unpooled.wrappedBuffer(
                    ("batch-payload-" + index).getBytes(StandardCharsets.UTF_8));
            try {
                Commands.serializeSingleMessageInBatchWithPayload(
                        metadata, payload, uncompressed);
            } finally {
                payload.release();
            }
        }
        int uncompressedSize = uncompressed.readableBytes();
        CompressionCodec compression = CompressionCodecProvider.getCompressionCodec(
                CompressionType.ZSTD);
        ByteBuf compressed = compression.encode(uncompressed);
        MessageMetadata metadata = new MessageMetadata()
                .setProducerName("batch-producer")
                .setSequenceId(20)
                .setHighestSequenceId(22)
                .setPublishTime(2_000)
                .setNumMessagesInBatch(3)
                .setCompression(CompressionType.ZSTD)
                .setUncompressedSize(uncompressedSize);
        ByteBuf serialized = Commands.serializeMetadataAndPayload(
                Commands.ChecksumType.Crc32c, metadata, compressed);
        try {
            return bytes(serialized);
        } finally {
            serialized.release();
            compressed.release();
            uncompressed.release();
        }
    }

    private static SingleMessageMetadata middleBatchMetadata(byte[] entryBytes) throws Exception {
        ByteBuf entry = Unpooled.wrappedBuffer(entryBytes);
        MessageMetadata metadata = new MessageMetadata();
        Commands.parseMessageMetadata(entry, metadata);
        assertThat(metadata.getNumMessagesInBatch()).isEqualTo(3);
        assertThat(metadata.getCompression()).isEqualTo(CompressionType.ZSTD);
        ByteBuf uncompressed = CompressionCodecProvider.getCompressionCodec(metadata.getCompression())
                .decode(entry, metadata.getUncompressedSize());
        try {
            SingleMessageMetadata current = new SingleMessageMetadata();
            for (int index = 0; index <= 1; index++) {
                current.clear();
                ByteBuf payload = Commands.deSerializeSingleMessageInBatch(
                        uncompressed, current, index, metadata.getNumMessagesInBatch());
                payload.release();
            }
            SingleMessageMetadata result = new SingleMessageMetadata();
            result.copyFrom(current);
            return result;
        } finally {
            uncompressed.release();
            entry.release();
        }
    }

    private static byte[] bytes(ByteBuf buffer) {
        byte[] value = new byte[buffer.readableBytes()];
        buffer.getBytes(buffer.readerIndex(), value);
        return value;
    }

    private static Checksum sha256(char value) {
        return new Checksum(ChecksumType.SHA256, Character.toString(value).repeat(64));
    }

    private static <T> Flow.Publisher<T> publisher(List<T> values) {
        List<T> immutable = List.copyOf(values);
        return subscriber -> subscriber.onSubscribe(new Flow.Subscription() {
            private int index;
            private boolean done;

            @Override
            public void request(long demand) {
                if (done) {
                    return;
                }
                if (demand <= 0) {
                    done = true;
                    subscriber.onError(new IllegalArgumentException("non-positive demand"));
                    return;
                }
                long emitted = 0;
                while (!done && emitted < demand && index < immutable.size()) {
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
}
