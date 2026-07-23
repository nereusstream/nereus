/* Licensed under the Apache License, Version 2.0 */
package com.nereusstream.objectstore.compacted;

import com.nereusstream.api.Checksum;
import com.nereusstream.api.EntryIndexLocation;
import com.nereusstream.api.EntryIndexRef;
import com.nereusstream.api.ErrorCode;
import com.nereusstream.api.NereusException;
import com.nereusstream.api.ObjectId;
import com.nereusstream.api.ObjectKey;
import com.nereusstream.api.OffsetRange;
import com.nereusstream.api.ReadView;
import com.nereusstream.api.StreamId;
import com.nereusstream.objectstore.Crc32cChecksums;
import com.nereusstream.objectstore.staging.PrivateStagedObjectFile;
import com.nereusstream.objectstore.staging.StagingFileManager;
import java.io.ByteArrayOutputStream;
import java.io.IOException;
import java.io.OutputStream;
import java.nio.ByteBuffer;
import java.util.ArrayDeque;
import java.util.ArrayList;
import java.util.List;
import java.util.Map;
import java.util.Objects;
import java.util.Optional;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.Executor;
import java.util.concurrent.Flow;
import java.util.concurrent.RejectedExecutionException;
import java.util.concurrent.atomic.AtomicBoolean;
import java.util.concurrent.atomic.AtomicReference;
import org.apache.hadoop.conf.Configuration;
import org.apache.parquet.bytes.HeapByteBufferAllocator;
import org.apache.parquet.column.ColumnWriteStore;
import org.apache.parquet.column.ParquetProperties;
import org.apache.parquet.compression.CompressionCodecFactory;
import org.apache.parquet.example.data.Group;
import org.apache.parquet.example.data.GroupWriter;
import org.apache.parquet.example.data.simple.SimpleGroupFactory;
import org.apache.parquet.hadoop.CodecFactory;
import org.apache.parquet.hadoop.ColumnChunkPageWriteStore;
import org.apache.parquet.hadoop.ParquetFileWriter;
import org.apache.parquet.hadoop.metadata.CompressionCodecName;
import org.apache.parquet.io.ColumnIOFactory;
import org.apache.parquet.io.MessageColumnIO;
import org.apache.parquet.io.OutputFile;
import org.apache.parquet.io.PositionOutputStream;
import org.apache.parquet.io.api.Binary;
import org.apache.parquet.schema.MessageType;

/** Shared bounded transport for the two closed V2 schemas; row codecs retain separate semantic validation. */
final class ParquetV2WriterSupport {
    private static final int COLUMN_INDEX_TRUNCATE_LENGTH = 64;

    private final StagingFileManager stagingFiles;
    private final Executor writerExecutor;

    ParquetV2WriterSupport(StagingFileManager stagingFiles, Executor writerExecutor) {
        this.stagingFiles = Objects.requireNonNull(stagingFiles, "stagingFiles");
        this.writerExecutor = Objects.requireNonNull(writerExecutor, "writerExecutor");
    }

    <R> CompletableFuture<RangedCompactedObjectWriteResult> write(
            WriteSpec<R> spec,
            Flow.Publisher<R> rows) {
        CompletableFuture<RangedCompactedObjectWriteResult> result = new CompletableFuture<>();
        if (rows == null) {
            result.completeExceptionally(invalid("V2 compacted row publisher is required", null));
            return result;
        }
        SerialExecutor serial = new SerialExecutor(writerExecutor);
        AtomicReference<WritingSubscriber<R>> admitted = new AtomicReference<>();
        try {
            serial.execute(() -> {
                if (result.isCancelled()) {
                    return;
                }
                try {
                    WritingSubscriber<R> subscriber = new WritingSubscriber<>(spec, result, serial);
                    admitted.set(subscriber);
                    rows.subscribe(subscriber);
                } catch (Throwable failure) {
                    result.completeExceptionally(mapWriteFailure("initialize V2 compacted writer", failure));
                }
            });
        } catch (RejectedExecutionException failure) {
            result.completeExceptionally(new NereusException(
                    ErrorCode.STORAGE_CLOSED,
                    false,
                    "V2 compacted writer executor rejected the operation",
                    failure));
        }
        result.whenComplete((ignored, failure) -> {
            if (result.isCancelled()) {
                WritingSubscriber<R> subscriber = admitted.get();
                if (subscriber != null) {
                    subscriber.cancel();
                }
            }
        });
        return result;
    }

    static RowCodec<RangedCompactedObjectRow> ncp2Codec(RangedCompactedObjectWriteRequest request) {
        return (row, factory, state) -> {
            Objects.requireNonNull(row, "row");
            byte[] payload = payload(row.exactPayload());
            requirePayloadCrc(payload, row.payloadCrc32c());
            long expectedStart = state.rowCount() == 0
                    ? request.sourceCoverage().startOffset()
                    : state.previousEndOffset();
            if (row.entryOrdinal() != state.rowCount()
                    || row.streamOffsetStart() != expectedStart
                    || row.endOffset() > request.sourceCoverage().endOffset()) {
                throw new CompactedObjectFormatException("NCP2 rows are not ordinal-dense within coverage");
            }
            Group group = factory.newGroup()
                    .append("stream_offset_start", row.streamOffsetStart())
                    .append("record_count", row.recordCount())
                    .append("entry_ordinal", row.entryOrdinal())
                    .append("payload", Binary.fromConstantByteArray(payload))
                    .append("payload_crc32c", row.payloadCrc32c());
            row.eventTimeMillis().ifPresent(value -> group.append("event_time_millis", value));
            return new EncodedRow(group, row.streamOffsetStart(), row.endOffset(), row.recordCount(), payload.length);
        };
    }

    static RowCodec<KafkaTopicCompactedObjectRow> ntc2Codec(
            KafkaTopicCompactedObjectWriteRequest request) {
        return (row, factory, state) -> {
            Objects.requireNonNull(row, "row");
            byte[] payload = payload(row.exactPayload());
            byte[] compactionKey = bytes(
                    row.compactionKey(), KafkaCompactionKeyEncodingV2.MAX_ENCODED_KEY_BYTES, "compaction key");
            requirePayloadCrc(payload, row.payloadCrc32c());
            long sourceOffset = Math.addExact(row.sourceBatchBaseOffset(), row.sourceRecordIndex());
            if (!request.sourceCoverage().contains(row.streamOffsetStart())
                    || row.endOffset() > request.sourceCoverage().endOffset()
                    || (state.rowCount() > 0 && row.streamOffsetStart() < state.previousEndOffset())
                    || sourceOffset != row.streamOffsetStart()) {
                throw new CompactedObjectFormatException("NTC2 row ordering/source identity is inconsistent");
            }
            byte[] sourceSha = java.util.HexFormat.of().parseHex(row.sourceBatchSha256().value());
            if (sourceSha.length != 32) {
                throw new CompactedObjectFormatException("NTC2 source batch digest must be 32 bytes");
            }
            Group group = factory.newGroup()
                    .append("stream_offset_start", row.streamOffsetStart())
                    .append("record_count", row.recordCount())
                    .append("disposition", row.disposition().wireId())
                    .append("compaction_key", Binary.fromConstantByteArray(compactionKey))
                    .append("payload", Binary.fromConstantByteArray(payload))
                    .append("payload_crc32c", row.payloadCrc32c())
                    .append("source_batch_base_offset", row.sourceBatchBaseOffset())
                    .append("source_record_index", row.sourceRecordIndex())
                    .append("source_batch_sha256", Binary.fromConstantByteArray(sourceSha));
            row.eventTimeMillis().ifPresent(value -> group.append("event_time_millis", value));
            return new EncodedRow(group, row.streamOffsetStart(), row.endOffset(), row.recordCount(), payload.length);
        };
    }

    record WriteSpec<R>(
            ReadView view,
            String cluster,
            StreamId streamId,
            OffsetRange sourceCoverage,
            String outputAttemptId,
            int expectedRows,
            long expectedRecords,
            long expectedLogicalBytes,
            int targetRowGroupRecords,
            String compression,
            MessageType schema,
            Map<String, String> metadata,
            RowCodec<R> rowCodec) {
        WriteSpec {
            Objects.requireNonNull(view, "view");
            Objects.requireNonNull(cluster, "cluster");
            Objects.requireNonNull(streamId, "streamId");
            Objects.requireNonNull(sourceCoverage, "sourceCoverage");
            Objects.requireNonNull(outputAttemptId, "outputAttemptId");
            Objects.requireNonNull(compression, "compression");
            Objects.requireNonNull(schema, "schema");
            metadata = java.util.Collections.unmodifiableMap(
                    new java.util.LinkedHashMap<>(Objects.requireNonNull(metadata, "metadata")));
            Objects.requireNonNull(rowCodec, "rowCodec");
        }
    }

    @FunctionalInterface
    interface RowCodec<R> {
        EncodedRow encode(R row, SimpleGroupFactory factory, RowState state);
    }

    record RowState(int rowCount, long previousEndOffset, long records, long logicalBytes) {
    }

    private record EncodedRow(Group group, long startOffset, long endOffset, int recordCount, int payloadBytes) {
    }

    private final class WritingSubscriber<R> implements Flow.Subscriber<R> {
        private final CompletableFuture<RangedCompactedObjectWriteResult> result;
        private final SerialExecutor serial;
        private final ParquetSink<R> sink;
        private final AtomicBoolean terminal = new AtomicBoolean();
        private final AtomicBoolean awaiting = new AtomicBoolean();
        private volatile Flow.Subscription subscription;

        private WritingSubscriber(
                WriteSpec<R> spec,
                CompletableFuture<RangedCompactedObjectWriteResult> result,
                SerialExecutor serial) throws IOException {
            this.result = result;
            this.serial = serial;
            sink = new ParquetSink<>(spec, stagingFiles.create("compacted-v2"));
        }

        @Override
        public void onSubscribe(Flow.Subscription value) {
            Objects.requireNonNull(value, "subscription");
            if (subscription != null || terminal.get()) {
                value.cancel();
                fail(new IllegalStateException("V2 row publisher subscribed more than once"));
                return;
            }
            subscription = value;
            requestOne();
        }

        @Override
        public void onNext(R row) {
            if (!awaiting.compareAndSet(true, false)) {
                fail(new IllegalStateException("V2 row publisher emitted without demand"));
                return;
            }
            submit(() -> {
                if (!terminal.get()) {
                    try {
                        sink.write(row);
                        requestOne();
                    } catch (Throwable failure) {
                        fail(failure);
                    }
                }
            });
        }

        @Override
        public void onError(Throwable failure) {
            submit(() -> fail(Objects.requireNonNull(failure, "failure")));
        }

        @Override
        public void onComplete() {
            submit(() -> {
                if (!terminal.compareAndSet(false, true)) {
                    return;
                }
                try {
                    RangedCompactedObjectWriteResult completed = sink.finish();
                    if (!result.complete(completed)) {
                        completed.close();
                    }
                } catch (Throwable failure) {
                    sink.close();
                    result.completeExceptionally(mapWriteFailure("finish V2 compacted object", failure));
                }
            });
        }

        private void requestOne() {
            submit(() -> {
                if (terminal.get()) {
                    return;
                }
                Flow.Subscription value = subscription;
                if (value == null) {
                    fail(new IllegalStateException("V2 row publisher omitted subscription"));
                    return;
                }
                awaiting.set(true);
                try {
                    value.request(1);
                } catch (Throwable failure) {
                    awaiting.set(false);
                    fail(failure);
                }
            });
        }

        private void submit(Runnable action) {
            try {
                serial.execute(action);
            } catch (RejectedExecutionException failure) {
                fail(failure);
            }
        }

        private void fail(Throwable failure) {
            if (!terminal.compareAndSet(false, true)) {
                return;
            }
            Flow.Subscription value = subscription;
            if (value != null) {
                value.cancel();
            }
            sink.close();
            result.completeExceptionally(mapWriteFailure("write V2 compacted object", failure));
        }

        private void cancel() {
            submit(() -> {
                if (terminal.compareAndSet(false, true)) {
                    Flow.Subscription value = subscription;
                    if (value != null) {
                        value.cancel();
                    }
                    sink.close();
                }
            });
        }
    }

    private static final class ParquetSink<R> implements AutoCloseable {
        private final WriteSpec<R> spec;
        private final PrivateStagedObjectFile stagingFile;
        private final CapturingPositionOutputStream output;
        private final ParquetFileWriter fileWriter;
        private final CodecFactory codecFactory;
        private final CompressionCodecFactory.BytesInputCompressor compressor;
        private final ParquetProperties properties;
        private final SimpleGroupFactory groups;
        private final List<Group> rowGroup = new ArrayList<>();
        private long rowGroupPayloadBytes;
        private long totalPayloadBytes;
        private long totalRecords;
        private long previousEndOffset = -1;
        private int rowCount;
        private int rowGroupCount;
        private boolean finished;

        private ParquetSink(WriteSpec<R> spec, PrivateStagedObjectFile stagingFile) throws IOException {
            this.spec = spec;
            this.stagingFile = stagingFile;
            groups = new SimpleGroupFactory(spec.schema());
            Configuration configuration = new Configuration(false);
            configuration.setInt("io.compression.codec.zstd.level", CompactedObjectFormatV2.ZSTD_LEVEL);
            codecFactory = new CodecFactory(configuration, CompactedObjectFormatV2.DATA_PAGE_BYTES);
            CompressionCodecName codec = spec.compression().equals("ZSTD")
                    ? CompressionCodecName.ZSTD
                    : CompressionCodecName.UNCOMPRESSED;
            compressor = codecFactory.getCompressor(codec);
            properties = ParquetProperties.builder()
                    .withPageSize(CompactedObjectFormatV2.DATA_PAGE_BYTES)
                    .withPageRowCountLimit(spec.targetRowGroupRecords())
                    .withDictionaryEncoding(false)
                    .withWriterVersion(ParquetProperties.WriterVersion.PARQUET_2_0)
                    .withBloomFilterEnabled(false)
                    .withPageWriteChecksumEnabled(true)
                    .withColumnIndexTruncateLength(COLUMN_INDEX_TRUNCATE_LENGTH)
                    .withStatisticsEnabled(true)
                    .build();
            output = new CapturingPositionOutputStream(stagingFile.outputStream());
            fileWriter = new ParquetFileWriter(
                    new SingleStreamOutputFile(output),
                    spec.schema(),
                    ParquetFileWriter.Mode.CREATE,
                    CompactedObjectFormatV2.MAX_ROW_GROUP_BUFFER_BYTES,
                    0);
            try {
                fileWriter.start();
            } catch (Throwable failure) {
                close();
                throw failure;
            }
        }

        private void write(R row) throws IOException {
            RowState state = new RowState(rowCount, previousEndOffset, totalRecords, totalPayloadBytes);
            EncodedRow encoded = spec.rowCodec().encode(Objects.requireNonNull(row, "row"), groups, state);
            if (!rowGroup.isEmpty()
                    && (rowGroup.size() >= spec.targetRowGroupRecords()
                            || Math.addExact(rowGroupPayloadBytes, encoded.payloadBytes())
                                    > CompactedObjectFormatV2.MAX_ROW_GROUP_BUFFER_BYTES)) {
                flushRowGroup();
            }
            rowGroup.add(encoded.group());
            rowGroupPayloadBytes = Math.addExact(rowGroupPayloadBytes, encoded.payloadBytes());
            totalPayloadBytes = Math.addExact(totalPayloadBytes, encoded.payloadBytes());
            totalRecords = Math.addExact(totalRecords, encoded.recordCount());
            previousEndOffset = encoded.endOffset();
            rowCount = Math.addExact(rowCount, 1);
            if (totalPayloadBytes > CompactedObjectFormatV2.MAX_OBJECT_BYTES
                    || rowCount > spec.expectedRows()
                    || totalRecords > spec.expectedRecords()) {
                throw new CompactedObjectFormatException("V2 row publisher exceeded frozen accounting");
            }
        }

        private RangedCompactedObjectWriteResult finish() throws IOException {
            if (finished) {
                throw new IllegalStateException("V2 compacted writer was already finished");
            }
            if (!rowGroup.isEmpty()) {
                flushRowGroup();
            }
            validateTerminalAccounting();
            output.startCapture();
            fileWriter.end(spec.metadata());
            byte[] footer = extractStandardFooter(output.stopCapture());
            long footerOffset = Math.subtractExact(output.getPos(), footer.length);
            fileWriter.close();
            codecFactory.release();
            if (output.getPos() > CompactedObjectFormatV2.MAX_OBJECT_BYTES) {
                throw new CompactedObjectFormatException("V2 compacted object exceeds 1 GiB");
            }
            PrivateStagedObjectFile sealed = stagingFile.seal();
            ObjectKey objectKey = CompactedObjectFormatV2.objectKey(
                    spec.cluster(), spec.view(), spec.streamId(), spec.sourceCoverage(),
                    sealed.contentSha256(), spec.outputAttemptId());
            ObjectId objectId = CompactedObjectFormatV2.objectId(objectKey);
            Checksum footerCrc32c = Crc32cChecksums.checksum(footer);
            EntryIndexRef footerRef = new EntryIndexRef(
                    EntryIndexLocation.OBJECT_FOOTER,
                    Optional.of(objectId),
                    Optional.of(objectKey),
                    Optional.empty(),
                    footerOffset,
                    footer.length,
                    footerCrc32c);
            finished = true;
            try {
                return new RangedCompactedObjectWriteResult(
                        sealed,
                        objectId,
                        objectKey,
                        CompactedObjectFormatV2.objectKeyHash(objectKey),
                        sealed.sealedLength(),
                        sealed.storageCrc32c(),
                        sealed.contentSha256(),
                        CompactedObjectFormatV2.physicalFormat(spec.view()),
                        footerRef,
                        rowCount,
                        totalRecords);
            } catch (Throwable failure) {
                sealed.close();
                throw failure;
            }
        }

        private void flushRowGroup() throws IOException {
            if (rowGroup.isEmpty()) {
                return;
            }
            if (rowGroupCount >= CompactedObjectFormatV2.MAX_ROW_GROUPS) {
                throw new CompactedObjectFormatException("V2 compacted object exceeds row-group limit");
            }
            fileWriter.startBlock(rowGroup.size());
            ColumnChunkPageWriteStore pageStore = new ColumnChunkPageWriteStore(
                    compressor,
                    spec.schema(),
                    HeapByteBufferAllocator.getInstance(),
                    properties.getColumnIndexTruncateLength(),
                    properties.getPageWriteChecksumEnabled(),
                    null,
                    rowGroupCount);
            ColumnWriteStore columnStore = properties.newColumnWriteStore(spec.schema(), pageStore, pageStore);
            try {
                MessageColumnIO columnIo = new ColumnIOFactory(true).getColumnIO(spec.schema());
                GroupWriter writer = new GroupWriter(columnIo.getRecordWriter(columnStore), spec.schema());
                for (Group group : rowGroup) {
                    writer.write(group);
                }
                columnStore.flush();
                pageStore.flushToFileWriter(fileWriter);
                fileWriter.endBlock();
            } finally {
                columnStore.close();
                pageStore.close();
            }
            rowGroup.clear();
            rowGroupPayloadBytes = 0;
            rowGroupCount++;
        }

        private void validateTerminalAccounting() {
            if (rowCount != spec.expectedRows()
                    || totalRecords != spec.expectedRecords()
                    || totalPayloadBytes != spec.expectedLogicalBytes()) {
                throw new CompactedObjectFormatException("V2 row publisher ended with mismatched accounting");
            }
            if (spec.view() == ReadView.COMMITTED
                    && previousEndOffset != spec.sourceCoverage().endOffset()) {
                throw new CompactedObjectFormatException("NCP2 rows do not cover the frozen dense range");
            }
        }

        @Override
        public void close() {
            if (finished) {
                return;
            }
            try {
                fileWriter.close();
            } catch (Throwable ignored) {
                // Partial-file cleanup below is authoritative.
            }
            try {
                codecFactory.release();
            } catch (Throwable ignored) {
                // Codec buffers are process-local.
            }
            stagingFile.close();
        }
    }

    private static final class SingleStreamOutputFile implements OutputFile {
        private final CapturingPositionOutputStream stream;
        private boolean claimed;

        private SingleStreamOutputFile(CapturingPositionOutputStream stream) {
            this.stream = stream;
        }

        @Override
        public synchronized PositionOutputStream create(long blockSizeHint) throws IOException {
            if (claimed) {
                throw new IOException("V2 Parquet staging stream was already claimed");
            }
            claimed = true;
            return stream;
        }

        @Override
        public PositionOutputStream createOrOverwrite(long blockSizeHint) throws IOException {
            throw new IOException("V2 compacted objects never overwrite staging bytes");
        }

        @Override
        public boolean supportsBlockSize() {
            return false;
        }

        @Override
        public long defaultBlockSize() {
            return CompactedObjectFormatV2.MAX_ROW_GROUP_BUFFER_BYTES;
        }

        @Override
        public String getPath() {
            return "nereus-private-staging://compacted-v2";
        }
    }

    private static final class CapturingPositionOutputStream extends PositionOutputStream {
        private final OutputStream delegate;
        private long position;
        private ByteArrayOutputStream capture;

        private CapturingPositionOutputStream(OutputStream delegate) {
            this.delegate = delegate;
        }

        @Override
        public void write(int value) throws IOException {
            delegate.write(value);
            if (capture != null) {
                capture.write(value);
            }
            position++;
        }

        @Override
        public void write(byte[] bytes, int offset, int length) throws IOException {
            delegate.write(bytes, offset, length);
            if (capture != null) {
                capture.write(bytes, offset, length);
            }
            position = Math.addExact(position, length);
        }

        @Override
        public long getPos() {
            return position;
        }

        @Override
        public void flush() throws IOException {
            delegate.flush();
        }

        @Override
        public void close() throws IOException {
            delegate.close();
        }

        private void startCapture() {
            if (capture != null) {
                throw new IllegalStateException("V2 footer capture already active");
            }
            capture = new ByteArrayOutputStream();
        }

        private byte[] stopCapture() {
            if (capture == null) {
                throw new IllegalStateException("V2 footer capture is not active");
            }
            byte[] result = capture.toByteArray();
            capture = null;
            return result;
        }
    }

    private static final class SerialExecutor implements Executor {
        private final Executor delegate;
        private final ArrayDeque<Runnable> queue = new ArrayDeque<>();
        private boolean running;

        private SerialExecutor(Executor delegate) {
            this.delegate = delegate;
        }

        @Override
        public void execute(Runnable command) {
            boolean schedule;
            synchronized (this) {
                queue.addLast(Objects.requireNonNull(command, "command"));
                schedule = !running;
                if (schedule) {
                    running = true;
                }
            }
            if (schedule) {
                try {
                    delegate.execute(this::drain);
                } catch (RuntimeException failure) {
                    synchronized (this) {
                        running = false;
                        queue.clear();
                    }
                    throw failure;
                }
            }
        }

        private void drain() {
            while (true) {
                Runnable next;
                synchronized (this) {
                    next = queue.pollFirst();
                    if (next == null) {
                        running = false;
                        return;
                    }
                }
                try {
                    next.run();
                } catch (Throwable ignored) {
                    // Every action completes its owning future; keep draining cleanup work.
                }
            }
        }
    }

    private static byte[] payload(ByteBuffer value) {
        return bytes(value, CompactedObjectFormatV2.MAX_PAYLOAD_BYTES, "payload");
    }

    private static byte[] bytes(ByteBuffer value, int maximum, String field) {
        ByteBuffer copy = Objects.requireNonNull(value, field).asReadOnlyBuffer();
        if (copy.remaining() > maximum) {
            throw new CompactedObjectFormatException(field + " exceeds the V2 buffer limit");
        }
        byte[] result = new byte[copy.remaining()];
        copy.get(result);
        return result;
    }

    private static void requirePayloadCrc(byte[] payload, int expected) {
        if (Crc32cChecksums.intValue(Crc32cChecksums.checksum(payload)) != expected) {
            throw new CompactedObjectFormatException("V2 row payload CRC32C does not match exact bytes");
        }
    }

    private static byte[] extractStandardFooter(byte[] endBytes) {
        if (endBytes.length < 8) {
            throw new CompactedObjectFormatException("V2 Parquet footer length is outside the limit");
        }
        int end = endBytes.length;
        if (endBytes[end - 4] != 'P'
                || endBytes[end - 3] != 'A'
                || endBytes[end - 2] != 'R'
                || endBytes[end - 1] != '1') {
            throw new CompactedObjectFormatException("V2 Parquet footer magic is missing");
        }
        long metadataLength = Integer.toUnsignedLong(
                (endBytes[end - 8] & 0xff)
                        | ((endBytes[end - 7] & 0xff) << 8)
                        | ((endBytes[end - 6] & 0xff) << 16)
                        | ((endBytes[end - 5] & 0xff) << 24));
        long footerLength = Math.addExact(metadataLength, 8);
        if (footerLength > CompactedObjectFormatV2.MAX_FOOTER_BYTES || footerLength > endBytes.length) {
            throw new CompactedObjectFormatException("V2 Parquet footer trailer is inconsistent");
        }
        return java.util.Arrays.copyOfRange(
                endBytes, Math.toIntExact(endBytes.length - footerLength), endBytes.length);
    }

    private static NereusException invalid(String message, Throwable cause) {
        return cause == null
                ? new NereusException(ErrorCode.INVALID_ARGUMENT, false, message)
                : new NereusException(ErrorCode.INVALID_ARGUMENT, false, message, cause);
    }

    private static Throwable mapWriteFailure(String action, Throwable failure) {
        Throwable current = failure;
        while (current instanceof java.util.concurrent.CompletionException && current.getCause() != null) {
            current = current.getCause();
        }
        if (current instanceof NereusException) {
            return current;
        }
        if (current instanceof IllegalArgumentException || current instanceof NullPointerException) {
            return invalid(action + " failed", current);
        }
        return new NereusException(ErrorCode.OBJECT_UPLOAD_FAILED, true, action + " failed", current);
    }
}
