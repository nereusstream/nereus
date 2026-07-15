/* Licensed under the Apache License, Version 2.0 */
package com.nereusstream.objectstore.compacted;

import com.nereusstream.api.Checksum;
import com.nereusstream.api.EntryIndexLocation;
import com.nereusstream.api.EntryIndexRef;
import com.nereusstream.api.ErrorCode;
import com.nereusstream.api.NereusException;
import com.nereusstream.api.ObjectId;
import com.nereusstream.api.ObjectKey;
import com.nereusstream.api.ObjectKeyHash;
import com.nereusstream.api.ReadView;
import com.nereusstream.objectstore.Crc32cChecksums;
import com.nereusstream.objectstore.staging.PrivateStagedObjectFile;
import com.nereusstream.objectstore.staging.StagingFileManager;
import java.io.ByteArrayOutputStream;
import java.io.IOException;
import java.io.OutputStream;
import java.nio.ByteBuffer;
import java.nio.charset.StandardCharsets;
import java.util.ArrayDeque;
import java.util.ArrayList;
import java.util.List;
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

/** Apache Parquet NCP1/NTC1 writer with exact record-bounded row groups and private staging. */
public final class ParquetCompactedObjectWriter implements CompactedObjectWriter {
    private static final int COLUMN_INDEX_TRUNCATE_LENGTH = 64;

    private final StagingFileManager stagingFiles;
    private final Executor writerExecutor;

    public ParquetCompactedObjectWriter(
            StagingFileManager stagingFiles,
            Executor writerExecutor) {
        this.stagingFiles = Objects.requireNonNull(stagingFiles, "stagingFiles");
        this.writerExecutor = Objects.requireNonNull(writerExecutor, "writerExecutor");
    }

    @Override
    public CompletableFuture<CompactedObjectWriteResult> write(
            CompactedObjectWriteRequest request,
            Flow.Publisher<CompactedObjectRow> rows) {
        CompletableFuture<CompactedObjectWriteResult> result = new CompletableFuture<>();
        if (request == null || rows == null) {
            result.completeExceptionally(new NereusException(
                    ErrorCode.INVALID_ARGUMENT,
                    false,
                    "compacted write request and row publisher are required"));
            return result;
        }
        SerialExecutor serial = new SerialExecutor(writerExecutor);
        AtomicReference<WritingSubscriber> admitted = new AtomicReference<>();
        try {
            serial.execute(() -> {
                if (result.isCancelled()) {
                    return;
                }
                try {
                    WritingSubscriber subscriber = new WritingSubscriber(request, result, serial);
                    admitted.set(subscriber);
                    rows.subscribe(subscriber);
                } catch (Throwable failure) {
                    result.completeExceptionally(mapFailure("initialize compacted writer", failure));
                }
            });
        } catch (RejectedExecutionException failure) {
            result.completeExceptionally(new NereusException(
                    ErrorCode.STORAGE_CLOSED,
                    false,
                    "compacted writer executor rejected the operation",
                    failure));
        }
        result.whenComplete((ignored, failure) -> {
            if (result.isCancelled()) {
                WritingSubscriber subscriber = admitted.get();
                if (subscriber != null) {
                    subscriber.cancel();
                }
            }
        });
        return result;
    }

    private final class WritingSubscriber implements Flow.Subscriber<CompactedObjectRow> {
        private final CompactedObjectWriteRequest request;
        private final CompletableFuture<CompactedObjectWriteResult> result;
        private final SerialExecutor serial;
        private final ParquetSink sink;
        private final AtomicBoolean terminal = new AtomicBoolean();
        private final AtomicBoolean awaitingItem = new AtomicBoolean();
        private volatile Flow.Subscription subscription;

        private WritingSubscriber(
                CompactedObjectWriteRequest request,
                CompletableFuture<CompactedObjectWriteResult> result,
                SerialExecutor serial) throws IOException {
            this.request = request;
            this.result = result;
            this.serial = serial;
            this.sink = new ParquetSink(request, stagingFiles.create("compacted"));
        }

        @Override
        public void onSubscribe(Flow.Subscription value) {
            Objects.requireNonNull(value, "subscription");
            if (subscription != null || terminal.get()) {
                value.cancel();
                fail(new IllegalStateException("compacted row publisher subscribed more than once"));
                return;
            }
            subscription = value;
            requestOne();
        }

        @Override
        public void onNext(CompactedObjectRow row) {
            if (!awaitingItem.compareAndSet(true, false)) {
                fail(new IllegalStateException("compacted row publisher emitted without demand"));
                return;
            }
            submit(() -> {
                if (terminal.get()) {
                    return;
                }
                try {
                    sink.write(Objects.requireNonNull(row, "row"));
                    requestOne();
                } catch (Throwable failure) {
                    fail(failure);
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
                    CompactedObjectWriteResult completed = sink.finish();
                    if (!result.complete(completed)) {
                        completed.close();
                    }
                } catch (Throwable failure) {
                    sink.close();
                    result.completeExceptionally(mapFailure("finish compacted object", failure));
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
                    fail(new IllegalStateException("row publisher omitted subscription"));
                    return;
                }
                awaitingItem.set(true);
                try {
                    value.request(1);
                } catch (Throwable failure) {
                    awaitingItem.set(false);
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
            result.completeExceptionally(mapFailure("write compacted object", failure));
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

    private static final class ParquetSink implements AutoCloseable {
        private final CompactedObjectWriteRequest request;
        private final PrivateStagedObjectFile stagingFile;
        private final CapturingPositionOutputStream output;
        private final ParquetFileWriter fileWriter;
        private final CodecFactory codecFactory;
        private final CompressionCodecFactory.BytesInputCompressor compressor;
        private final ParquetProperties properties;
        private final MessageType schema;
        private final SimpleGroupFactory groups;
        private final List<Group> rowGroup = new ArrayList<>();
        private long rowGroupPayloadBytes;
        private long totalPayloadBytes;
        private long previousOffset = -1;
        private int outputRecords;
        private int rowGroupCount;
        private boolean finished;

        private ParquetSink(
                CompactedObjectWriteRequest request,
                PrivateStagedObjectFile stagingFile) throws IOException {
            this.request = request;
            this.stagingFile = stagingFile;
            this.schema = CompactedObjectFormatV1.schema(request.view());
            this.groups = new SimpleGroupFactory(schema);
            Configuration configuration = new Configuration(false);
            configuration.setInt("io.compression.codec.zstd.level", CompactedObjectFormatV1.ZSTD_LEVEL);
            this.codecFactory = new CodecFactory(configuration, CompactedObjectFormatV1.DATA_PAGE_BYTES);
            CompressionCodecName codec = request.compression().equals("ZSTD")
                    ? CompressionCodecName.ZSTD
                    : CompressionCodecName.UNCOMPRESSED;
            this.compressor = codecFactory.getCompressor(codec);
            this.properties = ParquetProperties.builder()
                    .withPageSize(CompactedObjectFormatV1.DATA_PAGE_BYTES)
                    .withPageRowCountLimit(request.targetRowGroupRecords())
                    .withDictionaryEncoding(false)
                    .withWriterVersion(ParquetProperties.WriterVersion.PARQUET_2_0)
                    .withBloomFilterEnabled(false)
                    .withPageWriteChecksumEnabled(true)
                    .withColumnIndexTruncateLength(COLUMN_INDEX_TRUNCATE_LENGTH)
                    .withStatisticsEnabled(true)
                    .build();
            this.output = new CapturingPositionOutputStream(stagingFile.outputStream());
            this.fileWriter = new ParquetFileWriter(
                    new SingleStreamOutputFile(output),
                    schema,
                    ParquetFileWriter.Mode.CREATE,
                    CompactedObjectFormatV1.MAX_ROW_GROUP_BUFFER_BYTES,
                    0);
            try {
                fileWriter.start();
            } catch (Throwable failure) {
                close();
                throw failure;
            }
        }

        private void write(CompactedObjectRow row) throws IOException {
            byte[] payload = bytes(row.exactPayload(), CompactedObjectFormatV1.MAX_PAYLOAD_BYTES, "payload");
            validateRow(row, payload);
            if (!rowGroup.isEmpty()
                    && (rowGroup.size() >= request.targetRowGroupRecords()
                            || rowGroupPayloadBytes + payload.length
                                    > CompactedObjectFormatV1.MAX_ROW_GROUP_BUFFER_BYTES)) {
                flushRowGroup();
            }
            rowGroup.add(toGroup(row, payload));
            rowGroupPayloadBytes = Math.addExact(rowGroupPayloadBytes, payload.length);
            totalPayloadBytes = Math.addExact(totalPayloadBytes, payload.length);
            if (totalPayloadBytes > CompactedObjectFormatV1.MAX_OBJECT_BYTES) {
                throw new CompactedObjectFormatException("compacted payload exceeds the V1 1 GiB limit");
            }
            previousOffset = row.streamOffset();
            outputRecords = Math.addExact(outputRecords, 1);
            if (outputRecords > request.expectedOutputRecordCount()) {
                throw new CompactedObjectFormatException("row publisher exceeded expected output count");
            }
        }

        private CompactedObjectWriteResult finish() throws IOException {
            if (finished) {
                throw new IllegalStateException("compacted writer was already finished");
            }
            if (!rowGroup.isEmpty()) {
                flushRowGroup();
            }
            validateTerminalAccounting();
            output.startCapture();
            fileWriter.end(CompactedObjectFormatV1.metadata(request));
            byte[] footer = extractStandardFooter(output.stopCapture());
            long footerOffset = Math.subtractExact(output.getPos(), footer.length);
            fileWriter.close();
            codecFactory.release();
            if (output.getPos() > CompactedObjectFormatV1.MAX_OBJECT_BYTES) {
                throw new CompactedObjectFormatException(
                        "compacted Parquet object exceeds the V1 1 GiB limit");
            }
            PrivateStagedObjectFile sealed = stagingFile.seal();
            Checksum footerCrc32c = Crc32cChecksums.checksum(footer);
            ObjectKey objectKey = CompactedObjectFormatV1.objectKey(request, sealed.contentSha256());
            ObjectId objectId = CompactedObjectFormatV1.objectId(objectKey);
            ObjectKeyHash keyHash = CompactedObjectFormatV1.objectKeyHash(objectKey);
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
                return new CompactedObjectWriteResult(
                        sealed,
                        objectId,
                        objectKey,
                        keyHash,
                        sealed.sealedLength(),
                        sealed.storageCrc32c(),
                        sealed.contentSha256(),
                        CompactedObjectFormatV1.physicalFormat(request.view()),
                        footerRef,
                        outputRecords);
            } catch (Throwable failure) {
                sealed.close();
                throw failure;
            }
        }

        private void validateRow(CompactedObjectRow row, byte[] payload) {
            int actualCrc = Crc32cChecksums.intValue(Crc32cChecksums.checksum(payload));
            if (actualCrc != row.payloadCrc32c()) {
                throw new CompactedObjectFormatException("row payload CRC32C does not match exact bytes");
            }
            if (!request.sourceCoverage().contains(row.streamOffset())) {
                throw new CompactedObjectFormatException("row offset is outside dense source coverage");
            }
            row.messageKey().ifPresent(value -> requireSize(
                    value, CompactedObjectFormatV1.MAX_OPTIONAL_BINARY_BYTES, "message key"));
            row.orderingKey().ifPresent(value -> requireSize(
                    value, CompactedObjectFormatV1.MAX_OPTIONAL_BINARY_BYTES, "ordering key"));
            row.compactionKey().ifPresent(value -> requireSize(
                    value, CompactedObjectFormatV1.MAX_OPTIONAL_BINARY_BYTES, "compaction key"));
            row.schemaIdentity().ifPresent(value -> {
                if (value.getBytes(StandardCharsets.UTF_8).length
                        > CompactedObjectFormatV1.MAX_SCHEMA_IDENTITY_BYTES) {
                    throw new CompactedObjectFormatException("schema identity exceeds the V1 limit");
                }
            });
            if (request.view() == ReadView.COMMITTED) {
                long expectedOffset = Math.addExact(
                        request.sourceCoverage().startOffset(), outputRecords);
                if (row.streamOffset() != expectedOffset
                        || row.sparseDisposition().isPresent()
                        || row.compactionKey().isPresent()) {
                    throw new CompactedObjectFormatException(
                            "committed compacted rows must be dense and contain no sparse fields");
                }
                return;
            }
            if (row.streamOffset() <= previousOffset
                    || row.sparseDisposition().isEmpty()
                    || row.compactionKey().isEmpty()
                    || !row.compactionKey().orElseThrow().hasRemaining()) {
                throw new CompactedObjectFormatException(
                        "topic-compacted rows must be increasing and carry disposition/key");
            }
            TopicCompactionKeyEncodingV1.validateForOffset(
                    row.compactionKey().orElseThrow(), row.streamOffset());
            int disposition = row.sparseDisposition().getAsInt();
            if (disposition != 1 && disposition != 2) {
                throw new CompactedObjectFormatException("unknown topic-compaction disposition");
            }
            if (disposition == 2 && payload.length != 0) {
                throw new CompactedObjectFormatException("topic-compacted tombstone cannot contain payload");
            }
            if (row.messageKey().isPresent()
                    || row.orderingKey().isPresent()
                    || row.schemaIdentity().isPresent()
                    || row.producerId().isPresent()
                    || row.producerSequenceId().isPresent()
                    || row.batchMessageCount().isPresent()) {
                throw new CompactedObjectFormatException(
                        "topic-compacted rows cannot contain dense-only auxiliary fields");
            }
        }

        private Group toGroup(CompactedObjectRow row, byte[] payload) {
            Group group = groups.newGroup().append("stream_offset", row.streamOffset());
            if (request.view() == ReadView.COMMITTED) {
                group.append("payload", Binary.fromConstantByteArray(payload));
                group.append("payload_crc32c", row.payloadCrc32c());
                row.publishTimeMillis().ifPresent(value -> group.append("publish_time_millis", value));
                row.eventTimeMillis().ifPresent(value -> group.append("event_time_millis", value));
                row.messageKey().ifPresent(value -> group.append(
                        "message_key", Binary.fromConstantByteArray(bytes(value,
                                CompactedObjectFormatV1.MAX_OPTIONAL_BINARY_BYTES, "message key"))));
                row.orderingKey().ifPresent(value -> group.append(
                        "ordering_key", Binary.fromConstantByteArray(bytes(value,
                                CompactedObjectFormatV1.MAX_OPTIONAL_BINARY_BYTES, "ordering key"))));
                row.schemaIdentity().ifPresent(value -> group.append(
                        "schema_identity", Binary.fromString(value)));
                row.producerId().ifPresent(value -> group.append("producer_id", value));
                row.producerSequenceId().ifPresent(value -> group.append("producer_sequence_id", value));
                row.batchMessageCount().ifPresent(value -> group.append("batch_message_count", value));
                return group;
            }
            int disposition = row.sparseDisposition().orElseThrow();
            group.append("disposition", disposition);
            group.append("compaction_key", Binary.fromConstantByteArray(bytes(
                    row.compactionKey().orElseThrow(),
                    CompactedObjectFormatV1.MAX_OPTIONAL_BINARY_BYTES,
                    "compaction key")));
            if (disposition == 1) {
                group.append("payload", Binary.fromConstantByteArray(payload));
                group.append("payload_crc32c", row.payloadCrc32c());
            }
            row.publishTimeMillis().ifPresent(value -> group.append("publish_time_millis", value));
            row.eventTimeMillis().ifPresent(value -> group.append("event_time_millis", value));
            return group;
        }

        private void flushRowGroup() throws IOException {
            if (rowGroup.isEmpty()) {
                return;
            }
            if (rowGroupCount >= CompactedObjectFormatV1.MAX_ROW_GROUPS) {
                throw new CompactedObjectFormatException("compacted object exceeds the V1 row-group limit");
            }
            fileWriter.startBlock(rowGroup.size());
            ColumnChunkPageWriteStore pageStore = new ColumnChunkPageWriteStore(
                    compressor,
                    schema,
                    HeapByteBufferAllocator.getInstance(),
                    properties.getColumnIndexTruncateLength(),
                    properties.getPageWriteChecksumEnabled(),
                    null,
                    rowGroupCount);
            ColumnWriteStore columnStore = properties.newColumnWriteStore(schema, pageStore, pageStore);
            try {
                MessageColumnIO columnIo = new ColumnIOFactory(true).getColumnIO(schema);
                GroupWriter groupWriter = new GroupWriter(
                        columnIo.getRecordWriter(columnStore), schema);
                for (Group group : rowGroup) {
                    groupWriter.write(group);
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
            if (outputRecords != request.expectedOutputRecordCount()) {
                throw new CompactedObjectFormatException("row publisher ended before expected output count");
            }
            if (request.view() == ReadView.COMMITTED) {
                if (previousOffset != request.sourceCoverage().endOffset() - 1
                        || totalPayloadBytes != request.logicalBytes()) {
                    throw new CompactedObjectFormatException(
                            "committed compacted row coverage/bytes are not exact");
                }
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
                // Partial-file cleanup below is authoritative for a failed write.
            }
            try {
                codecFactory.release();
            } catch (Throwable ignored) {
                // Codec buffers are process-local and no durable state was published.
            }
            stagingFile.close();
        }

        private static byte[] extractStandardFooter(byte[] endBytes) {
            if (endBytes.length < 8) {
                throw new CompactedObjectFormatException("Parquet footer length is outside the V1 limit");
            }
            int end = endBytes.length;
            if (endBytes[end - 4] != 'P'
                    || endBytes[end - 3] != 'A'
                    || endBytes[end - 2] != 'R'
                    || endBytes[end - 1] != '1') {
                throw new CompactedObjectFormatException("Parquet footer magic is missing");
            }
            long metadataLength = Integer.toUnsignedLong(
                    (endBytes[end - 8] & 0xff)
                            | ((endBytes[end - 7] & 0xff) << 8)
                            | ((endBytes[end - 6] & 0xff) << 16)
                            | ((endBytes[end - 5] & 0xff) << 24));
            long footerLength = Math.addExact(metadataLength, 8);
            if (footerLength > CompactedObjectFormatV1.MAX_FOOTER_BYTES
                    || footerLength > endBytes.length) {
                throw new CompactedObjectFormatException("Parquet footer length trailer is inconsistent");
            }
            return java.util.Arrays.copyOfRange(
                    endBytes,
                    Math.toIntExact(endBytes.length - footerLength),
                    endBytes.length);
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
                throw new IOException("Parquet staging stream was already claimed");
            }
            claimed = true;
            return stream;
        }

        @Override
        public PositionOutputStream createOrOverwrite(long blockSizeHint) throws IOException {
            throw new IOException("compacted objects never overwrite staging bytes");
        }

        @Override
        public boolean supportsBlockSize() {
            return false;
        }

        @Override
        public long defaultBlockSize() {
            return CompactedObjectFormatV1.MAX_ROW_GROUP_BUFFER_BYTES;
        }

        @Override
        public String getPath() {
            return "nereus-private-staging://compacted";
        }
    }

    private static final class CapturingPositionOutputStream extends PositionOutputStream {
        private final OutputStream delegate;
        private long position;
        private ByteArrayOutputStream capture;

        private CapturingPositionOutputStream(OutputStream delegate) {
            this.delegate = Objects.requireNonNull(delegate, "delegate");
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
                throw new IllegalStateException("footer capture already active");
            }
            capture = new ByteArrayOutputStream();
        }

        private byte[] stopCapture() {
            if (capture == null) {
                throw new IllegalStateException("footer capture is not active");
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
            Objects.requireNonNull(command, "command");
            boolean schedule;
            synchronized (this) {
                queue.addLast(command);
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
                    // Each submitted action completes its owning future; keep draining cancellation/cleanup work.
                }
            }
        }
    }

    private static byte[] bytes(ByteBuffer value, int maximum, String field) {
        ByteBuffer copy = Objects.requireNonNull(value, field).asReadOnlyBuffer();
        if (copy.remaining() > maximum) {
            throw new CompactedObjectFormatException(field + " exceeds the V1 buffer limit");
        }
        byte[] result = new byte[copy.remaining()];
        copy.get(result);
        return result;
    }

    private static void requireSize(ByteBuffer value, int maximum, String field) {
        if (value.remaining() > maximum) {
            throw new CompactedObjectFormatException(field + " exceeds the V1 buffer limit");
        }
    }

    private static Throwable mapFailure(String action, Throwable failure) {
        Throwable current = failure;
        while (current instanceof java.util.concurrent.CompletionException && current.getCause() != null) {
            current = current.getCause();
        }
        if (current instanceof NereusException) {
            return current;
        }
        if (current instanceof IllegalArgumentException || current instanceof NullPointerException) {
            return new NereusException(ErrorCode.INVALID_ARGUMENT, false, action + " failed", current);
        }
        return new NereusException(ErrorCode.OBJECT_UPLOAD_FAILED, true, action + " failed", current);
    }
}
