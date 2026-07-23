/* Licensed under the Apache License, Version 2.0 */
package com.nereusstream.objectstore.compacted;

import com.nereusstream.api.Checksum;
import com.nereusstream.api.ChecksumType;
import com.nereusstream.api.ErrorCode;
import com.nereusstream.api.FirstEntryPolicy;
import com.nereusstream.api.NereusException;
import com.nereusstream.api.ObjectKey;
import com.nereusstream.api.OffsetRange;
import com.nereusstream.api.ReadBoundaryMode;
import com.nereusstream.api.ReadView;
import com.nereusstream.api.target.ObjectSliceReadTarget;
import com.nereusstream.objectstore.Crc32cChecksums;
import com.nereusstream.objectstore.ObjectStore;
import com.nereusstream.objectstore.RangeReadOptions;
import com.nereusstream.objectstore.RangeReadResult;
import java.io.IOException;
import java.nio.ByteBuffer;
import java.util.ArrayList;
import java.util.List;
import java.util.Objects;
import java.util.Optional;
import java.util.OptionalLong;
import java.util.Set;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.CompletionException;
import java.util.concurrent.Executor;
import java.util.concurrent.RejectedExecutionException;
import org.apache.parquet.ParquetReadOptions;
import org.apache.parquet.column.Encoding;
import org.apache.parquet.column.page.PageReadStore;
import org.apache.parquet.column.statistics.Statistics;
import org.apache.parquet.example.data.Group;
import org.apache.parquet.example.data.simple.convert.GroupRecordConverter;
import org.apache.parquet.hadoop.ParquetFileReader;
import org.apache.parquet.hadoop.metadata.BlockMetaData;
import org.apache.parquet.hadoop.metadata.ColumnChunkMetaData;
import org.apache.parquet.hadoop.metadata.CompressionCodecName;
import org.apache.parquet.io.ColumnIOFactory;
import org.apache.parquet.io.MessageColumnIO;
import org.apache.parquet.io.RecordReader;
import org.apache.parquet.schema.MessageType;

/** Strict shared Parquet transport for NCP2/NTC2; each closed schema has its own decoder. */
final class ParquetV2ReaderSupport {
    private static final Set<Encoding> DICTIONARY_ENCODINGS =
            Set.of(Encoding.PLAIN_DICTIONARY, Encoding.RLE_DICTIONARY);

    private final ObjectStore objectStore;
    private final Executor readerExecutor;

    ParquetV2ReaderSupport(ObjectStore objectStore, Executor readerExecutor) {
        this.objectStore = Objects.requireNonNull(objectStore, "objectStore");
        this.readerExecutor = Objects.requireNonNull(readerExecutor, "readerExecutor");
    }

    CompletableFuture<RangedCompactedObjectReadResult> readNcp2(
            RangedCompactedObjectReadRequest request) {
        return supply(() -> {
            ReadSession session = open(
                    request.target(), request.timeout(), CompactedObjectFormatV2.COMMITTED_SCHEMA);
            try (session) {
                RangedCompactedObjectMetadata metadata = validateFile(
                        session.reader(), request.target(), request.streamId(), request.sourceCoverage(),
                        request.payloadFormat(), ReadView.COMMITTED);
                List<RangedCompactedObjectRow> rows = new ArrayList<>();
                Selection selection = new Selection(
                        request.startOffset(), request.boundaryMode(), request.firstEntryPolicy(),
                        request.maxRecords(), request.maxBytes());
                DecodeTotals totals = decodeAll(session.reader(), metadata, group -> {
                    RangedCompactedObjectRow row = decodeNcp2(group);
                    selection.consider(row.streamOffsetStart(), row.endOffset(), row.recordCount(),
                            row.exactPayload().remaining(), row);
                    return new RowFacts(row.streamOffsetStart(), row.endOffset(), row.recordCount(),
                            row.exactPayload().remaining(), row.entryOrdinal());
                });
                for (Object row : selection.rows()) {
                    rows.add((RangedCompactedObjectRow) row);
                }
                requireNcp2Totals(metadata, totals);
                selection.requireExactBoundary();
                return new RangedCompactedObjectReadResult(
                        metadata, rows, selection.coverageEnd(metadata.sourceCoverage()),
                        session.budget().used(), session.footerBytes());
            }
        });
    }

    CompletableFuture<KafkaTopicCompactedObjectReadResult> readNtc2(
            KafkaTopicCompactedObjectReadRequest request) {
        return supply(() -> {
            ReadSession session = open(
                    request.target(), request.timeout(), CompactedObjectFormatV2.TOPIC_COMPACTED_SCHEMA);
            try (session) {
                RangedCompactedObjectMetadata metadata = validateFile(
                        session.reader(), request.target(), request.streamId(), request.sourceCoverage(),
                        com.nereusstream.api.PayloadFormat.KAFKA_RECORD_BATCH, ReadView.TOPIC_COMPACTED);
                List<KafkaTopicCompactedObjectRow> rows = new ArrayList<>();
                Selection selection = new Selection(
                        request.startOffset(), request.boundaryMode(), request.firstEntryPolicy(),
                        request.maxRecords(), request.maxBytes());
                DecodeTotals totals = decodeAll(session.reader(), metadata, group -> {
                    KafkaTopicCompactedObjectRow row = decodeNtc2(group);
                    selection.consider(row.streamOffsetStart(), row.endOffset(), row.recordCount(),
                            row.exactPayload().remaining(), row);
                    return new RowFacts(row.streamOffsetStart(), row.endOffset(), row.recordCount(),
                            row.exactPayload().remaining(), -1);
                });
                for (Object row : selection.rows()) {
                    rows.add((KafkaTopicCompactedObjectRow) row);
                }
                requireNtc2Totals(metadata, totals);
                selection.requireExactBoundary();
                return new KafkaTopicCompactedObjectReadResult(
                        metadata, rows, selection.coverageEnd(metadata.sourceCoverage()),
                        session.budget().used(), session.footerBytes());
            }
        });
    }

    private <T> CompletableFuture<T> supply(ThrowingSupplier<T> operation) {
        try {
            return CompletableFuture.supplyAsync(() -> {
                try {
                    return operation.get();
                } catch (Throwable failure) {
                    throw new CompletionException(mapFailure(failure));
                }
            }, readerExecutor);
        } catch (RejectedExecutionException failure) {
            return CompletableFuture.failedFuture(new NereusException(
                    ErrorCode.STORAGE_CLOSED,
                    false,
                    "V2 compacted reader executor rejected the operation",
                    failure));
        }
    }

    private ReadSession open(
            ObjectSliceReadTarget target,
            java.time.Duration timeout,
            MessageType schema) throws IOException {
        validateTargetIdentity(target);
        ObjectStoreParquetInputFile.ReadDeadline deadline =
                new ObjectStoreParquetInputFile.ReadDeadline(timeout);
        long footerBytes = target.entryIndexRef().length();
        RangeReadResult footer = objectStore.readRange(
                        target.objectKey(),
                        target.entryIndexRef().offset(),
                        footerBytes,
                        new RangeReadOptions(Optional.of(target.entryIndexRef().checksum()), deadline.remaining()))
                .join();
        if (!footer.key().equals(target.objectKey())
                || footer.offset() != target.entryIndexRef().offset()
                || footer.length() != footerBytes
                || footer.payload().remaining() != footerBytes
                || (footer.checksum().isPresent()
                        && !footer.checksum().orElseThrow().equals(target.entryIndexRef().checksum()))) {
            throw new CompactedObjectFormatException("object store returned a mismatched V2 footer range");
        }
        ObjectStoreParquetInputFile.ReadBudget budget = new ObjectStoreParquetInputFile.ReadBudget(
                Math.addExact(target.objectLength(), Math.multiplyExact(footerBytes, 2)));
        budget.reserve(footerBytes);
        ObjectStoreParquetInputFile input = new ObjectStoreParquetInputFile(
                objectStore, target.objectKey(), target.objectLength(), deadline, budget);
        ParquetReadOptions options = ParquetReadOptions.builder()
                .usePageChecksumVerification(true)
                .useBloomFilter(false)
                .withUseHadoopVectoredIo(false)
                .withMaxAllocationInBytes(CompactedObjectFormatV2.MAX_ROW_GROUP_BUFFER_BYTES)
                .build();
        ParquetFileReader reader = ParquetFileReader.open(input, options);
        if (!reader.getFileMetaData().getSchema().equals(schema)) {
            reader.close();
            throw new CompactedObjectFormatException("V2 compacted Parquet schema does not match exact format");
        }
        return new ReadSession(reader, budget, footerBytes);
    }

    private static RangedCompactedObjectMetadata validateFile(
            ParquetFileReader reader,
            ObjectSliceReadTarget target,
            com.nereusstream.api.StreamId streamId,
            OffsetRange coverage,
            com.nereusstream.api.PayloadFormat payloadFormat,
            ReadView view) {
        RangedCompactedObjectMetadata metadata = CompactedObjectFormatV2.parseMetadata(
                reader.getFileMetaData().getKeyValueMetaData());
        if (metadata.view() != view
                || !metadata.streamId().equals(streamId)
                || !metadata.sourceCoverage().equals(coverage)
                || metadata.payloadFormat() != payloadFormat
                || !metadata.logicalFormat().equals(target.logicalFormat())) {
            throw new CompactedObjectFormatException("V2 metadata does not match selected generation");
        }
        validateObjectKey(target.objectKey(), metadata);
        validateRowGroups(reader.getRowGroups(), metadata);
        return metadata;
    }

    private static void validateRowGroups(
            List<BlockMetaData> blocks,
            RangedCompactedObjectMetadata metadata) {
        if (blocks.size() > CompactedObjectFormatV2.MAX_ROW_GROUPS
                || (metadata.entryCount() > 0 && blocks.isEmpty())
                || (metadata.entryCount() == 0 && !blocks.isEmpty())) {
            throw new CompactedObjectFormatException("V2 row-group count is invalid");
        }
        long rows = 0;
        long previousMaximum = -1;
        CompressionCodecName expectedCodec = metadata.compression().equals("ZSTD")
                ? CompressionCodecName.ZSTD
                : CompressionCodecName.UNCOMPRESSED;
        for (BlockMetaData block : blocks) {
            if (block.getRowCount() <= 0 || block.getRowCount() > metadata.targetRowGroupRecords()) {
                throw new CompactedObjectFormatException("V2 row group exceeds its row bound");
            }
            ColumnChunkMetaData offsetColumn = block.getColumns().stream()
                    .filter(column -> column.getPath().toDotString().equals("stream_offset_start"))
                    .findFirst()
                    .orElseThrow(() -> new CompactedObjectFormatException(
                            "V2 row group omits stream_offset_start"));
            Statistics<?> statistics = offsetColumn.getStatistics();
            if (!statistics.hasNonNullValue()
                    || !(statistics.genericGetMin() instanceof Long minimum)
                    || !(statistics.genericGetMax() instanceof Long maximum)
                    || minimum < metadata.sourceCoverage().startOffset()
                    || maximum >= metadata.sourceCoverage().endOffset()
                    || minimum > maximum
                    || (previousMaximum >= 0 && minimum <= previousMaximum)) {
                throw new CompactedObjectFormatException("V2 row-group offset statistics are inconsistent");
            }
            for (ColumnChunkMetaData column : block.getColumns()) {
                if (column.getCodec() != expectedCodec
                        || column.getEncodings().stream().anyMatch(DICTIONARY_ENCODINGS::contains)
                        || column.getBloomFilterLength() > 0) {
                    throw new CompactedObjectFormatException("V2 column codec/dictionary/bloom profile is invalid");
                }
            }
            previousMaximum = maximum;
            rows = Math.addExact(rows, block.getRowCount());
        }
        if (rows != metadata.entryCount()) {
            throw new CompactedObjectFormatException("V2 row-group row count is incomplete");
        }
    }

    private static DecodeTotals decodeAll(
            ParquetFileReader reader,
            RangedCompactedObjectMetadata metadata,
            RowDecoder decoder) throws IOException {
        MessageType schema = CompactedObjectFormatV2.schema(metadata.view());
        MessageColumnIO columnIo = new ColumnIOFactory(true).getColumnIO(schema);
        long previousEnd = -1;
        long records = 0;
        long bytes = 0;
        int rows = 0;
        for (int blockIndex = 0; blockIndex < reader.getRowGroups().size(); blockIndex++) {
            try (PageReadStore pages = reader.readRowGroup(blockIndex)) {
                GroupRecordConverter converter = new GroupRecordConverter(schema);
                RecordReader<Group> recordReader = columnIo.getRecordReader(pages, converter);
                for (long rowIndex = 0; rowIndex < pages.getRowCount(); rowIndex++) {
                    RowFacts facts = decoder.decode(recordReader.read());
                    if (facts.startOffset() < metadata.sourceCoverage().startOffset()
                            || facts.endOffset() > metadata.sourceCoverage().endOffset()
                            || facts.endOffset() <= facts.startOffset()
                            || (rows > 0 && facts.startOffset() < previousEnd)) {
                        throw new CompactedObjectFormatException("V2 decoded row range ordering is invalid");
                    }
                    if (metadata.view() == ReadView.COMMITTED
                            && ((rows == 0 && facts.startOffset() != metadata.sourceCoverage().startOffset())
                                    || (rows > 0 && facts.startOffset() != previousEnd)
                                    || facts.ordinal() != rows)) {
                        throw new CompactedObjectFormatException("NCP2 decoded rows are not dense/ordinal");
                    }
                    previousEnd = facts.endOffset();
                    records = Math.addExact(records, facts.recordCount());
                    bytes = Math.addExact(bytes, facts.payloadBytes());
                    rows = Math.addExact(rows, 1);
                }
            }
        }
        return new DecodeTotals(rows, records, bytes, previousEnd);
    }

    private static RangedCompactedObjectRow decodeNcp2(Group group) {
        byte[] payload = requiredBinary(group, "payload");
        RangedCompactedObjectRow row = new RangedCompactedObjectRow(
                group.getLong("stream_offset_start", 0),
                group.getInteger("record_count", 0),
                group.getInteger("entry_ordinal", 0),
                ByteBuffer.wrap(payload),
                group.getInteger("payload_crc32c", 0),
                optionalLong(group, "event_time_millis"));
        requirePayloadCrc(payload, row.payloadCrc32c());
        return row;
    }

    private static KafkaTopicCompactedObjectRow decodeNtc2(Group group) {
        byte[] payload = requiredBinary(group, "payload");
        byte[] sourceSha = requiredBinary(group, "source_batch_sha256");
        if (sourceSha.length != 32) {
            throw new CompactedObjectFormatException("NTC2 source batch digest must be 32 bytes");
        }
        KafkaTopicCompactedObjectRow row = new KafkaTopicCompactedObjectRow(
                group.getLong("stream_offset_start", 0),
                group.getInteger("record_count", 0),
                KafkaCompactionDispositionV2.fromWireId(group.getInteger("disposition", 0)),
                ByteBuffer.wrap(requiredBinary(group, "compaction_key")),
                ByteBuffer.wrap(payload),
                group.getInteger("payload_crc32c", 0),
                group.getLong("source_batch_base_offset", 0),
                group.getInteger("source_record_index", 0),
                new Checksum(ChecksumType.SHA256, java.util.HexFormat.of().formatHex(sourceSha)),
                optionalLong(group, "event_time_millis"));
        requirePayloadCrc(payload, row.payloadCrc32c());
        return row;
    }

    private static void requireNcp2Totals(
            RangedCompactedObjectMetadata metadata,
            DecodeTotals totals) {
        if (totals.rows() != metadata.entryCount()
                || totals.records() != metadata.sourceRecordCount()
                || totals.bytes() != metadata.logicalBytes()
                || totals.previousEndOffset() != metadata.sourceCoverage().endOffset()) {
            throw new CompactedObjectFormatException("NCP2 decoded accounting is incomplete");
        }
    }

    private static void requireNtc2Totals(
            RangedCompactedObjectMetadata metadata,
            DecodeTotals totals) {
        if (totals.rows() != metadata.entryCount()
                || totals.records() != metadata.outputRecordCount()
                || totals.bytes() != metadata.logicalBytes()) {
            throw new CompactedObjectFormatException("NTC2 decoded accounting is incomplete");
        }
    }

    private static byte[] requiredBinary(Group group, String field) {
        if (group.getFieldRepetitionCount(field) != 1) {
            throw new CompactedObjectFormatException("required V2 field is missing: " + field);
        }
        return group.getBinary(field, 0).getBytes();
    }

    private static OptionalLong optionalLong(Group group, String field) {
        int count = group.getFieldRepetitionCount(field);
        if (count > 1) {
            throw new CompactedObjectFormatException("optional V2 field is repeated: " + field);
        }
        return count == 0 ? OptionalLong.empty() : OptionalLong.of(group.getLong(field, 0));
    }

    private static void requirePayloadCrc(byte[] payload, int expected) {
        if (Crc32cChecksums.intValue(Crc32cChecksums.checksum(payload)) != expected) {
            throw new CompactedObjectFormatException("V2 row payload CRC32C does not match exact bytes");
        }
    }

    private static void validateTargetIdentity(ObjectSliceReadTarget target) {
        if (!target.objectId().equals(CompactedObjectFormatV2.objectId(target.objectKey()))
                || target.sliceChecksum().type() != ChecksumType.CRC32C
                || target.entryIndexRef().checksum().type() != ChecksumType.CRC32C) {
            throw new CompactedObjectFormatException("V2 target object/checksum identity is invalid");
        }
    }

    private static void validateObjectKey(ObjectKey key, RangedCompactedObjectMetadata metadata) {
        String expectedView = metadata.view() == ReadView.COMMITTED ? "committed" : "topic-compacted-kafka";
        String stream = com.nereusstream.api.keys.KeyComponentCodec.encodeComponent(metadata.streamId().value());
        String range = com.nereusstream.api.keys.KeyComponentCodec.encodeNonNegativeLong(
                        metadata.sourceCoverage().startOffset())
                + "-"
                + com.nereusstream.api.keys.KeyComponentCodec.encodeNonNegativeLong(
                        metadata.sourceCoverage().endOffset());
        String[] components = key.value().split("/", -1);
        if (components.length != 7
                || components[0].isEmpty()
                || !components[1].equals("compacted")
                || !components[2].equals("v2")
                || !components[3].equals(expectedView)
                || !components[4].equals(stream)
                || !components[5].equals(range)
                || !components[6].endsWith("-" + metadata.outputAttemptId() + ".parquet")) {
            throw new CompactedObjectFormatException("V2 object key does not match file metadata");
        }
        int dash = components[6].indexOf('-');
        if (dash != 64 || !components[6].substring(0, dash).matches("[0-9a-f]{64}")) {
            throw new CompactedObjectFormatException("V2 object key content hash is not canonical");
        }
    }

    private static Throwable mapFailure(Throwable failure) {
        Throwable current = failure;
        while (current instanceof CompletionException && current.getCause() != null) {
            current = current.getCause();
        }
        for (Throwable cursor = current; cursor != null; cursor = cursor.getCause()) {
            if (cursor instanceof NereusException nereus) {
                return nereus;
            }
        }
        if (current instanceof IOException
                || current instanceof ArithmeticException
                || current instanceof IllegalArgumentException
                || current instanceof IllegalStateException) {
            return new CompactedObjectFormatException("cannot decode V2 compacted Parquet bytes", current);
        }
        return new CompactedObjectFormatException("cannot read immutable V2 compacted object", current);
    }

    private static final class Selection {
        private final long requested;
        private final ReadBoundaryMode boundaryMode;
        private final FirstEntryPolicy firstEntryPolicy;
        private final int maxRecords;
        private final int maxBytes;
        private final List<Object> rows = new ArrayList<>();
        private long returnedRecords;
        private long returnedBytes;
        private long lastEnd = -1;
        private boolean limited;
        private boolean requestInsideEntry;

        private Selection(
                long requested,
                ReadBoundaryMode boundaryMode,
                FirstEntryPolicy firstEntryPolicy,
                int maxRecords,
                int maxBytes) {
            this.requested = requested;
            this.boundaryMode = boundaryMode;
            this.firstEntryPolicy = firstEntryPolicy;
            this.maxRecords = maxRecords;
            this.maxBytes = maxBytes;
        }

        private void consider(long start, long end, int records, int bytes, Object row) {
            if (start < requested && requested < end) {
                requestInsideEntry = true;
            }
            boolean candidate = boundaryMode == ReadBoundaryMode.EXACT_START
                    ? start >= requested
                    : end > requested;
            if (!candidate || limited) {
                return;
            }
            if (boundaryMode == ReadBoundaryMode.EXACT_START && requestInsideEntry) {
                return;
            }
            boolean exceeds = records > maxRecords - returnedRecords || bytes > maxBytes - returnedBytes;
            boolean allowOverflow = rows.isEmpty()
                    && firstEntryPolicy == FirstEntryPolicy.ALLOW_FIRST_ENTRY_OVERFLOW;
            if (exceeds && !allowOverflow) {
                if (rows.isEmpty()) {
                    throw new NereusException(
                            ErrorCode.READ_LIMIT_TOO_SMALL, false, "V2 entry exceeds caller read limits");
                }
                limited = true;
                return;
            }
            rows.add(row);
            returnedRecords = Math.addExact(returnedRecords, records);
            returnedBytes = Math.addExact(returnedBytes, bytes);
            lastEnd = end;
            if (exceeds) {
                limited = true;
            }
        }

        private void requireExactBoundary() {
            if (boundaryMode == ReadBoundaryMode.EXACT_START && requestInsideEntry) {
                throw new NereusException(
                        ErrorCode.OFFSET_NOT_AVAILABLE,
                        false,
                        "requested offset is inside a ranged V2 entry");
            }
        }

        private long coverageEnd(OffsetRange coverage) {
            if (!limited) {
                return coverage.endOffset();
            }
            return lastEnd < 0 ? requested : lastEnd;
        }

        private List<Object> rows() {
            return rows;
        }
    }

    private record ReadSession(
            ParquetFileReader reader,
            ObjectStoreParquetInputFile.ReadBudget budget,
            long footerBytes) implements AutoCloseable {
        @Override
        public void close() {
            try {
                reader.close();
            } catch (IOException failure) {
                throw new CompactedObjectFormatException("cannot close V2 Parquet reader", failure);
            }
        }
    }

    private record RowFacts(
            long startOffset,
            long endOffset,
            int recordCount,
            int payloadBytes,
            int ordinal) {
    }

    private record DecodeTotals(
            int rows,
            long records,
            long bytes,
            long previousEndOffset) {
    }

    @FunctionalInterface
    private interface RowDecoder {
        RowFacts decode(Group group);
    }

    @FunctionalInterface
    private interface ThrowingSupplier<T> {
        T get() throws Exception;
    }
}
