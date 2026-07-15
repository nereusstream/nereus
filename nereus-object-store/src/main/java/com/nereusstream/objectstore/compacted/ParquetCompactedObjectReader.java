/* Licensed under the Apache License, Version 2.0 */
package com.nereusstream.objectstore.compacted;

import com.nereusstream.api.ChecksumType;
import com.nereusstream.api.ErrorCode;
import com.nereusstream.api.NereusException;
import com.nereusstream.api.ObjectId;
import com.nereusstream.api.ObjectKey;
import com.nereusstream.api.ReadView;
import com.nereusstream.objectstore.Crc32cChecksums;
import com.nereusstream.objectstore.ObjectStore;
import com.nereusstream.objectstore.RangeReadOptions;
import java.io.IOException;
import java.nio.ByteBuffer;
import java.nio.charset.StandardCharsets;
import java.util.ArrayList;
import java.util.List;
import java.util.Map;
import java.util.Objects;
import java.util.Optional;
import java.util.OptionalInt;
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

/** Strict range reader for NCP1 and the protocol-neutral NTC1 storage primitive. */
public final class ParquetCompactedObjectReader implements CompactedObjectReader {
    private static final Set<Encoding> DICTIONARY_ENCODINGS =
            Set.of(Encoding.PLAIN_DICTIONARY, Encoding.RLE_DICTIONARY);

    private final ObjectStore objectStore;
    private final Executor readerExecutor;

    public ParquetCompactedObjectReader(ObjectStore objectStore, Executor readerExecutor) {
        this.objectStore = Objects.requireNonNull(objectStore, "objectStore");
        this.readerExecutor = Objects.requireNonNull(readerExecutor, "readerExecutor");
    }

    @Override
    public CompletableFuture<CompactedObjectReadResult> read(CompactedObjectReadRequest request) {
        if (request == null) {
            return CompletableFuture.failedFuture(new NereusException(
                    ErrorCode.INVALID_ARGUMENT, false, "compacted read request is required"));
        }
        try {
            return CompletableFuture.supplyAsync(() -> readBlocking(request), readerExecutor);
        } catch (RejectedExecutionException failure) {
            return CompletableFuture.failedFuture(new NereusException(
                    ErrorCode.STORAGE_CLOSED,
                    false,
                    "compacted reader executor rejected the operation",
                    failure));
        }
    }

    private CompactedObjectReadResult readBlocking(CompactedObjectReadRequest request) {
        try {
            validateTargetIdentity(request);
            ObjectStoreParquetInputFile.ReadDeadline deadline =
                    new ObjectStoreParquetInputFile.ReadDeadline(request.timeout());
            long footerBytes = request.target().entryIndexRef().length();
            objectStore.readRange(
                            request.target().objectKey(),
                            request.target().entryIndexRef().offset(),
                            footerBytes,
                            new RangeReadOptions(
                                    Optional.of(request.target().entryIndexRef().checksum()),
                                    deadline.remaining()))
                    .join();
            long maximumReadBytes = Math.addExact(
                    request.target().objectLength(), Math.multiplyExact(footerBytes, 2));
            ObjectStoreParquetInputFile.ReadBudget budget =
                    new ObjectStoreParquetInputFile.ReadBudget(maximumReadBytes);
            budget.reserve(footerBytes);
            ObjectStoreParquetInputFile input = new ObjectStoreParquetInputFile(
                    objectStore,
                    request.target().objectKey(),
                    request.target().objectLength(),
                    deadline,
                    budget);
            ParquetReadOptions readOptions = ParquetReadOptions.builder()
                    .usePageChecksumVerification(true)
                    .useBloomFilter(false)
                    .withUseHadoopVectoredIo(false)
                    .withMaxAllocationInBytes(CompactedObjectFormatV1.MAX_ROW_GROUP_BUFFER_BYTES)
                    .build();
            try (ParquetFileReader reader = ParquetFileReader.open(input, readOptions)) {
                CompactedObjectMetadata metadata = validateFile(reader, request);
                ReadRows rows = readRows(reader, request, metadata);
                return new CompactedObjectReadResult(
                        metadata,
                        rows.rows(),
                        rows.sourceCoverageEndOffset(),
                        budget.used(),
                        footerBytes);
            }
        } catch (Throwable failure) {
            throw new CompletionException(mapFailure(failure));
        }
    }

    private static CompactedObjectMetadata validateFile(
            ParquetFileReader reader,
            CompactedObjectReadRequest request) {
        if (!reader.getFileMetaData().getSchema().equals(
                CompactedObjectFormatV1.schema(request.view()))) {
            throw new CompactedObjectFormatException("compacted Parquet schema is not the frozen V1 schema");
        }
        CompactedObjectMetadata metadata = CompactedObjectFormatV1.parseMetadata(
                reader.getFileMetaData().getKeyValueMetaData());
        if (metadata.view() != request.view()
                || !metadata.streamId().equals(request.streamId())
                || !metadata.sourceCoverage().equals(request.sourceCoverage())
                || metadata.payloadFormat() != request.payloadFormat()
                || !metadata.logicalFormat().equals(request.target().logicalFormat())) {
            throw new CompactedObjectFormatException(
                    "compacted Parquet metadata does not match the selected generation");
        }
        validateObjectKey(request.target().objectKey(), metadata);
        validateRowGroups(reader.getRowGroups(), metadata);
        return metadata;
    }

    private static void validateRowGroups(
            List<BlockMetaData> blocks,
            CompactedObjectMetadata metadata) {
        if (blocks.size() > CompactedObjectFormatV1.MAX_ROW_GROUPS
                || (metadata.outputRecordCount() > 0 && blocks.isEmpty())) {
            throw new CompactedObjectFormatException("compacted Parquet row-group count is invalid");
        }
        long rows = 0;
        long previousMaximum = -1;
        CompressionCodecName expectedCodec = metadata.compression().equals("ZSTD")
                ? CompressionCodecName.ZSTD
                : CompressionCodecName.UNCOMPRESSED;
        for (BlockMetaData block : blocks) {
            long rowCount = block.getRowCount();
            if (rowCount <= 0 || rowCount > metadata.targetRowGroupRecords()) {
                throw new CompactedObjectFormatException("compacted row group exceeds its record bound");
            }
            ColumnChunkMetaData offsetColumn = block.getColumns().stream()
                    .filter(column -> column.getPath().toDotString().equals("stream_offset"))
                    .findFirst()
                    .orElseThrow(() -> new CompactedObjectFormatException(
                            "compacted row group omits stream_offset"));
            Statistics<?> statistics = offsetColumn.getStatistics();
            if (!statistics.hasNonNullValue()
                    || !(statistics.genericGetMin() instanceof Long minimum)
                    || !(statistics.genericGetMax() instanceof Long maximum)
                    || minimum < metadata.sourceCoverage().startOffset()
                    || maximum >= metadata.sourceCoverage().endOffset()
                    || minimum > maximum
                    || (previousMaximum >= 0 && minimum <= previousMaximum)) {
                throw new CompactedObjectFormatException(
                        "compacted row-group offset statistics are missing or inconsistent");
            }
            if (metadata.view() == ReadView.COMMITTED
                    && ((previousMaximum >= 0 && minimum != previousMaximum + 1)
                            || maximum - minimum + 1 != rowCount)) {
                throw new CompactedObjectFormatException(
                        "committed row-group statistics are not dense");
            }
            if (metadata.view() == ReadView.COMMITTED
                    && previousMaximum < 0
                    && minimum != metadata.sourceCoverage().startOffset()) {
                throw new CompactedObjectFormatException(
                        "first committed row group does not begin at source coverage");
            }
            for (ColumnChunkMetaData column : block.getColumns()) {
                if (column.getCodec() != expectedCodec
                        || column.getEncodings().stream().anyMatch(DICTIONARY_ENCODINGS::contains)
                        || column.getBloomFilterLength() > 0) {
                    throw new CompactedObjectFormatException(
                            "compacted column codec/dictionary/bloom profile is invalid");
                }
            }
            previousMaximum = maximum;
            rows = Math.addExact(rows, rowCount);
        }
        if (rows != metadata.outputRecordCount()
                || (metadata.view() == ReadView.COMMITTED
                        && previousMaximum != metadata.sourceCoverage().endOffset() - 1)) {
            throw new CompactedObjectFormatException("compacted row-group coverage/count is incomplete");
        }
    }

    private static ReadRows readRows(
            ParquetFileReader reader,
            CompactedObjectReadRequest request,
            CompactedObjectMetadata metadata) throws IOException {
        List<CompactedObjectRow> returned = new ArrayList<>();
        long returnedBytes = 0;
        long coverageCursor = request.startOffset();
        long previousDecodedOffset = -1;
        boolean limited = false;
        MessageColumnIO columnIo = new ColumnIOFactory(true)
                .getColumnIO(CompactedObjectFormatV1.schema(request.view()));
        List<BlockMetaData> blocks = reader.getRowGroups();
        for (int blockIndex = 0; blockIndex < blocks.size() && !limited; blockIndex++) {
            BlockMetaData block = blocks.get(blockIndex);
            OffsetStats stats = offsetStats(block);
            if (stats.maximum() < request.startOffset()) {
                continue;
            }
            try (PageReadStore pages = reader.readRowGroup(blockIndex)) {
                GroupRecordConverter converter = new GroupRecordConverter(
                        CompactedObjectFormatV1.schema(request.view()));
                RecordReader<Group> records = columnIo.getRecordReader(pages, converter);
                for (long rowIndex = 0; rowIndex < pages.getRowCount(); rowIndex++) {
                    CompactedObjectRow row = decode(records.read(), request.view());
                    validateDecodedRow(row, metadata, previousDecodedOffset);
                    previousDecodedOffset = row.streamOffset();
                    if (row.streamOffset() < request.startOffset()) {
                        continue;
                    }
                    int payloadBytes = row.exactPayload().remaining();
                    if (returned.size() >= request.maxRecords()
                            || payloadBytes > request.maxBytes() - returnedBytes) {
                        if (returned.isEmpty()) {
                            throw new NereusException(
                                    ErrorCode.READ_LIMIT_TOO_SMALL,
                                    false,
                                    "compacted row exceeds caller read limits");
                        }
                        limited = true;
                        break;
                    }
                    returned.add(row);
                    returnedBytes = Math.addExact(returnedBytes, payloadBytes);
                    coverageCursor = row.streamOffset() + 1;
                }
            }
        }
        if (request.view() == ReadView.COMMITTED) {
            if (returned.isEmpty()
                    || returned.get(0).streamOffset() != request.startOffset()) {
                throw new CompactedObjectFormatException(
                        "committed compacted read did not return the requested dense offset");
            }
            for (int index = 1; index < returned.size(); index++) {
                if (returned.get(index).streamOffset()
                        != returned.get(index - 1).streamOffset() + 1) {
                    throw new CompactedObjectFormatException(
                            "committed compacted read returned a sparse sequence");
                }
            }
        } else if (!limited) {
            coverageCursor = metadata.sourceCoverage().endOffset();
        }
        return new ReadRows(returned, coverageCursor);
    }

    private static CompactedObjectRow decode(Group group, ReadView view) {
        long offset = group.getLong("stream_offset", 0);
        OptionalLong publishTime = optionalLong(group, "publish_time_millis");
        OptionalLong eventTime = optionalLong(group, "event_time_millis");
        if (view == ReadView.COMMITTED) {
            byte[] payload = requiredBinary(group, "payload");
            int crc = group.getInteger("payload_crc32c", 0);
            return new CompactedObjectRow(
                    offset,
                    ByteBuffer.wrap(payload),
                    crc,
                    publishTime,
                    eventTime,
                    optionalBinary(group, "message_key"),
                    optionalBinary(group, "ordering_key"),
                    optionalUtf8(group, "schema_identity"),
                    optionalLong(group, "producer_id"),
                    optionalLong(group, "producer_sequence_id"),
                    optionalInt(group, "batch_message_count"),
                    OptionalInt.empty(),
                    Optional.empty());
        }
        int disposition = group.getInteger("disposition", 0);
        Optional<ByteBuffer> payload = optionalBinary(group, "payload");
        OptionalInt crc = optionalInt(group, "payload_crc32c");
        if ((disposition == 1) != (payload.isPresent() && crc.isPresent())
                || (disposition == 2 && (payload.isPresent() || crc.isPresent()))) {
            throw new CompactedObjectFormatException("topic-compacted payload/disposition is inconsistent");
        }
        return new CompactedObjectRow(
                offset,
                payload.orElseGet(() -> ByteBuffer.allocate(0)),
                crc.orElseGet(() -> Crc32cChecksums.intValue(
                        Crc32cChecksums.checksum(new byte[0]))),
                publishTime,
                eventTime,
                Optional.empty(),
                Optional.empty(),
                Optional.empty(),
                OptionalLong.empty(),
                OptionalLong.empty(),
                OptionalInt.empty(),
                OptionalInt.of(disposition),
                Optional.of(ByteBuffer.wrap(requiredBinary(group, "compaction_key"))));
    }

    private static void validateDecodedRow(
            CompactedObjectRow row,
            CompactedObjectMetadata metadata,
            long previousOffset) {
        byte[] payload = new byte[row.exactPayload().remaining()];
        row.exactPayload().get(payload);
        if (Crc32cChecksums.intValue(Crc32cChecksums.checksum(payload)) != row.payloadCrc32c()
                || !metadata.sourceCoverage().contains(row.streamOffset())
                || (previousOffset >= 0 && row.streamOffset() <= previousOffset)) {
            throw new CompactedObjectFormatException(
                    "compacted row offset ordering or payload CRC is invalid");
        }
        if (metadata.view() == ReadView.COMMITTED
                && previousOffset >= 0
                && row.streamOffset() != previousOffset + 1) {
            throw new CompactedObjectFormatException("committed compacted rows are not dense");
        }
        if (metadata.view() == ReadView.TOPIC_COMPACTED
                && (row.sparseDisposition().isEmpty()
                        || row.compactionKey().isEmpty()
                        || !row.compactionKey().orElseThrow().hasRemaining())) {
            throw new CompactedObjectFormatException("topic-compacted row omits key/disposition");
        }
    }

    private static void validateTargetIdentity(CompactedObjectReadRequest request) {
        ObjectKey key = request.target().objectKey();
        ObjectId expectedId = CompactedObjectFormatV1.objectId(key);
        if (!request.target().objectId().equals(expectedId)
                || request.target().sliceChecksum().type() != ChecksumType.CRC32C
                || request.target().entryIndexRef().checksum().type() != ChecksumType.CRC32C) {
            throw new CompactedObjectFormatException("compacted target object/checksum identity is invalid");
        }
    }

    private static void validateObjectKey(ObjectKey key, CompactedObjectMetadata metadata) {
        String streamComponent = com.nereusstream.api.keys.KeyComponentCodec.encodeComponent(
                metadata.streamId().value());
        String rangeComponent = com.nereusstream.api.keys.KeyComponentCodec.encodeNonNegativeLong(
                        metadata.sourceCoverage().startOffset())
                + "-"
                + com.nereusstream.api.keys.KeyComponentCodec.encodeNonNegativeLong(
                        metadata.sourceCoverage().endOffset());
        String viewComponent = metadata.view() == ReadView.COMMITTED
                ? "/compacted/v1/committed/"
                : "/compacted/v1/topic-compacted/";
        String value = key.value();
        if (!value.contains(viewComponent + streamComponent + "/" + rangeComponent + "/")
                || !value.endsWith("-" + metadata.outputAttemptId() + ".parquet")) {
            throw new CompactedObjectFormatException("compacted object key does not match file metadata");
        }
        int slash = value.lastIndexOf('/');
        int dash = value.lastIndexOf('-', value.length() - ".parquet".length());
        if (slash < 0 || dash != slash + 1 + 64 || !isLowerHex(value.substring(slash + 1, dash))) {
            throw new CompactedObjectFormatException("compacted object key content hash is not canonical");
        }
    }

    private static OffsetStats offsetStats(BlockMetaData block) {
        ColumnChunkMetaData offsetColumn = block.getColumns().stream()
                .filter(column -> column.getPath().toDotString().equals("stream_offset"))
                .findFirst()
                .orElseThrow();
        return new OffsetStats(
                (Long) offsetColumn.getStatistics().genericGetMin(),
                (Long) offsetColumn.getStatistics().genericGetMax());
    }

    private static byte[] requiredBinary(Group group, String field) {
        if (group.getFieldRepetitionCount(field) != 1) {
            throw new CompactedObjectFormatException("required compacted field is missing: " + field);
        }
        return group.getBinary(field, 0).getBytes();
    }

    private static Optional<ByteBuffer> optionalBinary(Group group, String field) {
        int count = group.getFieldRepetitionCount(field);
        if (count > 1) {
            throw new CompactedObjectFormatException("optional compacted field is repeated: " + field);
        }
        return count == 0
                ? Optional.empty()
                : Optional.of(ByteBuffer.wrap(group.getBinary(field, 0).getBytes()));
    }

    private static Optional<String> optionalUtf8(Group group, String field) {
        return optionalBinary(group, field).map(value -> {
            byte[] bytes = new byte[value.remaining()];
            value.get(bytes);
            return new String(bytes, StandardCharsets.UTF_8);
        });
    }

    private static OptionalLong optionalLong(Group group, String field) {
        int count = group.getFieldRepetitionCount(field);
        if (count > 1) {
            throw new CompactedObjectFormatException("optional compacted field is repeated: " + field);
        }
        return count == 0 ? OptionalLong.empty() : OptionalLong.of(group.getLong(field, 0));
    }

    private static OptionalInt optionalInt(Group group, String field) {
        int count = group.getFieldRepetitionCount(field);
        if (count > 1) {
            throw new CompactedObjectFormatException("optional compacted field is repeated: " + field);
        }
        return count == 0 ? OptionalInt.empty() : OptionalInt.of(group.getInteger(field, 0));
    }

    private static boolean isLowerHex(String value) {
        if (value.length() != 64) {
            return false;
        }
        for (int index = 0; index < value.length(); index++) {
            char character = value.charAt(index);
            if (!((character >= '0' && character <= '9')
                    || (character >= 'a' && character <= 'f'))) {
                return false;
            }
        }
        return true;
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
        if (current instanceof IOException) {
            return new CompactedObjectFormatException("cannot decode compacted Parquet bytes", current);
        }
        if (current instanceof ArithmeticException
                || current instanceof IllegalArgumentException
                || current instanceof IllegalStateException) {
            return new CompactedObjectFormatException("invalid compacted Parquet structure", current);
        }
        return new NereusException(
                ErrorCode.OBJECT_READ_FAILED, true, "compacted Parquet read failed", current);
    }

    private record OffsetStats(long minimum, long maximum) {
    }

    private record ReadRows(List<CompactedObjectRow> rows, long sourceCoverageEndOffset) {
    }
}
