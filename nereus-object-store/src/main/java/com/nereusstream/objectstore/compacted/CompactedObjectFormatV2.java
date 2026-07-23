/* Licensed under the Apache License, Version 2.0 */
package com.nereusstream.objectstore.compacted;

import com.nereusstream.api.Checksum;
import com.nereusstream.api.ChecksumType;
import com.nereusstream.api.ObjectId;
import com.nereusstream.api.ObjectKey;
import com.nereusstream.api.ObjectKeyHash;
import com.nereusstream.api.OffsetRange;
import com.nereusstream.api.PayloadFormat;
import com.nereusstream.api.ReadView;
import com.nereusstream.api.StreamId;
import com.nereusstream.api.keys.DeterministicIds;
import com.nereusstream.api.keys.KeyComponentCodec;
import com.nereusstream.objectstore.ObjectKeyPrefix;
import java.util.LinkedHashMap;
import java.util.Map;
import java.util.Objects;
import java.util.Optional;
import java.util.Set;
import org.apache.parquet.schema.MessageType;
import org.apache.parquet.schema.MessageTypeParser;

/** Closed NCP2/NTC2 schemas, metadata registries, limits, and object identities. */
public final class CompactedObjectFormatV2 {
    public static final String COMMITTED_PHYSICAL_FORMAT = "NEREUS_COMPACTED_PARQUET_V2";
    public static final String TOPIC_COMPACTED_PHYSICAL_FORMAT =
            "NEREUS_TOPIC_COMPACTED_KAFKA_PARQUET_V2";
    public static final String COMMITTED_FORMAT_ID = "NCP2";
    public static final String TOPIC_COMPACTED_FORMAT_ID = "NTC2";
    public static final String KAFKA_LOGICAL_FORMAT = "KAFKA_RECORD_BATCH_V1";
    public static final String RANGE_MODEL = "ENTRY_START_PLUS_RECORD_COUNT_V1";
    public static final String KAFKA_KEY_CODEC = "KAFKA_KEY_BYTES_V1";
    public static final String KAFKA_REWRITE_CODEC = "KAFKA_RECORD_REWRITE_V1";
    public static final String PARQUET_LIBRARY_VERSION = CompactedObjectFormatV1.PARQUET_LIBRARY_VERSION;
    public static final String PARQUET_WRITER_VERSION = CompactedObjectFormatV1.PARQUET_WRITER_VERSION;
    public static final int DATA_PAGE_BYTES = CompactedObjectFormatV1.DATA_PAGE_BYTES;
    public static final int ZSTD_LEVEL = CompactedObjectFormatV1.ZSTD_LEVEL;
    public static final int MAX_FOOTER_BYTES = CompactedObjectFormatV1.MAX_FOOTER_BYTES;
    public static final int MAX_ROW_GROUPS = CompactedObjectFormatV1.MAX_ROW_GROUPS;
    public static final int MAX_ROW_GROUP_BUFFER_BYTES = CompactedObjectFormatV1.MAX_ROW_GROUP_BUFFER_BYTES;
    public static final long MAX_OBJECT_BYTES = CompactedObjectFormatV1.MAX_OBJECT_BYTES;
    public static final int MAX_PAYLOAD_BYTES = CompactedObjectFormatV1.MAX_PAYLOAD_BYTES;

    public static final MessageType COMMITTED_SCHEMA = MessageTypeParser.parseMessageType("""
            message nereus_committed_generation_v2 {
              required int64  stream_offset_start;
              required int32  record_count;
              required int32  entry_ordinal;
              required binary payload;
              required int32  payload_crc32c;
              optional int64  event_time_millis;
            }
            """);

    public static final MessageType TOPIC_COMPACTED_SCHEMA = MessageTypeParser.parseMessageType("""
            message nereus_topic_compacted_kafka_v2 {
              required int64  stream_offset_start;
              required int32  record_count;
              required int32  disposition;
              required binary compaction_key;
              required binary payload;
              required int32  payload_crc32c;
              required int64  source_batch_base_offset;
              required int32  source_record_index;
              required binary source_batch_sha256;
              optional int64  event_time_millis;
            }
            """);

    private static final Set<String> COMMON_METADATA_KEYS = Set.of(
            "nereus.format",
            "nereus.format.version",
            "nereus.read.view",
            "nereus.stream.id",
            "nereus.offset.start",
            "nereus.offset.end",
            "nereus.source.set.sha256",
            "nereus.policy.sha256",
            "nereus.output.attempt.id",
            "nereus.payload.format",
            "nereus.logical.format",
            "nereus.range.model",
            "nereus.source.record.count",
            "nereus.output.record.count",
            "nereus.entry.count",
            "nereus.logical.bytes",
            "nereus.cumulative.size.at.end",
            "nereus.writer",
            "nereus.parquet.library.version",
            "nereus.parquet.writer.version",
            "nereus.parquet.compression",
            "nereus.parquet.zstd.level",
            "nereus.parquet.data.page.bytes",
            "nereus.parquet.dictionary.enabled",
            "nereus.parquet.bloom.filter.enabled",
            "nereus.parquet.page.checksum.enabled",
            "nereus.parquet.row.group.records");
    private static final Set<String> TOPIC_METADATA_KEYS = Set.of(
            "nereus.source.coverage.start",
            "nereus.source.coverage.end",
            "nereus.compaction.strategy",
            "nereus.compaction.strategy.version",
            "nereus.compaction.key.codec",
            "nereus.compaction.key.encoding",
            "nereus.kafka.batch.mapping",
            "nereus.kafka.rewrite.codec",
            "nereus.kafka.message.format.digest",
            "nereus.source.batch.count",
            "nereus.output.batch.count");

    private CompactedObjectFormatV2() {
    }

    public static MessageType schema(ReadView view) {
        Objects.requireNonNull(view, "view");
        return view == ReadView.COMMITTED ? COMMITTED_SCHEMA : TOPIC_COMPACTED_SCHEMA;
    }

    public static String physicalFormat(ReadView view) {
        Objects.requireNonNull(view, "view");
        return view == ReadView.COMMITTED ? COMMITTED_PHYSICAL_FORMAT : TOPIC_COMPACTED_PHYSICAL_FORMAT;
    }

    public static ObjectKeyPrefix prefix(String cluster, ReadView view) {
        String viewComponent = view == ReadView.COMMITTED ? "committed" : "topic-compacted-kafka";
        return new ObjectKeyPrefix(KeyComponentCodec.encodeComponent(requireText(cluster, "cluster"))
                + "/compacted/v2/" + viewComponent + "/");
    }

    public static Map<String, String> metadata(RangedCompactedObjectWriteRequest request) {
        Objects.requireNonNull(request, "request");
        return commonMetadata(
                ReadView.COMMITTED,
                request.streamId(),
                request.sourceCoverage(),
                request.sourceSetSha256(),
                request.policySha256(),
                request.outputAttemptId(),
                request.payloadFormat(),
                request.logicalFormat(),
                request.sourceRecordCount(),
                request.sourceRecordCount(),
                request.entryCount(),
                request.logicalBytes(),
                request.cumulativeSizeAtEnd(),
                request.writerBuild(),
                request.compression(),
                request.targetRowGroupRecords());
    }

    public static Map<String, String> metadata(KafkaTopicCompactedObjectWriteRequest request) {
        Objects.requireNonNull(request, "request");
        Map<String, String> metadata = new LinkedHashMap<>(commonMetadata(
                ReadView.TOPIC_COMPACTED,
                request.streamId(),
                request.sourceCoverage(),
                request.sourceSetSha256(),
                request.policySha256(),
                request.outputAttemptId(),
                PayloadFormat.KAFKA_RECORD_BATCH,
                KAFKA_LOGICAL_FORMAT,
                request.sourceCoverage().recordCount(),
                request.outputRecordCount(),
                request.entryCount(),
                request.logicalBytes(),
                request.cumulativeSizeAtEnd(),
                request.writerBuild(),
                request.compression(),
                request.targetRowGroupRecords()));
        KafkaTopicCompactedFormatSpecV2 spec = request.topicCompaction();
        metadata.put("nereus.source.coverage.start", Long.toString(request.sourceCoverage().startOffset()));
        metadata.put("nereus.source.coverage.end", Long.toString(request.sourceCoverage().endOffset()));
        metadata.put("nereus.compaction.strategy", spec.strategyId());
        metadata.put("nereus.compaction.strategy.version", Long.toString(spec.strategyVersion()));
        metadata.put("nereus.compaction.key.codec", spec.keyCodecId());
        metadata.put("nereus.compaction.key.encoding", KafkaCompactionKeyEncodingV2.ID);
        metadata.put("nereus.kafka.batch.mapping", KAFKA_LOGICAL_FORMAT);
        metadata.put("nereus.kafka.rewrite.codec", spec.rewriteCodecId());
        metadata.put("nereus.kafka.message.format.digest", spec.messageFormatSha256().value());
        metadata.put("nereus.source.batch.count", Long.toString(spec.sourceBatchCount()));
        metadata.put("nereus.output.batch.count", Long.toString(spec.outputBatchCount()));
        return java.util.Collections.unmodifiableMap(new LinkedHashMap<>(metadata));
    }

    public static RangedCompactedObjectMetadata parseMetadata(Map<String, String> actual) {
        Objects.requireNonNull(actual, "actual");
        String formatId = require(actual, "nereus.format");
        ReadView view = switch (formatId) {
            case COMMITTED_FORMAT_ID -> ReadView.COMMITTED;
            case TOPIC_COMPACTED_FORMAT_ID -> ReadView.TOPIC_COMPACTED;
            default -> throw new CompactedObjectFormatException("unknown Nereus V2 compacted format id");
        };
        Set<String> allowed = view == ReadView.COMMITTED
                ? COMMON_METADATA_KEYS
                : union(COMMON_METADATA_KEYS, TOPIC_METADATA_KEYS);
        for (String key : allowed) {
            require(actual, key);
        }
        for (String key : actual.keySet()) {
            if (key.startsWith("nereus.") && !allowed.contains(key)) {
                throw new CompactedObjectFormatException("unknown Nereus V2 Parquet metadata: " + key);
            }
        }
        requireExact(actual, "nereus.format.version", "2");
        requireExact(actual, "nereus.read.view", view.name());
        requireExact(actual, "nereus.range.model", RANGE_MODEL);
        requireExact(actual, "nereus.parquet.library.version", PARQUET_LIBRARY_VERSION);
        requireExact(actual, "nereus.parquet.writer.version", PARQUET_WRITER_VERSION);
        requireExact(actual, "nereus.parquet.data.page.bytes", Integer.toString(DATA_PAGE_BYTES));
        requireExact(actual, "nereus.parquet.dictionary.enabled", "false");
        requireExact(actual, "nereus.parquet.bloom.filter.enabled", "false");
        requireExact(actual, "nereus.parquet.page.checksum.enabled", "true");

        long start = parseNonNegativeLong(actual, "nereus.offset.start");
        long end = parseNonNegativeLong(actual, "nereus.offset.end");
        OffsetRange coverage = new OffsetRange(start, end);
        if (coverage.isEmpty()) {
            throw new CompactedObjectFormatException("V2 compacted source coverage cannot be empty");
        }
        PayloadFormat payloadFormat;
        try {
            payloadFormat = PayloadFormat.valueOf(require(actual, "nereus.payload.format"));
        } catch (IllegalArgumentException failure) {
            throw new CompactedObjectFormatException("unknown V2 compacted payload format", failure);
        }
        long sourceRecords = parsePositiveLong(actual, "nereus.source.record.count");
        long outputRecords = parseNonNegativeLong(actual, "nereus.output.record.count");
        int entryCount = parseNonNegativeInt(actual, "nereus.entry.count");
        long logicalBytes = parseNonNegativeLong(actual, "nereus.logical.bytes");
        long cumulativeEnd = parseNonNegativeLong(actual, "nereus.cumulative.size.at.end");
        String compression = parseCompression(actual);
        int rowGroupRecords = parsePositiveInt(actual, "nereus.parquet.row.group.records");
        if (rowGroupRecords > MAX_ROW_GROUPS
                || sourceRecords != coverage.recordCount()
                || outputRecords > sourceRecords
                || (entryCount == 0) != (outputRecords == 0)) {
            throw new CompactedObjectFormatException("V2 compacted file accounting is inconsistent");
        }

        Optional<KafkaTopicCompactedFormatSpecV2> topicSpec;
        if (view == ReadView.COMMITTED) {
            if (outputRecords != sourceRecords || entryCount <= 0) {
                throw new CompactedObjectFormatException("NCP2 metadata must be dense and non-empty");
            }
            topicSpec = Optional.empty();
        } else {
            requireExact(actual, "nereus.source.coverage.start", Long.toString(start));
            requireExact(actual, "nereus.source.coverage.end", Long.toString(end));
            requireExact(actual, "nereus.compaction.key.encoding", KafkaCompactionKeyEncodingV2.ID);
            requireExact(actual, "nereus.kafka.batch.mapping", KAFKA_LOGICAL_FORMAT);
            requireExact(actual, "nereus.payload.format", PayloadFormat.KAFKA_RECORD_BATCH.name());
            requireExact(actual, "nereus.logical.format", KAFKA_LOGICAL_FORMAT);
            topicSpec = Optional.of(new KafkaTopicCompactedFormatSpecV2(
                    requireText(actual, "nereus.compaction.strategy"),
                    parsePositiveLong(actual, "nereus.compaction.strategy.version"),
                    requireText(actual, "nereus.compaction.key.codec"),
                    requireText(actual, "nereus.kafka.rewrite.codec"),
                    parseSha256(actual, "nereus.kafka.message.format.digest"),
                    parsePositiveLong(actual, "nereus.source.batch.count"),
                    parseNonNegativeLong(actual, "nereus.output.batch.count")));
            if (topicSpec.orElseThrow().outputBatchCount() != entryCount) {
                throw new CompactedObjectFormatException("NTC2 output batch count does not equal row count");
            }
        }
        return new RangedCompactedObjectMetadata(
                view,
                new StreamId(require(actual, "nereus.stream.id")),
                coverage,
                parseSha256(actual, "nereus.source.set.sha256"),
                parseSha256(actual, "nereus.policy.sha256"),
                requireBase32(require(actual, "nereus.output.attempt.id"), "outputAttemptId"),
                payloadFormat,
                requireText(actual, "nereus.logical.format"),
                RANGE_MODEL,
                sourceRecords,
                outputRecords,
                entryCount,
                logicalBytes,
                cumulativeEnd,
                requireText(actual, "nereus.writer"),
                compression,
                rowGroupRecords,
                topicSpec);
    }

    public static void validateMetadata(
            Map<String, String> actual,
            RangedCompactedObjectWriteRequest expected) {
        requireExactMap(actual, metadata(expected));
        parseMetadata(actual);
    }

    public static void validateMetadata(
            Map<String, String> actual,
            KafkaTopicCompactedObjectWriteRequest expected) {
        requireExactMap(actual, metadata(expected));
        parseMetadata(actual);
    }

    public static ObjectKey objectKey(
            String cluster,
            ReadView view,
            StreamId streamId,
            OffsetRange coverage,
            Checksum contentSha256,
            String outputAttemptId) {
        requireSha256(contentSha256, "contentSha256");
        String viewComponent = view == ReadView.COMMITTED ? "committed" : "topic-compacted-kafka";
        return new ObjectKey(KeyComponentCodec.encodeComponent(requireText(cluster, "cluster"))
                + "/compacted/v2/"
                + viewComponent
                + "/"
                + KeyComponentCodec.encodeComponent(Objects.requireNonNull(streamId, "streamId").value())
                + "/"
                + KeyComponentCodec.encodeNonNegativeLong(coverage.startOffset())
                + "-"
                + KeyComponentCodec.encodeNonNegativeLong(coverage.endOffset())
                + "/"
                + contentSha256.value()
                + "-"
                + requireBase32(outputAttemptId, "outputAttemptId")
                + ".parquet");
    }

    public static ObjectId objectId(ObjectKey objectKey) {
        Objects.requireNonNull(objectKey, "objectKey");
        return new ObjectId("co2-" + DeterministicIds.stableHashComponent(objectKey.value()));
    }

    public static ObjectKeyHash objectKeyHash(ObjectKey objectKey) {
        return ObjectKeyHash.from(Objects.requireNonNull(objectKey, "objectKey"));
    }

    static String requireText(String value, String field) {
        Objects.requireNonNull(value, field);
        if (value.isBlank()) {
            throw new IllegalArgumentException(field + " cannot be blank");
        }
        return value;
    }

    static String requireBase32(String value, String field) {
        value = requireText(value, field);
        if (value.length() < 26 || value.length() > 128 || !value.matches("[a-z2-7]+")) {
            throw new IllegalArgumentException(field + " must be lowercase base32 with at least 128 bits");
        }
        return value;
    }

    static String requireCompression(String value) {
        value = requireText(value, "compression");
        if (!value.equals("ZSTD") && !value.equals("UNCOMPRESSED")) {
            throw new IllegalArgumentException("compression must be ZSTD or UNCOMPRESSED");
        }
        return value;
    }

    private static Map<String, String> commonMetadata(
            ReadView view,
            StreamId streamId,
            OffsetRange coverage,
            Checksum sourceSetSha256,
            Checksum policySha256,
            String attemptId,
            PayloadFormat payloadFormat,
            String logicalFormat,
            long sourceRecords,
            long outputRecords,
            int entryCount,
            long logicalBytes,
            long cumulativeSizeAtEnd,
            String writerBuild,
            String compression,
            int rowGroupRecords) {
        Map<String, String> metadata = new LinkedHashMap<>();
        metadata.put("nereus.format", view == ReadView.COMMITTED ? COMMITTED_FORMAT_ID : TOPIC_COMPACTED_FORMAT_ID);
        metadata.put("nereus.format.version", "2");
        metadata.put("nereus.read.view", view.name());
        metadata.put("nereus.stream.id", streamId.value());
        metadata.put("nereus.offset.start", Long.toString(coverage.startOffset()));
        metadata.put("nereus.offset.end", Long.toString(coverage.endOffset()));
        metadata.put("nereus.source.set.sha256", sourceSetSha256.value());
        metadata.put("nereus.policy.sha256", policySha256.value());
        metadata.put("nereus.output.attempt.id", attemptId);
        metadata.put("nereus.payload.format", payloadFormat.name());
        metadata.put("nereus.logical.format", logicalFormat);
        metadata.put("nereus.range.model", RANGE_MODEL);
        metadata.put("nereus.source.record.count", Long.toString(sourceRecords));
        metadata.put("nereus.output.record.count", Long.toString(outputRecords));
        metadata.put("nereus.entry.count", Integer.toString(entryCount));
        metadata.put("nereus.logical.bytes", Long.toString(logicalBytes));
        metadata.put("nereus.cumulative.size.at.end", Long.toString(cumulativeSizeAtEnd));
        metadata.put("nereus.writer", writerBuild);
        metadata.put("nereus.parquet.library.version", PARQUET_LIBRARY_VERSION);
        metadata.put("nereus.parquet.writer.version", PARQUET_WRITER_VERSION);
        metadata.put("nereus.parquet.compression", compression);
        metadata.put("nereus.parquet.zstd.level", compression.equals("ZSTD") ? Integer.toString(ZSTD_LEVEL) : "");
        metadata.put("nereus.parquet.data.page.bytes", Integer.toString(DATA_PAGE_BYTES));
        metadata.put("nereus.parquet.dictionary.enabled", "false");
        metadata.put("nereus.parquet.bloom.filter.enabled", "false");
        metadata.put("nereus.parquet.page.checksum.enabled", "true");
        metadata.put("nereus.parquet.row.group.records", Integer.toString(rowGroupRecords));
        return java.util.Collections.unmodifiableMap(new LinkedHashMap<>(metadata));
    }

    private static void requireExactMap(Map<String, String> actual, Map<String, String> expected) {
        Objects.requireNonNull(actual, "actual");
        if (!actual.equals(expected)) {
            throw new CompactedObjectFormatException("V2 Parquet metadata does not exactly match the write request");
        }
    }

    private static Set<String> union(Set<String> left, Set<String> right) {
        java.util.HashSet<String> values = new java.util.HashSet<>(left);
        values.addAll(right);
        return Set.copyOf(values);
    }

    private static String require(Map<String, String> values, String key) {
        String value = values.get(key);
        if (value == null) {
            throw new CompactedObjectFormatException("missing required V2 Parquet metadata: " + key);
        }
        return value;
    }

    private static String requireText(Map<String, String> values, String key) {
        String value = require(values, key);
        if (value.isBlank()) {
            throw new CompactedObjectFormatException("blank required V2 Parquet metadata: " + key);
        }
        return value;
    }

    private static void requireExact(Map<String, String> values, String key, String expected) {
        if (!require(values, key).equals(expected)) {
            throw new CompactedObjectFormatException("V2 Parquet metadata mismatch for " + key);
        }
    }

    private static String parseCompression(Map<String, String> actual) {
        String compression = require(actual, "nereus.parquet.compression");
        String level = require(actual, "nereus.parquet.zstd.level");
        if (compression.equals("ZSTD") && level.equals(Integer.toString(ZSTD_LEVEL))) {
            return compression;
        }
        if (compression.equals("UNCOMPRESSED") && level.isEmpty()) {
            return compression;
        }
        throw new CompactedObjectFormatException("unknown V2 Parquet compression profile");
    }

    private static int parsePositiveInt(Map<String, String> values, String key) {
        int value = parseInt(values, key);
        if (value <= 0) {
            throw new CompactedObjectFormatException("metadata must be positive: " + key);
        }
        return value;
    }

    private static int parseNonNegativeInt(Map<String, String> values, String key) {
        int value = parseInt(values, key);
        if (value < 0) {
            throw new CompactedObjectFormatException("metadata must be non-negative: " + key);
        }
        return value;
    }

    private static int parseInt(Map<String, String> values, String key) {
        String encoded = require(values, key);
        try {
            int value = Integer.parseInt(encoded);
            if (!encoded.equals(Integer.toString(value))) {
                throw new NumberFormatException("non-canonical decimal");
            }
            return value;
        } catch (NumberFormatException failure) {
            throw new CompactedObjectFormatException("invalid canonical integer metadata: " + key, failure);
        }
    }

    private static long parsePositiveLong(Map<String, String> values, String key) {
        long value = parseLong(values, key);
        if (value <= 0) {
            throw new CompactedObjectFormatException("metadata must be positive: " + key);
        }
        return value;
    }

    private static long parseNonNegativeLong(Map<String, String> values, String key) {
        long value = parseLong(values, key);
        if (value < 0) {
            throw new CompactedObjectFormatException("metadata must be non-negative: " + key);
        }
        return value;
    }

    private static long parseLong(Map<String, String> values, String key) {
        String encoded = require(values, key);
        try {
            long value = Long.parseLong(encoded);
            if (!encoded.equals(Long.toString(value))) {
                throw new NumberFormatException("non-canonical decimal");
            }
            return value;
        } catch (NumberFormatException failure) {
            throw new CompactedObjectFormatException("invalid canonical long metadata: " + key, failure);
        }
    }

    private static Checksum parseSha256(Map<String, String> values, String key) {
        try {
            return new Checksum(ChecksumType.SHA256, require(values, key));
        } catch (IllegalArgumentException failure) {
            throw new CompactedObjectFormatException("invalid SHA256 metadata: " + key, failure);
        }
    }

    private static void requireSha256(Checksum checksum, String field) {
        Objects.requireNonNull(checksum, field);
        if (checksum.type() != ChecksumType.SHA256) {
            throw new IllegalArgumentException(field + " must use SHA256");
        }
    }
}
