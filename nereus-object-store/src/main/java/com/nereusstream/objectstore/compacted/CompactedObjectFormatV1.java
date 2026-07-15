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
import java.util.LinkedHashMap;
import java.util.Map;
import java.util.Objects;
import java.util.Optional;
import java.util.Set;
import org.apache.parquet.schema.MessageType;
import org.apache.parquet.schema.MessageTypeParser;

/** Closed NCP1/NTC1 Parquet schemas, metadata registry, limits, and object identity rules. */
public final class CompactedObjectFormatV1 {
    public static final String COMMITTED_PHYSICAL_FORMAT = "NEREUS_COMPACTED_PARQUET_V1";
    public static final String TOPIC_COMPACTED_PHYSICAL_FORMAT =
            "NEREUS_TOPIC_COMPACTED_PARQUET_V1";
    public static final String COMMITTED_FORMAT_ID = "NCP1";
    public static final String TOPIC_COMPACTED_FORMAT_ID = "NTC1";
    public static final String PARQUET_LIBRARY_VERSION = "1.15.2";
    public static final String PARQUET_WRITER_VERSION = "PARQUET_2_0";
    public static final int DATA_PAGE_BYTES = 1 << 20;
    public static final int ZSTD_LEVEL = 3;
    public static final int MAX_FOOTER_BYTES = 16 << 20;
    public static final int MAX_ROW_GROUPS = 65_536;
    public static final int MAX_ROW_GROUP_BUFFER_BYTES = 64 << 20;
    public static final int MAX_PAYLOAD_BYTES = 64 << 20;
    public static final int MAX_OPTIONAL_BINARY_BYTES = 1 << 20;
    public static final int MAX_SCHEMA_IDENTITY_BYTES = 4 << 10;

    public static final MessageType COMMITTED_SCHEMA = MessageTypeParser.parseMessageType("""
            message nereus_committed_generation_v1 {
              required int64  stream_offset;
              required binary payload;
              required int32  payload_crc32c;
              optional int64  publish_time_millis;
              optional int64  event_time_millis;
              optional binary message_key;
              optional binary ordering_key;
              optional binary schema_identity;
              optional int64  producer_id;
              optional int64  producer_sequence_id;
              optional int32  batch_message_count;
            }
            """);

    public static final MessageType TOPIC_COMPACTED_SCHEMA = MessageTypeParser.parseMessageType("""
            message nereus_topic_compacted_v1 {
              required int64  stream_offset;
              required int32  disposition;
              required binary compaction_key;
              optional binary payload;
              optional int32  payload_crc32c;
              optional int64  publish_time_millis;
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
            "nereus.projection.identity.sha256",
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
            "nereus.compaction.key.codec");

    private CompactedObjectFormatV1() {
    }

    public static MessageType schema(ReadView view) {
        Objects.requireNonNull(view, "view");
        return view == ReadView.COMMITTED ? COMMITTED_SCHEMA : TOPIC_COMPACTED_SCHEMA;
    }

    public static String physicalFormat(ReadView view) {
        Objects.requireNonNull(view, "view");
        return view == ReadView.COMMITTED
                ? COMMITTED_PHYSICAL_FORMAT
                : TOPIC_COMPACTED_PHYSICAL_FORMAT;
    }

    public static Map<String, String> metadata(CompactedObjectWriteRequest request) {
        Objects.requireNonNull(request, "request");
        Map<String, String> metadata = new LinkedHashMap<>();
        metadata.put("nereus.format", request.view() == ReadView.COMMITTED
                ? COMMITTED_FORMAT_ID
                : TOPIC_COMPACTED_FORMAT_ID);
        metadata.put("nereus.format.version", "1");
        metadata.put("nereus.read.view", request.view().name());
        metadata.put("nereus.stream.id", request.streamId().value());
        metadata.put("nereus.offset.start", Long.toString(request.sourceCoverage().startOffset()));
        metadata.put("nereus.offset.end", Long.toString(request.sourceCoverage().endOffset()));
        metadata.put("nereus.source.set.sha256", request.sourceSetSha256().value());
        metadata.put("nereus.policy.sha256", request.policySha256().value());
        metadata.put("nereus.output.attempt.id", request.outputAttemptId());
        metadata.put("nereus.payload.format", request.payloadFormat().name());
        metadata.put("nereus.logical.format", request.logicalFormat());
        metadata.put("nereus.projection.identity.sha256", request.projectionIdentitySha256()
                .map(Checksum::value)
                .orElse(""));
        metadata.put("nereus.source.record.count", Integer.toString(request.sourceRecordCount()));
        metadata.put("nereus.output.record.count", Integer.toString(request.expectedOutputRecordCount()));
        metadata.put("nereus.entry.count", Integer.toString(request.entryCount()));
        metadata.put("nereus.logical.bytes", Long.toString(request.logicalBytes()));
        metadata.put("nereus.cumulative.size.at.end", Long.toString(request.cumulativeSizeAtEnd()));
        metadata.put("nereus.writer", request.writerBuild());
        metadata.put("nereus.parquet.library.version", PARQUET_LIBRARY_VERSION);
        metadata.put("nereus.parquet.writer.version", PARQUET_WRITER_VERSION);
        metadata.put("nereus.parquet.compression", request.compression());
        metadata.put("nereus.parquet.zstd.level", request.compression().equals("ZSTD")
                ? Integer.toString(ZSTD_LEVEL)
                : "");
        metadata.put("nereus.parquet.data.page.bytes", Integer.toString(DATA_PAGE_BYTES));
        metadata.put("nereus.parquet.dictionary.enabled", "false");
        metadata.put("nereus.parquet.bloom.filter.enabled", "false");
        metadata.put("nereus.parquet.page.checksum.enabled", "true");
        metadata.put("nereus.parquet.row.group.records", Integer.toString(request.targetRowGroupRecords()));
        request.topicCompaction().ifPresent(spec -> {
            metadata.put("nereus.source.coverage.start", Long.toString(request.sourceCoverage().startOffset()));
            metadata.put("nereus.source.coverage.end", Long.toString(request.sourceCoverage().endOffset()));
            metadata.put("nereus.compaction.strategy", spec.strategyId());
            metadata.put("nereus.compaction.strategy.version", Long.toString(spec.strategyVersion()));
            metadata.put("nereus.compaction.key.codec", spec.keyCodecId());
        });
        return Map.copyOf(metadata);
    }

    public static void validateMetadata(
            Map<String, String> actual,
            CompactedObjectWriteRequest expected) {
        Objects.requireNonNull(actual, "actual");
        Objects.requireNonNull(expected, "expected");
        Map<String, String> expectedValues = metadata(expected);
        Set<String> allowed = expected.view() == ReadView.COMMITTED
                ? COMMON_METADATA_KEYS
                : unionMetadataKeys();
        for (String key : allowed) {
            if (!actual.containsKey(key)) {
                throw new CompactedObjectFormatException("missing required Parquet metadata: " + key);
            }
        }
        for (Map.Entry<String, String> entry : expectedValues.entrySet()) {
            if (!entry.getValue().equals(actual.get(entry.getKey()))) {
                throw new CompactedObjectFormatException(
                        "Parquet metadata mismatch for " + entry.getKey());
            }
        }
        for (String key : actual.keySet()) {
            if (key.startsWith("nereus.") && !allowed.contains(key)) {
                throw new CompactedObjectFormatException("unknown Nereus Parquet metadata: " + key);
            }
        }
    }

    public static CompactedObjectMetadata parseMetadata(Map<String, String> actual) {
        Objects.requireNonNull(actual, "actual");
        String format = require(actual, "nereus.format");
        ReadView view = switch (format) {
            case COMMITTED_FORMAT_ID -> ReadView.COMMITTED;
            case TOPIC_COMPACTED_FORMAT_ID -> ReadView.TOPIC_COMPACTED;
            default -> throw new CompactedObjectFormatException("unknown Nereus compacted format id");
        };
        Set<String> allowed = view == ReadView.COMMITTED ? COMMON_METADATA_KEYS : unionMetadataKeys();
        for (String key : allowed) {
            require(actual, key);
        }
        for (String key : actual.keySet()) {
            if (key.startsWith("nereus.") && !allowed.contains(key)) {
                throw new CompactedObjectFormatException("unknown Nereus Parquet metadata: " + key);
            }
        }
        requireExact(actual, "nereus.format.version", "1");
        requireExact(actual, "nereus.read.view", view.name());
        requireExact(actual, "nereus.parquet.library.version", PARQUET_LIBRARY_VERSION);
        requireExact(actual, "nereus.parquet.writer.version", PARQUET_WRITER_VERSION);
        requireExact(actual, "nereus.parquet.data.page.bytes", Integer.toString(DATA_PAGE_BYTES));
        requireExact(actual, "nereus.parquet.dictionary.enabled", "false");
        requireExact(actual, "nereus.parquet.bloom.filter.enabled", "false");
        requireExact(actual, "nereus.parquet.page.checksum.enabled", "true");

        StreamId streamId = new StreamId(require(actual, "nereus.stream.id"));
        long start = parseNonNegativeLong(actual, "nereus.offset.start");
        long end = parseNonNegativeLong(actual, "nereus.offset.end");
        OffsetRange coverage = new OffsetRange(start, end);
        if (coverage.isEmpty()) {
            throw new CompactedObjectFormatException("compacted source coverage cannot be empty");
        }
        Checksum sourceSet = parseSha256(actual, "nereus.source.set.sha256");
        Checksum policy = parseSha256(actual, "nereus.policy.sha256");
        String attempt = requireBase32(require(actual, "nereus.output.attempt.id"));
        PayloadFormat payloadFormat;
        try {
            payloadFormat = PayloadFormat.valueOf(require(actual, "nereus.payload.format"));
        } catch (IllegalArgumentException failure) {
            throw new CompactedObjectFormatException("unknown compacted payload format", failure);
        }
        String logicalFormat = requireText(actual, "nereus.logical.format");
        String projection = require(actual, "nereus.projection.identity.sha256");
        Optional<Checksum> projectionIdentity = projection.isEmpty()
                ? Optional.empty()
                : Optional.of(parseSha256Value(projection, "projection identity"));
        int sourceRecords = parsePositiveInt(actual, "nereus.source.record.count");
        int outputRecords = parseNonNegativeInt(actual, "nereus.output.record.count");
        int entryCount = parsePositiveInt(actual, "nereus.entry.count");
        long logicalBytes = parseNonNegativeLong(actual, "nereus.logical.bytes");
        long cumulativeEnd = parseNonNegativeLong(actual, "nereus.cumulative.size.at.end");
        String writer = requireText(actual, "nereus.writer");
        String compression = require(actual, "nereus.parquet.compression");
        String zstdLevel = require(actual, "nereus.parquet.zstd.level");
        if (compression.equals("ZSTD")) {
            if (!zstdLevel.equals(Integer.toString(ZSTD_LEVEL))) {
                throw new CompactedObjectFormatException("NCP1 ZSTD level must be 3");
            }
        } else if (!compression.equals("UNCOMPRESSED") || !zstdLevel.isEmpty()) {
            throw new CompactedObjectFormatException("unknown compacted Parquet compression profile");
        }
        int rowGroupRecords = parsePositiveInt(actual, "nereus.parquet.row.group.records");
        if (rowGroupRecords > 65_536) {
            throw new CompactedObjectFormatException("row-group record limit exceeds V1");
        }
        if (sourceRecords != coverage.recordCount()
                || outputRecords > sourceRecords
                || (view == ReadView.COMMITTED && outputRecords != sourceRecords)) {
            throw new CompactedObjectFormatException("compacted file record accounting is inconsistent");
        }
        Optional<TopicCompactionFormatSpec> topicSpec;
        if (view == ReadView.TOPIC_COMPACTED) {
            requireExact(actual, "nereus.source.coverage.start", Long.toString(start));
            requireExact(actual, "nereus.source.coverage.end", Long.toString(end));
            topicSpec = Optional.of(new TopicCompactionFormatSpec(
                    requireText(actual, "nereus.compaction.strategy"),
                    parsePositiveLong(actual, "nereus.compaction.strategy.version"),
                    requireText(actual, "nereus.compaction.key.codec")));
        } else {
            topicSpec = Optional.empty();
        }
        return new CompactedObjectMetadata(
                view,
                streamId,
                coverage,
                sourceSet,
                policy,
                attempt,
                payloadFormat,
                logicalFormat,
                projectionIdentity,
                sourceRecords,
                outputRecords,
                entryCount,
                logicalBytes,
                cumulativeEnd,
                writer,
                compression,
                rowGroupRecords,
                topicSpec);
    }

    public static ObjectKey objectKey(
            CompactedObjectWriteRequest request,
            Checksum contentSha256) {
        Objects.requireNonNull(request, "request");
        requireSha256(contentSha256, "contentSha256");
        String viewComponent = request.view() == ReadView.COMMITTED
                ? "committed"
                : "topic-compacted";
        return new ObjectKey(KeyComponentCodec.encodeComponent(request.cluster())
                + "/compacted/v1/"
                + viewComponent
                + "/"
                + KeyComponentCodec.encodeComponent(request.streamId().value())
                + "/"
                + KeyComponentCodec.encodeNonNegativeLong(request.sourceCoverage().startOffset())
                + "-"
                + KeyComponentCodec.encodeNonNegativeLong(request.sourceCoverage().endOffset())
                + "/"
                + contentSha256.value()
                + "-"
                + request.outputAttemptId()
                + ".parquet");
    }

    public static ObjectId objectId(ObjectKey objectKey) {
        Objects.requireNonNull(objectKey, "objectKey");
        return new ObjectId("co1-" + DeterministicIds.stableHashComponent(objectKey.value()));
    }

    public static ObjectKeyHash objectKeyHash(ObjectKey objectKey) {
        return ObjectKeyHash.from(Objects.requireNonNull(objectKey, "objectKey"));
    }

    private static Set<String> unionMetadataKeys() {
        java.util.HashSet<String> keys = new java.util.HashSet<>(COMMON_METADATA_KEYS);
        keys.addAll(TOPIC_METADATA_KEYS);
        return Set.copyOf(keys);
    }

    private static String require(Map<String, String> values, String key) {
        String value = values.get(key);
        if (value == null) {
            throw new CompactedObjectFormatException("missing required Parquet metadata: " + key);
        }
        return value;
    }

    private static String requireText(Map<String, String> values, String key) {
        String value = require(values, key);
        if (value.isBlank()) {
            throw new CompactedObjectFormatException("blank required Parquet metadata: " + key);
        }
        return value;
    }

    private static void requireExact(Map<String, String> values, String key, String expected) {
        if (!require(values, key).equals(expected)) {
            throw new CompactedObjectFormatException("Parquet metadata mismatch for " + key);
        }
    }

    private static Checksum parseSha256(Map<String, String> values, String key) {
        return parseSha256Value(require(values, key), key);
    }

    private static Checksum parseSha256Value(String value, String field) {
        try {
            return new Checksum(ChecksumType.SHA256, value);
        } catch (IllegalArgumentException failure) {
            throw new CompactedObjectFormatException("invalid SHA256 metadata for " + field, failure);
        }
    }

    private static String requireBase32(String value) {
        if (value.length() < 26 || value.length() > 128 || !value.matches("[a-z2-7]+")) {
            throw new CompactedObjectFormatException("invalid compacted output-attempt id");
        }
        return value;
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
        try {
            return Integer.parseInt(require(values, key));
        } catch (NumberFormatException failure) {
            throw new CompactedObjectFormatException("invalid integer metadata: " + key, failure);
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
        try {
            return Long.parseLong(require(values, key));
        } catch (NumberFormatException failure) {
            throw new CompactedObjectFormatException("invalid long metadata: " + key, failure);
        }
    }

    private static void requireSha256(Checksum checksum, String field) {
        Objects.requireNonNull(checksum, field);
        if (checksum.type() != ChecksumType.SHA256) {
            throw new IllegalArgumentException(field + " must be SHA256");
        }
    }
}
