/* Licensed under the Apache License, Version 2.0 */
package com.nereusstream.metadata.oxia.codec;

import java.nio.ByteBuffer;
import java.nio.charset.StandardCharsets;
import java.util.ArrayList;
import java.util.Comparator;
import java.util.LinkedHashMap;
import java.util.List;
import java.util.Map;
import java.util.Objects;

/** Explicit authority-capable StreamHead codec with byte-exact V1 compatibility. */
public final class StreamHeadRecordCodecV2
        implements MetadataRecordCodec<com.nereusstream.metadata.oxia.records.StreamHeadRecord> {
    private static final int LEGACY_VERSION = 1;
    private static final int VERSION = 2;
    private static final int MAX_ATTRIBUTES = 4_096;
    private final MetadataRecordCodec<StreamHeadRecord> legacy =
            Phase1MetadataCodecs.recordCodec(StreamHeadRecord.class);

    @Override
    public String recordType() {
        return com.nereusstream.metadata.oxia.records.StreamHeadRecord.class.getSimpleName();
    }

    @Override
    public int schemaVersion() {
        return VERSION;
    }

    @Override
    public int minReaderSchemaVersion() {
        return VERSION;
    }

    @Override
    public int schemaVersion(com.nereusstream.metadata.oxia.records.StreamHeadRecord value) {
        return Objects.requireNonNull(value, "value").appendSession().hasAuthority()
                ? VERSION
                : LEGACY_VERSION;
    }

    @Override
    public int minReaderSchemaVersion(com.nereusstream.metadata.oxia.records.StreamHeadRecord value) {
        return schemaVersion(value);
    }

    @Override
    public boolean supportsEnvelopeSchema(int writerSchemaVersion, int minimumReaderSchemaVersion) {
        return (writerSchemaVersion == LEGACY_VERSION && minimumReaderSchemaVersion == LEGACY_VERSION)
                || (writerSchemaVersion == VERSION && minimumReaderSchemaVersion == VERSION);
    }

    @Override
    public byte[] encode(com.nereusstream.metadata.oxia.records.StreamHeadRecord value) {
        Objects.requireNonNull(value, "value");
        if (!value.appendSession().hasAuthority()) {
            return legacy.encode(toLegacy(value));
        }
        try {
            F4Binary.Writer writer = new F4Binary.Writer();
            writer.writeUnsignedShort(VERSION);
            writer.writeString(value.streamId());
            writer.writeString(value.streamName());
            writer.writeString(value.streamNameHash());
            writer.writeString(value.state());
            writer.writeString(value.profile());
            writeMap(writer, value.attributes());
            writer.writeLong(value.createdAtMillis());
            writer.writeLong(value.policyVersion());
            writer.writeLong(value.committedEndOffset());
            writer.writeLong(value.cumulativeSize());
            writer.writeLong(value.commitVersion());
            writer.writeLong(value.trimOffset());
            writer.writeString(value.lastCommitId());
            AppendSessionSnapshotRecordCodecV2.writeV2(writer, value.appendSession());
            writer.writeLong(value.metadataVersion());
            return writer.toByteArray();
        } catch (RuntimeException failure) {
            throw F4Binary.malformed(recordType(), failure);
        }
    }

    @Override
    public com.nereusstream.metadata.oxia.records.StreamHeadRecord decode(byte[] bytes) {
        Objects.requireNonNull(bytes, "bytes");
        if (bytes.length >= Short.BYTES
                && Short.toUnsignedInt(ByteBuffer.wrap(bytes).getShort()) == VERSION) {
            return decodeV2(bytes);
        }
        return fromLegacy(legacy.decode(bytes));
    }

    private com.nereusstream.metadata.oxia.records.StreamHeadRecord decodeV2(byte[] bytes) {
        try {
            F4Binary.Reader reader = new F4Binary.Reader(bytes);
            int version = reader.readUnsignedShort("schemaVersion");
            if (version != VERSION) {
                throw new MetadataCodecException("unsupported StreamHeadRecord schema version");
            }
            com.nereusstream.metadata.oxia.records.StreamHeadRecord value =
                    new com.nereusstream.metadata.oxia.records.StreamHeadRecord(
                            reader.readString("streamId"),
                            reader.readString("streamName"),
                            reader.readString("streamNameHash"),
                            reader.readString("state"),
                            reader.readString("profile"),
                            readMap(reader),
                            reader.readLong("createdAtMillis"),
                            reader.readLong("policyVersion"),
                            reader.readLong("committedEndOffset"),
                            reader.readLong("cumulativeSize"),
                            reader.readLong("commitVersion"),
                            reader.readLong("trimOffset"),
                            reader.readString("lastCommitId"),
                            AppendSessionSnapshotRecordCodecV2.readV2(reader),
                            reader.readLong("metadataVersion"));
            if (!value.appendSession().hasAuthority()) {
                throw new MetadataCodecException("StreamHeadRecord V2 requires append authority");
            }
            reader.requireConsumed();
            return value;
        } catch (RuntimeException failure) {
            throw F4Binary.malformed(recordType(), failure);
        }
    }

    private static void writeMap(F4Binary.Writer writer, Map<String, String> values) {
        List<Map.Entry<String, String>> entries = new ArrayList<>(values.entrySet());
        entries.sort(Map.Entry.comparingByKey(UTF8_COMPARATOR));
        writer.writeInt(entries.size());
        for (Map.Entry<String, String> entry : entries) {
            writer.writeString(entry.getKey());
            writer.writeString(entry.getValue());
        }
    }

    private static Map<String, String> readMap(F4Binary.Reader reader) {
        int count = reader.readCount("attributeCount", Integer.BYTES * 2, MAX_ATTRIBUTES);
        Map<String, String> values = new LinkedHashMap<>();
        String previous = null;
        for (int index = 0; index < count; index++) {
            String key = reader.readString("attributeKey");
            if (previous != null && UTF8_COMPARATOR.compare(previous, key) >= 0) {
                throw new MetadataCodecException("StreamHeadRecord attributes are not canonical");
            }
            if (values.put(key, reader.readString("attributeValue")) != null) {
                throw new MetadataCodecException("duplicate StreamHeadRecord attribute");
            }
            previous = key;
        }
        return values;
    }

    private static StreamHeadRecord toLegacy(
            com.nereusstream.metadata.oxia.records.StreamHeadRecord value) {
        var session = value.appendSession();
        return new StreamHeadRecord(
                value.streamId(), value.streamName(), value.streamNameHash(), value.state(), value.profile(),
                value.attributes(), value.createdAtMillis(), value.policyVersion(), value.committedEndOffset(),
                value.cumulativeSize(), value.commitVersion(), value.trimOffset(), value.lastCommitId(),
                new AppendSessionSnapshotRecord(
                        session.writerId(), session.epoch(), session.fencingToken(),
                        session.leaseVersion(), session.expiresAtMillis()),
                value.metadataVersion());
    }

    private static com.nereusstream.metadata.oxia.records.StreamHeadRecord fromLegacy(StreamHeadRecord value) {
        AppendSessionSnapshotRecord session = value.appendSession();
        return new com.nereusstream.metadata.oxia.records.StreamHeadRecord(
                value.streamId(), value.streamName(), value.streamNameHash(), value.state(), value.profile(),
                value.attributes(), value.createdAtMillis(), value.policyVersion(), value.committedEndOffset(),
                value.cumulativeSize(), value.commitVersion(), value.trimOffset(), value.lastCommitId(),
                new com.nereusstream.metadata.oxia.records.AppendSessionSnapshotRecord(
                        session.writerId(), session.epoch(), session.fencingToken(),
                        session.leaseVersion(), session.expiresAtMillis()),
                value.metadataVersion());
    }

    private static final Comparator<String> UTF8_COMPARATOR = (left, right) -> {
        byte[] leftBytes = left.getBytes(StandardCharsets.UTF_8);
        byte[] rightBytes = right.getBytes(StandardCharsets.UTF_8);
        int limit = Math.min(leftBytes.length, rightBytes.length);
        for (int index = 0; index < limit; index++) {
            int compared = Integer.compare(leftBytes[index] & 0xff, rightBytes[index] & 0xff);
            if (compared != 0) {
                return compared;
            }
        }
        return Integer.compare(leftBytes.length, rightBytes.length);
    };

    record AppendSessionSnapshotRecord(
            String writerId,
            long epoch,
            String fencingToken,
            long leaseVersion,
            long expiresAtMillis) {
    }

    record StreamHeadRecord(
            String streamId,
            String streamName,
            String streamNameHash,
            String state,
            String profile,
            Map<String, String> attributes,
            long createdAtMillis,
            long policyVersion,
            long committedEndOffset,
            long cumulativeSize,
            long commitVersion,
            long trimOffset,
            String lastCommitId,
            AppendSessionSnapshotRecord appendSession,
            long metadataVersion) {
    }
}
