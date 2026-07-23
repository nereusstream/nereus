/* Licensed under the Apache License, Version 2.0 */
package com.nereusstream.metadata.oxia.codec;

import java.nio.ByteBuffer;
import java.util.Objects;

/** Dual V1/V2 codec that preserves the frozen five-field legacy session payload. */
public final class AppendSessionSnapshotRecordCodecV2
        implements MetadataRecordCodec<com.nereusstream.metadata.oxia.records.AppendSessionSnapshotRecord> {
    private static final int LEGACY_VERSION = 1;
    private static final int VERSION = 2;
    private final MetadataRecordCodec<AppendSessionSnapshotRecord> legacy =
            Phase1MetadataCodecs.recordCodec(AppendSessionSnapshotRecord.class);

    @Override
    public String recordType() {
        return com.nereusstream.metadata.oxia.records.AppendSessionSnapshotRecord.class.getSimpleName();
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
    public int schemaVersion(com.nereusstream.metadata.oxia.records.AppendSessionSnapshotRecord value) {
        return Objects.requireNonNull(value, "value").hasAuthority() ? VERSION : LEGACY_VERSION;
    }

    @Override
    public int minReaderSchemaVersion(com.nereusstream.metadata.oxia.records.AppendSessionSnapshotRecord value) {
        return schemaVersion(value);
    }

    @Override
    public boolean supportsEnvelopeSchema(int writerSchemaVersion, int minimumReaderSchemaVersion) {
        return (writerSchemaVersion == LEGACY_VERSION && minimumReaderSchemaVersion == LEGACY_VERSION)
                || (writerSchemaVersion == VERSION && minimumReaderSchemaVersion == VERSION);
    }

    @Override
    public byte[] encode(com.nereusstream.metadata.oxia.records.AppendSessionSnapshotRecord value) {
        Objects.requireNonNull(value, "value");
        if (!value.hasAuthority()) {
            return legacy.encode(new AppendSessionSnapshotRecord(
                    value.writerId(), value.epoch(), value.fencingToken(),
                    value.leaseVersion(), value.expiresAtMillis()));
        }
        F4Binary.Writer writer = new F4Binary.Writer();
        writer.writeUnsignedShort(VERSION);
        writeV2(writer, value);
        return writer.toByteArray();
    }

    @Override
    public com.nereusstream.metadata.oxia.records.AppendSessionSnapshotRecord decode(byte[] bytes) {
        Objects.requireNonNull(bytes, "bytes");
        if (bytes.length >= Short.BYTES
                && Short.toUnsignedInt(ByteBuffer.wrap(bytes).getShort()) == VERSION) {
            F4Binary.Reader reader = new F4Binary.Reader(bytes);
            int version = reader.readUnsignedShort("schemaVersion");
            if (version != VERSION) {
                throw new MetadataCodecException("unsupported append session snapshot schema");
            }
            com.nereusstream.metadata.oxia.records.AppendSessionSnapshotRecord value = readV2(reader);
            reader.requireConsumed();
            return value;
        }
        AppendSessionSnapshotRecord value = legacy.decode(bytes);
        return new com.nereusstream.metadata.oxia.records.AppendSessionSnapshotRecord(
                value.writerId(), value.epoch(), value.fencingToken(),
                value.leaseVersion(), value.expiresAtMillis());
    }

    static void writeV2(
            F4Binary.Writer writer,
            com.nereusstream.metadata.oxia.records.AppendSessionSnapshotRecord value) {
        writer.writeString(value.writerId());
        writer.writeLong(value.epoch());
        writer.writeString(value.fencingToken());
        writer.writeLong(value.leaseVersion());
        writer.writeLong(value.expiresAtMillis());
        writer.writeString(value.authorityType());
        writer.writeString(value.authorityId());
        writer.writeLong(value.authorityEpoch());
        writer.writeString(value.authorityOwnerId());
        writer.writeLong(value.authorityOwnerEpoch());
    }

    static com.nereusstream.metadata.oxia.records.AppendSessionSnapshotRecord readV2(
            F4Binary.Reader reader) {
        return new com.nereusstream.metadata.oxia.records.AppendSessionSnapshotRecord(
                reader.readString("writerId"),
                reader.readLong("epoch"),
                reader.readString("fencingToken"),
                reader.readLong("leaseVersion"),
                reader.readLong("expiresAtMillis"),
                reader.readString("authorityType"),
                reader.readString("authorityId"),
                reader.readLong("authorityEpoch"),
                reader.readString("authorityOwnerId"),
                reader.readLong("authorityOwnerEpoch"));
    }

    record AppendSessionSnapshotRecord(
            String writerId,
            long epoch,
            String fencingToken,
            long leaseVersion,
            long expiresAtMillis) {
    }
}
