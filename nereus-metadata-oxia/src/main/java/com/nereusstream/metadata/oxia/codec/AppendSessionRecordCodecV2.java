/* Licensed under the Apache License, Version 2.0 */
package com.nereusstream.metadata.oxia.codec;

import java.nio.ByteBuffer;
import java.util.Objects;

/** Dual V1/V2 codec that preserves the frozen six-field legacy append-session payload. */
public final class AppendSessionRecordCodecV2
        implements MetadataRecordCodec<com.nereusstream.metadata.oxia.records.AppendSessionRecord> {
    private static final int LEGACY_VERSION = 1;
    private static final int VERSION = 2;
    private final MetadataRecordCodec<AppendSessionRecord> legacy =
            Phase1MetadataCodecs.recordCodec(AppendSessionRecord.class);

    @Override
    public String recordType() {
        return com.nereusstream.metadata.oxia.records.AppendSessionRecord.class.getSimpleName();
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
    public int schemaVersion(com.nereusstream.metadata.oxia.records.AppendSessionRecord value) {
        return Objects.requireNonNull(value, "value").authority().isPresent() ? VERSION : LEGACY_VERSION;
    }

    @Override
    public int minReaderSchemaVersion(com.nereusstream.metadata.oxia.records.AppendSessionRecord value) {
        return schemaVersion(value);
    }

    @Override
    public boolean supportsEnvelopeSchema(int writerSchemaVersion, int minimumReaderSchemaVersion) {
        return (writerSchemaVersion == LEGACY_VERSION && minimumReaderSchemaVersion == LEGACY_VERSION)
                || (writerSchemaVersion == VERSION && minimumReaderSchemaVersion == VERSION);
    }

    @Override
    public byte[] encode(com.nereusstream.metadata.oxia.records.AppendSessionRecord value) {
        Objects.requireNonNull(value, "value");
        if (value.authority().isEmpty()) {
            return legacy.encode(new AppendSessionRecord(
                    value.streamId(), value.writerId(), value.epoch(), value.fencingToken(),
                    value.leaseVersion(), value.expiresAtMillis()));
        }
        F4Binary.Writer writer = new F4Binary.Writer();
        writer.writeUnsignedShort(VERSION);
        writer.writeString(value.streamId());
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
        return writer.toByteArray();
    }

    @Override
    public com.nereusstream.metadata.oxia.records.AppendSessionRecord decode(byte[] bytes) {
        Objects.requireNonNull(bytes, "bytes");
        if (bytes.length >= Short.BYTES
                && Short.toUnsignedInt(ByteBuffer.wrap(bytes).getShort()) == VERSION) {
            try {
                F4Binary.Reader reader = new F4Binary.Reader(bytes);
                int version = reader.readUnsignedShort("schemaVersion");
                if (version != VERSION) {
                    throw new MetadataCodecException("unsupported AppendSessionRecord schema version");
                }
                var value = new com.nereusstream.metadata.oxia.records.AppendSessionRecord(
                        reader.readString("streamId"),
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
                reader.requireConsumed();
                return value;
            } catch (RuntimeException failure) {
                throw F4Binary.malformed(recordType(), failure);
            }
        }
        AppendSessionRecord value = legacy.decode(bytes);
        return new com.nereusstream.metadata.oxia.records.AppendSessionRecord(
                value.streamId(), value.writerId(), value.epoch(), value.fencingToken(),
                value.leaseVersion(), value.expiresAtMillis());
    }

    record AppendSessionRecord(
            String streamId,
            String writerId,
            long epoch,
            String fencingToken,
            long leaseVersion,
            long expiresAtMillis) {
    }
}
