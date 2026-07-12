/* Licensed under the Apache License, Version 2.0 */
package com.nereusstream.metadata.oxia.codec;

import com.nereusstream.metadata.oxia.records.LedgerIdAllocatorRecord;
import com.nereusstream.metadata.oxia.records.PositionIndexRecord;
import com.nereusstream.metadata.oxia.records.TopicProjectionRecord;
import com.nereusstream.metadata.oxia.records.VirtualLedgerProjectionRecord;
import java.util.List;
import java.util.HexFormat;
import java.util.Objects;
import java.util.function.ToLongFunction;

/** Third explicit metadata record family for F2 projection records. */
public final class F2MetadataCodecs {
    private static final MapMetadataCodecRegistry REGISTRY = new MapMetadataCodecRegistry(List.of(
            registered(LedgerIdAllocatorRecord.class, LedgerIdAllocatorRecord::metadataVersion),
            registered(TopicProjectionRecord.class, TopicProjectionRecord::metadataVersion),
            registered(VirtualLedgerProjectionRecord.class, VirtualLedgerProjectionRecord::metadataVersion),
            registered(PositionIndexRecord.class, PositionIndexRecord::metadataVersion)));

    private F2MetadataCodecs() {
    }

    public static MetadataCodecRegistry registry() {
        return REGISTRY;
    }

    public static <T> byte[] encodeEnvelope(T record, Class<T> recordClass) {
        MetadataRecordCodec<T> codec = REGISTRY.codecForClass(recordClass);
        return MetadataRecordEnvelope.encode(
                codec.recordType(),
                codec.schemaVersion(),
                codec.minReaderSchemaVersion(),
                MetadataRecordEnvelope.PAYLOAD_ENCODING_BINARY_V1,
                codec.encode(record));
    }

    public static <T> T decodeEnvelope(byte[] bytes, Class<T> recordClass) {
        MetadataRecordEnvelope.DecodedEnvelope envelope = MetadataRecordEnvelope.decode(bytes);
        MetadataRecordCodec<T> codec = REGISTRY.codecForClass(recordClass);
        if (!codec.recordType().equals(envelope.recordType())
                || !MetadataRecordEnvelope.PAYLOAD_ENCODING_BINARY_V1.equals(envelope.payloadEncoding())
                || envelope.schemaVersion() != codec.schemaVersion()
                || envelope.minReaderSchemaVersion() > codec.schemaVersion()) {
            throw new MetadataCodecException("unsupported F2 metadata envelope for " + recordClass.getSimpleName());
        }
        return codec.decode(envelope.payload());
    }

    public static String envelopeHex(Object record, Class<?> recordClass) {
        return HexFormat.of().formatHex(encodeEnvelopeUnchecked(record, recordClass));
    }

    @SuppressWarnings({"unchecked", "rawtypes"})
    private static byte[] encodeEnvelopeUnchecked(Object record, Class<?> recordClass) {
        return encodeEnvelope(record, (Class) recordClass);
    }

    private static <T extends Record> MapMetadataCodecRegistry.RegisteredCodec<T> registered(
            Class<T> type,
            ToLongFunction<T> metadataVersion) {
        return new MapMetadataCodecRegistry.RegisteredCodec<>(
                type,
                new ZeroMetadataVersionCodec<>(
                        Phase1MetadataCodecs.recordCodec(type), metadataVersion));
    }

    private static final class ZeroMetadataVersionCodec<T extends Record> implements MetadataRecordCodec<T> {
        private final MetadataRecordCodec<T> delegate;
        private final ToLongFunction<T> metadataVersion;

        private ZeroMetadataVersionCodec(
                MetadataRecordCodec<T> delegate,
                ToLongFunction<T> metadataVersion) {
            this.delegate = Objects.requireNonNull(delegate, "delegate");
            this.metadataVersion = Objects.requireNonNull(metadataVersion, "metadataVersion");
        }

        @Override
        public String recordType() {
            return delegate.recordType();
        }

        @Override
        public int schemaVersion() {
            return delegate.schemaVersion();
        }

        @Override
        public int minReaderSchemaVersion() {
            return delegate.minReaderSchemaVersion();
        }

        @Override
        public byte[] encode(T record) {
            if (metadataVersion.applyAsLong(record) != 0) {
                throw new MetadataCodecException("encoded F2 metadata version must be zero");
            }
            return delegate.encode(record);
        }

        @Override
        public T decode(byte[] bytes) {
            T record = delegate.decode(bytes);
            if (metadataVersion.applyAsLong(record) != 0) {
                throw new MetadataCodecException("durable F2 metadata version must be zero");
            }
            return record;
        }
    }
}
