/* Licensed under the Apache License, Version 2.0 */
package com.nereusstream.metadata.oxia.codec;

import com.nereusstream.metadata.oxia.records.CursorRetentionRecord;
import com.nereusstream.metadata.oxia.records.CursorStateRecord;
import java.util.HexFormat;
import java.util.List;

/** Fourth explicit metadata record family for F3 cursor roots. */
public final class F3MetadataCodecs {
    private static final MapMetadataCodecRegistry REGISTRY = new MapMetadataCodecRegistry(List.of(
            new MapMetadataCodecRegistry.RegisteredCodec<>(
                    CursorStateRecord.class, new CursorStateRecordCodecV1()),
            new MapMetadataCodecRegistry.RegisteredCodec<>(
                    CursorRetentionRecord.class, new CursorRetentionRecordCodecV1())));

    private F3MetadataCodecs() {
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
            throw new MetadataCodecException("unsupported F3 metadata envelope for " + recordClass.getSimpleName());
        }
        return codec.decode(envelope.payload());
    }

    public static String envelopeHex(Object record, Class<?> recordClass) {
        return HexFormat.of().formatHex(encodeEnvelopeUnchecked(record, recordClass));
    }

    @SuppressWarnings({"rawtypes", "unchecked"})
    private static byte[] encodeEnvelopeUnchecked(Object record, Class<?> recordClass) {
        return encodeEnvelope(record, (Class) recordClass);
    }
}
