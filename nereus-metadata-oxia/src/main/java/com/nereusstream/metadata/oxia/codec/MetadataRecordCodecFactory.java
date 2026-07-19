/* Licensed under the Apache License, Version 2.0 */
package com.nereusstream.metadata.oxia.codec;

import java.util.Objects;

/** Explicit dual-generation envelope dispatch. It never probes decoders by trial and error. */
public final class MetadataRecordCodecFactory {
    private MetadataRecordCodecFactory() { }

    public static <T> byte[] encodeEnvelope(T record, Class<T> recordClass) {
        MetadataRecordCodec<T> codec = codecForClass(recordClass);
        return MetadataRecordEnvelope.encode(codec.recordType(), codec.schemaVersion(record),
                codec.minReaderSchemaVersion(record), MetadataRecordEnvelope.PAYLOAD_ENCODING_BINARY_V1,
                codec.encode(record));
    }

    public static <T> T decodeEnvelope(byte[] bytes, Class<T> recordClass) {
        Objects.requireNonNull(bytes, "bytes");
        MetadataRecordEnvelope.DecodedEnvelope envelope = MetadataRecordEnvelope.decode(bytes);
        MetadataRecordCodec<T> codec = codecForType(envelope.recordType());
        if (!codec.recordType().equals(recordClass.getSimpleName())) {
            throw new MetadataCodecException("metadata envelope record type mismatch: expected "
                    + recordClass.getSimpleName() + " but found " + envelope.recordType());
        }
        validateEnvelope(envelope, codec);
        T value = codec.decode(envelope.payload());
        if (codec.schemaVersion(value) != envelope.schemaVersion()
                || codec.minReaderSchemaVersion(value)
                        != envelope.minReaderSchemaVersion()) {
            throw new MetadataCodecException(
                    "metadata payload schema does not match its envelope");
        }
        if (!recordClass.isInstance(value)) {
            throw new MetadataCodecException("decoded metadata record has an unexpected Java type");
        }
        return value;
    }

    public static String recordType(byte[] bytes) {
        return MetadataRecordEnvelope.decode(bytes).recordType();
    }

    @SuppressWarnings("unchecked")
    private static <T> MetadataRecordCodec<T> codecForClass(Class<T> recordClass) {
        try {
            return Phase1MetadataCodecs.registry().codecForClass(recordClass);
        } catch (MetadataCodecException ignored) {
            try {
                return L0TargetMetadataCodecs.registry().codecForClass(recordClass);
            } catch (MetadataCodecException ignoredL0Target) {
                try {
                    return F2MetadataCodecs.registry().codecForClass(recordClass);
                } catch (MetadataCodecException ignoredF2) {
                    try {
                        return F3MetadataCodecs.registry().codecForClass(recordClass);
                    } catch (MetadataCodecException ignoredF3) {
                        return F4MetadataCodecs.registry().codecForClass(recordClass);
                    }
                }
            }
        }
    }

    @SuppressWarnings("unchecked")
    private static <T> MetadataRecordCodec<T> codecForType(String type) {
        try {
            return Phase1MetadataCodecs.registry().codecForType(type);
        } catch (MetadataCodecException ignored) {
            try {
                return L0TargetMetadataCodecs.registry().codecForType(type);
            } catch (MetadataCodecException ignoredL0Target) {
                try {
                    return F2MetadataCodecs.registry().codecForType(type);
                } catch (MetadataCodecException ignoredF2) {
                    try {
                        return F3MetadataCodecs.registry().codecForType(type);
                    } catch (MetadataCodecException ignoredF3) {
                        return F4MetadataCodecs.registry().codecForType(type);
                    }
                }
            }
        }
    }

    private static void validateEnvelope(
            MetadataRecordEnvelope.DecodedEnvelope envelope, MetadataRecordCodec<?> codec) {
        if (!MetadataRecordEnvelope.PAYLOAD_ENCODING_BINARY_V1.equals(envelope.payloadEncoding())
                || !codec.supportsEnvelopeSchema(
                        envelope.schemaVersion(), envelope.minReaderSchemaVersion())) {
            throw new MetadataCodecException("unsupported metadata envelope for " + codec.recordType());
        }
    }
}
