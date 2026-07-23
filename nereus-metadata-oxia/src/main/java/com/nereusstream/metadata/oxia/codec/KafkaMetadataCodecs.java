/* Licensed under the Apache License, Version 2.0 */
package com.nereusstream.metadata.oxia.codec;

import com.nereusstream.metadata.oxia.records.KafkaPartitionBindingRecord;
import com.nereusstream.metadata.oxia.records.KafkaPartitionRegistryRecord;
import java.util.List;

/** Closed F9 native-Kafka metadata codec family. */
public final class KafkaMetadataCodecs {
    private static final MapMetadataCodecRegistry REGISTRY = new MapMetadataCodecRegistry(List.of(
            registered(KafkaPartitionBindingRecord.class, new KafkaPartitionBindingRecordCodecV1()),
            registered(KafkaPartitionRegistryRecord.class, new KafkaPartitionRegistryRecordCodecV1())));

    private KafkaMetadataCodecs() { }

    public static MetadataCodecRegistry registry() { return REGISTRY; }

    public static <T> byte[] encodeEnvelope(T record, Class<T> type) {
        MetadataRecordCodec<T> codec = REGISTRY.codecForClass(type);
        return MetadataRecordEnvelope.encode(
                codec.recordType(), codec.schemaVersion(record), codec.minReaderSchemaVersion(record),
                MetadataRecordEnvelope.PAYLOAD_ENCODING_BINARY_V1, codec.encode(record));
    }

    public static <T> T decodeEnvelope(byte[] bytes, Class<T> type) {
        MetadataRecordEnvelope.DecodedEnvelope envelope = MetadataRecordEnvelope.decode(bytes);
        MetadataRecordCodec<T> codec = REGISTRY.codecForClass(type);
        if (!codec.recordType().equals(envelope.recordType())
                || !MetadataRecordEnvelope.PAYLOAD_ENCODING_BINARY_V1.equals(envelope.payloadEncoding())
                || !codec.supportsEnvelopeSchema(envelope.schemaVersion(), envelope.minReaderSchemaVersion())) {
            throw new MetadataCodecException("unsupported Kafka metadata envelope for " + type.getSimpleName());
        }
        T value = codec.decode(envelope.payload());
        if (codec.schemaVersion(value) != envelope.schemaVersion()
                || codec.minReaderSchemaVersion(value) != envelope.minReaderSchemaVersion()) {
            throw new MetadataCodecException("Kafka metadata payload schema does not match its envelope");
        }
        return value;
    }

    private static <T> MapMetadataCodecRegistry.RegisteredCodec<T> registered(
            Class<T> type, MetadataRecordCodec<T> codec) {
        return new MapMetadataCodecRegistry.RegisteredCodec<>(type, codec);
    }
}
