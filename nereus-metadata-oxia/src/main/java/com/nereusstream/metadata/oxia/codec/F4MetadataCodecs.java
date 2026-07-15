/* Licensed under the Apache License, Version 2.0 */
package com.nereusstream.metadata.oxia.codec;

import com.nereusstream.metadata.oxia.records.GenerationIndexRecord;
import com.nereusstream.metadata.oxia.records.GenerationSequenceRecord;
import com.nereusstream.metadata.oxia.records.MaterializationCheckpointRecord;
import com.nereusstream.metadata.oxia.records.MaterializationStreamRegistrationRecord;
import com.nereusstream.metadata.oxia.records.MaterializationTaskRecord;
import com.nereusstream.metadata.oxia.records.ObjectProtectionRecord;
import com.nereusstream.metadata.oxia.records.ObjectReaderLeaseRecord;
import com.nereusstream.metadata.oxia.records.PhysicalObjectRootRecord;
import com.nereusstream.metadata.oxia.records.RangeRetentionStatsRecord;
import com.nereusstream.metadata.oxia.records.RecoveryCheckpointRootRecord;
import java.util.HexFormat;
import java.util.List;

/** Explicit Phase 4 metadata record family using NRM1/binary-v1 envelopes. */
public final class F4MetadataCodecs {
    private static final MapMetadataCodecRegistry REGISTRY = new MapMetadataCodecRegistry(List.of(
            registered(GenerationSequenceRecord.class, new GenerationSequenceRecordCodecV1()),
            registered(GenerationIndexRecord.class, new GenerationIndexRecordCodecV1()),
            registered(MaterializationStreamRegistrationRecord.class,
                    new MaterializationStreamRegistrationRecordCodecV1()),
            registered(MaterializationTaskRecord.class, new MaterializationTaskRecordCodecV1()),
            registered(MaterializationCheckpointRecord.class, new MaterializationCheckpointRecordCodecV1()),
            registered(RangeRetentionStatsRecord.class, new RangeRetentionStatsRecordCodecV1()),
            registered(RecoveryCheckpointRootRecord.class, new RecoveryCheckpointRootRecordCodecV1()),
            registered(PhysicalObjectRootRecord.class, new PhysicalObjectRootRecordCodecV1()),
            registered(ObjectReaderLeaseRecord.class, new ObjectReaderLeaseRecordCodecV1()),
            registered(ObjectProtectionRecord.class, new ObjectProtectionRecordCodecV1())));

    private F4MetadataCodecs() {
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
                || envelope.minReaderSchemaVersion() != codec.minReaderSchemaVersion()) {
            throw new MetadataCodecException("unsupported F4 metadata envelope for " + recordClass.getSimpleName());
        }
        return codec.decode(envelope.payload());
    }

    public static String envelopeHex(Object record, Class<?> recordClass) {
        return HexFormat.of().formatHex(encodeEnvelopeUnchecked(record, recordClass));
    }

    private static <T> MapMetadataCodecRegistry.RegisteredCodec<T> registered(
            Class<T> type, MetadataRecordCodec<T> codec) {
        return new MapMetadataCodecRegistry.RegisteredCodec<>(type, codec);
    }

    @SuppressWarnings({"rawtypes", "unchecked"})
    private static byte[] encodeEnvelopeUnchecked(Object record, Class<?> recordClass) {
        return encodeEnvelope(record, (Class) recordClass);
    }
}
