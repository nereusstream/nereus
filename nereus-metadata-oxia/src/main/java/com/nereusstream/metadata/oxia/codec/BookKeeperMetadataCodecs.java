/* Licensed under the Apache License, Version 2.0 */
package com.nereusstream.metadata.oxia.codec;

import com.nereusstream.metadata.oxia.records.BookKeeperAllocationSlotRecord;
import com.nereusstream.metadata.oxia.records.BookKeeperAppendReservationRecord;
import com.nereusstream.metadata.oxia.records.BookKeeperLedgerProtectionRecord;
import com.nereusstream.metadata.oxia.records.BookKeeperLedgerReaderLeaseRecord;
import com.nereusstream.metadata.oxia.records.BookKeeperLedgerRootRecord;
import com.nereusstream.metadata.oxia.records.BookKeeperWriterStateRecord;
import com.nereusstream.metadata.oxia.records.LedgerAllocationIntentRecord;
import java.util.HexFormat;
import java.util.List;

/** Explicit V1 registry for BookKeeper primary-WAL metadata records. */
public final class BookKeeperMetadataCodecs {
    private static final MapMetadataCodecRegistry REGISTRY = new MapMetadataCodecRegistry(List.of(
            registered(BookKeeperWriterStateRecord.class, new BookKeeperWriterStateRecordCodecV1()),
            registered(LedgerAllocationIntentRecord.class, new LedgerAllocationIntentRecordCodecV1()),
            registered(BookKeeperAllocationSlotRecord.class, new BookKeeperAllocationSlotRecordCodecV1()),
            registered(BookKeeperLedgerRootRecord.class, new BookKeeperLedgerRootRecordCodecV1()),
            registered(BookKeeperAppendReservationRecord.class, new BookKeeperAppendReservationRecordCodecV1()),
            registered(BookKeeperLedgerProtectionRecord.class, new BookKeeperLedgerProtectionRecordCodecV1()),
            registered(BookKeeperLedgerReaderLeaseRecord.class, new BookKeeperLedgerReaderLeaseRecordCodecV1())));

    private BookKeeperMetadataCodecs() { }

    public static MetadataCodecRegistry registry() { return REGISTRY; }

    public static <T> byte[] encodeEnvelope(T record, Class<T> type) {
        MetadataRecordCodec<T> codec = REGISTRY.codecForClass(type);
        return MetadataRecordEnvelope.encode(codec.recordType(), codec.schemaVersion(record),
                codec.minReaderSchemaVersion(record), MetadataRecordEnvelope.PAYLOAD_ENCODING_BINARY_V1,
                codec.encode(record));
    }

    public static <T> T decodeEnvelope(byte[] bytes, Class<T> type) {
        MetadataRecordEnvelope.DecodedEnvelope envelope = MetadataRecordEnvelope.decode(bytes);
        MetadataRecordCodec<T> codec = REGISTRY.codecForClass(type);
        if (!codec.recordType().equals(envelope.recordType())
                || !MetadataRecordEnvelope.PAYLOAD_ENCODING_BINARY_V1.equals(envelope.payloadEncoding())
                || !codec.supportsEnvelopeSchema(envelope.schemaVersion(), envelope.minReaderSchemaVersion())) {
            throw new MetadataCodecException("unsupported BookKeeper metadata envelope for " + type.getSimpleName());
        }
        T value = codec.decode(envelope.payload());
        if (codec.schemaVersion(value) != envelope.schemaVersion()
                || codec.minReaderSchemaVersion(value) != envelope.minReaderSchemaVersion()) {
            throw new MetadataCodecException("BookKeeper metadata payload schema does not match its envelope");
        }
        return value;
    }

    public static String envelopeHex(Object record, Class<?> type) {
        return HexFormat.of().formatHex(encodeUnchecked(record, type));
    }

    private static <T> MapMetadataCodecRegistry.RegisteredCodec<T> registered(Class<T> type, MetadataRecordCodec<T> codec) {
        return new MapMetadataCodecRegistry.RegisteredCodec<>(type, codec);
    }

    @SuppressWarnings({"rawtypes", "unchecked"})
    private static byte[] encodeUnchecked(Object record, Class<?> type) {
        return encodeEnvelope(record, (Class) type);
    }
}
