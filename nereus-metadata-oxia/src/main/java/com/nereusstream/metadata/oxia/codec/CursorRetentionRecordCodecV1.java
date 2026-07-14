/* Licensed under the Apache License, Version 2.0 */
package com.nereusstream.metadata.oxia.codec;

import com.nereusstream.metadata.oxia.records.CursorProtectionIntentRecord;
import com.nereusstream.metadata.oxia.records.CursorProtectionKind;
import com.nereusstream.metadata.oxia.records.CursorRetentionLifecycle;
import com.nereusstream.metadata.oxia.records.CursorRetentionRecord;
import com.nereusstream.metadata.oxia.records.ManagedLedgerProjectionIdentity;
import java.util.Optional;
import java.util.OptionalLong;

/** Explicit canonical binary V1 codec for {@link CursorRetentionRecord}. */
public final class CursorRetentionRecordCodecV1 implements MetadataRecordCodec<CursorRetentionRecord> {
    private static final int VERSION = 1;
    private static final int MAX_PROTECTION_INTENT_BYTES = 48 * 1024;

    @Override
    public String recordType() {
        return CursorRetentionRecord.class.getSimpleName();
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
    public byte[] encode(CursorRetentionRecord record) {
        try {
            F3Binary.Writer writer = new F3Binary.Writer();
            writer.writeUnsignedShort(VERSION);
            writer.writeLong(record.metadataVersion());
            CursorStateRecordCodecV1.writeIdentity(writer, record.projection());
            writer.writeString(record.ownerSessionId());
            writer.writeByte(record.lifecycle().id());
            writer.writeLong(record.mutationSequence());
            writer.writeLong(record.protectedFloorOffset());
            writer.writeLong(record.lastCompletedTrimOffset());
            switch (record.lifecycle()) {
                case ACTIVE -> { }
                case PROTECTION_PENDING -> {
                    F3Binary.Writer intentWriter = new F3Binary.Writer();
                    writeProtectionIntent(intentWriter, record.pendingProtectionIntent().orElseThrow());
                    byte[] intent = intentWriter.toByteArray();
                    if (intent.length > MAX_PROTECTION_INTENT_BYTES) {
                        throw new MetadataCodecException("cursor protection intent exceeds the frozen byte cap");
                    }
                    writer.writeRaw(intent);
                }
                case TRIM_PENDING -> {
                    writer.writeString(record.pendingTrimAttemptId().orElseThrow());
                    writer.writeLong(record.pendingTrimOffset().orElseThrow());
                    writer.writeString(record.pendingTrimReason().orElseThrow());
                }
            }
            writer.writeLong(record.updatedAtMillis());
            return writer.toByteArray();
        } catch (RuntimeException e) {
            throw F3Binary.malformed(recordType(), e);
        }
    }

    @Override
    public CursorRetentionRecord decode(byte[] bytes) {
        try {
            F3Binary.Reader reader = new F3Binary.Reader(bytes);
            int version = reader.readUnsignedShort("payloadVersion");
            if (version != VERSION) {
                throw new MetadataCodecException("unsupported CursorRetentionRecord payload version: " + version);
            }
            long metadataVersion = reader.readLong("metadataVersion");
            ManagedLedgerProjectionIdentity projection = CursorStateRecordCodecV1.readIdentity(reader);
            String owner = reader.readString("ownerSessionId");
            CursorRetentionLifecycle lifecycle = CursorRetentionLifecycle.fromId(
                    reader.readUnsignedByte("lifecycleId"));
            long sequence = reader.readLong("mutationSequence");
            long floor = reader.readLong("protectedFloorOffset");
            long trim = reader.readLong("lastCompletedTrimOffset");
            Optional<CursorProtectionIntentRecord> protection = Optional.empty();
            Optional<String> trimAttempt = Optional.empty();
            OptionalLong trimOffset = OptionalLong.empty();
            Optional<String> trimReason = Optional.empty();
            if (lifecycle == CursorRetentionLifecycle.PROTECTION_PENDING) {
                int start = reader.position();
                protection = Optional.of(readProtectionIntent(reader));
                if (reader.position() - start > MAX_PROTECTION_INTENT_BYTES) {
                    throw new MetadataCodecException("cursor protection intent exceeds the frozen byte cap");
                }
            } else if (lifecycle == CursorRetentionLifecycle.TRIM_PENDING) {
                trimAttempt = Optional.of(reader.readString("pendingTrimAttemptId"));
                trimOffset = OptionalLong.of(reader.readLong("pendingTrimOffset"));
                trimReason = Optional.of(reader.readString("pendingTrimReason"));
            }
            long updatedAt = reader.readLong("updatedAtMillis");
            reader.requireConsumed();
            return new CursorRetentionRecord(
                    metadataVersion,
                    projection,
                    owner,
                    lifecycle,
                    sequence,
                    floor,
                    trim,
                    protection,
                    trimAttempt,
                    trimOffset,
                    trimReason,
                    updatedAt);
        } catch (RuntimeException e) {
            throw F3Binary.malformed(recordType(), e);
        }
    }

    private static void writeProtectionIntent(
            F3Binary.Writer writer, CursorProtectionIntentRecord intent) {
        writer.writeString(intent.attemptId());
        writer.writeByte(intent.kind().id());
        writer.writeString(intent.cursorName());
        writer.writeString(intent.cursorNameHash());
        writer.writeLong(intent.expectedCursorGeneration());
        writer.writeLong(intent.targetCursorGeneration());
        writer.writeLong(intent.targetMarkDeleteOffset());
        writer.writeByte(intent.targetPartialBatch().isPresent() ? 1 : 0);
        intent.targetPartialBatch().ifPresent(partial ->
                CursorStateRecordCodecV1.writePartial(writer, partial));
        F3Binary.writeLongMap(writer, intent.initialPositionProperties());
        F3Binary.writeStringMap(writer, intent.initialCursorProperties());
        writer.writeLong(intent.createdAtMillis());
    }

    private static CursorProtectionIntentRecord readProtectionIntent(F3Binary.Reader reader) {
        String attempt = reader.readString("protectionAttemptId");
        CursorProtectionKind kind = CursorProtectionKind.fromId(
                reader.readUnsignedByte("protectionKindId"));
        String name = reader.readString("cursorName");
        String hash = reader.readString("cursorNameHash");
        long expectedGeneration = reader.readLong("expectedCursorGeneration");
        long targetGeneration = reader.readLong("targetCursorGeneration");
        long targetMarkDelete = reader.readLong("targetMarkDeleteOffset");
        Optional<com.nereusstream.metadata.oxia.records.CursorPartialBatchAckRecord> partial = switch (
                reader.readUnsignedByte("targetPartialPresent")) {
            case 0 -> Optional.empty();
            case 1 -> Optional.of(CursorStateRecordCodecV1.readPartial(reader));
            default -> throw new MetadataCodecException("invalid target-partial-present flag");
        };
        var positionProperties = F3Binary.readLongMap(reader, "initialPositionProperties");
        var cursorProperties = F3Binary.readStringMap(reader, "initialCursorProperties");
        long createdAt = reader.readLong("intentCreatedAtMillis");
        return new CursorProtectionIntentRecord(
                attempt,
                kind,
                name,
                hash,
                expectedGeneration,
                targetGeneration,
                targetMarkDelete,
                partial,
                positionProperties,
                cursorProperties,
                createdAt);
    }
}
