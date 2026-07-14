/* Licensed under the Apache License, Version 2.0 */
package com.nereusstream.metadata.oxia.codec;

import com.nereusstream.metadata.oxia.records.CursorAckRangeRecord;
import com.nereusstream.metadata.oxia.records.CursorPartialBatchAckRecord;
import com.nereusstream.metadata.oxia.records.CursorRecordLifecycle;
import com.nereusstream.metadata.oxia.records.CursorSnapshotReferenceRecord;
import com.nereusstream.metadata.oxia.records.CursorStateRecord;
import com.nereusstream.metadata.oxia.records.ManagedLedgerProjectionIdentity;
import java.util.ArrayList;
import java.util.List;
import java.util.Optional;
import java.util.OptionalLong;

/** Explicit canonical binary V1 codec for {@link CursorStateRecord}. */
public final class CursorStateRecordCodecV1 implements MetadataRecordCodec<CursorStateRecord> {
    private static final int VERSION = 1;

    @Override
    public String recordType() {
        return CursorStateRecord.class.getSimpleName();
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
    public byte[] encode(CursorStateRecord record) {
        try {
            F3Binary.Writer writer = new F3Binary.Writer();
            writer.writeUnsignedShort(VERSION);
            writer.writeLong(record.metadataVersion());
            writeIdentity(writer, record.projection());
            writer.writeString(record.ownerSessionId());
            writer.writeString(record.cursorName());
            writer.writeString(record.cursorNameHash());
            writer.writeLong(record.cursorGeneration());
            writer.writeByte(record.lifecycle().id());
            writer.writeLong(record.mutationSequence());
            writer.writeLong(record.ackStateEpoch());
            writer.writeString(record.lastProtectionAttemptId());
            writer.writeLong(record.markDeleteOffset());
            writer.writeByte(record.snapshotReference().isPresent() ? 1 : 0);
            record.snapshotReference().ifPresent(reference -> writeSnapshotReference(writer, reference));
            writeRanges(writer, record.inlineWholeAckDeltas());
            writePartials(writer, record.inlinePartialAckOverrides());
            F3Binary.writeLongMap(writer, record.positionProperties());
            F3Binary.writeStringMap(writer, record.cursorProperties());
            writer.writeLong(record.createdAtMillis());
            writer.writeLong(record.updatedAtMillis());
            writer.writeByte(record.deletedAtMillis().isPresent() ? 1 : 0);
            record.deletedAtMillis().ifPresent(writer::writeLong);
            return writer.toByteArray();
        } catch (RuntimeException e) {
            throw F3Binary.malformed(recordType(), e);
        }
    }

    @Override
    public CursorStateRecord decode(byte[] bytes) {
        try {
            F3Binary.Reader reader = new F3Binary.Reader(bytes);
            requireVersion(reader.readUnsignedShort("payloadVersion"));
            long metadataVersion = reader.readLong("metadataVersion");
            ManagedLedgerProjectionIdentity identity = readIdentity(reader);
            String ownerSessionId = reader.readString("ownerSessionId");
            String cursorName = reader.readString("cursorName");
            String cursorNameHash = reader.readString("cursorNameHash");
            long generation = reader.readLong("cursorGeneration");
            CursorRecordLifecycle lifecycle = CursorRecordLifecycle.fromId(
                    reader.readUnsignedByte("lifecycleId"));
            long sequence = reader.readLong("mutationSequence");
            long ackStateEpoch = reader.readLong("ackStateEpoch");
            String attempt = reader.readString("lastProtectionAttemptId");
            long markDelete = reader.readLong("markDeleteOffset");
            Optional<CursorSnapshotReferenceRecord> snapshot = switch (
                    reader.readUnsignedByte("snapshotPresent")) {
                case 0 -> Optional.empty();
                case 1 -> Optional.of(readSnapshotReference(reader));
                default -> throw new MetadataCodecException("invalid snapshot-present flag");
            };
            List<CursorAckRangeRecord> ranges = readRanges(reader);
            List<CursorPartialBatchAckRecord> partials = readPartials(reader);
            var positionProperties = F3Binary.readLongMap(reader, "positionProperties");
            var cursorProperties = F3Binary.readStringMap(reader, "cursorProperties");
            long createdAt = reader.readLong("createdAtMillis");
            long updatedAt = reader.readLong("updatedAtMillis");
            OptionalLong deletedAt = switch (reader.readUnsignedByte("deletedAtPresent")) {
                case 0 -> OptionalLong.empty();
                case 1 -> OptionalLong.of(reader.readLong("deletedAtMillis"));
                default -> throw new MetadataCodecException("invalid deleted-at-present flag");
            };
            reader.requireConsumed();
            return new CursorStateRecord(
                    metadataVersion,
                    identity,
                    ownerSessionId,
                    cursorName,
                    cursorNameHash,
                    generation,
                    lifecycle,
                    sequence,
                    ackStateEpoch,
                    attempt,
                    markDelete,
                    snapshot,
                    ranges,
                    partials,
                    positionProperties,
                    cursorProperties,
                    createdAt,
                    updatedAt,
                    deletedAt);
        } catch (RuntimeException e) {
            throw F3Binary.malformed(recordType(), e);
        }
    }

    static void writeIdentity(F3Binary.Writer writer, ManagedLedgerProjectionIdentity identity) {
        writer.writeLong(identity.storageClassBindingGeneration());
        writer.writeLong(identity.incarnation());
        writer.writeString(identity.streamId());
        writer.writeLong(identity.virtualLedgerId());
    }

    static ManagedLedgerProjectionIdentity readIdentity(F3Binary.Reader reader) {
        return new ManagedLedgerProjectionIdentity(
                reader.readLong("storageClassBindingGeneration"),
                reader.readLong("incarnation"),
                reader.readString("streamId"),
                reader.readLong("virtualLedgerId"));
    }

    static void writePartial(F3Binary.Writer writer, CursorPartialBatchAckRecord partial) {
        writer.writeLong(partial.entryOffset());
        writer.writeInt(partial.batchSize());
        long[] words = partial.remainingWords();
        writer.writeCount(words.length);
        for (long word : words) {
            writer.writeLong(word);
        }
    }

    static CursorPartialBatchAckRecord readPartial(F3Binary.Reader reader) {
        long offset = reader.readLong("partial entryOffset");
        int batchSize = reader.readInt("partial batchSize");
        int wordCount = reader.readCount("partial wordCount", Long.BYTES);
        List<Long> decoded = F3Binary.readLongs(reader, "partial remainingWord", wordCount);
        long[] words = new long[decoded.size()];
        for (int index = 0; index < words.length; index++) {
            words[index] = decoded.get(index);
        }
        return new CursorPartialBatchAckRecord(offset, batchSize, words);
    }

    private static void writeRanges(F3Binary.Writer writer, List<CursorAckRangeRecord> ranges) {
        writer.writeCount(ranges.size());
        for (CursorAckRangeRecord range : ranges) {
            writer.writeLong(range.startOffset());
            writer.writeLong(range.endOffset());
        }
    }

    private static List<CursorAckRangeRecord> readRanges(F3Binary.Reader reader) {
        int count = reader.readCount("wholeRangeCount", Long.BYTES * 2);
        List<CursorAckRangeRecord> ranges = new ArrayList<>(count);
        for (int index = 0; index < count; index++) {
            ranges.add(new CursorAckRangeRecord(
                    reader.readLong("range startOffset"),
                    reader.readLong("range endOffset")));
        }
        return List.copyOf(ranges);
    }

    private static void writePartials(F3Binary.Writer writer, List<CursorPartialBatchAckRecord> partials) {
        writer.writeCount(partials.size());
        partials.forEach(partial -> writePartial(writer, partial));
    }

    private static List<CursorPartialBatchAckRecord> readPartials(F3Binary.Reader reader) {
        int count = reader.readCount("partialCount", Long.BYTES + Integer.BYTES * 2);
        List<CursorPartialBatchAckRecord> partials = new ArrayList<>(count);
        for (int index = 0; index < count; index++) {
            partials.add(readPartial(reader));
        }
        return List.copyOf(partials);
    }

    private static void writeSnapshotReference(
            F3Binary.Writer writer, CursorSnapshotReferenceRecord reference) {
        writer.writeString(reference.objectKey());
        writer.writeString(reference.snapshotId());
        writer.writeLong(reference.cursorGeneration());
        writer.writeLong(reference.sourceMutationSequence());
        writer.writeLong(reference.baseMarkDeleteOffset());
        writer.writeLong(reference.objectLength());
        writer.writeString(reference.storageChecksumType());
        writer.writeString(reference.storageChecksumValue());
        writer.writeInt(reference.formatCrc32c());
        writer.writeInt(reference.formatVersion());
        writer.writeLong(reference.createdAtMillis());
    }

    private static CursorSnapshotReferenceRecord readSnapshotReference(F3Binary.Reader reader) {
        return new CursorSnapshotReferenceRecord(
                reader.readString("snapshot objectKey"),
                reader.readString("snapshot snapshotId"),
                reader.readLong("snapshot cursorGeneration"),
                reader.readLong("snapshot sourceMutationSequence"),
                reader.readLong("snapshot baseMarkDeleteOffset"),
                reader.readLong("snapshot objectLength"),
                reader.readString("snapshot storageChecksumType"),
                reader.readString("snapshot storageChecksumValue"),
                reader.readInt("snapshot formatCrc32c"),
                reader.readInt("snapshot formatVersion"),
                reader.readLong("snapshot createdAtMillis"));
    }

    private static void requireVersion(int version) {
        if (version != VERSION) {
            throw new MetadataCodecException("unsupported CursorStateRecord payload version: " + version);
        }
    }
}
