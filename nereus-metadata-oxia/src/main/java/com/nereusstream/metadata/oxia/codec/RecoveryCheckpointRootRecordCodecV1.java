/* Licensed under the Apache License, Version 2.0 */
package com.nereusstream.metadata.oxia.codec;

import com.nereusstream.metadata.oxia.records.RecoveryCheckpointReferenceRecord;
import com.nereusstream.metadata.oxia.records.RecoveryCheckpointRootRecord;
import java.util.ArrayList;
import java.util.List;

public final class RecoveryCheckpointRootRecordCodecV1
        extends AbstractF4RecordCodecV1<RecoveryCheckpointRootRecord> {
    public RecoveryCheckpointRootRecordCodecV1() {
        super(RecoveryCheckpointRootRecord.class);
    }

    @Override
    public byte[] encode(RecoveryCheckpointRootRecord value) {
        try {
            F4Binary.Writer writer = writer();
            writer.writeString(value.streamId());
            writer.writeLong(value.checkpointSequence());
            writer.writeLong(value.coveredStartOffset());
            writer.writeLong(value.coveredEndOffset());
            writer.writeLong(value.firstCommitVersion());
            writer.writeLong(value.lastCommitVersion());
            writer.writeLong(value.cumulativeSizeAtStart());
            writer.writeLong(value.cumulativeSizeAtEnd());
            writer.writeString(value.firstCommitId());
            writer.writeString(value.lastCommitId());
            writer.writeInt(value.checkpoints().size());
            for (RecoveryCheckpointReferenceRecord checkpoint : value.checkpoints()) {
                writeReference(writer, checkpoint);
            }
            writer.writeString(value.checkpointSetSha256());
            writer.writeString(value.sourceHeadCommitId());
            writer.writeLong(value.sourceHeadCommitVersion());
            writer.writeLong(value.publishedAtMillis());
            writer.writeLong(value.metadataVersion());
            return writer.toByteArray();
        } catch (RuntimeException failure) {
            throw malformed(failure);
        }
    }

    @Override
    public RecoveryCheckpointRootRecord decode(byte[] bytes) {
        try {
            F4Binary.Reader reader = reader(bytes);
            String streamId = reader.readString("streamId");
            long checkpointSequence = reader.readLong("checkpointSequence");
            long coveredStartOffset = reader.readLong("coveredStartOffset");
            long coveredEndOffset = reader.readLong("coveredEndOffset");
            long firstCommitVersion = reader.readLong("firstCommitVersion");
            long lastCommitVersion = reader.readLong("lastCommitVersion");
            long cumulativeSizeAtStart = reader.readLong("cumulativeSizeAtStart");
            long cumulativeSizeAtEnd = reader.readLong("cumulativeSizeAtEnd");
            String firstCommitId = reader.readString("firstCommitId");
            String lastCommitId = reader.readString("lastCommitId");
            int count = reader.readCount("checkpointCount", Long.BYTES * 10, 32);
            List<RecoveryCheckpointReferenceRecord> checkpoints = new ArrayList<>(count);
            for (int index = 0; index < count; index++) {
                checkpoints.add(readReference(reader));
            }
            RecoveryCheckpointRootRecord value = new RecoveryCheckpointRootRecord(
                    VERSION,
                    streamId,
                    checkpointSequence,
                    coveredStartOffset,
                    coveredEndOffset,
                    firstCommitVersion,
                    lastCommitVersion,
                    cumulativeSizeAtStart,
                    cumulativeSizeAtEnd,
                    firstCommitId,
                    lastCommitId,
                    checkpoints,
                    reader.readString("checkpointSetSha256"),
                    reader.readString("sourceHeadCommitId"),
                    reader.readLong("sourceHeadCommitVersion"),
                    reader.readLong("publishedAtMillis"),
                    reader.readLong("metadataVersion"));
            reader.requireConsumed();
            return value;
        } catch (RuntimeException failure) {
            throw malformed(failure);
        }
    }

    private static void writeReference(F4Binary.Writer writer, RecoveryCheckpointReferenceRecord value) {
        writer.writeLong(value.checkpointSequence());
        writer.writeString(value.checkpointAttemptId());
        writer.writeLong(value.coveredStartOffset());
        writer.writeLong(value.coveredEndOffset());
        writer.writeLong(value.firstCommitVersion());
        writer.writeLong(value.lastCommitVersion());
        writer.writeLong(value.cumulativeSizeAtStart());
        writer.writeLong(value.cumulativeSizeAtEnd());
        writer.writeString(value.firstCommitId());
        writer.writeString(value.lastCommitId());
        writer.writeString(value.sourceHeadCommitId());
        writer.writeLong(value.sourceHeadCommitVersion());
        writer.writeString(value.projectionIdentitySha256());
        writer.writeString(value.objectId());
        writer.writeString(value.objectKey());
        writer.writeString(value.objectKeyHash());
        writer.writeLong(value.objectLength());
        writer.writeString(value.storageCrc32c());
        writer.writeString(value.contentSha256());
        writer.writeInt(value.commitEntryCount());
        writer.writeInt(value.publicationCount());
    }

    private static RecoveryCheckpointReferenceRecord readReference(F4Binary.Reader reader) {
        return new RecoveryCheckpointReferenceRecord(
                reader.readLong("checkpointSequence"),
                reader.readString("checkpointAttemptId"),
                reader.readLong("coveredStartOffset"),
                reader.readLong("coveredEndOffset"),
                reader.readLong("firstCommitVersion"),
                reader.readLong("lastCommitVersion"),
                reader.readLong("cumulativeSizeAtStart"),
                reader.readLong("cumulativeSizeAtEnd"),
                reader.readString("firstCommitId"),
                reader.readString("lastCommitId"),
                reader.readString("sourceHeadCommitId"),
                reader.readLong("sourceHeadCommitVersion"),
                reader.readString("projectionIdentitySha256"),
                reader.readString("objectId"),
                reader.readString("objectKey"),
                reader.readString("objectKeyHash"),
                reader.readLong("objectLength"),
                reader.readString("storageCrc32c"),
                reader.readString("contentSha256"),
                reader.readInt("commitEntryCount"),
                reader.readInt("publicationCount"));
    }
}
