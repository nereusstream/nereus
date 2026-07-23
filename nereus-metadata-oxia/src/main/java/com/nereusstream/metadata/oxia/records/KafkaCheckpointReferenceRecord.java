/* Licensed under the Apache License, Version 2.0 */
package com.nereusstream.metadata.oxia.records;

import java.util.Arrays;

public record KafkaCheckpointReferenceRecord(
        int referenceVersion,
        String objectId,
        String objectKey,
        long objectLength,
        byte[] objectSha256,
        long checkpointOffset,
        long logStartOffsetAtCheckpoint,
        long sourceCommitVersion,
        byte[] sourceHeadSha256,
        String writerBuild,
        long createdAtMillis) {
    public KafkaCheckpointReferenceRecord {
        if (referenceVersion != 1) throw new IllegalArgumentException("referenceVersion must be 1");
        objectId = KafkaMetadataValidation.text(objectId, "objectId");
        objectKey = KafkaMetadataValidation.text(objectKey, "objectKey");
        objectSha256 = KafkaMetadataValidation.sha256(objectSha256, "objectSha256", false);
        sourceHeadSha256 = KafkaMetadataValidation.sha256(sourceHeadSha256, "sourceHeadSha256", false);
        writerBuild = KafkaMetadataValidation.text(writerBuild, "writerBuild");
        if (objectLength <= 0 || checkpointOffset < 0 || logStartOffsetAtCheckpoint < 0
                || logStartOffsetAtCheckpoint > checkpointOffset || sourceCommitVersion < 0
                || createdAtMillis <= 0) {
            throw new IllegalArgumentException("invalid Kafka checkpoint reference");
        }
    }

    @Override public byte[] objectSha256() { return objectSha256.clone(); }
    @Override public byte[] sourceHeadSha256() { return sourceHeadSha256.clone(); }

    @Override
    public boolean equals(Object other) {
        return this == other || other instanceof KafkaCheckpointReferenceRecord that
                && referenceVersion == that.referenceVersion && objectLength == that.objectLength
                && checkpointOffset == that.checkpointOffset
                && logStartOffsetAtCheckpoint == that.logStartOffsetAtCheckpoint
                && sourceCommitVersion == that.sourceCommitVersion && createdAtMillis == that.createdAtMillis
                && objectId.equals(that.objectId) && objectKey.equals(that.objectKey)
                && writerBuild.equals(that.writerBuild)
                && Arrays.equals(objectSha256, that.objectSha256)
                && Arrays.equals(sourceHeadSha256, that.sourceHeadSha256);
    }

    @Override
    public int hashCode() {
        int result = java.util.Objects.hash(referenceVersion, objectId, objectKey, objectLength,
                checkpointOffset, logStartOffsetAtCheckpoint, sourceCommitVersion, writerBuild, createdAtMillis);
        result = 31 * result + Arrays.hashCode(objectSha256);
        return 31 * result + Arrays.hashCode(sourceHeadSha256);
    }
}
