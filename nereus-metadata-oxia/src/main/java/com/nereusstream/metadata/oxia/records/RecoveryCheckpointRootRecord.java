/* Licensed under the Apache License, Version 2.0 */
package com.nereusstream.metadata.oxia.records;

import com.nereusstream.metadata.oxia.RecoveryCheckpointRootDigests;
import java.util.List;

/** Single-key authority selecting the live NRC1 recovery-checkpoint set. */
public record RecoveryCheckpointRootRecord(
        int schemaVersion,
        String streamId,
        long checkpointSequence,
        long coveredStartOffset,
        long coveredEndOffset,
        long firstCommitVersion,
        long lastCommitVersion,
        long cumulativeSizeAtStart,
        long cumulativeSizeAtEnd,
        String firstCommitId,
        String lastCommitId,
        List<RecoveryCheckpointReferenceRecord> checkpoints,
        String checkpointSetSha256,
        String sourceHeadCommitId,
        long sourceHeadCommitVersion,
        long publishedAtMillis,
        long metadataVersion) {
    public RecoveryCheckpointRootRecord {
        F4RecordValidation.requireSchemaVersion(schemaVersion);
        streamId = F4RecordValidation.requireText(streamId, "streamId");
        F4RecordValidation.requireNonNegative(checkpointSequence, "checkpointSequence");
        F4RecordValidation.requireNonNegative(coveredStartOffset, "coveredStartOffset");
        if (coveredEndOffset < coveredStartOffset) {
            throw new IllegalArgumentException("recovery coverage is not ordered");
        }
        F4RecordValidation.requireNonNegative(firstCommitVersion, "firstCommitVersion");
        if (lastCommitVersion < firstCommitVersion) {
            throw new IllegalArgumentException("recovery commit versions are not ordered");
        }
        F4RecordValidation.requireNonNegative(cumulativeSizeAtStart, "cumulativeSizeAtStart");
        if (cumulativeSizeAtEnd < cumulativeSizeAtStart) {
            throw new IllegalArgumentException("recovery cumulative sizes are not ordered");
        }
        firstCommitId = F4RecordValidation.requireOptionalText(firstCommitId, "firstCommitId", 512);
        lastCommitId = F4RecordValidation.requireOptionalText(lastCommitId, "lastCommitId", 512);
        checkpoints = F4RecordValidation.immutableBoundedList(
                checkpoints, F4RecordValidation.MAX_RECOVERY_REFERENCES, "checkpoints");
        checkpointSetSha256 = F4RecordValidation.requireOptionalSha256(
                checkpointSetSha256, "checkpointSetSha256");
        sourceHeadCommitId = F4RecordValidation.requireOptionalText(
                sourceHeadCommitId, "sourceHeadCommitId", 512);
        F4RecordValidation.requireNonNegative(sourceHeadCommitVersion, "sourceHeadCommitVersion");
        F4RecordValidation.requireNonNegative(publishedAtMillis, "publishedAtMillis");
        F4RecordValidation.requireMetadataVersion(metadataVersion);
        if (checkpoints.isEmpty()) {
            if (checkpointSequence != 0 || coveredStartOffset != 0 || coveredEndOffset != 0
                    || firstCommitVersion != 0 || lastCommitVersion != 0
                    || cumulativeSizeAtStart != 0 || cumulativeSizeAtEnd != 0
                    || !firstCommitId.isEmpty() || !lastCommitId.isEmpty() || !checkpointSetSha256.isEmpty()
                    || !sourceHeadCommitId.isEmpty() || sourceHeadCommitVersion != 0 || publishedAtMillis != 0) {
                throw new IllegalArgumentException("empty recovery root must use the canonical bootstrap value");
            }
        } else {
            if (checkpointSequence <= 0 || checkpointSetSha256.isEmpty()
                    || firstCommitId.isEmpty() || lastCommitId.isEmpty() || sourceHeadCommitId.isEmpty()) {
                throw new IllegalArgumentException("non-empty recovery root lacks required identity");
            }
            RecoveryCheckpointReferenceRecord previous = null;
            for (RecoveryCheckpointReferenceRecord checkpoint : checkpoints) {
                if (previous != null && (checkpoint.coveredStartOffset() != previous.coveredEndOffset()
                        || checkpoint.firstCommitVersion() != previous.lastCommitVersion() + 1)) {
                    throw new IllegalArgumentException("checkpoint references must be gap-free and ordered");
                }
                previous = checkpoint;
            }
            RecoveryCheckpointReferenceRecord first = checkpoints.get(0);
            RecoveryCheckpointReferenceRecord last = checkpoints.get(checkpoints.size() - 1);
            if (first.coveredStartOffset() != coveredStartOffset || last.coveredEndOffset() != coveredEndOffset
                    || first.firstCommitVersion() != firstCommitVersion
                    || last.lastCommitVersion() != lastCommitVersion
                    || first.cumulativeSizeAtStart() != cumulativeSizeAtStart
                    || last.cumulativeSizeAtEnd() != cumulativeSizeAtEnd
                    || !first.firstCommitId().equals(firstCommitId) || !last.lastCommitId().equals(lastCommitId)) {
                throw new IllegalArgumentException("recovery root summary does not match checkpoint references");
            }
            if (!checkpointSetSha256.equals(
                    RecoveryCheckpointRootDigests.checkpointSetSha256(checkpoints).value())) {
                throw new IllegalArgumentException("checkpointSetSha256 does not match checkpoint references");
            }
        }
    }

    public RecoveryCheckpointRootRecord withMetadataVersion(long version) {
        return new RecoveryCheckpointRootRecord(
                schemaVersion, streamId, checkpointSequence, coveredStartOffset, coveredEndOffset,
                firstCommitVersion, lastCommitVersion, cumulativeSizeAtStart, cumulativeSizeAtEnd,
                firstCommitId, lastCommitId, checkpoints, checkpointSetSha256, sourceHeadCommitId,
                sourceHeadCommitVersion, publishedAtMillis, version);
    }
}
