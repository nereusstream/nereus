/* Licensed under the Apache License, Version 2.0 */
package com.nereusstream.metadata.oxia.records;

import com.nereusstream.api.ObjectKey;
import com.nereusstream.api.ObjectKeyHash;

/** One immutable NRC1 object referenced by the per-stream recovery root. */
public record RecoveryCheckpointReferenceRecord(
        long checkpointSequence,
        String checkpointAttemptId,
        long coveredStartOffset,
        long coveredEndOffset,
        long firstCommitVersion,
        long lastCommitVersion,
        long cumulativeSizeAtStart,
        long cumulativeSizeAtEnd,
        String firstCommitId,
        String lastCommitId,
        String sourceHeadCommitId,
        long sourceHeadCommitVersion,
        String projectionIdentitySha256,
        String objectId,
        String objectKey,
        String objectKeyHash,
        long objectLength,
        String storageCrc32c,
        String contentSha256,
        int commitEntryCount,
        int publicationCount) {
    public RecoveryCheckpointReferenceRecord {
        F4RecordValidation.requirePositive(checkpointSequence, "checkpointSequence");
        checkpointAttemptId = F4RecordValidation.requireBase32Id(checkpointAttemptId, "checkpointAttemptId");
        F4RecordValidation.requireRange(coveredStartOffset, coveredEndOffset, "checkpoint coverage");
        F4RecordValidation.requirePositive(firstCommitVersion, "firstCommitVersion");
        if (lastCommitVersion < firstCommitVersion) {
            throw new IllegalArgumentException("checkpoint commit versions are not ordered");
        }
        F4RecordValidation.requireNonNegative(cumulativeSizeAtStart, "cumulativeSizeAtStart");
        if (cumulativeSizeAtEnd < cumulativeSizeAtStart) {
            throw new IllegalArgumentException("checkpoint cumulative sizes are not ordered");
        }
        firstCommitId = F4RecordValidation.requireText(firstCommitId, "firstCommitId");
        lastCommitId = F4RecordValidation.requireText(lastCommitId, "lastCommitId");
        sourceHeadCommitId = F4RecordValidation.requireText(sourceHeadCommitId, "sourceHeadCommitId");
        if (sourceHeadCommitVersion < lastCommitVersion) {
            throw new IllegalArgumentException("source head version cannot precede checkpoint coverage");
        }
        projectionIdentitySha256 = F4RecordValidation.requireSha256(
                projectionIdentitySha256, "projectionIdentitySha256");
        objectId = F4RecordValidation.requireText(objectId, "objectId");
        objectKey = F4RecordValidation.requireText(objectKey, "objectKey");
        objectKeyHash = F4RecordValidation.requireText(objectKeyHash, "objectKeyHash");
        if (!ObjectKeyHash.from(new ObjectKey(objectKey)).value().equals(objectKeyHash)) {
            throw new IllegalArgumentException("objectKeyHash does not match objectKey");
        }
        F4RecordValidation.requirePositive(objectLength, "objectLength");
        storageCrc32c = F4RecordValidation.requireCrc32c(storageCrc32c, "storageCrc32c");
        contentSha256 = F4RecordValidation.requireSha256(contentSha256, "contentSha256");
        F4RecordValidation.requirePositive(commitEntryCount, "commitEntryCount");
        F4RecordValidation.requirePositive(publicationCount, "publicationCount");
    }
}
