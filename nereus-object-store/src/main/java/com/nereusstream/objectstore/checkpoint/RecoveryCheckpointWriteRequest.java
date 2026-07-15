/* Licensed under the Apache License, Version 2.0 */
package com.nereusstream.objectstore.checkpoint;

import com.nereusstream.api.Checksum;
import com.nereusstream.api.OffsetRange;
import com.nereusstream.api.StreamId;
import java.util.Objects;

/** Immutable facts copied into the NRC1 header and deterministic object key. */
public record RecoveryCheckpointWriteRequest(
        String cluster,
        StreamId streamId,
        long checkpointSequence,
        String checkpointAttemptId,
        OffsetRange coverage,
        long firstCommitVersion,
        long lastCommitVersion,
        long cumulativeSizeAtStart,
        long cumulativeSizeAtEnd,
        String firstCommitId,
        String lastCommitId,
        String sourceHeadCommitId,
        long sourceHeadCommitVersion,
        Checksum projectionIdentitySha256,
        int expectedEntryCount,
        int expectedPublicationCount) {
    public RecoveryCheckpointWriteRequest {
        cluster = RecoveryCheckpointValidation.requireText(cluster, "cluster");
        Objects.requireNonNull(streamId, "streamId");
        RecoveryCheckpointValidation.requireText(streamId.value(), "streamId");
        if (checkpointSequence <= 0) {
            throw new IllegalArgumentException("checkpointSequence must be positive");
        }
        checkpointAttemptId = RecoveryCheckpointValidation.requireBase32Id(
                checkpointAttemptId, "checkpointAttemptId");
        Objects.requireNonNull(coverage, "coverage");
        if (coverage.isEmpty()) {
            throw new IllegalArgumentException("checkpoint coverage cannot be empty");
        }
        if (firstCommitVersion <= 0 || lastCommitVersion < firstCommitVersion) {
            throw new IllegalArgumentException("checkpoint commit versions are invalid");
        }
        long versionCount;
        try {
            versionCount = Math.addExact(Math.subtractExact(lastCommitVersion, firstCommitVersion), 1L);
        } catch (ArithmeticException failure) {
            throw new IllegalArgumentException("checkpoint commit-version range overflows", failure);
        }
        if (cumulativeSizeAtStart < 0 || cumulativeSizeAtEnd < cumulativeSizeAtStart) {
            throw new IllegalArgumentException("checkpoint cumulative sizes are invalid");
        }
        firstCommitId = RecoveryCheckpointValidation.requireText(firstCommitId, "firstCommitId");
        lastCommitId = RecoveryCheckpointValidation.requireText(lastCommitId, "lastCommitId");
        sourceHeadCommitId = RecoveryCheckpointValidation.requireText(sourceHeadCommitId, "sourceHeadCommitId");
        if (sourceHeadCommitVersion < lastCommitVersion) {
            throw new IllegalArgumentException("source head cannot precede checkpoint commit coverage");
        }
        projectionIdentitySha256 = RecoveryCheckpointValidation.requireSha256(
                projectionIdentitySha256, "projectionIdentitySha256");
        if (expectedEntryCount <= 0
                || expectedEntryCount > RecoveryCheckpointFormatV1.MAX_ENTRY_COUNT
                || expectedEntryCount != versionCount) {
            throw new IllegalArgumentException(
                    "expectedEntryCount must exactly match the bounded commit-version range");
        }
        if (expectedPublicationCount <= 0
                || expectedPublicationCount > RecoveryCheckpointFormatV1.MAX_PUBLICATION_COUNT) {
            throw new IllegalArgumentException("expectedPublicationCount is outside the NRC1 limit");
        }
    }
}
