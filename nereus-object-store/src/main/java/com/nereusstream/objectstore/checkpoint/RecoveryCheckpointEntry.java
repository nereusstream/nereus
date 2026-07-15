/* Licensed under the Apache License, Version 2.0 */
package com.nereusstream.objectstore.checkpoint;

import com.nereusstream.api.Checksum;
import com.nereusstream.api.OffsetRange;
import java.nio.ByteBuffer;
import java.util.List;
import java.util.Objects;

/** One exact generic generation-zero commit envelope embedded in NRC1. */
public record RecoveryCheckpointEntry(
        long commitVersion,
        OffsetRange range,
        long cumulativeSizeAtEnd,
        String commitId,
        String previousCommitId,
        ByteBuffer canonicalCommitRecord,
        Checksum canonicalCommitRecordSha256,
        List<Integer> coveringPublicationIndexes) {
    public RecoveryCheckpointEntry {
        if (commitVersion <= 0) {
            throw new IllegalArgumentException("commitVersion must be positive");
        }
        Objects.requireNonNull(range, "range");
        if (range.isEmpty()) {
            throw new IllegalArgumentException("commit range cannot be empty");
        }
        if (cumulativeSizeAtEnd < 0) {
            throw new IllegalArgumentException("cumulativeSizeAtEnd must be non-negative");
        }
        commitId = RecoveryCheckpointValidation.requireText(commitId, "commitId");
        previousCommitId = RecoveryCheckpointValidation.requirePossiblyEmptyText(
                previousCommitId, "previousCommitId");
        canonicalCommitRecord = RecoveryCheckpointValidation.immutableRecordBytes(
                canonicalCommitRecord,
                canonicalCommitRecordSha256,
                "canonicalCommitRecord");
        canonicalCommitRecordSha256 = RecoveryCheckpointValidation.requireSha256(
                canonicalCommitRecordSha256, "canonicalCommitRecordSha256");
        coveringPublicationIndexes = List.copyOf(Objects.requireNonNull(
                coveringPublicationIndexes, "coveringPublicationIndexes"));
        if (coveringPublicationIndexes.isEmpty()
                || coveringPublicationIndexes.size()
                > RecoveryCheckpointFormatV1.MAX_PUBLICATION_REFS_PER_ENTRY) {
            throw new IllegalArgumentException("each NRC1 commit entry requires 1..8 publication references");
        }
        int previous = -1;
        for (int index : coveringPublicationIndexes) {
            if (index < 0 || index <= previous) {
                throw new IllegalArgumentException("publication indexes must be non-negative, unique, and sorted");
            }
            previous = index;
        }
    }
}
