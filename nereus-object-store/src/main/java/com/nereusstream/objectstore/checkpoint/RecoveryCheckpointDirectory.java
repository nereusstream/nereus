/* Licensed under the Apache License, Version 2.0 */
package com.nereusstream.objectstore.checkpoint;

/** Exact NRC1 directory byte ranges copied into the recovery-root reference. */
public record RecoveryCheckpointDirectory(
        long publicationDirectoryOffset,
        long publicationDirectoryLength,
        long commitDirectoryOffset,
        long commitDirectoryLength,
        int commitDirectoryStride) {
    public RecoveryCheckpointDirectory {
        if (publicationDirectoryOffset <= 0
                || publicationDirectoryLength < Integer.BYTES
                || commitDirectoryOffset <= publicationDirectoryOffset
                || commitDirectoryLength < Integer.BYTES * 2L
                || commitDirectoryStride != RecoveryCheckpointFormatV1.COMMIT_DIRECTORY_STRIDE) {
            throw new IllegalArgumentException("recovery checkpoint directory is invalid");
        }
        long publicationEnd;
        long directoryBytes;
        try {
            publicationEnd = Math.addExact(publicationDirectoryOffset, publicationDirectoryLength);
            directoryBytes = Math.addExact(publicationDirectoryLength, commitDirectoryLength);
        } catch (ArithmeticException failure) {
            throw new IllegalArgumentException("recovery checkpoint directory overflows", failure);
        }
        if (publicationEnd != commitDirectoryOffset
                || directoryBytes > RecoveryCheckpointFormatV1.MAX_DIRECTORY_BYTES) {
            throw new IllegalArgumentException("recovery checkpoint directory layout is non-canonical");
        }
    }
}
