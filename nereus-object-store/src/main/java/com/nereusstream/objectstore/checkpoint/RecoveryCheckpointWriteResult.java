/* Licensed under the Apache License, Version 2.0 */
package com.nereusstream.objectstore.checkpoint;

import com.nereusstream.api.Checksum;
import com.nereusstream.api.ObjectId;
import com.nereusstream.api.ObjectKey;
import com.nereusstream.api.ObjectKeyHash;
import com.nereusstream.objectstore.staging.StagedObjectFile;
import java.util.Objects;

/** Close-owned sealed NRC1 bytes and their exact immutable publication identity. */
public record RecoveryCheckpointWriteResult(
        StagedObjectFile stagingFile,
        ObjectId objectId,
        ObjectKey objectKey,
        ObjectKeyHash objectKeyHash,
        long objectLength,
        Checksum storageCrc32c,
        Checksum bodySha256,
        Checksum contentSha256,
        RecoveryCheckpointDirectory directory) implements AutoCloseable {
    public RecoveryCheckpointWriteResult {
        Objects.requireNonNull(stagingFile, "stagingFile");
        Objects.requireNonNull(objectId, "objectId");
        Objects.requireNonNull(objectKey, "objectKey");
        Objects.requireNonNull(objectKeyHash, "objectKeyHash");
        if (!objectId.equals(RecoveryCheckpointFormatV1.objectId(objectKey))
                || !objectKeyHash.equals(RecoveryCheckpointFormatV1.objectKeyHash(objectKey))
                || objectLength <= RecoveryCheckpointFormatV1.FOOTER_BYTES
                || objectLength > RecoveryCheckpointFormatV1.MAX_OBJECT_BYTES
                || stagingFile.sealedLength() != objectLength) {
            throw new IllegalArgumentException("recovery checkpoint identity or length is inconsistent");
        }
        storageCrc32c = RecoveryCheckpointValidation.requireCrc32c(storageCrc32c, "storageCrc32c");
        bodySha256 = RecoveryCheckpointValidation.requireSha256(bodySha256, "bodySha256");
        contentSha256 = RecoveryCheckpointValidation.requireSha256(contentSha256, "contentSha256");
        if (!storageCrc32c.equals(stagingFile.storageCrc32c())
                || !contentSha256.equals(stagingFile.contentSha256())) {
            throw new IllegalArgumentException("recovery checkpoint checksums do not match staging bytes");
        }
        Objects.requireNonNull(directory, "directory");
        if (directory.commitDirectoryOffset() + directory.commitDirectoryLength()
                != objectLength - RecoveryCheckpointFormatV1.FOOTER_BYTES) {
            throw new IllegalArgumentException("recovery checkpoint footer does not immediately follow directories");
        }
    }

    @Override
    public void close() {
        stagingFile.close();
    }
}
