/* Licensed under the Apache License, Version 2.0 */
package com.nereusstream.objectstore.checkpoint;

import com.nereusstream.api.Checksum;
import com.nereusstream.api.ObjectId;
import com.nereusstream.api.ObjectKey;
import java.util.Objects;

/** Verified bounded NRC1 header/footer state used by exact range lookups. */
public record RecoveryCheckpointObject(
        RecoveryCheckpointWriteRequest header,
        ObjectId objectId,
        ObjectKey objectKey,
        long objectLength,
        Checksum bodySha256,
        Checksum contentSha256,
        RecoveryCheckpointDirectory directory) {
    public RecoveryCheckpointObject {
        Objects.requireNonNull(header, "header");
        Objects.requireNonNull(objectId, "objectId");
        Objects.requireNonNull(objectKey, "objectKey");
        bodySha256 = RecoveryCheckpointValidation.requireSha256(bodySha256, "bodySha256");
        contentSha256 = RecoveryCheckpointValidation.requireSha256(contentSha256, "contentSha256");
        Objects.requireNonNull(directory, "directory");
        if (objectLength <= RecoveryCheckpointFormatV1.FOOTER_BYTES
                || objectLength > RecoveryCheckpointFormatV1.MAX_OBJECT_BYTES
                || !objectKey.equals(RecoveryCheckpointFormatV1.objectKey(header, contentSha256))
                || !objectId.equals(RecoveryCheckpointFormatV1.objectId(objectKey))
                || directory.commitDirectoryOffset() + directory.commitDirectoryLength()
                        != objectLength - RecoveryCheckpointFormatV1.FOOTER_BYTES) {
            throw new IllegalArgumentException("verified recovery checkpoint identity or layout is inconsistent");
        }
    }
}
