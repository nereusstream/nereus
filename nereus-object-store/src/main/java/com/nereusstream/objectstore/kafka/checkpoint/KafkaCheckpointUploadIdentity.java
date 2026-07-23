/* Licensed under the Apache License, Version 2.0 */
package com.nereusstream.objectstore.kafka.checkpoint;

import com.nereusstream.api.Checksum;
import com.nereusstream.api.ChecksumType;
import com.nereusstream.api.ObjectId;
import com.nereusstream.api.ObjectKey;
import java.util.Objects;

/** Exact immutable object identity known after NKC1 staging and before PUT. */
public record KafkaCheckpointUploadIdentity(
        ObjectId objectId,
        ObjectKey objectKey,
        long objectLength,
        Checksum storageCrc32c,
        Checksum objectSha256) {
    public KafkaCheckpointUploadIdentity {
        Objects.requireNonNull(objectId, "objectId");
        Objects.requireNonNull(objectKey, "objectKey");
        if (objectLength <= 0 || objectLength > KafkaCheckpointFormatV1.MAX_OBJECT_BYTES) {
            throw new IllegalArgumentException("invalid NKC1 upload length");
        }
        Objects.requireNonNull(storageCrc32c, "storageCrc32c");
        Objects.requireNonNull(objectSha256, "objectSha256");
        if (storageCrc32c.type() != ChecksumType.CRC32C
                || objectSha256.type() != ChecksumType.SHA256) {
            throw new IllegalArgumentException("invalid NKC1 upload checksum types");
        }
    }
}
