/* Licensed under the Apache License, Version 2.0 */
package com.nereusstream.objectstore.kafka.checkpoint;

import com.nereusstream.api.Checksum;
import com.nereusstream.api.ChecksumType;
import com.nereusstream.api.ObjectId;
import com.nereusstream.api.ObjectKey;
import java.util.List;
import java.util.Objects;
import java.util.Optional;

/** Fully verified immutable NKC1 object and decoded state sections. */
public record KafkaCheckpointObject(
        KafkaCheckpointHeader header,
        List<KafkaCheckpointSection> sections,
        ObjectId objectId,
        ObjectKey objectKey,
        long objectLength,
        Checksum storageCrc32c,
        Checksum objectSha256,
        Checksum contentSha256,
        Optional<String> etag) {
    public KafkaCheckpointObject {
        Objects.requireNonNull(header, "header");
        sections = List.copyOf(Objects.requireNonNull(sections, "sections"));
        Objects.requireNonNull(objectId, "objectId");
        Objects.requireNonNull(objectKey, "objectKey");
        if (objectLength <= 0 || objectLength > KafkaCheckpointFormatV1.MAX_OBJECT_BYTES) {
            throw new IllegalArgumentException("invalid Kafka checkpoint object length");
        }
        requireChecksum(storageCrc32c, ChecksumType.CRC32C, "storageCrc32c");
        requireChecksum(objectSha256, ChecksumType.SHA256, "objectSha256");
        requireChecksum(contentSha256, ChecksumType.SHA256, "contentSha256");
        etag = Objects.requireNonNull(etag, "etag").map(value -> {
            if (value.isBlank() || value.length() > 1_024) {
                throw new IllegalArgumentException("etag must be nonblank and bounded");
            }
            return value;
        });
    }

    private static void requireChecksum(Checksum value, ChecksumType type, String name) {
        Objects.requireNonNull(value, name);
        if (value.type() != type) throw new IllegalArgumentException(name + " has the wrong type");
    }
}
