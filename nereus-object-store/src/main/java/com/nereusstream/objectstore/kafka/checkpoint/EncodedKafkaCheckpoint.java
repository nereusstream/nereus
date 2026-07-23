/* Licensed under the Apache License, Version 2.0 */
package com.nereusstream.objectstore.kafka.checkpoint;

import com.nereusstream.api.Checksum;
import com.nereusstream.objectstore.staging.PrivateStagedObjectFile;
import java.util.Objects;

/** Close-owned private staged NKC1 bytes. */
public record EncodedKafkaCheckpoint(
        PrivateStagedObjectFile stagingFile,
        long contentLength,
        Checksum contentSha256) implements AutoCloseable {
    public EncodedKafkaCheckpoint {
        Objects.requireNonNull(stagingFile, "stagingFile");
        if (contentLength <= 0 || contentLength >= stagingFile.sealedLength()) {
            throw new IllegalArgumentException("invalid staged NKC1 content length");
        }
        KafkaCheckpointFormatV1.requireSha256(contentSha256, "contentSha256");
    }

    public long objectLength() {
        return stagingFile.sealedLength();
    }

    public Checksum storageCrc32c() {
        return stagingFile.storageCrc32c();
    }

    public Checksum objectSha256() {
        return stagingFile.contentSha256();
    }

    @Override
    public void close() {
        stagingFile.close();
    }
}
