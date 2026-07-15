/* Licensed under the Apache License, Version 2.0 */
package com.nereusstream.objectstore.checkpoint;

import com.nereusstream.api.Checksum;
import com.nereusstream.api.OffsetRange;
import com.nereusstream.api.PublicationId;
import java.nio.ByteBuffer;
import java.util.Objects;

/** One canonical committed lossless generation embedded in an NRC1 publication table. */
public record RecoveryCheckpointPublication(
        long generation,
        PublicationId publicationId,
        OffsetRange coverage,
        ByteBuffer canonicalGenerationIndexRecord,
        Checksum generationIndexRecordSha256) {
    public RecoveryCheckpointPublication {
        if (generation <= 0) {
            throw new IllegalArgumentException("generation must be positive");
        }
        Objects.requireNonNull(publicationId, "publicationId");
        Objects.requireNonNull(coverage, "coverage");
        if (coverage.isEmpty()) {
            throw new IllegalArgumentException("publication coverage cannot be empty");
        }
        canonicalGenerationIndexRecord = RecoveryCheckpointValidation.immutableRecordBytes(
                canonicalGenerationIndexRecord,
                generationIndexRecordSha256,
                "canonicalGenerationIndexRecord");
        generationIndexRecordSha256 = RecoveryCheckpointValidation.requireSha256(
                generationIndexRecordSha256, "generationIndexRecordSha256");
    }
}
