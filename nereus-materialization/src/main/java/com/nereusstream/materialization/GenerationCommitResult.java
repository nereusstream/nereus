/* Licensed under the Apache License, Version 2.0 */
package com.nereusstream.materialization;

import com.nereusstream.api.Checksum;
import com.nereusstream.api.ChecksumType;
import com.nereusstream.api.GenerationId;
import com.nereusstream.api.OffsetRange;
import com.nereusstream.api.PublicationId;
import com.nereusstream.api.ReadView;
import com.nereusstream.api.StreamId;
import java.util.Objects;

/** Exact committed index identity returned after index-owned physical protection is re-proved. */
public record GenerationCommitResult(
        StreamId streamId,
        ReadView view,
        OffsetRange coverage,
        GenerationId generation,
        PublicationId publicationId,
        String indexKey,
        long indexMetadataVersion,
        Checksum indexRecordSha256,
        boolean committedByThisCall) {
    public GenerationCommitResult {
        Objects.requireNonNull(streamId, "streamId");
        Objects.requireNonNull(view, "view");
        Objects.requireNonNull(coverage, "coverage");
        Objects.requireNonNull(generation, "generation");
        Objects.requireNonNull(publicationId, "publicationId");
        Objects.requireNonNull(indexKey, "indexKey");
        Objects.requireNonNull(indexRecordSha256, "indexRecordSha256");
        if (coverage.isEmpty() || generation.value() <= 0 || indexKey.isBlank()
                || indexMetadataVersion < 0 || indexRecordSha256.type() != ChecksumType.SHA256) {
            throw new IllegalArgumentException("generation commit result identity is invalid");
        }
    }
}
