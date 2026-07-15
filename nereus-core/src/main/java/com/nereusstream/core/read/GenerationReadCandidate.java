/* Licensed under the Apache License, Version 2.0 */
package com.nereusstream.core.read;

import com.nereusstream.api.Checksum;
import com.nereusstream.api.ChecksumType;
import com.nereusstream.api.PublicationId;
import com.nereusstream.api.ReadView;
import com.nereusstream.api.ResolvedRange;
import java.util.Objects;
import java.util.Optional;

/** Exact durable index identity admitted for one physical read attempt. */
public record GenerationReadCandidate(
        ReadView view,
        ResolvedRange resolvedRange,
        String indexKey,
        long indexMetadataVersion,
        Checksum indexRecordSha256,
        boolean generationZero,
        Optional<PublicationId> publicationId) {
    public GenerationReadCandidate {
        Objects.requireNonNull(view, "view");
        Objects.requireNonNull(resolvedRange, "resolvedRange");
        indexKey = requireText(indexKey, "indexKey");
        if (indexMetadataVersion < 0) {
            throw new IllegalArgumentException("indexMetadataVersion must be non-negative");
        }
        Objects.requireNonNull(indexRecordSha256, "indexRecordSha256");
        if (indexRecordSha256.type() != ChecksumType.SHA256) {
            throw new IllegalArgumentException("indexRecordSha256 must use SHA256");
        }
        publicationId = Objects.requireNonNull(publicationId, "publicationId");
        if (generationZero) {
            if (resolvedRange.generation() != 0 || publicationId.isPresent()) {
                throw new IllegalArgumentException(
                        "generation-zero candidate cannot carry higher-generation identity");
            }
        } else if (resolvedRange.generation() <= 0 || publicationId.isEmpty()) {
            throw new IllegalArgumentException(
                    "higher-generation candidate requires a positive generation and publication id");
        }
    }

    private static String requireText(String value, String field) {
        Objects.requireNonNull(value, field);
        if (value.isBlank() || value.length() > 16_384) {
            throw new IllegalArgumentException(field + " must be non-blank and bounded");
        }
        return value;
    }
}
