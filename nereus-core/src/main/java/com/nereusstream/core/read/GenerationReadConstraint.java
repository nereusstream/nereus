/*
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 *   https://www.apache.org/licenses/LICENSE-2.0
 */

package com.nereusstream.core.read;

import com.nereusstream.api.Checksum;
import com.nereusstream.api.ChecksumType;
import com.nereusstream.api.OffsetRange;
import com.nereusstream.api.PublicationId;
import com.nereusstream.api.ReadView;
import com.nereusstream.api.StreamId;
import com.nereusstream.metadata.oxia.VersionedGenerationIndex;
import java.util.List;
import java.util.Objects;

/**
 * Exact, gap-free durable generation identities admitted by an external activation root.
 */
public record GenerationReadConstraint(
        StreamId streamId, ReadView view, OffsetRange coverage, List<Identity> identities) {
    public GenerationReadConstraint {
        Objects.requireNonNull(streamId, "streamId");
        Objects.requireNonNull(view, "view");
        Objects.requireNonNull(coverage, "coverage");
        identities = List.copyOf(Objects.requireNonNull(identities, "identities"));
        if (coverage.isEmpty() || identities.isEmpty()) {
            throw new IllegalArgumentException("generation read constraint must cover a non-empty range");
        }
        long cursor = coverage.startOffset();
        for (Identity identity : identities) {
            if (identity.coverage().startOffset() != cursor) {
                throw new IllegalArgumentException(
                        "generation read constraint identities must be ordered and gap-free");
            }
            cursor = identity.coverage().endOffset();
        }
        if (cursor != coverage.endOffset()) {
            throw new IllegalArgumentException("generation read constraint identities do not cover the declared range");
        }
    }

    public boolean admits(GenerationReadCandidate candidate) {
        Objects.requireNonNull(candidate, "candidate");
        if (candidate.view() != view || candidate.generationZero()) {
            return false;
        }
        return identities.stream().anyMatch(identity -> identity.matches(candidate));
    }

    public boolean admits(VersionedGenerationIndex index) {
        Objects.requireNonNull(index, "index");
        if (!index.value().streamId().equals(streamId.value())
                || ReadView.fromWireId(index.value().readViewId()) != view) {
            return false;
        }
        return identities.stream().anyMatch(identity -> identity.matches(index));
    }

    /**
     * Exact committed index identity rooted by the activation authority.
     */
    public record Identity(
            OffsetRange coverage,
            long generation,
            PublicationId publicationId,
            String indexKey,
            long indexMetadataVersion,
            Checksum indexRecordSha256) {
        public Identity {
            Objects.requireNonNull(coverage, "coverage");
            Objects.requireNonNull(publicationId, "publicationId");
            indexKey = requireText(indexKey, "indexKey");
            Objects.requireNonNull(indexRecordSha256, "indexRecordSha256");
            if (coverage.isEmpty()
                    || generation <= 0
                    || indexMetadataVersion < 0
                    || indexRecordSha256.type() != ChecksumType.SHA256) {
                throw new IllegalArgumentException("invalid generation read constraint identity");
            }
        }

        private boolean matches(GenerationReadCandidate candidate) {
            return candidate.resolvedRange().offsetRange().equals(coverage)
                    && candidate.resolvedRange().generation() == generation
                    && candidate.publicationId().orElseThrow().equals(publicationId)
                    && candidate.indexKey().equals(indexKey)
                    && candidate.indexMetadataVersion() == indexMetadataVersion
                    && candidate.indexRecordSha256().equals(indexRecordSha256);
        }

        private boolean matches(VersionedGenerationIndex index) {
            return index.value().offsetStart() == coverage.startOffset()
                    && index.value().offsetEnd() == coverage.endOffset()
                    && index.value().generation() == generation
                    && index.value().publicationId().equals(publicationId.value())
                    && index.key().equals(indexKey)
                    && index.metadataVersion() == indexMetadataVersion
                    && index.durableValueSha256().equals(indexRecordSha256);
        }

        private static String requireText(String value, String field) {
            Objects.requireNonNull(value, field);
            if (value.isBlank() || value.length() > 16_384) {
                throw new IllegalArgumentException(field + " must be non-blank and bounded");
            }
            return value;
        }
    }
}
