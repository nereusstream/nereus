/* Licensed under the Apache License, Version 2.0 */
package com.nereusstream.materialization;

import com.nereusstream.api.Checksum;
import com.nereusstream.api.ChecksumType;
import com.nereusstream.api.OffsetRange;
import com.nereusstream.api.StreamId;
import java.util.Objects;

/** Minimal exact F4 authority identity persisted into a provider-specific source-retirement proof. */
public record CommittedGenerationRetirementProof(
        StreamId streamId,
        OffsetRange sourceRange,
        long sourceCommitVersion,
        String indexKey,
        long indexMetadataVersion,
        Checksum indexSha256) {
    public CommittedGenerationRetirementProof {
        Objects.requireNonNull(streamId, "streamId");
        Objects.requireNonNull(sourceRange, "sourceRange");
        indexKey = text(indexKey, "indexKey");
        if (sourceRange.isEmpty() || sourceCommitVersion <= 0 || indexMetadataVersion < 0) {
            throw new IllegalArgumentException("committed generation retirement identity is invalid");
        }
        Objects.requireNonNull(indexSha256, "indexSha256");
        if (indexSha256.type() != ChecksumType.SHA256) {
            throw new IllegalArgumentException("indexSha256 must use SHA256");
        }
    }

    public static CommittedGenerationRetirementProof from(CommittedObjectGenerationProof proof) {
        Objects.requireNonNull(proof, "proof");
        return new CommittedGenerationRetirementProof(
                proof.streamId(),
                proof.sourceRange(),
                proof.sourceCommitVersion(),
                proof.index().key(),
                proof.index().metadataVersion(),
                proof.index().durableValueSha256());
    }

    private static String text(String value, String field) {
        Objects.requireNonNull(value, field);
        if (value.isBlank()) {
            throw new IllegalArgumentException(field + " cannot be blank");
        }
        return value;
    }
}
