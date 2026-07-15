/* Licensed under the Apache License, Version 2.0 */
package com.nereusstream.materialization;

import com.nereusstream.api.Checksum;
import com.nereusstream.api.ChecksumType;
import com.nereusstream.api.MetadataCanonicalizer;
import com.nereusstream.api.OffsetRange;
import com.nereusstream.api.PayloadFormat;
import com.nereusstream.api.ProjectionRef;
import com.nereusstream.api.ReadView;
import com.nereusstream.api.SchemaRef;
import com.nereusstream.api.target.ReadTarget;
import com.nereusstream.metadata.oxia.codec.ReadTargetCodecRegistry;
import java.util.List;
import java.util.Objects;
import java.util.Optional;

/** Exact immutable source-index snapshot consumed by one materialization task. */
public record SourceGeneration(
        ReadView view,
        OffsetRange range,
        long generation,
        long commitVersion,
        String indexKey,
        long indexMetadataVersion,
        Checksum indexRecordSha256,
        ReadTarget readTarget,
        Checksum targetIdentitySha256,
        Optional<Checksum> materializationPolicySha256,
        PayloadFormat payloadFormat,
        Optional<ProjectionRef> projectionRef,
        int recordCount,
        int entryCount,
        long logicalBytes,
        List<SchemaRef> schemaRefs,
        long cumulativeSizeAtStart,
        long cumulativeSizeAtEnd) {
    public SourceGeneration {
        Objects.requireNonNull(view, "view");
        Objects.requireNonNull(range, "range");
        if (range.isEmpty() || generation < 0 || commitVersion <= 0
                || indexMetadataVersion < 0) {
            throw new IllegalArgumentException("source generation identity fields are invalid");
        }
        indexKey = requireText(indexKey, "indexKey");
        requireSha256(indexRecordSha256, "indexRecordSha256");
        Objects.requireNonNull(readTarget, "readTarget");
        requireSha256(targetIdentitySha256, "targetIdentitySha256");
        String encodedTargetIdentity = ReadTargetCodecRegistry.phase15()
                .encode(readTarget)
                .identityChecksumValue();
        if (!targetIdentitySha256.value().equals(encodedTargetIdentity)) {
            throw new IllegalArgumentException("targetIdentitySha256 does not match canonical target bytes");
        }
        materializationPolicySha256 = Objects.requireNonNull(
                materializationPolicySha256, "materializationPolicySha256");
        materializationPolicySha256.ifPresent(value -> requireSha256(
                value, "materializationPolicySha256"));
        if ((generation == 0) != materializationPolicySha256.isEmpty()) {
            throw new IllegalArgumentException("source policy digest presence must match generation zero");
        }
        if (generation == 0 && view != ReadView.COMMITTED) {
            throw new IllegalArgumentException("generation zero exists only in the COMMITTED source view");
        }
        Objects.requireNonNull(payloadFormat, "payloadFormat");
        projectionRef = Objects.requireNonNull(projectionRef, "projectionRef");
        if (recordCount <= 0 || recordCount != range.recordCount()
                || entryCount <= 0 || logicalBytes < 0
                || cumulativeSizeAtStart < 0
                || cumulativeSizeAtEnd < cumulativeSizeAtStart
                || Math.subtractExact(cumulativeSizeAtEnd, cumulativeSizeAtStart) != logicalBytes) {
            throw new IllegalArgumentException("source generation accounting is invalid");
        }
        schemaRefs = MetadataCanonicalizer.canonicalSchemaRefs(schemaRefs);
    }

    private static void requireSha256(Checksum checksum, String field) {
        Objects.requireNonNull(checksum, field);
        if (checksum.type() != ChecksumType.SHA256) {
            throw new IllegalArgumentException(field + " must be SHA256");
        }
    }

    private static String requireText(String value, String field) {
        Objects.requireNonNull(value, field);
        if (value.isBlank()) {
            throw new IllegalArgumentException(field + " cannot be blank");
        }
        return value;
    }
}
