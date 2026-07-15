/* Licensed under the Apache License, Version 2.0 */
package com.nereusstream.metadata.oxia.records;

import com.nereusstream.api.SchemaRef;
import java.util.List;
import java.util.Objects;

/** Immutable source-index snapshot embedded in one materialization task. */
public record SourceGenerationRecord(
        int readViewId,
        long offsetStart,
        long offsetEnd,
        long generation,
        long commitVersion,
        String indexKey,
        long indexMetadataVersion,
        String indexRecordSha256,
        ReadTargetRecord readTarget,
        String targetIdentitySha256,
        String materializationPolicySha256,
        String payloadFormat,
        String projectionRef,
        int recordCount,
        int entryCount,
        long logicalBytes,
        List<SchemaRef> schemaRefs,
        long cumulativeSizeAtStart,
        long cumulativeSizeAtEnd) {
    public SourceGenerationRecord {
        if (readViewId != 1 && readViewId != 2) {
            throw new IllegalArgumentException("readViewId must identify a known view");
        }
        F4RecordValidation.requireDenseRange(offsetStart, offsetEnd, recordCount, "source generation range");
        F4RecordValidation.requireNonNegative(generation, "generation");
        F4RecordValidation.requirePositive(commitVersion, "commitVersion");
        indexKey = F4RecordValidation.requireText(indexKey, "indexKey");
        F4RecordValidation.requireMetadataVersion(indexMetadataVersion);
        indexRecordSha256 = F4RecordValidation.requireSha256(indexRecordSha256, "indexRecordSha256");
        Objects.requireNonNull(readTarget, "readTarget");
        targetIdentitySha256 = F4RecordValidation.requireSha256(targetIdentitySha256, "targetIdentitySha256");
        materializationPolicySha256 = F4RecordValidation.requireOptionalSha256(
                materializationPolicySha256, "materializationPolicySha256");
        if ((generation == 0) != materializationPolicySha256.isEmpty()) {
            throw new IllegalArgumentException("source policy digest presence must match generation zero");
        }
        if (generation == 0 && readViewId != 1) {
            throw new IllegalArgumentException("generation zero exists only in the COMMITTED source view");
        }
        payloadFormat = F4RecordValidation.requireText(payloadFormat, "payloadFormat");
        projectionRef = F4RecordValidation.requireOptionalText(projectionRef, "projectionRef", 4096);
        F4RecordValidation.requirePositive(entryCount, "entryCount");
        F4RecordValidation.requireNonNegative(logicalBytes, "logicalBytes");
        schemaRefs = F4RecordValidation.canonicalSchemaRefs(schemaRefs);
        F4RecordValidation.requireNonNegative(cumulativeSizeAtStart, "cumulativeSizeAtStart");
        if (cumulativeSizeAtEnd < cumulativeSizeAtStart
                || Math.subtractExact(cumulativeSizeAtEnd, cumulativeSizeAtStart) != logicalBytes) {
            throw new IllegalArgumentException("source cumulative sizes do not match logical bytes");
        }
    }
}
