/* Licensed under the Apache License, Version 2.0 */
package com.nereusstream.metadata.oxia.records;

import com.nereusstream.api.ObjectKey;
import com.nereusstream.api.ObjectKeyHash;
import com.nereusstream.api.SchemaRef;
import java.util.List;
import java.util.Objects;

/** Strictly verified immutable object output frozen into a task root. */
public record MaterializationOutputRecord(
        String outputAttemptId,
        String objectId,
        String objectKey,
        String objectKeyHash,
        long objectLength,
        String storageCrc32c,
        String contentSha256,
        String etag,
        String physicalFormat,
        String logicalFormat,
        ReadTargetRecord readTarget,
        String targetIdentitySha256,
        int sourceRecordCount,
        int outputRecordCount,
        int entryCount,
        long logicalBytes,
        List<SchemaRef> schemaRefs,
        long cumulativeSizeAtStart,
        long cumulativeSizeAtEnd,
        String sourceSetSha256,
        String projectionRef) {
    public MaterializationOutputRecord {
        outputAttemptId = F4RecordValidation.requireBase32Id(outputAttemptId, "outputAttemptId");
        objectId = F4RecordValidation.requireText(objectId, "objectId");
        objectKey = F4RecordValidation.requireText(objectKey, "objectKey");
        objectKeyHash = F4RecordValidation.requireText(objectKeyHash, "objectKeyHash");
        if (!ObjectKeyHash.from(new ObjectKey(objectKey)).value().equals(objectKeyHash)) {
            throw new IllegalArgumentException("objectKeyHash does not match objectKey");
        }
        F4RecordValidation.requirePositive(objectLength, "objectLength");
        storageCrc32c = F4RecordValidation.requireCrc32c(storageCrc32c, "storageCrc32c");
        contentSha256 = F4RecordValidation.requireSha256(contentSha256, "contentSha256");
        etag = F4RecordValidation.requireOptionalText(etag, "etag", 1024);
        physicalFormat = F4RecordValidation.requireText(physicalFormat, "physicalFormat");
        logicalFormat = F4RecordValidation.requireText(logicalFormat, "logicalFormat");
        Objects.requireNonNull(readTarget, "readTarget");
        targetIdentitySha256 = F4RecordValidation.requireSha256(targetIdentitySha256, "targetIdentitySha256");
        F4RecordValidation.requirePositive(sourceRecordCount, "sourceRecordCount");
        F4RecordValidation.requireNonNegative(outputRecordCount, "outputRecordCount");
        if (outputRecordCount > sourceRecordCount) {
            throw new IllegalArgumentException("output count cannot exceed source count");
        }
        F4RecordValidation.requirePositive(entryCount, "entryCount");
        F4RecordValidation.requireNonNegative(logicalBytes, "logicalBytes");
        schemaRefs = F4RecordValidation.canonicalSchemaRefs(schemaRefs);
        F4RecordValidation.requireNonNegative(cumulativeSizeAtStart, "cumulativeSizeAtStart");
        if (cumulativeSizeAtEnd < cumulativeSizeAtStart) {
            throw new IllegalArgumentException("output cumulative sizes are not ordered");
        }
        sourceSetSha256 = F4RecordValidation.requireSha256(sourceSetSha256, "sourceSetSha256");
        projectionRef = F4RecordValidation.requireOptionalText(projectionRef, "projectionRef", 4096);
    }
}
