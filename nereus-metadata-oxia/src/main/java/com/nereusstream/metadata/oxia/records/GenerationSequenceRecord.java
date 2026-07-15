/* Licensed under the Apache License, Version 2.0 */
package com.nereusstream.metadata.oxia.records;

/** View-scoped monotonic generation allocator root. */
public record GenerationSequenceRecord(
        int schemaVersion,
        String streamId,
        int readViewId,
        long lastAllocatedGeneration,
        long allocationSequence,
        String lastPublicationId,
        long updatedAtMillis,
        long metadataVersion) {
    public GenerationSequenceRecord {
        F4RecordValidation.requireSchemaVersion(schemaVersion);
        streamId = F4RecordValidation.requireText(streamId, "streamId");
        if (readViewId != 1 && readViewId != 2) {
            throw new IllegalArgumentException("readViewId must identify a known view");
        }
        F4RecordValidation.requireNonNegative(lastAllocatedGeneration, "lastAllocatedGeneration");
        F4RecordValidation.requireNonNegative(allocationSequence, "allocationSequence");
        if (lastAllocatedGeneration != allocationSequence) {
            throw new IllegalArgumentException("generation and allocation sequence must advance together in V1");
        }
        if (allocationSequence == 0) {
            if (!lastPublicationId.isEmpty()) {
                throw new IllegalArgumentException("empty generation sequence cannot carry a publication id");
            }
        } else {
            lastPublicationId = F4RecordValidation.requireBase32Id(lastPublicationId, "lastPublicationId");
        }
        F4RecordValidation.requireNonNegative(updatedAtMillis, "updatedAtMillis");
        F4RecordValidation.requireMetadataVersion(metadataVersion);
    }

    public GenerationSequenceRecord withMetadataVersion(long version) {
        return new GenerationSequenceRecord(
                schemaVersion, streamId, readViewId, lastAllocatedGeneration, allocationSequence,
                lastPublicationId, updatedAtMillis, version);
    }
}
