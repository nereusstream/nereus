/* Licensed under the Apache License, Version 2.0 */
package com.nereusstream.metadata.oxia.records;

/** Advisory per-policy materialization progress; never a visibility record. */
public record MaterializationCheckpointRecord(
        int schemaVersion,
        String streamId,
        String policyId,
        long policyVersion,
        String policySha256,
        long contiguousCoveredOffset,
        long observedCommitVersion,
        long lastTaskSequence,
        String lastTaskId,
        long updatedAtMillis,
        long metadataVersion) {
    public MaterializationCheckpointRecord {
        F4RecordValidation.requireSchemaVersion(schemaVersion);
        streamId = F4RecordValidation.requireText(streamId, "streamId");
        policyId = F4RecordValidation.requireText(policyId, "policyId");
        F4RecordValidation.requirePositive(policyVersion, "policyVersion");
        policySha256 = F4RecordValidation.requireSha256(policySha256, "policySha256");
        F4RecordValidation.requireNonNegative(contiguousCoveredOffset, "contiguousCoveredOffset");
        F4RecordValidation.requireNonNegative(observedCommitVersion, "observedCommitVersion");
        F4RecordValidation.requireNonNegative(lastTaskSequence, "lastTaskSequence");
        lastTaskId = F4RecordValidation.requireOptionalText(lastTaskId, "lastTaskId", 256);
        if (lastTaskSequence == 0 != lastTaskId.isEmpty()) {
            throw new IllegalArgumentException("last task sequence and id must be empty together");
        }
        F4RecordValidation.requireNonNegative(updatedAtMillis, "updatedAtMillis");
        F4RecordValidation.requireMetadataVersion(metadataVersion);
    }

    public MaterializationCheckpointRecord withMetadataVersion(long version) {
        return new MaterializationCheckpointRecord(
                schemaVersion, streamId, policyId, policyVersion, policySha256, contiguousCoveredOffset,
                observedCommitVersion, lastTaskSequence, lastTaskId, updatedAtMillis, version);
    }
}
