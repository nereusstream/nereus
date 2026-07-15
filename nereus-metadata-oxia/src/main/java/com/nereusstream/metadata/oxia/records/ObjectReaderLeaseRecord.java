/* Licensed under the Apache License, Version 2.0 */
package com.nereusstream.metadata.oxia.records;

/** One process's durable, time-bounded read admission for a physical object. */
public record ObjectReaderLeaseRecord(
        int schemaVersion,
        String objectKeyHash,
        String processRunId,
        String leaseId,
        long rootLifecycleEpoch,
        long acquiredAtMillis,
        long expiresAtMillis,
        long maximumReadDeadlineMillis,
        long renewalSequence,
        long metadataVersion) {
    public ObjectReaderLeaseRecord {
        F4RecordValidation.requireSchemaVersion(schemaVersion);
        objectKeyHash = F4RecordValidation.requireText(objectKeyHash, "objectKeyHash");
        processRunId = F4RecordValidation.requireBase32Id(processRunId, "processRunId");
        leaseId = F4RecordValidation.requireBase32Id(leaseId, "leaseId");
        F4RecordValidation.requirePositive(rootLifecycleEpoch, "rootLifecycleEpoch");
        F4RecordValidation.requireNonNegative(acquiredAtMillis, "acquiredAtMillis");
        if (expiresAtMillis <= acquiredAtMillis) {
            throw new IllegalArgumentException("reader lease expiry must follow acquisition");
        }
        if (maximumReadDeadlineMillis < acquiredAtMillis || maximumReadDeadlineMillis > expiresAtMillis) {
            throw new IllegalArgumentException("maximum read deadline is outside the lease");
        }
        F4RecordValidation.requireNonNegative(renewalSequence, "renewalSequence");
        F4RecordValidation.requireMetadataVersion(metadataVersion);
    }

    public ObjectReaderLeaseRecord withMetadataVersion(long version) {
        return new ObjectReaderLeaseRecord(
                schemaVersion, objectKeyHash, processRunId, leaseId, rootLifecycleEpoch,
                acquiredAtMillis, expiresAtMillis, maximumReadDeadlineMillis, renewalSequence, version);
    }
}
