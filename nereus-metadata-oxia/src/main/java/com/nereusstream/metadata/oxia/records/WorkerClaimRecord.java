/* Licensed under the Apache License, Version 2.0 */
package com.nereusstream.metadata.oxia.records;

/** Time-bounded exclusive worker claim embedded in a task root. */
public record WorkerClaimRecord(
        String claimId,
        String processRunId,
        long attempt,
        long claimedAtMillis,
        long expiresAtMillis) {
    public WorkerClaimRecord {
        claimId = F4RecordValidation.requireBase32Id(claimId, "claimId");
        processRunId = F4RecordValidation.requireBase32Id(processRunId, "processRunId");
        F4RecordValidation.requirePositive(attempt, "attempt");
        F4RecordValidation.requireNonNegative(claimedAtMillis, "claimedAtMillis");
        if (expiresAtMillis <= claimedAtMillis) {
            throw new IllegalArgumentException("worker claim expiry must follow acquisition");
        }
    }
}
