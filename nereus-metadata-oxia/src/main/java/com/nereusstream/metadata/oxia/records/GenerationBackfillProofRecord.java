/* Licensed under the Apache License, Version 2.0 */
package com.nereusstream.metadata.oxia.records;

/** Exact durable coverage proof for one generation-protocol activation backfill. */
public record GenerationBackfillProofRecord(
        String runId,
        long brokerReadinessEpoch,
        String coverageSha256,
        boolean complete,
        long completedAtMillis) {
    public GenerationBackfillProofRecord {
        F4RecordValidation.requireNonNegative(
                brokerReadinessEpoch, "brokerReadinessEpoch");
        if (complete) {
            runId = F4RecordValidation.requireBase32Id(runId, "runId");
            coverageSha256 = F4RecordValidation.requireSha256(
                    coverageSha256, "coverageSha256");
            F4RecordValidation.requirePositive(
                    completedAtMillis, "completedAtMillis");
        } else {
            runId = F4RecordValidation.requireOptionalText(
                    runId, "runId", 128);
            coverageSha256 = F4RecordValidation.requireOptionalText(
                    coverageSha256, "coverageSha256", 64);
            if (!runId.isEmpty()
                    || !coverageSha256.isEmpty()
                    || completedAtMillis != 0) {
                throw new IllegalArgumentException(
                        "incomplete backfill proof cannot carry completion fields");
            }
        }
    }

    public static GenerationBackfillProofRecord incomplete(long brokerReadinessEpoch) {
        return new GenerationBackfillProofRecord(
                "", brokerReadinessEpoch, "", false, 0);
    }
}
