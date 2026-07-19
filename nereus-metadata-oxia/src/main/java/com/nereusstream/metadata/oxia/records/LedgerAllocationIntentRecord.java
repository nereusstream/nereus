/* Licensed under the Apache License, Version 2.0 */
package com.nereusstream.metadata.oxia.records;

/** Recoverable exact-ledger allocation workflow intent. */
public record LedgerAllocationIntentRecord(
        int schemaVersion, String allocationId, String streamId, long segmentSequence, String clusterAlias,
        long candidateLedgerId, int allocationSlot, String configurationBindingSha256, String writerId,
        String writerRunIdHash, long appendSessionEpoch, String fencingTokenHash, long writerStateEpoch,
        LedgerAllocationLifecycle lifecycle, boolean lateCreateHazard, String bookKeeperMetadataSha256,
        long createdAtMillis, long updatedAtMillis, String stateReason, long metadataVersion) {
    public LedgerAllocationIntentRecord {
        BookKeeperRecordValidation.version(schemaVersion);
        allocationId = BookKeeperRecordValidation.text(allocationId, "allocationId");
        streamId = BookKeeperRecordValidation.text(streamId, "streamId");
        BookKeeperRecordValidation.nonNegative(segmentSequence, "segmentSequence");
        clusterAlias = BookKeeperRecordValidation.text(clusterAlias, "clusterAlias");
        BookKeeperRecordValidation.positive(candidateLedgerId, "candidateLedgerId");
        BookKeeperRecordValidation.nonNegative(allocationSlot, "allocationSlot");
        configurationBindingSha256 = BookKeeperRecordValidation.sha256(configurationBindingSha256, "configurationBindingSha256");
        writerId = BookKeeperRecordValidation.text(writerId, "writerId");
        writerRunIdHash = BookKeeperRecordValidation.sha256(writerRunIdHash, "writerRunIdHash");
        BookKeeperRecordValidation.nonNegative(appendSessionEpoch, "appendSessionEpoch");
        fencingTokenHash = BookKeeperRecordValidation.sha256(fencingTokenHash, "fencingTokenHash");
        BookKeeperRecordValidation.positive(writerStateEpoch, "writerStateEpoch");
        if (lifecycle == null) throw new NullPointerException("lifecycle");
        bookKeeperMetadataSha256 = BookKeeperRecordValidation.optionalSha256(bookKeeperMetadataSha256, "bookKeeperMetadataSha256");
        BookKeeperRecordValidation.times(createdAtMillis, updatedAtMillis);
        stateReason = BookKeeperRecordValidation.optional(stateReason, "stateReason");
        BookKeeperRecordValidation.metadataVersion(metadataVersion);
        boolean createMayHaveBeenTransmitted = lifecycle == LedgerAllocationLifecycle.CREATE_UNCERTAIN
                || lifecycle == LedgerAllocationLifecycle.PHYSICAL_CREATED
                || lifecycle == LedgerAllocationLifecycle.ACTIVATED
                || lifecycle == LedgerAllocationLifecycle.FOREIGN_COLLISION;
        if (lateCreateHazard && !createMayHaveBeenTransmitted) {
            throw new IllegalArgumentException("pre-transmission allocation cannot carry lateCreateHazard");
        }
        if ((lifecycle == LedgerAllocationLifecycle.PHYSICAL_CREATED || lifecycle == LedgerAllocationLifecycle.ACTIVATED)
                && bookKeeperMetadataSha256.isEmpty()) {
            throw new IllegalArgumentException("created allocation requires BookKeeper metadata digest");
        }
    }

    public LedgerAllocationIntentRecord withMetadataVersion(long version) {
        return new LedgerAllocationIntentRecord(schemaVersion, allocationId, streamId, segmentSequence, clusterAlias,
                candidateLedgerId, allocationSlot, configurationBindingSha256, writerId, writerRunIdHash,
                appendSessionEpoch, fencingTokenHash, writerStateEpoch, lifecycle, lateCreateHazard,
                bookKeeperMetadataSha256, createdAtMillis, updatedAtMillis, stateReason, version);
    }
}
