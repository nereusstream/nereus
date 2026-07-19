/* Licensed under the Apache License, Version 2.0 */
package com.nereusstream.metadata.oxia.records;

/** One durable stream-scoped BookKeeper writer state. */
public record BookKeeperWriterStateRecord(
        int schemaVersion, String streamId, String clusterAlias, String configurationBindingSha256,
        BookKeeperWriterLifecycle lifecycle, long writerStateEpoch, String writerId, String writerRunIdHash,
        long appendSessionEpoch, String fencingTokenHash, long appendSessionLeaseVersion, long nextSegmentSequence,
        String allocationId, long allocationLedgerId, long activeSegmentSequence, long activeLedgerId,
        long activeLedgerRootEpoch, long nextEntryId, long activePhysicalBytes, int activeAppendRangeCount,
        String activeReservationId, long openedAtMillis, long updatedAtMillis, String stateReason,
        long metadataVersion) {
    public BookKeeperWriterStateRecord {
        BookKeeperRecordValidation.version(schemaVersion);
        streamId = BookKeeperRecordValidation.text(streamId, "streamId");
        clusterAlias = BookKeeperRecordValidation.text(clusterAlias, "clusterAlias");
        configurationBindingSha256 = BookKeeperRecordValidation.sha256(configurationBindingSha256, "configurationBindingSha256");
        if (lifecycle == null) throw new NullPointerException("lifecycle");
        BookKeeperRecordValidation.positive(writerStateEpoch, "writerStateEpoch");
        writerId = BookKeeperRecordValidation.text(writerId, "writerId");
        writerRunIdHash = BookKeeperRecordValidation.sha256(writerRunIdHash, "writerRunIdHash");
        BookKeeperRecordValidation.nonNegative(appendSessionEpoch, "appendSessionEpoch");
        fencingTokenHash = BookKeeperRecordValidation.sha256(fencingTokenHash, "fencingTokenHash");
        BookKeeperRecordValidation.nonNegative(appendSessionLeaseVersion, "appendSessionLeaseVersion");
        BookKeeperRecordValidation.nonNegative(nextSegmentSequence, "nextSegmentSequence");
        allocationId = BookKeeperRecordValidation.optional(allocationId, "allocationId");
        activeReservationId = BookKeeperRecordValidation.optional(activeReservationId, "activeReservationId");
        BookKeeperRecordValidation.nonNegative(allocationLedgerId, "allocationLedgerId");
        BookKeeperRecordValidation.nonNegative(activeSegmentSequence, "activeSegmentSequence");
        BookKeeperRecordValidation.nonNegative(activeLedgerId, "activeLedgerId");
        BookKeeperRecordValidation.nonNegative(activeLedgerRootEpoch, "activeLedgerRootEpoch");
        BookKeeperRecordValidation.nonNegative(nextEntryId, "nextEntryId");
        BookKeeperRecordValidation.nonNegative(activePhysicalBytes, "activePhysicalBytes");
        BookKeeperRecordValidation.nonNegative(activeAppendRangeCount, "activeAppendRangeCount");
        BookKeeperRecordValidation.nonNegative(openedAtMillis, "openedAtMillis");
        if (updatedAtMillis < openedAtMillis) throw new IllegalArgumentException("updatedAtMillis cannot precede openedAtMillis");
        stateReason = BookKeeperRecordValidation.optional(stateReason, "stateReason");
        BookKeeperRecordValidation.metadataVersion(metadataVersion);
        boolean allocating = lifecycle == BookKeeperWriterLifecycle.ALLOCATING;
        boolean active = lifecycle == BookKeeperWriterLifecycle.ACTIVE || lifecycle == BookKeeperWriterLifecycle.RECOVERING;
        if (allocating != (!allocationId.isEmpty() && allocationLedgerId > 0)) {
            throw new IllegalArgumentException("allocation identity must match ALLOCATING lifecycle");
        }
        if (active != (activeLedgerId > 0 && activeLedgerRootEpoch > 0)) {
            throw new IllegalArgumentException("active ledger identity must match ACTIVE/RECOVERING lifecycle");
        }
        if (!active && (!activeReservationId.isEmpty() || nextEntryId != 0 || activePhysicalBytes != 0 || activeAppendRangeCount != 0)) {
            throw new IllegalArgumentException("inactive writer cannot carry active ledger counters");
        }
    }

    public BookKeeperWriterStateRecord withMetadataVersion(long version) {
        return new BookKeeperWriterStateRecord(schemaVersion, streamId, clusterAlias, configurationBindingSha256,
                lifecycle, writerStateEpoch, writerId, writerRunIdHash, appendSessionEpoch, fencingTokenHash,
                appendSessionLeaseVersion, nextSegmentSequence, allocationId, allocationLedgerId,
                activeSegmentSequence, activeLedgerId, activeLedgerRootEpoch, nextEntryId, activePhysicalBytes,
                activeAppendRangeCount, activeReservationId, openedAtMillis, updatedAtMillis, stateReason, version);
    }
}
