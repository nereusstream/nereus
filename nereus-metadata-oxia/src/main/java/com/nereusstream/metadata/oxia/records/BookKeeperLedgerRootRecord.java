/* Licensed under the Apache License, Version 2.0 */
package com.nereusstream.metadata.oxia.records;

/** Authoritative ownership/lifecycle root for one exact physical BookKeeper ledger. */
public record BookKeeperLedgerRootRecord(
        int schemaVersion, String ledgerIdentitySha256, String clusterAlias, String providerScopeSha256,
        long ledgerId, String streamId, long segmentSequence, String allocationId, int allocationSlot,
        String configurationBindingSha256, String ledgerIdNamespaceSha256, boolean lateCreateHazard,
        String writerId, String writerRunIdHash, long appendSessionEpoch, String fencingTokenHash,
        int ensembleSize, int writeQuorumSize, int ackQuorumSize, String digestType,
        String customMetadataSha256, BookKeeperLedgerLifecycle lifecycle, long lifecycleEpoch,
        long createdAtMillis, long activatedAtMillis, long sealStartedAtMillis, long sealedAtMillis,
        long sealedLastEntryId, long sealedLength, String sealReason, String gcAttemptId,
        String referenceSetSha256, long markedAtMillis, long deleteNotBeforeMillis, long deleteStartedAtMillis,
        long firstAbsentAtMillis, long deletedAtMillis, String stateReason, long metadataVersion) {
    public BookKeeperLedgerRootRecord {
        BookKeeperRecordValidation.version(schemaVersion);
        ledgerIdentitySha256 = BookKeeperRecordValidation.sha256(ledgerIdentitySha256, "ledgerIdentitySha256");
        clusterAlias = BookKeeperRecordValidation.text(clusterAlias, "clusterAlias");
        providerScopeSha256 = BookKeeperRecordValidation.sha256(providerScopeSha256, "providerScopeSha256");
        BookKeeperRecordValidation.positive(ledgerId, "ledgerId");
        streamId = BookKeeperRecordValidation.text(streamId, "streamId");
        BookKeeperRecordValidation.nonNegative(segmentSequence, "segmentSequence");
        allocationId = BookKeeperRecordValidation.text(allocationId, "allocationId");
        BookKeeperRecordValidation.nonNegative(allocationSlot, "allocationSlot");
        configurationBindingSha256 = BookKeeperRecordValidation.sha256(configurationBindingSha256, "configurationBindingSha256");
        ledgerIdNamespaceSha256 = BookKeeperRecordValidation.sha256(ledgerIdNamespaceSha256, "ledgerIdNamespaceSha256");
        writerId = BookKeeperRecordValidation.text(writerId, "writerId");
        writerRunIdHash = BookKeeperRecordValidation.sha256(writerRunIdHash, "writerRunIdHash");
        BookKeeperRecordValidation.nonNegative(appendSessionEpoch, "appendSessionEpoch");
        fencingTokenHash = BookKeeperRecordValidation.sha256(fencingTokenHash, "fencingTokenHash");
        BookKeeperRecordValidation.positive(ensembleSize, "ensembleSize");
        BookKeeperRecordValidation.positive(writeQuorumSize, "writeQuorumSize");
        BookKeeperRecordValidation.positive(ackQuorumSize, "ackQuorumSize");
        if (ackQuorumSize > writeQuorumSize || writeQuorumSize > ensembleSize) {
            throw new IllegalArgumentException("BookKeeper quorum sizes are inconsistent");
        }
        digestType = BookKeeperRecordValidation.text(digestType, "digestType");
        customMetadataSha256 = BookKeeperRecordValidation.sha256(customMetadataSha256, "customMetadataSha256");
        if (lifecycle == null) throw new NullPointerException("lifecycle");
        BookKeeperRecordValidation.positive(lifecycleEpoch, "lifecycleEpoch");
        BookKeeperRecordValidation.nonNegative(createdAtMillis, "createdAtMillis");
        BookKeeperRecordValidation.nonNegative(activatedAtMillis, "activatedAtMillis");
        BookKeeperRecordValidation.nonNegative(sealStartedAtMillis, "sealStartedAtMillis");
        BookKeeperRecordValidation.nonNegative(sealedAtMillis, "sealedAtMillis");
        if (sealedLastEntryId < -1) throw new IllegalArgumentException("sealedLastEntryId must be at least -1");
        BookKeeperRecordValidation.nonNegative(sealedLength, "sealedLength");
        sealReason = BookKeeperRecordValidation.optional(sealReason, "sealReason");
        gcAttemptId = BookKeeperRecordValidation.optional(gcAttemptId, "gcAttemptId");
        referenceSetSha256 = BookKeeperRecordValidation.optionalSha256(referenceSetSha256, "referenceSetSha256");
        BookKeeperRecordValidation.nonNegative(markedAtMillis, "markedAtMillis");
        BookKeeperRecordValidation.nonNegative(deleteNotBeforeMillis, "deleteNotBeforeMillis");
        BookKeeperRecordValidation.nonNegative(deleteStartedAtMillis, "deleteStartedAtMillis");
        BookKeeperRecordValidation.nonNegative(firstAbsentAtMillis, "firstAbsentAtMillis");
        BookKeeperRecordValidation.nonNegative(deletedAtMillis, "deletedAtMillis");
        stateReason = BookKeeperRecordValidation.optional(stateReason, "stateReason");
        BookKeeperRecordValidation.metadataVersion(metadataVersion);
        boolean sealed = lifecycle == BookKeeperLedgerLifecycle.SEALED
                || lifecycle == BookKeeperLedgerLifecycle.MARKED
                || lifecycle == BookKeeperLedgerLifecycle.DELETING
                || lifecycle == BookKeeperLedgerLifecycle.DELETED;
        if (sealed != (sealedAtMillis > 0 && sealStartedAtMillis > 0 && !sealReason.isEmpty())) {
            throw new IllegalArgumentException("sealed lifecycle must carry exact seal facts");
        }
        boolean marked = lifecycle == BookKeeperLedgerLifecycle.MARKED
                || lifecycle == BookKeeperLedgerLifecycle.DELETING || lifecycle == BookKeeperLedgerLifecycle.DELETED;
        if (marked != (!gcAttemptId.isEmpty() && !referenceSetSha256.isEmpty() && markedAtMillis > 0
                && deleteNotBeforeMillis >= markedAtMillis)) {
            throw new IllegalArgumentException("GC mark facts must match lifecycle");
        }
        if (lateCreateHazard && marked) {
            throw new IllegalArgumentException("lateCreateHazard permanently vetoes physical GC");
        }
        if (lifecycle == BookKeeperLedgerLifecycle.DELETED
                && !(deleteStartedAtMillis > 0 && firstAbsentAtMillis >= deleteStartedAtMillis
                && deletedAtMillis > firstAbsentAtMillis)) {
            throw new IllegalArgumentException("DELETED root requires dual-absence timestamps");
        }
        if ((lifecycle == BookKeeperLedgerLifecycle.QUARANTINED) != !stateReason.isEmpty()) {
            throw new IllegalArgumentException("only QUARANTINED roots carry a state reason");
        }
    }

    public BookKeeperLedgerRootRecord withMetadataVersion(long version) {
        return new BookKeeperLedgerRootRecord(schemaVersion, ledgerIdentitySha256, clusterAlias, providerScopeSha256,
                ledgerId, streamId, segmentSequence, allocationId, allocationSlot, configurationBindingSha256,
                ledgerIdNamespaceSha256, lateCreateHazard, writerId, writerRunIdHash, appendSessionEpoch,
                fencingTokenHash, ensembleSize, writeQuorumSize, ackQuorumSize, digestType, customMetadataSha256,
                lifecycle, lifecycleEpoch, createdAtMillis, activatedAtMillis, sealStartedAtMillis, sealedAtMillis,
                sealedLastEntryId, sealedLength, sealReason, gcAttemptId, referenceSetSha256, markedAtMillis,
                deleteNotBeforeMillis, deleteStartedAtMillis, firstAbsentAtMillis, deletedAtMillis, stateReason, version);
    }
}
