/* Licensed under the Apache License, Version 2.0 */
package com.nereusstream.metadata.oxia.records;

/** Fixed-slot, process-owned lease that fences whole-ledger deletion during reads. */
public record BookKeeperLedgerReaderLeaseRecord(
        int schemaVersion, String ledgerIdentitySha256, long ledgerId, long rootLifecycleEpoch,
        int readerSlot, String processRunId, long leaseEpoch, long acquiredAtMillis,
        long expiresAtMillis, long metadataVersion) {
    public BookKeeperLedgerReaderLeaseRecord {
        BookKeeperRecordValidation.version(schemaVersion);
        ledgerIdentitySha256 = BookKeeperRecordValidation.sha256(ledgerIdentitySha256, "ledgerIdentitySha256");
        BookKeeperRecordValidation.positive(ledgerId, "ledgerId");
        BookKeeperRecordValidation.positive(rootLifecycleEpoch, "rootLifecycleEpoch");
        BookKeeperRecordValidation.nonNegative(readerSlot, "readerSlot");
        processRunId = BookKeeperRecordValidation.text(processRunId, "processRunId");
        BookKeeperRecordValidation.positive(leaseEpoch, "leaseEpoch");
        BookKeeperRecordValidation.nonNegative(acquiredAtMillis, "acquiredAtMillis");
        if (expiresAtMillis <= acquiredAtMillis) throw new IllegalArgumentException("reader lease must expire after acquisition");
        BookKeeperRecordValidation.metadataVersion(metadataVersion);
    }

    public BookKeeperLedgerReaderLeaseRecord withMetadataVersion(long version) {
        return new BookKeeperLedgerReaderLeaseRecord(schemaVersion, ledgerIdentitySha256, ledgerId,
                rootLifecycleEpoch, readerSlot, processRunId, leaseEpoch, acquiredAtMillis, expiresAtMillis, version);
    }
}
