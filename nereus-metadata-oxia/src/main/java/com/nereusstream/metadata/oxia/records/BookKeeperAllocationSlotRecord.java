/* Licensed under the Apache License, Version 2.0 */
package com.nereusstream.metadata.oxia.records;

/** One fixed cluster-wide allocation uncertainty slot. */
public record BookKeeperAllocationSlotRecord(
        int schemaVersion, int slot, String allocationId, String streamId, long candidateLedgerId,
        String ledgerIdentitySha256, String configurationBindingSha256, AllocationSlotLifecycle lifecycle,
        long createdAtMillis, long updatedAtMillis, long metadataVersion) {
    public BookKeeperAllocationSlotRecord {
        BookKeeperRecordValidation.version(schemaVersion);
        BookKeeperRecordValidation.nonNegative(slot, "slot");
        allocationId = BookKeeperRecordValidation.text(allocationId, "allocationId");
        streamId = BookKeeperRecordValidation.text(streamId, "streamId");
        BookKeeperRecordValidation.positive(candidateLedgerId, "candidateLedgerId");
        ledgerIdentitySha256 = BookKeeperRecordValidation.sha256(ledgerIdentitySha256, "ledgerIdentitySha256");
        configurationBindingSha256 = BookKeeperRecordValidation.sha256(configurationBindingSha256, "configurationBindingSha256");
        if (lifecycle == null) throw new NullPointerException("lifecycle");
        BookKeeperRecordValidation.times(createdAtMillis, updatedAtMillis);
        BookKeeperRecordValidation.metadataVersion(metadataVersion);
    }

    public BookKeeperAllocationSlotRecord withMetadataVersion(long version) {
        return new BookKeeperAllocationSlotRecord(schemaVersion, slot, allocationId, streamId, candidateLedgerId,
                ledgerIdentitySha256, configurationBindingSha256, lifecycle, createdAtMillis, updatedAtMillis, version);
    }
}
