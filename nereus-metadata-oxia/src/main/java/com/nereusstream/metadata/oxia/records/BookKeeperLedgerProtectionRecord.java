/* Licensed under the Apache License, Version 2.0 */
package com.nereusstream.metadata.oxia.records;

/** Exact fixed-slot physical reference and complete ledger-to-range inventory row. */
public record BookKeeperLedgerProtectionRecord(
        int schemaVersion, String ledgerIdentitySha256, String clusterAlias, long ledgerId,
        long rootLifecycleEpoch, int ledgerRangeSlot, int protectionSlot, int protectionTypeId,
        String referenceId, long firstEntryId, int entryCount, String rangeChecksumSha256,
        String streamId, long offsetStart, long offsetEnd, long commitVersion, String ownerKey,
        long ownerMetadataVersion, String ownerIdentitySha256, ProtectionLifecycle lifecycle,
        long createdAtMillis, long expiresAtMillis, long metadataVersion) {
    public BookKeeperLedgerProtectionRecord {
        BookKeeperRecordValidation.version(schemaVersion);
        ledgerIdentitySha256 = BookKeeperRecordValidation.sha256(ledgerIdentitySha256, "ledgerIdentitySha256");
        clusterAlias = BookKeeperRecordValidation.text(clusterAlias, "clusterAlias");
        BookKeeperRecordValidation.positive(ledgerId, "ledgerId");
        BookKeeperRecordValidation.positive(rootLifecycleEpoch, "rootLifecycleEpoch");
        BookKeeperRecordValidation.nonNegative(ledgerRangeSlot, "ledgerRangeSlot");
        BookKeeperRecordValidation.nonNegative(protectionSlot, "protectionSlot");
        BookKeeperProtectionType type = BookKeeperProtectionType.fromWireId(protectionTypeId);
        referenceId = BookKeeperRecordValidation.text(referenceId, "referenceId");
        BookKeeperRecordValidation.nonNegative(firstEntryId, "firstEntryId");
        BookKeeperRecordValidation.positive(entryCount, "entryCount");
        Math.addExact(firstEntryId, entryCount);
        rangeChecksumSha256 = BookKeeperRecordValidation.sha256(rangeChecksumSha256, "rangeChecksumSha256");
        streamId = BookKeeperRecordValidation.text(streamId, "streamId");
        BookKeeperRecordValidation.nonNegative(offsetStart, "offsetStart");
        if (offsetEnd <= offsetStart) throw new IllegalArgumentException("offset range must be non-empty");
        BookKeeperRecordValidation.nonNegative(commitVersion, "commitVersion");
        ownerKey = BookKeeperRecordValidation.optional(ownerKey, "ownerKey");
        BookKeeperRecordValidation.nonNegative(ownerMetadataVersion, "ownerMetadataVersion");
        ownerIdentitySha256 = BookKeeperRecordValidation.optionalSha256(ownerIdentitySha256, "ownerIdentitySha256");
        if (lifecycle == null) throw new NullPointerException("lifecycle");
        BookKeeperRecordValidation.nonNegative(createdAtMillis, "createdAtMillis");
        BookKeeperRecordValidation.nonNegative(expiresAtMillis, "expiresAtMillis");
        BookKeeperRecordValidation.metadataVersion(metadataVersion);
        boolean owner = !ownerKey.isEmpty() && !ownerIdentitySha256.isEmpty();
        if (ownerKey.isEmpty() != ownerIdentitySha256.isEmpty()) {
            throw new IllegalArgumentException("protection owner facts must be fully absent or fully present");
        }
        if ((lifecycle != ProtectionLifecycle.RESERVED) != owner) {
            throw new IllegalArgumentException("ACTIVE/RETIRED protection requires exact owner facts");
        }
        if (type == BookKeeperProtectionType.REPAIR) {
            if (expiresAtMillis <= createdAtMillis) throw new IllegalArgumentException("REPAIR protection must expire");
        } else if (expiresAtMillis != 0) {
            throw new IllegalArgumentException("permanent protection cannot expire");
        }
    }

    public BookKeeperProtectionType protectionType() {
        return BookKeeperProtectionType.fromWireId(protectionTypeId);
    }

    public BookKeeperLedgerProtectionRecord withMetadataVersion(long version) {
        return new BookKeeperLedgerProtectionRecord(schemaVersion, ledgerIdentitySha256, clusterAlias, ledgerId,
                rootLifecycleEpoch, ledgerRangeSlot, protectionSlot, protectionTypeId, referenceId, firstEntryId,
                entryCount, rangeChecksumSha256, streamId, offsetStart, offsetEnd, commitVersion, ownerKey,
                ownerMetadataVersion, ownerIdentitySha256, lifecycle, createdAtMillis, expiresAtMillis, version);
    }
}
