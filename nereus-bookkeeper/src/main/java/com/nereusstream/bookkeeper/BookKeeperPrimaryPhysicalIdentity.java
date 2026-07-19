/* Licensed under the Apache License, Version 2.0 */
package com.nereusstream.bookkeeper;

import com.nereusstream.api.Checksum;
import com.nereusstream.api.ChecksumType;
import com.nereusstream.api.target.ReadTargetType;
import com.nereusstream.core.wal.PrimaryPhysicalIdentity;
import java.nio.ByteBuffer;
import java.nio.charset.StandardCharsets;
import java.util.Objects;

/** Exact BookKeeper ledger/root/range identity carried by a durable provider append. */
public record BookKeeperPrimaryPhysicalIdentity(
        String clusterAlias,
        long ledgerId,
        long ledgerRootEpoch,
        long firstEntryId,
        int entryCount,
        Checksum rangeChecksum) implements PrimaryPhysicalIdentity {
    public BookKeeperPrimaryPhysicalIdentity {
        Objects.requireNonNull(clusterAlias, "clusterAlias");
        Objects.requireNonNull(rangeChecksum, "rangeChecksum");
        if (clusterAlias.isBlank() || ledgerId < 0 || ledgerRootEpoch <= 0 || firstEntryId < 0
                || entryCount <= 0 || rangeChecksum.type() != ChecksumType.SHA256) {
            throw new IllegalArgumentException("invalid BookKeeper primary physical identity");
        }
        Math.addExact(firstEntryId, (long) entryCount - 1);
    }
    @Override public ReadTargetType targetType() { return ReadTargetType.BOOKKEEPER_ENTRY_RANGE; }
    @Override public byte[] canonicalIdentity() {
        byte[] alias = clusterAlias.getBytes(StandardCharsets.UTF_8);
        byte[] checksum = rangeChecksum.value().getBytes(StandardCharsets.US_ASCII);
        return ByteBuffer.allocate(Integer.BYTES + alias.length + Long.BYTES * 3 + Integer.BYTES + checksum.length)
                .putInt(alias.length).put(alias).putLong(ledgerId).putLong(ledgerRootEpoch).putLong(firstEntryId)
                .putInt(entryCount).put(checksum).array();
    }
}
