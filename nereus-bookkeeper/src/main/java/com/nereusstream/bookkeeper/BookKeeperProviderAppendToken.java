/* Licensed under the Apache License, Version 2.0 */
package com.nereusstream.bookkeeper;

import com.nereusstream.api.Checksum;
import com.nereusstream.api.ChecksumType;
import com.nereusstream.core.wal.ProviderAppendToken;
import java.util.Objects;

/** Process-local bridge back to the exact durable range reservation after provider writes complete. */
public record BookKeeperProviderAppendToken(
        String reservationId,
        long ledgerId,
        long ledgerRootEpoch,
        int ledgerRangeSlot,
        long firstEntryId,
        int entryCount,
        Checksum rangeChecksum,
        long reservationMetadataVersion,
        long writerMetadataVersion,
        long rootMetadataVersion) implements ProviderAppendToken {
    public BookKeeperProviderAppendToken {
        Objects.requireNonNull(reservationId, "reservationId");
        Objects.requireNonNull(rangeChecksum, "rangeChecksum");
        if (reservationId.isBlank() || ledgerId <= 0 || ledgerRootEpoch <= 0 || ledgerRangeSlot < 0
                || firstEntryId < 0 || entryCount <= 0 || rangeChecksum.type() != ChecksumType.SHA256
                || reservationMetadataVersion < 0 || writerMetadataVersion < 0 || rootMetadataVersion < 0) {
            throw new IllegalArgumentException("invalid BookKeeper provider append token");
        }
        Math.addExact(firstEntryId, (long) entryCount - 1);
    }
}
