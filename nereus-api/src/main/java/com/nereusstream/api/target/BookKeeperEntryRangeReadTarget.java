/*
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 *   https://www.apache.org/licenses/LICENSE-2.0
 */
package com.nereusstream.api.target;

import com.nereusstream.api.Checksum;
import com.nereusstream.api.ChecksumType;
import java.nio.charset.StandardCharsets;
import java.util.Objects;

/** Durable reservation for a contiguous BookKeeper entry range. Phase 1.5 installs no BookKeeper IO adapter. */
public record BookKeeperEntryRangeReadTarget(
        int version,
        String clusterAlias,
        long ledgerId,
        long firstEntryId,
        int entryCount,
        BookKeeperEntryMapping entryMapping,
        Checksum rangeChecksum) implements ReadTarget {
    public BookKeeperEntryRangeReadTarget {
        if (version != 1) {
            throw new IllegalArgumentException("BookKeeper target version must be 1");
        }
        Objects.requireNonNull(clusterAlias, "clusterAlias");
        if (clusterAlias.isBlank() || clusterAlias.getBytes(StandardCharsets.UTF_8).length > 16 * 1024) {
            throw new IllegalArgumentException("clusterAlias must be nonblank and bounded");
        }
        if (ledgerId < 0 || firstEntryId < 0 || entryCount <= 0) {
            throw new IllegalArgumentException("BookKeeper range fields are invalid");
        }
        Objects.requireNonNull(entryMapping, "entryMapping");
        Objects.requireNonNull(rangeChecksum, "rangeChecksum");
        if (rangeChecksum.type() != ChecksumType.SHA256) {
            throw new IllegalArgumentException("BookKeeper range checksum must be SHA256");
        }
        lastEntryIdInclusive(firstEntryId, entryCount);
    }

    @Override
    public ReadTargetType type() {
        return ReadTargetType.BOOKKEEPER_ENTRY_RANGE;
    }

    public long lastEntryIdInclusive() {
        return lastEntryIdInclusive(firstEntryId, entryCount);
    }

    private static long lastEntryIdInclusive(long firstEntryId, int entryCount) {
        try {
            return Math.addExact(firstEntryId, (long) entryCount - 1);
        } catch (ArithmeticException e) {
            throw new IllegalArgumentException("BookKeeper entry range overflows", e);
        }
    }
}
