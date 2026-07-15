/* Licensed under the Apache License, Version 2.0 */
package com.nereusstream.metadata.oxia;

import com.nereusstream.api.Checksum;
import com.nereusstream.api.ChecksumType;
import com.nereusstream.metadata.oxia.records.VirtualLedgerProjectionRecord;
import java.util.Objects;

/** Exact stored per-stream F2 projection binding with its Oxia version and envelope digest. */
public record VersionedVirtualLedgerProjection(
        String key,
        VirtualLedgerProjectionRecord value,
        long metadataVersion,
        Checksum durableValueSha256) {
    public VersionedVirtualLedgerProjection {
        Objects.requireNonNull(key, "key");
        if (key.isBlank()) {
            throw new IllegalArgumentException("key must be non-blank");
        }
        Objects.requireNonNull(value, "value");
        if (value.metadataVersion() != 0 || metadataVersion < 0) {
            throw new IllegalArgumentException(
                    "virtual-ledger projection wire version must be zero and Oxia version non-negative");
        }
        Objects.requireNonNull(durableValueSha256, "durableValueSha256");
        if (durableValueSha256.type() != ChecksumType.SHA256) {
            throw new IllegalArgumentException("durableValueSha256 must use SHA256");
        }
    }
}
