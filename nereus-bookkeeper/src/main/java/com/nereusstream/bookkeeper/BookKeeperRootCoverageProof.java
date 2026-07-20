/* Licensed under the Apache License, Version 2.0 */
package com.nereusstream.bookkeeper;

import com.nereusstream.api.Checksum;
import com.nereusstream.api.ChecksumType;
import java.util.Objects;

/** Complete binding-scoped root/protection/reader traversal under one broker-readiness fact. */
public record BookKeeperRootCoverageProof(
        long brokerReadinessEpoch,
        Checksum brokerSetSha256,
        int shardsScanned,
        long rootsScanned,
        long matchingRoots,
        long protectionsScanned,
        long readerLeasesScanned,
        Checksum coverageSha256) {
    public BookKeeperRootCoverageProof {
        if (brokerReadinessEpoch <= 0 || shardsScanned != BookKeeperLedgerRetentionScanner.ROOT_SHARDS) {
            throw new IllegalArgumentException("root coverage requires positive readiness and every root shard");
        }
        brokerSetSha256 = sha256(brokerSetSha256, "brokerSetSha256");
        coverageSha256 = sha256(coverageSha256, "coverageSha256");
        if (rootsScanned < 0
                || matchingRoots < 0
                || matchingRoots > rootsScanned
                || protectionsScanned < 0
                || readerLeasesScanned < 0) {
            throw new IllegalArgumentException("root coverage counts are invalid");
        }
    }

    private static Checksum sha256(Checksum value, String name) {
        Checksum exact = Objects.requireNonNull(value, name);
        if (exact.type() != ChecksumType.SHA256) {
            throw new IllegalArgumentException(name + " must use SHA256");
        }
        return exact;
    }
}
