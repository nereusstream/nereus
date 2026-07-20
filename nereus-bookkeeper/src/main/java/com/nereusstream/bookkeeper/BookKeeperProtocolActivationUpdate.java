/* Licensed under the Apache License, Version 2.0 */
package com.nereusstream.bookkeeper;

import java.util.Objects;

/** Operator-reviewed monotonic activation request; zero proof digests mean deletion remains disabled. */
public record BookKeeperProtocolActivationUpdate(
        long brokerReadinessEpoch,
        String brokerReadinessSha256,
        boolean asyncPublicationEnabled,
        boolean syncPublicationEnabled,
        boolean ledgerDeletionEnabled,
        String rootCoverageProofSha256,
        String streamCoverageProofSha256,
        String bookKeeperScopeProofSha256,
        long expectedMetadataVersion) {
    public BookKeeperProtocolActivationUpdate {
        if (brokerReadinessEpoch <= 0 || expectedMetadataVersion < 0) {
            throw new IllegalArgumentException("activation update versions are invalid");
        }
        brokerReadinessSha256 = sha(brokerReadinessSha256, "brokerReadinessSha256");
        rootCoverageProofSha256 = sha(rootCoverageProofSha256, "rootCoverageProofSha256");
        streamCoverageProofSha256 = sha(streamCoverageProofSha256, "streamCoverageProofSha256");
        bookKeeperScopeProofSha256 = sha(
                bookKeeperScopeProofSha256, "bookKeeperScopeProofSha256");
        if (syncPublicationEnabled && !asyncPublicationEnabled) {
            throw new IllegalArgumentException("sync publication requires async publication");
        }
        if (ledgerDeletionEnabled && !syncPublicationEnabled) {
            throw new IllegalArgumentException("ledger deletion requires sync publication");
        }
    }

    public static BookKeeperProtocolActivationUpdate publications(
            long brokerReadinessEpoch,
            String brokerReadinessSha256,
            boolean asyncPublicationEnabled,
            boolean syncPublicationEnabled,
            long expectedMetadataVersion) {
        String zero = BookKeeperProtocolActivationValue.zeroSha256();
        return new BookKeeperProtocolActivationUpdate(
                brokerReadinessEpoch,
                brokerReadinessSha256,
                asyncPublicationEnabled,
                syncPublicationEnabled,
                false,
                zero,
                zero,
                zero,
                expectedMetadataVersion);
    }

    private static String sha(String value, String name) {
        return BookKeeperWalConfiguration.sha256(Objects.requireNonNull(value, name), name);
    }
}
