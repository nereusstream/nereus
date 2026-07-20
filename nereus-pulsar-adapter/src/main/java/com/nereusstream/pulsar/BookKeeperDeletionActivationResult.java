/* Licensed under the Apache License, Version 2.0 */
package com.nereusstream.pulsar;

import com.nereusstream.api.Checksum;
import com.nereusstream.api.ChecksumType;
import com.nereusstream.bookkeeper.BookKeeperProtocolActivation;
import java.util.Objects;

/** Exact durable activation and the three producer-owned proof identities installed by its one CAS. */
public record BookKeeperDeletionActivationResult(
        BookKeeperProtocolActivation activation,
        Checksum rootCoverageProofSha256,
        Checksum streamCoverageProofSha256,
        Checksum bookKeeperScopeProofSha256,
        boolean newlyActivated) {
    public BookKeeperDeletionActivationResult {
        activation = Objects.requireNonNull(activation, "activation");
        rootCoverageProofSha256 = sha256(rootCoverageProofSha256, "rootCoverageProofSha256");
        streamCoverageProofSha256 = sha256(streamCoverageProofSha256, "streamCoverageProofSha256");
        bookKeeperScopeProofSha256 = sha256(bookKeeperScopeProofSha256, "bookKeeperScopeProofSha256");
        if (!activation.value().ledgerDeletionEnabled()
                || !activation.value().rootCoverageProofSha256().equals(rootCoverageProofSha256.value())
                || !activation.value().streamCoverageProofSha256().equals(streamCoverageProofSha256.value())
                || !activation.value().bookKeeperScopeProofSha256().equals(bookKeeperScopeProofSha256.value())) {
            throw new IllegalArgumentException("deletion activation result does not match its durable proof fields");
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
