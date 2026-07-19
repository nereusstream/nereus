/* Licensed under the Apache License, Version 2.0 */
package com.nereusstream.bookkeeper;

import com.nereusstream.api.Checksum;
import com.nereusstream.api.ChecksumType;
import java.util.Objects;

/** Reloaded durable rollout proof required in addition to local GC configuration. */
public record BookKeeperProtocolActivationProof(
        int protocolVersion,
        String clusterAlias,
        String providerScopeSha256,
        String configurationBindingSha256,
        String ledgerIdNamespaceSha256,
        long brokerReadinessEpoch,
        Checksum brokerReadinessSha256,
        Checksum rootCoverageProofSha256,
        Checksum streamCoverageProofSha256,
        Checksum bookKeeperScopeProofSha256,
        boolean ledgerDeletionEnabled,
        long activationMetadataVersion,
        Checksum activationRecordSha256) {
    public BookKeeperProtocolActivationProof {
        if (protocolVersion <= 0 || brokerReadinessEpoch <= 0 || activationMetadataVersion < 0) {
            throw new IllegalArgumentException("BookKeeper activation proof versions are invalid");
        }
        clusterAlias = text(clusterAlias, "clusterAlias");
        providerScopeSha256 = sha(providerScopeSha256, "providerScopeSha256");
        configurationBindingSha256 = sha(configurationBindingSha256, "configurationBindingSha256");
        ledgerIdNamespaceSha256 = sha(ledgerIdNamespaceSha256, "ledgerIdNamespaceSha256");
        brokerReadinessSha256 = sha(brokerReadinessSha256, "brokerReadinessSha256");
        rootCoverageProofSha256 = sha(rootCoverageProofSha256, "rootCoverageProofSha256");
        streamCoverageProofSha256 = sha(streamCoverageProofSha256, "streamCoverageProofSha256");
        bookKeeperScopeProofSha256 = sha(bookKeeperScopeProofSha256, "bookKeeperScopeProofSha256");
        activationRecordSha256 = sha(activationRecordSha256, "activationRecordSha256");
    }

    public void requireExact(
            BookKeeperWalConfiguration configuration,
            BookKeeperLedgerIdNamespaceReservation namespace) {
        Objects.requireNonNull(configuration, "configuration");
        Objects.requireNonNull(namespace, "namespace");
        if (protocolVersion != 1
                || !clusterAlias.equals(configuration.clusterAlias())
                || !providerScopeSha256.equals(configuration.providerScopeSha256())
                || !configurationBindingSha256.equals(configuration.configurationBindingSha256().value())
                || !ledgerIdNamespaceSha256.equals(namespace.ledgerIdNamespaceSha256().value())
                || !ledgerDeletionEnabled) {
            throw new IllegalArgumentException(
                    "BookKeeper deletion activation does not match the exact WAL/namespace binding");
        }
    }

    private static Checksum sha(Checksum value, String name) {
        Objects.requireNonNull(value, name);
        if (value.type() != ChecksumType.SHA256) {
            throw new IllegalArgumentException(name + " must use SHA256");
        }
        return value;
    }

    private static String sha(String value, String name) {
        return new Checksum(ChecksumType.SHA256, Objects.requireNonNull(value, name)).value();
    }

    private static String text(String value, String name) {
        Objects.requireNonNull(value, name);
        if (value.isBlank()) throw new IllegalArgumentException(name + " cannot be blank");
        return value;
    }
}
