/* Licensed under the Apache License, Version 2.0 */
package com.nereusstream.bookkeeper;

import java.util.Objects;

/** Stored NBKA1 activation value; Oxia key/version/value digest are attached after an authoritative read. */
public record BookKeeperProtocolActivationValue(
        int schemaVersion,
        BookKeeperProtocolActivationLifecycle lifecycle,
        int protocolVersion,
        String clusterAlias,
        String providerScopeSha256,
        long brokerReadinessEpoch,
        String brokerReadinessSha256,
        String configurationBindingSha256,
        String ledgerIdNamespaceSha256,
        boolean walOnlyPublicationEnabled,
        boolean asyncPublicationEnabled,
        boolean syncPublicationEnabled,
        boolean ledgerDeletionEnabled,
        String rootCoverageProofSha256,
        String streamCoverageProofSha256,
        String bookKeeperScopeProofSha256,
        long activatedAtMillis) {
    private static final String ZERO_SHA256 = "0".repeat(64);

    public BookKeeperProtocolActivationValue {
        if (schemaVersion != 1 || protocolVersion != 1) {
            throw new IllegalArgumentException("BookKeeper activation schema/protocol must be 1");
        }
        Objects.requireNonNull(lifecycle, "lifecycle");
        clusterAlias = text(clusterAlias, "clusterAlias");
        providerScopeSha256 = sha(providerScopeSha256, "providerScopeSha256");
        brokerReadinessSha256 = sha(brokerReadinessSha256, "brokerReadinessSha256");
        configurationBindingSha256 = sha(
                configurationBindingSha256, "configurationBindingSha256");
        ledgerIdNamespaceSha256 = sha(
                ledgerIdNamespaceSha256, "ledgerIdNamespaceSha256");
        rootCoverageProofSha256 = sha(
                rootCoverageProofSha256, "rootCoverageProofSha256");
        streamCoverageProofSha256 = sha(
                streamCoverageProofSha256, "streamCoverageProofSha256");
        bookKeeperScopeProofSha256 = sha(
                bookKeeperScopeProofSha256, "bookKeeperScopeProofSha256");
        if (brokerReadinessEpoch <= 0 || activatedAtMillis < 0) {
            throw new IllegalArgumentException("BookKeeper activation epochs/timestamps are invalid");
        }
        if (asyncPublicationEnabled && !walOnlyPublicationEnabled) {
            throw new IllegalArgumentException("async publication requires WAL_ONLY publication");
        }
        if (syncPublicationEnabled && !asyncPublicationEnabled) {
            throw new IllegalArgumentException("sync publication requires async publication");
        }
        if (ledgerDeletionEnabled && !syncPublicationEnabled) {
            throw new IllegalArgumentException("ledger deletion requires every publication profile");
        }
        if (lifecycle == BookKeeperProtocolActivationLifecycle.PREPARED
                && (walOnlyPublicationEnabled
                        || asyncPublicationEnabled
                        || syncPublicationEnabled
                        || ledgerDeletionEnabled
                        || activatedAtMillis != 0
                        || !proofsAreZero(
                                rootCoverageProofSha256,
                                streamCoverageProofSha256,
                                bookKeeperScopeProofSha256))) {
            throw new IllegalArgumentException("PREPARED activation cannot publish capabilities");
        }
        if (lifecycle == BookKeeperProtocolActivationLifecycle.ACTIVE
                && (!walOnlyPublicationEnabled || activatedAtMillis == 0)) {
            throw new IllegalArgumentException("ACTIVE activation must publish WAL_ONLY");
        }
        if (ledgerDeletionEnabled
                && (ZERO_SHA256.equals(rootCoverageProofSha256)
                        || ZERO_SHA256.equals(streamCoverageProofSha256)
                        || ZERO_SHA256.equals(bookKeeperScopeProofSha256))) {
            throw new IllegalArgumentException("ledger deletion requires nonzero coverage proofs");
        }
        if (!ledgerDeletionEnabled
                && !proofsAreZero(
                        rootCoverageProofSha256,
                        streamCoverageProofSha256,
                        bookKeeperScopeProofSha256)) {
            throw new IllegalArgumentException("coverage proofs cannot be published before ledger deletion");
        }
    }

    public static BookKeeperProtocolActivationValue prepared(
            BookKeeperWalConfiguration configuration,
            BookKeeperLedgerIdNamespaceReservation namespace,
            long brokerReadinessEpoch,
            String brokerReadinessSha256) {
        Objects.requireNonNull(configuration, "configuration");
        Objects.requireNonNull(namespace, "namespace");
        return new BookKeeperProtocolActivationValue(
                1,
                BookKeeperProtocolActivationLifecycle.PREPARED,
                1,
                configuration.clusterAlias(),
                configuration.providerScopeSha256(),
                brokerReadinessEpoch,
                brokerReadinessSha256,
                configuration.configurationBindingSha256().value(),
                namespace.ledgerIdNamespaceSha256().value(),
                false,
                false,
                false,
                false,
                ZERO_SHA256,
                ZERO_SHA256,
                ZERO_SHA256,
                0);
    }

    public BookKeeperProtocolActivation materialize(
            String canonicalKey,
            long metadataVersion,
            com.nereusstream.api.Checksum storedValueSha256) {
        return new BookKeeperProtocolActivation(
                this,
                metadataVersion,
                storedValueSha256,
                canonicalKey);
    }

    static void requireValidReplacement(
            BookKeeperProtocolActivationValue current,
            BookKeeperProtocolActivationValue replacement) {
        Objects.requireNonNull(current, "current");
        Objects.requireNonNull(replacement, "replacement");
        if (current.schemaVersion != replacement.schemaVersion
                || current.protocolVersion != replacement.protocolVersion
                || !current.clusterAlias.equals(replacement.clusterAlias)
                || !current.providerScopeSha256.equals(replacement.providerScopeSha256)
                || !current.configurationBindingSha256.equals(
                        replacement.configurationBindingSha256)
                || !current.ledgerIdNamespaceSha256.equals(
                        replacement.ledgerIdNamespaceSha256)) {
            throw new IllegalArgumentException("BookKeeper activation identity is immutable");
        }
        boolean readinessEpochChanged = replacement.brokerReadinessEpoch
                != current.brokerReadinessEpoch;
        boolean readinessDigestChanged = !replacement.brokerReadinessSha256.equals(
                current.brokerReadinessSha256);
        if (readinessEpochChanged != readinessDigestChanged) {
            throw new IllegalArgumentException(
                    "BookKeeper readiness epoch and digest must change atomically");
        }
        if ((current.walOnlyPublicationEnabled && !replacement.walOnlyPublicationEnabled)
                || (current.asyncPublicationEnabled && !replacement.asyncPublicationEnabled)
                || (current.syncPublicationEnabled && !replacement.syncPublicationEnabled)
                || (current.ledgerDeletionEnabled && !replacement.ledgerDeletionEnabled)) {
            throw new IllegalArgumentException("BookKeeper activation bits are monotonic");
        }
        if (current.lifecycle == BookKeeperProtocolActivationLifecycle.ACTIVE
                && replacement.lifecycle != BookKeeperProtocolActivationLifecycle.ACTIVE) {
            throw new IllegalArgumentException("BookKeeper activation lifecycle cannot regress");
        }
        boolean deletionProofChanged = !current.rootCoverageProofSha256.equals(
                        replacement.rootCoverageProofSha256)
                || !current.streamCoverageProofSha256.equals(
                        replacement.streamCoverageProofSha256)
                || !current.bookKeeperScopeProofSha256.equals(
                        replacement.bookKeeperScopeProofSha256);
        if (current.ledgerDeletionEnabled
                && replacement.ledgerDeletionEnabled
                && !readinessEpochChanged
                && deletionProofChanged) {
            throw new IllegalArgumentException(
                    "BookKeeper deletion proofs require a new readiness identity");
        }
        if (replacement.activatedAtMillis < current.activatedAtMillis) {
            throw new IllegalArgumentException("BookKeeper activation timestamp cannot regress");
        }
    }

    public static String zeroSha256() {
        return ZERO_SHA256;
    }

    private static boolean proofsAreZero(String root, String stream, String scope) {
        return ZERO_SHA256.equals(root)
                && ZERO_SHA256.equals(stream)
                && ZERO_SHA256.equals(scope);
    }

    private static String sha(String value, String name) {
        return BookKeeperWalConfiguration.sha256(value, name);
    }

    private static String text(String value, String name) {
        String exact = Objects.requireNonNull(value, name);
        if (exact.isBlank()) {
            throw new IllegalArgumentException(name + " cannot be blank");
        }
        return exact;
    }
}
