/* Licensed under the Apache License, Version 2.0 */
package com.nereusstream.bookkeeper;

import com.nereusstream.api.Checksum;
import com.nereusstream.api.ChecksumType;
import java.nio.ByteBuffer;
import java.nio.charset.StandardCharsets;
import java.security.MessageDigest;
import java.security.NoSuchAlgorithmException;
import java.util.HexFormat;
import java.util.Objects;

/** Authoritative durable activation plus the exact key/version/value identity that produced it. */
public record BookKeeperProtocolActivation(
        BookKeeperProtocolActivationValue value,
        long metadataVersion,
        Checksum storedValueSha256,
        String canonicalKey) {
    public BookKeeperProtocolActivation {
        value = Objects.requireNonNull(value, "value");
        if (metadataVersion < 0) {
            throw new IllegalArgumentException("activation metadataVersion must be non-negative");
        }
        storedValueSha256 = sha(storedValueSha256, "storedValueSha256");
        canonicalKey = Objects.requireNonNull(canonicalKey, "canonicalKey");
        BookKeeperProtocolActivationKeys.requireExact(
                canonicalKey,
                value.clusterAlias(),
                value.configurationBindingSha256(),
                value.ledgerIdNamespaceSha256());
    }

    /** Exact NBKA1 record identity used by deletion proofs; any CAS creates a new identity. */
    public Checksum activationRecordSha256() {
        try {
            MessageDigest digest = MessageDigest.getInstance("SHA-256");
            digest.update("NBKA1".getBytes(StandardCharsets.US_ASCII));
            byte[] key = canonicalKey.getBytes(StandardCharsets.UTF_8);
            digest.update(ByteBuffer.allocate(Integer.BYTES).putInt(key.length).array());
            digest.update(key);
            digest.update(ByteBuffer.allocate(Long.BYTES).putLong(metadataVersion).array());
            digest.update(HexFormat.of().parseHex(storedValueSha256.value()));
            return new Checksum(
                    ChecksumType.SHA256,
                    HexFormat.of().formatHex(digest.digest()));
        } catch (NoSuchAlgorithmException failure) {
            throw new IllegalStateException("SHA-256 is unavailable", failure);
        }
    }

    /**
     * Stable NBKAP1 publication identity advertised by brokers.
     *
     * <p>The identity intentionally excludes broker readiness, deletion proofs, metadata version,
     * timestamps, and stored bytes. Those fields may advance after publication is enabled and are
     * revalidated independently before every physical deletion.
     */
    public Checksum publicationActivationSha256() {
        if (!supportsAllPublications()) {
            throw new IllegalStateException(
                    "all BookKeeper primary-WAL publications are not active");
        }
        try {
            MessageDigest digest = MessageDigest.getInstance("SHA-256");
            frame(digest, "NBKAP1");
            frame(digest, canonicalKey);
            frame(digest, Integer.toString(value.protocolVersion()));
            frame(digest, Integer.toString(value.schemaVersion()));
            frame(digest, value.clusterAlias());
            frame(digest, value.providerScopeSha256());
            frame(digest, value.configurationBindingSha256());
            frame(digest, value.ledgerIdNamespaceSha256());
            frame(digest, Integer.toString(value.lifecycle().wireId()));
            frame(digest, Boolean.toString(value.walOnlyPublicationEnabled()));
            frame(digest, Boolean.toString(value.asyncPublicationEnabled()));
            frame(digest, Boolean.toString(value.syncPublicationEnabled()));
            return new Checksum(
                    ChecksumType.SHA256,
                    HexFormat.of().formatHex(digest.digest()));
        } catch (NoSuchAlgorithmException failure) {
            throw new IllegalStateException("SHA-256 is unavailable", failure);
        }
    }

    public boolean supportsAllPublications() {
        return value.lifecycle() == BookKeeperProtocolActivationLifecycle.ACTIVE
                && value.walOnlyPublicationEnabled()
                && value.asyncPublicationEnabled()
                && value.syncPublicationEnabled();
    }

    public BookKeeperProtocolActivationProof deletionProof() {
        if (value.lifecycle() != BookKeeperProtocolActivationLifecycle.ACTIVE
                || !value.ledgerDeletionEnabled()) {
            throw new IllegalStateException("BookKeeper ledger deletion is not active");
        }
        return new BookKeeperProtocolActivationProof(
                value.protocolVersion(),
                value.clusterAlias(),
                value.providerScopeSha256(),
                value.configurationBindingSha256(),
                value.ledgerIdNamespaceSha256(),
                value.brokerReadinessEpoch(),
                checksum(value.brokerReadinessSha256()),
                checksum(value.rootCoverageProofSha256()),
                checksum(value.streamCoverageProofSha256()),
                checksum(value.bookKeeperScopeProofSha256()),
                true,
                metadataVersion,
                activationRecordSha256());
    }

    private static Checksum checksum(String value) {
        return new Checksum(ChecksumType.SHA256, value);
    }

    private static Checksum sha(Checksum value, String name) {
        Checksum exact = Objects.requireNonNull(value, name);
        if (exact.type() != ChecksumType.SHA256) {
            throw new IllegalArgumentException(name + " must use SHA256");
        }
        return exact;
    }

    private static void frame(MessageDigest digest, String value) {
        byte[] bytes = value.getBytes(StandardCharsets.UTF_8);
        digest.update(ByteBuffer.allocate(Integer.BYTES).putInt(bytes.length).array());
        digest.update(bytes);
    }
}
