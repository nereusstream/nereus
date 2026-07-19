/* Licensed under the Apache License, Version 2.0 */
package com.nereusstream.core.capability;

import com.nereusstream.api.Checksum;
import com.nereusstream.api.ChecksumType;
import com.nereusstream.core.physical.GcReferenceQuery;
import java.nio.ByteBuffer;
import java.nio.charset.StandardCharsets;
import java.security.MessageDigest;
import java.security.NoSuchAlgorithmException;
import java.util.HexFormat;
import java.util.Objects;

/** Short-lived proof that must be revalidated immediately before an F4 mutation CAS. */
public record GenerationActivationProof(
        GenerationOperation operation,
        GenerationActivationSubject subject,
        Checksum subjectSha256,
        long subjectValidationVersion,
        long clusterActivationMetadataVersion,
        long brokerCapabilityReadinessEpoch,
        Checksum referenceDomainSetSha256,
        boolean publicationEnabled,
        boolean deletionEnabled,
        long provedAtMillis) {
    public GenerationActivationProof {
        Objects.requireNonNull(operation, "operation");
        Objects.requireNonNull(subject, "subject");
        subjectSha256 = GcReferenceQuery.requireSha256(subjectSha256, "subjectSha256");
        if (!subjectSha256.equals(subjectSha256(subject))) {
            throw new IllegalArgumentException("subjectSha256 does not match canonical subject fields");
        }
        if (subjectValidationVersion < 0
                || clusterActivationMetadataVersion < 0
                || brokerCapabilityReadinessEpoch < 0
                || provedAtMillis < 0) {
            throw new IllegalArgumentException("activation proof versions/times must be non-negative");
        }
        referenceDomainSetSha256 = GcReferenceQuery.requireSha256(
                referenceDomainSetSha256, "referenceDomainSetSha256");
        if (subject instanceof DomainValidatedDeletionSubject) {
            if (operation != GenerationOperation.PHYSICAL_DELETE || subjectValidationVersion != 0) {
                throw new IllegalArgumentException("domain-validated subjects are only legal for physical delete");
            }
        } else if (operation == GenerationOperation.PHYSICAL_DELETE) {
            throw new IllegalArgumentException("physical delete requires a domain-validated subject");
        }
        boolean requiresDeletion =
                operation == GenerationOperation.PHYSICAL_DELETE;
        if ((requiresDeletion && !deletionEnabled) || (!requiresDeletion && !publicationEnabled)) {
            throw new IllegalArgumentException("operation-specific F4 capability is not enabled");
        }
    }

    public static GenerationActivationProof create(
            GenerationOperation operation,
            GenerationActivationSubject subject,
            long subjectValidationVersion,
            long clusterActivationMetadataVersion,
            long brokerCapabilityReadinessEpoch,
            Checksum referenceDomainSetSha256,
            boolean publicationEnabled,
            boolean deletionEnabled,
            long provedAtMillis) {
        return new GenerationActivationProof(
                operation,
                subject,
                subjectSha256(subject),
                subjectValidationVersion,
                clusterActivationMetadataVersion,
                brokerCapabilityReadinessEpoch,
                referenceDomainSetSha256,
                publicationEnabled,
                deletionEnabled,
                provedAtMillis);
    }

    public static Checksum subjectSha256(GenerationActivationSubject subject) {
        Objects.requireNonNull(subject, "subject");
        Digest digest = new Digest();
        if (subject instanceof LiveProjectionSubject live) {
            digest.text("nereus-generation-live-subject-v1");
            digest.text(live.streamId().value());
            digest.text(live.projectionRef().type().name());
            digest.text(live.projectionRef().value());
            digest.text(live.projectionIdentitySha256().value());
        } else if (subject instanceof DomainValidatedDeletionSubject deletion) {
            digest.text("nereus-generation-delete-subject-v1");
            digest.text(deletion.referenceQuery().queryIdentitySha256().value());
            digest.text(deletion.projectionDomainSnapshotSha256().value());
        } else {
            throw new IllegalArgumentException("unknown generation activation subject");
        }
        return digest.finish();
    }

    private static final class Digest {
        private final MessageDigest digest;

        private Digest() {
            try {
                digest = MessageDigest.getInstance("SHA-256");
            } catch (NoSuchAlgorithmException failure) {
                throw new IllegalStateException("SHA-256 is unavailable", failure);
            }
        }

        private void text(String value) {
            byte[] bytes = value.getBytes(StandardCharsets.UTF_8);
            digest.update(ByteBuffer.allocate(Integer.BYTES).putInt(bytes.length).array());
            digest.update(bytes);
        }

        private Checksum finish() {
            return new Checksum(ChecksumType.SHA256, HexFormat.of().formatHex(digest.digest()));
        }
    }
}
