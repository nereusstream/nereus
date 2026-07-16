/* Licensed under the Apache License, Version 2.0 */
package com.nereusstream.materialization.gc;

import com.nereusstream.api.Checksum;
import com.nereusstream.api.ChecksumType;
import com.nereusstream.core.physical.GcReferenceSnapshot;
import com.nereusstream.core.physical.PhysicalObjectIdentity;
import com.nereusstream.metadata.oxia.VersionedPhysicalObjectRoot;
import com.nereusstream.metadata.oxia.records.PhysicalObjectLifecycle;
import java.nio.ByteBuffer;
import java.nio.charset.StandardCharsets;
import java.security.MessageDigest;
import java.security.NoSuchAlgorithmException;
import java.util.HexFormat;
import java.util.List;
import java.util.Objects;

/** Canonical digests for the restart-safe separated tombstone-absence window. */
final class TombstoneRetirementDigests {
    private TombstoneRetirementDigests() {
    }

    static Checksum candidateEvidence(VersionedPhysicalObjectRoot root) {
        Objects.requireNonNull(root, "root");
        if (root.value().lifecycle() != PhysicalObjectLifecycle.DELETED) {
            throw new IllegalArgumentException("tombstone evidence requires a DELETED root");
        }
        DigestWriter writer = new DigestWriter("nereus-deleted-root-evidence-v1");
        writer.checksum(PhysicalObjectIdentity.from(root.value()).identitySha256());
        writer.int64(root.value().lifecycleEpoch());
        writer.text(root.value().gcAttemptId());
        writer.text(root.value().referenceSetSha256());
        writer.int64(root.value().createdAtMillis());
        writer.int64(root.value().orphanNotBeforeMillis());
        writer.int64(root.value().markedAtMillis());
        writer.int64(root.value().deleteNotBeforeMillis());
        writer.int64(root.value().deleteStartedAtMillis());
        writer.int64(root.value().deletedAtMillis());
        return writer.finish();
    }

    static Checksum proof(
            VersionedPhysicalObjectRoot root,
            Checksum queryIdentitySha256,
            List<GcReferenceSnapshot> snapshots) {
        List<GcReferenceSnapshot> exact = GcPlanValidation.canonical(
                snapshots,
                GcPlanValidation.DOMAIN_ORDER,
                GcPlanValidation.MAX_REFERENCE_DOMAINS,
                "tombstoneDomainSnapshots");
        DigestWriter writer = new DigestWriter("nereus-deleted-root-tombstone-proof-v1");
        writer.checksum(candidateEvidence(root));
        writer.checksum(queryIdentitySha256);
        writer.text("reader-leases-absent");
        writer.text("object-protections-absent");
        writer.int32(exact.size());
        for (GcReferenceSnapshot snapshot : exact) {
            if (!snapshot.complete()
                    || snapshot.veto()
                    || snapshot.referenceCount() != 0
                    || !snapshot.queryIdentitySha256().equals(queryIdentitySha256)) {
                throw new IllegalArgumentException(
                        "tombstone proof requires complete clear query-bound domains");
            }
            writer.text(snapshot.domainId());
            writer.int32(snapshot.protocolVersion());
            writer.checksum(snapshot.snapshotSha256());
        }
        return writer.finish();
    }

    private static final class DigestWriter {
        private final MessageDigest digest;

        private DigestWriter(String domain) {
            try {
                digest = MessageDigest.getInstance("SHA-256");
            } catch (NoSuchAlgorithmException failure) {
                throw new IllegalStateException("SHA-256 is unavailable", failure);
            }
            text(domain);
        }

        private void int32(int value) {
            digest.update(ByteBuffer.allocate(Integer.BYTES).putInt(value).array());
        }

        private void int64(long value) {
            digest.update(ByteBuffer.allocate(Long.BYTES).putLong(value).array());
        }

        private void text(String value) {
            byte[] bytes = Objects.requireNonNull(value, "value")
                    .getBytes(StandardCharsets.UTF_8);
            int32(bytes.length);
            digest.update(bytes);
        }

        private void checksum(Checksum value) {
            Objects.requireNonNull(value, "value");
            text(value.type().name());
            text(value.value());
        }

        private Checksum finish() {
            return new Checksum(
                    ChecksumType.SHA256,
                    HexFormat.of().formatHex(digest.digest()));
        }
    }
}
