/* Licensed under the Apache License, Version 2.0 */
package com.nereusstream.materialization.gc;

import com.nereusstream.api.Checksum;
import com.nereusstream.api.ChecksumType;
import com.nereusstream.core.physical.GcReference;
import com.nereusstream.core.physical.GcReferenceQuery;
import com.nereusstream.core.physical.GcReferenceSnapshot;
import com.nereusstream.metadata.oxia.records.ObjectProtectionRecord;
import java.nio.ByteBuffer;
import java.nio.charset.StandardCharsets;
import java.security.MessageDigest;
import java.security.NoSuchAlgorithmException;
import java.util.Comparator;
import java.util.HashMap;
import java.util.HexFormat;
import java.util.List;
import java.util.Map;
import java.util.Objects;

final class GcPlanValidation {
    static final int MAX_REFERENCE_DOMAINS = 32;
    static final Comparator<GcReferenceSnapshot> DOMAIN_ORDER = Comparator
            .comparing(GcReferenceSnapshot::domainId)
            .thenComparingInt(GcReferenceSnapshot::protocolVersion);
    static final Comparator<GcDomainSnapshotProof> DOMAIN_PROOF_ORDER = Comparator
            .comparing(GcDomainSnapshotProof::domainId)
            .thenComparingInt(GcDomainSnapshotProof::protocolVersion);
    static final Comparator<GcPlannedProtectionRemoval> PROTECTION_ORDER = Comparator
            .comparing(value -> value.protection().key());
    static final Comparator<GcPlannedMetadataRemoval> METADATA_ORDER = Comparator
            .comparing(GcPlannedMetadataRemoval::key);

    private GcPlanValidation() {
    }

    static String requireBase32Id(String value, String name) {
        Objects.requireNonNull(value, name);
        if (value.length() < 26 || value.length() > 128) {
            throw new IllegalArgumentException(name + " must encode at least 128 bits and be bounded");
        }
        for (int index = 0; index < value.length(); index++) {
            char character = value.charAt(index);
            if (!((character >= 'a' && character <= 'z')
                    || (character >= '2' && character <= '7'))) {
                throw new IllegalArgumentException(name + " must be lowercase base32 without padding");
            }
        }
        return value;
    }

    static <T> List<T> canonical(
            List<T> values,
            Comparator<T> order,
            int maximum,
            String name) {
        List<T> exact = List.copyOf(Objects.requireNonNull(values, name));
        if (exact.isEmpty() || exact.size() > maximum || exact.stream().anyMatch(Objects::isNull)) {
            throw new IllegalArgumentException(name + " must be non-empty, bounded, and contain no nulls");
        }
        for (int index = 1; index < exact.size(); index++) {
            if (order.compare(exact.get(index - 1), exact.get(index)) >= 0) {
                throw new IllegalArgumentException(name + " must be strictly sorted and unique");
            }
        }
        return exact;
    }

    static <T> List<T> canonicalAllowEmpty(
            List<T> values,
            Comparator<T> order,
            int maximum,
            String name) {
        List<T> exact = List.copyOf(Objects.requireNonNull(values, name));
        if (exact.size() > maximum || exact.stream().anyMatch(Objects::isNull)) {
            throw new IllegalArgumentException(name + " exceeds its bound or contains null");
        }
        for (int index = 1; index < exact.size(); index++) {
            if (order.compare(exact.get(index - 1), exact.get(index)) >= 0) {
                throw new IllegalArgumentException(name + " must be strictly sorted and unique");
            }
        }
        return exact;
    }

    static Checksum referenceSetSha256(
            GcReferenceQuery query,
            List<GcReferenceSnapshot> domainSnapshots,
            List<GcPlannedProtectionRemoval> protections,
            List<GcPlannedMetadataRemoval> metadataRemovals) {
        Objects.requireNonNull(query, "query");
        List<GcDomainSnapshotProof> proofs = domainSnapshots.stream()
                .map(GcDomainSnapshotProof::from)
                .toList();
        return referenceSetSha256(
                query.queryIdentitySha256(), proofs, protections, metadataRemovals);
    }

    static Checksum referenceSetSha256(
            Checksum queryIdentitySha256,
            List<GcDomainSnapshotProof> domainProofs,
            List<GcPlannedProtectionRemoval> protections,
            List<GcPlannedMetadataRemoval> metadataRemovals) {
        Checksum query = GcReferenceQuery.requireSha256(
                queryIdentitySha256, "queryIdentitySha256");
        List<GcDomainSnapshotProof> proofs = canonical(
                domainProofs,
                DOMAIN_PROOF_ORDER,
                MAX_REFERENCE_DOMAINS,
                "domainProofs");
        for (GcDomainSnapshotProof proof : proofs) {
            if (!proof.queryIdentitySha256().equals(query)) {
                throw new IllegalArgumentException(
                        "domain proof belongs to another GC reference query");
            }
        }
        List<GcPlannedProtectionRemoval> exactProtections = canonicalAllowEmpty(
                protections,
                PROTECTION_ORDER,
                PhysicalGcConfig.MAX_DOMAIN_VALUES,
                "protections");
        List<GcPlannedMetadataRemoval> exactMetadataRemovals = canonicalAllowEmpty(
                metadataRemovals,
                METADATA_ORDER,
                PhysicalGcConfig.MAX_DOMAIN_VALUES,
                "metadataRemovals");
        DigestWriter writer = new DigestWriter("nereus-gc-reference-set-v2");
        writer.checksum(query);
        writer.int32(proofs.size());
        for (GcDomainSnapshotProof proof : proofs) {
            writer.text(proof.domainId());
            writer.int32(proof.protocolVersion());
            writer.checksum(proof.queryIdentitySha256());
            writer.checksum(proof.snapshotSha256());
        }
        writer.int32(exactProtections.size());
        for (GcPlannedProtectionRemoval removal : exactProtections) {
            ObjectProtectionRecord protection = removal.protection().value();
            writer.text(removal.protection().key());
            writer.int64(removal.protection().metadataVersion());
            writer.checksum(removal.protection().durableValueSha256());
            writer.text(protection.objectKeyHash());
            writer.int32(protection.protectionTypeId());
            writer.text(protection.referenceId());
            writer.text(protection.ownerKey());
            writer.int64(protection.ownerMetadataVersion());
            writer.text(protection.ownerIdentitySha256());
            writer.int64(protection.rootLifecycleEpoch());
            writer.int64(protection.createdAtMillis());
            writer.int64(protection.expiresAtMillis());
        }
        writer.int32(exactMetadataRemovals.size());
        for (GcPlannedMetadataRemoval removal : exactMetadataRemovals) {
            writer.text(removal.removalType());
            writer.text(removal.key());
            writer.int64(removal.metadataVersion());
            writer.checksum(removal.durableValueSha256());
        }
        return writer.finish();
    }

    static void requireEveryReferenceHasExactRemoval(
            List<GcReferenceSnapshot> snapshots,
            List<GcPlannedMetadataRemoval> metadataRemovals) {
        Map<String, GcPlannedMetadataRemoval> removalsByKey = new HashMap<>();
        for (GcPlannedMetadataRemoval removal : metadataRemovals) {
            GcPlannedMetadataRemoval previous = removalsByKey.put(removal.key(), removal);
            if (previous != null) {
                throw new IllegalArgumentException(
                        "planned metadata removal keys must be unique across removal types");
            }
        }
        for (GcReferenceSnapshot snapshot : snapshots) {
            for (GcReference reference : snapshot.references()) {
                GcPlannedMetadataRemoval removal = removalsByKey.get(reference.ownerKey());
                if (removal == null
                        || !removal.removalType().equals(reference.referenceType())
                        || removal.metadataVersion() != reference.ownerMetadataVersion()
                        || !removal.durableValueSha256().equals(
                                reference.ownerIdentitySha256())) {
                    throw new IllegalArgumentException(
                            "every domain reference must have an exact planned metadata removal");
                }
            }
        }
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
            byte[] bytes = value.getBytes(StandardCharsets.UTF_8);
            int32(bytes.length);
            digest.update(bytes);
        }

        private void checksum(Checksum value) {
            text(value.type().name());
            text(value.value());
        }

        private Checksum finish() {
            return new Checksum(ChecksumType.SHA256, HexFormat.of().formatHex(digest.digest()));
        }
    }
}
