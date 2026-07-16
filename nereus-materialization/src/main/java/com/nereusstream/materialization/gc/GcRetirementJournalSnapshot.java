/* Licensed under the Apache License, Version 2.0 */
package com.nereusstream.materialization.gc;

import com.nereusstream.api.Checksum;
import com.nereusstream.api.ChecksumType;
import com.nereusstream.api.ObjectKeyHash;
import com.nereusstream.metadata.oxia.VersionedGcRetirementManifest;
import com.nereusstream.metadata.oxia.VersionedGcRetirementProtection;
import com.nereusstream.metadata.oxia.VersionedGcRetirementRemoval;
import com.nereusstream.metadata.oxia.VersionedObjectProtection;
import com.nereusstream.metadata.oxia.records.GcDomainSnapshotProofRecord;
import com.nereusstream.metadata.oxia.records.GcRetirementManifestRecord;
import com.nereusstream.metadata.oxia.records.GcRetirementProtectionRecord;
import com.nereusstream.metadata.oxia.records.GcRetirementRemovalRecord;
import java.util.List;
import java.util.Objects;

/** Fully scanned and digest-verified view of one sealed GC retirement journal. */
public final class GcRetirementJournalSnapshot {
    private final VersionedGcRetirementManifest manifest;
    private final List<VersionedGcRetirementProtection> protectionEntries;
    private final List<VersionedGcRetirementRemoval> removalEntries;
    private final List<GcDomainSnapshotProof> domainProofs;
    private final List<GcPlannedProtectionRemoval> plannedProtectionRemovals;
    private final List<GcPlannedMetadataRemoval> plannedMetadataRemovals;
    private final Checksum referenceSetSha256;

    public GcRetirementJournalSnapshot(
            VersionedGcRetirementManifest manifest,
            List<VersionedGcRetirementProtection> protectionEntries,
            List<VersionedGcRetirementRemoval> removalEntries) {
        this.manifest = Objects.requireNonNull(manifest, "manifest");
        this.protectionEntries = List.copyOf(Objects.requireNonNull(
                protectionEntries, "protectionEntries"));
        this.removalEntries = List.copyOf(Objects.requireNonNull(
                removalEntries, "removalEntries"));
        GcRetirementManifestRecord manifestValue = manifest.value();
        ObjectKeyHash object = new ObjectKeyHash(manifestValue.objectKeyHash());
        String attempt = manifestValue.gcAttemptId();
        if (this.protectionEntries.size() != manifestValue.protectionCount()
                || this.removalEntries.size() != manifestValue.metadataRemovalCount()) {
            throw new IllegalArgumentException(
                    "GC retirement journal entry counts do not match its manifest");
        }
        this.protectionEntries.forEach(entry -> requireJournalIdentity(
                object, attempt, entry.value().objectKeyHash(), entry.value().gcAttemptId()));
        this.removalEntries.forEach(entry -> requireJournalIdentity(
                object, attempt, entry.value().objectKeyHash(), entry.value().gcAttemptId()));
        this.domainProofs = GcPlanValidation.canonical(
                manifestValue.domainProofs().stream()
                        .map(GcRetirementJournalSnapshot::domainProof)
                        .toList(),
                GcPlanValidation.DOMAIN_PROOF_ORDER,
                GcPlanValidation.MAX_REFERENCE_DOMAINS,
                "domainProofs");
        this.plannedProtectionRemovals = GcPlanValidation.canonicalAllowEmpty(
                this.protectionEntries.stream()
                        .map(GcRetirementJournalSnapshot::plannedProtection)
                        .sorted(GcPlanValidation.PROTECTION_ORDER)
                        .toList(),
                GcPlanValidation.PROTECTION_ORDER,
                GcRetirementManifestRecord.MAX_PLAN_ENTRIES,
                "plannedProtectionRemovals");
        this.plannedMetadataRemovals = GcPlanValidation.canonicalAllowEmpty(
                this.removalEntries.stream()
                        .map(GcRetirementJournalSnapshot::plannedRemoval)
                        .sorted(GcPlanValidation.METADATA_ORDER)
                        .toList(),
                GcPlanValidation.METADATA_ORDER,
                GcRetirementManifestRecord.MAX_PLAN_ENTRIES,
                "plannedMetadataRemovals");
        Checksum query = sha256(manifestValue.queryIdentitySha256());
        this.referenceSetSha256 = GcPlanValidation.referenceSetSha256(
                query,
                domainProofs,
                plannedProtectionRemovals,
                plannedMetadataRemovals);
        if (!referenceSetSha256.value().equals(manifestValue.referenceSetSha256())) {
            throw new IllegalArgumentException(
                    "GC retirement journal digest does not match its manifest");
        }
    }

    public VersionedGcRetirementManifest manifest() {
        return manifest;
    }

    public List<VersionedGcRetirementProtection> protectionEntries() {
        return protectionEntries;
    }

    public List<VersionedGcRetirementRemoval> removalEntries() {
        return removalEntries;
    }

    public ObjectKeyHash object() {
        return new ObjectKeyHash(manifest.value().objectKeyHash());
    }

    public String gcAttemptId() {
        return manifest.value().gcAttemptId();
    }

    public Checksum queryIdentitySha256() {
        return sha256(manifest.value().queryIdentitySha256());
    }

    public List<GcDomainSnapshotProof> domainProofs() {
        return domainProofs;
    }

    public List<GcPlannedProtectionRemoval> plannedProtectionRemovals() {
        return plannedProtectionRemovals;
    }

    public List<GcPlannedMetadataRemoval> plannedMetadataRemovals() {
        return plannedMetadataRemovals;
    }

    public Checksum referenceSetSha256() {
        return referenceSetSha256;
    }

    static GcDomainSnapshotProof domainProof(GcDomainSnapshotProofRecord value) {
        return new GcDomainSnapshotProof(
                value.domainId(),
                value.protocolVersion(),
                sha256(value.queryIdentitySha256()),
                sha256(value.snapshotSha256()));
    }

    static GcPlannedProtectionRemoval plannedProtection(
            VersionedGcRetirementProtection entry) {
        GcRetirementProtectionRecord value = entry.value();
        VersionedObjectProtection source = new VersionedObjectProtection(
                value.protectionKey(),
                value.protection(),
                value.protectionMetadataVersion(),
                sha256(value.protectionDurableValueSha256()));
        return new GcPlannedProtectionRemoval(source);
    }

    static GcPlannedMetadataRemoval plannedRemoval(
            VersionedGcRetirementRemoval entry) {
        GcRetirementRemovalRecord value = entry.value();
        return new GcPlannedMetadataRemoval(
                value.removalType(),
                value.removalKey(),
                value.removalMetadataVersion(),
                sha256(value.removalDurableValueSha256()));
    }

    private static void requireJournalIdentity(
            ObjectKeyHash object,
            String attempt,
            String entryObject,
            String entryAttempt) {
        if (!object.value().equals(entryObject) || !attempt.equals(entryAttempt)) {
            throw new IllegalArgumentException(
                    "GC retirement journal entry belongs to another object or attempt");
        }
    }

    private static Checksum sha256(String value) {
        return new Checksum(ChecksumType.SHA256, value);
    }
}
