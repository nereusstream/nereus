/* Licensed under the Apache License, Version 2.0 */
package com.nereusstream.metadata.oxia.records;

import com.nereusstream.api.ObjectKeyHash;
import java.util.Comparator;
import java.util.List;

/** Sealed recovery manifest authenticating the sharded facts required after GC enters DELETING. */
public record GcRetirementManifestRecord(
        int schemaVersion,
        String objectKeyHash,
        String gcAttemptId,
        int referenceSetProtocolVersion,
        String queryIdentitySha256,
        List<GcDomainSnapshotProofRecord> domainProofs,
        int protectionCount,
        int metadataRemovalCount,
        String referenceSetSha256,
        long createdAtMillis,
        long metadataVersion) {
    public static final int REFERENCE_SET_PROTOCOL_VERSION = 2;
    public static final int MAX_REFERENCE_DOMAINS = 32;
    public static final int MAX_PLAN_ENTRIES = 100_000;
    private static final Comparator<GcDomainSnapshotProofRecord> DOMAIN_ORDER = Comparator
            .comparing(GcDomainSnapshotProofRecord::domainId)
            .thenComparingInt(GcDomainSnapshotProofRecord::protocolVersion);

    public GcRetirementManifestRecord {
        F4RecordValidation.requireSchemaVersion(schemaVersion);
        new ObjectKeyHash(objectKeyHash);
        gcAttemptId = F4RecordValidation.requireBase32Id(gcAttemptId, "gcAttemptId");
        if (referenceSetProtocolVersion != REFERENCE_SET_PROTOCOL_VERSION) {
            throw new IllegalArgumentException("referenceSetProtocolVersion must be 2");
        }
        queryIdentitySha256 = F4RecordValidation.requireSha256(
                queryIdentitySha256, "queryIdentitySha256");
        domainProofs = F4RecordValidation.immutableBoundedList(
                domainProofs, MAX_REFERENCE_DOMAINS, "domainProofs");
        if (domainProofs.isEmpty()) {
            throw new IllegalArgumentException("domainProofs cannot be empty");
        }
        for (int index = 0; index < domainProofs.size(); index++) {
            GcDomainSnapshotProofRecord proof = domainProofs.get(index);
            if (!proof.queryIdentitySha256().equals(queryIdentitySha256)) {
                throw new IllegalArgumentException("domain proof belongs to another query");
            }
            if (index > 0 && DOMAIN_ORDER.compare(domainProofs.get(index - 1), proof) >= 0) {
                throw new IllegalArgumentException("domainProofs must be strictly sorted and unique");
            }
        }
        requireEntryCount(protectionCount, "protectionCount");
        requireEntryCount(metadataRemovalCount, "metadataRemovalCount");
        referenceSetSha256 = F4RecordValidation.requireSha256(
                referenceSetSha256, "referenceSetSha256");
        F4RecordValidation.requireNonNegative(createdAtMillis, "createdAtMillis");
        F4RecordValidation.requireMetadataVersion(metadataVersion);
    }

    public GcRetirementManifestRecord withMetadataVersion(long version) {
        return new GcRetirementManifestRecord(
                schemaVersion,
                objectKeyHash,
                gcAttemptId,
                referenceSetProtocolVersion,
                queryIdentitySha256,
                domainProofs,
                protectionCount,
                metadataRemovalCount,
                referenceSetSha256,
                createdAtMillis,
                version);
    }

    private static void requireEntryCount(int value, String name) {
        if (value < 0 || value > MAX_PLAN_ENTRIES) {
            throw new IllegalArgumentException(name + " must be in [0, " + MAX_PLAN_ENTRIES + "]");
        }
    }
}
