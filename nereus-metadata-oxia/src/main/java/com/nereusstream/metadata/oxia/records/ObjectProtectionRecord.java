/* Licensed under the Apache License, Version 2.0 */
package com.nereusstream.metadata.oxia.records;

/** Durable object-deletion veto tied to one revalidatable authoritative owner. */
public record ObjectProtectionRecord(
        int schemaVersion,
        String objectKeyHash,
        int protectionTypeId,
        String referenceId,
        String ownerKey,
        long ownerMetadataVersion,
        String ownerIdentitySha256,
        long rootLifecycleEpoch,
        long createdAtMillis,
        long expiresAtMillis,
        long metadataVersion) {
    public ObjectProtectionRecord {
        F4RecordValidation.requireSchemaVersion(schemaVersion);
        objectKeyHash = F4RecordValidation.requireText(objectKeyHash, "objectKeyHash");
        ObjectProtectionType type = ObjectProtectionType.fromWireId(protectionTypeId);
        referenceId = F4RecordValidation.requireText(
                referenceId, "referenceId", F4RecordValidation.MAX_REFERENCE_ID_BYTES, false);
        ownerKey = F4RecordValidation.requireText(ownerKey, "ownerKey");
        F4RecordValidation.requireMetadataVersion(ownerMetadataVersion);
        ownerIdentitySha256 = F4RecordValidation.requireSha256(ownerIdentitySha256, "ownerIdentitySha256");
        F4RecordValidation.requirePositive(rootLifecycleEpoch, "rootLifecycleEpoch");
        F4RecordValidation.requireNonNegative(createdAtMillis, "createdAtMillis");
        F4RecordValidation.requireNonNegative(expiresAtMillis, "expiresAtMillis");
        boolean pending = type == ObjectProtectionType.CURSOR_SNAPSHOT_PENDING
                || type == ObjectProtectionType.RECOVERY_CHECKPOINT_PENDING
                || type == ObjectProtectionType.KAFKA_CHECKPOINT_PENDING;
        if (pending != (expiresAtMillis > createdAtMillis)) {
            throw new IllegalArgumentException("protection expiry does not match permanent/pending type");
        }
        F4RecordValidation.requireMetadataVersion(metadataVersion);
    }

    public ObjectProtectionRecord withMetadataVersion(long version) {
        return new ObjectProtectionRecord(
                schemaVersion, objectKeyHash, protectionTypeId, referenceId, ownerKey,
                ownerMetadataVersion, ownerIdentitySha256, rootLifecycleEpoch,
                createdAtMillis, expiresAtMillis, version);
    }
}
