/* Licensed under the Apache License, Version 2.0 */
package com.nereusstream.metadata.oxia.records;

import com.nereusstream.api.StorageProfile;

/** Sharded liveness hint for restart-safe materialization work discovery. */
public record MaterializationStreamRegistrationRecord(
        int schemaVersion,
        String streamId,
        String projectionRef,
        String projectionIdentitySha256,
        String storageProfile,
        long registeredAtMillis,
        long lastHintCommitVersion,
        long updatedAtMillis,
        long metadataVersion) {
    public MaterializationStreamRegistrationRecord {
        F4RecordValidation.requireSchemaVersion(schemaVersion);
        streamId = F4RecordValidation.requireText(streamId, "streamId");
        projectionRef = F4RecordValidation.requireText(projectionRef, "projectionRef");
        projectionIdentitySha256 = F4RecordValidation.requireSha256(
                projectionIdentitySha256, "projectionIdentitySha256");
        storageProfile = F4RecordValidation.requireText(storageProfile, "storageProfile");
        try {
            StorageProfile.valueOf(storageProfile);
        } catch (IllegalArgumentException error) {
            throw new IllegalArgumentException("storageProfile is not canonical", error);
        }
        F4RecordValidation.requireNonNegative(registeredAtMillis, "registeredAtMillis");
        F4RecordValidation.requireNonNegative(lastHintCommitVersion, "lastHintCommitVersion");
        if (updatedAtMillis < registeredAtMillis) {
            throw new IllegalArgumentException("updatedAtMillis cannot precede registration");
        }
        F4RecordValidation.requireMetadataVersion(metadataVersion);
    }

    public MaterializationStreamRegistrationRecord withMetadataVersion(long version) {
        return new MaterializationStreamRegistrationRecord(
                schemaVersion, streamId, projectionRef, projectionIdentitySha256, storageProfile,
                registeredAtMillis, lastHintCommitVersion, updatedAtMillis, version);
    }
}
