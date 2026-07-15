/* Licensed under the Apache License, Version 2.0 */
package com.nereusstream.metadata.oxia.records;

import com.nereusstream.api.ObjectKey;
import com.nereusstream.api.ObjectKeyHash;

/** Authoritative lifecycle/deletion fence for one immutable physical object key. */
public record PhysicalObjectRootRecord(
        int schemaVersion,
        String objectKeyHash,
        String objectKey,
        String objectId,
        int objectKindId,
        long objectLength,
        String storageChecksumType,
        String storageChecksumValue,
        String contentSha256,
        String etag,
        PhysicalObjectLifecycle lifecycle,
        long lifecycleEpoch,
        long createdAtMillis,
        long orphanNotBeforeMillis,
        String gcAttemptId,
        String referenceSetSha256,
        long markedAtMillis,
        long deleteNotBeforeMillis,
        long deleteStartedAtMillis,
        long deletedAtMillis,
        long tombstoneFirstAbsentAtMillis,
        String tombstoneProofSha256,
        String stateReason,
        long metadataVersion) {
    public PhysicalObjectRootRecord {
        F4RecordValidation.requireSchemaVersion(schemaVersion);
        objectKey = F4RecordValidation.requireText(objectKey, "objectKey");
        objectKeyHash = F4RecordValidation.requireText(objectKeyHash, "objectKeyHash");
        if (!ObjectKeyHash.from(new ObjectKey(objectKey)).value().equals(objectKeyHash)) {
            throw new IllegalArgumentException("objectKeyHash does not match objectKey");
        }
        objectId = F4RecordValidation.requireOptionalText(objectId, "objectId", 512);
        if (objectKindId < 1 || objectKindId > 7) {
            throw new IllegalArgumentException("objectKindId is unknown");
        }
        F4RecordValidation.requirePositive(objectLength, "objectLength");
        storageChecksumType = F4RecordValidation.requireText(storageChecksumType, "storageChecksumType");
        if (!"CRC32C".equals(storageChecksumType)) {
            throw new IllegalArgumentException("V1 physical roots require CRC32C storage checksums");
        }
        storageChecksumValue = F4RecordValidation.requireCrc32c(
                storageChecksumValue, "storageChecksumValue");
        contentSha256 = F4RecordValidation.requireOptionalSha256(contentSha256, "contentSha256");
        etag = F4RecordValidation.requireOptionalText(etag, "etag", 1024);
        if (lifecycle == null) {
            throw new NullPointerException("lifecycle");
        }
        F4RecordValidation.requirePositive(lifecycleEpoch, "lifecycleEpoch");
        F4RecordValidation.requireNonNegative(createdAtMillis, "createdAtMillis");
        if (orphanNotBeforeMillis < createdAtMillis) {
            throw new IllegalArgumentException("orphanNotBeforeMillis cannot precede creation");
        }
        gcAttemptId = F4RecordValidation.requireOptionalText(gcAttemptId, "gcAttemptId", 128);
        referenceSetSha256 = F4RecordValidation.requireOptionalSha256(
                referenceSetSha256, "referenceSetSha256");
        F4RecordValidation.requireNonNegative(markedAtMillis, "markedAtMillis");
        F4RecordValidation.requireNonNegative(deleteNotBeforeMillis, "deleteNotBeforeMillis");
        F4RecordValidation.requireNonNegative(deleteStartedAtMillis, "deleteStartedAtMillis");
        F4RecordValidation.requireNonNegative(deletedAtMillis, "deletedAtMillis");
        F4RecordValidation.requireNonNegative(tombstoneFirstAbsentAtMillis, "tombstoneFirstAbsentAtMillis");
        tombstoneProofSha256 = F4RecordValidation.requireOptionalSha256(
                tombstoneProofSha256, "tombstoneProofSha256");
        stateReason = F4RecordValidation.requireOptionalText(
                stateReason, "stateReason", F4RecordValidation.MAX_REASON_BYTES);
        F4RecordValidation.requireMetadataVersion(metadataVersion);

        boolean tombstoneEmpty = tombstoneFirstAbsentAtMillis == 0 && tombstoneProofSha256.isEmpty();
        boolean tombstonePresent = tombstoneFirstAbsentAtMillis > deletedAtMillis && !tombstoneProofSha256.isEmpty();
        if (lifecycle != PhysicalObjectLifecycle.DELETED && !tombstoneEmpty) {
            throw new IllegalArgumentException("only DELETED roots may carry tombstone audit state");
        }
        if (lifecycle == PhysicalObjectLifecycle.DELETED && !(tombstoneEmpty || tombstonePresent)) {
            throw new IllegalArgumentException("DELETED tombstone observation fields are inconsistent");
        }
        switch (lifecycle) {
            case ACTIVE -> requireNoGcState(
                    gcAttemptId, referenceSetSha256, markedAtMillis, deleteNotBeforeMillis,
                    deleteStartedAtMillis, deletedAtMillis, stateReason);
            case MARKED -> requireMarkState(
                    gcAttemptId, referenceSetSha256, markedAtMillis, deleteNotBeforeMillis,
                    deleteStartedAtMillis, deletedAtMillis, stateReason, false, false);
            case DELETING -> requireMarkState(
                    gcAttemptId, referenceSetSha256, markedAtMillis, deleteNotBeforeMillis,
                    deleteStartedAtMillis, deletedAtMillis, stateReason, true, false);
            case DELETED -> requireMarkState(
                    gcAttemptId, referenceSetSha256, markedAtMillis, deleteNotBeforeMillis,
                    deleteStartedAtMillis, deletedAtMillis, stateReason, true, true);
            case QUARANTINED -> {
                if (stateReason.isEmpty()) {
                    throw new IllegalArgumentException("QUARANTINED root requires a state reason");
                }
            }
        }
    }

    private static void requireMarkState(
            String attempt,
            String referenceSet,
            long marked,
            long notBefore,
            long started,
            long deletedAt,
            String reason,
            boolean deleting,
            boolean deleted) {
        F4RecordValidation.requireBase32Id(attempt, "gcAttemptId");
        F4RecordValidation.requireSha256(referenceSet, "referenceSetSha256");
        if (marked <= 0 || notBefore < marked) {
            throw new IllegalArgumentException("marked root has invalid mark/drain timestamps");
        }
        if (deleting != (started >= notBefore && started > 0)) {
            throw new IllegalArgumentException("root delete-start timestamp does not match lifecycle");
        }
        if (deleted != (deletedAt >= started && deletedAt > 0)) {
            throw new IllegalArgumentException("root deleted timestamp does not match lifecycle");
        }
        if (!reason.isEmpty()) {
            throw new IllegalArgumentException("ordinary GC lifecycle cannot carry a state reason");
        }
    }

    private static void requireNoGcState(
            String attempt,
            String referenceSet,
            long marked,
            long notBefore,
            long started,
            long deleted,
            String reason) {
        if (!attempt.isEmpty() || !referenceSet.isEmpty() || marked != 0 || notBefore != 0
                || started != 0 || deleted != 0 || !reason.isEmpty()) {
            throw new IllegalArgumentException("ACTIVE root cannot carry GC state");
        }
    }

    public PhysicalObjectRootRecord withMetadataVersion(long version) {
        return new PhysicalObjectRootRecord(
                schemaVersion, objectKeyHash, objectKey, objectId, objectKindId, objectLength,
                storageChecksumType, storageChecksumValue, contentSha256, etag, lifecycle, lifecycleEpoch,
                createdAtMillis, orphanNotBeforeMillis, gcAttemptId, referenceSetSha256, markedAtMillis,
                deleteNotBeforeMillis, deleteStartedAtMillis, deletedAtMillis, tombstoneFirstAbsentAtMillis,
                tombstoneProofSha256, stateReason, version);
    }
}
