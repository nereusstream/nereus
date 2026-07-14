/* Licensed under the Apache License, Version 2.0 */
package com.nereusstream.metadata.oxia.records;

import com.nereusstream.api.Checksum;
import com.nereusstream.api.ChecksumType;
import com.nereusstream.metadata.oxia.CursorIds;
import java.util.Objects;

/** Immutable object-store reference embedded in one cursor root. */
public record CursorSnapshotReferenceRecord(
        String objectKey,
        String snapshotId,
        long cursorGeneration,
        long sourceMutationSequence,
        long baseMarkDeleteOffset,
        long objectLength,
        String storageChecksumType,
        String storageChecksumValue,
        int formatCrc32c,
        int formatVersion,
        long createdAtMillis) {
    public static final long MAX_OBJECT_LENGTH = 64L * 1024 * 1024;

    public CursorSnapshotReferenceRecord {
        objectKey = CursorRecordValidation.requireString(objectKey, "objectKey", 64 * 1024, false);
        snapshotId = CursorIds.requireRandomId(snapshotId, "snapshotId");
        if (cursorGeneration < 1 || sourceMutationSequence < 1) {
            throw new IllegalArgumentException("snapshot generation and source sequence must be positive");
        }
        if (baseMarkDeleteOffset < 0) {
            throw new IllegalArgumentException("snapshot baseMarkDeleteOffset must be non-negative");
        }
        if (objectLength <= 0 || objectLength > MAX_OBJECT_LENGTH) {
            throw new IllegalArgumentException("snapshot objectLength is outside the F3 limit");
        }
        storageChecksumType = Objects.requireNonNull(storageChecksumType, "storageChecksumType");
        storageChecksumValue = Objects.requireNonNull(storageChecksumValue, "storageChecksumValue");
        if (!ChecksumType.CRC32C.name().equals(storageChecksumType)) {
            throw new IllegalArgumentException("cursor snapshots require CRC32C storage checksums");
        }
        new Checksum(ChecksumType.CRC32C, storageChecksumValue);
        if (formatVersion != 1) {
            throw new IllegalArgumentException("unsupported cursor snapshot formatVersion");
        }
        if (createdAtMillis < 0) {
            throw new IllegalArgumentException("snapshot createdAtMillis must be non-negative");
        }
    }
}
