/* Licensed under the Apache License, Version 2.0 */
package com.nereusstream.managedledger.cursor;

import com.nereusstream.api.Checksum;
import com.nereusstream.api.ChecksumType;
import com.nereusstream.api.ObjectKey;
import com.nereusstream.metadata.oxia.CursorIds;
import java.util.Objects;

/** Strict immutable reference to one full cursor ack-state snapshot object. */
public record CursorSnapshotReference(
        ObjectKey objectKey,
        String snapshotId,
        long cursorGeneration,
        long sourceMutationSequence,
        long baseMarkDeleteOffset,
        long objectLength,
        Checksum storageChecksum,
        int formatCrc32c,
        int formatVersion,
        long createdAtMillis) {
    public CursorSnapshotReference {
        Objects.requireNonNull(objectKey, "objectKey");
        snapshotId = CursorIds.requireRandomId(snapshotId, "snapshotId");
        if (cursorGeneration < 1 || sourceMutationSequence < 1) {
            throw new IllegalArgumentException("snapshot generation and source sequence must be positive");
        }
        if (baseMarkDeleteOffset < 0 || objectLength <= 0 || objectLength > 64L * 1024 * 1024) {
            throw new IllegalArgumentException("snapshot offsets or length are invalid");
        }
        Objects.requireNonNull(storageChecksum, "storageChecksum");
        if (storageChecksum.type() != ChecksumType.CRC32C) {
            throw new IllegalArgumentException("cursor snapshots require CRC32C storage checksums");
        }
        if (formatVersion != 1 || createdAtMillis < 0) {
            throw new IllegalArgumentException("cursor snapshot format or timestamp is invalid");
        }
    }
}
