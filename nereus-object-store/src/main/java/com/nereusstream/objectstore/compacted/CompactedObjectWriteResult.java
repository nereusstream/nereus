/* Licensed under the Apache License, Version 2.0 */
package com.nereusstream.objectstore.compacted;

import com.nereusstream.api.Checksum;
import com.nereusstream.api.ChecksumType;
import com.nereusstream.api.EntryIndexLocation;
import com.nereusstream.api.EntryIndexRef;
import com.nereusstream.api.ObjectId;
import com.nereusstream.api.ObjectKey;
import com.nereusstream.api.ObjectKeyHash;
import com.nereusstream.objectstore.staging.StagedObjectFile;
import java.util.Objects;

/** Close-owned sealed compacted object and its exact immutable publication identity. */
public record CompactedObjectWriteResult(
        StagedObjectFile stagingFile,
        ObjectId objectId,
        ObjectKey objectKey,
        ObjectKeyHash objectKeyHash,
        long objectLength,
        Checksum storageCrc32c,
        Checksum contentSha256,
        String physicalFormat,
        EntryIndexRef entryIndexRef,
        int outputRecordCount) implements AutoCloseable {
    public CompactedObjectWriteResult {
        Objects.requireNonNull(stagingFile, "stagingFile");
        Objects.requireNonNull(objectId, "objectId");
        Objects.requireNonNull(objectKey, "objectKey");
        Objects.requireNonNull(objectKeyHash, "objectKeyHash");
        if (!objectId.equals(CompactedObjectFormatV1.objectId(objectKey))
                || !objectKeyHash.equals(ObjectKeyHash.from(objectKey))
                || objectLength <= 0
                || stagingFile.sealedLength() != objectLength) {
            throw new IllegalArgumentException("compacted object identity/length is inconsistent");
        }
        requireChecksum(storageCrc32c, ChecksumType.CRC32C, "storageCrc32c");
        requireChecksum(contentSha256, ChecksumType.SHA256, "contentSha256");
        if (!storageCrc32c.equals(stagingFile.storageCrc32c())
                || !contentSha256.equals(stagingFile.contentSha256())) {
            throw new IllegalArgumentException("compacted object checksums do not match staging bytes");
        }
        if (!physicalFormat.equals(CompactedObjectFormatV1.COMMITTED_PHYSICAL_FORMAT)
                && !physicalFormat.equals(CompactedObjectFormatV1.TOPIC_COMPACTED_PHYSICAL_FORMAT)) {
            throw new IllegalArgumentException("unknown compacted physical format");
        }
        Objects.requireNonNull(entryIndexRef, "entryIndexRef");
        if (entryIndexRef.location() != EntryIndexLocation.OBJECT_FOOTER
                || !entryIndexRef.objectId().equals(java.util.Optional.of(objectId))
                || !entryIndexRef.objectKey().equals(java.util.Optional.of(objectKey))
                || entryIndexRef.offset() + entryIndexRef.length() != objectLength
                || entryIndexRef.length() > CompactedObjectFormatV1.MAX_FOOTER_BYTES
                || outputRecordCount < 0) {
            throw new IllegalArgumentException("compacted footer reference/accounting is invalid");
        }
    }

    @Override
    public void close() {
        stagingFile.close();
    }

    private static void requireChecksum(Checksum checksum, ChecksumType type, String field) {
        Objects.requireNonNull(checksum, field);
        if (checksum.type() != type) {
            throw new IllegalArgumentException(field + " must use " + type);
        }
    }
}
