/* Licensed under the Apache License, Version 2.0 */
package com.nereusstream.core.physical;

import com.nereusstream.api.Checksum;
import com.nereusstream.api.ChecksumType;
import com.nereusstream.api.ObjectId;
import com.nereusstream.api.ObjectKey;
import com.nereusstream.api.ObjectKeyHash;
import com.nereusstream.metadata.oxia.records.PhysicalObjectRootRecord;
import java.util.Objects;
import java.util.Optional;

/** Exact immutable identity registered under one physical-object deletion root. */
public record PhysicalObjectIdentity(
        ObjectKey objectKey,
        ObjectKeyHash objectKeyHash,
        Optional<ObjectId> objectId,
        PhysicalObjectKind kind,
        long objectLength,
        Checksum storageChecksum,
        Optional<Checksum> contentSha256,
        Optional<String> etag) {
    public PhysicalObjectIdentity {
        Objects.requireNonNull(objectKey, "objectKey");
        Objects.requireNonNull(objectKeyHash, "objectKeyHash");
        if (!ObjectKeyHash.from(objectKey).equals(objectKeyHash)) {
            throw new IllegalArgumentException("objectKeyHash does not match objectKey");
        }
        objectId = Objects.requireNonNull(objectId, "objectId");
        Objects.requireNonNull(kind, "kind");
        if (objectLength <= 0) {
            throw new IllegalArgumentException("objectLength must be positive");
        }
        Objects.requireNonNull(storageChecksum, "storageChecksum");
        if (storageChecksum.type() != ChecksumType.CRC32C) {
            throw new IllegalArgumentException("storageChecksum must use CRC32C");
        }
        contentSha256 = Objects.requireNonNull(contentSha256, "contentSha256").map(value -> {
            if (value.type() != ChecksumType.SHA256) {
                throw new IllegalArgumentException("contentSha256 must use SHA256");
            }
            return value;
        });
        etag = Objects.requireNonNull(etag, "etag").map(value -> {
            if (value.isBlank() || value.length() > 1024) {
                throw new IllegalArgumentException("etag must be non-blank and at most 1024 characters");
            }
            return value;
        });
    }

    public static PhysicalObjectIdentity create(
            ObjectKey objectKey,
            Optional<ObjectId> objectId,
            PhysicalObjectKind kind,
            long objectLength,
            Checksum storageChecksum,
            Optional<Checksum> contentSha256,
            Optional<String> etag) {
        return new PhysicalObjectIdentity(
                objectKey,
                ObjectKeyHash.from(objectKey),
                objectId,
                kind,
                objectLength,
                storageChecksum,
                contentSha256,
                etag);
    }

    public static PhysicalObjectIdentity from(PhysicalObjectRootRecord root) {
        Objects.requireNonNull(root, "root");
        return new PhysicalObjectIdentity(
                new ObjectKey(root.objectKey()),
                new ObjectKeyHash(root.objectKeyHash()),
                root.objectId().isEmpty()
                        ? Optional.empty()
                        : Optional.of(new ObjectId(root.objectId())),
                PhysicalObjectKind.fromWireId(root.objectKindId()),
                root.objectLength(),
                new Checksum(ChecksumType.valueOf(root.storageChecksumType()), root.storageChecksumValue()),
                root.contentSha256().isEmpty()
                        ? Optional.empty()
                        : Optional.of(new Checksum(ChecksumType.SHA256, root.contentSha256())),
                root.etag().isEmpty() ? Optional.empty() : Optional.of(root.etag()));
    }

    public Checksum identitySha256() {
        return PhysicalValueDigests.physicalIdentity(this);
    }
}
