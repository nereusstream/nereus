/* Licensed under the Apache License, Version 2.0 */
package com.nereusstream.materialization.gc;

import com.nereusstream.api.Checksum;
import com.nereusstream.api.ChecksumType;
import com.nereusstream.api.ObjectId;
import com.nereusstream.api.ObjectKey;
import com.nereusstream.core.physical.PhysicalObjectIdentity;
import com.nereusstream.core.physical.PhysicalObjectKind;
import com.nereusstream.metadata.oxia.VersionedPhysicalObjectRoot;
import com.nereusstream.objectstore.HeadObjectResult;
import java.util.Objects;
import java.util.Optional;

/** Immutable identity derivable from one strict product-owned object-key grammar. */
public record ObjectInventoryKey(
        ObjectKey objectKey,
        Optional<ObjectId> objectId,
        PhysicalObjectKind kind,
        Optional<Checksum> contentSha256) {
    public ObjectInventoryKey {
        Objects.requireNonNull(objectKey, "objectKey");
        objectId = Objects.requireNonNull(objectId, "objectId");
        Objects.requireNonNull(kind, "kind");
        contentSha256 = Objects.requireNonNull(contentSha256, "contentSha256")
                .map(value -> {
                    if (value.type() != ChecksumType.SHA256) {
                        throw new IllegalArgumentException("contentSha256 must use SHA256");
                    }
                    return value;
                });
    }

    public PhysicalObjectIdentity exactHeadIdentity(HeadObjectResult head) {
        Objects.requireNonNull(head, "head");
        if (!head.key().equals(objectKey)
                || head.objectLength() <= 0
                || head.checksum().type() != ChecksumType.CRC32C) {
            throw new IllegalArgumentException("object HEAD does not expose a collectible exact identity");
        }
        return PhysicalObjectIdentity.create(
                objectKey,
                objectId,
                kind,
                head.objectLength(),
                head.checksum(),
                contentSha256,
                head.etag());
    }

    public boolean matchesRootKeyFacts(VersionedPhysicalObjectRoot root) {
        Objects.requireNonNull(root, "root");
        var value = root.value();
        return value.objectKey().equals(objectKey.value())
                && value.objectKeyHash().equals(
                        com.nereusstream.api.ObjectKeyHash.from(objectKey).value())
                && value.objectId().equals(objectId.map(ObjectId::value).orElse(""))
                && value.objectKindId() == kind.wireId()
                && value.contentSha256().equals(contentSha256.map(Checksum::value).orElse(""));
    }
}
