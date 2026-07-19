/* Licensed under the Apache License, Version 2.0 */
package com.nereusstream.core.wal.object;

import com.nereusstream.api.Checksum;
import com.nereusstream.api.ObjectId;
import com.nereusstream.api.ObjectKey;
import com.nereusstream.api.ObjectType;
import com.nereusstream.api.ReadTargetIdentities;
import com.nereusstream.api.target.ObjectSliceReadTarget;
import com.nereusstream.api.target.ReadTargetType;
import com.nereusstream.core.wal.PrimaryPhysicalIdentity;
import java.nio.charset.StandardCharsets;
import java.util.Objects;

/** Exact Object-WAL slice identity exposed through the provider-neutral append seam. */
public record ObjectPrimaryPhysicalIdentity(
        ObjectId objectId,
        ObjectKey objectKey,
        ObjectType objectType,
        long objectOffset,
        long objectLength,
        Checksum sliceChecksum,
        Checksum targetIdentity) implements PrimaryPhysicalIdentity {
    public ObjectPrimaryPhysicalIdentity {
        Objects.requireNonNull(objectId, "objectId");
        Objects.requireNonNull(objectKey, "objectKey");
        Objects.requireNonNull(objectType, "objectType");
        Objects.requireNonNull(sliceChecksum, "sliceChecksum");
        Objects.requireNonNull(targetIdentity, "targetIdentity");
        if (objectOffset < 0 || objectLength <= 0) {
            throw new IllegalArgumentException("invalid Object physical range");
        }
        Math.addExact(objectOffset, objectLength);
    }

    public static ObjectPrimaryPhysicalIdentity from(ObjectSliceReadTarget target) {
        return new ObjectPrimaryPhysicalIdentity(target.objectId(), target.objectKey(), target.objectType(),
                target.objectOffset(), target.objectLength(), target.sliceChecksum(),
                ReadTargetIdentities.sha256(target));
    }

    @Override public ReadTargetType targetType() { return ReadTargetType.OBJECT_SLICE; }
    @Override public byte[] canonicalIdentity() { return targetIdentity.value().getBytes(StandardCharsets.US_ASCII); }
}
