/*
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 *   https://www.apache.org/licenses/LICENSE-2.0
 */
package com.nereusstream.api.target;

import com.nereusstream.api.Checksum;
import com.nereusstream.api.EntryIndexLocation;
import com.nereusstream.api.EntryIndexRef;
import com.nereusstream.api.ObjectId;
import com.nereusstream.api.ObjectKey;
import com.nereusstream.api.ObjectType;
import java.nio.charset.StandardCharsets;
import java.util.Objects;

/** Immutable selection of one stream slice stored in an object. */
public record ObjectSliceReadTarget(
        int version,
        ObjectId objectId,
        ObjectKey objectKey,
        ObjectType objectType,
        String physicalFormat,
        String logicalFormat,
        String sliceId,
        long objectOffset,
        long objectLength,
        Checksum sliceChecksum,
        EntryIndexRef entryIndexRef) implements ReadTarget {
    public ObjectSliceReadTarget {
        if (version != 1) {
            throw new IllegalArgumentException("object target version must be 1");
        }
        Objects.requireNonNull(objectId, "objectId");
        Objects.requireNonNull(objectKey, "objectKey");
        Objects.requireNonNull(objectType, "objectType");
        requireText(physicalFormat, "physicalFormat");
        requireText(logicalFormat, "logicalFormat");
        requireText(sliceId, "sliceId");
        Objects.requireNonNull(sliceChecksum, "sliceChecksum");
        Objects.requireNonNull(entryIndexRef, "entryIndexRef");
        if (objectOffset < 0 || objectLength <= 0) {
            throw new IllegalArgumentException("object range must be non-negative and non-empty");
        }
        try {
            Math.addExact(objectOffset, objectLength);
        } catch (ArithmeticException e) {
            throw new IllegalArgumentException("object range overflows", e);
        }
        if (entryIndexRef.location() == EntryIndexLocation.OBJECT_FOOTER
                && entryIndexRef.objectId().isPresent()
                && (!entryIndexRef.objectId().orElseThrow().equals(objectId)
                || !entryIndexRef.objectKey().orElseThrow().equals(objectKey))) {
            throw new IllegalArgumentException("OBJECT_FOOTER entry index must reference the target object");
        }
    }

    @Override
    public ReadTargetType type() {
        return ReadTargetType.OBJECT_SLICE;
    }

    private static void requireText(String value, String name) {
        Objects.requireNonNull(value, name);
        if (value.isBlank() || value.getBytes(StandardCharsets.UTF_8).length > 16 * 1024) {
            throw new IllegalArgumentException(name + " must be nonblank and at most 16384 UTF-8 bytes");
        }
    }
}
