/* Licensed under the Apache License, Version 2.0 */
package com.nereusstream.managedledger.cursor;

import com.nereusstream.core.physical.ObjectProtection;
import com.nereusstream.core.physical.PhysicalObjectIdentity;
import com.nereusstream.metadata.oxia.records.ObjectProtectionType;
import java.util.Objects;

/** Uploaded snapshot plus its exact current-root-owned pending physical protection. */
public record CursorSnapshotPublication(
        CursorSnapshotWriteRequest request,
        CursorSnapshotWriteAuthority authority,
        CursorSnapshotReference reference,
        PhysicalObjectIdentity object,
        ObjectProtection pendingProtection) {
    public CursorSnapshotPublication {
        Objects.requireNonNull(request, "request");
        Objects.requireNonNull(authority, "authority");
        authority.requireMatches(request);
        Objects.requireNonNull(reference, "reference");
        Objects.requireNonNull(object, "object");
        Objects.requireNonNull(pendingProtection, "pendingProtection");
        if (!reference.objectKey().equals(object.objectKey())
                || !reference.storageChecksum().equals(object.storageChecksum())
                || reference.objectLength() != object.objectLength()
                || pendingProtection.identity().type()
                        != ObjectProtectionType.CURSOR_SNAPSHOT_PENDING
                || !pendingProtection.object().equals(object)
                || !pendingProtection.identity().referenceId().equals(
                        reference.snapshotId())) {
            throw new IllegalArgumentException(
                    "cursor snapshot publication facts are inconsistent");
        }
    }
}
