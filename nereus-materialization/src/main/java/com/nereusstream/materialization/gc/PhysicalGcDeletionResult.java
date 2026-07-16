/* Licensed under the Apache License, Version 2.0 */
package com.nereusstream.materialization.gc;

import com.nereusstream.metadata.oxia.VersionedPhysicalObjectRoot;
import com.nereusstream.objectstore.DeleteObjectResult;
import java.util.Objects;
import java.util.Optional;

/** Terminal or non-mutating result of one DELETING-root recovery attempt. */
public record PhysicalGcDeletionResult(
        PhysicalGcDeletionStatus status,
        Optional<VersionedPhysicalObjectRoot> root,
        int metadataRetired,
        int metadataAlreadyAbsent,
        int protectionsRetired,
        int protectionsAlreadyAbsent,
        Optional<DeleteObjectResult.Status> objectStatus) {
    public PhysicalGcDeletionResult {
        Objects.requireNonNull(status, "status");
        root = Objects.requireNonNull(root, "root");
        objectStatus = Objects.requireNonNull(objectStatus, "objectStatus");
        if (metadataRetired < 0
                || metadataAlreadyAbsent < 0
                || protectionsRetired < 0
                || protectionsAlreadyAbsent < 0) {
            throw new IllegalArgumentException("retirement counts must be non-negative");
        }
        boolean terminal = status == PhysicalGcDeletionStatus.DELETED
                || status == PhysicalGcDeletionStatus.ALREADY_DELETED;
        if (terminal != root.isPresent()) {
            throw new IllegalArgumentException(
                    "terminal deletion results must carry the authoritative root");
        }
        if (status == PhysicalGcDeletionStatus.DELETED && objectStatus.isEmpty()) {
            throw new IllegalArgumentException("DELETED result requires an object outcome");
        }
    }

    public static PhysicalGcDeletionResult simple(PhysicalGcDeletionStatus status) {
        return new PhysicalGcDeletionResult(
                status, Optional.empty(), 0, 0, 0, 0, Optional.empty());
    }

    public static PhysicalGcDeletionResult alreadyDeleted(
            VersionedPhysicalObjectRoot root) {
        return new PhysicalGcDeletionResult(
                PhysicalGcDeletionStatus.ALREADY_DELETED,
                Optional.of(root),
                0,
                0,
                0,
                0,
                Optional.empty());
    }
}
