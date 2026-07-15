/* Licensed under the Apache License, Version 2.0 */
package com.nereusstream.materialization.recovery;

import com.nereusstream.core.physical.PhysicalObjectIdentity;
import com.nereusstream.metadata.oxia.VersionedRecoveryCheckpointRoot;
import com.nereusstream.metadata.oxia.records.RecoveryCheckpointReferenceRecord;
import java.util.Objects;

/** Exact root/object facts returned after publication protections converge. */
public record PublishedRecoveryCheckpoint(
        VersionedRecoveryCheckpointRoot root,
        RecoveryCheckpointReferenceRecord reference,
        PhysicalObjectIdentity object) {
    public PublishedRecoveryCheckpoint {
        Objects.requireNonNull(root, "root");
        Objects.requireNonNull(reference, "reference");
        Objects.requireNonNull(object, "object");
        if (!root.value().checkpoints().contains(reference)
                || !reference.objectKey().equals(object.objectKey().value())
                || !reference.objectKeyHash().equals(object.objectKeyHash().value())) {
            throw new IllegalArgumentException(
                    "published checkpoint root/reference/object identity is inconsistent");
        }
    }
}
