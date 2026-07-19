/* Licensed under the Apache License, Version 2.0 */
package com.nereusstream.materialization;

import com.nereusstream.api.OffsetRange;
import com.nereusstream.api.StreamId;
import com.nereusstream.api.target.ObjectSliceReadTarget;
import com.nereusstream.metadata.oxia.VersionedGenerationIndex;
import com.nereusstream.metadata.oxia.VersionedMaterializationCheckpoint;
import com.nereusstream.metadata.oxia.VersionedObjectProtection;
import com.nereusstream.metadata.oxia.VersionedPhysicalObjectRoot;
import java.util.Objects;

/** Exact metadata authority proving that one logical source range has a healthy committed Object replacement. */
public record CommittedObjectGenerationProof(
        StreamId streamId,
        OffsetRange sourceRange,
        long sourceCommitVersion,
        VersionedGenerationIndex index,
        ObjectSliceReadTarget target,
        VersionedPhysicalObjectRoot root,
        VersionedObjectProtection visibleProtection,
        VersionedMaterializationCheckpoint checkpoint) {
    public CommittedObjectGenerationProof {
        Objects.requireNonNull(streamId, "streamId");
        Objects.requireNonNull(sourceRange, "sourceRange");
        if (sourceRange.isEmpty() || sourceCommitVersion <= 0) {
            throw new IllegalArgumentException("committed Object replacement source identity is invalid");
        }
        Objects.requireNonNull(index, "index");
        Objects.requireNonNull(target, "target");
        Objects.requireNonNull(root, "root");
        Objects.requireNonNull(visibleProtection, "visibleProtection");
        Objects.requireNonNull(checkpoint, "checkpoint");
    }
}
