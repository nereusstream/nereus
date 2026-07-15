/* Licensed under the Apache License, Version 2.0 */
package com.nereusstream.materialization.recovery;

import com.nereusstream.core.recovery.AnchorAwareCommitWalk;
import com.nereusstream.metadata.oxia.VersionedMaterializationStreamRegistration;
import com.nereusstream.metadata.oxia.VersionedRecoveryCheckpointRoot;
import com.nereusstream.metadata.oxia.records.RecoveryCheckpointReferenceRecord;
import com.nereusstream.objectstore.checkpoint.RecoveryCheckpointEntry;
import com.nereusstream.objectstore.checkpoint.RecoveryCheckpointPublication;
import com.nereusstream.objectstore.checkpoint.RecoveryCheckpointWriteRequest;
import java.util.List;
import java.util.Objects;

/** Fully canonical, still-unpublished recovery-checkpoint object/root plan. */
public record RecoveryCheckpointPlan(
        VersionedRecoveryCheckpointRoot baseRoot,
        VersionedMaterializationStreamRegistration registration,
        AnchorAwareCommitWalk commitWalk,
        RecoveryCheckpointWriteRequest writeRequest,
        List<RecoveryCheckpointReferenceRecord> retainedReferences,
        List<RecoveryCheckpointTarget> targets,
        List<RecoveryCheckpointEntry> entries,
        long maximumObjectBytes) {
    public RecoveryCheckpointPlan {
        Objects.requireNonNull(baseRoot, "baseRoot");
        Objects.requireNonNull(registration, "registration");
        Objects.requireNonNull(commitWalk, "commitWalk");
        Objects.requireNonNull(writeRequest, "writeRequest");
        retainedReferences = List.copyOf(Objects.requireNonNull(
                retainedReferences, "retainedReferences"));
        targets = List.copyOf(Objects.requireNonNull(targets, "targets"));
        entries = List.copyOf(Objects.requireNonNull(entries, "entries"));
        if (maximumObjectBytes <= 0
                || !commitWalk.anchorReached()
                || !baseRoot.value().streamId().equals(writeRequest.streamId().value())
                || !registration.value().streamId().equals(writeRequest.streamId().value())
                || entries.size() != writeRequest.expectedEntryCount()
                || targets.size() != writeRequest.expectedPublicationCount()
                || entries.isEmpty()
                || targets.isEmpty()) {
            throw new IllegalArgumentException("recovery checkpoint plan identity/counts are inconsistent");
        }
        for (int index = 0; index < targets.size(); index++) {
            RecoveryCheckpointPublication publication = targets.get(index).publication();
            if (index > 0) {
                RecoveryCheckpointPublication previous = targets.get(index - 1).publication();
                if (publication.generation() < previous.generation()
                        || (publication.generation() == previous.generation()
                                && publication.publicationId().value().compareTo(
                                        previous.publicationId().value()) <= 0)) {
                    throw new IllegalArgumentException("checkpoint targets are not canonically sorted");
                }
            }
        }
    }

    public List<RecoveryCheckpointPublication> publications() {
        return targets.stream().map(RecoveryCheckpointTarget::publication).toList();
    }
}
