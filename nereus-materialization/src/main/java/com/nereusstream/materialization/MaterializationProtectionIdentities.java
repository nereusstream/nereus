/* Licensed under the Apache License, Version 2.0 */
package com.nereusstream.materialization;

import com.nereusstream.api.keys.DeterministicIds;
import com.nereusstream.core.physical.ObjectProtectionOwner;
import com.nereusstream.metadata.oxia.VersionedGenerationIndex;
import com.nereusstream.metadata.oxia.VersionedMaterializationTask;

/** Canonical durable owner and reference identities shared by workers and recovery. */
final class MaterializationProtectionIdentities {
    private MaterializationProtectionIdentities() {
    }

    static ObjectProtectionOwner taskOwner(VersionedMaterializationTask task) {
        return new ObjectProtectionOwner(
                task.key(), task.metadataVersion(), task.durableValueSha256());
    }

    static ObjectProtectionOwner indexOwner(VersionedGenerationIndex index) {
        return new ObjectProtectionOwner(
                index.key(), index.metadataVersion(), index.durableValueSha256());
    }

    static String sourceReferenceId(
            String cluster,
            MaterializationTask task,
            SourceGeneration source) {
        String canonical = cluster + '\0' + task.streamId().value() + '\0'
                + task.taskId() + '\0' + source.indexKey();
        return "ms1-" + DeterministicIds.stableHashComponent(canonical);
    }

    static String outputReferenceId(
            String cluster,
            MaterializationTask task,
            MaterializationOutput output) {
        String canonical = cluster + '\0' + task.streamId().value() + '\0'
                + task.taskId() + '\0' + output.outputAttemptId() + '\0'
                + output.objectKeyHash().value();
        return "mo1-" + DeterministicIds.stableHashComponent(canonical);
    }

    static String visibleReferenceId(
            String cluster,
            MaterializationTask task,
            long generation,
            String publicationId) {
        String canonical = cluster
                + '\0' + task.streamId().value()
                + '\0' + task.view().wireId()
                + '\0' + task.coverage().endOffset()
                + '\0' + generation
                + '\0' + publicationId
                + '\0' + task.taskId();
        return "vg1-" + DeterministicIds.stableHashComponent(canonical);
    }
}
