/* Licensed under the Apache License, Version 2.0 */
package com.nereusstream.materialization.gc;

import com.nereusstream.metadata.oxia.VersionedPhysicalObjectRoot;
import com.nereusstream.metadata.oxia.records.PhysicalObjectLifecycle;
import java.util.Objects;

/** Root-authenticated durable context shared with one typed metadata-retirement handler. */
public record GcMetadataRetirementContext(
        VersionedPhysicalObjectRoot deletingRoot,
        GcRetirementJournalSnapshot journal) {
    public GcMetadataRetirementContext {
        Objects.requireNonNull(deletingRoot, "deletingRoot");
        Objects.requireNonNull(journal, "journal");
        if (deletingRoot.value().lifecycle() != PhysicalObjectLifecycle.DELETING
                || !deletingRoot.value().objectKeyHash().equals(journal.object().value())
                || !deletingRoot.value().gcAttemptId().equals(journal.gcAttemptId())
                || !deletingRoot.value().referenceSetSha256().equals(
                        journal.referenceSetSha256().value())) {
            throw new IllegalArgumentException(
                    "metadata-retirement context root and journal do not match");
        }
    }
}
