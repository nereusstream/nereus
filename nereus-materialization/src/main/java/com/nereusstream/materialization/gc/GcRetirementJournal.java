/* Licensed under the Apache License, Version 2.0 */
package com.nereusstream.materialization.gc;

import com.nereusstream.api.Checksum;
import com.nereusstream.api.ObjectKeyHash;
import com.nereusstream.core.physical.GcReferenceSnapshot;
import com.nereusstream.materialization.MaterializationDeadline;
import java.util.List;
import java.util.Optional;
import java.util.concurrent.CompletableFuture;

/** Durable recovery evidence sealed before a physical root may enter MARKED. */
public interface GcRetirementJournal {
    CompletableFuture<GcRetirementJournalSnapshot> prepare(
            String gcAttemptId,
            GcCandidate candidate,
            List<GcReferenceSnapshot> domainSnapshots,
            List<GcPlannedProtectionRemoval> plannedProtectionRemovals,
            List<GcPlannedMetadataRemoval> plannedMetadataRemovals,
            Checksum referenceSetSha256,
            long createdAtMillis,
            MaterializationDeadline deadline);

    CompletableFuture<Optional<GcRetirementJournalSnapshot>> load(
            ObjectKeyHash object,
            String gcAttemptId,
            MaterializationDeadline deadline);
}
