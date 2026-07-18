/* Licensed under the Apache License, Version 2.0 */
package com.nereusstream.pulsar;

import com.nereusstream.api.ObjectKeyHash;
import com.nereusstream.managedledger.cursor.CursorLedgerIdentity;
import com.nereusstream.managedledger.retention.CursorSnapshotGcScanner;
import com.nereusstream.materialization.gc.GcCandidate;
import com.nereusstream.materialization.gc.GcIdGenerator;
import com.nereusstream.materialization.gc.GcPlan;
import com.nereusstream.materialization.gc.GcPlannedProtectionRemoval;
import com.nereusstream.materialization.gc.GcReferenceCollection;
import com.nereusstream.materialization.gc.GcReferenceDomainRegistry;
import com.nereusstream.materialization.gc.PhysicalGcAdvanceResult;
import com.nereusstream.materialization.gc.PhysicalGcAdvanceStatus;
import com.nereusstream.materialization.gc.PhysicalGcConfig;
import com.nereusstream.materialization.gc.PhysicalGcDeletionResult;
import com.nereusstream.materialization.gc.PhysicalGcMarkResult;
import com.nereusstream.materialization.gc.PhysicalGcMarkStatus;
import com.nereusstream.materialization.gc.PhysicalObjectGarbageCollector;
import com.nereusstream.materialization.gc.SourceRetirementCoordinator;
import com.nereusstream.metadata.oxia.VersionedPhysicalObjectRoot;
import java.util.ArrayList;
import java.util.List;
import java.util.Objects;
import java.util.Optional;
import java.util.concurrent.CompletableFuture;

/** Bridges F3 cursor inventory to the single F4 physical-root/retirement correctness owner. */
public final class CursorSnapshotGcExecutor {
    private final PhysicalGcConfig config;
    private final CursorSnapshotGcScanner scanner;
    private final GcReferenceDomainRegistry referenceDomains;
    private final PhysicalObjectGarbageCollector garbageCollector;
    private final SourceRetirementCoordinator sourceRetirement;
    private final GcIdGenerator candidateIds;

    public CursorSnapshotGcExecutor(
            PhysicalGcConfig config,
            CursorSnapshotGcScanner scanner,
            GcReferenceDomainRegistry referenceDomains,
            PhysicalObjectGarbageCollector garbageCollector,
            SourceRetirementCoordinator sourceRetirement,
            GcIdGenerator candidateIds) {
        this.config = Objects.requireNonNull(config, "config");
        this.scanner = Objects.requireNonNull(scanner, "scanner");
        this.referenceDomains = Objects.requireNonNull(
                referenceDomains, "referenceDomains");
        this.garbageCollector = Objects.requireNonNull(
                garbageCollector, "garbageCollector");
        this.sourceRetirement = Objects.requireNonNull(
                sourceRetirement, "sourceRetirement");
        this.candidateIds = Objects.requireNonNull(candidateIds, "candidateIds");
    }

    /** Discovers and processes every eligible ACTIVE candidate for one exact ledger identity, sequentially. */
    public CompletableFuture<ScanExecutionReport> scan(
            CursorLedgerIdentity ledger) {
        Objects.requireNonNull(ledger, "ledger");
        ArrayList<CandidateExecutionResult> executions = new ArrayList<>();
        return scanner.scan(
                        ledger,
                        candidate -> executeActive(candidate)
                                .thenAccept(executions::add))
                .thenApply(scan -> new ScanExecutionReport(scan, executions));
    }

    /**
     * Reconstructs and resumes one exact MARKED cursor root after restart.
     *
     * <p>A complete but changed inventory cannot reproduce the root-authenticated plan digest and is conditionally
     * rolled back to ACTIVE. An incomplete/failed read propagates and leaves MARKED for a later retry.
     */
    public CompletableFuture<CandidateExecutionResult> recoverMarked(
            CursorLedgerIdentity ledger,
            VersionedPhysicalObjectRoot markedRoot) {
        Objects.requireNonNull(ledger, "ledger");
        VersionedPhysicalObjectRoot marked = Objects.requireNonNull(
                markedRoot, "markedRoot");
        return scanner.recoverMarked(ledger, marked)
                .thenCompose(optional -> optional
                        .<CompletableFuture<CandidateExecutionResult>>map(candidate ->
                                reconstructAndAdvance(candidate, marked))
                        .orElseGet(() -> unmark(marked)));
    }

    /** Resumes an already durable DELETING intent without consulting object-store listing. */
    public CompletableFuture<PhysicalGcDeletionResult> recoverDeleting(
            VersionedPhysicalObjectRoot deletingRoot) {
        return sourceRetirement.resume(Objects.requireNonNull(
                deletingRoot, "deletingRoot"));
    }

    private CompletableFuture<CandidateExecutionResult> executeActive(
            CursorSnapshotGcScanner.Candidate candidate) {
        if (candidate.sourceRoot().value().lifecycle()
                != com.nereusstream.metadata.oxia.records.PhysicalObjectLifecycle.ACTIVE) {
            return CompletableFuture.failedFuture(new IllegalArgumentException(
                    "ACTIVE cursor execution received a non-ACTIVE source root"));
        }
        GcCandidate gcCandidate = GcCandidate.fromActiveRoot(
                config,
                candidateIds.next(),
                candidate.sourceRoot(),
                candidate.referenceQuery(),
                candidate.discoveryEvidenceSha256(),
                candidate.discoveredAtMillis(),
                candidate.notBeforeMillis());
        List<GcPlannedProtectionRemoval> protections = protections(candidate);
        return garbageCollector.mark(gcCandidate, protections, List.of())
                .thenCompose(mark -> {
                    if (mark.status() != PhysicalGcMarkStatus.MARKED) {
                        return CompletableFuture.completedFuture(
                                CandidateExecutionResult.markOnly(
                                        gcCandidate.object().objectKeyHash(), mark));
                    }
                    return advance(
                            candidate,
                            mark.plan().orElseThrow(),
                            Optional.of(mark));
                });
    }

    private CompletableFuture<CandidateExecutionResult> reconstructAndAdvance(
            CursorSnapshotGcScanner.Candidate candidate,
            VersionedPhysicalObjectRoot marked) {
        GcCandidate gcCandidate = GcCandidate.fromMarkedRoot(
                config,
                candidateIds.next(),
                marked,
                candidate.referenceQuery(),
                candidate.discoveryEvidenceSha256(),
                candidate.discoveredAtMillis());
        return referenceDomains.snapshotForDeletion(candidate.referenceQuery())
                .thenCompose(collection -> reconstructPlan(
                        candidate, gcCandidate, marked, collection));
    }

    private CompletableFuture<CandidateExecutionResult> reconstructPlan(
            CursorSnapshotGcScanner.Candidate candidate,
            GcCandidate gcCandidate,
            VersionedPhysicalObjectRoot marked,
            GcReferenceCollection collection) {
        if (!collection.clear()) {
            return unmark(marked);
        }
        final GcPlan plan;
        try {
            plan = GcPlan.fromMarkedRoot(
                    config,
                    marked.value().gcAttemptId(),
                    gcCandidate,
                    collection.snapshots(),
                    protections(candidate),
                    List.of(),
                    marked);
        } catch (IllegalArgumentException drift) {
            return unmark(marked);
        }
        return advance(candidate, plan, Optional.empty());
    }

    private CompletableFuture<CandidateExecutionResult> advance(
            CursorSnapshotGcScanner.Candidate candidate,
            GcPlan plan,
            Optional<PhysicalGcMarkResult> mark) {
        return garbageCollector.advanceToDeleteIntent(
                        plan,
                        ignored -> scanner.revalidate(candidate))
                .thenCompose(advance -> {
                    if (advance.status() != PhysicalGcAdvanceStatus.DELETE_INTENT) {
                        return CompletableFuture.completedFuture(
                                CandidateExecutionResult.advanced(
                                        plan.candidate().object().objectKeyHash(),
                                        mark,
                                        advance));
                    }
                    return sourceRetirement.resume(advance.root().orElseThrow())
                            .thenApply(deletion -> CandidateExecutionResult.deleted(
                                    plan.candidate().object().objectKeyHash(),
                                    mark,
                                    advance,
                                    deletion));
                });
    }

    private CompletableFuture<CandidateExecutionResult> unmark(
            VersionedPhysicalObjectRoot marked) {
        ObjectKeyHash object = new ObjectKeyHash(marked.value().objectKeyHash());
        return garbageCollector.unmarkDrifted(marked)
                .thenApply(advance -> CandidateExecutionResult.advanced(
                        object, Optional.empty(), advance));
    }

    private static List<GcPlannedProtectionRemoval> protections(
            CursorSnapshotGcScanner.Candidate candidate) {
        return candidate.plannedProtectionRemovals().stream()
                .map(GcPlannedProtectionRemoval::new)
                .toList();
    }

    public record ScanExecutionReport(
            CursorSnapshotGcScanner.ScanResult scan,
            List<CandidateExecutionResult> executions) {
        public ScanExecutionReport {
            Objects.requireNonNull(scan, "scan");
            executions = List.copyOf(Objects.requireNonNull(
                    executions, "executions"));
            if (executions.size() != scan.visitedCandidates()) {
                throw new IllegalArgumentException(
                        "cursor GC execution count differs from visited candidates");
            }
        }
    }

    public record CandidateExecutionResult(
            ObjectKeyHash object,
            Optional<PhysicalGcMarkResult> mark,
            Optional<PhysicalGcAdvanceResult> advance,
            Optional<PhysicalGcDeletionResult> deletion) {
        public CandidateExecutionResult {
            Objects.requireNonNull(object, "object");
            mark = Objects.requireNonNull(mark, "mark");
            advance = Objects.requireNonNull(advance, "advance");
            deletion = Objects.requireNonNull(deletion, "deletion");
            if (deletion.isPresent()
                    && (advance.isEmpty()
                            || advance.orElseThrow().status()
                                    != PhysicalGcAdvanceStatus.DELETE_INTENT)) {
                throw new IllegalArgumentException(
                        "cursor GC deletion requires a durable delete-intent result");
            }
        }

        private static CandidateExecutionResult markOnly(
                ObjectKeyHash object,
                PhysicalGcMarkResult mark) {
            return new CandidateExecutionResult(
                    object, Optional.of(mark), Optional.empty(), Optional.empty());
        }

        private static CandidateExecutionResult advanced(
                ObjectKeyHash object,
                Optional<PhysicalGcMarkResult> mark,
                PhysicalGcAdvanceResult advance) {
            return new CandidateExecutionResult(
                    object, mark, Optional.of(advance), Optional.empty());
        }

        private static CandidateExecutionResult deleted(
                ObjectKeyHash object,
                Optional<PhysicalGcMarkResult> mark,
                PhysicalGcAdvanceResult advance,
                PhysicalGcDeletionResult deletion) {
            return new CandidateExecutionResult(
                    object,
                    mark,
                    Optional.of(advance),
                    Optional.of(deletion));
        }
    }
}
