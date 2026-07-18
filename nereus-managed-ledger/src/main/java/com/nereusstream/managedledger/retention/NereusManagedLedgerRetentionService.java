/* Licensed under the Apache License, Version 2.0 */
package com.nereusstream.managedledger.retention;

import com.nereusstream.api.ErrorCode;
import com.nereusstream.api.NereusException;
import com.nereusstream.api.StreamId;
import com.nereusstream.core.capability.GenerationActivationProof;
import com.nereusstream.core.capability.GenerationOperation;
import com.nereusstream.core.capability.GenerationProtocolActivationGuard;
import com.nereusstream.core.capability.LiveProjectionSubject;
import com.nereusstream.managedledger.NereusManagedLedgerOwnershipGuard;
import com.nereusstream.managedledger.cursor.CursorOwnerSession;
import com.nereusstream.managedledger.cursor.CursorRetentionCoordinator;
import com.nereusstream.managedledger.cursor.CursorRetentionView;
import java.util.Objects;
import java.util.Optional;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.CompletionException;
import java.util.concurrent.ExecutionException;
import java.util.function.Consumer;

/** Ownership- and activation-gated logical trim that delegates the only mutation to the F3 coordinator. */
public final class NereusManagedLedgerRetentionService {
    private static final int MAX_PREPARE_ATTEMPTS = 4;

    private final StreamId streamId;
    private final LiveProjectionSubject liveProjection;
    private final NereusManagedLedgerOwnershipGuard ownershipGuard;
    private final GenerationProtocolActivationGuard activationGuard;
    private final RetentionPolicySnapshotProvider policies;
    private final RetentionCandidatePlanner planner;
    private final CursorRetentionCoordinator cursorRetention;
    private final CursorOwnerSession owner;
    private final Consumer<CursorRetentionView> completedTrimObserver;

    public NereusManagedLedgerRetentionService(
            StreamId streamId,
            LiveProjectionSubject liveProjection,
            NereusManagedLedgerOwnershipGuard ownershipGuard,
            GenerationProtocolActivationGuard activationGuard,
            RetentionPolicySnapshotProvider policies,
            RetentionCandidatePlanner planner,
            CursorRetentionCoordinator cursorRetention,
            CursorOwnerSession owner,
            Consumer<CursorRetentionView> completedTrimObserver) {
        this.streamId = Objects.requireNonNull(streamId, "streamId");
        this.liveProjection = Objects.requireNonNull(
                liveProjection, "liveProjection");
        this.ownershipGuard = Objects.requireNonNull(
                ownershipGuard, "ownershipGuard");
        this.activationGuard = Objects.requireNonNull(
                activationGuard, "activationGuard");
        this.policies = Objects.requireNonNull(policies, "policies");
        this.planner = Objects.requireNonNull(planner, "planner");
        this.cursorRetention = Objects.requireNonNull(
                cursorRetention, "cursorRetention");
        this.owner = Objects.requireNonNull(owner, "owner");
        this.completedTrimObserver = Objects.requireNonNull(
                completedTrimObserver, "completedTrimObserver");
        if (!liveProjection.streamId().equals(streamId)
                || !owner.ledger().projection().streamId()
                        .equals(streamId.value())) {
            throw new IllegalArgumentException(
                    "retention service stream, projection, and cursor owner must agree");
        }
    }

    /** Completes after F3 has made the logical trim ACTIVE; physical deletion is intentionally out of scope. */
    public CompletableFuture<Optional<RetentionCandidate>> trim(
            String reason) {
        final String exactReason;
        try {
            exactReason = requireReason(reason);
        } catch (Throwable failure) {
            return CompletableFuture.failedFuture(failure);
        }
        return prepare(0)
                .thenCompose(prepared -> {
                    if (prepared.candidate().isEmpty()) {
                        return ownershipGuard
                                .requireOwned(
                                        "logical retention no-op completion")
                                .thenApply(ignored -> Optional.empty());
                    }
                    RetentionCandidate candidate =
                            prepared.candidate().orElseThrow();
                    return cursorRetention.requestTrim(
                                    owner,
                                    candidate.candidateTrimOffset(),
                                    exactReason)
                            .thenCompose(view -> ownershipGuard
                                    .requireOwned(
                                            "logical retention callback completion")
                                    .thenApply(ignored -> view))
                            .thenApply(view -> {
                                completedTrimObserver.accept(view);
                                return Optional.of(candidate);
                            });
                });
    }

    StreamId streamId() {
        return streamId;
    }

    private CompletableFuture<PreparedTrim> prepare(int attempt) {
        if (attempt >= MAX_PREPARE_ATTEMPTS) {
            return CompletableFuture.failedFuture(new NereusException(
                    ErrorCode.METADATA_CONDITION_FAILED,
                    true,
                    "logical retention did not stabilize before entering the F3 trim protocol"));
        }
        return ownershipGuard.requireOwned(
                        "logical retention admission")
                .thenCompose(ignored -> activationGuard.requireReady(
                        GenerationOperation.LOGICAL_TRIM,
                        liveProjection,
                        true))
                .thenCompose(proof -> snapshotPolicy()
                        .thenCompose(policy -> planner.plan(
                                        streamId, policy)
                                .thenCompose(candidate -> prepareCandidate(
                                        proof,
                                        policy,
                                        candidate))))
                .handle((prepared, failure) -> {
                    Throwable cause = failure == null
                            ? null
                            : unwrap(failure);
                    if (cause instanceof NereusException nereus
                            && nereus.retriable()
                            && nereus.code()
                                    == ErrorCode.METADATA_CONDITION_FAILED) {
                        return prepare(attempt + 1);
                    }
                    if (cause != null) {
                        return CompletableFuture
                                .<PreparedTrim>failedFuture(cause);
                    }
                    return CompletableFuture.completedFuture(prepared);
                })
                .thenCompose(value -> value);
    }

    private CompletableFuture<PreparedTrim> prepareCandidate(
            GenerationActivationProof proof,
            RetentionPolicySnapshot policy,
            Optional<RetentionCandidate> candidate) {
        if (candidate.isEmpty()) {
            return CompletableFuture.completedFuture(
                    new PreparedTrim(Optional.empty()));
        }
        RetentionCandidate exact = candidate.orElseThrow();
        return activationGuard.revalidate(proof)
                .thenCompose(ignored -> planner.revalidate(
                        exact, policy))
                .thenApply(ignored -> new PreparedTrim(
                        Optional.of(exact)));
    }

    private CompletableFuture<RetentionPolicySnapshot> snapshotPolicy() {
        try {
            return Objects.requireNonNull(
                    policies.snapshot(streamId),
                    "retention policy provider returned null future");
        } catch (Throwable failure) {
            return CompletableFuture.failedFuture(failure);
        }
    }

    private static String requireReason(String reason) {
        Objects.requireNonNull(reason, "reason");
        if (reason.isBlank() || reason.indexOf('\0') >= 0) {
            throw new IllegalArgumentException(
                    "logical retention reason must be non-blank and cannot contain NUL");
        }
        return reason;
    }

    private static Throwable unwrap(Throwable failure) {
        Throwable current = Objects.requireNonNull(failure, "failure");
        while ((current instanceof CompletionException
                        || current instanceof ExecutionException)
                && current.getCause() != null) {
            current = current.getCause();
        }
        return current;
    }

    private record PreparedTrim(
            Optional<RetentionCandidate> candidate) {
        private PreparedTrim {
            candidate = Objects.requireNonNull(candidate, "candidate");
        }
    }
}
