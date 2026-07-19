/* Licensed under the Apache License, Version 2.0 */
package com.nereusstream.bookkeeper;

import com.nereusstream.api.AppendAttemptId;
import com.nereusstream.api.AppendOutcome;
import com.nereusstream.api.AppendResult;
import com.nereusstream.api.AppendSession;
import com.nereusstream.api.Checksum;
import com.nereusstream.api.ChecksumType;
import com.nereusstream.api.DurabilityLevel;
import com.nereusstream.api.ErrorCode;
import com.nereusstream.api.NereusException;
import com.nereusstream.api.PayloadFormat;
import com.nereusstream.api.StreamId;
import com.nereusstream.api.target.BookKeeperEntryMapping;
import com.nereusstream.api.target.BookKeeperEntryRangeReadTarget;
import com.nereusstream.metadata.oxia.BookKeeperVersionedValue;
import com.nereusstream.metadata.oxia.BookKeeperWriterMetadataStore;
import com.nereusstream.metadata.oxia.CommitAppendRequest;
import com.nereusstream.metadata.oxia.CommittedAppend;
import com.nereusstream.metadata.oxia.OxiaMetadataStore;
import com.nereusstream.metadata.oxia.records.AppendReservationLifecycle;
import com.nereusstream.metadata.oxia.records.BookKeeperAppendReservationRecord;
import com.nereusstream.metadata.oxia.records.BookKeeperWriterLifecycle;
import java.time.Clock;
import java.time.Duration;
import java.util.Objects;
import java.util.Optional;
import java.util.concurrent.CompletableFuture;

/** Replays one payload-free durable BookKeeper reservation after process loss without another provider write. */
public final class BookKeeperAppendRecoveryCoordinator {
    private final String cluster;
    private final BookKeeperWalConfiguration configuration;
    private final BookKeeperWriterMetadataStore writerMetadata;
    private final OxiaMetadataStore l0;
    private final BookKeeperPrimaryPhysicalReferenceAdapter references;
    private final BookKeeperLedgerRecovery ledgerRecovery;
    private final Clock clock;

    public BookKeeperAppendRecoveryCoordinator(
            String cluster,
            BookKeeperWalConfiguration configuration,
            BookKeeperWriterMetadataStore writerMetadata,
            OxiaMetadataStore l0,
            BookKeeperPrimaryPhysicalReferenceAdapter references,
            BookKeeperLedgerRecovery ledgerRecovery,
            Clock clock) {
        this.cluster = text(cluster, "cluster");
        this.configuration = Objects.requireNonNull(configuration, "configuration");
        this.writerMetadata = Objects.requireNonNull(writerMetadata, "writerMetadata");
        this.l0 = Objects.requireNonNull(l0, "l0");
        this.references = Objects.requireNonNull(references, "references");
        this.ledgerRecovery = Objects.requireNonNull(ledgerRecovery, "ledgerRecovery");
        this.clock = Objects.requireNonNull(clock, "clock");
    }

    public CompletableFuture<AppendResult> recoverAfterRestart(
            AppendSession currentSession,
            AppendAttemptId attemptId,
            DurabilityLevel durability,
            Duration timeout) {
        AppendSession session = Objects.requireNonNull(currentSession, "currentSession");
        AppendAttemptId attempt = Objects.requireNonNull(attemptId, "attemptId");
        Objects.requireNonNull(durability, "durability");
        BookKeeperOperationDeadline deadline = new BookKeeperOperationDeadline(
                Objects.requireNonNull(timeout, "timeout"));
        String reservationId = BookKeeperAppendReservationIds.forAttempt(session.streamId(), attempt);
        return deadline.bound(l0.revalidateAppendSession(cluster, session))
                .thenCompose(ignored -> deadline.bound(writerMetadata.getReservation(
                        cluster, session.streamId(), reservationId)))
                .thenCompose(optional -> recoverReservation(
                        session,
                        attempt,
                        durability,
                        optional.orElseThrow(() -> failure(
                                ErrorCode.METADATA_CONDITION_FAILED,
                                false,
                                AppendOutcome.KNOWN_NOT_COMMITTED,
                                "BookKeeper append reservation is absent")),
                        deadline));
    }

    private CompletableFuture<AppendResult> recoverReservation(
            AppendSession session,
            AppendAttemptId attempt,
            DurabilityLevel durability,
            BookKeeperVersionedValue<BookKeeperAppendReservationRecord> reservation,
            BookKeeperOperationDeadline deadline) {
        BookKeeperAppendReservationRecord value = reservation.value();
        requireAttempt(value, session.streamId(), attempt);
        boolean sameSession = value.writerId().equals(session.writerId())
                && value.appendSessionEpoch() == session.epoch()
                && value.fencingTokenHash().equals(BookKeeperIdentityDigests.sha256(session.fencingToken()));
        if (!sameSession) {
            return rejectFencedReservation(session, reservation, deadline);
        }
        if (value.lifecycle() == AppendReservationLifecycle.ABANDONED) {
            return CompletableFuture.failedFuture(failure(
                    ErrorCode.OFFSET_CONFLICT,
                    false,
                    AppendOutcome.KNOWN_NOT_COMMITTED,
                    "BookKeeper append reservation was abandoned"));
        }
        if (value.lifecycle() == AppendReservationLifecycle.RESERVED
                || value.lifecycle() == AppendReservationLifecycle.WRITING) {
            return sealSelectedWriter(session, deadline, "recover non-durable append reservation")
                    .thenCompose(ignored -> CompletableFuture.failedFuture(failure(
                            ErrorCode.OFFSET_CONFLICT,
                            false,
                            AppendOutcome.KNOWN_NOT_COMMITTED,
                            "BookKeeper append did not reach durable range completion")));
        }
        return resumeDurable(session, value, durability, deadline)
                .thenCompose(committed -> sealSelectedWriter(
                                session, deadline, "seal recovered BookKeeper writer after restart")
                        .thenApply(ignored -> toResult(committed)));
    }

    private CompletableFuture<AppendResult> rejectFencedReservation(
            AppendSession newOwner,
            BookKeeperVersionedValue<BookKeeperAppendReservationRecord> reservation,
            BookKeeperOperationDeadline deadline) {
        AppendReservationLifecycle lifecycle = reservation.value().lifecycle();
        if (lifecycle == AppendReservationLifecycle.COMMIT_PREPARED
                || lifecycle == AppendReservationLifecycle.HEAD_COMMITTED) {
            return CompletableFuture.failedFuture(failure(
                    ErrorCode.FENCED_APPEND,
                    true,
                    AppendOutcome.MAY_HAVE_COMMITTED,
                    "fenced BookKeeper reservation already has stable-commit facts"));
        }
        return abandon(reservation, "append session was fenced")
                .thenCompose(ignored -> sealSelectedWriter(
                        newOwner, deadline, "fence old BookKeeper append reservation"))
                .thenCompose(ignored -> CompletableFuture.failedFuture(failure(
                        ErrorCode.FENCED_APPEND,
                        false,
                        AppendOutcome.KNOWN_NOT_COMMITTED,
                        "BookKeeper append reservation belongs to a fenced session")));
    }

    private CompletableFuture<CommittedAppend> resumeDurable(
            AppendSession session,
            BookKeeperAppendReservationRecord reservation,
            DurabilityLevel durability,
            BookKeeperOperationDeadline deadline) {
        if (durability != DurabilityLevel.WAL_DURABLE
                && durability != DurabilityLevel.WAL_DURABLE_AND_INDEX_COMMITTED) {
            return CompletableFuture.failedFuture(failure(
                    ErrorCode.UNSUPPORTED_DURABILITY_LEVEL,
                    false,
                    AppendOutcome.KNOWN_NOT_COMMITTED,
                    "unsupported BookKeeper recovery durability"));
        }
        CommitAppendRequest request = request(session, reservation);
        return deadline.bound(l0.prepareStableAppend(cluster, request))
                .thenCompose(prepared -> deadline.bound(references.protectBeforeHead(
                        prepared,
                        (BookKeeperEntryRangeReadTarget) request.readTarget(),
                        deadline.remaining())))
                .thenCompose(protectedAppend -> deadline.bound(l0.commitPreparedStableAppend(
                        cluster,
                        protectedAppend.prepared(),
                        protectedAppend.proof())))
                // Recovery is intentionally stronger than a fast-path WAL_DURABLE acknowledgement: always finish
                // generation zero so the terminal result remains readable after this process seals the old ledger.
                .thenCompose(stable -> deadline.bound(l0.materializeGenerationZero(
                        cluster, stable.reachableAppend())))
                .thenCompose(materialized -> deadline.bound(references.protectVisibleIndex(
                        materialized,
                        (BookKeeperEntryRangeReadTarget) request.readTarget(),
                        deadline.remaining())))
                .thenApply(protectedIndex -> protectedIndex.materialized().committedAppend());
    }

    private CompletableFuture<Void> sealSelectedWriter(
            AppendSession owner,
            BookKeeperOperationDeadline deadline,
            String reason) {
        return deadline.bound(writerMetadata.getWriter(cluster, owner.streamId())).thenCompose(optional -> {
            if (optional.isEmpty()) {
                return CompletableFuture.completedFuture(null);
            }
            BookKeeperWriterLifecycle lifecycle = optional.orElseThrow().value().lifecycle();
            if (lifecycle != BookKeeperWriterLifecycle.ACTIVE
                    && lifecycle != BookKeeperWriterLifecycle.RECOVERING) {
                return CompletableFuture.completedFuture(null);
            }
            return deadline.bound(ledgerRecovery.recoverWriter(owner, deadline.remaining(), reason))
                    .thenApply(ignored -> null);
        });
    }

    private CompletableFuture<BookKeeperVersionedValue<BookKeeperAppendReservationRecord>> abandon(
            BookKeeperVersionedValue<BookKeeperAppendReservationRecord> reservation,
            String reason) {
        BookKeeperAppendReservationRecord before = reservation.value();
        if (before.lifecycle() == AppendReservationLifecycle.ABANDONED) {
            return CompletableFuture.completedFuture(reservation);
        }
        BookKeeperAppendReservationRecord abandoned = new BookKeeperAppendReservationRecord(
                before.schemaVersion(), before.reservationId(), before.appendAttemptId(), before.streamId(),
                before.writerId(), before.writerRunIdHash(), before.appendSessionEpoch(), before.fencingTokenHash(),
                before.writerStateEpoch(), before.ledgerId(), before.ledgerRootEpoch(), before.ledgerRangeSlot(),
                before.firstEntryId(), before.entryCount(), before.rangeChecksumSha256(), before.expectedStartOffset(),
                before.payloadFormat(), before.recordCount(), before.logicalBytes(), before.physicalBytes(),
                before.schemaRefs(), before.projectionIdentity(), before.minEventTimeMillis(),
                before.maxEventTimeMillis(), AppendReservationLifecycle.ABANDONED, "", "", 0, "",
                before.createdAtMillis(), clock.millis(), text(reason, "reason"), 0);
        return writerMetadata.compareAndSetReservation(cluster, abandoned, reservation.metadataVersion());
    }

    private CommitAppendRequest request(
            AppendSession session,
            BookKeeperAppendReservationRecord reservation) {
        if (!reservation.projectionIdentity().equals(CommitAppendRequest.absentProjectionIdentity())) {
            throw failure(
                    ErrorCode.METADATA_INVARIANT_VIOLATION,
                    false,
                    AppendOutcome.MAY_HAVE_COMMITTED,
                    "BookKeeper recovery cannot reconstruct a non-absent projection");
        }
        BookKeeperEntryRangeReadTarget target = new BookKeeperEntryRangeReadTarget(
                1,
                configuration.clusterAlias(),
                reservation.ledgerId(),
                reservation.firstEntryId(),
                reservation.entryCount(),
                BookKeeperEntryMapping.ONE_NEREUS_ENTRY_PER_BOOKKEEPER_ENTRY,
                new Checksum(ChecksumType.SHA256, reservation.rangeChecksumSha256()));
        return new CommitAppendRequest(
                session.streamId(),
                reservation.writerId(),
                reservation.writerRunIdHash(),
                reservation.appendSessionEpoch(),
                session.fencingToken(),
                reservation.expectedStartOffset(),
                target,
                PayloadFormat.valueOf(reservation.payloadFormat()),
                reservation.recordCount(),
                reservation.entryCount(),
                reservation.logicalBytes(),
                reservation.schemaRefs(),
                reservation.minEventTimeMillis(),
                reservation.maxEventTimeMillis(),
                Optional.empty());
    }

    private static AppendResult toResult(CommittedAppend committed) {
        return new AppendResult(
                committed.streamId(),
                committed.range(),
                committed.range().endOffset(),
                committed.cumulativeSize(),
                committed.generation(),
                committed.readTarget(),
                committed.payloadFormat(),
                committed.recordCount(),
                committed.entryCount(),
                committed.logicalBytes(),
                committed.schemaRefs(),
                committed.projectionRef(),
                committed.commitVersion());
    }

    private static void requireAttempt(
            BookKeeperAppendReservationRecord reservation,
            StreamId stream,
            AppendAttemptId attempt) {
        if (!reservation.streamId().equals(stream.value())
                || !reservation.appendAttemptId().equals(attempt.value())
                || !reservation.reservationId().equals(BookKeeperAppendReservationIds.forAttempt(stream, attempt))) {
            throw failure(
                    ErrorCode.METADATA_INVARIANT_VIOLATION,
                    false,
                    AppendOutcome.MAY_HAVE_COMMITTED,
                    "BookKeeper reservation identity does not match the append attempt");
        }
    }

    private static NereusException failure(
            ErrorCode code,
            boolean retriable,
            AppendOutcome outcome,
            String message) {
        return new NereusException(code, retriable, message, outcome);
    }

    private static String text(String value, String field) {
        Objects.requireNonNull(value, field);
        if (value.isBlank()) throw new IllegalArgumentException(field + " cannot be blank");
        return value;
    }
}
