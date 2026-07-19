/* Licensed under the Apache License, Version 2.0 */
package com.nereusstream.bookkeeper;

import com.nereusstream.api.AppendSession;
import com.nereusstream.api.ErrorCode;
import com.nereusstream.api.NereusException;
import com.nereusstream.api.StreamId;
import com.nereusstream.metadata.oxia.BookKeeperVersionedValue;
import com.nereusstream.metadata.oxia.BookKeeperMetadataConditionFailedException;
import com.nereusstream.metadata.oxia.BookKeeperWriterMetadataStore;
import com.nereusstream.metadata.oxia.records.BookKeeperLedgerLifecycle;
import com.nereusstream.metadata.oxia.records.BookKeeperLedgerRootRecord;
import com.nereusstream.metadata.oxia.records.BookKeeperWriterLifecycle;
import com.nereusstream.metadata.oxia.records.BookKeeperWriterStateRecord;
import com.nereusstream.metadata.oxia.records.LedgerAllocationIntentRecord;
import java.time.Clock;
import java.util.Objects;
import java.util.concurrent.CompletableFuture;

/** Exact stream-writer CAS state machine; it owns no BookKeeper handle or provider IO. */
public final class BookKeeperWriterStateMachine {
    private static final int MAX_ADOPTION_ATTEMPTS = 64;

    private final String cluster;
    private final BookKeeperWalConfiguration configuration;
    private final BookKeeperWriterMetadataStore metadata;
    private final Clock clock;
    private final String writerRunIdHash;
    private final String configurationBindingSha256;

    public BookKeeperWriterStateMachine(
            String cluster,
            BookKeeperWalConfiguration configuration,
            BookKeeperWriterMetadataStore metadata,
            Clock clock,
            String writerRunId) {
        this.cluster = text(cluster, "cluster");
        this.configuration = Objects.requireNonNull(configuration, "configuration");
        this.metadata = Objects.requireNonNull(metadata, "metadata");
        this.clock = Objects.requireNonNull(clock, "clock");
        this.writerRunIdHash = BookKeeperIdentityDigests.sha256(text(writerRunId, "writerRunId"));
        this.configurationBindingSha256 = configuration.configurationBindingSha256().value();
    }

    public CompletableFuture<BookKeeperVersionedValue<BookKeeperWriterStateRecord>> requireIdle(
            AppendSession session) {
        return requireIdle(checkedSession(session), 0);
    }

    public CompletableFuture<BookKeeperVersionedValue<BookKeeperWriterStateRecord>> claimAllocation(
            BookKeeperVersionedValue<BookKeeperWriterStateRecord> observed,
            AppendSession session,
            String allocationId,
            long candidateLedgerId) {
        BookKeeperVersionedValue<BookKeeperWriterStateRecord> current = Objects.requireNonNull(observed, "observed");
        AppendSession exactSession = checkedSession(session);
        BookKeeperWriterStateRecord before = current.value();
        requireProfile(before);
        requireSession(before, exactSession);
        if (before.lifecycle() != BookKeeperWriterLifecycle.IDLE) {
            return failed(ErrorCode.METADATA_CONDITION_FAILED, "BookKeeper writer is not IDLE");
        }
        long now = clock.millis();
        long segment = before.nextSegmentSequence();
        BookKeeperWriterStateRecord replacement = record(
                before, BookKeeperWriterLifecycle.ALLOCATING, before.writerStateEpoch() + 1,
                Math.addExact(segment, 1), text(allocationId, "allocationId"), candidateLedgerId,
                0, 0, 0, 0, 0, 0, "", now, "");
        return metadata.compareAndSetWriter(cluster, replacement, current.metadataVersion());
    }

    public CompletableFuture<BookKeeperVersionedValue<BookKeeperWriterStateRecord>> activate(
            BookKeeperVersionedValue<BookKeeperWriterStateRecord> observed,
            AppendSession session,
            LedgerAllocationIntentRecord allocation,
            BookKeeperLedgerRootRecord root) {
        BookKeeperVersionedValue<BookKeeperWriterStateRecord> current = Objects.requireNonNull(observed, "observed");
        AppendSession exactSession = checkedSession(session);
        LedgerAllocationIntentRecord exactAllocation = Objects.requireNonNull(allocation, "allocation");
        BookKeeperLedgerRootRecord exactRoot = Objects.requireNonNull(root, "root");
        BookKeeperWriterStateRecord before = current.value();
        requireProfile(before);
        requireSession(before, exactSession);
        if (before.lifecycle() != BookKeeperWriterLifecycle.ALLOCATING
                || !before.allocationId().equals(exactAllocation.allocationId())
                || before.allocationLedgerId() != exactAllocation.candidateLedgerId()
                || exactRoot.lifecycle() != BookKeeperLedgerLifecycle.ACTIVE
                || exactRoot.ledgerId() != exactAllocation.candidateLedgerId()
                || !exactRoot.allocationId().equals(exactAllocation.allocationId())) {
            return failed(ErrorCode.METADATA_CONDITION_FAILED,
                    "BookKeeper allocation/root no longer matches the writer claim");
        }
        long now = clock.millis();
        BookKeeperWriterStateRecord replacement = record(
                before, BookKeeperWriterLifecycle.ACTIVE, before.writerStateEpoch() + 1,
                before.nextSegmentSequence(), "", 0, exactAllocation.segmentSequence(), exactRoot.ledgerId(),
                exactRoot.lifecycleEpoch(), 0, 0, 0, "", now, "");
        return metadata.compareAndSetWriter(cluster, replacement, current.metadataVersion());
    }

    public CompletableFuture<BookKeeperVersionedValue<BookKeeperWriterStateRecord>> detachAllocation(
            BookKeeperVersionedValue<BookKeeperWriterStateRecord> observed,
            AppendSession session,
            String allocationId,
            String reason) {
        BookKeeperVersionedValue<BookKeeperWriterStateRecord> current = Objects.requireNonNull(observed, "observed");
        AppendSession exactSession = checkedSession(session);
        BookKeeperWriterStateRecord before = current.value();
        requireProfile(before);
        requireSession(before, exactSession);
        if (before.lifecycle() != BookKeeperWriterLifecycle.ALLOCATING
                || !before.allocationId().equals(allocationId)) {
            return failed(ErrorCode.METADATA_CONDITION_FAILED,
                    "BookKeeper writer no longer selects the allocation being detached");
        }
        long now = clock.millis();
        BookKeeperWriterStateRecord replacement = record(
                before, BookKeeperWriterLifecycle.IDLE, before.writerStateEpoch() + 1,
                before.nextSegmentSequence(), "", 0, 0, 0, 0, 0, 0, 0, "", now,
                text(reason, "reason"));
        return metadata.compareAndSetWriter(cluster, replacement, current.metadataVersion());
    }

    public CompletableFuture<BookKeeperVersionedValue<BookKeeperWriterStateRecord>> reserveRange(
            BookKeeperVersionedValue<BookKeeperWriterStateRecord> observed,
            AppendSession session,
            String reservationId,
            int entryCount,
            long physicalBytes) {
        if (entryCount <= 0 || physicalBytes < 0) {
            throw new IllegalArgumentException("reserved entry count must be positive and bytes non-negative");
        }
        BookKeeperVersionedValue<BookKeeperWriterStateRecord> current = Objects.requireNonNull(observed, "observed");
        BookKeeperWriterStateRecord before = current.value();
        requireProfile(before);
        requireSession(before, checkedSession(session));
        if (before.lifecycle() != BookKeeperWriterLifecycle.ACTIVE || !before.activeReservationId().isEmpty()) {
            return failed(ErrorCode.METADATA_CONDITION_FAILED,
                    "BookKeeper writer is not available for one serialized range reservation");
        }
        BookKeeperWriterStateRecord replacement = record(
                before, BookKeeperWriterLifecycle.ACTIVE, before.writerStateEpoch() + 1,
                before.nextSegmentSequence(), "", 0, before.activeSegmentSequence(), before.activeLedgerId(),
                before.activeLedgerRootEpoch(), Math.addExact(before.nextEntryId(), entryCount),
                Math.addExact(before.activePhysicalBytes(), physicalBytes),
                Math.addExact(before.activeAppendRangeCount(), 1), text(reservationId, "reservationId"),
                clock.millis(), "");
        return metadata.compareAndSetWriter(cluster, replacement, current.metadataVersion());
    }

    public CompletableFuture<BookKeeperVersionedValue<BookKeeperWriterStateRecord>> clearRangeReservation(
            BookKeeperVersionedValue<BookKeeperWriterStateRecord> observed,
            AppendSession session,
            String reservationId) {
        BookKeeperVersionedValue<BookKeeperWriterStateRecord> current = Objects.requireNonNull(observed, "observed");
        BookKeeperWriterStateRecord before = current.value();
        requireProfile(before);
        requireSession(before, checkedSession(session));
        if (before.lifecycle() != BookKeeperWriterLifecycle.ACTIVE
                || !before.activeReservationId().equals(reservationId)) {
            return failed(ErrorCode.METADATA_CONDITION_FAILED,
                    "BookKeeper writer no longer selects the completed range reservation");
        }
        BookKeeperWriterStateRecord replacement = record(
                before, BookKeeperWriterLifecycle.ACTIVE, before.writerStateEpoch() + 1,
                before.nextSegmentSequence(), "", 0, before.activeSegmentSequence(), before.activeLedgerId(),
                before.activeLedgerRootEpoch(), before.nextEntryId(), before.activePhysicalBytes(),
                before.activeAppendRangeCount(), "", clock.millis(), "");
        return metadata.compareAndSetWriter(cluster, replacement, current.metadataVersion());
    }

    public CompletableFuture<BookKeeperVersionedValue<BookKeeperWriterStateRecord>> beginRecovery(
            BookKeeperVersionedValue<BookKeeperWriterStateRecord> observed,
            AppendSession session,
            String reason) {
        BookKeeperVersionedValue<BookKeeperWriterStateRecord> current = Objects.requireNonNull(observed, "observed");
        BookKeeperWriterStateRecord before = current.value();
        requireProfile(before);
        requireSession(before, checkedSession(session));
        if (before.lifecycle() != BookKeeperWriterLifecycle.ACTIVE) {
            return failed(ErrorCode.METADATA_CONDITION_FAILED, "BookKeeper writer is not ACTIVE");
        }
        BookKeeperWriterStateRecord replacement = record(
                before, BookKeeperWriterLifecycle.RECOVERING, before.writerStateEpoch() + 1,
                before.nextSegmentSequence(), "", 0, before.activeSegmentSequence(), before.activeLedgerId(),
                before.activeLedgerRootEpoch(), before.nextEntryId(), before.activePhysicalBytes(),
                before.activeAppendRangeCount(), before.activeReservationId(), clock.millis(), text(reason, "reason"));
        return metadata.compareAndSetWriter(cluster, replacement, current.metadataVersion());
    }

    public CompletableFuture<BookKeeperVersionedValue<BookKeeperWriterStateRecord>> adoptForRecovery(
            BookKeeperVersionedValue<BookKeeperWriterStateRecord> observed,
            AppendSession session,
            String reason) {
        BookKeeperVersionedValue<BookKeeperWriterStateRecord> current = Objects.requireNonNull(observed, "observed");
        AppendSession exactSession = checkedSession(session);
        BookKeeperWriterStateRecord before = current.value();
        requireProfile(before);
        requireSessionNotOlder(before, exactSession);
        if (before.lifecycle() != BookKeeperWriterLifecycle.ACTIVE
                && before.lifecycle() != BookKeeperWriterLifecycle.RECOVERING) {
            return failed(ErrorCode.METADATA_CONDITION_FAILED,
                    "only ACTIVE/RECOVERING BookKeeper writers can transfer recovery ownership");
        }
        BookKeeperWriterStateRecord replacement = new BookKeeperWriterStateRecord(
                before.schemaVersion(), before.streamId(), before.clusterAlias(),
                before.configurationBindingSha256(), BookKeeperWriterLifecycle.RECOVERING,
                before.writerStateEpoch() + 1, exactSession.writerId(), writerRunIdHash, exactSession.epoch(),
                BookKeeperIdentityDigests.sha256(exactSession.fencingToken()), exactSession.leaseVersion(),
                before.nextSegmentSequence(), "", 0, before.activeSegmentSequence(), before.activeLedgerId(),
                before.activeLedgerRootEpoch(), before.nextEntryId(), before.activePhysicalBytes(),
                before.activeAppendRangeCount(), before.activeReservationId(), before.openedAtMillis(),
                clock.millis(), text(reason, "reason"), 0);
        return metadata.compareAndSetWriter(cluster, replacement, current.metadataVersion());
    }

    public CompletableFuture<BookKeeperVersionedValue<BookKeeperWriterStateRecord>> finishRecovery(
            BookKeeperVersionedValue<BookKeeperWriterStateRecord> observed,
            AppendSession session,
            String reason) {
        BookKeeperVersionedValue<BookKeeperWriterStateRecord> current = Objects.requireNonNull(observed, "observed");
        BookKeeperWriterStateRecord before = current.value();
        requireProfile(before);
        requireSession(before, checkedSession(session));
        if (before.lifecycle() != BookKeeperWriterLifecycle.RECOVERING) {
            return failed(ErrorCode.METADATA_CONDITION_FAILED, "BookKeeper writer is not RECOVERING");
        }
        BookKeeperWriterStateRecord replacement = record(
                before, BookKeeperWriterLifecycle.IDLE, before.writerStateEpoch() + 1,
                before.nextSegmentSequence(), "", 0, 0, 0, 0, 0, 0, 0, "", clock.millis(), text(reason, "reason"));
        return metadata.compareAndSetWriter(cluster, replacement, current.metadataVersion());
    }

    public void requireCurrentActive(
            BookKeeperWriterStateRecord writer, AppendSession session, long ledgerId, long rootEpoch) {
        BookKeeperWriterStateRecord value = Objects.requireNonNull(writer, "writer");
        requireProfile(value);
        requireSession(value, checkedSession(session));
        if (value.lifecycle() != BookKeeperWriterLifecycle.ACTIVE
                || value.activeLedgerId() != ledgerId
                || value.activeLedgerRootEpoch() != rootEpoch) {
            throw new NereusException(ErrorCode.FENCED_APPEND, false,
                    "BookKeeper ledger is not active under the supplied writer session");
        }
    }

    private CompletableFuture<BookKeeperVersionedValue<BookKeeperWriterStateRecord>> requireIdle(
            AppendSession session, int attempt) {
        if (attempt >= MAX_ADOPTION_ATTEMPTS) {
            return failed(ErrorCode.METADATA_CONDITION_FAILED,
                    "BookKeeper writer adoption exceeded its retry bound");
        }
        StreamId stream = session.streamId();
        return metadata.getWriter(cluster, stream).thenCompose(optional -> {
            if (optional.isEmpty()) {
                return metadata.createWriter(cluster, idle(session, 1, 0, clock.millis()));
            }
            BookKeeperVersionedValue<BookKeeperWriterStateRecord> current = optional.orElseThrow();
            BookKeeperWriterStateRecord before = current.value();
            requireProfile(before);
            if (before.lifecycle() != BookKeeperWriterLifecycle.IDLE) {
                return failed(ErrorCode.FENCED_APPEND, "BookKeeper writer requires recovery before allocation");
            }
            requireSessionNotOlder(before, session);
            if (sameOwner(before, session)) {
                return CompletableFuture.completedFuture(current);
            }
            BookKeeperWriterStateRecord replacement = idle(
                    session, before.writerStateEpoch() + 1, before.nextSegmentSequence(), clock.millis());
            return metadata.compareAndSetWriter(cluster, replacement, current.metadataVersion());
        }).exceptionallyCompose(failure -> {
            Throwable cause = unwrap(failure);
            if (cause instanceof BookKeeperMetadataConditionFailedException) {
                return requireIdle(session, attempt + 1);
            }
            return CompletableFuture.failedFuture(cause);
        });
    }

    private BookKeeperWriterStateRecord idle(
            AppendSession session, long writerEpoch, long nextSegmentSequence, long now) {
        return new BookKeeperWriterStateRecord(1, session.streamId().value(), configuration.clusterAlias(),
                configurationBindingSha256, BookKeeperWriterLifecycle.IDLE, writerEpoch, session.writerId(),
                writerRunIdHash, session.epoch(), BookKeeperIdentityDigests.sha256(session.fencingToken()),
                session.leaseVersion(), nextSegmentSequence, "", 0, 0, 0, 0, 0, 0, 0,
                "", now, now, "", 0);
    }

    private BookKeeperWriterStateRecord record(
            BookKeeperWriterStateRecord before,
            BookKeeperWriterLifecycle lifecycle,
            long writerStateEpoch,
            long nextSegmentSequence,
            String allocationId,
            long allocationLedgerId,
            long activeSegmentSequence,
            long activeLedgerId,
            long activeLedgerRootEpoch,
            long nextEntryId,
            long activePhysicalBytes,
            int activeAppendRangeCount,
            String activeReservationId,
            long updatedAtMillis,
            String stateReason) {
        return new BookKeeperWriterStateRecord(1, before.streamId(), before.clusterAlias(),
                before.configurationBindingSha256(), lifecycle, writerStateEpoch, before.writerId(),
                before.writerRunIdHash(), before.appendSessionEpoch(), before.fencingTokenHash(),
                before.appendSessionLeaseVersion(), nextSegmentSequence, allocationId, allocationLedgerId,
                activeSegmentSequence, activeLedgerId, activeLedgerRootEpoch, nextEntryId, activePhysicalBytes,
                activeAppendRangeCount, activeReservationId, before.openedAtMillis(), updatedAtMillis,
                stateReason, 0);
    }

    private void requireProfile(BookKeeperWriterStateRecord writer) {
        if (!writer.clusterAlias().equals(configuration.clusterAlias())
                || !writer.configurationBindingSha256().equals(configurationBindingSha256)) {
            throw new NereusException(ErrorCode.METADATA_INVARIANT_VIOLATION, false,
                    "BookKeeper writer profile/configuration binding drifted");
        }
    }

    private void requireSession(BookKeeperWriterStateRecord writer, AppendSession session) {
        requireSessionNotOlder(writer, session);
        if (!writer.streamId().equals(session.streamId().value()) || !sameOwner(writer, session)) {
            throw new NereusException(ErrorCode.FENCED_APPEND, false,
                    "BookKeeper writer is owned by another append session or process run");
        }
    }

    private void requireSessionNotOlder(BookKeeperWriterStateRecord writer, AppendSession session) {
        String tokenHash = BookKeeperIdentityDigests.sha256(session.fencingToken());
        if (!writer.streamId().equals(session.streamId().value())
                || session.epoch() < writer.appendSessionEpoch()
                || (session.epoch() == writer.appendSessionEpoch()
                && (!session.writerId().equals(writer.writerId())
                || !tokenHash.equals(writer.fencingTokenHash())
                || session.leaseVersion() < writer.appendSessionLeaseVersion()))) {
            throw new NereusException(ErrorCode.FENCED_APPEND, false,
                    "stale append session cannot own the BookKeeper writer");
        }
    }

    private boolean sameOwner(BookKeeperWriterStateRecord writer, AppendSession session) {
        return writer.writerId().equals(session.writerId())
                && writer.writerRunIdHash().equals(writerRunIdHash)
                && writer.appendSessionEpoch() == session.epoch()
                && writer.fencingTokenHash().equals(BookKeeperIdentityDigests.sha256(session.fencingToken()))
                && writer.appendSessionLeaseVersion() == session.leaseVersion();
    }

    private static AppendSession checkedSession(AppendSession session) {
        return Objects.requireNonNull(session, "session");
    }

    private static Throwable unwrap(Throwable failure) {
        Throwable current = failure;
        while ((current instanceof java.util.concurrent.CompletionException
                || current instanceof java.util.concurrent.ExecutionException) && current.getCause() != null) {
            current = current.getCause();
        }
        return current;
    }

    private static <T> CompletableFuture<T> failed(ErrorCode code, String message) {
        return CompletableFuture.failedFuture(new NereusException(code, false, message));
    }

    private static String text(String value, String name) {
        Objects.requireNonNull(value, name);
        if (value.isBlank()) throw new IllegalArgumentException(name + " cannot be blank");
        return value;
    }
}
