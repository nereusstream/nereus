/* Licensed under the Apache License, Version 2.0 */
package com.nereusstream.bookkeeper;

import com.nereusstream.api.AppendSession;
import com.nereusstream.api.Checksum;
import com.nereusstream.api.ErrorCode;
import com.nereusstream.api.NereusException;
import com.nereusstream.api.StreamId;
import com.nereusstream.api.target.BookKeeperEntryMapping;
import com.nereusstream.api.target.BookKeeperEntryRangeReadTarget;
import com.nereusstream.api.target.ReadTargetType;
import com.nereusstream.core.wal.DurablePrimaryAppend;
import com.nereusstream.core.wal.PrimaryAppendRequest;
import com.nereusstream.core.wal.PrimaryWalAppender;
import com.nereusstream.metadata.oxia.BookKeeperLedgerMetadataStore;
import com.nereusstream.metadata.oxia.BookKeeperVersionedValue;
import com.nereusstream.metadata.oxia.BookKeeperWriterMetadataStore;
import com.nereusstream.metadata.oxia.CommitAppendRequest;
import com.nereusstream.metadata.oxia.records.AppendReservationLifecycle;
import com.nereusstream.metadata.oxia.records.BookKeeperAppendReservationRecord;
import com.nereusstream.metadata.oxia.records.BookKeeperLedgerLifecycle;
import com.nereusstream.metadata.oxia.records.BookKeeperLedgerProtectionRecord;
import com.nereusstream.metadata.oxia.records.BookKeeperLedgerRootRecord;
import com.nereusstream.metadata.oxia.records.BookKeeperProtectionType;
import com.nereusstream.metadata.oxia.records.BookKeeperWriterLifecycle;
import com.nereusstream.metadata.oxia.records.BookKeeperWriterStateRecord;
import com.nereusstream.metadata.oxia.records.ProtectionLifecycle;
import io.netty.buffer.ByteBuf;
import java.time.Clock;
import java.time.Duration;
import java.util.ArrayDeque;
import java.util.List;
import java.util.Objects;
import java.util.Optional;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.atomic.AtomicBoolean;

/** BOOKKEEPER_WAL_ONLY primary appender with pre-write range reservation and exact-id writes. */
public final class BookKeeperPrimaryWalAppender
        implements PrimaryWalAppender<BookKeeperPreparedPrimaryAppend>, AutoCloseable {
    private final String cluster;
    private final BookKeeperWalConfiguration configuration;
    private final BookKeeperWriterMetadataStore writerMetadata;
    private final BookKeeperLedgerMetadataStore ledgerMetadata;
    private final BookKeeperLedgerAllocator allocator;
    private final BookKeeperLedgerRecovery recovery;
    private final BookKeeperWriterStateMachine writerState;
    private final BookKeeperClientOperations client;
    private final Clock clock;
    private final ConcurrentHashMap<StreamId, ActiveLedger> activeLedgers = new ConcurrentHashMap<>();
    private final AtomicBoolean closed = new AtomicBoolean();

    public BookKeeperPrimaryWalAppender(
            String cluster,
            BookKeeperWalConfiguration configuration,
            BookKeeperWriterMetadataStore writerMetadata,
            BookKeeperLedgerMetadataStore ledgerMetadata,
            BookKeeperLedgerAllocator allocator,
            BookKeeperLedgerRecovery recovery,
            BookKeeperWriterStateMachine writerState,
            BookKeeperClientOperations client,
            Clock clock) {
        this.cluster = text(cluster, "cluster");
        this.configuration = Objects.requireNonNull(configuration, "configuration");
        this.writerMetadata = Objects.requireNonNull(writerMetadata, "writerMetadata");
        this.ledgerMetadata = Objects.requireNonNull(ledgerMetadata, "ledgerMetadata");
        this.allocator = Objects.requireNonNull(allocator, "allocator");
        this.recovery = Objects.requireNonNull(recovery, "recovery");
        this.writerState = Objects.requireNonNull(writerState, "writerState");
        this.client = Objects.requireNonNull(client, "client");
        this.clock = Objects.requireNonNull(clock, "clock");
    }

    @Override
    public ReadTargetType targetType() {
        return ReadTargetType.BOOKKEEPER_ENTRY_RANGE;
    }

    @Override
    public Class<BookKeeperPreparedPrimaryAppend> preparedClass() {
        return BookKeeperPreparedPrimaryAppend.class;
    }

    @Override
    public BookKeeperPreparedPrimaryAppend prepare(PrimaryAppendRequest request) {
        ensureOpen();
        BookKeeperPreparedPrimaryAppend prepared = new BookKeeperPreparedPrimaryAppend(
                Objects.requireNonNull(request, "request"));
        try {
            validateBatchBound(prepared);
            return prepared;
        } catch (Throwable failure) {
            prepared.close();
            throw failure;
        }
    }

    @Override
    public CompletableFuture<DurablePrimaryAppend> persist(
            BookKeeperPreparedPrimaryAppend prepared, Duration timeout) {
        ensureOpen();
        BookKeeperPreparedPrimaryAppend exact = Objects.requireNonNull(prepared, "prepared");
        Duration budget = min(Objects.requireNonNull(timeout, "timeout"), configuration.operationTimeout());
        validateBatchBound(exact);
        BookKeeperOperationDeadline deadline = new BookKeeperOperationDeadline(budget);
        return ensureActive(exact.request(), deadline)
                .thenCompose(active -> rolloverIfRequired(active, exact, deadline))
                .thenCompose(active -> persistRange(active, exact, deadline));
    }

    @Override
    public CompletableFuture<Void> validateBeforeHeadCommit(
            DurablePrimaryAppend append, AppendSession session, Duration timeout) {
        ensureOpen();
        DurablePrimaryAppend durable = Objects.requireNonNull(append, "append");
        AppendSession exactSession = Objects.requireNonNull(session, "session");
        if (!(durable.readTarget() instanceof BookKeeperEntryRangeReadTarget target)
                || !(durable.providerToken() instanceof BookKeeperProviderAppendToken token)
                || target.ledgerId() != token.ledgerId()
                || target.firstEntryId() != token.firstEntryId()
                || target.entryCount() != token.entryCount()
                || !target.rangeChecksum().equals(token.rangeChecksum())) {
            return CompletableFuture.failedFuture(new NereusException(
                    ErrorCode.METADATA_INVARIANT_VIOLATION, false,
                    "BookKeeper durable append target/token identity mismatch"));
        }
        BookKeeperOperationDeadline deadline = new BookKeeperOperationDeadline(
                min(Objects.requireNonNull(timeout, "timeout"), configuration.operationTimeout()));
        var writerFuture = writerMetadata.getWriter(cluster, durable.streamId());
        var reservationFuture = writerMetadata.getReservation(cluster, durable.streamId(), token.reservationId());
        var rootFuture = ledgerMetadata.getRoot(
                cluster, configuration.providerScopeSha256(), token.ledgerId());
        @SuppressWarnings("unchecked")
        CompletableFuture<Optional<BookKeeperVersionedValue<BookKeeperLedgerProtectionRecord>>>[] protections =
                new CompletableFuture[3];
        for (int slot = 0; slot < protections.length; slot++) {
            protections[slot] = ledgerMetadata.getProtection(
                    cluster, configuration.providerScopeSha256(), token.ledgerId(), token.ledgerRangeSlot(), slot);
        }
        CompletableFuture<?>[] all = new CompletableFuture<?>[] {
            writerFuture, reservationFuture, rootFuture, protections[0], protections[1], protections[2]
        };
        return deadline.bound(CompletableFuture.allOf(all)).thenApply(ignored -> {
            BookKeeperVersionedValue<BookKeeperWriterStateRecord> writer = writerFuture.join().orElseThrow(
                    () -> invariant("BookKeeper writer disappeared before head commit"));
            BookKeeperVersionedValue<BookKeeperAppendReservationRecord> reservation =
                    reservationFuture.join().orElseThrow(
                            () -> invariant("BookKeeper reservation disappeared before head commit"));
            BookKeeperVersionedValue<BookKeeperLedgerRootRecord> root = rootFuture.join().orElseThrow(
                    () -> invariant("BookKeeper root disappeared before head commit"));
            writerState.requireCurrentActive(writer.value(), exactSession, token.ledgerId(), token.ledgerRootEpoch());
            if (writer.metadataVersion() != token.writerMetadataVersion()
                    || reservation.metadataVersion() != token.reservationMetadataVersion()
                    || root.metadataVersion() != token.rootMetadataVersion()
                    || reservation.value().lifecycle() != AppendReservationLifecycle.DURABLE
                    || root.value().lifecycle() != BookKeeperLedgerLifecycle.ACTIVE) {
                throw invariant("BookKeeper append metadata changed before head commit");
            }
            for (int slot = 0; slot < protections.length; slot++) {
                BookKeeperLedgerProtectionRecord protection = protections[slot].join().orElseThrow(
                        () -> invariant("mandatory BookKeeper range protection is absent")).value();
                if (protection.ledgerId() != token.ledgerId()
                        || protection.ledgerRangeSlot() != token.ledgerRangeSlot()
                        || protection.protectionSlot() != slot
                        || (slot == 2 && protection.lifecycle() != ProtectionLifecycle.ACTIVE)
                        || (slot != 2 && protection.lifecycle() != ProtectionLifecycle.RESERVED)) {
                    throw invariant("mandatory BookKeeper range protection identity drifted");
                }
            }
            return null;
        });
    }

    private CompletableFuture<ActiveLedger> ensureActive(
            PrimaryAppendRequest request, BookKeeperOperationDeadline deadline) {
        ActiveLedger cached = activeLedgers.get(request.streamId());
        if (cached != null) {
            try {
                writerState.requireCurrentActive(cached.writer().value(), request.session(),
                        cached.root().value().ledgerId(), cached.root().value().lifecycleEpoch());
                return CompletableFuture.completedFuture(cached);
            } catch (NereusException stale) {
                if (stale.code() != ErrorCode.FENCED_APPEND) {
                    return CompletableFuture.failedFuture(stale);
                }
                return recoverAndAllocate(request, cached, deadline, "cached writer ownership changed");
            }
        }
        return writerMetadata.getWriter(cluster, request.streamId()).thenCompose(optional -> {
            if (optional.isEmpty() || optional.orElseThrow().value().lifecycle() == BookKeeperWriterLifecycle.IDLE) {
                return allocate(request, deadline);
            }
            return recoverAndAllocate(request, null, deadline, "cold-start active writer recovery");
        });
    }

    private CompletableFuture<ActiveLedger> rolloverIfRequired(
            ActiveLedger active,
            BookKeeperPreparedPrimaryAppend prepared,
            BookKeeperOperationDeadline deadline) {
        if (!requiresRollover(active, prepared)) {
            return CompletableFuture.completedFuture(active);
        }
        return recoverAndAllocate(prepared.request(), active, deadline, "configured ledger rollover");
    }

    private CompletableFuture<ActiveLedger> recoverAndAllocate(
            PrimaryAppendRequest request,
            ActiveLedger cached,
            BookKeeperOperationDeadline deadline,
            String reason) {
        if (cached != null) {
            activeLedgers.remove(request.streamId(), cached);
            cached.handle().closeAsync();
        }
        return recovery.recoverWriter(request.session(), deadline.remaining(), reason)
                .thenCompose(ignored -> allocate(request, deadline));
    }

    private CompletableFuture<ActiveLedger> allocate(
            PrimaryAppendRequest request, BookKeeperOperationDeadline deadline) {
        return allocator.allocate(new BookKeeperLedgerAllocationRequest(
                        request.streamId(), request.session(), deadline.remaining()))
                .thenApply(allocated -> {
                    ActiveLedger active = new ActiveLedger(
                            allocated.handle(), allocated.writer(), allocated.root(), allocated.customMetadata());
                    ActiveLedger previous = activeLedgers.put(request.streamId(), active);
                    if (previous != null && previous.handle() != active.handle()) {
                        previous.handle().closeAsync();
                    }
                    return active;
                });
    }

    private CompletableFuture<DurablePrimaryAppend> persistRange(
            ActiveLedger active,
            BookKeeperPreparedPrimaryAppend prepared,
            BookKeeperOperationDeadline deadline) {
        BookKeeperWriterStateRecord writer = active.writer().value();
        long firstEntryId = writer.nextEntryId();
        int rangeSlot = writer.activeAppendRangeCount();
        Checksum rangeChecksum = prepared.rangeChecksum(firstEntryId);
        String reservationId = BookKeeperIdentityDigests.sha256(
                "NBKR-RESERVATION\0" + prepared.request().attemptId().value() + "\0"
                        + active.root().value().ledgerId() + "\0" + firstEntryId + "\0" + prepared.entryCount());
        BookKeeperAppendReservationRecord desired = reservation(
                active, prepared, reservationId, firstEntryId, rangeSlot, rangeChecksum);
        return writerMetadata.createReservation(cluster, desired).thenCompose(reservation ->
                writerState.reserveRange(active.writer(), prepared.request().session(), reservationId,
                                prepared.entryCount(), prepared.physicalBytes())
                        .handle((reservedWriter, reserveFailure) -> {
                            if (reserveFailure != null) {
                                return abandonWithoutSeal(reservation, unwrap(reserveFailure));
                            }
                            ActiveLedger reservedActive = active.withWriter(reservedWriter);
                            activeLedgers.replace(prepared.streamId(), active, reservedActive);
                            CompletableFuture<DurablePrimaryAppend> pipeline = createMandatoryProtections(
                                            reservedActive, prepared, reservation)
                                    .thenCompose(ignored -> casReservation(
                                            reservation, AppendReservationLifecycle.WRITING, ""))
                                    .thenCompose(writing -> writeEntries(
                                                    reservedActive.handle(), firstEntryId,
                                                    prepared.retainedEntries(), deadline)
                                            .thenCompose(ignored -> casReservation(
                                                    writing, AppendReservationLifecycle.DURABLE, "")))
                                    .thenCompose(durableReservation -> activateAppendRecoveryProtection(
                                                    reservedActive, durableReservation)
                                            .thenApply(ignored -> durableReservation))
                                    .thenCompose(durableReservation -> writerState.clearRangeReservation(
                                                    reservedWriter, prepared.request().session(), reservationId)
                                            .thenApply(clearedWriter -> durableAppend(
                                                    reservedActive.withWriter(clearedWriter), prepared,
                                                    durableReservation, firstEntryId, rangeSlot, rangeChecksum)));
                            return pipeline.handle((durable, failure) -> {
                                if (failure == null) {
                                    return CompletableFuture.completedFuture(durable);
                                }
                                return failAndSeal(
                                        reservedActive, prepared, reservation, unwrap(failure), deadline);
                            }).thenCompose(java.util.function.Function.identity());
                        }).thenCompose(java.util.function.Function.identity()));
    }

    private CompletableFuture<Void> createMandatoryProtections(
            ActiveLedger active,
            BookKeeperPreparedPrimaryAppend prepared,
            BookKeeperVersionedValue<BookKeeperAppendReservationRecord> reservation) {
        CompletableFuture<Void> chain = CompletableFuture.completedFuture(null);
        BookKeeperProtectionType[] types = {
            BookKeeperProtectionType.REACHABLE_APPEND,
            BookKeeperProtectionType.VISIBLE_GENERATION,
            BookKeeperProtectionType.APPEND_RECOVERY
        };
        for (int slot = 0; slot < types.length; slot++) {
            int exactSlot = slot;
            chain = chain.thenCompose(ignored -> ledgerMetadata.createProtection(
                            cluster, configuration.providerScopeSha256(),
                            reservedProtection(active, prepared, reservation.value(), exactSlot, types[exactSlot]))
                    .thenApply(created -> null));
        }
        return chain;
    }

    private CompletableFuture<BookKeeperVersionedValue<BookKeeperLedgerProtectionRecord>>
            activateAppendRecoveryProtection(
                    ActiveLedger active,
                    BookKeeperVersionedValue<BookKeeperAppendReservationRecord> reservation) {
        return ledgerMetadata.getProtection(
                        cluster,
                        configuration.providerScopeSha256(),
                        active.root().value().ledgerId(),
                        reservation.value().ledgerRangeSlot(),
                        2)
                .thenCompose(optional -> {
                    BookKeeperVersionedValue<BookKeeperLedgerProtectionRecord> current = optional.orElseThrow(
                            () -> invariant("APPEND_RECOVERY protection disappeared before activation"));
                    BookKeeperLedgerProtectionRecord before = current.value();
                    if (before.lifecycle() == ProtectionLifecycle.ACTIVE) {
                        if (!before.ownerKey().equals(reservation.key())
                                || before.ownerMetadataVersion() != reservation.metadataVersion()
                                || !before.ownerIdentitySha256().equals(
                                        reservation.durableValueSha256().value())) {
                            throw invariant("APPEND_RECOVERY protection owner conflicts with reservation");
                        }
                        return CompletableFuture.completedFuture(current);
                    }
                    return ledgerMetadata.compareAndSetProtection(
                            cluster,
                            configuration.providerScopeSha256(),
                            activeProtection(
                                    before,
                                    reservation.value().reservationId(),
                                    0,
                                    reservation.key(),
                                    reservation.metadataVersion(),
                                    reservation.durableValueSha256().value()),
                            current.metadataVersion());
                });
    }

    private static BookKeeperLedgerProtectionRecord activeProtection(
            BookKeeperLedgerProtectionRecord before,
            String referenceId,
            long commitVersion,
            String ownerKey,
            long ownerMetadataVersion,
            String ownerIdentitySha256) {
        return new BookKeeperLedgerProtectionRecord(
                before.schemaVersion(), before.ledgerIdentitySha256(), before.clusterAlias(), before.ledgerId(),
                before.rootLifecycleEpoch(), before.ledgerRangeSlot(), before.protectionSlot(),
                before.protectionTypeId(), referenceId, before.firstEntryId(), before.entryCount(),
                before.rangeChecksumSha256(), before.streamId(), before.offsetStart(), before.offsetEnd(),
                commitVersion, ownerKey, ownerMetadataVersion, ownerIdentitySha256,
                ProtectionLifecycle.ACTIVE, before.createdAtMillis(), before.expiresAtMillis(), 0);
    }

    private CompletableFuture<Void> writeEntries(
            org.apache.bookkeeper.client.api.WriteAdvHandle handle,
            long firstEntryId,
            List<ByteBuf> retainedEntries,
            BookKeeperOperationDeadline deadline) {
        ArrayDeque<ByteBuf> remaining = new ArrayDeque<>(retainedEntries);
        CompletableFuture<Void> result = writeNext(handle, firstEntryId, 0, remaining, deadline);
        return result.whenComplete((ignored, failure) -> remaining.forEach(ByteBuf::release));
    }

    private CompletableFuture<Void> writeNext(
            org.apache.bookkeeper.client.api.WriteAdvHandle handle,
            long firstEntryId,
            int index,
            ArrayDeque<ByteBuf> remaining,
            BookKeeperOperationDeadline deadline) {
        ByteBuf entry = remaining.pollFirst();
        if (entry == null) {
            return CompletableFuture.completedFuture(null);
        }
        long entryId = Math.addExact(firstEntryId, index);
        CompletableFuture<Long> write;
        try {
            write = client.write(handle, entryId, entry, deadline);
        } catch (Throwable failure) {
            entry.release();
            return CompletableFuture.failedFuture(failure);
        }
        return write.whenComplete((ignored, failure) -> entry.release()).thenCompose(writtenId -> {
            if (writtenId != entryId) {
                return CompletableFuture.failedFuture(invariant(
                        "BookKeeper explicit write returned another entry id"));
            }
            return writeNext(handle, firstEntryId, index + 1, remaining, deadline);
        });
    }

    private DurablePrimaryAppend durableAppend(
            ActiveLedger active,
            BookKeeperPreparedPrimaryAppend prepared,
            BookKeeperVersionedValue<BookKeeperAppendReservationRecord> reservation,
            long firstEntryId,
            int rangeSlot,
            Checksum rangeChecksum) {
        activeLedgers.compute(prepared.streamId(), (ignored, current) ->
                current != null && current.root().value().ledgerId() == active.root().value().ledgerId()
                        ? active : current);
        BookKeeperEntryRangeReadTarget target = new BookKeeperEntryRangeReadTarget(
                1, configuration.clusterAlias(), active.root().value().ledgerId(), firstEntryId,
                prepared.entryCount(), BookKeeperEntryMapping.ONE_NEREUS_ENTRY_PER_BOOKKEEPER_ENTRY, rangeChecksum);
        BookKeeperPrimaryPhysicalIdentity identity = new BookKeeperPrimaryPhysicalIdentity(
                configuration.clusterAlias(), target.ledgerId(), active.root().value().lifecycleEpoch(),
                target.firstEntryId(), target.entryCount(), target.rangeChecksum());
        BookKeeperProviderAppendToken token = new BookKeeperProviderAppendToken(
                reservation.value().reservationId(), target.ledgerId(), active.root().value().lifecycleEpoch(),
                rangeSlot, target.firstEntryId(), target.entryCount(), target.rangeChecksum(),
                reservation.metadataVersion(), active.writer().metadataVersion(), active.root().metadataVersion());
        return new DurablePrimaryAppend(prepared.streamId(), target, identity, rangeChecksum,
                prepared.request().batch().payloadFormat(), prepared.recordCount(), prepared.entryCount(),
                prepared.logicalBytes(), prepared.request().batch().schemaRefs(),
                prepared.request().batch().minEventTimeMillis(), prepared.request().batch().maxEventTimeMillis(), token);
    }

    private CompletableFuture<DurablePrimaryAppend> abandonWithoutSeal(
            BookKeeperVersionedValue<BookKeeperAppendReservationRecord> reservation, Throwable failure) {
        return casReservation(reservation, AppendReservationLifecycle.ABANDONED, "writer reservation CAS failed")
                .handle((ignored, metadataFailure) -> CompletableFuture.<DurablePrimaryAppend>failedFuture(failure))
                .thenCompose(java.util.function.Function.identity());
    }

    private CompletableFuture<DurablePrimaryAppend> failAndSeal(
            ActiveLedger active,
            BookKeeperPreparedPrimaryAppend prepared,
            BookKeeperVersionedValue<BookKeeperAppendReservationRecord> reservation,
            Throwable failure,
            BookKeeperOperationDeadline deadline) {
        return writerMetadata.getReservation(cluster, prepared.streamId(), reservation.value().reservationId())
                .thenCompose(optional -> {
                    if (optional.isEmpty()
                            || optional.orElseThrow().value().lifecycle() == AppendReservationLifecycle.ABANDONED) {
                        return CompletableFuture.completedFuture(null);
                    }
                    BookKeeperVersionedValue<BookKeeperAppendReservationRecord> current = optional.orElseThrow();
                    if (current.value().lifecycle() == AppendReservationLifecycle.RESERVED
                            || current.value().lifecycle() == AppendReservationLifecycle.WRITING) {
                        return casReservation(current, AppendReservationLifecycle.ABANDONED,
                                "provider write/metadata pipeline failed").thenApply(ignored -> null);
                    }
                    // Once the exact range reached DURABLE it remains a recoverable commit candidate. A foreground
                    // metadata timeout must not destroy that fact; generic append recovery will either commit this
                    // reservation or prove it unreachable after the ledger has been recovery-opened and sealed.
                    return CompletableFuture.completedFuture(null);
                }).handle((ignored, abandonFailure) -> null)
                .thenCompose(ignored -> recoverWithinRemainingBudget(
                        prepared.request().session(), deadline, "append range failure"))
                .handle((ignored, recoveryFailure) -> {
                    activeLedgers.remove(prepared.streamId(), active);
                    active.handle().closeAsync();
                    if (recoveryFailure != null) {
                        failure.addSuppressed(unwrap(recoveryFailure));
                    }
                    return CompletableFuture.<DurablePrimaryAppend>failedFuture(failure);
                }).thenCompose(java.util.function.Function.identity());
    }

    private CompletableFuture<BookKeeperLedgerRecoveryResult> recoverWithinRemainingBudget(
            AppendSession session, BookKeeperOperationDeadline deadline, String reason) {
        try {
            return recovery.recoverWriter(session, deadline.remaining(), reason);
        } catch (Throwable failure) {
            return CompletableFuture.failedFuture(failure);
        }
    }

    private CompletableFuture<BookKeeperVersionedValue<BookKeeperAppendReservationRecord>> casReservation(
            BookKeeperVersionedValue<BookKeeperAppendReservationRecord> current,
            AppendReservationLifecycle lifecycle,
            String reason) {
        BookKeeperAppendReservationRecord before = current.value();
        BookKeeperAppendReservationRecord replacement = new BookKeeperAppendReservationRecord(
                before.schemaVersion(), before.reservationId(), before.appendAttemptId(), before.streamId(),
                before.writerId(), before.writerRunIdHash(), before.appendSessionEpoch(), before.fencingTokenHash(),
                before.writerStateEpoch(), before.ledgerId(), before.ledgerRootEpoch(), before.ledgerRangeSlot(),
                before.firstEntryId(), before.entryCount(), before.rangeChecksumSha256(), before.expectedStartOffset(),
                before.payloadFormat(), before.recordCount(), before.logicalBytes(), before.physicalBytes(),
                before.schemaRefs(), before.projectionIdentity(), before.minEventTimeMillis(),
                before.maxEventTimeMillis(), lifecycle, before.commitId(), before.commitKey(),
                before.commitMetadataVersion(), before.commitRecordSha256(), before.createdAtMillis(), clock.millis(),
                lifecycle == AppendReservationLifecycle.ABANDONED ? text(reason, "reason") : "", 0);
        return writerMetadata.compareAndSetReservation(cluster, replacement, current.metadataVersion());
    }

    private BookKeeperAppendReservationRecord reservation(
            ActiveLedger active,
            BookKeeperPreparedPrimaryAppend prepared,
            String reservationId,
            long firstEntryId,
            int rangeSlot,
            Checksum checksum) {
        BookKeeperWriterStateRecord writer = active.writer().value();
        long now = clock.millis();
        return new BookKeeperAppendReservationRecord(1, reservationId, prepared.request().attemptId().value(),
                prepared.streamId().value(), writer.writerId(), writer.writerRunIdHash(), writer.appendSessionEpoch(),
                writer.fencingTokenHash(), writer.writerStateEpoch() + 1, active.root().value().ledgerId(),
                active.root().value().lifecycleEpoch(), rangeSlot, firstEntryId, prepared.entryCount(), checksum.value(),
                prepared.request().expectedStartOffset(), prepared.request().batch().payloadFormat().name(),
                prepared.recordCount(), prepared.logicalBytes(), prepared.physicalBytes(),
                prepared.request().batch().schemaRefs(), CommitAppendRequest.absentProjectionIdentity(),
                prepared.request().batch().minEventTimeMillis(), prepared.request().batch().maxEventTimeMillis(),
                AppendReservationLifecycle.RESERVED, "", "", 0, "", now, now, "", 0);
    }

    private BookKeeperLedgerProtectionRecord reservedProtection(
            ActiveLedger active,
            BookKeeperPreparedPrimaryAppend prepared,
            BookKeeperAppendReservationRecord reservation,
            int protectionSlot,
            BookKeeperProtectionType type) {
        return new BookKeeperLedgerProtectionRecord(1, active.root().value().ledgerIdentitySha256(),
                configuration.clusterAlias(), active.root().value().ledgerId(),
                active.root().value().lifecycleEpoch(), reservation.ledgerRangeSlot(), protectionSlot, type.wireId(),
                reservation.reservationId(), reservation.firstEntryId(), reservation.entryCount(),
                reservation.rangeChecksumSha256(), prepared.streamId().value(),
                prepared.request().expectedStartOffset(),
                Math.addExact(prepared.request().expectedStartOffset(), prepared.recordCount()),
                0, "", 0, "", ProtectionLifecycle.RESERVED, reservation.createdAtMillis(), 0, 0);
    }

    private boolean requiresRollover(ActiveLedger active, BookKeeperPreparedPrimaryAppend prepared) {
        BookKeeperWriterStateRecord writer = active.writer().value();
        try {
            return Math.addExact(writer.nextEntryId(), prepared.entryCount()) > configuration.maxEntriesPerLedger()
                    || Math.addExact(writer.activePhysicalBytes(), prepared.physicalBytes())
                            > configuration.maxBytesPerLedger()
                    || Math.addExact(writer.activeAppendRangeCount(), 1) > configuration.maxAppendRangesPerLedger()
                    || clock.instant().isAfter(InstantMath.add(
                            active.root().value().createdAtMillis(), configuration.maxLedgerAge()));
        } catch (ArithmeticException overflow) {
            return true;
        }
    }

    private void validateBatchBound(BookKeeperPreparedPrimaryAppend prepared) {
        if (prepared.entryCount() > configuration.maxEntriesPerLedger()
                || prepared.physicalBytes() > configuration.maxBytesPerLedger()) {
            throw new NereusException(ErrorCode.INVALID_ARGUMENT, false,
                    "one append batch exceeds an empty BookKeeper ledger bound");
        }
    }

    @Override
    public void close() {
        if (!closed.compareAndSet(false, true)) return;
        activeLedgers.values().forEach(active -> active.handle().closeAsync());
        activeLedgers.clear();
    }

    private void ensureOpen() {
        if (closed.get()) {
            throw new NereusException(ErrorCode.STORAGE_CLOSED, false, "BookKeeper primary WAL appender is closed");
        }
    }

    private static NereusException invariant(String message) {
        return new NereusException(ErrorCode.METADATA_INVARIANT_VIOLATION, false, message);
    }

    private static Throwable unwrap(Throwable failure) {
        Throwable current = failure;
        while ((current instanceof java.util.concurrent.CompletionException
                || current instanceof java.util.concurrent.ExecutionException) && current.getCause() != null) {
            current = current.getCause();
        }
        return current;
    }

    private static Duration min(Duration left, Duration right) {
        return left.compareTo(right) <= 0 ? left : right;
    }

    private static String text(String value, String name) {
        Objects.requireNonNull(value, name);
        if (value.isBlank()) throw new IllegalArgumentException(name + " cannot be blank");
        return value;
    }

    private record ActiveLedger(
            org.apache.bookkeeper.client.api.WriteAdvHandle handle,
            BookKeeperVersionedValue<BookKeeperWriterStateRecord> writer,
            BookKeeperVersionedValue<BookKeeperLedgerRootRecord> root,
            BookKeeperLedgerCustomMetadata customMetadata) {
        private ActiveLedger {
            Objects.requireNonNull(handle, "handle");
            Objects.requireNonNull(writer, "writer");
            Objects.requireNonNull(root, "root");
            Objects.requireNonNull(customMetadata, "customMetadata");
        }

        private ActiveLedger withWriter(BookKeeperVersionedValue<BookKeeperWriterStateRecord> replacement) {
            return new ActiveLedger(handle, replacement, root, customMetadata);
        }
    }

    private static final class InstantMath {
        private InstantMath() { }

        private static java.time.Instant add(long epochMillis, Duration duration) {
            try {
                return java.time.Instant.ofEpochMilli(epochMillis).plus(duration);
            } catch (java.time.DateTimeException | ArithmeticException overflow) {
                return java.time.Instant.MAX;
            }
        }
    }
}
