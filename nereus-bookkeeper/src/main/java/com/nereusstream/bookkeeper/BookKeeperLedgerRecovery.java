/* Licensed under the Apache License, Version 2.0 */
package com.nereusstream.bookkeeper;

import com.nereusstream.api.AppendSession;
import com.nereusstream.api.ErrorCode;
import com.nereusstream.api.NereusException;
import com.nereusstream.metadata.oxia.BookKeeperLedgerMetadataStore;
import com.nereusstream.metadata.oxia.BookKeeperVersionedValue;
import com.nereusstream.metadata.oxia.BookKeeperWriterMetadataStore;
import com.nereusstream.metadata.oxia.records.BookKeeperLedgerLifecycle;
import com.nereusstream.metadata.oxia.records.AppendReservationLifecycle;
import com.nereusstream.metadata.oxia.records.BookKeeperAppendReservationRecord;
import com.nereusstream.metadata.oxia.records.BookKeeperLedgerProtectionRecord;
import com.nereusstream.metadata.oxia.records.BookKeeperLedgerRootRecord;
import com.nereusstream.metadata.oxia.records.BookKeeperProtectionType;
import com.nereusstream.metadata.oxia.records.BookKeeperWriterLifecycle;
import com.nereusstream.metadata.oxia.records.BookKeeperWriterStateRecord;
import com.nereusstream.metadata.oxia.records.ProtectionLifecycle;
import java.time.Clock;
import java.time.Duration;
import java.util.Arrays;
import java.util.Objects;
import java.util.concurrent.CompletableFuture;
import org.apache.bookkeeper.client.api.LedgerMetadata;
import org.apache.bookkeeper.client.api.ReadHandle;

/** Recovery-open is centralized here; ordinary readers must never depend on this class. */
public final class BookKeeperLedgerRecovery {
    private final String cluster;
    private final BookKeeperWalConfiguration configuration;
    private final BookKeeperWriterMetadataStore writerMetadata;
    private final BookKeeperLedgerMetadataStore ledgerMetadata;
    private final BookKeeperLedgerIdNamespaceReservationVerifier namespaceVerifier;
    private final BookKeeperClientOperations client;
    private final BookKeeperPasswordProvider passwordProvider;
    private final BookKeeperWriterStateMachine writerState;
    private final Clock clock;

    public BookKeeperLedgerRecovery(
            String cluster,
            BookKeeperWalConfiguration configuration,
            BookKeeperWriterMetadataStore writerMetadata,
            BookKeeperLedgerMetadataStore ledgerMetadata,
            BookKeeperLedgerIdNamespaceReservationVerifier namespaceVerifier,
            BookKeeperClientOperations client,
            BookKeeperPasswordProvider passwordProvider,
            BookKeeperWriterStateMachine writerState,
            Clock clock) {
        this.cluster = text(cluster, "cluster");
        this.configuration = Objects.requireNonNull(configuration, "configuration");
        this.writerMetadata = Objects.requireNonNull(writerMetadata, "writerMetadata");
        this.ledgerMetadata = Objects.requireNonNull(ledgerMetadata, "ledgerMetadata");
        this.namespaceVerifier = Objects.requireNonNull(namespaceVerifier, "namespaceVerifier");
        this.client = Objects.requireNonNull(client, "client");
        this.passwordProvider = Objects.requireNonNull(passwordProvider, "passwordProvider");
        this.writerState = Objects.requireNonNull(writerState, "writerState");
        this.clock = Objects.requireNonNull(clock, "clock");
    }

    public CompletableFuture<BookKeeperLedgerRecoveryResult> recoverWriter(
            AppendSession newOwner, Duration timeout, String reason) {
        AppendSession session = Objects.requireNonNull(newOwner, "newOwner");
        Duration budget = min(Objects.requireNonNull(timeout, "timeout"), configuration.sealTimeout());
        BookKeeperOperationDeadline deadline = new BookKeeperOperationDeadline(budget);
        return namespaceVerifier.requireActive(configuration, deadline.remaining())
                .thenCompose(namespace -> writerMetadata.getWriter(cluster, session.streamId())
                        .thenCompose(optional -> {
                            BookKeeperVersionedValue<BookKeeperWriterStateRecord> current = optional.orElseThrow(
                                    () -> new NereusException(ErrorCode.METADATA_CONDITION_FAILED, false,
                                            "BookKeeper writer is absent during recovery"));
                            if (current.value().lifecycle() != BookKeeperWriterLifecycle.ACTIVE
                                    && current.value().lifecycle() != BookKeeperWriterLifecycle.RECOVERING) {
                                return CompletableFuture.failedFuture(new NereusException(
                                        ErrorCode.METADATA_CONDITION_FAILED, false,
                                        "BookKeeper writer has no active ledger to recover"));
                            }
                            return writerState.adoptForRecovery(current, session, reason)
                                    .thenCompose(recovering -> ledgerMetadata.getRoot(
                                                    cluster, configuration.providerScopeSha256(),
                                                    recovering.value().activeLedgerId())
                                            .thenCompose(rootOptional -> {
                                                BookKeeperVersionedValue<BookKeeperLedgerRootRecord> root =
                                                        rootOptional.orElseThrow(() -> new NereusException(
                                                                ErrorCode.METADATA_INVARIANT_VIOLATION, false,
                                                                "active BookKeeper writer root is absent"));
                                                requireWriterRoot(recovering.value(), root.value());
                                                return seal(root, namespace, deadline, reason)
                                                        .thenCompose(sealed -> reconcileActiveReservation(
                                                                        recovering, sealed, reason)
                                                                .thenCompose(ignored -> writerState.finishRecovery(
                                                                        recovering, session, reason))
                                                                .thenApply(idle ->
                                                                        new BookKeeperLedgerRecoveryResult(
                                                                                idle, sealed)));
                                            }));
                        }));
    }

    private CompletableFuture<Void> reconcileActiveReservation(
            BookKeeperVersionedValue<BookKeeperWriterStateRecord> writer,
            BookKeeperVersionedValue<BookKeeperLedgerRootRecord> sealed,
            String reason) {
        String reservationId = writer.value().activeReservationId();
        if (reservationId.isEmpty()) return CompletableFuture.completedFuture(null);
        return writerMetadata.getReservation(
                        cluster, new com.nereusstream.api.StreamId(writer.value().streamId()), reservationId)
                .thenCompose(optional -> {
                    BookKeeperVersionedValue<BookKeeperAppendReservationRecord> reservation = optional.orElseThrow(
                            () -> new NereusException(ErrorCode.METADATA_INVARIANT_VIOLATION, false,
                                    "active BookKeeper recovery reservation is absent"));
                    requireRecoveryReservation(writer.value(), sealed.value(), reservation.value());
                    return reconcileMandatoryProtections(sealed.value(), reservation.value())
                            .thenCompose(ignored -> terminalizeOrProtectReservation(reservation, reason));
                });
    }

    private CompletableFuture<Void> reconcileMandatoryProtections(
            BookKeeperLedgerRootRecord root,
            BookKeeperAppendReservationRecord reservation) {
        BookKeeperProtectionType[] types = {
            BookKeeperProtectionType.REACHABLE_APPEND,
            BookKeeperProtectionType.VISIBLE_GENERATION,
            BookKeeperProtectionType.APPEND_RECOVERY
        };
        CompletableFuture<Void> chain = CompletableFuture.completedFuture(null);
        for (int slot = 0; slot < types.length; slot++) {
            int exactSlot = slot;
            chain = chain.thenCompose(ignored -> ledgerMetadata.createProtection(
                            cluster,
                            configuration.providerScopeSha256(),
                            reservedProtection(root, reservation, exactSlot, types[exactSlot]))
                    .thenApply(created -> null));
        }
        return chain;
    }

    private CompletableFuture<Void> terminalizeOrProtectReservation(
            BookKeeperVersionedValue<BookKeeperAppendReservationRecord> reservation,
            String reason) {
        AppendReservationLifecycle lifecycle = reservation.value().lifecycle();
        if (lifecycle == AppendReservationLifecycle.RESERVED
                || lifecycle == AppendReservationLifecycle.WRITING) {
            BookKeeperAppendReservationRecord before = reservation.value();
            BookKeeperAppendReservationRecord abandoned = new BookKeeperAppendReservationRecord(
                    before.schemaVersion(), before.reservationId(), before.appendAttemptId(), before.streamId(),
                    before.writerId(), before.writerRunIdHash(), before.appendSessionEpoch(),
                    before.fencingTokenHash(), before.writerStateEpoch(), before.ledgerId(), before.ledgerRootEpoch(),
                    before.ledgerRangeSlot(), before.firstEntryId(), before.entryCount(),
                    before.rangeChecksumSha256(), before.expectedStartOffset(), before.payloadFormat(),
                    before.recordCount(), before.logicalBytes(), before.physicalBytes(), before.schemaRefs(),
                    before.projectionIdentity(), before.minEventTimeMillis(), before.maxEventTimeMillis(),
                    AppendReservationLifecycle.ABANDONED, "", "", 0, "", before.createdAtMillis(),
                    clock.millis(), text(reason, "reason"), 0);
            return writerMetadata.compareAndSetReservation(cluster, abandoned, reservation.metadataVersion())
                    .thenApply(ignored -> null);
        }
        if (lifecycle == AppendReservationLifecycle.DURABLE
                || lifecycle == AppendReservationLifecycle.COMMIT_PREPARED
                || lifecycle == AppendReservationLifecycle.HEAD_COMMITTED) {
            return activateRecoveryProtection(reservation).thenApply(ignored -> null);
        }
        return CompletableFuture.completedFuture(null);
    }

    private CompletableFuture<BookKeeperVersionedValue<BookKeeperLedgerProtectionRecord>> activateRecoveryProtection(
            BookKeeperVersionedValue<BookKeeperAppendReservationRecord> reservation) {
        BookKeeperAppendReservationRecord owner = reservation.value();
        return ledgerMetadata.getProtection(
                        cluster,
                        configuration.providerScopeSha256(),
                        owner.ledgerId(),
                        owner.ledgerRangeSlot(),
                        2)
                .thenCompose(optional -> {
                    BookKeeperVersionedValue<BookKeeperLedgerProtectionRecord> current = optional.orElseThrow(
                            () -> new NereusException(ErrorCode.METADATA_INVARIANT_VIOLATION, false,
                                    "APPEND_RECOVERY protection is absent after reconciliation"));
                    if (current.value().lifecycle() == ProtectionLifecycle.ACTIVE) {
                        if (!current.value().ownerKey().equals(reservation.key())
                                || current.value().ownerMetadataVersion() != reservation.metadataVersion()
                                || !current.value().ownerIdentitySha256()
                                        .equals(reservation.durableValueSha256().value())) {
                            throw new NereusException(ErrorCode.METADATA_INVARIANT_VIOLATION, false,
                                    "APPEND_RECOVERY protection owner changed during recovery");
                        }
                        return CompletableFuture.completedFuture(current);
                    }
                    BookKeeperLedgerProtectionRecord before = current.value();
                    BookKeeperLedgerProtectionRecord active = new BookKeeperLedgerProtectionRecord(
                            before.schemaVersion(), before.ledgerIdentitySha256(), before.clusterAlias(),
                            before.ledgerId(), before.rootLifecycleEpoch(), before.ledgerRangeSlot(),
                            before.protectionSlot(), before.protectionTypeId(), owner.reservationId(),
                            before.firstEntryId(), before.entryCount(), before.rangeChecksumSha256(),
                            before.streamId(), before.offsetStart(), before.offsetEnd(), 0, reservation.key(),
                            reservation.metadataVersion(), reservation.durableValueSha256().value(),
                            ProtectionLifecycle.ACTIVE, before.createdAtMillis(), before.expiresAtMillis(), 0);
                    return ledgerMetadata.compareAndSetProtection(
                            cluster, configuration.providerScopeSha256(), active, current.metadataVersion());
                });
    }

    private static BookKeeperLedgerProtectionRecord reservedProtection(
            BookKeeperLedgerRootRecord root,
            BookKeeperAppendReservationRecord reservation,
            int protectionSlot,
            BookKeeperProtectionType type) {
        return new BookKeeperLedgerProtectionRecord(
                1, root.ledgerIdentitySha256(), root.clusterAlias(), root.ledgerId(),
                reservation.ledgerRootEpoch(), reservation.ledgerRangeSlot(), protectionSlot, type.wireId(),
                reservation.reservationId(), reservation.firstEntryId(), reservation.entryCount(),
                reservation.rangeChecksumSha256(), reservation.streamId(), reservation.expectedStartOffset(),
                Math.addExact(reservation.expectedStartOffset(), reservation.recordCount()), 0, "", 0, "",
                ProtectionLifecycle.RESERVED, reservation.createdAtMillis(), 0, 0);
    }

    private static void requireRecoveryReservation(
            BookKeeperWriterStateRecord writer,
            BookKeeperLedgerRootRecord root,
            BookKeeperAppendReservationRecord reservation) {
        if (!reservation.reservationId().equals(writer.activeReservationId())
                || !reservation.streamId().equals(writer.streamId())
                || reservation.ledgerId() != writer.activeLedgerId()
                || reservation.ledgerId() != root.ledgerId()
                || reservation.ledgerRootEpoch() > root.lifecycleEpoch()
                || reservation.ledgerRangeSlot() >= writer.activeAppendRangeCount()
                || Math.addExact(reservation.firstEntryId(), reservation.entryCount()) > writer.nextEntryId()) {
            throw new NereusException(ErrorCode.METADATA_INVARIANT_VIOLATION, false,
                    "active BookKeeper recovery reservation does not match writer/root counters");
        }
    }

    public CompletableFuture<BookKeeperVersionedValue<BookKeeperLedgerRootRecord>> sealUnowned(
            BookKeeperVersionedValue<BookKeeperLedgerRootRecord> root,
            Duration timeout,
            String reason) {
        BookKeeperOperationDeadline deadline = new BookKeeperOperationDeadline(
                min(Objects.requireNonNull(timeout, "timeout"), configuration.sealTimeout()));
        return namespaceVerifier.requireActive(configuration, deadline.remaining())
                .thenCompose(namespace -> seal(root, namespace, deadline, reason));
    }

    private CompletableFuture<BookKeeperVersionedValue<BookKeeperLedgerRootRecord>> seal(
            BookKeeperVersionedValue<BookKeeperLedgerRootRecord> observed,
            BookKeeperLedgerIdNamespaceReservation namespace,
            BookKeeperOperationDeadline deadline,
            String reason) {
        BookKeeperVersionedValue<BookKeeperLedgerRootRecord> root = Objects.requireNonNull(observed, "root");
        BookKeeperLedgerLifecycle lifecycle = root.value().lifecycle();
        if (lifecycle == BookKeeperLedgerLifecycle.SEALED) {
            return CompletableFuture.completedFuture(root);
        }
        CompletableFuture<BookKeeperVersionedValue<BookKeeperLedgerRootRecord>> sealing;
        if (lifecycle == BookKeeperLedgerLifecycle.SEALING) {
            sealing = CompletableFuture.completedFuture(root);
        } else if (lifecycle == BookKeeperLedgerLifecycle.ACTIVE
                || lifecycle == BookKeeperLedgerLifecycle.ALLOCATING) {
            sealing = ledgerMetadata.compareAndSetRoot(cluster,
                    sealing(root.value(), reason), root.metadataVersion());
        } else {
            return CompletableFuture.failedFuture(new NereusException(
                    ErrorCode.METADATA_CONDITION_FAILED, false,
                    "BookKeeper root lifecycle cannot enter recovery sealing: " + lifecycle));
        }
        return sealing.thenCompose(value -> recoveryOpen(value, namespace, deadline, reason));
    }

    private CompletableFuture<BookKeeperVersionedValue<BookKeeperLedgerRootRecord>> recoveryOpen(
            BookKeeperVersionedValue<BookKeeperLedgerRootRecord> sealing,
            BookKeeperLedgerIdNamespaceReservation namespace,
            BookKeeperOperationDeadline deadline,
            String reason) {
        BookKeeperLedgerRootRecord root = sealing.value();
        BookKeeperLedgerCustomMetadata expected = BookKeeperLedgerCustomMetadata.create(
                cluster, configuration, namespace, new com.nereusstream.api.StreamId(root.streamId()),
                root.segmentSequence(), root.allocationId());
        if (!expected.sha256().value().equals(root.customMetadataSha256())
                || !namespace.ledgerIdNamespaceSha256().value().equals(root.ledgerIdNamespaceSha256())) {
            return quarantine(sealing, "durable BookKeeper root binding cannot reconstruct expected provider metadata")
                    .thenCompose(ignored -> CompletableFuture.failedFuture(new NereusException(
                            ErrorCode.METADATA_INVARIANT_VIOLATION, false,
                            "BookKeeper recovery root binding drifted")));
        }
        byte[] password = Objects.requireNonNull(
                passwordProvider.resolve(configuration.passwordRef()), "resolved password").clone();
        CompletableFuture<ReadHandle> open;
        try {
            open = client.open(root.ledgerId(), configuration.digestType(), password, true, deadline);
        } catch (Throwable failure) {
            Arrays.fill(password, (byte) 0);
            return CompletableFuture.failedFuture(failure);
        }
        return open.whenComplete((ignored, failure) -> Arrays.fill(password, (byte) 0))
                .thenCompose(handle -> {
                    try {
                        if (handle.getId() != root.ledgerId()) {
                            throw new NereusException(ErrorCode.METADATA_INVARIANT_VIOLATION, false,
                                    "recovery-open returned another BookKeeper ledger");
                        }
                        LedgerMetadata metadata = handle.getLedgerMetadata();
                        expected.requireExactImmutableLedgerMetadata(root.ledgerId(), configuration, metadata);
                        if (!metadata.isClosed()) {
                            throw new NereusException(ErrorCode.METADATA_INVARIANT_VIOLATION, false,
                                    "BookKeeper recovery-open completed without closed ledger metadata");
                        }
                        return ledgerMetadata.compareAndSetRoot(cluster,
                                sealed(root, metadata, reason), sealing.metadataVersion())
                                .whenComplete((ignored, failure) -> handle.closeAsync());
                    } catch (Throwable failure) {
                        handle.closeAsync();
                        return quarantine(sealing, "BookKeeper recovery-open identity/closed-state mismatch")
                                .thenCompose(ignored -> CompletableFuture.failedFuture(failure));
                    }
                });
    }

    private CompletableFuture<BookKeeperVersionedValue<BookKeeperLedgerRootRecord>> quarantine(
            BookKeeperVersionedValue<BookKeeperLedgerRootRecord> current, String reason) {
        BookKeeperLedgerRootRecord before = current.value();
        return ledgerMetadata.compareAndSetRoot(cluster, replace(before,
                BookKeeperLedgerLifecycle.QUARANTINED, before.lifecycleEpoch() + 1,
                before.sealStartedAtMillis(), before.sealedAtMillis(), before.sealedLastEntryId(),
                before.sealedLength(), before.sealReason(), text(reason, "reason")), current.metadataVersion());
    }

    private BookKeeperLedgerRootRecord sealing(BookKeeperLedgerRootRecord before, String reason) {
        return replace(before, BookKeeperLedgerLifecycle.SEALING, before.lifecycleEpoch() + 1,
                clock.millis(), 0, -1, 0, text(reason, "reason"), "");
    }

    private BookKeeperLedgerRootRecord sealed(
            BookKeeperLedgerRootRecord before, LedgerMetadata metadata, String reason) {
        return replace(before, BookKeeperLedgerLifecycle.SEALED, before.lifecycleEpoch() + 1,
                before.sealStartedAtMillis(), clock.millis(), metadata.getLastEntryId(), metadata.getLength(),
                text(reason, "reason"), "");
    }

    private static BookKeeperLedgerRootRecord replace(
            BookKeeperLedgerRootRecord before,
            BookKeeperLedgerLifecycle lifecycle,
            long epoch,
            long sealStartedAt,
            long sealedAt,
            long lastEntry,
            long length,
            String sealReason,
            String stateReason) {
        return new BookKeeperLedgerRootRecord(before.schemaVersion(), before.ledgerIdentitySha256(),
                before.clusterAlias(), before.providerScopeSha256(), before.ledgerId(), before.streamId(),
                before.segmentSequence(), before.allocationId(), before.allocationSlot(),
                before.configurationBindingSha256(), before.ledgerIdNamespaceSha256(), before.lateCreateHazard(),
                before.writerId(), before.writerRunIdHash(), before.appendSessionEpoch(), before.fencingTokenHash(),
                before.ensembleSize(), before.writeQuorumSize(), before.ackQuorumSize(), before.digestType(),
                before.customMetadataSha256(), lifecycle, epoch, before.createdAtMillis(), before.activatedAtMillis(),
                sealStartedAt, sealedAt, lastEntry, length, sealReason, before.gcAttemptId(),
                before.referenceSetSha256(), before.markedAtMillis(), before.deleteNotBeforeMillis(),
                before.deleteStartedAtMillis(), before.firstAbsentAtMillis(), before.deletedAtMillis(),
                stateReason, 0);
    }

    private static void requireWriterRoot(
            BookKeeperWriterStateRecord writer, BookKeeperLedgerRootRecord root) {
        if (!writer.streamId().equals(root.streamId())
                || writer.activeLedgerId() != root.ledgerId()
                || writer.activeLedgerRootEpoch() > root.lifecycleEpoch()) {
            throw new NereusException(ErrorCode.METADATA_INVARIANT_VIOLATION, false,
                    "BookKeeper writer active identity does not match its root");
        }
    }

    private static Duration min(Duration left, Duration right) {
        return left.compareTo(right) <= 0 ? left : right;
    }

    private static String text(String value, String name) {
        Objects.requireNonNull(value, name);
        if (value.isBlank()) throw new IllegalArgumentException(name + " cannot be blank");
        return value;
    }
}
