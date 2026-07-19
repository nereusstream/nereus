/* Licensed under the Apache License, Version 2.0 */
package com.nereusstream.bookkeeper;

import com.nereusstream.api.Checksum;
import com.nereusstream.api.ErrorCode;
import com.nereusstream.api.NereusException;
import com.nereusstream.api.ReadTargetIdentities;
import com.nereusstream.api.StreamId;
import com.nereusstream.api.keys.DeterministicIds;
import com.nereusstream.api.target.BookKeeperEntryRangeReadTarget;
import com.nereusstream.api.target.ReadTargetType;
import com.nereusstream.core.append.PrimaryPhysicalReferenceAdapter;
import com.nereusstream.core.append.ProtectedGenerationZero;
import com.nereusstream.core.append.ProtectedStableAppend;
import com.nereusstream.metadata.oxia.BookKeeperLedgerMetadataStore;
import com.nereusstream.metadata.oxia.BookKeeperPhysicalReferenceProof;
import com.nereusstream.metadata.oxia.BookKeeperScanToken;
import com.nereusstream.metadata.oxia.BookKeeperVersionedValue;
import com.nereusstream.metadata.oxia.BookKeeperWriterMetadataStore;
import com.nereusstream.metadata.oxia.MaterializedGenerationZero;
import com.nereusstream.metadata.oxia.PhysicalReferencePurpose;
import com.nereusstream.metadata.oxia.PreparedStableAppend;
import com.nereusstream.metadata.oxia.records.AppendReservationLifecycle;
import com.nereusstream.metadata.oxia.records.BookKeeperAppendReservationRecord;
import com.nereusstream.metadata.oxia.records.BookKeeperLedgerLifecycle;
import com.nereusstream.metadata.oxia.records.BookKeeperLedgerProtectionRecord;
import com.nereusstream.metadata.oxia.records.BookKeeperLedgerRootRecord;
import com.nereusstream.metadata.oxia.records.BookKeeperProtectionType;
import com.nereusstream.metadata.oxia.records.ProtectionLifecycle;
import java.time.Clock;
import java.time.Duration;
import java.util.Objects;
import java.util.Optional;
import java.util.concurrent.CompletableFuture;

/** Activates fixed BookKeeper range protections under the exact generic commit/index owner. */
public final class BookKeeperPrimaryPhysicalReferenceAdapter
        implements PrimaryPhysicalReferenceAdapter<BookKeeperEntryRangeReadTarget> {
    private final String cluster;
    private final BookKeeperWalConfiguration configuration;
    private final BookKeeperWriterMetadataStore writerMetadata;
    private final BookKeeperLedgerMetadataStore ledgerMetadata;
    private final Clock clock;

    public BookKeeperPrimaryPhysicalReferenceAdapter(
            String cluster,
            BookKeeperWalConfiguration configuration,
            BookKeeperWriterMetadataStore writerMetadata,
            BookKeeperLedgerMetadataStore ledgerMetadata,
            Clock clock) {
        this.cluster = text(cluster, "cluster");
        this.configuration = Objects.requireNonNull(configuration, "configuration");
        this.writerMetadata = Objects.requireNonNull(writerMetadata, "writerMetadata");
        this.ledgerMetadata = Objects.requireNonNull(ledgerMetadata, "ledgerMetadata");
        this.clock = Objects.requireNonNull(clock, "clock");
    }

    @Override
    public ReadTargetType targetType() {
        return ReadTargetType.BOOKKEEPER_ENTRY_RANGE;
    }

    @Override
    public Class<BookKeeperEntryRangeReadTarget> targetClass() {
        return BookKeeperEntryRangeReadTarget.class;
    }

    @Override
    public CompletableFuture<ProtectedStableAppend> protectBeforeHead(
            PreparedStableAppend append,
            BookKeeperEntryRangeReadTarget target,
            Duration timeout) {
        PreparedStableAppend prepared = Objects.requireNonNull(append, "append");
        BookKeeperEntryRangeReadTarget exactTarget = exactTarget(target);
        BookKeeperOperationDeadline deadline = deadline(timeout);
        String referenceId = reachableReferenceId(prepared);
        return loadRoot(exactTarget, deadline)
                .thenCompose(root -> {
                    requireRoot(root.value(), prepared.request().streamId(), exactTarget, true);
                    return locateProtection(
                                    exactTarget,
                                    prepared.request().streamId(),
                                    prepared.request().expectedStartOffset(),
                                    prepared.request().recordCount(),
                                    BookKeeperProtectionType.REACHABLE_APPEND,
                                    0,
                                    deadline,
                                    Optional.empty())
                            .thenCompose(protection -> prepareReservation(
                                            protection,
                                            prepared,
                                            exactTarget)
                                    .thenCompose(reservation -> activate(
                                            protection,
                                            referenceId,
                                            0,
                                            prepared.commitKey(),
                                            prepared.commitMetadataVersion(),
                                            prepared.commitRecordSha256().value())))
                            .thenCompose(protection -> loadRoot(exactTarget, deadline)
                                    .thenApply(reloaded -> {
                                        requireRoot(reloaded.value(), prepared.request().streamId(), exactTarget, true);
                                        return new ProtectedStableAppend(
                                                prepared,
                                                proof(
                                                        PhysicalReferencePurpose.REACHABLE_APPEND,
                                                        ReadTargetIdentities.sha256(exactTarget),
                                                        referenceId,
                                                        reloaded,
                                                        protection));
                                    }));
                });
    }

    @Override
    public CompletableFuture<ProtectedGenerationZero> protectVisibleIndex(
            MaterializedGenerationZero append,
            BookKeeperEntryRangeReadTarget target,
            Duration timeout) {
        MaterializedGenerationZero materialized = Objects.requireNonNull(append, "append");
        BookKeeperEntryRangeReadTarget exactTarget = exactTarget(target);
        BookKeeperOperationDeadline deadline = deadline(timeout);
        var committed = materialized.committedAppend();
        String referenceId = visibleReferenceId(materialized);
        return loadRoot(exactTarget, deadline)
                .thenCompose(root -> {
                    requireRoot(root.value(), committed.streamId(), exactTarget, false);
                    return locateProtection(
                                    exactTarget,
                                    committed.streamId(),
                                    committed.range().startOffset(),
                                    committed.recordCount(),
                                    BookKeeperProtectionType.VISIBLE_GENERATION,
                                    1,
                                    deadline,
                                    Optional.empty())
                            .thenCompose(protection -> commitReservation(
                                            protection,
                                            materialized,
                                            exactTarget)
                                    .thenCompose(reservation -> activate(
                                            protection,
                                            referenceId,
                                            committed.commitVersion(),
                                            materialized.indexKey(),
                                            materialized.indexMetadataVersion(),
                                            materialized.indexRecordSha256().value())))
                            .thenCompose(protection -> loadRoot(exactTarget, deadline)
                                    .thenApply(reloaded -> {
                                        requireRoot(reloaded.value(), committed.streamId(), exactTarget, false);
                                        return new ProtectedGenerationZero(
                                                materialized,
                                                proof(
                                                        PhysicalReferencePurpose.VISIBLE_GENERATION,
                                                        ReadTargetIdentities.sha256(exactTarget),
                                                        referenceId,
                                                        reloaded,
                                                        protection));
                                    }));
                });
    }

    private CompletableFuture<BookKeeperVersionedValue<BookKeeperAppendReservationRecord>> prepareReservation(
            BookKeeperVersionedValue<BookKeeperLedgerProtectionRecord> protection,
            PreparedStableAppend prepared,
            BookKeeperEntryRangeReadTarget target) {
        if (protection.value().lifecycle() == ProtectionLifecycle.ACTIVE) {
            requireActiveOwner(
                    protection.value(),
                    reachableReferenceId(prepared),
                    prepared.commitKey(),
                    prepared.commitMetadataVersion(),
                    prepared.commitRecordSha256().value());
            return findReservationByRange(
                            prepared.request().streamId(), target, protection.value().ledgerRangeSlot())
                    .thenApply(reservation -> {
                        requirePreparedReservation(reservation.value(), prepared);
                        requireCommitOwner(reservation.value(), prepared);
                        return reservation;
                    });
        }
        return writerMetadata.getReservation(
                        cluster,
                        prepared.request().streamId(),
                        protection.value().referenceId())
                .thenCompose(optional -> {
                    BookKeeperVersionedValue<BookKeeperAppendReservationRecord> reservation = optional.orElseThrow(
                            () -> invariant("BookKeeper durable reservation is absent before commit preparation"));
                    requireReservation(
                            reservation.value(),
                            prepared.request().streamId(),
                            target,
                            protection.value().ledgerRangeSlot());
                    requirePreparedReservation(reservation.value(), prepared);
                    if (reservation.value().lifecycle() == AppendReservationLifecycle.COMMIT_PREPARED
                            || reservation.value().lifecycle() == AppendReservationLifecycle.HEAD_COMMITTED) {
                        requireCommitOwner(reservation.value(), prepared);
                        return CompletableFuture.completedFuture(reservation);
                    }
                    if (reservation.value().lifecycle() != AppendReservationLifecycle.DURABLE) {
                        return CompletableFuture.failedFuture(invariant(
                                "BookKeeper reservation is not durable before commit preparation"));
                    }
                    return writerMetadata.compareAndSetReservation(
                            cluster,
                            withPreparedCommit(reservation.value(), prepared),
                            reservation.metadataVersion());
                });
    }

    private CompletableFuture<BookKeeperVersionedValue<BookKeeperAppendReservationRecord>> commitReservation(
            BookKeeperVersionedValue<BookKeeperLedgerProtectionRecord> protection,
            MaterializedGenerationZero materialized,
            BookKeeperEntryRangeReadTarget target) {
        StreamId stream = materialized.committedAppend().streamId();
        CompletableFuture<BookKeeperVersionedValue<BookKeeperAppendReservationRecord>> reservationFuture =
                protection.value().lifecycle() == ProtectionLifecycle.RESERVED
                        ? writerMetadata.getReservation(cluster, stream, protection.value().referenceId())
                                .thenApply(optional -> optional.orElseThrow(
                                        () -> invariant("BookKeeper reservation is absent at generation zero")))
                        : findReservationByRange(stream, target, protection.value().ledgerRangeSlot());
        return reservationFuture.thenCompose(reservation -> {
            requireReservation(reservation.value(), stream, target, protection.value().ledgerRangeSlot());
            if (!reservation.value().commitId().equals(materialized.committedAppend().commitId())) {
                return CompletableFuture.failedFuture(invariant(
                        "BookKeeper reservation commit does not match generation zero"));
            }
            requireCommittedReservation(reservation.value(), materialized, target);
            if (reservation.value().lifecycle() == AppendReservationLifecycle.HEAD_COMMITTED) {
                return CompletableFuture.completedFuture(reservation);
            }
            if (reservation.value().lifecycle() != AppendReservationLifecycle.COMMIT_PREPARED) {
                return CompletableFuture.failedFuture(invariant(
                        "BookKeeper reservation did not reach COMMIT_PREPARED before generation zero"));
            }
            return writerMetadata.compareAndSetReservation(
                    cluster,
                    withLifecycle(reservation.value(), AppendReservationLifecycle.HEAD_COMMITTED),
                    reservation.metadataVersion());
        });
    }

    private CompletableFuture<BookKeeperVersionedValue<BookKeeperLedgerProtectionRecord>> activate(
            BookKeeperVersionedValue<BookKeeperLedgerProtectionRecord> current,
            String referenceId,
            long commitVersion,
            String ownerKey,
            long ownerMetadataVersion,
            String ownerIdentitySha256) {
        if (current.value().lifecycle() == ProtectionLifecycle.ACTIVE) {
            requireActiveOwner(
                    current.value(), referenceId, ownerKey, ownerMetadataVersion, ownerIdentitySha256);
            return CompletableFuture.completedFuture(current);
        }
        BookKeeperLedgerProtectionRecord before = current.value();
        BookKeeperLedgerProtectionRecord replacement = new BookKeeperLedgerProtectionRecord(
                before.schemaVersion(), before.ledgerIdentitySha256(), before.clusterAlias(), before.ledgerId(),
                before.rootLifecycleEpoch(), before.ledgerRangeSlot(), before.protectionSlot(),
                before.protectionTypeId(), referenceId, before.firstEntryId(), before.entryCount(),
                before.rangeChecksumSha256(), before.streamId(), before.offsetStart(), before.offsetEnd(),
                commitVersion, ownerKey, ownerMetadataVersion, ownerIdentitySha256,
                ProtectionLifecycle.ACTIVE, before.createdAtMillis(), before.expiresAtMillis(), 0);
        return ledgerMetadata.compareAndSetProtection(
                cluster,
                configuration.providerScopeSha256(),
                replacement,
                current.metadataVersion());
    }

    private CompletableFuture<BookKeeperVersionedValue<BookKeeperLedgerProtectionRecord>> locateProtection(
            BookKeeperEntryRangeReadTarget target,
            StreamId stream,
            long offsetStart,
            int recordCount,
            BookKeeperProtectionType type,
            int slot,
            BookKeeperOperationDeadline deadline,
            Optional<BookKeeperScanToken> continuation) {
        int pageSize = Math.min(configuration.retentionPageSize(), 1_024);
        return deadline.bound(ledgerMetadata.scanProtections(
                        cluster,
                        configuration.providerScopeSha256(),
                        target.ledgerId(),
                        continuation,
                        pageSize))
                .thenCompose(page -> {
                    BookKeeperVersionedValue<BookKeeperLedgerProtectionRecord> match = null;
                    long offsetEnd = Math.addExact(offsetStart, recordCount);
                    for (BookKeeperVersionedValue<BookKeeperLedgerProtectionRecord> candidate : page.values()) {
                        BookKeeperLedgerProtectionRecord value = candidate.value();
                        if (value.protectionSlot() == slot
                                && value.protectionType() == type
                                && value.ledgerId() == target.ledgerId()
                                && value.firstEntryId() == target.firstEntryId()
                                && value.entryCount() == target.entryCount()
                                && value.rangeChecksumSha256().equals(target.rangeChecksum().value())
                                && value.streamId().equals(stream.value())
                                && value.offsetStart() == offsetStart
                                && value.offsetEnd() == offsetEnd) {
                            if (match != null) {
                                return CompletableFuture.failedFuture(invariant(
                                        "multiple BookKeeper protection rows describe one exact range"));
                            }
                            match = candidate;
                        }
                    }
                    if (match != null) return CompletableFuture.completedFuture(match);
                    if (page.continuation().isPresent()) {
                        return locateProtection(
                                target,
                                stream,
                                offsetStart,
                                recordCount,
                                type,
                                slot,
                                deadline,
                                page.continuation());
                    }
                    return CompletableFuture.failedFuture(invariant(
                            "mandatory BookKeeper protection row is absent for the exact range"));
                });
    }

    private CompletableFuture<BookKeeperVersionedValue<BookKeeperLedgerRootRecord>> loadRoot(
            BookKeeperEntryRangeReadTarget target,
            BookKeeperOperationDeadline deadline) {
        return deadline.bound(ledgerMetadata.getRoot(
                        cluster,
                        configuration.providerScopeSha256(),
                        target.ledgerId()))
                .thenApply(optional -> optional.orElseThrow(
                        () -> invariant("BookKeeper ledger root is absent")));
    }

    private CompletableFuture<BookKeeperVersionedValue<BookKeeperAppendReservationRecord>> findReservationByRange(
            StreamId stream,
            BookKeeperEntryRangeReadTarget target,
            int rangeSlot) {
        return findReservationByRange(stream, target, rangeSlot, Optional.empty());
    }

    private CompletableFuture<BookKeeperVersionedValue<BookKeeperAppendReservationRecord>> findReservationByRange(
            StreamId stream,
            BookKeeperEntryRangeReadTarget target,
            int rangeSlot,
            Optional<BookKeeperScanToken> continuation) {
        int pageSize = Math.min(configuration.retentionPageSize(), 1_024);
        return writerMetadata.scanReservations(cluster, stream, continuation, pageSize).thenCompose(page -> {
            BookKeeperVersionedValue<BookKeeperAppendReservationRecord> match = null;
            for (BookKeeperVersionedValue<BookKeeperAppendReservationRecord> candidate : page.values()) {
                BookKeeperAppendReservationRecord value = candidate.value();
                if (value.ledgerId() == target.ledgerId()
                        && value.ledgerRangeSlot() == rangeSlot
                        && value.firstEntryId() == target.firstEntryId()
                        && value.entryCount() == target.entryCount()
                        && value.rangeChecksumSha256().equals(target.rangeChecksum().value())) {
                    if (match != null) {
                        return CompletableFuture.failedFuture(invariant(
                                "multiple reservations describe one BookKeeper range"));
                    }
                    match = candidate;
                }
            }
            if (match != null) return CompletableFuture.completedFuture(match);
            if (page.continuation().isPresent()) {
                return findReservationByRange(stream, target, rangeSlot, page.continuation());
            }
            return CompletableFuture.failedFuture(invariant("BookKeeper range reservation is absent"));
        });
    }

    private BookKeeperPhysicalReferenceProof proof(
            PhysicalReferencePurpose purpose,
            Checksum targetIdentity,
            String referenceId,
            BookKeeperVersionedValue<BookKeeperLedgerRootRecord> root,
            BookKeeperVersionedValue<BookKeeperLedgerProtectionRecord> protection) {
        return new BookKeeperPhysicalReferenceProof(
                purpose,
                targetIdentity,
                referenceId,
                configuration.providerScopeSha256(),
                root.value().ledgerIdentitySha256(),
                configuration.clusterAlias(),
                root.value().ledgerId(),
                root.value().lifecycleEpoch(),
                protection.value().ledgerRangeSlot(),
                protection.value().protectionSlot(),
                root.metadataVersion(),
                root.durableValueSha256(),
                protection.metadataVersion(),
                protection.durableValueSha256());
    }

    private static void requireRoot(
            BookKeeperLedgerRootRecord root,
            StreamId stream,
            BookKeeperEntryRangeReadTarget target,
            boolean requireActive) {
        boolean readable = root.lifecycle() == BookKeeperLedgerLifecycle.ACTIVE
                || root.lifecycle() == BookKeeperLedgerLifecycle.SEALING
                || root.lifecycle() == BookKeeperLedgerLifecycle.SEALED;
        if ((requireActive && root.lifecycle() != BookKeeperLedgerLifecycle.ACTIVE)
                || (!requireActive && !readable)
                || root.ledgerId() != target.ledgerId()
                || !root.clusterAlias().equals(target.clusterAlias())
                || !root.streamId().equals(stream.value())) {
            throw invariant("BookKeeper ledger root cannot protect the supplied range");
        }
    }

    private static void requireReservation(
            BookKeeperAppendReservationRecord reservation,
            StreamId stream,
            BookKeeperEntryRangeReadTarget target,
            int rangeSlot) {
        if (!reservation.streamId().equals(stream.value())
                || reservation.ledgerId() != target.ledgerId()
                || reservation.ledgerRangeSlot() != rangeSlot
                || reservation.firstEntryId() != target.firstEntryId()
                || reservation.entryCount() != target.entryCount()
                || !reservation.rangeChecksumSha256().equals(target.rangeChecksum().value())) {
            throw invariant("BookKeeper reservation does not describe the protected range");
        }
    }

    private static void requireCommitOwner(
            BookKeeperAppendReservationRecord reservation,
            PreparedStableAppend prepared) {
        if (!reservation.commitId().equals(prepared.commitId())
                || !reservation.commitKey().equals(prepared.commitKey())
                || reservation.commitMetadataVersion() != prepared.commitMetadataVersion()
                || !reservation.commitRecordSha256().equals(prepared.commitRecordSha256().value())) {
            throw invariant("BookKeeper reservation commit owner changed");
        }
    }

    private static void requirePreparedReservation(
            BookKeeperAppendReservationRecord reservation,
            PreparedStableAppend prepared) {
        var request = prepared.request();
        if (!reservation.writerId().equals(request.writerId())
                || reservation.appendSessionEpoch() != request.epoch()
                || !reservation.fencingTokenHash().equals(
                        BookKeeperIdentityDigests.sha256(request.fencingToken()))
                || reservation.expectedStartOffset() != request.expectedStartOffset()
                || !reservation.payloadFormat().equals(request.payloadFormat().name())
                || reservation.recordCount() != request.recordCount()
                || reservation.entryCount() != request.entryCount()
                || reservation.logicalBytes() != request.logicalBytes()
                || !reservation.schemaRefs().equals(request.schemaRefs())
                || !reservation.projectionIdentity().equals(request.projectionIdentity())
                || reservation.minEventTimeMillis() != request.minEventTimeMillis()
                || reservation.maxEventTimeMillis() != request.maxEventTimeMillis()) {
            throw invariant("BookKeeper reservation logical/session facts do not match the commit intent");
        }
    }

    private static void requireCommittedReservation(
            BookKeeperAppendReservationRecord reservation,
            MaterializedGenerationZero materialized,
            BookKeeperEntryRangeReadTarget target) {
        var committed = materialized.committedAppend();
        if (!committed.readTarget().equals(target)
                || reservation.expectedStartOffset() != committed.range().startOffset()
                || reservation.recordCount() != committed.recordCount()
                || reservation.entryCount() != committed.entryCount()
                || reservation.logicalBytes() != committed.logicalBytes()
                || !reservation.payloadFormat().equals(committed.payloadFormat().name())
                || !reservation.schemaRefs().equals(committed.schemaRefs())
                || reservation.minEventTimeMillis() != committed.minEventTimeMillis()
                || reservation.maxEventTimeMillis() != committed.maxEventTimeMillis()) {
            throw invariant("BookKeeper reservation does not match the reachable committed append");
        }
    }

    private static void requireActiveOwner(
            BookKeeperLedgerProtectionRecord protection,
            String referenceId,
            String ownerKey,
            long ownerMetadataVersion,
            String ownerIdentitySha256) {
        if (protection.lifecycle() != ProtectionLifecycle.ACTIVE
                || !protection.referenceId().equals(referenceId)
                || !protection.ownerKey().equals(ownerKey)
                || protection.ownerMetadataVersion() != ownerMetadataVersion
                || !protection.ownerIdentitySha256().equals(ownerIdentitySha256)) {
            throw invariant("BookKeeper protection is active under another owner");
        }
    }

    private BookKeeperAppendReservationRecord withPreparedCommit(
            BookKeeperAppendReservationRecord before,
            PreparedStableAppend prepared) {
        return replaceReservation(
                before,
                AppendReservationLifecycle.COMMIT_PREPARED,
                prepared.commitId(),
                prepared.commitKey(),
                prepared.commitMetadataVersion(),
                prepared.commitRecordSha256().value());
    }

    private BookKeeperAppendReservationRecord withLifecycle(
            BookKeeperAppendReservationRecord before,
            AppendReservationLifecycle lifecycle) {
        return replaceReservation(
                before,
                lifecycle,
                before.commitId(),
                before.commitKey(),
                before.commitMetadataVersion(),
                before.commitRecordSha256());
    }

    private BookKeeperAppendReservationRecord replaceReservation(
            BookKeeperAppendReservationRecord before,
            AppendReservationLifecycle lifecycle,
            String commitId,
            String commitKey,
            long commitMetadataVersion,
            String commitRecordSha256) {
        return new BookKeeperAppendReservationRecord(
                before.schemaVersion(), before.reservationId(), before.appendAttemptId(), before.streamId(),
                before.writerId(), before.writerRunIdHash(), before.appendSessionEpoch(), before.fencingTokenHash(),
                before.writerStateEpoch(), before.ledgerId(), before.ledgerRootEpoch(), before.ledgerRangeSlot(),
                before.firstEntryId(), before.entryCount(), before.rangeChecksumSha256(),
                before.expectedStartOffset(), before.payloadFormat(), before.recordCount(), before.logicalBytes(),
                before.physicalBytes(), before.schemaRefs(), before.projectionIdentity(),
                before.minEventTimeMillis(), before.maxEventTimeMillis(), lifecycle, commitId, commitKey,
                commitMetadataVersion, commitRecordSha256, before.createdAtMillis(), clock.millis(), "", 0);
    }

    private static String reachableReferenceId(PreparedStableAppend prepared) {
        return "ra1-" + DeterministicIds.stableHashComponent(
                prepared.request().streamId().value()
                        + prepared.commitId()
                        + prepared.primaryTargetIdentitySha256().value());
    }

    private static String visibleReferenceId(MaterializedGenerationZero materialized) {
        return "vg0-" + DeterministicIds.stableHashComponent(
                materialized.committedAppend().streamId().value()
                        + materialized.indexKey()
                        + materialized.indexRecordSha256().value());
    }

    private BookKeeperEntryRangeReadTarget exactTarget(BookKeeperEntryRangeReadTarget target) {
        BookKeeperEntryRangeReadTarget exact = Objects.requireNonNull(target, "target");
        if (!exact.clusterAlias().equals(configuration.clusterAlias())) {
            throw invariant("BookKeeper target resolves to another configured cluster alias");
        }
        return exact;
    }

    private BookKeeperOperationDeadline deadline(Duration timeout) {
        return new BookKeeperOperationDeadline(min(
                Objects.requireNonNull(timeout, "timeout"),
                configuration.operationTimeout()));
    }

    private static Duration min(Duration left, Duration right) {
        return left.compareTo(right) <= 0 ? left : right;
    }

    private static NereusException invariant(String message) {
        return new NereusException(ErrorCode.METADATA_INVARIANT_VIOLATION, false, message);
    }

    private static String text(String value, String name) {
        Objects.requireNonNull(value, name);
        if (value.isBlank()) throw new IllegalArgumentException(name + " cannot be blank");
        return value;
    }
}
