/* Licensed under the Apache License, Version 2.0 */
package com.nereusstream.bookkeeper;

import com.nereusstream.api.ErrorCode;
import com.nereusstream.api.NereusException;
import com.nereusstream.api.StreamId;
import com.nereusstream.metadata.oxia.BookKeeperKeyspace;
import com.nereusstream.metadata.oxia.BookKeeperLedgerMetadataStore;
import com.nereusstream.metadata.oxia.BookKeeperMetadataStoreConfig;
import com.nereusstream.metadata.oxia.BookKeeperScanToken;
import com.nereusstream.metadata.oxia.BookKeeperVersionedValue;
import com.nereusstream.metadata.oxia.BookKeeperWriterMetadataStore;
import com.nereusstream.metadata.oxia.records.AllocationSlotLifecycle;
import com.nereusstream.metadata.oxia.records.BookKeeperAllocationSlotRecord;
import com.nereusstream.metadata.oxia.records.BookKeeperLedgerLifecycle;
import com.nereusstream.metadata.oxia.records.BookKeeperLedgerRootRecord;
import com.nereusstream.metadata.oxia.records.LedgerAllocationIntentRecord;
import com.nereusstream.metadata.oxia.records.LedgerAllocationLifecycle;
import java.time.Clock;
import java.time.Duration;
import java.util.Objects;
import java.util.Optional;
import java.util.concurrent.CompletableFuture;
import org.apache.bookkeeper.client.api.LedgerMetadata;

/** Bounded fixed-slot reconciliation for create requests whose provider outcome was uncertain. */
public final class BookKeeperUncertainAllocationReconciler {
    private final String cluster;
    private final BookKeeperWalConfiguration configuration;
    private final BookKeeperWriterMetadataStore writerMetadata;
    private final BookKeeperLedgerMetadataStore ledgerMetadata;
    private final BookKeeperLedgerIdNamespaceReservationVerifier namespaceVerifier;
    private final BookKeeperClientOperations client;
    private final BookKeeperLedgerRecovery ledgerRecovery;
    private final Clock clock;
    private final BookKeeperKeyspace keys;

    public BookKeeperUncertainAllocationReconciler(
            String cluster,
            BookKeeperWalConfiguration configuration,
            BookKeeperWriterMetadataStore writerMetadata,
            BookKeeperLedgerMetadataStore ledgerMetadata,
            BookKeeperLedgerIdNamespaceReservationVerifier namespaceVerifier,
            BookKeeperClientOperations client,
            BookKeeperLedgerRecovery ledgerRecovery,
            Clock clock) {
        this.cluster = text(cluster, "cluster");
        this.configuration = Objects.requireNonNull(configuration, "configuration");
        this.writerMetadata = Objects.requireNonNull(writerMetadata, "writerMetadata");
        this.ledgerMetadata = Objects.requireNonNull(ledgerMetadata, "ledgerMetadata");
        this.namespaceVerifier = Objects.requireNonNull(namespaceVerifier, "namespaceVerifier");
        this.client = Objects.requireNonNull(client, "client");
        this.ledgerRecovery = Objects.requireNonNull(ledgerRecovery, "ledgerRecovery");
        this.clock = Objects.requireNonNull(clock, "clock");
        this.keys = new BookKeeperMetadataStoreConfig(
                        configuration.maxAppendRangesPerLedger(),
                        configuration.protectionSlotsPerRange(),
                        configuration.maxReaderLeasesPerLedger(),
                        configuration.maxUncertainAllocations())
                .keyspace(cluster);
    }

    public CompletableFuture<BookKeeperUncertainAllocationRecoveryResult> reconcile(Duration timeout) {
        BookKeeperOperationDeadline deadline = new BookKeeperOperationDeadline(
                min(Objects.requireNonNull(timeout, "timeout"), configuration.allocationTimeout()));
        MutableResult result = new MutableResult();
        return namespaceVerifier.requireActive(configuration, deadline.remaining())
                .thenCompose(namespace -> scanShards(namespace, 0, deadline, result))
                .thenApply(ignored -> result.snapshot());
    }

    private CompletableFuture<Void> scanShards(
            BookKeeperLedgerIdNamespaceReservation namespace,
            int shard,
            BookKeeperOperationDeadline deadline,
            MutableResult result) {
        if (shard == BookKeeperKeyspace.ALLOCATION_SLOT_SHARDS) {
            return CompletableFuture.completedFuture(null);
        }
        return scanPage(namespace, shard, Optional.empty(), deadline, result)
                .thenCompose(ignored -> scanShards(namespace, shard + 1, deadline, result));
    }

    private CompletableFuture<Void> scanPage(
            BookKeeperLedgerIdNamespaceReservation namespace,
            int shard,
            Optional<BookKeeperScanToken> continuation,
            BookKeeperOperationDeadline deadline,
            MutableResult result) {
        int pageSize = Math.min(configuration.retentionPageSize(), 1_024);
        return deadline.bound(writerMetadata.scanAllocationSlots(cluster, shard, continuation, pageSize))
                .thenCompose(page -> {
                    CompletableFuture<Void> chain = CompletableFuture.completedFuture(null);
                    for (BookKeeperVersionedValue<BookKeeperAllocationSlotRecord> slot : page.values()) {
                        result.scannedSlots++;
                        if (slot.value().lifecycle() == AllocationSlotLifecycle.CREATE_UNCERTAIN) {
                            result.uncertainSlots++;
                            chain = chain.thenCompose(ignored -> reconcileSlot(namespace, slot, deadline, result));
                        }
                    }
                    return chain.thenCompose(ignored -> page.continuation().isPresent()
                            ? scanPage(namespace, shard, page.continuation(), deadline, result)
                            : CompletableFuture.completedFuture(null));
                });
    }

    private CompletableFuture<Void> reconcileSlot(
            BookKeeperLedgerIdNamespaceReservation namespace,
            BookKeeperVersionedValue<BookKeeperAllocationSlotRecord> slot,
            BookKeeperOperationDeadline deadline,
            MutableResult result) {
        BookKeeperAllocationSlotRecord slotValue = slot.value();
        requireSlot(slotValue);
        StreamId stream = new StreamId(slotValue.streamId());
        CompletableFuture<Optional<BookKeeperVersionedValue<LedgerAllocationIntentRecord>>> allocationFuture =
                writerMetadata.getAllocation(cluster, stream, slotValue.allocationId());
        CompletableFuture<Optional<BookKeeperVersionedValue<BookKeeperLedgerRootRecord>>> rootFuture =
                ledgerMetadata.getRoot(
                        cluster, configuration.providerScopeSha256(), slotValue.candidateLedgerId());
        return deadline.bound(CompletableFuture.allOf(allocationFuture, rootFuture)).thenCompose(ignored -> {
            BookKeeperVersionedValue<LedgerAllocationIntentRecord> allocation = allocationFuture.join().orElseThrow(
                    () -> invariant("CREATE_UNCERTAIN slot has no allocation intent"));
            BookKeeperVersionedValue<BookKeeperLedgerRootRecord> root = rootFuture.join().orElseThrow(
                    () -> invariant("CREATE_UNCERTAIN slot has no ledger root"));
            requireIdentity(namespace, slotValue, allocation.value(), root.value());
            if (allocation.value().lifecycle() == LedgerAllocationLifecycle.FOREIGN_COLLISION
                    || root.value().lifecycle() == BookKeeperLedgerLifecycle.QUARANTINED) {
                result.quarantinedLedgers++;
                return CompletableFuture.completedFuture(null);
            }
            return probe(namespace, allocation, root, deadline).thenAccept(outcome -> {
                switch (outcome) {
                    case ABSENT -> result.absentLedgers++;
                    case RECOVERED -> result.recoveredLedgers++;
                    case QUARANTINED -> result.quarantinedLedgers++;
                }
            });
        });
    }

    private CompletableFuture<Outcome> probe(
            BookKeeperLedgerIdNamespaceReservation namespace,
            BookKeeperVersionedValue<LedgerAllocationIntentRecord> allocation,
            BookKeeperVersionedValue<BookKeeperLedgerRootRecord> root,
            BookKeeperOperationDeadline deadline) {
        CompletableFuture<LedgerMetadata> metadata;
        try {
            metadata = client.metadata(root.value().ledgerId(), deadline);
        } catch (Throwable failure) {
            return absentOrFailure(failure);
        }
        return metadata.handle((value, failure) -> {
                    if (failure != null) {
                        return absentOrFailure(failure);
                    }
                    BookKeeperLedgerCustomMetadata expected = BookKeeperLedgerCustomMetadata.create(
                            cluster,
                            configuration,
                            namespace,
                            new StreamId(root.value().streamId()),
                            root.value().segmentSequence(),
                            root.value().allocationId());
                    try {
                        String metadataSha256 = expected.requireExactImmutableLedgerMetadata(
                                root.value().ledgerId(), configuration, value).value();
                        return recoverMatching(allocation, root, metadataSha256, deadline);
                    } catch (Throwable mismatch) {
                        return quarantine(allocation, root, "late BookKeeper create has foreign metadata");
                    }
                })
                .thenCompose(java.util.function.Function.identity());
    }

    private CompletableFuture<Outcome> recoverMatching(
            BookKeeperVersionedValue<LedgerAllocationIntentRecord> allocation,
            BookKeeperVersionedValue<BookKeeperLedgerRootRecord> root,
            String metadataSha256,
            BookKeeperOperationDeadline deadline) {
        CompletableFuture<BookKeeperVersionedValue<LedgerAllocationIntentRecord>> physical;
        if (allocation.value().lifecycle() == LedgerAllocationLifecycle.CREATE_UNCERTAIN) {
            physical = writerMetadata.compareAndSetAllocation(
                    cluster,
                    replaceAllocation(
                            allocation.value(),
                            LedgerAllocationLifecycle.PHYSICAL_CREATED,
                            metadataSha256,
                            "late matching provider create recovered without writable handle"),
                    allocation.metadataVersion());
        } else if (allocation.value().lifecycle() == LedgerAllocationLifecycle.PHYSICAL_CREATED) {
            physical = CompletableFuture.completedFuture(allocation);
        } else {
            return CompletableFuture.failedFuture(invariant(
                    "uncertain allocation has an invalid matching-provider lifecycle"));
        }
        return physical.thenCompose(ignored -> {
            if (root.value().lifecycle() == BookKeeperLedgerLifecycle.SEALED) {
                return CompletableFuture.completedFuture(Outcome.RECOVERED);
            }
            if (root.value().lifecycle() != BookKeeperLedgerLifecycle.ALLOCATING
                    && root.value().lifecycle() != BookKeeperLedgerLifecycle.SEALING) {
                return CompletableFuture.failedFuture(invariant(
                        "matching uncertain ledger root cannot be recovery-sealed"));
            }
            return ledgerRecovery.sealUnowned(
                            root, deadline.remaining(), "bounded late CreateAdv reconciliation")
                    .thenApply(sealed -> Outcome.RECOVERED);
        });
    }

    private CompletableFuture<Outcome> quarantine(
            BookKeeperVersionedValue<LedgerAllocationIntentRecord> allocation,
            BookKeeperVersionedValue<BookKeeperLedgerRootRecord> root,
            String reason) {
        CompletableFuture<BookKeeperVersionedValue<BookKeeperLedgerRootRecord>> quarantinedRoot;
        if (root.value().lifecycle() == BookKeeperLedgerLifecycle.QUARANTINED) {
            quarantinedRoot = CompletableFuture.completedFuture(root);
        } else {
            quarantinedRoot = ledgerMetadata.compareAndSetRoot(
                    cluster,
                    replaceRoot(root.value(), BookKeeperLedgerLifecycle.QUARANTINED, reason),
                    root.metadataVersion());
        }
        return quarantinedRoot.thenCompose(ignored -> {
            if (allocation.value().lifecycle() == LedgerAllocationLifecycle.FOREIGN_COLLISION) {
                return CompletableFuture.completedFuture(Outcome.QUARANTINED);
            }
            return writerMetadata.compareAndSetAllocation(
                            cluster,
                            replaceAllocation(
                                    allocation.value(),
                                    LedgerAllocationLifecycle.FOREIGN_COLLISION,
                                    allocation.value().bookKeeperMetadataSha256(),
                                    reason),
                            allocation.metadataVersion())
                    .thenApply(value -> Outcome.QUARANTINED);
        });
    }

    private void requireSlot(BookKeeperAllocationSlotRecord slot) {
        if (!configuration.ledgerIdNamespace().contains(slot.candidateLedgerId())
                || !slot.configurationBindingSha256().equals(configuration.configurationBindingSha256().value())
                || !slot.ledgerIdentitySha256().equals(
                        keys.ledgerIdentitySha256(configuration.providerScopeSha256(), slot.candidateLedgerId()))) {
            throw invariant("CREATE_UNCERTAIN allocation slot binding drifted");
        }
    }

    private void requireIdentity(
            BookKeeperLedgerIdNamespaceReservation namespace,
            BookKeeperAllocationSlotRecord slot,
            LedgerAllocationIntentRecord allocation,
            BookKeeperLedgerRootRecord root) {
        if (!allocation.allocationId().equals(slot.allocationId())
                || !allocation.streamId().equals(slot.streamId())
                || allocation.candidateLedgerId() != slot.candidateLedgerId()
                || allocation.allocationSlot() != slot.slot()
                || !allocation.configurationBindingSha256().equals(slot.configurationBindingSha256())
                || !allocation.lateCreateHazard()
                || !root.allocationId().equals(slot.allocationId())
                || !root.streamId().equals(slot.streamId())
                || root.ledgerId() != slot.candidateLedgerId()
                || root.allocationSlot() != slot.slot()
                || !root.ledgerIdentitySha256().equals(slot.ledgerIdentitySha256())
                || !root.configurationBindingSha256().equals(slot.configurationBindingSha256())
                || !root.ledgerIdNamespaceSha256().equals(namespace.ledgerIdNamespaceSha256().value())
                || !root.lateCreateHazard()) {
            throw invariant("CREATE_UNCERTAIN slot/allocation/root identity drifted");
        }
    }

    private LedgerAllocationIntentRecord replaceAllocation(
            LedgerAllocationIntentRecord before,
            LedgerAllocationLifecycle lifecycle,
            String metadataSha256,
            String reason) {
        return new LedgerAllocationIntentRecord(
                before.schemaVersion(),
                before.allocationId(),
                before.streamId(),
                before.segmentSequence(),
                before.clusterAlias(),
                before.candidateLedgerId(),
                before.allocationSlot(),
                before.configurationBindingSha256(),
                before.writerId(),
                before.writerRunIdHash(),
                before.appendSessionEpoch(),
                before.fencingTokenHash(),
                before.writerStateEpoch(),
                lifecycle,
                true,
                metadataSha256,
                before.createdAtMillis(),
                clock.millis(),
                reason,
                0);
    }

    private BookKeeperLedgerRootRecord replaceRoot(
            BookKeeperLedgerRootRecord before,
            BookKeeperLedgerLifecycle lifecycle,
            String reason) {
        return new BookKeeperLedgerRootRecord(
                before.schemaVersion(),
                before.ledgerIdentitySha256(),
                before.clusterAlias(),
                before.providerScopeSha256(),
                before.ledgerId(),
                before.streamId(),
                before.segmentSequence(),
                before.allocationId(),
                before.allocationSlot(),
                before.configurationBindingSha256(),
                before.ledgerIdNamespaceSha256(),
                before.lateCreateHazard(),
                before.writerId(),
                before.writerRunIdHash(),
                before.appendSessionEpoch(),
                before.fencingTokenHash(),
                before.ensembleSize(),
                before.writeQuorumSize(),
                before.ackQuorumSize(),
                before.digestType(),
                before.customMetadataSha256(),
                lifecycle,
                before.lifecycleEpoch() + 1,
                before.createdAtMillis(),
                before.activatedAtMillis(),
                before.sealStartedAtMillis(),
                before.sealedAtMillis(),
                before.sealedLastEntryId(),
                before.sealedLength(),
                before.sealReason(),
                before.gcAttemptId(),
                before.referenceSetSha256(),
                before.markedAtMillis(),
                before.deleteNotBeforeMillis(),
                before.deleteStartedAtMillis(),
                before.firstAbsentAtMillis(),
                before.deletedAtMillis(),
                reason,
                0);
    }

    private static CompletableFuture<Outcome> absentOrFailure(Throwable failure) {
        Throwable cause = unwrap(failure);
        if (cause instanceof NereusException nereus
                && nereus.code() == ErrorCode.PRIMARY_WAL_TARGET_NOT_FOUND) {
            return CompletableFuture.completedFuture(Outcome.ABSENT);
        }
        return CompletableFuture.failedFuture(cause);
    }

    private static Throwable unwrap(Throwable failure) {
        Throwable current = failure;
        while ((current instanceof java.util.concurrent.CompletionException
                        || current instanceof java.util.concurrent.ExecutionException)
                && current.getCause() != null) {
            current = current.getCause();
        }
        return current;
    }

    private static NereusException invariant(String message) {
        return new NereusException(ErrorCode.METADATA_INVARIANT_VIOLATION, false, message);
    }

    private static Duration min(Duration left, Duration right) {
        return left.compareTo(right) <= 0 ? left : right;
    }

    private static String text(String value, String name) {
        Objects.requireNonNull(value, name);
        if (value.isBlank()) {
            throw new IllegalArgumentException(name + " cannot be blank");
        }
        return value;
    }

    private enum Outcome {
        ABSENT,
        RECOVERED,
        QUARANTINED
    }

    private static final class MutableResult {
        private int scannedSlots;
        private int uncertainSlots;
        private int absentLedgers;
        private int recoveredLedgers;
        private int quarantinedLedgers;

        private BookKeeperUncertainAllocationRecoveryResult snapshot() {
            return new BookKeeperUncertainAllocationRecoveryResult(
                    scannedSlots,
                    uncertainSlots,
                    absentLedgers,
                    recoveredLedgers,
                    quarantinedLedgers);
        }
    }
}
