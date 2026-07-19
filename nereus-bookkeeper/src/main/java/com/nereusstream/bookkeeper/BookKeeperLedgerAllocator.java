/* Licensed under the Apache License, Version 2.0 */
package com.nereusstream.bookkeeper;

import com.nereusstream.api.AppendSession;
import com.nereusstream.api.ErrorCode;
import com.nereusstream.api.NereusException;
import com.nereusstream.api.StreamId;
import com.nereusstream.metadata.oxia.BookKeeperKeyspace;
import com.nereusstream.metadata.oxia.BookKeeperLedgerMetadataStore;
import com.nereusstream.metadata.oxia.BookKeeperMetadataConditionFailedException;
import com.nereusstream.metadata.oxia.BookKeeperMetadataStoreConfig;
import com.nereusstream.metadata.oxia.BookKeeperVersionedValue;
import com.nereusstream.metadata.oxia.BookKeeperWriterMetadataStore;
import com.nereusstream.metadata.oxia.records.AllocationSlotLifecycle;
import com.nereusstream.metadata.oxia.records.BookKeeperAllocationSlotRecord;
import com.nereusstream.metadata.oxia.records.BookKeeperLedgerLifecycle;
import com.nereusstream.metadata.oxia.records.BookKeeperLedgerRootRecord;
import com.nereusstream.metadata.oxia.records.BookKeeperWriterStateRecord;
import com.nereusstream.metadata.oxia.records.LedgerAllocationIntentRecord;
import com.nereusstream.metadata.oxia.records.LedgerAllocationLifecycle;
import java.time.Clock;
import java.time.Duration;
import java.util.Arrays;
import java.util.Objects;
import java.util.concurrent.CompletableFuture;
import java.util.function.Supplier;
import java.util.random.RandomGenerator;
import org.apache.bookkeeper.client.api.LedgerMetadata;
import org.apache.bookkeeper.client.api.WriteAdvHandle;

/** Reserved-id, fixed-slot BookKeeper allocator. No entry write is admitted by this class. */
public final class BookKeeperLedgerAllocator {
    private static final int MAX_CANDIDATE_ATTEMPTS = 64;
    private static final char[] BASE32 = "abcdefghijklmnopqrstuvwxyz234567".toCharArray();

    private final String cluster;
    private final BookKeeperWalConfiguration configuration;
    private final BookKeeperWriterMetadataStore writerMetadata;
    private final BookKeeperLedgerMetadataStore ledgerMetadata;
    private final BookKeeperLedgerIdNamespaceReservationVerifier namespaceVerifier;
    private final BookKeeperClientOperations client;
    private final BookKeeperPasswordProvider passwordProvider;
    private final BookKeeperWriterStateMachine writerState;
    private final BookKeeperLedgerRecovery allocationRecovery;
    private final BookKeeperUncertainAllocationReconciler uncertainAllocations;
    private final Clock clock;
    private final RandomGenerator random;
    private final Supplier<String> allocationIds;
    private final BookKeeperKeyspace keys;
    private final String configurationBindingSha256;

    public BookKeeperLedgerAllocator(
            String cluster,
            BookKeeperWalConfiguration configuration,
            BookKeeperWriterMetadataStore writerMetadata,
            BookKeeperLedgerMetadataStore ledgerMetadata,
            BookKeeperLedgerIdNamespaceReservationVerifier namespaceVerifier,
            BookKeeperClientOperations client,
            BookKeeperPasswordProvider passwordProvider,
            BookKeeperWriterStateMachine writerState,
            Clock clock) {
        this(cluster, configuration, writerMetadata, ledgerMetadata, namespaceVerifier, client, passwordProvider,
                writerState, clock, new java.security.SecureRandom(), null);
    }

    BookKeeperLedgerAllocator(
            String cluster,
            BookKeeperWalConfiguration configuration,
            BookKeeperWriterMetadataStore writerMetadata,
            BookKeeperLedgerMetadataStore ledgerMetadata,
            BookKeeperLedgerIdNamespaceReservationVerifier namespaceVerifier,
            BookKeeperClientOperations client,
            BookKeeperPasswordProvider passwordProvider,
            BookKeeperWriterStateMachine writerState,
            Clock clock,
            RandomGenerator random,
            Supplier<String> allocationIds) {
        this.cluster = text(cluster, "cluster");
        this.configuration = Objects.requireNonNull(configuration, "configuration");
        this.writerMetadata = Objects.requireNonNull(writerMetadata, "writerMetadata");
        this.ledgerMetadata = Objects.requireNonNull(ledgerMetadata, "ledgerMetadata");
        this.namespaceVerifier = Objects.requireNonNull(namespaceVerifier, "namespaceVerifier");
        this.client = Objects.requireNonNull(client, "client");
        this.passwordProvider = Objects.requireNonNull(passwordProvider, "passwordProvider");
        this.writerState = Objects.requireNonNull(writerState, "writerState");
        this.clock = Objects.requireNonNull(clock, "clock");
        this.allocationRecovery = new BookKeeperLedgerRecovery(
                cluster,
                configuration,
                writerMetadata,
                ledgerMetadata,
                namespaceVerifier,
                client,
                passwordProvider,
                writerState,
                clock);
        this.uncertainAllocations = new BookKeeperUncertainAllocationReconciler(
                cluster,
                configuration,
                writerMetadata,
                ledgerMetadata,
                namespaceVerifier,
                client,
                allocationRecovery,
                clock);
        this.random = Objects.requireNonNull(random, "random");
        this.allocationIds = allocationIds == null ? this::newAllocationId : allocationIds;
        BookKeeperMetadataStoreConfig metadataConfig = new BookKeeperMetadataStoreConfig(
                configuration.maxAppendRangesPerLedger(), configuration.protectionSlotsPerRange(),
                configuration.maxReaderLeasesPerLedger(), configuration.maxUncertainAllocations());
        this.keys = metadataConfig.keyspace(cluster);
        this.configurationBindingSha256 = configuration.configurationBindingSha256().value();
    }

    public CompletableFuture<AllocatedBookKeeperLedger> allocate(BookKeeperLedgerAllocationRequest request) {
        BookKeeperLedgerAllocationRequest exact = Objects.requireNonNull(request, "request");
        Duration budget = min(exact.timeout(), configuration.allocationTimeout());
        BookKeeperOperationDeadline deadline = new BookKeeperOperationDeadline(budget);
        return namespaceVerifier.requireActive(configuration, deadline.remaining())
                .thenCompose(reservation -> writerState.requireIdle(exact.session())
                        .thenCompose(writer -> allocateCandidate(exact, reservation, writer, deadline, 0)));
    }

    /** Reconciles every fixed uncertainty slot without clearing its permanent provider-transmission hazard. */
    public CompletableFuture<BookKeeperUncertainAllocationRecoveryResult> reconcileUncertainAllocations(
            Duration timeout) {
        return uncertainAllocations.reconcile(timeout);
    }

    private CompletableFuture<AllocatedBookKeeperLedger> allocateCandidate(
            BookKeeperLedgerAllocationRequest request,
            BookKeeperLedgerIdNamespaceReservation namespace,
            BookKeeperVersionedValue<BookKeeperWriterStateRecord> idleWriter,
            BookKeeperOperationDeadline deadline,
            int attempt) {
        if (attempt >= MAX_CANDIDATE_ATTEMPTS) {
            return failed(ErrorCode.METADATA_LIMIT_EXCEEDED,
                    "BookKeeper ledger allocation exhausted its bounded candidate attempts");
        }
        String allocationId = text(allocationIds.get(), "allocationId");
        long candidate = candidate();
        if (!configuration.ledgerIdNamespace().contains(candidate)) {
            return failed(ErrorCode.METADATA_INVARIANT_VIOLATION,
                    "BookKeeper candidate escaped its provisioned ledger-id namespace");
        }
        String ledgerIdentity = keys.ledgerIdentitySha256(configuration.providerScopeSha256(), candidate);
        long segmentSequence = idleWriter.value().nextSegmentSequence();
        BookKeeperLedgerCustomMetadata customMetadata = BookKeeperLedgerCustomMetadata.create(
                cluster, configuration, namespace, request.streamId(), segmentSequence, allocationId);
        return claimSlot(request.streamId(), allocationId, candidate, ledgerIdentity, deadline)
                .thenCompose(slot -> createIntent(
                        request, allocationId, candidate, segmentSequence, slot, idleWriter)
                        .thenCompose(intent -> writerState.claimAllocation(
                                        idleWriter, request.session(), allocationId, candidate)
                                .thenCompose(claimedWriter -> reserveRootAndCreate(
                                        request, namespace, customMetadata, slot, intent, claimedWriter,
                                        deadline, attempt))));
    }

    private CompletableFuture<BookKeeperVersionedValue<BookKeeperAllocationSlotRecord>> claimSlot(
            StreamId stream,
            String allocationId,
            long candidate,
            String ledgerIdentity,
            BookKeeperOperationDeadline deadline) {
        int maximum = configuration.maxUncertainAllocations();
        int hash = Integer.parseUnsignedInt(BookKeeperIdentityDigests.sha256(allocationId).substring(0, 8), 16);
        int start = Integer.remainderUnsigned(hash, maximum);
        return claimSlot(stream, allocationId, candidate, ledgerIdentity, start, 0, deadline);
    }

    private CompletableFuture<BookKeeperVersionedValue<BookKeeperAllocationSlotRecord>> claimSlot(
            StreamId stream,
            String allocationId,
            long candidate,
            String ledgerIdentity,
            int start,
            int probe,
            BookKeeperOperationDeadline deadline) {
        deadline.remaining();
        int maximum = configuration.maxUncertainAllocations();
        if (probe >= maximum) {
            return failed(ErrorCode.METADATA_LIMIT_EXCEEDED,
                    "all fixed BookKeeper allocation uncertainty slots are occupied");
        }
        int slot = (start + probe) % maximum;
        long now = clock.millis();
        BookKeeperAllocationSlotRecord desired = new BookKeeperAllocationSlotRecord(
                1, slot, allocationId, stream.value(), candidate, ledgerIdentity,
                configurationBindingSha256, AllocationSlotLifecycle.CLAIMED, now, now, 0);
        return writerMetadata.createAllocationSlot(cluster, desired).exceptionallyCompose(failure -> {
            if (unwrap(failure) instanceof BookKeeperMetadataConditionFailedException) {
                return claimSlot(stream, allocationId, candidate, ledgerIdentity, start, probe + 1, deadline);
            }
            return CompletableFuture.failedFuture(unwrap(failure));
        });
    }

    private CompletableFuture<BookKeeperVersionedValue<LedgerAllocationIntentRecord>> createIntent(
            BookKeeperLedgerAllocationRequest request,
            String allocationId,
            long candidate,
            long segmentSequence,
            BookKeeperVersionedValue<BookKeeperAllocationSlotRecord> slot,
            BookKeeperVersionedValue<BookKeeperWriterStateRecord> idleWriter) {
        AppendSession session = request.session();
        long now = clock.millis();
        LedgerAllocationIntentRecord intent = new LedgerAllocationIntentRecord(
                1, allocationId, request.streamId().value(), segmentSequence, configuration.clusterAlias(),
                candidate, slot.value().slot(), configurationBindingSha256, session.writerId(),
                idleWriter.value().writerRunIdHash(),
                session.epoch(), BookKeeperIdentityDigests.sha256(session.fencingToken()),
                idleWriter.value().writerStateEpoch() + 1,
                LedgerAllocationLifecycle.PREPARED, false, "", now, now, "", 0);
        return writerMetadata.createAllocation(cluster, intent);
    }

    private CompletableFuture<AllocatedBookKeeperLedger> reserveRootAndCreate(
            BookKeeperLedgerAllocationRequest request,
            BookKeeperLedgerIdNamespaceReservation namespace,
            BookKeeperLedgerCustomMetadata customMetadata,
            BookKeeperVersionedValue<BookKeeperAllocationSlotRecord> slot,
            BookKeeperVersionedValue<LedgerAllocationIntentRecord> intent,
            BookKeeperVersionedValue<BookKeeperWriterStateRecord> claimedWriter,
            BookKeeperOperationDeadline deadline,
            int attempt) {
        BookKeeperLedgerRootRecord desiredRoot = allocatingRoot(
                request, namespace, customMetadata, slot, intent, claimedWriter);
        return ledgerMetadata.createRoot(cluster, desiredRoot).handle((root, failure) -> {
            if (failure == null) {
                return advanceToProviderCreate(
                        request, customMetadata, slot, intent, claimedWriter, root, deadline);
            }
            Throwable cause = unwrap(failure);
            if (cause instanceof BookKeeperMetadataConditionFailedException) {
                return abortPreTransmission(request, slot, intent, claimedWriter,
                                "candidate ledger identity is already reserved")
                        .thenCompose(idle -> allocateCandidate(
                                request, namespace, idle, deadline, attempt + 1));
            }
            return CompletableFuture.<AllocatedBookKeeperLedger>failedFuture(cause);
        }).thenCompose(java.util.function.Function.identity());
    }

    private CompletableFuture<AllocatedBookKeeperLedger> advanceToProviderCreate(
            BookKeeperLedgerAllocationRequest request,
            BookKeeperLedgerCustomMetadata customMetadata,
            BookKeeperVersionedValue<BookKeeperAllocationSlotRecord> slot,
            BookKeeperVersionedValue<LedgerAllocationIntentRecord> intent,
            BookKeeperVersionedValue<BookKeeperWriterStateRecord> claimedWriter,
            BookKeeperVersionedValue<BookKeeperLedgerRootRecord> root,
            BookKeeperOperationDeadline deadline) {
        return casIntent(intent, LedgerAllocationLifecycle.ROOT_RESERVED, false, "", "")
                .thenCompose(rootReserved -> {
                    byte[] password;
                    try {
                        password = Objects.requireNonNull(
                                passwordProvider.resolve(configuration.passwordRef()), "resolved password").clone();
                    } catch (Throwable failure) {
                        return abortOwnedPreTransmission(
                                request, slot, rootReserved, claimedWriter, root, failure);
                    }
                    return casSlot(slot, AllocationSlotLifecycle.CREATE_STARTED)
                            .thenCompose(createStarted -> invokeCreate(
                                    request, customMetadata, password, createStarted,
                                    rootReserved, claimedWriter, root, deadline))
                            .whenComplete((ignored, failure) -> Arrays.fill(password, (byte) 0));
                });
    }

    private CompletableFuture<AllocatedBookKeeperLedger> invokeCreate(
            BookKeeperLedgerAllocationRequest request,
            BookKeeperLedgerCustomMetadata customMetadata,
            byte[] password,
            BookKeeperVersionedValue<BookKeeperAllocationSlotRecord> slot,
            BookKeeperVersionedValue<LedgerAllocationIntentRecord> intent,
            BookKeeperVersionedValue<BookKeeperWriterStateRecord> claimedWriter,
            BookKeeperVersionedValue<BookKeeperLedgerRootRecord> root,
            BookKeeperOperationDeadline deadline) {
        CompletableFuture<WriteAdvHandle> create;
        try {
            create = client.createAdvanced(root.value().ledgerId(), configuration, password,
                    customMetadata.values(), deadline);
        } catch (Throwable failure) {
            return recordUncertain(request, customMetadata, slot, intent, claimedWriter, root, failure, deadline);
        }
        return create.handle((handle, failure) -> {
                    if (failure != null) {
                        return recordUncertain(
                                request, customMetadata, slot, intent, claimedWriter, root, failure, deadline);
                    }
                    try {
                        if (handle.getId() != root.value().ledgerId()) {
                            throw new NereusException(ErrorCode.METADATA_INVARIANT_VIOLATION, false,
                                    "CreateAdv returned a handle for another ledger id");
                        }
                        var metadataSha256 = customMetadata.requireExactImmutableLedgerMetadata(
                                root.value().ledgerId(), configuration, handle.getLedgerMetadata());
                        return activate(request, customMetadata, handle, metadataSha256.value(),
                                slot, intent, claimedWriter, root);
                    } catch (Throwable validationFailure) {
                        handle.closeAsync();
                        return recordUncertain(request, customMetadata, slot, intent, claimedWriter, root,
                                validationFailure, deadline);
                    }
                }).thenCompose(java.util.function.Function.identity());
    }

    private CompletableFuture<AllocatedBookKeeperLedger> abortOwnedPreTransmission(
            BookKeeperLedgerAllocationRequest request,
            BookKeeperVersionedValue<BookKeeperAllocationSlotRecord> slot,
            BookKeeperVersionedValue<LedgerAllocationIntentRecord> intent,
            BookKeeperVersionedValue<BookKeeperWriterStateRecord> claimedWriter,
            BookKeeperVersionedValue<BookKeeperLedgerRootRecord> root,
            Throwable failure) {
        BookKeeperLedgerRootRecord before = root.value();
        return ledgerMetadata.compareAndSetRoot(cluster, rootReplacement(before,
                        BookKeeperLedgerLifecycle.ABORTED, before.lifecycleEpoch() + 1,
                        false, before.activatedAtMillis(), ""), root.metadataVersion())
                .thenCompose(ignored -> casIntent(
                        intent, LedgerAllocationLifecycle.ABORTED, false, "", "pre-transmission setup failed"))
                .thenCompose(ignored -> writerState.detachAllocation(
                        claimedWriter, request.session(), intent.value().allocationId(),
                        "pre-transmission setup failed"))
                .thenCompose(ignored -> writerMetadata.deleteAllocationSlot(
                        cluster, slot.value().slot(), slot.metadataVersion()))
                .thenCompose(ignored -> CompletableFuture.failedFuture(unwrap(failure)));
    }

    private CompletableFuture<AllocatedBookKeeperLedger> activate(
            BookKeeperLedgerAllocationRequest request,
            BookKeeperLedgerCustomMetadata customMetadata,
            WriteAdvHandle handle,
            String metadataSha256,
            BookKeeperVersionedValue<BookKeeperAllocationSlotRecord> slot,
            BookKeeperVersionedValue<LedgerAllocationIntentRecord> intent,
            BookKeeperVersionedValue<BookKeeperWriterStateRecord> claimedWriter,
            BookKeeperVersionedValue<BookKeeperLedgerRootRecord> root) {
        return casRootActive(root).thenCompose(activeRoot ->
                casIntent(intent, LedgerAllocationLifecycle.PHYSICAL_CREATED, false, metadataSha256, "")
                        .thenCompose(physical -> casIntent(
                                physical, LedgerAllocationLifecycle.ACTIVATED, false, metadataSha256, ""))
                        .thenCompose(activated -> writerState.activate(
                                        claimedWriter, request.session(), activated.value(), activeRoot.value())
                                .thenCompose(activeWriter -> writerMetadata.deleteAllocationSlot(
                                                cluster, slot.value().slot(), slot.metadataVersion())
                                        .handle((ignored, failure) -> new AllocatedBookKeeperLedger(
                                                handle, activeWriter, activated, activeRoot, customMetadata)))));
    }

    private CompletableFuture<AllocatedBookKeeperLedger> recordUncertain(
            BookKeeperLedgerAllocationRequest request,
            BookKeeperLedgerCustomMetadata customMetadata,
            BookKeeperVersionedValue<BookKeeperAllocationSlotRecord> slot,
            BookKeeperVersionedValue<LedgerAllocationIntentRecord> intent,
            BookKeeperVersionedValue<BookKeeperWriterStateRecord> claimedWriter,
            BookKeeperVersionedValue<BookKeeperLedgerRootRecord> root,
            Throwable createFailure,
            BookKeeperOperationDeadline deadline) {
        return casSlot(slot, AllocationSlotLifecycle.CREATE_UNCERTAIN)
                .thenCompose(uncertainSlot -> casIntent(
                        intent, LedgerAllocationLifecycle.CREATE_UNCERTAIN, true, "", "provider create outcome unknown")
                        .thenCompose(uncertainIntent -> casRootUncertain(root)
                                .thenCompose(uncertainRoot -> probeUncertain(
                                        customMetadata, uncertainRoot, uncertainIntent, deadline)
                                        .thenCompose(probe -> writerState.detachAllocation(
                                                claimedWriter, request.session(), intent.value().allocationId(),
                                                "provider create outcome unknown"))
                                        .thenCompose(ignored -> CompletableFuture.failedFuture(
                                                uncertainFailure(createFailure))))));
    }

    private CompletableFuture<UncertainProbe> probeUncertain(
            BookKeeperLedgerCustomMetadata expected,
            BookKeeperVersionedValue<BookKeeperLedgerRootRecord> root,
            BookKeeperVersionedValue<LedgerAllocationIntentRecord> intent,
            BookKeeperOperationDeadline deadline) {
        CompletableFuture<LedgerMetadata> probe;
        try {
            probe = client.metadata(root.value().ledgerId(), deadline);
        } catch (Throwable ignored) {
            return CompletableFuture.completedFuture(new UncertainProbe(root, intent));
        }
        return probe.handle((metadata, failure) -> {
            if (failure != null) {
                return CompletableFuture.completedFuture(new UncertainProbe(root, intent));
            }
            try {
                String metadataSha256 = expected.requireExactImmutableLedgerMetadata(
                        root.value().ledgerId(), configuration, metadata).value();
                return casIntent(
                                intent,
                                LedgerAllocationLifecycle.PHYSICAL_CREATED,
                                true,
                                metadataSha256,
                                "matching provider create recovered without writable handle")
                        .thenCompose(physical -> sealRecoveredCreate(root, physical, deadline));
            } catch (Throwable mismatch) {
                String reason = "foreign or mismatching ledger metadata after uncertain create";
                return casRootQuarantined(root, reason)
                        .thenCompose(quarantined -> casIntent(
                                intent, LedgerAllocationLifecycle.FOREIGN_COLLISION, true, "", reason)
                                .thenApply(foreign -> new UncertainProbe(quarantined, foreign)));
            }
        }).thenCompose(java.util.function.Function.identity());
    }

    private CompletableFuture<UncertainProbe> sealRecoveredCreate(
            BookKeeperVersionedValue<BookKeeperLedgerRootRecord> root,
            BookKeeperVersionedValue<LedgerAllocationIntentRecord> physical,
            BookKeeperOperationDeadline deadline) {
        CompletableFuture<BookKeeperVersionedValue<BookKeeperLedgerRootRecord>> sealed;
        try {
            sealed = allocationRecovery.sealUnowned(
                    root, deadline.remaining(), "lost CreateAdv response recovered exact physical ledger");
        } catch (Throwable failure) {
            return CompletableFuture.completedFuture(new UncertainProbe(root, physical));
        }
        return sealed.handle((value, failure) ->
                new UncertainProbe(failure == null ? value : root, physical));
    }

    private CompletableFuture<BookKeeperVersionedValue<BookKeeperWriterStateRecord>> abortPreTransmission(
            BookKeeperLedgerAllocationRequest request,
            BookKeeperVersionedValue<BookKeeperAllocationSlotRecord> slot,
            BookKeeperVersionedValue<LedgerAllocationIntentRecord> intent,
            BookKeeperVersionedValue<BookKeeperWriterStateRecord> claimedWriter,
            String reason) {
        return casIntent(intent, LedgerAllocationLifecycle.ABORTED, false, "", reason)
                .thenCompose(ignored -> writerState.detachAllocation(
                        claimedWriter, request.session(), intent.value().allocationId(), reason))
                .thenCompose(idle -> writerMetadata.deleteAllocationSlot(
                                cluster, slot.value().slot(), slot.metadataVersion())
                        .thenApply(ignored -> idle));
    }

    private CompletableFuture<BookKeeperVersionedValue<LedgerAllocationIntentRecord>> casIntent(
            BookKeeperVersionedValue<LedgerAllocationIntentRecord> current,
            LedgerAllocationLifecycle lifecycle,
            boolean hazard,
            String metadataSha256,
            String reason) {
        LedgerAllocationIntentRecord before = current.value();
        LedgerAllocationIntentRecord replacement = new LedgerAllocationIntentRecord(
                before.schemaVersion(), before.allocationId(), before.streamId(), before.segmentSequence(),
                before.clusterAlias(), before.candidateLedgerId(), before.allocationSlot(),
                before.configurationBindingSha256(), before.writerId(), before.writerRunIdHash(),
                before.appendSessionEpoch(), before.fencingTokenHash(), before.writerStateEpoch(), lifecycle, hazard,
                metadataSha256, before.createdAtMillis(), clock.millis(), reason, 0);
        return writerMetadata.compareAndSetAllocation(cluster, replacement, current.metadataVersion());
    }

    private CompletableFuture<BookKeeperVersionedValue<BookKeeperAllocationSlotRecord>> casSlot(
            BookKeeperVersionedValue<BookKeeperAllocationSlotRecord> current,
            AllocationSlotLifecycle lifecycle) {
        BookKeeperAllocationSlotRecord before = current.value();
        BookKeeperAllocationSlotRecord replacement = new BookKeeperAllocationSlotRecord(
                before.schemaVersion(), before.slot(), before.allocationId(), before.streamId(),
                before.candidateLedgerId(), before.ledgerIdentitySha256(), before.configurationBindingSha256(),
                lifecycle, before.createdAtMillis(), clock.millis(), 0);
        return writerMetadata.compareAndSetAllocationSlot(cluster, replacement, current.metadataVersion());
    }

    private CompletableFuture<BookKeeperVersionedValue<BookKeeperLedgerRootRecord>> casRootActive(
            BookKeeperVersionedValue<BookKeeperLedgerRootRecord> current) {
        BookKeeperLedgerRootRecord before = current.value();
        return ledgerMetadata.compareAndSetRoot(cluster, rootReplacement(before,
                BookKeeperLedgerLifecycle.ACTIVE, before.lifecycleEpoch() + 1, false, clock.millis(), ""),
                current.metadataVersion());
    }

    private CompletableFuture<BookKeeperVersionedValue<BookKeeperLedgerRootRecord>> casRootUncertain(
            BookKeeperVersionedValue<BookKeeperLedgerRootRecord> current) {
        BookKeeperLedgerRootRecord before = current.value();
        return ledgerMetadata.compareAndSetRoot(cluster, rootReplacement(before,
                BookKeeperLedgerLifecycle.ALLOCATING, before.lifecycleEpoch() + 1, true, 0, ""),
                current.metadataVersion());
    }

    private CompletableFuture<BookKeeperVersionedValue<BookKeeperLedgerRootRecord>> casRootQuarantined(
            BookKeeperVersionedValue<BookKeeperLedgerRootRecord> current, String reason) {
        BookKeeperLedgerRootRecord before = current.value();
        return ledgerMetadata.compareAndSetRoot(cluster, rootReplacement(before,
                BookKeeperLedgerLifecycle.QUARANTINED, before.lifecycleEpoch() + 1,
                before.lateCreateHazard(), before.activatedAtMillis(), reason), current.metadataVersion());
    }

    private BookKeeperLedgerRootRecord allocatingRoot(
            BookKeeperLedgerAllocationRequest request,
            BookKeeperLedgerIdNamespaceReservation namespace,
            BookKeeperLedgerCustomMetadata customMetadata,
            BookKeeperVersionedValue<BookKeeperAllocationSlotRecord> slot,
            BookKeeperVersionedValue<LedgerAllocationIntentRecord> intent,
            BookKeeperVersionedValue<BookKeeperWriterStateRecord> writer) {
        AppendSession session = request.session();
        long now = clock.millis();
        return new BookKeeperLedgerRootRecord(1, slot.value().ledgerIdentitySha256(), configuration.clusterAlias(),
                configuration.providerScopeSha256(), intent.value().candidateLedgerId(), request.streamId().value(),
                intent.value().segmentSequence(), intent.value().allocationId(), slot.value().slot(),
                configurationBindingSha256, namespace.ledgerIdNamespaceSha256().value(), false, session.writerId(),
                writer.value().writerRunIdHash(), session.epoch(), BookKeeperIdentityDigests.sha256(
                        session.fencingToken()), configuration.ensembleSize(), configuration.writeQuorumSize(),
                configuration.ackQuorumSize(), configuration.digestType().name(), customMetadata.sha256().value(),
                BookKeeperLedgerLifecycle.ALLOCATING, 1, now, 0, 0, 0, -1, 0, "", "", "",
                0, 0, 0, 0, 0, "", 0);
    }

    private static BookKeeperLedgerRootRecord rootReplacement(
            BookKeeperLedgerRootRecord before,
            BookKeeperLedgerLifecycle lifecycle,
            long epoch,
            boolean hazard,
            long activatedAtMillis,
            String stateReason) {
        return new BookKeeperLedgerRootRecord(before.schemaVersion(), before.ledgerIdentitySha256(),
                before.clusterAlias(), before.providerScopeSha256(), before.ledgerId(), before.streamId(),
                before.segmentSequence(), before.allocationId(), before.allocationSlot(),
                before.configurationBindingSha256(), before.ledgerIdNamespaceSha256(), hazard, before.writerId(),
                before.writerRunIdHash(), before.appendSessionEpoch(), before.fencingTokenHash(), before.ensembleSize(),
                before.writeQuorumSize(), before.ackQuorumSize(), before.digestType(), before.customMetadataSha256(),
                lifecycle, epoch, before.createdAtMillis(), activatedAtMillis, before.sealStartedAtMillis(),
                before.sealedAtMillis(), before.sealedLastEntryId(), before.sealedLength(), before.sealReason(),
                before.gcAttemptId(), before.referenceSetSha256(), before.markedAtMillis(),
                before.deleteNotBeforeMillis(), before.deleteStartedAtMillis(), before.firstAbsentAtMillis(),
                before.deletedAtMillis(), stateReason, 0);
    }

    private synchronized long candidate() {
        return configuration.ledgerIdNamespace().candidate(random);
    }

    private synchronized String newAllocationId() {
        char[] encoded = new char[26];
        for (int index = 0; index < encoded.length; index++) {
            encoded[index] = BASE32[random.nextInt(BASE32.length)];
        }
        return new String(encoded);
    }

    private static NereusException uncertainFailure(Throwable cause) {
        return new NereusException(ErrorCode.PRIMARY_WAL_WRITE_FAILED, true,
                "BookKeeper ledger create outcome is unknown; durable recovery is required", unwrap(cause));
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

    private static <T> CompletableFuture<T> failed(ErrorCode code, String message) {
        return CompletableFuture.failedFuture(new NereusException(code, false, message));
    }

    private static String text(String value, String name) {
        Objects.requireNonNull(value, name);
        if (value.isBlank()) throw new IllegalArgumentException(name + " cannot be blank");
        return value;
    }

    private record UncertainProbe(
            BookKeeperVersionedValue<BookKeeperLedgerRootRecord> root,
            BookKeeperVersionedValue<LedgerAllocationIntentRecord> intent) { }
}
