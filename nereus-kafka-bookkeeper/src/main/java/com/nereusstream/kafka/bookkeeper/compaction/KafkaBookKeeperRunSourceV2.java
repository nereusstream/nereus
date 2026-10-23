/*
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 *   https://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

package com.nereusstream.kafka.bookkeeper.compaction;

import com.nereusstream.domain.bytes.CanonicalBytes;
import com.nereusstream.domain.bytes.CanonicalUtf8;
import com.nereusstream.domain.bytes.Sha256Digest;
import com.nereusstream.domain.codec.TopicIncarnationIdentityCodecV1;
import com.nereusstream.domain.identity.Id128;
import com.nereusstream.kafka.bookkeeper.checkpoint.KafkaProtocolCheckpointStateV1;
import com.nereusstream.kafka.bookkeeper.compaction.KafkaCompactionRecordsV1.CompactionPlan;
import com.nereusstream.kafka.bookkeeper.compaction.KafkaCompactionRecordsV1.InputBatch;
import com.nereusstream.kafka.bookkeeper.nbke2.Nbke2CodecV1;
import com.nereusstream.kafka.bookkeeper.nbke2.Nbke2DataV1;
import com.nereusstream.kafka.bookkeeper.nbke2.Nbke2FrameV1;
import com.nereusstream.kafka.bookkeeper.nbke2.Nbke2IndexDirectoryEntryV1;
import com.nereusstream.kafka.bookkeeper.nbke2.Nbke2ProtocolCheckpointV1;
import com.nereusstream.kafka.bookkeeper.nbke2.Nbke2RangeIndexBlockV1;
import com.nereusstream.kafka.bookkeeper.nbke2.Nbke2RunBindingV1;
import com.nereusstream.kafka.bookkeeper.nbke2.Nbke2RunFooterV1;
import com.nereusstream.kafka.bookkeeper.nbke2.Nbke2RunHeaderV1;
import com.nereusstream.storage.api.bookkeeper.BookKeeperCellSession;
import com.nereusstream.storage.api.bookkeeper.RunLedgerHandleV1;
import com.nereusstream.storage.api.kafka.KafkaRunRootCatalogV2;
import com.nereusstream.storage.api.kafka.KafkaRunRootRecordV2;
import com.nereusstream.storage.api.kafka.KafkaRunRootStateV1;
import com.nereusstream.storage.api.lifecycle.PhysicalResourceIdV2;
import com.nereusstream.storage.bookkeeper.M5BookKeeperDeleteAdapterV1.BookKeeperDeleteTargetV1;
import com.nereusstream.storage.bookkeeper.M5BookKeeperNativeCreateClientV2;
import com.nereusstream.storage.object.gc.M5TargetDeleteAuthorityRecordsV1.ProofBoundWriterClassV1;
import com.nereusstream.storage.object.gc.M5TargetDeleteMultiWriterGuardV2;
import com.nereusstream.storage.object.gc.M5TargetDeleteMultiWriterGuardV2.Completion;
import com.nereusstream.storage.object.gc.M5TargetDeleteMultiWriterGuardV2.Context;
import com.nereusstream.storage.object.materialization.M5MaterializationRecordsV1.MaterializationSourceCut;
import com.nereusstream.storage.object.materialization.M5MaterializationRecordsV1.PositionDomain;
import com.nereusstream.storage.object.materialization.M5MaterializationRecordsV1.ProtocolCoverage;
import com.nereusstream.storage.object.materialization.M5MaterializationRecordsV1.SourceExtent;
import com.nereusstream.storage.object.materialization.M5MaterializationRecordsV1.SourceKind;
import com.nereusstream.storage.object.read.control.M4ReadControlRecordsV1.BindingIdentity;
import java.nio.ByteBuffer;
import java.security.MessageDigest;
import java.security.NoSuchAlgorithmException;
import java.util.ArrayList;
import java.util.HashSet;
import java.util.LinkedHashMap;
import java.util.List;
import java.util.Map;
import java.util.Objects;
import java.util.Optional;
import java.util.Set;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.CompletionStage;
import java.util.concurrent.Executor;
import java.util.function.Consumer;
import java.util.function.Function;
import java.util.function.Supplier;

/**
 * Native sealed NBKE2 source capture and exact physical membership. Reads hold a physical ticket until their owned
 * session drains/closes. Protocol frontiers, semantic key/transaction proofs and M4 source protection remain owner
 * obligations; this adapter cannot manufacture them from a sealed ledger. Unknown close retains its read ticket.
 * One caller-supplied Cell reservation covers the entire capture or multi-run resolution, including retained inputs
 * between sequential sessions. Native/payload/decoded allowances are separate; returned caches are not charged.
 */
public final class KafkaBookKeeperRunSourceV2 implements KafkaBookKeeperPublicationTicketsV2.InputMembership {
    /** A native owner retains the session and observes this invocation's actual physical-ticket cleanup. */
    public record OwnedReadSession(
            BookKeeperCellSession session, Consumer<CompletionStage<Void>> observeTicketCleanup) {
        public OwnedReadSession {
            Objects.requireNonNull(session, "native source session");
            Objects.requireNonNull(observeTicketCleanup, "observeTicketCleanup");
        }
    }

    /** This invocation has no accepted native session or physical-ticket operation left unresolved. */
    public static final class TerminatedReadException extends IllegalStateException {
        private static final long serialVersionUID = 1L;

        private TerminatedReadException(Throwable failure) {
            super(failure.getMessage(), failure);
        }
    }
    /** Created only after the exact native read session has confirmed termination or admission never dispatched it. */
    public static final class TicketCleanupException extends IllegalStateException {
        private static final long serialVersionUID = 1L;
        private final M5TargetDeleteMultiWriterGuardV2.TerminalCleanup cleanup;
        private final Sha256Digest operationId;

        private TicketCleanupException(
                M5TargetDeleteMultiWriterGuardV2.TerminalCleanup cleanup,
                Sha256Digest operationId,
                Throwable readFailure) {
            super("native source session terminated but physical ticket cleanup is unresolved", readFailure);
            this.cleanup = cleanup;
            this.operationId = operationId;
        }

        public Sha256Digest operationId() {
            return operationId;
        }

        /** One bounded, detached metadata pass. It never reopens the native ledger or session. */
        public CompletionStage<List<PhysicalResourceIdV2>> reconcileTickets() {
            return cleanup.reconcile();
        }
    }

    public record Bounds(
            int entries, int batches, int records, long nativeBytes, long payloadBytes, long decodedBytes) {
        public Bounds {
            if (entries < 2
                    || entries > 131072
                    || batches < 1
                    || batches > KafkaCompactionRecordsV1.MAX_BATCHES
                    || records < 1
                    || records > KafkaCompactionRecordsV1.MAX_RECORDS
                    || nativeBytes < 1
                    || nativeBytes > 256L * 1024 * 1024
                    || payloadBytes < 1
                    || payloadBytes > 128L * 1024 * 1024
                    || decodedBytes < 1
                    || decodedBytes > 256L * 1024 * 1024) {
                throw new IllegalArgumentException("native run source budget exceeds its hard bounds");
            }
        }
    }

    public record Snapshot(
            KafkaRunRootRecordV2 root,
            SourceExtent extent,
            List<InputBatch> batches,
            boolean rangeIndexesCoverAllData) {
        public Snapshot {
            batches = List.copyOf(batches);
        }

        public BindingIdentity binding() {
            return new BindingIdentity(
                    root.root().bindingId(),
                    Sha256Digest.hash(
                            TopicIncarnationIdentityCodecV1.encode(root.root().topicIncarnation())),
                    root.root().storageEpochId().digest());
        }
    }

    private record ReadOutcome(Snapshot snapshot, Throwable failure) {}

    private record DataRow(Nbke2DataV1 data, long groupOrdinal) {}

    private final KafkaBookKeeperReadCellBudgetV2 cellBudget;
    private final KafkaRunRootCatalogV2 catalog;
    private final M5BookKeeperNativeCreateClientV2 client;
    private final M5TargetDeleteMultiWriterGuardV2 guard;
    private final Bounds bounds;
    private final Executor owner;
    private final Function<KafkaRunRootRecordV2, OwnedReadSession> sessions;

    public KafkaBookKeeperRunSourceV2(
            KafkaBookKeeperReadCellBudgetV2 cellBudget,
            KafkaRunRootCatalogV2 catalog,
            M5BookKeeperNativeCreateClientV2 client,
            M5TargetDeleteMultiWriterGuardV2 guard,
            Bounds bounds,
            Executor owner) {
        this(
                cellBudget,
                catalog,
                client,
                guard,
                bounds,
                owner,
                root -> new OwnedReadSession(client.newSession(), ignored -> {}));
    }

    KafkaBookKeeperRunSourceV2(
            KafkaBookKeeperReadCellBudgetV2 cellBudget,
            KafkaRunRootCatalogV2 catalog,
            M5BookKeeperNativeCreateClientV2 client,
            M5TargetDeleteMultiWriterGuardV2 guard,
            Bounds bounds,
            Executor owner,
            Supplier<? extends BookKeeperCellSession> sessions) {
        this(
                cellBudget,
                catalog,
                client,
                guard,
                bounds,
                owner,
                root -> new OwnedReadSession(sessions.get(), ignored -> {}));
    }

    public KafkaBookKeeperRunSourceV2(
            KafkaBookKeeperReadCellBudgetV2 cellBudget,
            KafkaRunRootCatalogV2 catalog,
            M5BookKeeperNativeCreateClientV2 client,
            M5TargetDeleteMultiWriterGuardV2 guard,
            Bounds bounds,
            Executor owner,
            Function<KafkaRunRootRecordV2, OwnedReadSession> sessions) {
        this.cellBudget = Objects.requireNonNull(cellBudget, "cellBudget");
        this.catalog = Objects.requireNonNull(catalog, "catalog");
        this.client = Objects.requireNonNull(client, "client");
        this.guard = Objects.requireNonNull(guard, "guard");
        this.bounds = Objects.requireNonNull(bounds, "bounds");
        this.owner = Objects.requireNonNull(owner, "owner");
        this.sessions = Objects.requireNonNull(sessions, "sessions");
    }

    /** The caller's exact Binding is admitted before even the selected-root metadata read. */
    public CompletionStage<Snapshot> capture(String nativeRootKey, BindingIdentity expectedBinding) {
        Objects.requireNonNull(nativeRootKey, "nativeRootKey");
        Objects.requireNonNull(expectedBinding, "expectedBinding");
        return withinBudget(expectedBinding, scope -> capture(nativeRootKey, new Budget(bounds), scope));
    }

    /** One complete ordered native cut shares a single admission and aggregate scan budget. */
    public CompletionStage<List<Snapshot>> captureSelectedRuns(
            List<String> nativeRootKeys, BindingIdentity expectedBinding) {
        var keys = List.copyOf(nativeRootKeys);
        Objects.requireNonNull(expectedBinding, "expectedBinding");
        if (keys.isEmpty()
                || keys.size() > com.nereusstream.storage.object.materialization.M5MaterializationRecordsV1.MAX_SOURCES
                || new HashSet<>(keys).size() != keys.size()) {
            throw new IllegalArgumentException("native source cut is empty, duplicate or exceeds its bound");
        }
        return withinBudget(expectedBinding, scope -> {
            var budget = new Budget(bounds);
            CompletionStage<List<Snapshot>> sequence = CompletableFuture.completedFuture(new ArrayList<>());
            for (var key : keys) {
                sequence = sequence.thenComposeAsync(
                        found -> capture(key, budget, scope).thenApply(snapshot -> {
                            found.add(snapshot);
                            return found;
                        }),
                        owner);
            }
            return sequence.thenApply(List::copyOf);
        });
    }

    @Override
    public CompletionStage<Map<Sha256Digest, List<PhysicalResourceIdV2>>> resolve(CompactionPlan plan) {
        Objects.requireNonNull(plan, "plan");
        return resolve(plan.sourceCut(), Optional.of(plan.inputBatches()));
    }

    /** Physical membership only; publication must use the complete-plan overload. */
    public CompletionStage<Map<Sha256Digest, List<PhysicalResourceIdV2>>> resolve(MaterializationSourceCut cut) {
        return resolve(cut, Optional.empty());
    }

    private CompletionStage<Map<Sha256Digest, List<PhysicalResourceIdV2>>> resolve(
            MaterializationSourceCut cut, Optional<List<InputBatch>> expectedInputs) {
        return resolve(cut, cut.sources(), expectedInputs);
    }

    /** The mixed catalog owns the complete plan; this reader verifies every raw source under one native budget. */
    CompletionStage<Map<Sha256Digest, List<PhysicalResourceIdV2>>> resolveRawSubset(
            MaterializationSourceCut cut, List<SourceExtent> rawSources, List<InputBatch> expectedInputs) {
        return resolve(cut, rawSources, Optional.of(List.copyOf(expectedInputs)));
    }

    boolean usesBudget(KafkaBookKeeperReadCellBudgetV2 expected) {
        return cellBudget == expected;
    }

    private CompletionStage<Map<Sha256Digest, List<PhysicalResourceIdV2>>> resolve(
            MaterializationSourceCut cut, List<SourceExtent> rawSources, Optional<List<InputBatch>> expectedInputs) {
        Objects.requireNonNull(cut, "cut");
        rawSources = List.copyOf(rawSources);
        var admitted = cut.sources().stream()
                .filter(source -> source.kind() == SourceKind.BOOKKEEPER_LEDGER)
                .toList();
        if (!cut.identity()
                        .providerScopeSha256()
                        .equals(client.capabilitySnapshot().providerScopeId().digest())
                || rawSources.isEmpty()
                || !rawSources.equals(admitted)
                || (rawSources.size() != cut.sources().size() && expectedInputs.isEmpty())) {
            return CompletableFuture.failedFuture(
                    new IllegalArgumentException("native run catalog requires exact BK Cell sources"));
        }
        var sources = rawSources;
        return withinBudget(cut.identity().binding(), scope -> resolve(cut, sources, expectedInputs, scope));
    }

    private CompletionStage<Map<Sha256Digest, List<PhysicalResourceIdV2>>> resolve(
            MaterializationSourceCut cut,
            List<SourceExtent> rawSources,
            Optional<List<InputBatch>> expectedInputs,
            ReadScope scope) {
        var budget = new Budget(bounds);
        Map<Sha256Digest, List<PhysicalResourceIdV2>> result = new LinkedHashMap<>();
        List<InputBatch> nativeInputs = new ArrayList<>();
        CompletionStage<Void> sequence = CompletableFuture.completedFuture(null);
        for (var extent : rawSources) {
            sequence = sequence.thenComposeAsync(
                    ignored -> capture(extent.physicalKey(), budget, scope).thenAccept(snapshot -> {
                        if (!snapshot.extent().equals(extent)
                                || !snapshot.binding().equals(cut.identity().binding())
                                || !snapshot.root()
                                        .root()
                                        .providerScopeId()
                                        .digest()
                                        .equals(cut.identity().providerScopeSha256())) {
                            throw new IllegalArgumentException(
                                    "native run source differs from the complete frozen source extent");
                        }
                        if (result.put(
                                        extent.sourceIdentitySha256(),
                                        List.of(snapshot.root().resource()))
                                != null) {
                            throw new IllegalArgumentException("duplicate native source identity");
                        }
                        if (expectedInputs.isPresent()) {
                            nativeInputs.addAll(snapshot.batches());
                        }
                    }),
                    owner);
        }
        return sequence.thenApply(ignored -> {
            if (expectedInputs.isPresent() && !nativeInputs.equals(expectedInputs.orElseThrow())) {
                throw new IllegalArgumentException("native source input batches differ from compaction plan");
            }
            return Map.copyOf(result);
        });
    }

    private CompletionStage<Snapshot> capture(String key, Budget budget, ReadScope scope) {
        var work = catalog.readSelectedRoot(key)
                .thenComposeAsync(
                        observed -> {
                            var actual = observed.orElseThrow(
                                    () -> new IllegalArgumentException("native source root is not selected"));
                            if (!binding(actual).equals(scope.binding)
                                    || !actual.root()
                                            .providerScopeId()
                                            .equals(client.capabilitySnapshot().providerScopeId())
                                    || actual.root().state() != KafkaRunRootStateV1.SEALED
                                    || !actual.resource()
                                            .namespace()
                                            .equals(client.spec().namespace())
                                    || actual.root().kafkaEndOffsetExclusive().orElseThrow()
                                            == actual.root().kafkaStartOffset()) {
                                throw new IllegalArgumentException(
                                        "native source requires a nonempty sealed run in the exact namespace");
                            }
                            var stable = new KafkaRunRootRecordV2(
                                    actual.resource(),
                                    actual.root(),
                                    true,
                                    Optional.empty(),
                                    false,
                                    actual.recoveryCut());
                            var identity = Sha256Digest.hash(stable.encode());
                            var context = new Context(
                                    ProofBoundWriterClassV1.OWNER_WORKER_LEASE_HANDLE_PIN_V1,
                                    client.capabilitySnapshot().configurationDigest(),
                                    stable.initialLink().initialRootSha256(),
                                    identity);
                            var terminalProof = Sha256Digest.hash(CanonicalBytes.copyOf(ByteBuffer.allocate(36)
                                    .putInt(0x4d355243)
                                    .put(identity.bytes().toByteArray())
                                    .array()));
                            var ticketCleanup = new CompletableFuture<Void>();
                            scope.ticketStarting();
                            return guard.execute(List.of(stable.resource()), context, () -> {
                                        final BookKeeperCellSession reads;
                                        final OwnedReadSession owned;
                                        try {
                                            owned = Objects.requireNonNull(
                                                    sessions.apply(stable), "owned native source session");
                                            reads = owned.session();
                                        } catch (Throwable failure) {
                                            // No native operation was accepted, so this ticket has an exact terminal.
                                            return CompletableFuture.completedFuture(new Completion<>(
                                                    new ReadOutcome(null, failure), Optional.of(terminalProof)));
                                        }
                                        scope.sessionStarting();
                                        CompletionStage<Snapshot> scan;
                                        try {
                                            owned.observeTicketCleanup().accept(ticketCleanup);
                                            scan = new Scan(key, stable, reads, budget).start();
                                        } catch (RuntimeException failure) {
                                            scan = CompletableFuture.failedFuture(failure);
                                        }
                                        return scan.handle(ReadOutcome::new).thenCompose(outcome -> reads.closeAsync()
                                                .thenApply(ignored -> {
                                                    scope.sessionDrained();
                                                    return new Completion<>(outcome, Optional.of(terminalProof));
                                                }));
                                    })
                                    .whenComplete((ignored, failure) -> {
                                        if (failure != null) {
                                            ticketCleanup.completeExceptionally(failure);
                                        }
                                    })
                                    .thenCompose(result -> {
                                        if (result.unresolvedTargets().isEmpty()) {
                                            scope.ticketDrained();
                                        }
                                        if (!result.mutationInvoked()
                                                || result.value().isEmpty()) {
                                            IllegalStateException failure = new IllegalStateException(
                                                    "native source physical admission failed");
                                            if (!result.mutationInvoked()
                                                    && !result.unresolvedTargets()
                                                            .isEmpty()) {
                                                failure = new TicketCleanupException(
                                                        result.terminalCleanup().orElseThrow(),
                                                        result.operationId(),
                                                        failure);
                                            }
                                            ticketCleanup.completeExceptionally(failure);
                                            return CompletableFuture.failedFuture(failure);
                                        }
                                        var outcome = result.value().orElseThrow();
                                        if (!result.unresolvedTargets().isEmpty()) {
                                            var failure = new TicketCleanupException(
                                                    result.terminalCleanup().orElseThrow(),
                                                    result.operationId(),
                                                    outcome.failure());
                                            ticketCleanup.completeExceptionally(failure);
                                            return CompletableFuture.failedFuture(failure);
                                        }
                                        ticketCleanup.complete(null);
                                        return outcome.failure() == null
                                                ? CompletableFuture.completedFuture(outcome.snapshot())
                                                : CompletableFuture.failedFuture(outcome.failure());
                                    });
                        },
                        owner);
        // Cancellation never abandons in-flight reads, session drain or release of this invocation's physical ticket.
        return work.thenApply(value -> value);
    }

    private <T> CompletionStage<T> withinBudget(BindingIdentity binding, Function<ReadScope, CompletionStage<T>> work) {
        final ReadScope scope;
        try {
            scope = new ReadScope(
                    binding, cellBudget.reserveRaw(client.capabilitySnapshot().providerScopeId(), binding, bounds));
        } catch (Throwable failure) {
            return CompletableFuture.failedFuture(new TerminatedReadException(failure));
        }
        CompletionStage<T> operation;
        try {
            operation = Objects.requireNonNull(work.apply(scope), "raw read scope returned no stage");
        } catch (Throwable failure) {
            scope.releaseIfDrained();
            return CompletableFuture.failedFuture(scope.failure(failure));
        }
        return operation
                .handle((value, failure) -> {
                    scope.releaseIfDrained();
                    if (failure != null) {
                        throw new java.util.concurrent.CompletionException(scope.failure(failure));
                    }
                    return value;
                })
                .thenApply(value -> value);
    }

    private static BindingIdentity binding(KafkaRunRootRecordV2 root) {
        return new BindingIdentity(
                root.root().bindingId(),
                Sha256Digest.hash(
                        TopicIncarnationIdentityCodecV1.encode(root.root().topicIncarnation())),
                root.root().storageEpochId().digest());
    }

    /** One sequential capture/resolve lifetime, including accumulated inputs between native sessions. */
    private static final class ReadScope {
        final BindingIdentity binding;
        final KafkaBookKeeperReadCellBudgetV2.Reservation reservation;
        volatile boolean sessionUnresolved;
        volatile boolean ticketUnresolved;

        ReadScope(BindingIdentity binding, KafkaBookKeeperReadCellBudgetV2.Reservation reservation) {
            this.binding = binding;
            this.reservation = reservation;
        }

        void sessionStarting() {
            sessionUnresolved = true;
            reservation.sessionStarting();
        }

        void sessionDrained() {
            sessionUnresolved = false;
        }

        void ticketStarting() {
            ticketUnresolved = true;
        }

        void ticketDrained() {
            ticketUnresolved = false;
        }

        Throwable failure(Throwable failure) {
            while (failure instanceof java.util.concurrent.CompletionException && failure.getCause() != null) {
                failure = failure.getCause();
            }
            return sessionUnresolved || ticketUnresolved ? failure : new TerminatedReadException(failure);
        }

        void releaseIfDrained() {
            if (!sessionUnresolved) {
                reservation.releaseAfterDrain();
            }
        }
    }

    private static final class Budget {
        final Bounds maximum;
        int entries;
        int batches;
        int records;
        long nativeBytes;
        long payloadBytes;
        long decodedBytes;

        Budget(Bounds maximum) {
            this.maximum = maximum;
        }
    }

    private final class Scan {
        final String key;
        final KafkaRunRootRecordV2 root;
        final BookKeeperCellSession reads;
        final Budget budget;
        final RunLedgerHandleV1 handle;
        final Nbke2RunBindingV1 run;
        final MessageDigest frames = sha();
        final MessageDigest bodies = sha();
        final Map<Long, DataRow> data = new LinkedHashMap<>();
        final Map<Long, Nbke2RangeIndexBlockV1> indexes = new LinkedHashMap<>();
        final Set<Id128> groups = new HashSet<>();
        long nextOffset;
        long length;
        long nativeLength;
        long latestIndex = -1;
        int records;
        long minimumTimestamp = Long.MAX_VALUE;
        long maximumTimestamp = -1;
        long groupOrdinal;
        Nbke2DataV1 firstGroup;
        MessageDigest groupDigest;
        long firstGroupEntry;
        int nextMember;
        long latestCheckpoint = -1;
        BookKeeperDeleteTargetV1 seal;
        Nbke2RunFooterV1 footer;

        Scan(String key, KafkaRunRootRecordV2 root, BookKeeperCellSession reads, Budget budget) {
            this.key = key;
            this.root = root;
            this.reads = reads;
            this.budget = budget;
            var value = root.root();
            handle = new RunLedgerHandleV1(
                    value.providerScopeId(),
                    value.runId(),
                    value.ledgerIdentity(),
                    client.capabilitySnapshot().configurationDigest());
            run = new Nbke2RunBindingV1(
                    value.bindingId(),
                    value.topicIncarnation(),
                    value.partitionId(),
                    value.storageEpochId(),
                    value.creatorOwnerEpoch(),
                    value.kafkaLeaderEpoch(),
                    value.providerScopeId(),
                    value.runId());
            nextOffset = value.kafkaStartOffset();
        }

        CompletionStage<Snapshot> start() {
            // The initial create inventory does not enumerate a Broker's admitted rollover/history read sources.
            // Catalog, Binding, namespace and physical tickets admit this read; the SDK session must match exactly.
            if (!reads.providerScopeId().equals(handle.providerScopeId())
                    || !reads.capabilitySnapshot().equals(client.capabilitySnapshot())) {
                throw new IllegalArgumentException("source reader differs from the native client capability");
            }
            return client.captureExactTarget(handle)
                    .thenCompose(captured -> {
                        seal = captured.exactTarget()
                                .orElseThrow(() ->
                                        new IllegalArgumentException("native source lacks exact closed metadata"));
                        if (root.recoveryCut()
                                .filter(cut -> cut.recoveredLastAddConfirmed() != seal.sealedLastEntryId())
                                .isPresent()) {
                            throw new IllegalArgumentException(
                                    "native recovered source LAC differs from its fixed cut");
                        }
                        long count = Math.addExact(seal.sealedLastEntryId(), 1);
                        if (count < 2 || count > budget.maximum.entries() - budget.entries) {
                            throw new IllegalArgumentException(
                                    "native source entry budget exhausted before data reads");
                        }
                        return reads.openRunLedger(handle);
                    })
                    .thenComposeAsync(
                            open -> {
                                if (!open.exactHandle().equals(Optional.of(handle))) {
                                    throw new IllegalArgumentException("native source exact open failed");
                                }
                                CompletionStage<Void> sequence = CompletableFuture.completedFuture(null);
                                for (long entry = 0; entry <= seal.sealedLastEntryId(); entry++) {
                                    long next = entry;
                                    sequence = sequence.thenComposeAsync(ignored -> read(next), owner);
                                }
                                return sequence;
                            },
                            owner)
                    .thenCompose(ignored -> catalog.readSelectedRoot(key))
                    .thenApplyAsync(
                            observed -> {
                                var current = observed.orElseThrow(
                                        () -> new IllegalArgumentException("native source root disappeared"));
                                if (!current.root().equals(root.root())
                                        || !current.resource().equals(root.resource())
                                        || !current.recoveryCut().equals(root.recoveryCut())) {
                                    throw new IllegalArgumentException("native source root changed during capture");
                                }
                                return finish();
                            },
                            owner);
        }

        CompletionStage<Void> read(long entry) {
            return reads.readExactEntry(handle, entry)
                    .thenAcceptAsync(
                            result -> {
                                var exact = result.exactEntry()
                                        .orElseThrow(
                                                () -> new IllegalArgumentException("native source entry unavailable"));
                                if (!exact.handle().equals(handle)
                                        || exact.entryId() != entry
                                        || exact.payload().length()
                                                > budget.maximum.nativeBytes() - budget.nativeBytes) {
                                    throw new IllegalArgumentException(
                                            "native source entry identity or byte budget differs");
                                }
                                budget.entries++;
                                budget.nativeBytes += exact.payload().length();
                                nativeLength += exact.payload().length();
                                frames.update(ByteBuffer.allocate(12)
                                        .putLong(entry)
                                        .putInt(exact.payload().length())
                                        .array());
                                frames.update(exact.payload().toByteArray());
                                if (root.recoveryCut()
                                        .filter(cut -> cut.inertFromEntryId().isPresent()
                                                && entry
                                                        >= cut.inertFromEntryId()
                                                                .getAsLong())
                                        .isPresent()) {
                                    // The native owner fixed this physical suffix as logically inert during recovery.
                                    return;
                                }
                                accept(
                                        entry,
                                        Nbke2CodecV1.decode(
                                                exact.payload().toByteArray(),
                                                handle.ledgerIdentity().ledgerId(),
                                                entry));
                            },
                            owner);
        }

        void accept(long entry, Nbke2FrameV1 frame) {
            if (!frame.runBinding().equals(run)) {
                throw new IllegalArgumentException("native source frame has another run binding");
            }
            if (entry == 0) {
                if (!(frame instanceof Nbke2RunHeaderV1 header)
                        || header.kafkaStartOffset() != nextOffset
                        || header.firstDataEntryId() != 1
                        || !header.ledgerConfigurationDigest().equals(handle.configurationDigest())) {
                    throw new IllegalArgumentException("native source header differs");
                }
                return;
            }
            if (frame instanceof Nbke2DataV1 batch) {
                acceptData(entry, batch);
                return;
            }
            if (firstGroup != null) {
                throw new IllegalArgumentException("control entry splits an incomplete append group");
            }
            if (frame instanceof Nbke2RangeIndexBlockV1 index) {
                if (entry <= index.lastDataEntryId()
                        || entry >= index.successorDataEntryId()
                        || index.predecessorBlockEntryId() != latestIndex) {
                    throw new IllegalArgumentException("native range index has another physical cut");
                }
                validateIndex(index);
                indexes.put(entry, index);
                latestIndex = entry;
            } else if (frame instanceof Nbke2ProtocolCheckpointV1 checkpoint) {
                KafkaProtocolCheckpointStateV1.fromNbke2(checkpoint);
                for (long covered : new long[] {
                    checkpoint.rangeIndexCoveredThrough(),
                    checkpoint.producerStateCoveredThrough(),
                    checkpoint.transactionIndexCoveredThrough(),
                    checkpoint.leaderEpochCoveredThrough()
                }) {
                    if (covered < root.root().kafkaStartOffset() || covered > nextOffset) {
                        throw new IllegalArgumentException("native checkpoint exceeds observed DATA coverage");
                    }
                }
                latestCheckpoint = entry;
            } else if (frame instanceof Nbke2RunFooterV1 last) {
                if (entry != seal.sealedLastEntryId()
                        || last.lastPhysicalEntryIdExclusive() != entry + 1
                        || last.kafkaEndOffsetExclusive() != nextOffset
                        || nextOffset != root.root().kafkaEndOffsetExclusive().orElseThrow()
                        || last.sealOwnerEpoch() < run.creatorOwnerEpoch()
                        || last.protocolCheckpointEntryId() != latestCheckpoint) {
                    throw new IllegalArgumentException("native source footer differs from actual terminal coverage");
                }
                footer = last;
            } else {
                throw new IllegalArgumentException("duplicate native run header");
            }
        }

        void acceptData(long entry, Nbke2DataV1 batch) {
            if (batch.baseOffset() != nextOffset
                    || budget.batches == budget.maximum.batches()
                    || batch.rawAssignedRecordBatch().length() > budget.maximum.payloadBytes() - budget.payloadBytes) {
                throw new IllegalArgumentException("native source DATA coverage or payload budget differs");
            }
            if (firstGroup == null) {
                if (batch.memberOrdinal() != 0 || !groups.add(batch.appendGroupId())) {
                    throw new IllegalArgumentException("native append group starts at another member or repeats");
                }
                firstGroup = batch;
                firstGroupEntry = entry;
                nextMember = 0;
                groupDigest = sha();
            }
            if (batch.memberOrdinal() != nextMember
                    || batch.memberCount() != firstGroup.memberCount()
                    || !batch.appendGroupId().equals(firstGroup.appendGroupId())
                    || !batch.storageAttemptId().equals(firstGroup.storageAttemptId())
                    || entry != firstGroupEntry + nextMember) {
                throw new IllegalArgumentException("native append-group members differ");
            }
            var parsed = KafkaRecordBatchCodecV1.parseBounded(
                    batch.rawAssignedRecordBatch(),
                    budget.maximum.records() - budget.records,
                    budget.maximum.decodedBytes() - budget.decodedBytes);
            var facts = parsed.batch();
            if (facts.baseOffset() != batch.baseOffset()
                    || facts.lastOffset() + 1 != batch.endOffsetExclusive()
                    || facts.partitionLeaderEpoch() != run.kafkaLeaderEpoch()) {
                throw new IllegalArgumentException("native Kafka body differs from its NBKE2 DATA envelope");
            }
            budget.batches++;
            budget.records += facts.records().size();
            budget.decodedBytes += parsed.decodedBytes();
            budget.payloadBytes += batch.rawAssignedRecordBatch().length();
            records += facts.records().size();
            length += batch.rawAssignedRecordBatch().length();
            for (var record : facts.records()) {
                if (record.timestamp() < 0) {
                    throw new IllegalArgumentException("source record timestamp is unrepresentable");
                }
                minimumTimestamp = Math.min(minimumTimestamp, record.timestamp());
                maximumTimestamp = Math.max(maximumTimestamp, record.timestamp());
            }
            bodies.update(batch.rawAssignedRecordBatch().toByteArray());
            groupDigest.update(batch.rawAssignedRecordBatch().toByteArray());
            data.put(entry, new DataRow(batch, groupOrdinal));
            nextOffset = batch.endOffsetExclusive();
            nextMember++;
            if (batch.terminalDescriptor().isPresent()) {
                var terminal = batch.terminalDescriptor().orElseThrow();
                if (terminal.groupStartOffset() != firstGroup.baseOffset()
                        || terminal.firstDataEntryId() != firstGroupEntry
                        || terminal.lastDataEntryId() != entry
                        || terminal.groupEndOffsetExclusive() != nextOffset
                        || !terminal.aggregateAssignedPayloadSha256()
                                .equals(Sha256Digest.copyOf(groupDigest.digest()))) {
                    throw new IllegalArgumentException("native terminal append-group digest or bounds differ");
                }
                firstGroup = null;
                groupOrdinal++;
            }
        }

        void validateIndex(Nbke2RangeIndexBlockV1 index) {
            Long groupBase = null;
            Long deltaBase = null;
            for (var locator : index.locators()) {
                var row = data.get(Math.addExact(index.anchorEntryId(), locator.entryIdDelta()));
                if (row == null
                        || row.data().baseOffset() != Math.addExact(index.anchorOffset(), locator.baseOffsetDelta())
                        || row.data().endOffsetExclusive() - row.data().baseOffset() != locator.logicalOffsetCount()
                        || row.data().rawAssignedRecordBatch().length() != locator.payloadLength()) {
                    throw new IllegalArgumentException("native index locator differs from actual DATA");
                }
                if (groupBase == null) {
                    groupBase = row.groupOrdinal();
                    deltaBase = locator.appendGroupDelta();
                }
                if (row.groupOrdinal() - groupBase != locator.appendGroupDelta() - deltaBase) {
                    throw new IllegalArgumentException("native index append-group delta differs");
                }
            }
        }

        Snapshot finish() {
            if (footer == null && root.recoveryCut().isEmpty()
                    || firstGroup != null
                    || data.isEmpty()
                    || nativeLength != seal.sealedLength()
                    || nextOffset != root.root().kafkaEndOffsetExclusive().orElseThrow()) {
                throw new IllegalArgumentException("native source is incomplete");
            }
            var expectedDirectory = indexes.entrySet().stream()
                    .map(entry -> new Nbke2IndexDirectoryEntryV1(
                            entry.getKey(),
                            entry.getValue().anchorOffset(),
                            entry.getValue().coveredThroughOffset()))
                    .toList();
            if (footer != null && !footer.indexDirectory().equals(expectedDirectory)) {
                throw new IllegalArgumentException("native footer index directory differs");
            }
            boolean completeIndexes = !indexes.isEmpty()
                    && expectedDirectory.get(0).blockStartOffset()
                            == root.root().kafkaStartOffset();
            var format = Sha256Digest.copyOf(frames.digest());
            var body = Sha256Digest.copyOf(bodies.digest());
            var identity = Sha256Digest.hash(
                    CanonicalBytes.copyOf(ByteBuffer.allocate(84 + root.encode().length())
                            .putInt(0x4d354e53)
                            .put(root.encode().toByteArray())
                            .putLong(seal.sealedLastEntryId())
                            .putLong(seal.sealedLength())
                            .put(format.bytes().toByteArray())
                            .put(body.bytes().toByteArray())
                            .array()));
            var extent = new SourceExtent(
                    SourceKind.BOOKKEEPER_LEDGER,
                    identity,
                    new ProtocolCoverage(
                            PositionDomain.KAFKA_OFFSET, root.root().kafkaStartOffset(), nextOffset),
                    key,
                    length,
                    records,
                    records == 0 ? -1 : minimumTimestamp,
                    maximumTimestamp,
                    body,
                    Optional.empty(),
                    Optional.of(root.resource().sha256()),
                    format,
                    Sha256Digest.hash(CanonicalUtf8.fromString("NBKE2_V1_PLAINTEXT/"
                                    + client.capabilitySnapshot().credentialIdentityVersion())
                            .bytes()),
                    true,
                    false,
                    List.of(root.root().bindingId().digest()));
            var batches = new ArrayList<InputBatch>();
            data.values()
                    .forEach(row -> batches.add(
                            new InputBatch(identity, batches.size(), row.data().rawAssignedRecordBatch())));
            return new Snapshot(root, extent, batches, completeIndexes);
        }
    }

    private static MessageDigest sha() {
        try {
            return MessageDigest.getInstance("SHA-256");
        } catch (NoSuchAlgorithmException impossible) {
            throw new IllegalStateException(impossible);
        }
    }
}
