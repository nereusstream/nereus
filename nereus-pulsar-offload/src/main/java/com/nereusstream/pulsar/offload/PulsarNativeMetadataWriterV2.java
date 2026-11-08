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
package com.nereusstream.pulsar.offload;

import com.nereusstream.domain.bytes.CanonicalBytes;
import com.nereusstream.domain.bytes.Sha256Digest;
import com.nereusstream.metadata.oxia.v2.retention.OxiaPhysicalMemberInventoryV2.Member;
import com.nereusstream.metadata.oxia.v2.retention.OxiaQuotaTargetDeleteStoreV2;
import com.nereusstream.metadata.spi.retention.ExactMetadataTransactionStoreV1;
import com.nereusstream.storage.api.lifecycle.PhysicalResourceIdCodecV2;
import com.nereusstream.storage.api.lifecycle.PhysicalResourceIdV2;
import com.nereusstream.storage.object.gc.M5TargetDeleteAuthorityCoordinatorV1;
import com.nereusstream.storage.object.gc.M5TargetDeleteAuthorityRecordsV1.ProofBoundWriterClassV1;
import com.nereusstream.storage.object.gc.M5TargetDeleteMultiWriterGuardV2;
import com.nereusstream.storage.object.materialization.M5MaterializationRecordsV1.PositionDomain;
import com.nereusstream.storage.object.retention.M5ClosedWriterRegistryV1;
import com.nereusstream.storage.object.retention.M5ClosedWriterRegistryV1.RegisteredReferenceWriterV1;
import com.nereusstream.storage.object.retention.M5PulsarAggregateAuthorityCodecV1;
import com.nereusstream.storage.object.retention.M5PulsarAggregateRetirementCoordinatorV1;
import com.nereusstream.storage.object.retention.M5ReferenceMutationGuardV1;
import com.nereusstream.storage.object.retention.M5ReferenceMutationGuardV1.ExternalMutationOutcomeV1;
import com.nereusstream.storage.object.retention.M5ReferenceMutationGuardV1.ExternalMutationResultV1;
import com.nereusstream.storage.object.retention.M5ReferenceMutationGuardV1.GuardOutcomeV1;
import com.nereusstream.storage.object.retention.M5ReferenceMutationGuardV1.GuardedMutationRequestV1;
import com.nereusstream.storage.object.retention.M5RetentionRecordsV1.ReferenceKindV1;
import com.nereusstream.storage.object.retention.M5RetentionRecordsV1.ReferenceTargetKindV1;
import java.io.ByteArrayOutputStream;
import java.io.DataOutputStream;
import java.util.HashSet;
import java.util.List;
import java.util.Map;
import java.util.Objects;
import java.util.Optional;
import java.util.Set;
import java.util.TreeSet;
import java.util.UUID;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.CompletionStage;
import java.util.function.Supplier;
import org.apache.bookkeeper.mledger.impl.MetaStore;
import org.apache.bookkeeper.mledger.impl.MetaStoreImpl;
import org.apache.bookkeeper.mledger.offload.OffloadUtils;
import org.apache.bookkeeper.mledger.proto.ManagedLedgerInfo.LedgerInfo;
import org.apache.pulsar.metadata.api.Stat;

/** Native metadata C-aggregate tickets. This adapter alone is not all-writer or physical-delete qualification. */
public final class PulsarNativeMetadataWriterV2 implements MetaStore.StoredMutationGuard, AutoCloseable {
    private final PulsarNativeBindingV1 binding;
    private final PulsarNativeReadCellBudgetV1 budget;
    private static final int MAX_BYTES = 8_388_608;
    private final PulsarNativeReadCellBudgetV1.Reservation lifetime;
    private final MetaStoreImpl nativeStore;
    private final M5ReferenceMutationGuardV1 guard;
    private final RegisteredReferenceWriterV1 writer;
    private final PulsarNativeBookKeeperCreateV1 creation;
    private final PulsarNativeM4ControlV1 sourceControl;
    private final Set<Mutation<?>> pending = new HashSet<>();
    private long pendingBytes;
    private boolean closed;
    private CompletableFuture<Void> predecessorReads = CompletableFuture.completedFuture(null);
    private S3PulsarOffloadObjectStoreV1.MaterializationConnection objectConnection;
    private com.nereusstream.metadata.oxia.v2.retention.OxiaQuotaTargetDeleteStoreV2 objectRoute;
    private CompletableFuture<Void> objectAdmission;
    private NereusPulsarLedgerOffloaderV1 objectOffloader;
    private SdkRecoveryReference sdkRecoveryReference;

    private PulsarNativeMetadataWriterV2(
            PulsarNativeBindingV1 binding,
            PulsarNativeReadCellBudgetV1.Reservation lifetime,
            MetaStoreImpl nativeStore,
            M5ReferenceMutationGuardV1 guard,
            RegisteredReferenceWriterV1 writer,
            PulsarNativeBookKeeperCreateV1 creation,
            PulsarNativeReadCellBudgetV1 budget) {
        this.binding = binding;
        this.lifetime = lifetime;
        this.nativeStore = nativeStore;
        this.guard = guard;
        this.writer = writer;
        this.creation = creation;
        this.budget = budget;
        sourceControl = binding.m4Control();
    }

    /** Install before factory writes; the same actual aggregate must already have the exact closed enrollment. */
    public static CompletableFuture<PulsarNativeMetadataWriterV2> install(
            PulsarNativeBindingV1 binding,
            PulsarNativeReadCellBudgetV1 budget,
            MetaStoreImpl nativeStore,
            M5ClosedWriterRegistryV1 registry,
            M5PulsarAggregateRetirementCoordinatorV1 coordinator,
            String authorityKey,
            PulsarNativeBookKeeperCreateV1 creation) {
        try {
            PulsarNativeWriterRegistryV2.requireNativeOwners(registry);
        } catch (RuntimeException failure) {
            return CompletableFuture.failedFuture(failure);
        }
        Objects.requireNonNull(nativeStore, "Native metadata store");
        Objects.requireNonNull(creation, "actual Native BookKeeper creation scope");
        if (!creation.ownsMetadataWriterScope(binding, budget)
                || !creation.spec().allocatorOwner().equals(budget.nativeOwnerIdentity(binding))) {
            return CompletableFuture.failedFuture(
                    new IllegalArgumentException("Native metadata writer differs from its actual SDK/Cell owner"));
        }
        String expectedAuthority = new com.nereusstream.metadata.oxia.v2.key.OxiaV2AuthorityKeys(
                        binding.authorityRoot())
                .aggregateKey(binding.authority().aggregate().binding().incarnationIdentity());
        if (!expectedAuthority.equals(authorityKey)) {
            return CompletableFuture.failedFuture(
                    new IllegalArgumentException("Native metadata authority key differs"));
        }
        var writer = registry.writerFor(ReferenceKindV1.PULSAR_SUBSCRIPTION_OR_REPLICATION_CURSOR);
        if (!writer.writerClass().equals(PulsarNativeMetadataWriterV2.class.getName())) {
            return CompletableFuture.failedFuture(
                    new IllegalArgumentException("Native metadata writer is not enrolled"));
        }
        var work = binding.revalidate()
                .thenCompose(unused -> coordinator.read(authorityKey))
                .thenCompose(value -> {
                    var actual = M5PulsarAggregateAuthorityCodecV1.decodeAuthority(
                            value.orElseThrow().canonicalStoredBytes());
                    if (!actual.originalAggregateSha256()
                                    .equals(binding.authority().aggregate().canonicalStoredDigest())
                            || !actual.bindingId()
                                    .equals(binding.bindingIdentity().bindingId())
                            || !actual.writerEnrollment().equals(Optional.of(registry.enrollment()))
                            || !actual.originalAggregateVersion()
                                    .equals(Optional.of(
                                            binding.authority().aggregate().aggregateSourceVersion()))) {
                        throw new IllegalArgumentException("Native metadata enrollment differs from actual P1 source");
                    }
                    var lifetime = budget.reserveCursorObservation(binding, MAX_BYTES);
                    var original = new PulsarNativeMetadataWriterV2(
                            binding,
                            lifetime,
                            nativeStore,
                            M5ReferenceMutationGuardV1.forPulsarAggregate(
                                    registry, coordinator, authorityKey, actual.originalAggregateSha256()),
                            writer,
                            creation,
                            budget);
                    return lifetime.ready()
                            .thenApply(unused -> {
                                nativeStore.installStoredMutationGuard(original);
                                lifetime.actorStarting();
                                return original;
                            })
                            .exceptionallyCompose(failure -> lifetime.releaseBeforeActor()
                                    .thenCompose(unused -> CompletableFuture.failedFuture(failure)));
                })
                .toCompletableFuture();
        var observer = new CompletableFuture<PulsarNativeMetadataWriterV2>();
        work.whenComplete((original, failure) -> {
            if (failure != null) {
                observer.completeExceptionally(failure);
            } else if (!observer.complete(original)) {
                original.lifetime.retainUndeliveredSdk(original::closeAsync, false);
                original.closeAsync();
            }
        });
        return observer;
    }

    /** Actual SDK birth/recovery metadata stays referenced from before factory work until its original scope closes. */
    synchronized CompletableFuture<Void> openSdkRecoveryReference(M5ClosedWriterRegistryV1 registry) {
        if (closed) {
            return CompletableFuture.failedFuture(new IllegalStateException("Native metadata writer is closed"));
        }
        if (sdkRecoveryReference == null) {
            var owner = registry.writerFor(ReferenceKindV1.RECOVERY_CHECKPOINT_OR_SNAPSHOT);
            if (!owner.writerClass().equals(getClass().getName())) {
                return CompletableFuture.failedFuture(
                        new IllegalArgumentException("SDK recovery writer is not enrolled"));
            }
            if (pendingBytes + SdkRecoveryReference.BYTES > MAX_BYTES) {
                return CompletableFuture.failedFuture(
                        new IllegalStateException("SDK recovery descriptor bound exhausted"));
            }
            sdkRecoveryReference = new SdkRecoveryReference(owner);
            pendingBytes += SdkRecoveryReference.BYTES;
            sdkRecoveryReference.start();
        }
        return sdkRecoveryReference.ready.copy();
    }

    private final class SdkRecoveryReference {
        private static final int BYTES = 4096;
        private final Sha256Digest root = creation.spec().sha256();
        private final GuardedMutationRequestV1 request;
        private final CompletableFuture<Void> ready = new CompletableFuture<>();
        private final CompletableFuture<Void> ended = new CompletableFuture<>();
        private final CompletableFuture<Void> settled = new CompletableFuture<>();
        private boolean dispatched;
        private boolean released;

        private SdkRecoveryReference(RegisteredReferenceWriterV1 owner) {
            request = new GuardedMutationRequestV1(
                    owner,
                    ReferenceTargetKindV1.PULSAR_AGGREGATE,
                    binding.authority().aggregate().canonicalStoredDigest(),
                    Sha256Digest.hash(CanonicalBytes.copyOf(
                            UUID.randomUUID().toString().getBytes(java.nio.charset.StandardCharsets.US_ASCII))),
                    root);
        }

        private void start() {
            guard.execute(request, () -> budget.requireNativeOwner(
                                    binding, creation.spec().allocatorOwner())
                            .thenCompose(unused -> binding.revalidate())
                            .thenCompose(unused -> {
                                dispatched = true;
                                ready.complete(null);
                                return ended.thenApply(done ->
                                        new ExternalMutationResultV1(ExternalMutationOutcomeV1.APPLIED_EXACT, root));
                            }))
                    .whenComplete((result, failure) -> {
                        settled.complete(null);
                        if (!ready.isDone()) {
                            ready.completeExceptionally(
                                    failure != null
                                            ? failure
                                            : new IllegalStateException(
                                                    "original SDK recovery reference was not admitted"));
                        }
                        if (failure == null && result.outcome() == GuardOutcomeV1.MUTATION_APPLIED_AND_TICKET_CLEARED) {
                            release();
                        }
                    });
        }

        private CompletableFuture<Void> close() {
            creation.requireSdkMetadataScopeClosed();
            ended.complete(null);
            return settled.thenCompose(unused -> dispatched
                            ? guard.reconcileCompleted(
                                    request,
                                    new ExternalMutationResultV1(ExternalMutationOutcomeV1.APPLIED_EXACT, root))
                            : guard.reconcileUndispatched(request))
                    .thenAccept(result -> {
                        if (result.outcome() != GuardOutcomeV1.MUTATION_APPLIED_AND_TICKET_CLEARED
                                && result.outcome() != GuardOutcomeV1.UNDISPATCHED_TICKET_FENCED_AND_CLEARED) {
                            throw new IllegalStateException("original SDK recovery reference cleanup remains unknown");
                        }
                        release();
                    })
                    .toCompletableFuture();
        }

        private void release() {
            synchronized (PulsarNativeMetadataWriterV2.this) {
                if (!released) {
                    released = true;
                    pendingBytes -= BYTES;
                }
            }
        }
    }

    @Override
    public <T> CompletableFuture<T> execute(
            String ledgerName,
            String path,
            byte[] proposedStoredBytes,
            Optional<Long> expectedVersion,
            Supplier<CompletableFuture<T>> originalDispatch) {
        return admitMutation(ledgerName, path, proposedStoredBytes, expectedVersion, originalDispatch, null);
    }

    @Override
    public <T> CompletableFuture<T> executeCursorReset(
            String ledgerName,
            String cursorName,
            MetaStore.CursorResetContext context,
            Supplier<CompletableFuture<T>> originalDispatch) {
        if (cursorName.isEmpty() || cursorName.contains("/") || cursorName.indexOf('\0') >= 0) {
            return CompletableFuture.failedFuture(new IllegalArgumentException("Native cursor reset name differs"));
        }
        return admitMutation(
                ledgerName,
                "/managed-ledgers/" + ledgerName + "/" + cursorName,
                null,
                Optional.of(context.cursorMetadataVersion()),
                originalDispatch,
                context);
    }

    private <T> CompletableFuture<T> admitMutation(
            String ledgerName,
            String path,
            byte[] proposedStoredBytes,
            Optional<Long> expectedVersion,
            Supplier<CompletableFuture<T>> originalDispatch,
            MetaStore.CursorResetContext reset) {
        final Mutation<T> original;
        try {
            String parent = "/managed-ledgers/" + binding.managedLedgerName();
            if (!ledgerName.equals(binding.managedLedgerName())
                    || !(path.equals(parent) || path.startsWith(parent + "/"))
                    || path.getBytes(java.nio.charset.StandardCharsets.UTF_8).length > 4096
                    || expectedVersion.isPresent() && expectedVersion.orElseThrow() < -1) {
                throw new IllegalArgumentException("Native metadata mutation path/version differs from its Binding");
            }
            int bytes = Math.addExact(4096, proposedStoredBytes == null ? 0 : proposedStoredBytes.length);
            if (bytes > MAX_BYTES) {
                throw new IllegalArgumentException("Native metadata input exceeds its admitted writer lifetime");
            }
            original = new Mutation<>(
                    ledgerName, path, proposedStoredBytes, expectedVersion, originalDispatch, bytes, reset);
            final boolean admitted;
            synchronized (this) {
                admitted = !closed && pending.size() < 256 && pendingBytes + bytes <= MAX_BYTES;
                if (admitted) {
                    pending.add(original);
                    pendingBytes += bytes;
                }
            }
            if (!admitted) {
                throw new IllegalStateException("Native metadata writer is closed or at its admitted bound");
            }
        } catch (Throwable failure) {
            return CompletableFuture.failedFuture(failure);
        }
        original.start();
        return original.result.copy();
    }

    public CompletableFuture<Void> reconcileCleanup() {
        final Set<Mutation<?>> originals;
        synchronized (this) {
            originals = Set.copyOf(pending);
        }
        return CompletableFuture.allOf(
                originals.stream().map(Mutation::reconcile).toArray(CompletableFuture[]::new));
    }

    CompletableFuture<Void> admitSealedPlainSources() {
        synchronized (this) {
            if (closed || !pending.isEmpty()) {
                return CompletableFuture.failedFuture(new IllegalStateException(
                        "Native source admission requires its idle installed metadata writer"));
            }
        }
        return creation.admitSealedPlainSources(
                nativeStore, guard, writer, objectOffloader, objectConnection, objectRoute);
    }

    /** Borrows this factory's original configured SDK; it may reopen, but cannot initialize, Object quota. */
    synchronized CompletableFuture<Void> admitObjectPhysicalScope(
            NereusPulsarLedgerOffloaderV1 offloader,
            PhysicalResourceIdV2.Namespace namespace,
            long maximumObjectBytes) {
        if (closed || !pending.isEmpty() || objectConnection != null) {
            return CompletableFuture.failedFuture(
                    new IllegalStateException("Native Object scope requires its idle original metadata writer"));
        }
        try {
            objectConnection =
                    offloader.borrowConfiguredMaterializationConnection(binding, namespace, maximumObjectBytes);
            objectOffloader = offloader;
            var original = objectConnection;
            objectAdmission = budget.requireNativeOwner(binding, creation.spec().allocatorOwner())
                    .thenCompose(unused -> original.admitProfile(binding))
                    .thenCompose(unused -> binding.materializationQuota(original.physical()))
                    .thenCompose(route -> original.requireProfileCurrent().thenRun(() -> {
                        synchronized (PulsarNativeMetadataWriterV2.this) {
                            objectRoute = route;
                        }
                    }));
            return objectAdmission.copy();
        } catch (Throwable failure) {
            return CompletableFuture.failedFuture(failure);
        }
    }

    synchronized CompletableFuture<Void> requireObjectPhysicalScope() {
        if (objectConnection == null || objectAdmission == null) {
            return CompletableFuture.failedFuture(new IllegalStateException("Native Object scope is absent"));
        }
        objectAdmission = objectAdmission
                .thenCompose(unused -> objectConnection.requireProfileCurrent())
                .thenCompose(unused -> binding.materializationQuota(objectConnection.physical()))
                .thenCompose(current -> current.quota()
                        .snapshot()
                        .thenCombine(objectRoute.quota().snapshot(), (fresh, original) -> {
                            if (!fresh.head().scope().equals(original.head().scope())) {
                                throw new IllegalStateException("Native Object quota route changed");
                            }
                            return (Void) null;
                        }))
                .toCompletableFuture();
        return objectAdmission.copy();
    }

    CompletableFuture<Void> enableNativeOutput(
            M5ClosedWriterRegistryV1 registry, Supplier<CompletionStage<Void>> requireNativeOwnership) {
        return requireObjectPhysicalScope().thenRun(() -> {
            synchronized (this) {
                if (closed || objectConnection == null || objectRoute == null) {
                    throw new IllegalStateException("Native factory cannot enable an unowned Object output route");
                }
                objectOffloader.enableEnrolledOutput(
                        binding, budget, objectConnection, objectRoute, registry, requireNativeOwnership);
            }
        });
    }

    private CompletableFuture<Void> closeObjectPhysicalScope() {
        final S3PulsarOffloadObjectStoreV1.MaterializationConnection original;
        final CompletableFuture<Void> admission;
        synchronized (this) {
            original = objectConnection;
            admission = objectAdmission;
        }
        if (original == null) {
            return CompletableFuture.completedFuture(null);
        }
        var settled = admission == null ? CompletableFuture.<Void>completedFuture(null) : admission;
        return settled.handle((unused, failure) -> null)
                .thenCompose(unused -> original.reconcileScopeForClose())
                .thenRun(original::close);
    }

    /** One original predecessor buffer at a time within the factory's retained workspace. */
    private synchronized CompletableFuture<Void> capturePredecessor(Mutation<?> mutation) {
        var read = predecessorReads
                .handle((unused, failure) -> null)
                .thenCompose(
                        unused -> nativeStore.readStoredMutationValue(mutation.ledgerName, mutation.path, MAX_BYTES))
                .thenCompose(
                        predecessor -> sourceControl.captureSelectedSourceMap().thenCompose(selection -> {
                            mutation.bindSelection(selection);
                            return mutation.bindPredecessor(predecessor);
                        }));
        predecessorReads = read.handle((unused, failure) -> null);
        return read;
    }

    private synchronized CompletableFuture<Void> revalidatePredecessor(Mutation<?> mutation) {
        var read = predecessorReads
                .handle((unused, failure) -> null)
                .thenCompose(
                        unused -> nativeStore.readStoredMutationValue(mutation.ledgerName, mutation.path, MAX_BYTES))
                .thenAccept(mutation::requirePredecessor)
                .thenCompose(unused -> mutation.revalidateResetParent())
                .thenCompose(unused -> sourceControl.requireSelectedSourceSnapshot(mutation.selection));
        predecessorReads = read.handle((unused, failure) -> null);
        return read;
    }

    CompletableFuture<Void> prepareSdkClose() {
        synchronized (this) {
            closed = true;
        }
        return reconcileCleanup()
                .thenCompose(unused -> creation.reconcileSourceAdmission())
                .thenCompose(unused -> objectOffloader == null
                        ? CompletableFuture.completedFuture(null)
                        : objectOffloader.drainOriginalOutput())
                .thenCompose(unused -> closeObjectPhysicalScope());
    }

    public CompletableFuture<Void> closeAsync() {
        return prepareSdkClose()
                .thenCompose(unused -> sdkRecoveryReference == null
                        ? CompletableFuture.completedFuture(null)
                        : sdkRecoveryReference.close())
                .thenCompose(unused -> lifetime.releaseAfterDrain());
    }

    @Override
    public void close() throws Exception {
        closeAsync().get();
    }

    private final class Mutation<T> {
        private final String ledgerName;
        private final String path;
        private final byte[] bytes;
        private final Supplier<CompletableFuture<T>> dispatch;
        private int charge;
        private GuardedMutationRequestV1 request;
        private Optional<Stat> predecessorStat = Optional.empty();
        private Optional<Sha256Digest> predecessorBody = Optional.empty();
        private PulsarNativeM4ControlV1.SelectedSourceSnapshot selection;
        private List<PhysicalResourceIdV2> physicalTargets = List.of();
        private M5TargetDeleteMultiWriterGuardV2.Context physicalContext;
        private Map<PhysicalResourceIdV2, M5TargetDeleteMultiWriterGuardV2.Context> physicalContexts = Map.of();
        private M5TargetDeleteMultiWriterGuardV2.Result<ExternalMutationResultV1> physicalResult;
        private M5TargetDeleteMultiWriterGuardV2 physicalGuard;
        private final List<PulsarNativeOffloadedReferenceV2> sdkCaptures = new java.util.ArrayList<>();
        private final Map<PhysicalResourceIdV2, Sha256Digest> objectFacts = new java.util.HashMap<>();
        private PulsarNativeReadCellBudgetV1.Reservation mappedWorkspace;
        private PulsarNativeM4ControlV1.SelectedMaterializationSnapshot materialization;
        private PulsarNativeM4SourceMapV1 selectedMap;
        private final Optional<Long> expectedVersion;
        private final MetaStore.CursorResetContext reset;
        private Optional<Stat> resetParentStat = Optional.empty();
        private Optional<Sha256Digest> resetParentBody = Optional.empty();
        private final Sha256Digest operation;
        private final CompletableFuture<T> result = new CompletableFuture<>();
        private final CompletableFuture<Void> settled = new CompletableFuture<>();
        private CompletableFuture<T> sdk;
        private ExternalMutationResultV1 terminal;
        private Throwable sdkFailure;
        private boolean dispatched;
        private boolean enteredGuard;
        private boolean finished;
        private CompletableFuture<Void> cleanup;
        private CompletableFuture<Void> released;

        private Mutation(
                String ledgerName,
                String path,
                byte[] proposed,
                Optional<Long> version,
                Supplier<CompletableFuture<T>> dispatch,
                int charge,
                MetaStore.CursorResetContext reset)
                throws Exception {
            this.ledgerName = ledgerName;
            this.path = path;
            this.bytes = proposed == null ? null : proposed.clone();
            this.dispatch = dispatch;
            this.charge = charge;
            this.reset = reset;
            expectedVersion = version;
            operation = Sha256Digest.hash(CanonicalBytes.copyOf(
                    UUID.randomUUID().toString().getBytes(java.nio.charset.StandardCharsets.US_ASCII)));
        }

        private CompletableFuture<Void> bindPredecessor(Optional<MetaStoreImpl.StoredMutationValue> predecessor) {
            predecessorStat = predecessor.map(MetaStoreImpl.StoredMutationValue::stat);
            predecessorBody = predecessor.map(value -> Sha256Digest.hash(CanonicalBytes.copyOf(value.storedBytes())));
            var targets = new TreeSet<PhysicalResourceIdV2>();
            CompletableFuture<Void> captured = CompletableFuture.completedFuture(null);
            try {
                if (path.equals("/managed-ledgers/" + ledgerName)) {
                    if (selection.source().isPresent()) {
                        mappedWorkspace = budget.reserveCursorObservation(binding, MAX_BYTES);
                        captured = mappedWorkspace
                                .ready()
                                .thenRun(mappedWorkspace::actorStarting)
                                .thenCompose(unused -> requireObjectPhysicalScope())
                                .thenCompose(unused -> captureMappedSources());
                    }
                    if (predecessor.isPresent()) {
                        captured = captured.thenCompose(unused -> addDataTargets(
                                targets, predecessor.orElseThrow().storedBytes()));
                    }
                    if (bytes != null) {
                        captured = captured.thenCompose(unused -> addDataTargets(targets, bytes));
                    }
                } else {
                    if (predecessor.isPresent()) {
                        addCheckpointTarget(targets, predecessor.orElseThrow().storedBytes());
                    }
                    if (bytes != null) {
                        addCheckpointTarget(targets, bytes);
                    }
                    if (reset != null) {
                        var cursor = predecessor.orElseThrow(
                                () -> new IllegalStateException("Native reset cursor is absent"));
                        if (cursor.stat().getVersion() != reset.cursorMetadataVersion()
                                || nativeStore
                                                .parseManagedCursorInfo(cursor.storedBytes(), MAX_BYTES)
                                                .getCursorsLedgerId()
                                        != reset.cursorLedgerId()) {
                            throw new IllegalStateException("Native reset cursor predecessor differs");
                        }
                        captured = bindResetParent(targets);
                    }
                }
            } catch (Exception invalid) {
                return CompletableFuture.failedFuture(
                        new IllegalArgumentException("actual Native reference metadata is invalid", invalid));
            }
            return captured.thenRun(() -> bindPhysicalTargets(predecessor, targets));
        }

        private CompletableFuture<Void> bindResetParent(Set<PhysicalResourceIdV2> targets) {
            return nativeStore
                    .readStoredMutationValue(ledgerName, "/managed-ledgers/" + ledgerName, MAX_BYTES)
                    .thenCompose(observed -> {
                        var parent =
                                observed.orElseThrow(() -> new IllegalStateException("Native reset parent is absent"));
                        try {
                            var info = nativeStore.parseManagedLedgerInfo(parent.storedBytes(), MAX_BYTES);
                            if (parent.stat().getVersion() != reset.parentMetadataVersion()
                                    || !java.util.Arrays.equals(
                                            reset.parentLedgerFactsSha256(),
                                            MetaStore.CursorResetContext.ledgerFactsSha256(
                                                    info.getLedgerInfosList()))) {
                                throw new IllegalStateException("Native reset parent facts differ");
                            }
                            resetParentStat = Optional.of(parent.stat());
                            resetParentBody =
                                    Optional.of(Sha256Digest.hash(CanonicalBytes.copyOf(parent.storedBytes())));
                            CompletableFuture<Void> mapped = CompletableFuture.completedFuture(null);
                            if (selection.source().isPresent()) {
                                mappedWorkspace = budget.reserveCursorObservation(binding, MAX_BYTES);
                                mapped = mappedWorkspace
                                        .ready()
                                        .thenRun(mappedWorkspace::actorStarting)
                                        .thenCompose(unused -> requireObjectPhysicalScope())
                                        .thenCompose(unused -> captureMappedSources());
                            }
                            return mapped.thenCompose(unused -> addDataTargets(targets, parent.storedBytes()));
                        } catch (Exception invalid) {
                            return CompletableFuture.failedFuture(invalid);
                        }
                    });
        }

        private CompletableFuture<Void> revalidateResetParent() {
            if (reset == null) {
                return CompletableFuture.completedFuture(null);
            }
            return nativeStore
                    .readStoredMutationValue(ledgerName, "/managed-ledgers/" + ledgerName, MAX_BYTES)
                    .thenCompose(parent -> {
                        if (!parent.map(MetaStoreImpl.StoredMutationValue::stat).equals(resetParentStat)
                                || !parent.map(value -> Sha256Digest.hash(CanonicalBytes.copyOf(value.storedBytes())))
                                        .equals(resetParentBody)) {
                            throw new IllegalStateException("Native reset parent changed before dispatch");
                        }
                        // The original P1 ticket prevents trim from advancing until this reset settles.
                        return binding.logicalTrimCoordinator().readFrontier().thenAccept(frontier -> {
                            if (frontier.isEmpty()) {
                                return;
                            }
                            var trim = frontier.orElseThrow()
                                    .pulsarContext()
                                    .orElseThrow()
                                    .newFrontier();
                            final org.apache.bookkeeper.mledger.proto.ManagedLedgerInfo info;
                            try {
                                info = nativeStore.parseManagedLedgerInfo(
                                        parent.orElseThrow().storedBytes(), MAX_BYTES);
                            } catch (Exception invalid) {
                                throw new MetaStore.CursorResetNotDispatchedException(
                                        "Native reset trim directory cannot be decoded", invalid);
                            }
                            int requestedIndex = -1;
                            int trimIndex = -1;
                            var ledgers = info.getLedgerInfosList();
                            for (int index = 0; index < ledgers.size(); index++) {
                                long ledgerId = ledgers.get(index).getLedgerId();
                                if (ledgerId == reset.requestedLedgerId()) {
                                    requestedIndex = index;
                                }
                                if (ledgerId == trim.ledgerId()) {
                                    trimIndex = index;
                                }
                            }
                            if (requestedIndex < 0 || trimIndex < 0) {
                                throw new MetaStore.CursorResetNotDispatchedException(
                                        "Native reset lacks the committed trim directory lineage", null);
                            }
                            if (requestedIndex < trimIndex
                                    || requestedIndex == trimIndex && reset.requestedEntryId() < trim.entryId()) {
                                throw new MetaStore.CursorResetNotDispatchedException(
                                        "Native cursor reset precedes committed logical trim", null);
                            }
                        });
                    });
        }

        private void bindPhysicalTargets(
                Optional<MetaStoreImpl.StoredMutationValue> predecessor, Set<PhysicalResourceIdV2> targets) {
            physicalTargets = List.copyOf(targets);
            var routes = new java.util.HashMap<String, OxiaQuotaTargetDeleteStoreV2>();
            for (var target : physicalTargets) {
                routes.put(target.authorityKey(), routeFor(target));
            }
            physicalGuard = new M5TargetDeleteMultiWriterGuardV2(
                    new M5TargetDeleteAuthorityCoordinatorV1(new PhysicalRoutes(Map.copyOf(routes))));
            var encoded = new ByteArrayOutputStream();
            try (var out = new DataOutputStream(encoded)) {
                out.writeInt(0x4e504d57); // NPMW
                out.writeInt(reset == null ? 7 : 8);
                out.write(binding.metadataNamespace().encode().toByteArray());
                out.write(binding.bindingIdentity().bindingId().digest().bytes().toByteArray());
                out.write(binding.bindingIdentity().incarnationSha256().bytes().toByteArray());
                out.write(binding.bindingIdentity().storageEpochSha256().bytes().toByteArray());
                out.writeBoolean(selection.selector().isPresent());
                out.writeBoolean(selection.manifest());
                if (selection.selector().isPresent()) {
                    var selector = selection.selector().orElseThrow();
                    var sourceMap = selection.source().orElseThrow();
                    out.write(selector.canonicalStoredSha256().bytes().toByteArray());
                    out.write(Sha256Digest.hash(selector.metadataVersion().value())
                            .bytes()
                            .toByteArray());
                    out.write(sourceMap.canonicalStoredSha256().bytes().toByteArray());
                    out.write(Sha256Digest.hash(sourceMap.metadataVersion().value())
                            .bytes()
                            .toByteArray());
                }
                out.writeBoolean(materialization != null);
                if (materialization != null) {
                    var facts = new java.util.TreeMap<String, ExactMetadataTransactionStoreV1.VersionedValue>();
                    facts.putAll(materialization.immutableFacts());
                    facts.putAll(materialization.nativeFacts());
                    out.writeInt(facts.size());
                    for (var fact : facts.values()) {
                        out.writeUTF(fact.key());
                        out.write(fact.canonicalStoredSha256().bytes().toByteArray());
                        out.write(Sha256Digest.hash(fact.metadataVersion().value())
                                .bytes()
                                .toByteArray());
                    }
                }
                out.writeUTF(path);
                if (reset != null) {
                    out.writeLong(reset.parentMetadataVersion());
                    out.writeLong(reset.cursorMetadataVersion());
                    out.writeLong(reset.cursorLedgerId());
                    out.writeLong(reset.requestedLedgerId());
                    out.writeLong(reset.requestedEntryId());
                    out.writeBoolean(reset.forceReset());
                    out.write(reset.parentLedgerFactsSha256());
                    out.write(resetParentBody.orElseThrow().bytes().toByteArray());
                }
                out.writeBoolean(bytes != null);
                out.writeLong(expectedVersion.orElse(Long.MIN_VALUE));
                out.writeBoolean(predecessor.isPresent());
                if (predecessor.isPresent()) {
                    var original = predecessor.orElseThrow();
                    out.writeLong(original.stat().getVersion());
                    out.writeLong(original.stat().getCreationTimestamp());
                    out.writeLong(original.stat().getModificationTimestamp());
                    out.writeBoolean(original.stat().isEphemeral());
                    out.writeBoolean(original.stat().isCreatedBySelf());
                    out.write(predecessorBody.orElseThrow().bytes().toByteArray());
                }
                out.write(Sha256Digest.hash(CanonicalBytes.copyOf(bytes == null ? new byte[0] : bytes))
                        .bytes()
                        .toByteArray());
                out.writeInt(physicalTargets.size());
                for (var target : physicalTargets) {
                    out.write(target.sha256().bytes().toByteArray());
                    out.writeBoolean(objectFacts.containsKey(target));
                    if (objectFacts.containsKey(target)) {
                        out.write(objectFacts.get(target).bytes().toByteArray());
                    }
                }
            } catch (java.io.IOException failure) {
                throw new IllegalStateException("Native mutation source commitment failed", failure);
            }
            var root = Sha256Digest.hash(CanonicalBytes.copyOf(encoded.toByteArray()));
            request = new GuardedMutationRequestV1(
                    writer,
                    ReferenceTargetKindV1.PULSAR_AGGREGATE,
                    binding.authority().aggregate().canonicalStoredDigest(),
                    operation,
                    root);
            physicalContext = new M5TargetDeleteMultiWriterGuardV2.Context(
                    ProofBoundWriterClassV1.REFERENCE_SHARED_PHYSICAL_MEMBER_V1,
                    creation.spec().configuration(),
                    Sha256Digest.hash(creation.spec().allocatorOwner()),
                    root);
            physicalContexts = physicalTargets.stream()
                    .collect(java.util.stream.Collectors.toUnmodifiableMap(
                            resource -> resource,
                            resource -> resource instanceof PhysicalResourceIdV2.ObjectVersion
                                    ? new M5TargetDeleteMultiWriterGuardV2.Context(
                                            ProofBoundWriterClassV1.REFERENCE_SHARED_PHYSICAL_MEMBER_V1,
                                            objectConnection.profile().providerConfigurationSha256(),
                                            Sha256Digest.hash(creation.spec().allocatorOwner()),
                                            root)
                                    : physicalContext));
        }

        private CompletableFuture<Void> captureMappedSources() {
            if (!selection.manifest()) {
                selectedMap = selection.map().orElseThrow();
                if (!PulsarNativeMaterializedReadV2.isRecipe(selectedMap.actualViewBytes())) {
                    return CompletableFuture.completedFuture(null);
                }
            }
            return sourceControl
                    .readSelectedMaterializationSnapshot(creation.controlExecutor())
                    .thenCompose(actual -> {
                        if (!actual.selected().providerProfile().equals(objectConnection.profile())
                                || !actual.selected()
                                        .selectorFact()
                                        .canonicalStoredBytes()
                                        .equals(selection
                                                .selector()
                                                .orElseThrow()
                                                .canonicalStoredBytes())
                                || actual.selected().readMap().decodedWorkspaceBytes() > 64L * 1024 * 1024) {
                            throw new IllegalArgumentException(
                                    "Native selected map differs from its actual SDK scope/cap");
                        }
                        materialization = actual;
                        return sourceControl.requireSelectedSourceSnapshot(selection);
                    });
        }

        private void bindSelection(PulsarNativeM4ControlV1.SelectedSourceSnapshot observed) {
            int bytes = 0;
            for (var fact : java.util.stream.Stream.concat(observed.selector().stream(), observed.source().stream())
                    .toList()) {
                bytes = Math.addExact(bytes, fact.canonicalStoredBytes().length());
                bytes = Math.addExact(bytes, fact.metadataVersion().value().length());
            }
            synchronized (PulsarNativeMetadataWriterV2.this) {
                if (pendingBytes + bytes > MAX_BYTES) {
                    throw new IllegalStateException("Native selection snapshots exceed the admitted writer bound");
                }
                pendingBytes += bytes;
                charge = Math.addExact(charge, bytes);
                selection = observed;
            }
        }

        private void addCheckpointTarget(Set<PhysicalResourceIdV2> targets, byte[] stored) throws Exception {
            long id = nativeStore.parseManagedCursorInfo(stored, MAX_BYTES).getCursorsLedgerId();
            if (id < -1) {
                throw new IllegalArgumentException("actual Native checkpoint ledger ID is invalid");
            }
            if (id >= 0) {
                targets.add(new PhysicalResourceIdV2.BookKeeperLedger(
                        creation.namespace().namespace(), id));
            }
        }

        private CompletableFuture<Void> addDataTargets(Set<PhysicalResourceIdV2> targets, byte[] stored) {
            final org.apache.bookkeeper.mledger.proto.ManagedLedgerInfo info;
            try {
                info = nativeStore.parseManagedLedgerInfo(stored, MAX_BYTES);
            } catch (Exception invalid) {
                return CompletableFuture.failedFuture(invalid);
            }
            if (info.getLedgerInfosList().size() > 512) {
                throw new IllegalArgumentException("actual Native data chain exceeds its admitted bound");
            }
            CompletableFuture<Void> work = CompletableFuture.completedFuture(null);
            for (var source : info.getLedgerInfosList()) {
                if (source.getLedgerId() < 0) {
                    throw new IllegalArgumentException("actual Native data ledger ID is invalid");
                }
                if (materialization != null || selectedMap != null) {
                    var resolved = materialization != null
                            ? materialization
                                    .selected()
                                    .readMap()
                                    .physicalLedgerReferences(
                                            binding,
                                            creation.spec().namespaceBinding(),
                                            objectConnection.binding(),
                                            source.getLedgerId(),
                                            source.getEntries())
                            : selectedMap.physicalLedgerReferences(
                                    binding,
                                    creation.spec().namespaceBinding(),
                                    objectConnection.binding(),
                                    source.getLedgerId(),
                                    source.getEntries());
                    if (resolved.isPresent()) {
                        targets.addAll(resolved.orElseThrow());
                        continue;
                    }
                }
                if (!source.hasOffloadContext() || !source.getOffloadContext().isComplete()) {
                    targets.add(new PhysicalResourceIdV2.BookKeeperLedger(
                            creation.namespace().namespace(), source.getLedgerId()));
                } else {
                    if (!source.getOffloadContext().isBookkeeperDeleted()) {
                        targets.add(new PhysicalResourceIdV2.BookKeeperLedger(
                                creation.namespace().namespace(), source.getLedgerId()));
                    }
                    work = work.thenCompose(unused -> captureOffloadReferences(source))
                            .thenAccept(actual -> {
                                for (var resource : actual.resources()) {
                                    var old = objectFacts.putIfAbsent(resource, actual.sdkFactsSha256());
                                    if (old != null && !old.equals(actual.sdkFactsSha256())) {
                                        throw new IllegalStateException("Native immutable Object source facts differ");
                                    }
                                    if (old == null) {
                                        int charge = Math.addExact(
                                                128,
                                                PhysicalResourceIdCodecV2.encode(resource)
                                                        .length());
                                        synchronized (PulsarNativeMetadataWriterV2.this) {
                                            if (pendingBytes + charge > MAX_BYTES) {
                                                throw new IllegalStateException(
                                                        "Native Object references exceed the writer bound");
                                            }
                                            pendingBytes += charge;
                                            this.charge = Math.addExact(this.charge, charge);
                                        }
                                    }
                                    targets.add(resource);
                                }
                            });
                }
            }
            return work;
        }

        private CompletionStage<PulsarNativeOffloadedReferenceV2.Captured> captureOffloadReferences(LedgerInfo source) {
            if (objectOffloader == null || !objectOffloader.preservesIncompleteAttempt()) {
                return requireObjectPhysicalScope().thenCompose(unused -> {
                    var original = new PulsarNativeOffloadedReferenceV2(
                            objectOffloader, objectConnection.profile(), ledgerName, source);
                    sdkCaptures.add(original);
                    return original.observe();
                });
            }
            var context = source.getOffloadContext();
            if (!context.hasUidMsb()
                    || !context.hasUidLsb()
                    || !context.getDriverMetadata().getName().equals(objectOffloader.getOffloadDriverName())) {
                return CompletableFuture.failedFuture(
                        new IllegalStateException("Published native offload lacks its original provider attempt"));
            }
            var attempt = new UUID(context.getUidMsb(), context.getUidLsb());
            return objectOffloader
                    .captureEnrolledOffload(
                            source.getLedgerId(), attempt, OffloadUtils.getOffloadDriverMetadata(source))
                    .thenApply(actual -> {
                        var root = PulsarNativeOffloadedReferenceV2.resource(objectConnection.profile(), actual.root());
                        var data = PulsarNativeOffloadedReferenceV2.resource(objectConnection.profile(), actual.data());
                        var digest = Sha256Digest.hash(CanonicalBytes.copyOf(java.nio.ByteBuffer.allocate(128)
                                .put(root.sha256().bytes().toByteArray())
                                .put(data.sha256().bytes().toByteArray())
                                .put(java.util.HexFormat.of()
                                        .parseHex(actual.root().sha256()))
                                .put(java.util.HexFormat.of()
                                        .parseHex(actual.data().sha256()))
                                .array()));
                        return new PulsarNativeOffloadedReferenceV2.Captured(List.of(root, data), digest);
                    });
        }

        private void requirePredecessor(Optional<MetaStoreImpl.StoredMutationValue> current) {
            if (!current.map(MetaStoreImpl.StoredMutationValue::stat).equals(predecessorStat)
                    || !current.map(value -> Sha256Digest.hash(CanonicalBytes.copyOf(value.storedBytes())))
                            .equals(predecessorBody)) {
                throw new IllegalStateException("actual Native metadata predecessor changed before dispatch");
            }
        }

        private void start() {
            lifetime.ready()
                    .thenCompose(unused ->
                            budget.requireNativeOwner(binding, creation.spec().allocatorOwner()))
                    .thenCompose(unused -> binding.revalidate())
                    .thenCompose(unused -> capturePredecessor(this))
                    .thenCompose(unused -> {
                        enteredGuard = true;
                        return guard.execute(request, this::dispatchOriginal);
                    })
                    .whenComplete((guarded, failure) -> {
                        settled.complete(null);
                        if (failure != null && !enteredGuard) {
                            release()
                                    .whenComplete((unused, cleanupFailure) -> result.completeExceptionally(
                                            cleanupFailure == null ? failure : cleanupFailure));
                            return;
                        }
                        if (failure != null
                                || guarded.outcome() == GuardOutcomeV1.RETAINED_TICKET_STATE_UNKNOWN
                                || guarded.outcome() == GuardOutcomeV1.RETAINED_AMBIGUOUS_EXTERNAL_MUTATION) {
                            result.completeExceptionally(new IllegalStateException(
                                    "original Native metadata ticket is unresolved: path=" + path + " dispatched="
                                            + dispatched + " outcome="
                                            + (guarded == null ? "metadata failure" : guarded.outcome()),
                                    failure == null ? sdkFailure : failure));
                            return;
                        }
                        release().whenComplete((unused, cleanupFailure) -> {
                            if (cleanupFailure != null) {
                                result.completeExceptionally(cleanupFailure);
                            } else if (sdkFailure != null) {
                                result.completeExceptionally(sdkFailure);
                            } else if (!dispatched) {
                                result.completeExceptionally(
                                        new IllegalStateException("Native metadata mutation is fenced"));
                            } else {
                                sdk.whenComplete((value, error) -> result.complete(value));
                            }
                        });
                    });
        }

        private CompletionStage<ExternalMutationResultV1> dispatchOriginal() {
            if (physicalTargets.isEmpty()) {
                return dispatchSdk();
            }
            CompletionStage<Void> facts = CompletableFuture.completedFuture(null);
            for (var target : physicalTargets) {
                var physicalRoute = routeFor(target);
                facts = facts.thenCompose(unused -> physicalRoute
                        .readResourceFact(target)
                        .thenCompose(value -> physicalRoute.members().readFact(target))
                        .thenAccept(value -> {
                            var expected = new Member(
                                    PositionDomain.PULSAR_ENTRY,
                                    target instanceof PhysicalResourceIdV2.ObjectVersion
                                            ? objectConnection
                                                    .profile()
                                                    .scopeId()
                                                    .digest()
                                            : creation.spec().providerScope(),
                                    binding.bindingIdentity());
                            if (!value.inventory()
                                            .protocolCell()
                                            .equals(Sha256Digest.hash(
                                                    creation.spec().protocolCell()))
                                    || !value.inventory().members().contains(expected)) {
                                throw new IllegalArgumentException(
                                        "Native physical member differs from its original SDK Binding/Cell");
                            }
                        }));
            }
            return facts.whenComplete((unused, failure) -> {
                        if (failure != null) {
                            sdkFailure = failure;
                        }
                    })
                    .thenCompose(
                            unused -> physicalGuard.executeWithContexts(physicalContexts, () -> binding.revalidate()
                                    .thenCompose(done -> dispatchSdk())
                                    .thenApply(value -> new M5TargetDeleteMultiWriterGuardV2.Completion<>(
                                            value,
                                            value.outcome() == ExternalMutationOutcomeV1.RESPONSE_UNKNOWN
                                                    ? Optional.empty()
                                                    : Optional.of(value.authoritativeKeySetRootSha256())))))
                    .thenApply(value -> {
                        physicalResult = value;
                        if (!value.unresolvedTargets().isEmpty()) {
                            return new ExternalMutationResultV1(
                                    ExternalMutationOutcomeV1.RESPONSE_UNKNOWN, request.externalAuthorityRootSha256());
                        }
                        return value.value()
                                .orElseGet(() -> new ExternalMutationResultV1(
                                        ExternalMutationOutcomeV1.DEFINITIVELY_NOT_APPLIED,
                                        request.externalAuthorityRootSha256()));
                    });
        }

        private CompletionStage<ExternalMutationResultV1> dispatchSdk() {
            return budget.requireNativeOwner(binding, creation.spec().allocatorOwner())
                    .thenCompose(unused -> binding.revalidate())
                    .thenCompose(unused -> {
                        synchronized (PulsarNativeMetadataWriterV2.this) {
                            return objectConnection == null
                                            || objectOffloader.preservesIncompleteAttempt()
                                                    && selection.source().isEmpty()
                                    ? CompletableFuture.<Void>completedFuture(null)
                                    : requireObjectPhysicalScope();
                        }
                    })
                    .thenCompose(unused -> revalidatePredecessor(this))
                    .thenCompose(unused -> materialization == null
                            ? CompletableFuture.completedFuture(null)
                            : sourceControl.requireSelectedMaterializationSnapshot(
                                    materialization, creation.controlExecutor()))
                    .handle((unused, failure) -> {
                        if (failure == null) {
                            return dispatchPreparedSdk();
                        }
                        sdkFailure = failure;
                        terminal = new ExternalMutationResultV1(
                                ExternalMutationOutcomeV1.DEFINITIVELY_NOT_APPLIED,
                                request.externalAuthorityRootSha256());
                        return CompletableFuture.completedFuture(terminal);
                    })
                    .thenCompose(value -> value);
        }

        private CompletionStage<ExternalMutationResultV1> dispatchPreparedSdk() {
            dispatched = true;
            try {
                sdk = Objects.requireNonNull(dispatch.get(), "original Native SDK future")
                        .exceptionallyCompose(this::observeOriginalEnrolledParentPut);
            } catch (Throwable failure) {
                sdk = CompletableFuture.failedFuture(failure);
            }
            return sdk.handle((value, failure) -> {
                        sdkFailure = failure;
                        if (failure != null) {
                            Throwable cause = failure;
                            while ((cause instanceof java.util.concurrent.CompletionException
                                            || cause instanceof java.util.concurrent.ExecutionException)
                                    && cause.getCause() != null) {
                                cause = cause.getCause();
                            }
                            // A reset may have written BK before its metadata fallback fails.
                            boolean definitelyNotApplied = reset == null
                                    ? cause
                                            instanceof
                                            org.apache.pulsar.metadata.api.MetadataStoreException.BadVersionException
                                    : cause instanceof MetaStore.CursorResetNotDispatchedException;
                            if (definitelyNotApplied) {
                                terminal = new ExternalMutationResultV1(
                                        ExternalMutationOutcomeV1.DEFINITIVELY_NOT_APPLIED,
                                        request.externalAuthorityRootSha256());
                                return CompletableFuture.completedFuture(terminal);
                            }
                            return CompletableFuture.completedFuture(new ExternalMutationResultV1(
                                    ExternalMutationOutcomeV1.RESPONSE_UNKNOWN, request.externalAuthorityRootSha256()));
                        }
                        return verifySdkCompletion(value).thenApply(unused -> {
                            terminal = new ExternalMutationResultV1(
                                    ExternalMutationOutcomeV1.APPLIED_EXACT, request.externalAuthorityRootSha256());
                            return terminal;
                        });
                    })
                    .thenCompose(value -> value);
        }

        @SuppressWarnings("unchecked") // The native metadata SPI returns Stat for a versioned put.
        private CompletableFuture<T> observeOriginalEnrolledParentPut(Throwable originalFailure) {
            if (bytes == null
                    || !path.equals("/managed-ledgers/" + ledgerName)
                    || objectOffloader == null
                    || !objectOffloader.preservesIncompleteAttempt()
                    || expectedVersion.isEmpty()
                    || predecessorStat.isEmpty()
                    || expectedVersion.orElseThrow()
                            != predecessorStat.orElseThrow().getVersion()) {
                return CompletableFuture.failedFuture(originalFailure);
            }
            // The caller holds the original Native parent mutation and the physical reference tickets.
            // Only the exact successor on the original backend can settle a lost callback.
            long before = expectedVersion.orElseThrow();
            return budget.requireNativeOwner(binding, creation.spec().allocatorOwner())
                    .thenCompose(unused -> binding.revalidate())
                    .thenCompose(unused -> nativeStore.readStoredMutationValue(ledgerName, path, MAX_BYTES))
                    .handle((observed, readFailure) -> {
                        if (readFailure != null && readFailure != originalFailure) {
                            originalFailure.addSuppressed(readFailure);
                        }
                        return readFailure == null
                                ? observed.filter(value -> value.stat().getVersion() > before
                                        && java.util.Arrays.equals(value.storedBytes(), bytes))
                                : Optional.<MetaStoreImpl.StoredMutationValue>empty();
                    })
                    .thenCompose(exact -> exact.isPresent()
                            ? CompletableFuture.completedFuture(
                                    (T) exact.orElseThrow().stat())
                            : CompletableFuture.failedFuture(originalFailure));
        }

        private CompletableFuture<Void> verifySdkCompletion(T value) {
            if (reset != null) {
                // The original Native reset callback follows its durable BK/meta-store acknowledgement.
                // An unchanged cursor znode cannot prove a failed or timed-out reset was not applied.
                return CompletableFuture.completedFuture(null);
            }
            return nativeStore.verifyStoredMutationResult(ledgerName, path, bytes, bytes == null ? null : (Stat) value);
        }

        private synchronized CompletableFuture<Void> reconcile() {
            if (finished) {
                return CompletableFuture.completedFuture(null);
            }
            if (cleanup == null || cleanup.isCompletedExceptionally()) {
                cleanup = settled.thenCompose(unused -> {
                    if (!enteredGuard) {
                        return release();
                    }
                    if (!dispatched) {
                        return reconcilePhysical()
                                .thenCompose(done -> guard.reconcileUndispatched(request))
                                .thenCompose(guarded -> guarded.outcome()
                                                == GuardOutcomeV1.UNDISPATCHED_TICKET_FENCED_AND_CLEARED
                                        ? release()
                                        : CompletableFuture.failedFuture(new IllegalStateException(
                                                "original undispatched Native ticket cleanup remains unknown")));
                    }
                    if (terminal != null && terminal.outcome() == ExternalMutationOutcomeV1.DEFINITIVELY_NOT_APPLIED) {
                        return reconcilePhysical()
                                .thenCompose(done -> guard.reconcileCompleted(request, terminal))
                                .thenCompose(guarded ->
                                        guarded.outcome() == GuardOutcomeV1.MUTATION_NOT_APPLIED_AND_TICKET_CLEARED
                                                ? release()
                                                : CompletableFuture.failedFuture(new IllegalStateException(
                                                        "original rejected Native ticket cleanup remains unknown")));
                    }
                    if (sdk == null || !sdk.isDone() || sdk.isCompletedExceptionally()) {
                        return CompletableFuture.failedFuture(new IllegalStateException(
                                "original Native SDK/ticket outcome remains unknown; redispatch is forbidden"));
                    }
                    return verifySdkCompletion(sdk.join())
                            .thenCompose(done -> {
                                terminal = new ExternalMutationResultV1(
                                        ExternalMutationOutcomeV1.APPLIED_EXACT, request.externalAuthorityRootSha256());
                                return reconcilePhysical()
                                        .thenCompose(cleaned -> guard.reconcileCompleted(request, terminal));
                            })
                            .thenCompose(
                                    guarded -> guarded.outcome() == GuardOutcomeV1.MUTATION_APPLIED_AND_TICKET_CLEARED
                                            ? release()
                                            : CompletableFuture.failedFuture(new IllegalStateException(
                                                    "original Native metadata ticket cleanup remains unknown")));
                });
            }
            return cleanup.copy();
        }

        private CompletionStage<Void> reconcilePhysical() {
            if (physicalResult == null || physicalResult.unresolvedTargets().isEmpty()) {
                return CompletableFuture.completedFuture(null);
            }
            CompletionStage<List<PhysicalResourceIdV2>> original;
            if (physicalResult.terminalCleanup().isPresent()) {
                original = physicalResult.terminalCleanup().orElseThrow().reconcile();
            } else if (terminal != null) {
                original = physicalGuard.reconcileOperationWithContexts(
                        physicalContexts, physicalResult.operationId(), terminal.authoritativeKeySetRootSha256());
            } else if (!dispatched) {
                original = physicalGuard.reconcileOperationWithContexts(
                        physicalContexts, physicalResult.operationId(), request.externalAuthorityRootSha256());
            } else {
                return CompletableFuture.failedFuture(
                        new IllegalStateException("original Native reference SDK outcome remains unknown"));
            }
            return original.thenApply(left -> {
                if (!left.isEmpty()) {
                    throw new IllegalStateException("original Native reference ticket cleanup remains unknown");
                }
                return null;
            });
        }

        private synchronized CompletableFuture<Void> release() {
            if (released != null && !released.isCompletedExceptionally()) {
                return released.copy();
            }
            released = CompletableFuture.allOf(List.copyOf(sdkCaptures).stream()
                            .map(PulsarNativeOffloadedReferenceV2::close)
                            .toArray(CompletableFuture[]::new))
                    .thenCompose(unused -> mappedWorkspace == null
                            ? CompletableFuture.completedFuture(null)
                            : mappedWorkspace.releaseAfterDrain())
                    .thenRun(this::releaseAfterSdkDrain);
            return released.copy();
        }

        private void releaseAfterSdkDrain() {
            synchronized (PulsarNativeMetadataWriterV2.this) {
                if (pending.remove(this)) {
                    pendingBytes -= charge;
                }
            }
            selection = null;
            sdkCaptures.clear();
            objectFacts.clear();
            materialization = null;
            selectedMap = null;
            finished = true;
        }
    }

    private OxiaQuotaTargetDeleteStoreV2 routeFor(PhysicalResourceIdV2 resource) {
        if (resource instanceof PhysicalResourceIdV2.BookKeeperLedger
                && resource.namespace().equals(creation.namespace().namespace())) {
            return creation.route();
        }
        if (resource instanceof PhysicalResourceIdV2.ObjectVersion
                && objectConnection != null
                && objectRoute != null
                && resource.namespace().equals(objectConnection.binding().physicalNamespace())) {
            return objectRoute;
        }
        throw new IllegalArgumentException("Native physical target has no original SDK namespace route");
    }

    /** Exact bounded keys for this original invocation; it does not synthesize a multi-namespace transaction. */
    static record PhysicalRoutes(Map<String, OxiaQuotaTargetDeleteStoreV2> routes)
            implements ExactMetadataTransactionStoreV1 {
        private OxiaQuotaTargetDeleteStoreV2 route(String key) {
            var route = routes.get(key);
            if (route == null) {
                throw new IllegalArgumentException("Native physical key is outside the original target set");
            }
            return route;
        }

        @Override
        public CompletionStage<Optional<VersionedValue>> read(String key) {
            return route(key).read(key);
        }

        @Override
        public CompletionStage<MutationOutcome> compareAndSet(
                Optional<VersionedValue> predecessor, String key, CanonicalBytes candidate) {
            return route(key).compareAndSet(predecessor, key, candidate);
        }

        @Override
        public CompletionStage<TransactionOutcome> conditionalTransaction(ExactTransaction transaction) {
            return CompletableFuture.completedFuture(TransactionOutcome.UNSUPPORTED);
        }

        @Override
        public boolean supportsAtomicMultiKeyTransactions() {
            return false;
        }
    }
}
