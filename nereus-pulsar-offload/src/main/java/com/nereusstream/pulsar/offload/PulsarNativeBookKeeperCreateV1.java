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
import com.nereusstream.domain.codec.ProtocolCellIdentityCodecV1;
import com.nereusstream.metadata.oxia.v2.retention.Oxia09ExactMetadataTransactionStoreV1;
import com.nereusstream.metadata.oxia.v2.retention.OxiaPhysicalMemberInventoryV2.Member;
import com.nereusstream.metadata.oxia.v2.retention.OxiaPhysicalMetadataNamespaceV2;
import com.nereusstream.metadata.oxia.v2.retention.OxiaQuotaTargetDeleteStoreV2;
import com.nereusstream.storage.api.lifecycle.PhysicalResourceIdV2;
import com.nereusstream.storage.bookkeeper.M5BookKeeperNamespaceAuthorityV2;
import com.nereusstream.storage.bookkeeper.M5PulsarNativeCreateSpecV1;
import com.nereusstream.storage.bookkeeper.M5PulsarNativeMetadataDriverV1;
import com.nereusstream.storage.object.gc.M5TargetDeleteAuthorityCoordinatorV1;
import com.nereusstream.storage.object.gc.M5TargetDeleteAuthorityRecordsV1.PhysicalDeleteTargetV1;
import com.nereusstream.storage.object.gc.M5TargetDeleteAuthorityRecordsV1.ProofBoundWriterClassV1;
import com.nereusstream.storage.object.gc.M5TargetDeleteAuthorityRecordsV1.ProofBoundWriterEnrollmentV1;
import com.nereusstream.storage.object.gc.M5TargetDeleteAuthorityStateMachineV1;
import com.nereusstream.storage.object.gc.M5TargetDeleteMultiWriterGuardV2;
import com.nereusstream.storage.object.materialization.M5MaterializationRecordsV1.PositionDomain;
import io.oxia.client.api.AsyncOxiaClient;
import java.nio.ByteBuffer;
import java.util.HashSet;
import java.util.List;
import java.util.Objects;
import java.util.Optional;
import java.util.Set;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.Executor;
import java.util.concurrent.atomic.AtomicReference;
import org.apache.bookkeeper.client.api.LedgerMetadata;

/** Actual P1/Cell/namespace-bound Native BK births and members. Supplies no deletion or all-writer qualification. */
public final class PulsarNativeBookKeeperCreateV1 implements AutoCloseable {
    /** Retains this failed SDK opening's original cleanup; retry never starts another SDK connection. */
    public static final class OpenCleanupException extends IllegalStateException {
        private final java.util.function.Supplier<CompletableFuture<Void>> cleanup;
        private CompletableFuture<Void> attempt;

        private OpenCleanupException(
                Throwable workFailure,
                Throwable cleanupFailure,
                java.util.function.Supplier<CompletableFuture<Void>> cleanup,
                PulsarNativeReadCellBudgetV1.Reservation lifetime) {
            super("Original Native SDK opening cleanup remains unresolved", workFailure);
            addSuppressed(cleanupFailure);
            this.cleanup = cleanup;
            lifetime.retainUndeliveredSdk(this::reconcileCleanup, false);
        }

        public synchronized CompletableFuture<Void> reconcileCleanup() {
            if (attempt == null || attempt.isCompletedExceptionally()) {
                attempt = cleanup.get();
            }
            return attempt.copy();
        }
    }

    private final PulsarNativeBindingV1 binding;
    private final PulsarNativeReadCellBudgetV1 budget;
    private final M5PulsarNativeCreateSpecV1 spec;
    private final OxiaQuotaTargetDeleteStoreV2 route;
    private final M5BookKeeperNamespaceAuthorityV2 namespace;
    private final Sha256Digest implementation;
    private final PulsarNativeReadCellBudgetV1.Reservation lifetime;
    private final Executor controlExecutor;
    private final Object metadataLock = new Object();
    private final Set<MetadataMutation> metadataMutations = new HashSet<>();
    private boolean sdkClosed;
    private boolean sdkDataDrainAttempted;
    private final java.util.Map<Long, SdkMetadataHandle> sdkMetadataHandles = new java.util.HashMap<>();
    private PulsarNativeSealedPlainAdmissionV2 sourceAdmission;

    private PulsarNativeBookKeeperCreateV1(
            PulsarNativeBindingV1 binding,
            PulsarNativeReadCellBudgetV1 budget,
            M5PulsarNativeCreateSpecV1 spec,
            OxiaQuotaTargetDeleteStoreV2 route,
            M5BookKeeperNamespaceAuthorityV2 namespace,
            PulsarNativeReadCellBudgetV1.Reservation lifetime,
            Executor controlExecutor) {
        this.binding = binding;
        this.budget = budget;
        this.spec = spec;
        this.route = route;
        this.namespace = namespace;
        this.lifetime = lifetime;
        this.controlExecutor = controlExecutor;
        lifetime.actorStarting();
        try (var bytes =
                M5PulsarNativeMetadataDriverV1.class.getResourceAsStream("M5PulsarNativeMetadataDriverV1.class")) {
            implementation = Sha256Digest.hash(
                    CanonicalBytes.copyOf(Objects.requireNonNull(bytes).readAllBytes()));
        } catch (java.io.IOException failure) {
            throw new IllegalStateException("Native Pulsar driver bytecode is unavailable", failure);
        }
    }

    static CompletableFuture<PulsarNativeBookKeeperCreateV1> open(
            AsyncOxiaClient client,
            PulsarNativeBindingV1 binding,
            String metadataServiceUri,
            M5BookKeeperNamespaceAuthorityV2 bootstrap,
            PulsarNativeReadCellBudgetV1 budget,
            long capacityBytes,
            Executor controlExecutor) {
        return open(
                client,
                binding,
                metadataServiceUri,
                bootstrap,
                budget,
                capacityBytes,
                controlExecutor,
                Optional.empty());
    }

    static CompletableFuture<PulsarNativeBookKeeperCreateV1> open(
            AsyncOxiaClient client,
            PulsarNativeBindingV1 binding,
            String metadataServiceUri,
            M5BookKeeperNamespaceAuthorityV2 bootstrap,
            PulsarNativeReadCellBudgetV1 budget,
            long capacityBytes,
            Executor controlExecutor,
            Optional<java.util.UUID> factoryInstance) {
        return open(
                client,
                binding,
                metadataServiceUri,
                bootstrap,
                budget,
                capacityBytes,
                controlExecutor,
                factoryInstance,
                Optional.empty());
    }

    static CompletableFuture<PulsarNativeBookKeeperCreateV1> open(
            AsyncOxiaClient client,
            PulsarNativeBindingV1 binding,
            String metadataServiceUri,
            M5BookKeeperNamespaceAuthorityV2 bootstrap,
            PulsarNativeReadCellBudgetV1 budget,
            long capacityBytes,
            Executor controlExecutor,
            Optional<java.util.UUID> factoryInstance,
            Optional<M5PulsarNativeCreateSpecV1.BookKeeperGcGraceV1> gcGrace) {
        if (gcGrace.isPresent() && factoryInstance.isEmpty()) {
            return CompletableFuture.failedFuture(
                    new IllegalArgumentException("Native BK GC policy requires explicit factory admission"));
        }
        Objects.requireNonNull(controlExecutor, "control executor");
        var observer = new CompletableFuture<PulsarNativeBookKeeperCreateV1>();
        var opening = binding.revalidate()
                .thenCompose(ignored -> bootstrap.readBinding())
                .thenCompose(found -> gcGrace.isEmpty()
                        ? CompletableFuture.completedFuture(found)
                        : bootstrap
                                .confirmedDrainProviderMinimumMillis(found.orElseThrow())
                                .thenApply(minimum -> {
                                    if (minimum > gcGrace.orElseThrow().providerMinimumMillis()) {
                                        throw new IllegalArgumentException(
                                                "Native BK GC provider minimum is understated");
                                    }
                                    return found;
                                }))
                .thenCompose(found -> {
                    var assignment =
                            found.orElseThrow(() -> new IllegalStateException("Native BK namespace is unbound"));
                    if (!assignment.metadataNamespace().equals(binding.metadataNamespace())) {
                        throw new IllegalStateException("Native BK namespace belongs to another metadata authority");
                    }
                    var cell = ProtocolCellIdentityCodecV1.encode(
                            binding.authority().aggregate().binding().cellIdentity());
                    var identity = binding.bindingIdentity();
                    var bytes = CanonicalBytes.copyOf(ByteBuffer.allocate(96)
                            .put(identity.bindingId().digest().bytes().toByteArray())
                            .put(identity.incarnationSha256().bytes().toByteArray())
                            .put(identity.storageEpochSha256().bytes().toByteArray())
                            .array());
                    var capability = bootstrap.capability();
                    String physical = bootstrap.namespace().serviceIdentity().value();
                    var legacySpec = new M5PulsarNativeCreateSpecV1(
                            physical.substring("bookkeeper-instance:".length()),
                            assignment,
                            cell,
                            budget.nativeOwnerIdentity(binding),
                            capability.providerScopeId().digest(),
                            capability.configurationDigest(),
                            capability.ensembleSize(),
                            capability.writeQuorumSize(),
                            capability.ackQuorumSize(),
                            List.of(new M5PulsarNativeCreateSpecV1.Binding(binding.managedLedgerName(), bytes)));
                    var spec = factoryInstance
                            .map(instance -> gcGrace.map(grace -> legacySpec.forFactory(instance, grace))
                                    .orElseGet(() -> legacySpec.forFactory(instance)))
                            .orElse(legacySpec);
                    return OxiaPhysicalMetadataNamespaceV2.connect(client, binding.metadataNamespace())
                            .thenCompose(metadata -> metadata.openAuthorityRoute(
                                    bootstrap, new Oxia09ExactMetadataTransactionStoreV1(client)))
                            .thenCompose(
                                    route -> route.initialize(capacityBytes).thenApply(ignored -> route))
                            .thenApplyAsync(
                                    route -> {
                                        var owner = new AtomicReference<PulsarNativeBookKeeperCreateV1>();
                                        M5BookKeeperNamespaceAuthorityV2 guarded = null;
                                        var lifetime = budget.reserveNativeClient(
                                                binding, spec.encode().length());
                                        boolean started = false;
                                        try {
                                            lifetime.ready().join();
                                            if (observer.isDone()) {
                                                throw new java.util.concurrent.CancellationException(
                                                        "Native SDK opening observer ended before SDK dispatch");
                                            }
                                            started = true;
                                            guarded = M5BookKeeperNamespaceAuthorityV2.connectPulsar(
                                                    metadataServiceUri,
                                                    capability,
                                                    spec,
                                                    (ledger, metadata, creating, mutation) -> Objects.requireNonNull(
                                                                    owner.get(), "Native birth owner")
                                                            .mutate(ledger, metadata, creating, mutation));
                                            var result = new PulsarNativeBookKeeperCreateV1(
                                                    binding, budget, spec, route, guarded, lifetime, controlExecutor);
                                            owner.set(result);
                                            return result;
                                        } catch (Exception failure) {
                                            if (guarded != null) {
                                                var original = guarded;
                                                try {
                                                    guarded.close();
                                                } catch (Exception closeFailure) {
                                                    var sdkClosed = new java.util.concurrent.atomic.AtomicBoolean();
                                                    throw new OpenCleanupException(
                                                            failure,
                                                            closeFailure,
                                                            () -> retrySdkCleanup(
                                                                    original, lifetime, sdkClosed, controlExecutor),
                                                            lifetime);
                                                }
                                                try {
                                                    lifetime.releaseAfterDrain().join();
                                                } catch (Throwable cleanupFailure) {
                                                    throw new OpenCleanupException(
                                                            failure,
                                                            cleanupFailure,
                                                            lifetime::releaseAfterDrain,
                                                            lifetime);
                                                }
                                            }
                                            if (!started) {
                                                try {
                                                    lifetime.releaseBeforeActor()
                                                            .join();
                                                } catch (Throwable cleanupFailure) {
                                                    throw new OpenCleanupException(
                                                            failure,
                                                            cleanupFailure,
                                                            lifetime::releaseBeforeActor,
                                                            lifetime);
                                                }
                                            }
                                            throw new java.util.concurrent.CompletionException(failure);
                                        }
                                    },
                                    controlExecutor);
                })
                .toCompletableFuture();
        opening.whenComplete((actual, failure) -> {
            if (failure != null) {
                observer.completeExceptionally(failure);
            } else if (!observer.complete(actual)) {
                actual.lifetime.retainUndeliveredSdk(
                        () -> CompletableFuture.runAsync(
                                () -> {
                                    try {
                                        actual.close();
                                    } catch (Exception unresolved) {
                                        throw new java.util.concurrent.CompletionException(unresolved);
                                    }
                                },
                                controlExecutor),
                        true);
            }
        });
        return observer;
    }

    private static CompletableFuture<Void> retrySdkCleanup(
            M5BookKeeperNamespaceAuthorityV2 original,
            PulsarNativeReadCellBudgetV1.Reservation lifetime,
            java.util.concurrent.atomic.AtomicBoolean sdkClosed,
            Executor controlExecutor) {
        return CompletableFuture.runAsync(
                        () -> {
                            if (!sdkClosed.get()) {
                                try {
                                    original.close();
                                    sdkClosed.set(true);
                                } catch (Exception unresolved) {
                                    throw new java.util.concurrent.CompletionException(unresolved);
                                }
                            }
                        },
                        controlExecutor)
                .thenCompose(ignored -> lifetime.releaseAfterDrain());
    }

    public M5BookKeeperNamespaceAuthorityV2 namespace() {
        return namespace;
    }

    public M5PulsarNativeCreateSpecV1 spec() {
        return spec;
    }

    public OxiaQuotaTargetDeleteStoreV2 route() {
        return route;
    }

    Executor controlExecutor() {
        return controlExecutor;
    }

    boolean ownsMetadataWriterScope(
            PulsarNativeBindingV1 originalBinding, PulsarNativeReadCellBudgetV1 originalBudget) {
        return binding == originalBinding && budget == originalBudget;
    }

    /** Explicit original-source admission; no Native ledger creation or retagging. */
    synchronized CompletableFuture<Void> admitSealedPlainSources(
            org.apache.bookkeeper.mledger.impl.MetaStoreImpl store,
            com.nereusstream.storage.object.retention.M5ReferenceMutationGuardV1 guard,
            com.nereusstream.storage.object.retention.M5ClosedWriterRegistryV1.RegisteredReferenceWriterV1 writer,
            NereusPulsarLedgerOffloaderV1 offloader,
            S3PulsarOffloadObjectStoreV1.MaterializationConnection objectConnection,
            OxiaQuotaTargetDeleteStoreV2 objectRoute) {
        if (sdkClosed) {
            return CompletableFuture.failedFuture(new IllegalStateException("Original Native SDK scope is closed"));
        }
        if (sourceAdmission == null) {
            sourceAdmission = new PulsarNativeSealedPlainAdmissionV2(
                    this,
                    binding,
                    budget,
                    store,
                    implementation,
                    guard,
                    writer,
                    offloader,
                    objectConnection,
                    objectRoute);
        }
        return sourceAdmission.observe(store);
    }

    synchronized CompletableFuture<Void> reconcileSourceAdmission() {
        return sourceAdmission == null ? CompletableFuture.completedFuture(null) : sourceAdmission.reconcile();
    }

    /** One bounded pass over original confirmed terminal/no-dispatch calls; never invokes native mutations. */
    public CompletableFuture<Void> reconcileMetadataCleanup() {
        final List<MetadataMutation> pending;
        synchronized (metadataLock) {
            pending = List.copyOf(metadataMutations);
        }
        return CompletableFuture.allOf(
                        pending.stream().map(MetadataMutation::reconcile).toArray(CompletableFuture[]::new))
                .copy();
    }

    private CompletableFuture<Void> holdSdkMetadataHandle(
            PhysicalResourceIdV2.BookKeeperLedger resource, M5TargetDeleteMultiWriterGuardV2 guard) {
        if (spec.factoryInstance().isEmpty()) {
            return CompletableFuture.completedFuture(null); // Existing partial v1 API has no factory lifetime grant.
        }
        final SdkMetadataHandle accepted;
        synchronized (metadataLock) {
            var original = sdkMetadataHandles.get(resource.ledgerId());
            if (original != null) {
                return original.ready.copy();
            }
            if (sdkMetadataHandles.size() >= M5PulsarNativeCreateSpecV1.MAX_SDK_METADATA_HANDLES) {
                return CompletableFuture.failedFuture(new IllegalStateException("SDK metadata handle bound exhausted"));
            }
            accepted = new SdkMetadataHandle(resource, guard);
            sdkMetadataHandles.put(resource.ledgerId(), accepted);
        }
        // SDK/guard callbacks stay outside the metadata ownership lock.
        accepted.start();
        return accepted.ready.copy();
    }

    private final class SdkMetadataHandle {
        private final PhysicalResourceIdV2.BookKeeperLedger resource;
        private final M5TargetDeleteMultiWriterGuardV2 guard;
        private final Sha256Digest root;
        private final M5TargetDeleteMultiWriterGuardV2.Context context;
        private final CompletableFuture<Void> ready = new CompletableFuture<>();
        private final CompletableFuture<Void> ended = new CompletableFuture<>();
        private final CompletableFuture<Void> settled = new CompletableFuture<>();
        private M5TargetDeleteMultiWriterGuardV2.Result<Boolean> result;

        private SdkMetadataHandle(
                PhysicalResourceIdV2.BookKeeperLedger resource, M5TargetDeleteMultiWriterGuardV2 guard) {
            this.resource = resource;
            this.guard = guard;
            root = Sha256Digest.hash(CanonicalBytes.copyOf(ByteBuffer.allocate(100)
                    .putInt(0x4e505348)
                    .put(spec.sha256().bytes().toByteArray())
                    .put(resource.sha256().bytes().toByteArray())
                    .put(Sha256Digest.hash(spec.allocatorOwner()).bytes().toByteArray())
                    .array()));
            context = new M5TargetDeleteMultiWriterGuardV2.Context(
                    ProofBoundWriterClassV1.OWNER_WORKER_LEASE_HANDLE_PIN_V1,
                    spec.configuration(),
                    Sha256Digest.hash(spec.allocatorOwner()),
                    root);
        }

        private void start() {
            guard.execute(List.of(resource), context, () -> {
                        ready.complete(null);
                        return ended.thenApply(
                                unused -> new M5TargetDeleteMultiWriterGuardV2.Completion<>(true, Optional.of(root)));
                    })
                    .whenComplete((value, failure) -> {
                        result = value;
                        if (!ready.isDone()) {
                            ready.completeExceptionally(
                                    failure != null
                                            ? failure
                                            : new IllegalStateException(
                                                    "original SDK metadata handle is READ_FENCED or unknown"));
                        }
                        settled.complete(null);
                    });
        }

        private CompletableFuture<Void> close() {
            requireSdkMetadataScopeClosed();
            ended.complete(null);
            return settled.thenCompose(unused -> {
                if (result == null || result.terminalCleanup().isEmpty()) {
                    return CompletableFuture.failedFuture(
                            new IllegalStateException("original SDK metadata handle outcome remains unknown"));
                }
                return result.terminalCleanup()
                        .orElseThrow()
                        .reconcile()
                        .thenAccept(left -> {
                            if (!left.isEmpty()) {
                                throw new IllegalStateException("original SDK metadata handle cleanup remains unknown");
                            }
                            synchronized (metadataLock) {
                                sdkMetadataHandles.remove(resource.ledgerId(), this);
                            }
                        })
                        .toCompletableFuture();
            });
        }
    }

    private CompletableFuture<Void> closeSdkMetadataHandles() {
        final List<SdkMetadataHandle> originals;
        synchronized (metadataLock) {
            originals = List.copyOf(sdkMetadataHandles.values());
        }
        return CompletableFuture.allOf(
                originals.stream().map(SdkMetadataHandle::close).toArray(CompletableFuture[]::new));
    }

    /** Stops this original SDK metadata/allocator scope; its Cell charge remains until all owner references settle. */
    synchronized void closeSdkMetadataScope() throws Exception {
        if (!sdkClosed) {
            var driver =
                    (M5PulsarNativeMetadataDriverV1) namespace.borrowClient().getMetadataClientDriver();
            driver.fenceCreates().get();
            if (spec.factoryInstance().isPresent()) {
                if (sdkDataDrainAttempted) {
                    namespace.reconcileSdkDataWrites().get();
                }
                sdkDataDrainAttempted = true;
                namespace.drainSdkDataWrites().get();
            }
            driver.drainMetadata().get();
            final List<MetadataMutation> pending;
            synchronized (metadataLock) {
                pending = List.copyOf(metadataMutations);
            }
            CompletableFuture.allOf(pending.stream()
                            .map(MetadataMutation::observeNativeTerminal)
                            .toArray(CompletableFuture[]::new))
                    .get();
            namespace.close();
            sdkClosed = true;
        }
        closeSdkMetadataHandles().get();
    }

    synchronized void requireSdkMetadataScopeClosed() {
        if (!sdkClosed) {
            throw new IllegalStateException("original Native SDK metadata scope is not closed");
        }
    }

    @Override
    public synchronized void close() throws Exception {
        if (sourceAdmission != null) {
            sourceAdmission.reconcile().get();
        }
        closeSdkMetadataScope();
        reconcileMetadataCleanup().get();
        lifetime.releaseAfterDrain().get();
    }

    private record NativeOutcome(org.apache.bookkeeper.versioning.Versioned<LedgerMetadata> value, Throwable failure) {}

    private final class MetadataMutation {
        private final PhysicalResourceIdV2.BookKeeperLedger resource;
        private final M5TargetDeleteMultiWriterGuardV2 guard;
        private final M5TargetDeleteMultiWriterGuardV2.Context context;
        private final CompletableFuture<Void> settled = new CompletableFuture<>();
        private M5TargetDeleteMultiWriterGuardV2.Result<NativeOutcome> result;
        private CompletableFuture<Void> cleanup;
        private volatile Sha256Digest nativeTerminal;

        private MetadataMutation(
                PhysicalResourceIdV2.BookKeeperLedger resource,
                M5TargetDeleteMultiWriterGuardV2 guard,
                M5TargetDeleteMultiWriterGuardV2.Context context) {
            this.resource = resource;
            this.guard = guard;
            this.context = context;
        }

        private CompletableFuture<Void> observeNativeTerminal() {
            return settled.thenCompose(ignored -> {
                if (result == null
                        || result.unresolvedTargets().isEmpty()
                        || result.terminalCleanup().isPresent()
                        || nativeTerminal != null) {
                    return CompletableFuture.completedFuture(null);
                }
                Throwable failure = result.value().map(NativeOutcome::failure).orElse(null);
                while (failure != null
                        && !(failure instanceof M5PulsarNativeMetadataDriverV1.NativeMetadataWriteUnknownException)) {
                    failure = failure.getCause();
                }
                if (!(failure instanceof M5PulsarNativeMetadataDriverV1.NativeMetadataWriteUnknownException original)
                        || original.ledgerId() != resource.ledgerId()
                        || !original.scopeSha256().equals(spec.sha256())
                        || !original.requestedMetadataSha256().equals(context.externalFactsRootSha256())) {
                    return CompletableFuture.failedFuture(
                            new IllegalStateException("Native BK metadata cleanup remains unresolved"));
                }
                return budget.requireNativeOwner(binding, spec.allocatorOwner())
                        .thenCompose(value -> binding.revalidate())
                        .thenCompose(value -> original.observeTerminal())
                        .thenAccept(proof -> nativeTerminal = proof);
            });
        }

        private void finish() {
            synchronized (metadataLock) {
                if (result == null || result.unresolvedTargets().isEmpty()) {
                    metadataMutations.remove(this);
                }
            }
            settled.complete(null);
        }

        private synchronized CompletableFuture<Void> reconcile() {
            if (cleanup == null || cleanup.isCompletedExceptionally()) {
                cleanup = settled.thenCompose(ignored -> {
                    if (result == null || result.unresolvedTargets().isEmpty()) {
                        return CompletableFuture.completedFuture(null);
                    }
                    if (result.terminalCleanup().isEmpty()) {
                        return observeNativeTerminal()
                                .thenCompose(value -> guard.reconcileOperation(
                                        List.of(resource), context, result.operationId(), nativeTerminal))
                                .thenAccept(this::finishCleanup);
                    }
                    return result.terminalCleanup().orElseThrow().reconcile().thenAccept(this::finishCleanup);
                });
            }
            return cleanup.copy();
        }

        private void finishCleanup(List<PhysicalResourceIdV2> left) {
            if (!left.isEmpty()) {
                throw new IllegalStateException("Native BK metadata cleanup remains unresolved");
            }
            synchronized (metadataLock) {
                metadataMutations.remove(this);
            }
        }
    }

    private MetadataMutation admitMetadataMutation(
            PhysicalResourceIdV2.BookKeeperLedger resource,
            M5TargetDeleteMultiWriterGuardV2 guard,
            M5TargetDeleteMultiWriterGuardV2.Context context)
            throws org.apache.bookkeeper.client.BKException {
        synchronized (metadataLock) {
            if (metadataMutations.size() == M5PulsarNativeCreateSpecV1.MAX_PENDING_METADATA_MUTATIONS) {
                throw new org.apache.bookkeeper.client.BKException.BKTooManyRequestsException();
            }
            var accepted = new MetadataMutation(resource, guard, context);
            metadataMutations.add(accepted);
            return accepted;
        }
    }

    private CompletableFuture<org.apache.bookkeeper.versioning.Versioned<LedgerMetadata>> mutate(
            long ledgerId,
            LedgerMetadata metadata,
            boolean creating,
            java.util.function.Supplier<CompletableFuture<org.apache.bookkeeper.versioning.Versioned<LedgerMetadata>>>
                    mutation) {
        try {
            if (spec.bindingFor(metadata, true).isEmpty()
                    || creating && !spec.allocatorOwner().equals(budget.nativeOwnerIdentity(binding))) {
                throw new IllegalStateException("Native BK mutation differs from its actual Binding/Cell owner");
            }
            var resource = new PhysicalResourceIdV2.BookKeeperLedger(namespace.namespace(), ledgerId);
            var coordinator = new M5TargetDeleteAuthorityCoordinatorV1(route);
            var enrollment = new ProofBoundWriterEnrollmentV1(
                    List.of(ProofBoundWriterClassV1.values()), spec.configuration(), implementation, spec.sha256());
            var initial = M5TargetDeleteAuthorityStateMachineV1.open(
                    PhysicalDeleteTargetV1.create(resource), enrollment, spec.sha256());
            var guard = new M5TargetDeleteMultiWriterGuardV2(coordinator);
            var facts = Sha256Digest.hash(
                    CanonicalBytes.copyOf(new org.apache.bookkeeper.meta.LedgerMetadataSerDe().serialize(metadata)));
            var context = new M5TargetDeleteMultiWriterGuardV2.Context(
                    creating
                            ? ProofBoundWriterClassV1.TASK_PROJECTION_MIGRATION_RECOVERY_V1
                            : ProofBoundWriterClassV1.OWNER_WORKER_LEASE_HANDLE_PIN_V1,
                    spec.configuration(),
                    Sha256Digest.hash(spec.allocatorOwner()),
                    facts);
            var pending = admitMetadataMutation(resource, guard, context);
            return CompletableFuture.completedFuture(null)
                    .thenCompose(ignored -> budget.requireNativeOwner(binding, spec.allocatorOwner()))
                    .thenCompose(ignored -> binding.revalidate())
                    .thenCompose(ignored -> namespace.requirePulsarReservation(ledgerId))
                    .thenCompose(ignored -> creating
                            ? coordinator.create(initial).thenCompose(result -> {
                                if (!result.exactCandidateIsAuthoritative()) {
                                    throw new IllegalStateException(
                                            "Native BK birth OPEN authority is unknown or conflicts");
                                }
                                return route.members()
                                        .register(
                                                resource,
                                                Sha256Digest.hash(spec.protocolCell()),
                                                new Member(
                                                        PositionDomain.PULSAR_ENTRY,
                                                        spec.providerScope(),
                                                        binding.bindingIdentity()),
                                                spec.configuration(),
                                                Sha256Digest.hash(spec.allocatorOwner()))
                                        .thenApply(value -> (Void) null);
                            })
                            : CompletableFuture.completedFuture(null))
                    .thenCompose(ignored -> route.readResourceFact(resource))
                    .thenCompose(ignored -> binding.revalidate())
                    .thenCompose(ignored -> holdSdkMetadataHandle(resource, guard))
                    .thenCompose(ignored -> guard.execute(
                            List.of(resource), context, () -> mutation.get().handle((value, failure) -> {
                                java.util.Optional<Sha256Digest> terminal = java.util.Optional.empty();
                                if (failure == null) {
                                    try {
                                        var version = (org.apache.bookkeeper.versioning.LongVersion) value.getVersion();
                                        var actual = new org.apache.bookkeeper.meta.LedgerMetadataSerDe()
                                                .serialize(value.getValue());
                                        terminal = java.util.Optional.of(Sha256Digest.hash(
                                                CanonicalBytes.copyOf(ByteBuffer.allocate(104 + actual.length)
                                                        .put(resource.sha256()
                                                                .bytes()
                                                                .toByteArray())
                                                        .put(spec.sha256()
                                                                .bytes()
                                                                .toByteArray())
                                                        .put(facts.bytes().toByteArray())
                                                        .putLong(version.getLongVersion())
                                                        .put(actual)
                                                        .array())));
                                    } catch (Exception invalid) {
                                        failure = invalid;
                                    }
                                }
                                return new M5TargetDeleteMultiWriterGuardV2.Completion<>(
                                        new NativeOutcome(value, failure), terminal);
                            })))
                    .thenApply(result -> {
                        pending.result = result;
                        if (!result.mutationInvoked()) {
                            throw new IllegalStateException("Native BK metadata mutation is READ_FENCED");
                        }
                        var outcome = result.value().orElseThrow();
                        if (outcome.failure() != null) {
                            throw new java.util.concurrent.CompletionException(outcome.failure());
                        }
                        if (!result.unresolvedTargets().isEmpty()) {
                            throw new IllegalStateException("Native BK metadata mutation cleanup is unresolved");
                        }
                        return outcome.value();
                    })
                    .whenComplete((value, failure) -> pending.finish())
                    .toCompletableFuture()
                    .copy();
        } catch (Throwable failure) {
            return CompletableFuture.failedFuture(failure);
        }
    }
}
