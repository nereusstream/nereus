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
import com.nereusstream.domain.bytes.CanonicalUtf8;
import com.nereusstream.domain.bytes.Sha256Digest;
import com.nereusstream.metadata.oxia.v2.retention.OxiaQuotaTargetDeleteStoreV2;
import com.nereusstream.metadata.spi.retention.ExactMetadataTransactionStoreV1.VersionedValue;
import com.nereusstream.storage.api.lifecycle.PhysicalNamespaceAuthorityBindingV2;
import com.nereusstream.storage.api.lifecycle.PhysicalResourceIdV2;
import com.nereusstream.storage.object.gc.DeleteEligibilitySnapshotV2;
import com.nereusstream.storage.object.gc.DeleteObservationAuthorityVerifierV2;
import com.nereusstream.storage.object.gc.DeleteObservationContextV2;
import com.nereusstream.storage.object.gc.M5GcQuotaCoordinatorV2;
import com.nereusstream.storage.object.gc.M5TargetDeleteAuthorityCodecV1;
import com.nereusstream.storage.object.gc.M5TargetDeleteAuthorityCoordinatorV1;
import com.nereusstream.storage.object.gc.M5TargetDeleteAuthorityRecordsV1.ExactExternalIdentityV1;
import com.nereusstream.storage.object.gc.M5TargetDeleteAuthorityRecordsV1.TargetDeleteAuthorityStateV1;
import com.nereusstream.storage.object.materialization.M5CellProviderProfileV2;
import com.nereusstream.storage.object.materialization.M5MaterializationCodecV1;
import com.nereusstream.storage.object.materialization.M5MaterializationKeysV1;
import com.nereusstream.storage.object.materialization.M5MaterializationRecordsV1;
import com.nereusstream.storage.object.materialization.M5PulsarCoverageV2;
import com.nereusstream.storage.object.provider.ObjectIdentity;
import com.nereusstream.storage.object.read.control.M4ReadControlCodecV1;
import com.nereusstream.storage.object.read.control.M4ReadControlRecordsV1.ProtectionState;
import com.nereusstream.storage.object.retention.M5ClosedWriterRegistryV1;
import com.nereusstream.storage.object.retention.M5PulsarAggregateAuthorityCodecV1;
import com.nereusstream.storage.object.retention.M5RetentionCodecV1;
import com.nereusstream.storage.object.retention.M5RetentionEvidenceAssemblerV1;
import com.nereusstream.storage.object.retention.M5RetentionRecordsV1;
import com.nereusstream.storage.object.retention.M5RetentionRecordsV1.AuthorityFactV1;
import com.nereusstream.storage.object.retention.M5RetentionRecordsV1.ReferenceDispositionV1;
import com.nereusstream.storage.object.retention.M5RetentionRecordsV1.ReferenceKindV1;
import com.nereusstream.storage.object.retention.M5RetentionRecordsV1.ReferenceObservationV1;
import com.nereusstream.storage.object.s3.S3M5ObjectDeleteIdentityReaderV2;
import java.util.ArrayList;
import java.util.Comparator;
import java.util.HashSet;
import java.util.List;
import java.util.Optional;
import java.util.Set;
import java.util.UUID;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.CompletionStage;
import java.util.concurrent.Executor;
import java.util.function.Supplier;

/**
 * One original Native GC observation under the existing Cell allocator and cursor observation lifetime.
 * This scope proves native ownership and configured elapsed grace, not complete deletion eligibility.
 */
public final class PulsarNativeGcScopeV1 implements AutoCloseable {
    private final PulsarNativeBindingV1 binding;
    private final PhysicalResourceIdV2 resource;
    private final com.nereusstream.storage.bookkeeper.M5BookKeeperNamespaceAuthorityV2 bookKeeper;
    private final PulsarNativeReadCellBudgetV1 budget;
    private final PulsarNativeCursorFloorScanV3 cursors;
    private final VersionedValue scan;
    private final Executor executor;
    private final PulsarNativeCursorFloorScanV3.ObservationGroup operations;
    private int activeCalls;
    private int activeIo;
    private final Set<CompletableFuture<Void>> accepted = new HashSet<>();
    private S3PulsarOffloadObjectStoreV1.MaterializationConnection connection;
    private OxiaQuotaTargetDeleteStoreV2 route;
    private M5CellProviderProfileV2 profile;
    private PulsarNativeReadCellAuthorityV1.OwnerEpoch owner;
    private DeleteObservationContextV2 context;
    private VersionedValue ownerFact;
    private M5TargetDeleteAuthorityCoordinatorV1 coordinator;
    private M5TargetDeleteAuthorityCoordinatorV1.EpochGraceSession grace;
    private CompletableFuture<Void> originalMark;
    private CompletableFuture<VersionedValue> originalDelete;
    private DeleteAttempt deletion;
    private ReleasedOffloadTarget releasedOffload;
    private BookKeeperDeleteAttempt bookKeeperDeletion;
    private PulsarNativeBookKeeperDeleteV1 nativeDelete;
    private com.nereusstream.storage.bookkeeper.M5PulsarNativeCreateSpecV1 bookKeeperScope;
    private boolean closed;
    private CompletableFuture<Void> closing;

    private PulsarNativeGcScopeV1(
            PulsarNativeBindingV1 binding,
            PhysicalResourceIdV2.ObjectVersion resource,
            PulsarNativeReadCellBudgetV1 budget,
            PulsarNativeCursorFloorScanV3 cursors,
            VersionedValue scan,
            Executor executor) {
        this(binding, resource, budget, cursors, scan, executor, null);
    }

    private PulsarNativeGcScopeV1(
            PulsarNativeBindingV1 binding,
            PhysicalResourceIdV2 resource,
            PulsarNativeReadCellBudgetV1 budget,
            PulsarNativeCursorFloorScanV3 cursors,
            VersionedValue scan,
            Executor executor,
            com.nereusstream.storage.bookkeeper.M5BookKeeperNamespaceAuthorityV2 bookKeeper) {
        this.executor = executor;
        this.binding = binding;
        this.resource = resource;
        this.bookKeeper = bookKeeper;
        this.budget = budget;
        this.cursors = cursors;
        this.scan = scan;
        operations = cursors.observationGroup();
    }

    /** Borrows the actual SDK on the existing executor; GC can only reopen an already admitted profile. */
    public static CompletableFuture<PulsarNativeGcScopeV1> open(
            PulsarNativeBindingV1 binding,
            PhysicalResourceIdV2.ObjectVersion resource,
            PulsarNativeReadCellBudgetV1 budget,
            PulsarNativeCursorFloorScanV3 cursors,
            VersionedValue scan,
            NereusPulsarLedgerOffloaderV1 offloader,
            long maximumObjectBytes,
            Executor executor) {
        var scope = new PulsarNativeGcScopeV1(binding, resource, budget, cursors, scan, executor);
        var observer = new CompletableFuture<PulsarNativeGcScopeV1>();
        try {
            cursors.requireObservationScope(scan);
            cursors.retainObservation(() -> CompletableFuture.runAsync(
                            () -> {
                                cursors.requireObservationScope(scan);
                                scope.connection = offloader.borrowConfiguredMaterializationConnection(
                                        binding, resource.namespace(), maximumObjectBytes);
                                scope.profile = scope.connection.profile();
                                scope.profile
                                        .gcGracePolicy()
                                        .orElseThrow(() -> new IllegalStateException(
                                                "Native GC requires its explicit first-admitted grace policy"));
                            },
                            executor)
                    .thenCompose(ignored -> scope.initialize())
                    .handle((ignored, failure) -> {
                        if (failure != null) {
                            observer.completeExceptionally(failure);
                        }
                        if (failure != null || !observer.complete(scope)) {
                            return scope.closeAsync();
                        }
                        return CompletableFuture.<Void>completedFuture(null);
                    })
                    .thenCompose(value -> value));
        } catch (Throwable failure) {
            observer.completeExceptionally(failure);
        }
        return observer;
    }

    /** Explicit confirmed-drain Native scope; old/no-policy scopes cannot enter the new BK GC channel. */
    public static CompletableFuture<PulsarNativeGcScopeV1> openBookKeeper(
            PulsarNativeBindingV1 binding,
            PhysicalResourceIdV2.BookKeeperLedger resource,
            PulsarNativeReadCellBudgetV1 budget,
            PulsarNativeCursorFloorScanV3 cursors,
            VersionedValue scan,
            com.nereusstream.storage.bookkeeper.M5BookKeeperNamespaceAuthorityV2 bookKeeper,
            Executor executor) {
        return openBookKeeper(binding, resource, budget, cursors, scan, bookKeeper, executor, Optional.empty());
    }

    /** Package test seam replaces only native transport, preserving the exact admitted create scope. */
    static CompletableFuture<PulsarNativeGcScopeV1> openBookKeeper(
            PulsarNativeBindingV1 binding,
            PhysicalResourceIdV2.BookKeeperLedger resource,
            PulsarNativeReadCellBudgetV1 budget,
            PulsarNativeCursorFloorScanV3 cursors,
            VersionedValue scan,
            com.nereusstream.storage.bookkeeper.M5BookKeeperNamespaceAuthorityV2 bookKeeper,
            Executor executor,
            Optional<com.nereusstream.storage.bookkeeper.M5BookKeeperNativeDeleteAuthorityV2> transport) {
        var scope = new PulsarNativeGcScopeV1(binding, resource, budget, cursors, scan, executor, bookKeeper);
        var observer = new CompletableFuture<PulsarNativeGcScopeV1>();
        try {
            cursors.requireObservationScope(scan);
            if (!(bookKeeper.borrowClient().getMetadataClientDriver()
                    instanceof com.nereusstream.storage.bookkeeper.M5PulsarNativeMetadataDriverV1 driver)) {
                throw new IllegalArgumentException("Native BK GC requires the original tagged scope connection");
            }
            scope.bookKeeperScope = driver.createSpec();
            scope.bookKeeperScope
                    .bookKeeperGcGrace()
                    .orElseThrow(() -> new IllegalStateException("Native BK scope has no explicit GC grace policy"));
            var actual = transport.orElseGet(() -> bookKeeper.pulsarDeleteAuthority(resource));
            if (!actual.resource().equals(resource)
                    || !actual.pulsarCreateScopeSha256().equals(Optional.of(scope.bookKeeperScope.sha256()))) {
                throw new IllegalArgumentException("Native BK GC transport belongs to another original create scope");
            }
            scope.nativeDelete = new PulsarNativeBookKeeperDeleteV1(actual, scope.new NativeMetadata());
            cursors.retainObservation(() -> scope.initializeBookKeeper()
                    .handle((ignored, failure) -> {
                        if (failure != null) {
                            observer.completeExceptionally(failure);
                        }
                        if (failure != null || !observer.complete(scope)) {
                            return scope.closeAsync();
                        }
                        return CompletableFuture.<Void>completedFuture(null);
                    })
                    .thenCompose(value -> value));
        } catch (Throwable failure) {
            observer.completeExceptionally(failure);
        }
        return observer;
    }

    private CompletionStage<Void> initializeBookKeeper() {
        return binding.materializationQuota(bookKeeper)
                .thenCompose(actual -> {
                    route = actual;
                    return budget.captureOwnerEpoch(binding);
                })
                .thenCompose(actualOwner -> {
                    owner = actualOwner;
                    ownerFact = nativeOwnerFact(owner);
                    return bookKeeper
                            .nativeDeleteCellBudget()
                            .snapshot()
                            .thenCompose(ignored -> nativeDelete.claim(java.util.UUID.randomUUID()));
                })
                .thenCompose(actual -> {
                    context = actual;
                    coordinator =
                            new M5TargetDeleteAuthorityCoordinatorV1(new NativeMetadata(), new NativeOwnerVerifier());
                    return requireCurrent();
                });
    }

    private PhysicalResourceIdV2.ObjectVersion objectResource() {
        if (!(resource instanceof PhysicalResourceIdV2.ObjectVersion object)) {
            throw new IllegalStateException("Native BK resource cannot use the Object deletion channel");
        }
        return object;
    }

    private CompletionStage<Void> initialize() {
        return connection
                .reopenProfile(binding)
                .thenCompose(ignored -> binding.materializationQuota(connection.physical()))
                .thenCompose(actual -> {
                    route = actual;
                    return budget.captureOwnerEpoch(binding);
                })
                .thenCompose(actualOwner -> {
                    owner = actualOwner;
                    return connection.captureProfileAuthority();
                })
                .thenCompose(capability -> route.readResourceFact(resource).thenCompose(target -> {
                    var authority = M5TargetDeleteAuthorityCodecV1.decodeAuthority(
                            target.authority().canonicalStoredBytes());
                    ownerFact = nativeOwnerFact(owner);
                    var prior = authority
                            .readFence()
                            .map(fence -> authority.deleteIntent().isPresent()
                                    ? com.nereusstream.storage.object.gc.M5TargetDeleteAuthorityStateMachineV1
                                            .dispatchContext(authority)
                                    : fence.observationContext());
                    var histories = java.util.stream.Stream.of(
                                    prior, authority.epochGrace().map(mark -> mark.authority()))
                            .flatMap(Optional::stream)
                            .toList();
                    histories.forEach(value -> requireNativeOwnerFact(value.coordinatorOwner(), false));
                    var currentOwner = fact(ownerFact);
                    var predecessor = histories.stream()
                                    .anyMatch(value -> !value.coordinatorOwner().equals(currentOwner))
                            ? Optional.of(currentOwner)
                            : Optional.<AuthorityFactV1>empty();
                    long epoch = histories.stream()
                            .mapToLong(DeleteObservationContextV2::observationEpoch)
                            .max()
                            .orElse(0);
                    context = new DeleteObservationContextV2(
                            Math.addExact(epoch, 1), currentOwner, capability, predecessor);
                    coordinator =
                            new M5TargetDeleteAuthorityCoordinatorV1(new NativeMetadata(), new NativeOwnerVerifier());
                    return requireCurrent();
                }));
    }

    private String nativeOwnerKey() {
        return "v2/native-pulsar-gc-owner/" + resource.sha256().toHex() + "/fact-v1";
    }

    private String trimProjectionKey() {
        return scan.key() + "/logical-trim-frontier-projection-v1";
    }

    /** Read-only canonical projection of the frontier at the exact actual P1 version, never a stored authority. */
    private VersionedValue trimProjection() {
        var frontier = M5PulsarAggregateAuthorityCodecV1.decodeAuthority(scan.canonicalStoredBytes())
                .logicalTrim()
                .flatMap(value -> value.frontier())
                .orElseThrow(() -> new IllegalStateException("Native expiry has no committed frontier"));
        return VersionedValue.of(
                trimProjectionKey(), M5RetentionCodecV1.encodeTrimFrontier(frontier), scan.metadataVersion());
    }

    private static AuthorityFactV1 fact(VersionedValue value) {
        return new AuthorityFactV1(value.key(), value.metadataVersion(), value.canonicalStoredSha256());
    }

    private static AuthorityFactV1 materializationFact(
            VersionedValue value, String nativeKey, M5RetentionRecordsV1.RetentionFloorSnapshotV1 floors) {
        var actual = floors.rows().stream()
                .map(row -> row.authority())
                .filter(authority -> authority.key().equals(nativeKey))
                .distinct()
                .toList();
        if (actual.size() != 1 || !actual.get(0).valueSha256().equals(value.canonicalStoredSha256())) {
            throw new IllegalStateException("Native materialization lacks its exact raw floor observation");
        }
        return actual.get(0);
    }

    /** Native semantic version is the actual persisted Cell owner identity, not an invented Oxia revision. */
    private VersionedValue nativeOwnerFact(PulsarNativeReadCellAuthorityV1.OwnerEpoch epoch) {
        try {
            var bytes = new java.io.ByteArrayOutputStream();
            try (var out = new java.io.DataOutputStream(bytes)) {
                out.writeInt(0x4e504745); // NPGE
                out.writeInt(1);
                out.write(resource.sha256().bytes().toByteArray());
                out.write(epoch.scope().valueSha256().bytes().toByteArray());
                out.write(epoch.scope().metadataVersion().value().toByteArray());
                out.write(epoch.nativeOwner().toByteArray());
            }
            return VersionedValue.of(
                    nativeOwnerKey(),
                    CanonicalBytes.copyOf(bytes.toByteArray()),
                    new com.nereusstream.metadata.spi.model.MetadataVersion(
                            CanonicalBytes.copyOf(java.nio.ByteBuffer.allocate(104)
                                    .put(epoch.nativeOwner().toByteArray())
                                    .put(epoch.scope().metadataVersion().value().toByteArray())
                                    .array())));
        } catch (java.io.IOException impossible) {
            throw new IllegalStateException(impossible);
        }
    }

    private long requireNativeOwnerFact(AuthorityFactV1 previous, boolean fenced) {
        var raw = previous.metadataVersion().value().toByteArray();
        var identity = CanonicalBytes.copyOf(java.util.Arrays.copyOf(raw, 96));
        var current = owner.nativeOwner().toByteArray();
        if (!previous.key().equals(nativeOwnerKey())
                || raw.length != 104
                || !java.util.Arrays.equals(java.util.Arrays.copyOf(raw, 72), java.util.Arrays.copyOf(current, 72))) {
            throw new IllegalStateException("Native GC cannot adopt another native observation owner format/domain");
        }
        long generation = java.nio.ByteBuffer.wrap(raw).getLong(72);
        if (generation < 1 || generation > owner.generation() || fenced && generation == owner.generation()) {
            throw new IllegalStateException("Native GC predecessor has no confirmed durable Cell drain");
        }
        var reconstructed =
                nativeOwnerFact(new PulsarNativeReadCellAuthorityV1.OwnerEpoch(generation, owner.scope(), identity));
        if (!fact(reconstructed).equals(previous)
                || generation == owner.generation() && !identity.equals(owner.nativeOwner())) {
            throw new IllegalStateException("Native GC owner snapshot differs from its actual native identity");
        }
        return generation;
    }

    /** Read-only native owner port; all durable D mutations remain on the original bound Oxia route. */
    private final class NativeMetadata
            implements com.nereusstream.storage.object.gc.BoundPhysicalDeleteAuthorityRouteV2 {
        @Override
        public CompletionStage<java.util.Optional<VersionedValue>> read(String key) {
            return io(() -> {
                if (nativeDelete != null && key.equals(nativeDelete.factKey())) {
                    return nativeDelete.readFact().thenApply(Optional::of);
                }
                if (key.equals(nativeOwnerKey())) {
                    return budget.captureOwnerEpoch(binding).thenApply(value -> Optional.of(nativeOwnerFact(value)));
                }
                if (key.equals(trimProjectionKey())) {
                    return binding.logicalTrimCoordinator()
                            .requireSourceScanCurrent(scan)
                            .thenApply(ignored -> Optional.of(trimProjection()));
                }
                return route.read(key);
            });
        }

        @Override
        public CompletionStage<TransactionOutcome> conditionalTransaction(ExactTransaction transaction) {
            return CompletableFuture.completedFuture(TransactionOutcome.UNSUPPORTED);
        }

        @Override
        public boolean supportsAtomicMultiKeyTransactions() {
            return false;
        }

        @Override
        public CompletionStage<MutationOutcome> compareAndSet(
                Optional<VersionedValue> expected, String key, CanonicalBytes candidate) {
            if (key.equals(nativeOwnerKey())
                    || key.equals(trimProjectionKey())
                    || nativeDelete != null && key.equals(nativeDelete.factKey())) {
                return CompletableFuture.failedFuture(
                        new UnsupportedOperationException("native owner facts are read only"));
            }
            return io(() -> route.compareAndSet(expected, key, candidate));
        }

        @Override
        public CompletionStage<com.nereusstream.storage.api.lifecycle.PhysicalNamespaceAuthorityBindingV2>
                requireActiveResource(PhysicalResourceIdV2 target) {
            return io(() -> route.requireActiveResource(target));
        }
    }

    public synchronized DeleteObservationContextV2 context() {
        requireOpen();
        return context;
    }

    private synchronized void requireOpen() {
        if (closed || context == null) {
            throw new IllegalStateException("Native GC scope is not open");
        }
    }

    private CompletionStage<Void> requireCurrent() {
        requireOpen();
        cursors.requireObservationScope(scan);
        if (nativeDelete != null) {
            return requireCurrentBookKeeper();
        }
        return binding.logicalTrimCoordinator()
                .requireSourceScanCurrent(scan)
                .thenCompose(ignored -> budget.captureOwnerEpoch(binding))
                .thenCompose(actual -> {
                    if (!actual.equals(owner)) {
                        throw new IllegalStateException("Native GC Cell owner changed");
                    }
                    return connection.requireProfileCurrent();
                })
                .thenCompose(ignored -> connection.captureProfileAuthority())
                .thenCompose(actual -> {
                    if (!actual.equals(context.capability())) {
                        throw new IllegalStateException("Native GC policy authority changed");
                    }
                    return route.members().readFact(resource);
                })
                .thenCompose(members -> {
                    if (!members.inventory().protocolCell().equals(profile.protocolCellSha256())
                            || members.inventory().members().stream()
                                    .noneMatch(member -> member.binding().equals(binding.bindingIdentity())
                                            && member.providerScope()
                                                    .equals(profile.scopeId().digest()))
                            || !objectResource()
                                    .objectKey()
                                    .value()
                                    .startsWith(profile.exclusiveNamespacePrefix() + "/")) {
                        throw new IllegalStateException(
                                "Native GC resource has another physical Cell/provider membership");
                    }
                    return route.requireActiveResource(resource).thenApply(ignored -> null);
                })
                .thenCompose(ignored -> CompletableFuture.runAsync(
                        () -> {
                            try {
                                connection.requireObjectDeletePolicyAllows(objectResource());
                            } catch (java.io.IOException failure) {
                                throw new java.util.concurrent.CompletionException(failure);
                            }
                        },
                        executor));
    }

    private CompletionStage<Void> requireCurrentBookKeeper() {
        return binding.logicalTrimCoordinator()
                .requireSourceScanCurrent(scan)
                .thenCompose(ignored -> budget.captureOwnerEpoch(binding))
                .thenCompose(actual -> {
                    if (!actual.equals(owner)) {
                        throw new IllegalStateException("Native BK GC Cell owner changed");
                    }
                    return bookKeeper.confirmedDrainProviderMinimumMillis(bookKeeperScope.namespaceBinding());
                })
                .thenCompose(minimum -> {
                    if (minimum
                            > bookKeeperScope.bookKeeperGcGrace().orElseThrow().providerMinimumMillis()) {
                        throw new IllegalStateException("Native BK GC provider minimum changed or is understated");
                    }
                    return nativeDelete.requireCurrent(resource, context);
                })
                .thenCompose(ignored -> route.members().readFact(resource))
                .thenCompose(members -> {
                    if (!members.inventory().protocolCell().equals(Sha256Digest.hash(bookKeeperScope.protocolCell()))
                            || members.inventory().members().stream()
                                    .noneMatch(member -> member.binding().equals(binding.bindingIdentity())
                                            && member.providerScope().equals(bookKeeperScope.providerScope()))) {
                        throw new IllegalStateException("Native BK target physical Cell/provider membership differs");
                    }
                    return route.requireActiveResource(resource).thenApply(ignored -> null);
                });
    }

    private final class NativeOwnerVerifier implements DeleteObservationAuthorityVerifierV2 {
        @Override
        public CompletionStage<Void> requireCurrent(PhysicalResourceIdV2 target, DeleteObservationContextV2 expected) {
            if (!target.equals(resource) || !expected.equals(context)) {
                return CompletableFuture.failedFuture(
                        new IllegalArgumentException("Native GC context belongs to another resource or epoch"));
            }
            return io(() -> PulsarNativeGcScopeV1.this.requireCurrent());
        }

        @Override
        public CompletionStage<Void> requirePredecessorFenced(
                PhysicalResourceIdV2 target,
                DeleteObservationContextV2 previous,
                DeleteObservationContextV2 successor) {
            if (nativeDelete != null) {
                return io(() -> nativeDelete
                        .requirePredecessorFenced(target, previous, successor)
                        .thenCompose(ignored -> PulsarNativeGcScopeV1.this.requireCurrent()));
            }
            requireNativeOwnerFact(previous.coordinatorOwner(), true);
            if (previous.observationEpoch() >= context.observationEpoch()
                    || !successor.predecessorOwnerFenced().equals(Optional.of(fact(ownerFact)))) {
                return CompletableFuture.failedFuture(
                        new IllegalStateException("Native GC predecessor lacks a durable Cell drain"));
            }
            // This native allocator increments generation only after exact durable drain, never lease expiry.
            return requireCurrent(target, successor);
        }
    }

    /** Root comes exclusively from all actual producers; caller time/duration/absence assertions are not accepted. */
    public synchronized CompletableFuture<Void> mark(
            PulsarNativeM4ControlV1 control,
            PulsarNativeMaterializationTaskV2.RetentionRoots tasks,
            M5ClosedWriterRegistryV1 registry,
            Executor executor) {
        requireOpen();
        if (originalMark != null && !originalMark.isCompletedExceptionally()) {
            return originalMark.copy();
        }
        if (grace != null) {
            originalMark = own(() -> requireCurrent().thenCompose(ignored -> grace.confirmMark()));
            return originalMark.copy();
        }
        originalMark = own(() -> requireCurrent()
                // Capture D before scanning: every resource writer must invalidate this exact predecessor.
                .thenCompose(ignored -> route.read(resource.authorityKey()))
                .thenCompose(predecessor -> control.capturePhysicalReferences(
                                scan, cursors, tasks, registry, route.members(), resource, budget, executor)
                        .thenCompose(results -> {
                            if (results.stream()
                                    .anyMatch(value -> !value.summary().complete())) {
                                throw new IllegalStateException("Native GC initial reference inventory is incomplete");
                            }
                            var rows = new ArrayList<ReferenceObservationV1>();
                            results.forEach(result -> rows.addAll(result.observations()));
                            if (rows.stream().anyMatch(row -> row.disposition() != ReferenceDispositionV1.ABSENT)) {
                                throw new IllegalStateException("Native GC initial inventory still has a reference");
                            }
                            var coverage = rows.get(0).coverage();
                            for (AuthorityFactV1 fact : auditFacts()) {
                                rows.add(new ReferenceObservationV1(
                                        ReferenceKindV1.AUDIT_GRACE,
                                        fact,
                                        resource.sha256(),
                                        coverage,
                                        ReferenceDispositionV1.ABSENT,
                                        true));
                            }
                            rows.sort(Comparator.comparing(ReferenceObservationV1::kind)
                                    .thenComparing(row -> row.authority().key()));
                            var root = M5RetentionCodecV1.calculateObservationsRoot(rows);
                            synchronized (this) {
                                grace = coordinator.prepareEpochGrace(
                                        predecessor.orElseThrow(),
                                        context,
                                        nativeDelete == null
                                                ? profile.gcGracePolicy()
                                                        .orElseThrow()
                                                        .durationMillis()
                                                : bookKeeperScope
                                                        .bookKeeperGcGrace()
                                                        .orElseThrow()
                                                        .durationMillis(),
                                        root);
                                return grace.confirmMark();
                            }
                        })));
        return originalMark.copy();
    }

    private java.util.List<AuthorityFactV1> auditFacts() {
        return java.util.stream.Stream.concat(
                        context.authorityFacts().stream(), java.util.stream.Stream.of(fact(ownerFact)))
                .distinct()
                .toList();
    }

    public CompletableFuture<M5TargetDeleteAuthorityCoordinatorV1.EpochGraceReady> requireElapsed() {
        return own(() -> {
            if (grace == null) {
                return CompletableFuture.failedFuture(new IllegalStateException("Native GC has no original mark"));
            }
            return requireCurrent().thenCompose(ignored -> grace.requireElapsed());
        });
    }

    /** Published Native source reclamation; the actual Native INTENT must precede this frozen source scan. */
    public synchronized CompletableFuture<VersionedValue> reclaimBookKeeperAfterElapsed(
            PulsarNativeM4ControlV1 control,
            PulsarNativeMaterializationTaskV2.RetentionRoots tasks,
            M5ClosedWriterRegistryV1 registry,
            PulsarNativeM4FallbackRetirementV1.Result released,
            DeleteEligibilitySnapshotV2.ReclamationReason reason) {
        try {
            requireOpen();
            if (nativeDelete == null || reason == DeleteEligibilitySnapshotV2.ReclamationReason.UNPUBLISHED_ARTIFACT) {
                throw new IllegalArgumentException("Native BK GC requires replacement or logical expiry");
            }
            if (bookKeeperDeletion != null
                    && (bookKeeperDeletion.reason != reason
                            || !bookKeeperDeletion.batch.equals(released.batch().batchIdSha256()))) {
                throw new IllegalArgumentException("Original Native BK GC reason or M4 release changed");
            }
            if (originalDelete != null && !originalDelete.isCompletedExceptionally()) {
                return originalDelete.copy();
            }
            originalDelete = own(() -> requireCurrent()
                    .thenCompose(ignored -> route.read(resource.authorityKey()))
                    .thenCompose(stored -> {
                        var exact = stored.orElseThrow();
                        var value = com.nereusstream.storage.object.gc.M5TargetDeleteStoredValueV2.decode(exact);
                        if (value.state() == TargetDeleteAuthorityStateV1.DELETE_DONE_V1) {
                            return settleBookKeeperDone(exact);
                        }
                        if (bookKeeperDeletion != null) {
                            return bookKeeperDeletion.run();
                        }
                        if (grace == null) {
                            throw new IllegalStateException("Native BK GC lacks its original confirmed mark");
                        }
                        return nativeDelete
                                .reconcileFencedHold(bookKeeper.nativeDeleteCellBudget())
                                .thenCompose(ignored -> grace.requireElapsed())
                                .thenCompose(ready -> captureBookKeeperDeletion(
                                        control, tasks, registry, released, reason, exact, ready))
                                .thenCompose(attempt -> {
                                    bookKeeperDeletion = attempt;
                                    return attempt.run();
                                });
                    }));
            return originalDelete.copy();
        } catch (Throwable failure) {
            return CompletableFuture.failedFuture(failure);
        }
    }

    private CompletionStage<VersionedValue> settleBookKeeperDone(VersionedValue done) {
        var stored = com.nereusstream.storage.object.gc.M5TargetDeleteStoredValueV2.decode(done);
        var mutations =
                new M5TargetDeleteAuthorityCoordinatorV1(new NativeMetadata(), new NativeOwnerVerifier(), nativeDelete);
        CompletionStage<VersionedValue> terminal = stored.compactDone().isPresent()
                ? CompletableFuture.completedFuture(done)
                : mutations.compactDone(done).thenApply(result -> applied(result, true));
        return terminal.thenCompose(compact -> route.quota().settle(resource).thenApply(result -> {
            if (result != M5GcQuotaCoordinatorV2.Result.SETTLED) {
                throw new IllegalStateException(
                        "Native BK compact DONE quota settlement remains unresolved: " + result);
            }
            return compact;
        }));
    }

    private CompletionStage<BookKeeperDeleteAttempt> captureBookKeeperDeletion(
            PulsarNativeM4ControlV1 control,
            PulsarNativeMaterializationTaskV2.RetentionRoots tasks,
            M5ClosedWriterRegistryV1 registry,
            PulsarNativeM4FallbackRetirementV1.Result released,
            DeleteEligibilitySnapshotV2.ReclamationReason reason,
            VersionedValue original,
            M5TargetDeleteAuthorityCoordinatorV1.EpochGraceReady ready) {
        var ledger = (PhysicalResourceIdV2.BookKeeperLedger) resource;
        var nativeIntent = cursors.bookKeeperIntent(ledger.ledgerId());
        return control.readSelectedMaterializationSnapshot(executor).thenCompose(selected -> {
            var map = selected.selected().readMap();
            var plan = map.plan();
            if (!released.batch().binding().equals(binding.bindingIdentity())
                    || !map.fullyReplaces(ledger.ledgerId(), nativeIntent.entries() - 1)
                    || plan.sourceCut().coverage().pulsarCoverage().orElseThrow().ranges().stream()
                            .noneMatch(range -> range.ledgerId() == ledger.ledgerId()
                                    && range.startEntryInclusive() == 0
                                    && range.endEntryExclusive() == nativeIntent.entries())) {
                throw new IllegalStateException(
                        "Native BK GC lacks its complete verified selected replacement and M4 release");
            }
            var old = map.predecessor()
                    .publishedObjects(
                            binding,
                            bookKeeperScope.namespaceBinding(),
                            selected.selected().providerProfile().physicalBinding())
                    .get(ledger.ledgerId());
            if (old == null
                    || !old.identity().attemptUuid().equals(nativeIntent.offloadAttempt())
                    || old.lastEntry() + 1 != nativeIntent.entries()
                    || old.logicalLength() != nativeIntent.logicalLength()) {
                throw new IllegalStateException(
                        "Native BK intent differs from its original offload completion/selection");
            }
            var coverage = M5MaterializationRecordsV1.ProtocolCoverage.pulsar(cursors.coverage()
                    .withRanges(java.util.List.of(
                            new M5PulsarCoverageV2.EntryRange(ledger.ledgerId(), 0, nativeIntent.entries()))));
            return capturePublishedEvidence(
                            control,
                            tasks,
                            registry,
                            released,
                            bookKeeperScope.namespaceBinding(),
                            selected,
                            reason,
                            original,
                            ready,
                            coverage,
                            ledger.ledgerId(),
                            nativeIntent.entries())
                    .thenApply(eligibility -> new BookKeeperDeleteAttempt(
                            original, eligibility, ready, released.batch().batchIdSha256()));
        });
    }

    private CompletionStage<DeleteEligibilitySnapshotV2> capturePublishedEvidence(
            PulsarNativeM4ControlV1 control,
            PulsarNativeMaterializationTaskV2.RetentionRoots tasks,
            M5ClosedWriterRegistryV1 registry,
            PulsarNativeM4FallbackRetirementV1.Result released,
            PhysicalNamespaceAuthorityBindingV2 bookKeeperBinding,
            PulsarNativeM4ControlV1.SelectedMaterializationSnapshot selected,
            DeleteEligibilitySnapshotV2.ReclamationReason reason,
            VersionedValue original,
            M5TargetDeleteAuthorityCoordinatorV1.EpochGraceReady ready,
            M5MaterializationRecordsV1.ProtocolCoverage coverage,
            long sourceLedger,
            long sourceEntries) {
        var replacements = reason == DeleteEligibilitySnapshotV2.ReclamationReason.REPLACED_REPRESENTATION
                ? selected.selected()
                        .readMap()
                        .physicalLedgerReferences(
                                binding,
                                bookKeeperBinding,
                                selected.selected().providerProfile().physicalBinding(),
                                sourceLedger,
                                sourceEntries)
                        .orElseThrow()
                : List.<PhysicalResourceIdV2>of();
        return control.captureRetentionFloors(
                        scan, cursors, tasks, registry, route.members(), resource, budget, executor)
                .thenCompose(floors -> control.capturePhysicalReferences(
                                scan, cursors, tasks, registry, route.members(), resource, budget, executor)
                        .thenCompose(references -> captureSourceReleases(control, selected, released, bookKeeperBinding)
                                .thenCompose(releases -> buildPublishedEvidence(
                                        control,
                                        selected,
                                        tasks,
                                        registry,
                                        reason,
                                        original,
                                        ready,
                                        coverage,
                                        floors,
                                        references,
                                        releases,
                                        replacements))));
    }

    private record SourceReleases(
            java.util.List<M5RetentionRecordsV1.M4ReleaseBindingV1> bindings, java.util.List<VersionedValue> facts) {}

    private CompletionStage<SourceReleases> captureSourceReleases(
            PulsarNativeM4ControlV1 control,
            PulsarNativeM4ControlV1.SelectedMaterializationSnapshot selected,
            PulsarNativeM4FallbackRetirementV1.Result released,
            PhysicalNamespaceAuthorityBindingV2 bookKeeperBinding) {
        var predecessor = released.predecessor();
        var batch = released.batch();
        var mapFact = released.originalSourceMap();
        var sourceMap =
                PulsarNativeM4SourceMapV1.decode(mapFact.canonicalStoredBytes(), predecessor.selectedViewSha256());
        if (!mapFact.key().equals(control.sourceMapKey(predecessor.selectedViewSha256()))
                || !batch.predecessorSelectorCoreSha256().equals(M4ReadControlCodecV1.selectorCoreSha256(predecessor))
                || !batch.successorSelectorCoreSha256()
                        .equals(M4ReadControlCodecV1.selectorCoreSha256(released.selector()))
                || !selected.selected().selector().activeBatches().contains(batch)
                || !released.selector().activeBatches().contains(batch)
                || released.selector().pendingAnchors().stream()
                        .noneMatch(anchor -> anchor.predecessorSelectorCoreSha256()
                                        .equals(batch.predecessorSelectorCoreSha256())
                                && anchor.successorSelectorCoreSha256().equals(batch.successorSelectorCoreSha256())
                                && anchor.transitionSha256().equals(batch.transitionSha256())
                                && M4ReadControlCodecV1.anchorSha256(anchor)
                                        .equals(released.terminal().closureAnchorSha256()))) {
            throw new IllegalStateException("Native release lacks its original source-map/selector closure");
        }
        var facts = new ArrayList<VersionedValue>();
        CompletionStage<Void> capture = route.read(mapFact.key()).thenAccept(current -> {
            if (!current.equals(Optional.of(mapFact))) {
                throw new IllegalStateException("Native original source-map version changed");
            }
            facts.add(mapFact);
        });
        for (var expected : java.util.List.of(
                new java.util.AbstractMap.SimpleImmutableEntry<>(
                        control.terminalKey(released.terminal().readAdmissionEpoch()),
                        M4ReadControlCodecV1.encodeTerminal(released.terminal())),
                new java.util.AbstractMap.SimpleImmutableEntry<>(
                        control.proofKey(released.proof().readAdmissionEpoch()),
                        M4ReadControlCodecV1.encodeProof(released.proof())))) {
            capture =
                    capture.thenCompose(ignored -> route.read(expected.getKey()).thenAccept(current -> {
                        var value = current.orElseThrow();
                        if (!value.canonicalStoredBytes().equals(expected.getValue())) {
                            throw new IllegalStateException("Native original M4 terminal/proof changed");
                        }
                        facts.add(value);
                    }));
        }
        capture = capture.thenCompose(ignored -> route.read(control.proofHeadKey())
                .thenAcceptAsync(
                        current -> {
                            var value = current.orElseThrow();
                            var head = M4ReadControlCodecV1.decodeHead(value.canonicalStoredBytes());
                            if (!control.coordinator()
                                    .verifyInterval(
                                            head,
                                            batch.minimumFirstEpochSummary(),
                                            batch.sharedLastFallbackCapableReadAdmissionEpoch())) {
                                throw new IllegalStateException(
                                        "Native original M4 release interval is no longer proven");
                            }
                            facts.add(value);
                        },
                        executor));
        CompletionStage<java.util.List<M5RetentionRecordsV1.M4ReleaseBindingV1>> work =
                capture.thenApply(ignored -> new ArrayList<>());
        for (var source : released.batch().sources()) {
            if (!sourceMap
                    .protectedPhysicalReferences(
                            binding,
                            bookKeeperBinding,
                            selected.selected().providerProfile().physicalBinding(),
                            source.sourceIdentitySha256())
                    .contains(resource)) {
                continue;
            }
            work = work.thenCompose(
                    rows -> route.read(control.protectionKey(source)).thenApply(stored -> {
                        var value = stored.orElseThrow();
                        var protection =
                                com.nereusstream.storage.object.read.control.M4ReadControlCodecV1.decodeProtection(
                                        value.canonicalStoredBytes());
                        if (!protection.binding().equals(binding.bindingIdentity())
                                || !protection.identity().equals(source)
                                || protection.state() != ProtectionState.RELEASED
                                || !protection
                                        .releasedByBatchSha256()
                                        .equals(Optional.of(released.batch().batchIdSha256()))
                                || !protection
                                        .releaseProofHeadSha256()
                                        .equals(Optional.of(Sha256Digest.hash(
                                                released.releaseHead().canonicalStoredBytes())))) {
                            throw new IllegalStateException(
                                    "Native protection is not released by this exact original M4 batch");
                        }
                        rows.add(new M5RetentionRecordsV1.M4ReleaseBindingV1(
                                source.sourceIdentitySha256(),
                                source.protectionGeneration(),
                                fact(value),
                                value.canonicalStoredBytes(),
                                released.batch().batchIdSha256(),
                                protection.releaseProofHeadSha256().orElseThrow()));
                        return rows;
                    }));
        }
        return work.thenApply(rows -> {
            if (rows.isEmpty()) {
                throw new IllegalStateException("Native resource has no exact M4 RELEASED binding");
            }
            return new SourceReleases(java.util.List.copyOf(rows), java.util.List.copyOf(facts));
        });
    }

    private VersionedValue applied(M5TargetDeleteAuthorityCoordinatorV1.MutationResultV1 result, boolean terminal) {
        if (!(terminal ? result.exactTerminalIsAuthoritative() : result.exactCandidateIsAuthoritative())) {
            throw new IllegalStateException("Original Native delete mutation remains unresolved: " + result.outcome());
        }
        return result.observed().orElseThrow();
    }

    private CompletionStage<DeleteEligibilitySnapshotV2> buildPublishedEvidence(
            PulsarNativeM4ControlV1 control,
            PulsarNativeM4ControlV1.SelectedMaterializationSnapshot selected,
            PulsarNativeMaterializationTaskV2.RetentionRoots tasks,
            M5ClosedWriterRegistryV1 registry,
            DeleteEligibilitySnapshotV2.ReclamationReason reason,
            VersionedValue original,
            M5TargetDeleteAuthorityCoordinatorV1.EpochGraceReady ready,
            M5MaterializationRecordsV1.ProtocolCoverage coverage,
            java.util.List<M5RetentionEvidenceAssemblerV1.FloorAdapterResultV1> floors,
            java.util.List<M5RetentionEvidenceAssemblerV1.ReferenceAdapterResultV1> references,
            SourceReleases releases,
            List<PhysicalResourceIdV2> replacements) {
        var authority = M5TargetDeleteAuthorityCodecV1.decodeAuthority(original.canonicalStoredBytes());
        var source = M5PulsarAggregateAuthorityCodecV1.decodeAuthority(scan.canonicalStoredBytes());
        var configured = source.nativeRetention().orElseThrow();
        var policy = Sha256Digest.hash(CanonicalBytes.copyOf(
                java.nio.ByteBuffer.allocate(36 + configured.encode().length())
                        .putInt(0x4e525031)
                        .put(M5PulsarAggregateAuthorityCodecV1.nativeOnlyPolicyRoot(source)
                                .bytes()
                                .toByteArray())
                        .put(configured.encode().toByteArray())
                        .array()));
        var frontier = source.logicalTrim().flatMap(value -> value.frontier());
        var prior = frontier.map(value -> value.pulsarContext().orElseThrow().newFrontier())
                .orElse(new M5PulsarCoverageV2.Position(
                        cursors.coverage().chain().get(0).ledgerId(), 0));
        var selectedValue = selected.selected();
        var plan = selectedValue.readMap().plan();
        var keys = new M5MaterializationKeysV1(0, binding.bindingIdentity());
        var manifest = selected.immutableFacts().get(keys.manifest(selectedValue.materializedManifestSha256()));
        if (manifest == null) {
            return CompletableFuture.failedFuture(
                    new IllegalStateException("Native source lacks actual selected manifest"));
        }
        var floorRequest = new M5RetentionEvidenceAssemblerV1.FloorSnapshotRequestV1(
                plan.sourceCut().identity(),
                M5MaterializationRecordsV1.PositionDomain.PULSAR_ENTRY,
                Math.addExact(authority.authorityRevision(), 1),
                -1,
                policy,
                fact(selectedValue.selectorFact()),
                fact(scan),
                Optional.of(new M5RetentionRecordsV1.PulsarFloorContextV2(cursors.coverage(), prior)));
        var floorAdapters =
                new java.util.EnumMap<M5RetentionRecordsV1.FloorClassV1, M5RetentionEvidenceAssemblerV1.FloorAdapterV1>(
                        M5RetentionRecordsV1.FloorClassV1.class);
        floors.stream()
                .filter(value -> value.floorClass() != M5RetentionRecordsV1.FloorClassV1.LIFECYCLE_TASK)
                .forEach(value ->
                        floorAdapters.put(value.floorClass(), ignored -> CompletableFuture.completedFuture(value)));
        var taskFloor = tasks.closedTaskFloor(registry, resource, cursors);
        floorAdapters.put(taskFloor.floorClass(), ignored -> CompletableFuture.completedFuture(taskFloor));
        var referenceAdapters = new java.util.EnumMap<
                ReferenceKindV1, M5RetentionEvidenceAssemblerV1.ReferenceAdapterV1>(ReferenceKindV1.class);
        for (var result : references) {
            var rows = new ArrayList<ReferenceObservationV1>();
            for (var row : result.observations()) {
                if (!row.coverage().contains(coverage)) {
                    throw new IllegalStateException("Native reference inventory omits part of the physical source");
                }
                rows.add(new ReferenceObservationV1(
                        row.kind(),
                        row.authority(),
                        row.targetIdentitySha256(),
                        coverage,
                        row.disposition(),
                        row.enumerationComplete()));
            }
            long scannedBytes = result.summary().scannedBytes();
            if (result.referenceKind() == ReferenceKindV1.AUDIT_GRACE) {
                for (var value : auditFacts()) {
                    rows.add(new ReferenceObservationV1(
                            ReferenceKindV1.AUDIT_GRACE,
                            value,
                            resource.sha256(),
                            coverage,
                            ReferenceDispositionV1.ABSENT,
                            true));
                }
                scannedBytes = Math.addExact(
                        scannedBytes, ownerFact.canonicalStoredBytes().length());
                scannedBytes = Math.addExact(
                        scannedBytes,
                        context.capability().metadataVersion().value().length() + Sha256Digest.LENGTH);
                for (var value : releases.facts()) {
                    rows.add(new ReferenceObservationV1(
                            ReferenceKindV1.AUDIT_GRACE,
                            fact(value),
                            resource.sha256(),
                            coverage,
                            ReferenceDispositionV1.ABSENT,
                            true));
                    scannedBytes = Math.addExact(
                            scannedBytes, value.canonicalStoredBytes().length());
                }
            }
            rows.sort(Comparator.comparing(row -> row.authority().key()));
            var updated = new M5RetentionEvidenceAssemblerV1.ReferenceAdapterResultV1(
                    result.referenceKind(),
                    new M5RetentionRecordsV1.ReferenceScanSummaryV1(
                            result.referenceKind(),
                            rows.size(),
                            result.summary().pageCount(),
                            scannedBytes,
                            result.summary().complete()),
                    rows);
            referenceAdapters.put(result.referenceKind(), ignored -> CompletableFuture.completedFuture(updated));
        }
        var assembler = new M5RetentionEvidenceAssemblerV1(new NativeMetadata(), floorAdapters, referenceAdapters);
        var trim = reason == DeleteEligibilitySnapshotV2.ReclamationReason.LOGICAL_EXPIRY
                ? fact(trimProjection())
                : fact(scan);
        return assembler.buildFloorSnapshot(floorRequest).thenCompose(snapshot -> {
            var request = new M5RetentionEvidenceAssemblerV1.ReferenceProofRequestV1(
                    plan.sourceCut().identity(),
                    M5RetentionRecordsV1.ReferenceTargetKindV1.READABLE_SOURCE,
                    resource.sha256(),
                    coverage,
                    fact(selectedValue.selectorFact()),
                    materializationFact(
                            manifest,
                            control.materializationManifestKey(selectedValue.materializedManifestSha256()),
                            snapshot),
                    trim,
                    snapshot.snapshotRootSha256(),
                    releases.bindings(),
                    fact(selectedValue.selectorFact()),
                    fact(selectedValue.selectorFact()),
                    fact(scan),
                    context.capability(),
                    0,
                    0,
                    Optional.of(context));
            return assembler
                    .buildEpochReferenceFreeProof(request, ready)
                    .thenCompose(proof -> route.members().readFact(resource).thenApply(members -> {
                        boolean replacement =
                                reason == DeleteEligibilitySnapshotV2.ReclamationReason.REPLACED_REPRESENTATION;
                        var member = new DeleteEligibilitySnapshotV2.MemberSnapshot(
                                snapshot,
                                proof,
                                fact(scan),
                                replacement
                                        ? DeleteEligibilitySnapshotV2.NativeDisposition.REPLACEMENT_ALLOWED
                                        : DeleteEligibilitySnapshotV2.NativeDisposition.LOGICAL_EXPIRY_ALLOWED,
                                replacement
                                        ? Optional.of(replacementEvidence(
                                                control, selected, manifest, coverage, snapshot, replacements))
                                        : Optional.empty(),
                                replacement ? Optional.empty() : Optional.of(frontier.orElseThrow()),
                                Optional.empty());
                        return new DeleteEligibilitySnapshotV2(
                                resource,
                                reason,
                                snapshot.generation(),
                                context.capability(),
                                members.authority(),
                                java.util.List.of(member));
                    }));
        });
    }

    /** Compares the persisted independent full validation with the original source cut, never caller roots. */
    private DeleteEligibilitySnapshotV2.ReplacementEvidence replacementEvidence(
            PulsarNativeM4ControlV1 control,
            PulsarNativeM4ControlV1.SelectedMaterializationSnapshot selected,
            VersionedValue manifestFact,
            M5MaterializationRecordsV1.ProtocolCoverage coverage,
            M5RetentionRecordsV1.RetentionFloorSnapshotV1 floors,
            List<PhysicalResourceIdV2> replacements) {
        var keys = new M5MaterializationKeysV1(0, binding.bindingIdentity());
        var manifest = M5MaterializationCodecV1.decodeManifest(manifestFact.canonicalStoredBytes());
        var generationFact = selected.immutableFacts().get(keys.generation(manifest.preferredGenerationSha256()));
        var generation = M5MaterializationCodecV1.decodeGeneration(generationFact.canonicalStoredBytes());
        var validationFact = selected.immutableFacts().get(keys.validation(generation.validationRootSha256()));
        var validation = M5MaterializationCodecV1.decodeValidationRoot(validationFact.canonicalStoredBytes());
        var cut = selected.selected().readMap().plan().sourceCut();
        var expectedEntries = Sha256Digest.hash(CanonicalUtf8.fromString("M5-A-PULSAR-ENTRY-EQUALITY-V2\0"
                        + String.join(
                                "\0",
                                cut.sources().stream()
                                        .map(value -> value.pulsarEntryRootSha256()
                                                .orElseThrow()
                                                .toHex())
                                        .toList()))
                .bytes());
        var before = java.util.List.of(
                cut.protocolStateRootSha256(),
                cut.recoveryCheckpointRootSha256(),
                cut.sourceSetSha256(),
                expectedEntries);
        var after = java.util.List.of(
                generation.protocolStateRootSha256(),
                selected.selected().nativeInput().recoveryInputSha256(),
                validation.sourceSetSha256(),
                validation.payloadEqualityOrSemanticRootSha256());
        var transfers = new ArrayList<DeleteEligibilitySnapshotV2.SemanticTransfer>();
        for (var aspect :
                DeleteEligibilitySnapshotV2.requiredSemantics(M5MaterializationRecordsV1.PositionDomain.PULSAR_ENTRY)) {
            transfers.add(new DeleteEligibilitySnapshotV2.SemanticTransfer(
                    aspect,
                    coverage,
                    semanticRoot(aspect, coverage, before),
                    semanticRoot(aspect, coverage, after),
                    materializationFact(
                            validationFact,
                            control.materializationValidationKey(generation.validationRootSha256()),
                            floors)));
        }
        return new DeleteEligibilitySnapshotV2.ReplacementEvidence(
                materializationFact(
                        manifestFact,
                        control.materializationManifestKey(selected.selected().materializedManifestSha256()),
                        floors),
                replacements,
                transfers);
    }

    private static Sha256Digest semanticRoot(
            DeleteEligibilitySnapshotV2.SemanticAspect aspect,
            M5MaterializationRecordsV1.ProtocolCoverage coverage,
            java.util.List<Sha256Digest> roots) {
        try {
            var bytes = new java.io.ByteArrayOutputStream();
            try (var out = new java.io.DataOutputStream(bytes)) {
                out.writeUTF("NEREUS_NATIVE_PULSAR_VERIFIED_REPLACEMENT");
                out.writeInt(1);
                out.writeUTF(aspect.name());
                var encoded = coverage.pulsarCoverage().orElseThrow().encode();
                out.writeInt(encoded.length());
                out.write(encoded.toByteArray());
                out.writeInt(roots.size());
                for (var root : roots) {
                    out.write(root.bytes().toByteArray());
                }
            }
            return Sha256Digest.hash(CanonicalBytes.copyOf(bytes.toByteArray()));
        } catch (java.io.IOException impossible) {
            throw new IllegalStateException(impossible);
        }
    }

    private final class BookKeeperDeleteAttempt {
        private final VersionedValue original;
        private final DeleteEligibilitySnapshotV2 snapshot;
        private final M5TargetDeleteAuthorityCoordinatorV1.EpochGraceReady ready;
        private final DeleteEligibilitySnapshotV2.ReclamationReason reason;
        private final Sha256Digest batch;
        private final Sha256Digest attempt;
        private final TargetDeleteAuthorityStateV1 initialState;
        private final M5TargetDeleteAuthorityCoordinatorV1 mutations;
        private M5TargetDeleteAuthorityCoordinatorV1.MutationResultV1 refreshResult;
        private VersionedValue qualified;
        private VersionedValue fenced;
        private ExactExternalIdentityV1 external;
        private VersionedValue intent;
        private com.nereusstream.storage.bookkeeper.M5BookKeeperNativeDeleteIntentV2 nativeIntent;
        private VersionedValue done;

        private BookKeeperDeleteAttempt(
                VersionedValue original,
                DeleteEligibilitySnapshotV2 snapshot,
                M5TargetDeleteAuthorityCoordinatorV1.EpochGraceReady ready,
                Sha256Digest batch) {
            this.original = original;
            this.snapshot = snapshot;
            this.ready = ready;
            this.reason = snapshot.reason();
            this.batch = batch;
            var stored = M5TargetDeleteAuthorityCodecV1.decodeAuthority(original.canonicalStoredBytes());
            initialState = stored.state();
            attempt = stored.deleteIntent()
                    .map(value -> value.deleteAttemptIdSha256())
                    .orElseGet(() -> stored.readFence()
                            .map(value -> value.attemptIdSha256())
                            .orElseGet(() -> Sha256Digest.hash(CanonicalUtf8.fromString("NATIVE_BOOKKEEPER_DELETE_V1:"
                                            + ready.mark().clockDomain())
                                    .bytes())));
            mutations = new M5TargetDeleteAuthorityCoordinatorV1(
                    new NativeMetadata(), new NativeOwnerVerifier(), nativeDelete);
        }

        private CompletionStage<VersionedValue> refreshOriginal() {
            if (refreshResult == null) {
                return performRefresh();
            }
            return route.read(resource.authorityKey()).thenCompose(stored -> {
                if (stored.filter(value -> value.canonicalStoredBytes().equals(refreshResult.exactCandidate()))
                        .isPresent()) {
                    return ready.requireAdmittedCurrent(resource, ready.mark())
                            .thenApply(ignored -> stored.orElseThrow());
                }
                if (stored.equals(Optional.of(original))) {
                    return performRefresh();
                }
                return CompletableFuture.failedFuture(new IllegalStateException("Original Native BK refresh changed"));
            });
        }

        private CompletionStage<VersionedValue> performRefresh() {
            var work = initialState == TargetDeleteAuthorityStateV1.READ_FENCED_V1
                    ? mutations.refreshIdentityRead(original, context, snapshot, ready)
                    : mutations.refreshDispatch(original, context, snapshot, ready);
            return work.thenApply(result -> {
                refreshResult = result;
                return applied(result, false);
            });
        }

        private CompletionStage<VersionedValue> run() {
            CompletionStage<Void> work = CompletableFuture.completedFuture(null);
            if (initialState == TargetDeleteAuthorityStateV1.READ_FENCED_V1) {
                work = work.thenCompose(ignored -> fenced != null
                        ? CompletableFuture.completedFuture(null)
                        : refreshOriginal().thenAccept(value -> fenced = value));
            } else if (initialState == TargetDeleteAuthorityStateV1.DELETE_INTENT_V1) {
                work = work.thenCompose(ignored -> intent != null
                        ? CompletableFuture.completedFuture(null)
                        : refreshOriginal().thenAccept(value -> intent = value));
            } else {
                work = work.thenCompose(ignored -> qualified != null
                        ? CompletableFuture.completedFuture(null)
                        : mutations
                                .qualifyEligibility(original, snapshot, ready)
                                .thenAccept(result -> qualified = applied(result, false)));
                work = work.thenCompose(ignored -> fenced != null
                        ? CompletableFuture.completedFuture(null)
                        : mutations
                                .prepareIdentityRead(qualified, attempt, context, ready)
                                .thenAccept(result -> fenced = applied(result, false)));
            }
            work = work.thenCompose(ignored -> intent != null || external != null
                    ? CompletableFuture.completedFuture(null)
                    : nativeDelete
                            .capture(M5TargetDeleteAuthorityCodecV1.decodeAuthority(fenced.canonicalStoredBytes()))
                            .thenAccept(value -> external = value));
            work = work.thenCompose(ignored -> intent != null
                    ? CompletableFuture.completedFuture(null)
                    : mutations
                            .bindDeleteIntent(fenced, external, attempt, ready)
                            .thenAccept(result -> intent = applied(result, false)));
            var metadata = new NativeMetadata();
            work = work.thenCompose(ignored -> {
                if (done != null) {
                    return CompletableFuture.completedFuture(null);
                }
                var identity = M5TargetDeleteAuthorityCodecV1.decodeAuthority(intent.canonicalStoredBytes())
                        .externalIdentity()
                        .orElseThrow();
                return nativeDelete
                        .rereadExact(identity)
                        .thenCompose(observed -> {
                            if (observed
                                    == com.nereusstream.storage.object.gc.M5TargetDeleteAuthorityRecordsV1
                                            .ExternalIdentityObservationV1.ABSENT_EXACT_V1) {
                                return nativeDelete.finishObservedAbsence(
                                        metadata, bookKeeper.nativeDeleteCellBudget(), intent, ready);
                            }
                            CompletionStage<com.nereusstream.storage.bookkeeper.M5BookKeeperNativeDeleteIntentV2>
                                    bound = nativeIntent == null
                                            ? nativeDelete
                                                    .bindIntent(metadata, intent, ready)
                                                    .thenApply(value -> nativeIntent = value)
                                            : CompletableFuture.completedFuture(nativeIntent);
                            return bound.thenCompose(value -> nativeDelete.deleteOrReconcile(
                                    metadata, bookKeeper.nativeDeleteCellBudget(), intent, value, ready));
                        })
                        .thenCompose(ignoredAbsent -> mutations.completeAbsent(intent))
                        .thenAccept(result -> done = applied(result, true));
            });
            return work.thenCompose(ignored -> settleBookKeeperDone(done));
        }
    }

    /** Reclaims one originally assigned, terminal, never-selected output; UNKNOWN retries retain this candidate. */
    public synchronized CompletableFuture<VersionedValue> reclaimUnpublishedAfterElapsed(
            PulsarNativeM4ControlV1 control,
            PulsarNativeMaterializationTaskV2.RetentionRoots tasks,
            M5ClosedWriterRegistryV1 registry) {
        requireOpen();
        if (originalDelete != null && !originalDelete.isCompletedExceptionally()) {
            return originalDelete.copy();
        }
        originalDelete = own(() -> {
            if (deletion != null) {
                return deletion.run();
            }
            if (grace == null) {
                return CompletableFuture.failedFuture(
                        new IllegalStateException("Native deletion lacks its original mark"));
            }
            return grace.requireElapsed()
                    .thenCompose(ready -> captureUnpublished(control, tasks, registry, ready))
                    .thenCompose(attempt -> {
                        deletion = attempt;
                        return attempt.run();
                    });
        });
        return originalDelete.copy();
    }

    private record ReleasedOffloadTarget(
            PhysicalNamespaceAuthorityBindingV2 bookKeeper, long ledger, UUID attempt, Sha256Digest release) {}

    /** Reclaims one original exact NPD1/NPO1 version after its selected materialization retired the fallback. */
    public synchronized CompletableFuture<VersionedValue> reclaimPublishedOffloadAfterElapsed(
            PulsarNativeM4ControlV1 control,
            PulsarNativeMaterializationTaskV2.RetentionRoots tasks,
            M5ClosedWriterRegistryV1 registry,
            PulsarNativeM4FallbackRetirementV1.Result released,
            PhysicalNamespaceAuthorityBindingV2 bookKeeperBinding,
            long sourceLedger,
            UUID attempt) {
        requireOpen();
        objectResource();
        var target = new ReleasedOffloadTarget(
                bookKeeperBinding, sourceLedger, attempt, released.batch().batchIdSha256());
        if (releasedOffload != null && !releasedOffload.equals(target)
                || releasedOffload == null && originalDelete != null) {
            return CompletableFuture.failedFuture(
                    new IllegalStateException("Original Native offload deletion changed"));
        }
        releasedOffload = target;
        if (originalDelete != null && !originalDelete.isCompletedExceptionally()) {
            return originalDelete.copy();
        }
        if (grace == null) {
            return CompletableFuture.failedFuture(
                    new IllegalStateException("Native offload deletion lacks its original mark"));
        }
        originalDelete = own(() -> requireCurrent().thenCompose(unused -> {
            if (deletion != null) {
                return deletion.run();
            }
            return grace.requireElapsed().thenCompose(ready -> route.read(resource.authorityKey())
                    .thenCompose(found -> capturePublishedOffload(
                            control, tasks, registry, released, target, found.orElseThrow(), ready))
                    .thenCompose(original -> {
                        deletion = original;
                        return original.run();
                    }));
        }));
        return originalDelete.copy();
    }

    private CompletionStage<DeleteAttempt> capturePublishedOffload(
            PulsarNativeM4ControlV1 control,
            PulsarNativeMaterializationTaskV2.RetentionRoots tasks,
            M5ClosedWriterRegistryV1 registry,
            PulsarNativeM4FallbackRetirementV1.Result released,
            ReleasedOffloadTarget target,
            VersionedValue original,
            M5TargetDeleteAuthorityCoordinatorV1.EpochGraceReady ready) {
        return control.readSelectedMaterializationSnapshot(executor).thenCompose(selected -> {
            var selectedValue = selected.selected();
            var map = selectedValue.readMap();
            var objects = selectedValue.providerProfile().physicalBinding();
            var old = map.predecessor()
                    .publishedObjects(binding, target.bookKeeper(), objects)
                    .get(target.ledger());
            if (old == null
                    || !old.identity().attemptUuid().equals(target.attempt())
                    || !map.fullyReplaces(target.ledger(), old.lastEntry())
                    || !released.batch().binding().equals(binding.bindingIdentity())) {
                throw new IllegalStateException(
                        "Native offload deletion lacks the original attempt and full replacement");
            }
            long entries = Math.addExact(old.lastEntry(), 1);
            var coverage = M5MaterializationRecordsV1.ProtocolCoverage.pulsar(cursors.coverage()
                    .withRanges(List.of(new M5PulsarCoverageV2.EntryRange(target.ledger(), 0, entries))));
            var identity = java.util.stream.Stream.of(
                            old.identity().root(), old.identity().data())
                    .filter(value -> PulsarNativeOffloadedReferenceV2.resource(selectedValue.providerProfile(), value)
                            .equals(resource))
                    .findFirst()
                    .orElseThrow(() -> new IllegalStateException("Native offload exact version is absent"));
            var body = new ObjectIdentity(
                    identity.key(),
                    identity.length(),
                    Sha256Digest.copyOf(java.util.HexFormat.of().parseHex(identity.sha256())));
            return capturePublishedEvidence(
                            control,
                            tasks,
                            registry,
                            released,
                            target.bookKeeper(),
                            selected,
                            DeleteEligibilitySnapshotV2.ReclamationReason.REPLACED_REPRESENTATION,
                            original,
                            ready,
                            coverage,
                            target.ledger(),
                            entries)
                    .thenApply(eligible -> new DeleteAttempt(original, eligible, body, ready));
        });
    }

    private CompletionStage<DeleteAttempt> captureUnpublished(
            PulsarNativeM4ControlV1 control,
            PulsarNativeMaterializationTaskV2.RetentionRoots tasks,
            M5ClosedWriterRegistryV1 registry,
            M5TargetDeleteAuthorityCoordinatorV1.EpochGraceReady ready) {
        return route.read(resource.authorityKey()).thenCompose(open -> {
            var original = open.orElseThrow();
            var authority = M5TargetDeleteAuthorityCodecV1.decodeAuthority(original.canonicalStoredBytes());
            if (authority.state() != TargetDeleteAuthorityStateV1.OPEN_V1
                    && authority.state() != TargetDeleteAuthorityStateV1.READ_FENCED_V1
                    && authority.state() != TargetDeleteAuthorityStateV1.DELETE_INTENT_V1) {
                throw new IllegalStateException("Native terminal resources use exact terminal reconciliation");
            }
            return control.captureRetentionFloors(
                            scan, cursors, tasks, registry, route.members(), resource, budget, executor)
                    .thenCompose(floors -> control.capturePhysicalReferences(
                                    scan, cursors, tasks, registry, route.members(), resource, budget, executor)
                            .thenCompose(references -> {
                                // The reference capture's original bounded workspace has now drained. Refresh only
                                // the Task producer from its already reread actual reservation facts, with no new I/O
                                // actor.
                                var currentFloors = new ArrayList<>(floors);
                                currentFloors.removeIf(value ->
                                        value.floorClass() == M5RetentionRecordsV1.FloorClassV1.LIFECYCLE_TASK);
                                currentFloors.add(tasks.closedTaskFloor(registry, resource, cursors));
                                var unpublished = tasks.unpublishedOutput(registry, objectResource(), cursors);
                                return control.readSelectedMaterializationSnapshot(executor)
                                        .thenCompose(selected -> {
                                            var selector = selected.selected().selectorFact();
                                            var manifest = tasks.facts().values().stream()
                                                    .filter(value -> value.key()
                                                            .endsWith("/manifests/"
                                                                    + selected.selected()
                                                                            .materializedManifestSha256()
                                                                            .toHex()))
                                                    .findFirst()
                                                    .orElseThrow();
                                            var source = M5PulsarAggregateAuthorityCodecV1.decodeAuthority(
                                                    scan.canonicalStoredBytes());
                                            var configured =
                                                    source.nativeRetention().orElseThrow();
                                            var policy = Sha256Digest.hash(
                                                    CanonicalBytes.copyOf(java.nio.ByteBuffer.allocate(36
                                                                    + configured
                                                                            .encode()
                                                                            .length())
                                                            .putInt(0x4e525031)
                                                            .put(M5PulsarAggregateAuthorityCodecV1.nativeOnlyPolicyRoot(
                                                                            source)
                                                                    .bytes()
                                                                    .toByteArray())
                                                            .put(configured
                                                                    .encode()
                                                                    .toByteArray())
                                                            .array()));
                                            var prior = source.logicalTrim()
                                                    .flatMap(value -> value.frontier())
                                                    .map(value -> value.pulsarContext()
                                                            .orElseThrow()
                                                            .newFrontier())
                                                    .orElse(new M5PulsarCoverageV2.Position(
                                                            cursors.coverage()
                                                                    .chain()
                                                                    .get(0)
                                                                    .ledgerId(),
                                                            0));
                                            var domain = M5MaterializationRecordsV1.PositionDomain.PULSAR_ENTRY;
                                            var identity = unpublished
                                                    .plan()
                                                    .sourceCut()
                                                    .identity();
                                            var floorRequest =
                                                    new M5RetentionEvidenceAssemblerV1.FloorSnapshotRequestV1(
                                                            identity,
                                                            domain,
                                                            Math.addExact(authority.authorityRevision(), 1),
                                                            -1,
                                                            policy,
                                                            fact(selector),
                                                            fact(scan),
                                                            Optional.of(new M5RetentionRecordsV1.PulsarFloorContextV2(
                                                                    cursors.coverage(), prior)));
                                            var floorAdapters = new java.util.EnumMap<
                                                    M5RetentionRecordsV1.FloorClassV1,
                                                    M5RetentionEvidenceAssemblerV1.FloorAdapterV1>(
                                                    M5RetentionRecordsV1.FloorClassV1.class);
                                            currentFloors.forEach(value -> floorAdapters.put(
                                                    value.floorClass(),
                                                    ignored -> CompletableFuture.completedFuture(value)));
                                            var referenceAdapters = new java.util.EnumMap<
                                                    ReferenceKindV1, M5RetentionEvidenceAssemblerV1.ReferenceAdapterV1>(
                                                    ReferenceKindV1.class);
                                            for (var result : references) {
                                                var rows = new ArrayList<>(result.observations());
                                                if (result.referenceKind() == ReferenceKindV1.AUDIT_GRACE) {
                                                    for (var value : auditFacts()) {
                                                        rows.add(new ReferenceObservationV1(
                                                                ReferenceKindV1.AUDIT_GRACE,
                                                                value,
                                                                resource.sha256(),
                                                                M5MaterializationRecordsV1.ProtocolCoverage.pulsar(
                                                                        cursors.coverage()),
                                                                ReferenceDispositionV1.ABSENT,
                                                                true));
                                                    }
                                                    rows.sort(Comparator.comparing(row ->
                                                            row.authority().key()));
                                                }
                                                var updated =
                                                        new M5RetentionEvidenceAssemblerV1.ReferenceAdapterResultV1(
                                                                result.referenceKind(),
                                                                new M5RetentionRecordsV1.ReferenceScanSummaryV1(
                                                                        result.referenceKind(),
                                                                        rows.size(),
                                                                        result.summary()
                                                                                .pageCount(),
                                                                        Math.addExact(
                                                                                result.summary()
                                                                                        .scannedBytes(),
                                                                                result.referenceKind()
                                                                                                == ReferenceKindV1
                                                                                                        .AUDIT_GRACE
                                                                                        ? ownerFact
                                                                                                .canonicalStoredBytes()
                                                                                                .length()
                                                                                        : 0),
                                                                        result.summary()
                                                                                .complete()),
                                                                rows);
                                                referenceAdapters.put(
                                                        result.referenceKind(),
                                                        ignored -> CompletableFuture.completedFuture(updated));
                                            }
                                            var assembler = new M5RetentionEvidenceAssemblerV1(
                                                    new NativeMetadata(), floorAdapters, referenceAdapters);
                                            return assembler
                                                    .buildFloorSnapshot(floorRequest)
                                                    .thenCompose(snapshot -> {
                                                        var request =
                                                                new M5RetentionEvidenceAssemblerV1
                                                                        .ReferenceProofRequestV1(
                                                                        identity,
                                                                        M5RetentionRecordsV1.ReferenceTargetKindV1
                                                                                .UNSELECTED_OUTPUT,
                                                                        resource.sha256(),
                                                                        M5MaterializationRecordsV1.ProtocolCoverage
                                                                                .pulsar(cursors.coverage()),
                                                                        fact(selector),
                                                                        fact(manifest),
                                                                        fact(scan),
                                                                        snapshot.snapshotRootSha256(),
                                                                        java.util.List.of(),
                                                                        fact(selector),
                                                                        fact(selector),
                                                                        fact(scan),
                                                                        context.capability(),
                                                                        0,
                                                                        0,
                                                                        Optional.of(context));
                                                        return assembler
                                                                .buildEpochReferenceFreeProof(request, ready)
                                                                .thenCompose(proof -> createDeleteAttempt(
                                                                        original, ready, unpublished, snapshot, proof));
                                                    });
                                        });
                            }));
        });
    }

    private CompletionStage<DeleteAttempt> createDeleteAttempt(
            VersionedValue original,
            M5TargetDeleteAuthorityCoordinatorV1.EpochGraceReady ready,
            PulsarNativeMaterializationTaskV2.RetentionRoots.UnpublishedOutput unpublished,
            M5RetentionRecordsV1.RetentionFloorSnapshotV1 snapshot,
            M5RetentionRecordsV1.ReferenceFreeProofV1 proof) {
        return route.members()
                .readFact(resource)
                .thenApplyAsync(
                        members -> {
                            var reason = new DeleteEligibilitySnapshotV2.UnpublishedEvidence(
                                    unpublished.plan().taskIdSha256(),
                                    fact(unpublished.terminal()),
                                    fact(scan),
                                    fact(unpublished.pending()),
                                    Optional.of(fact(scan)));
                            var member = new DeleteEligibilitySnapshotV2.MemberSnapshot(
                                    snapshot,
                                    proof,
                                    fact(scan),
                                    DeleteEligibilitySnapshotV2.NativeDisposition.UNPUBLISHED_CLEANUP_ALLOWED,
                                    Optional.empty(),
                                    Optional.empty(),
                                    Optional.of(reason));
                            var eligibility = new DeleteEligibilitySnapshotV2(
                                    resource,
                                    DeleteEligibilitySnapshotV2.ReclamationReason.UNPUBLISHED_ARTIFACT,
                                    snapshot.generation(),
                                    context.capability(),
                                    members.authority(),
                                    java.util.List.of(member));
                            return new DeleteAttempt(original, eligibility, unpublished.object(), ready);
                        },
                        executor);
    }

    private final class DeleteAttempt {
        private final VersionedValue open;
        private final DeleteEligibilitySnapshotV2 snapshot;
        private final M5TargetDeleteAuthorityCoordinatorV1.EpochGraceReady ready;
        private final Sha256Digest attempt;
        private final TargetDeleteAuthorityStateV1 initialState;
        private final S3M5ObjectDeleteIdentityReaderV2 reader;
        private final M5TargetDeleteAuthorityCoordinatorV1 mutations;
        private M5TargetDeleteAuthorityCoordinatorV1.MutationResultV1 refreshResult;
        private VersionedValue qualified;
        private VersionedValue fenced;
        private ExactExternalIdentityV1 external;
        private VersionedValue intent;
        private VersionedValue done;
        private VersionedValue compact;

        private DeleteAttempt(
                VersionedValue open,
                DeleteEligibilitySnapshotV2 snapshot,
                ObjectIdentity object,
                M5TargetDeleteAuthorityCoordinatorV1.EpochGraceReady ready) {
            this.open = open;
            this.snapshot = snapshot;
            this.ready = ready;
            var original = M5TargetDeleteAuthorityCodecV1.decodeAuthority(open.canonicalStoredBytes());
            initialState = original.state();
            attempt = initialState == TargetDeleteAuthorityStateV1.DELETE_INTENT_V1
                    ? original.deleteIntent().orElseThrow().deleteAttemptIdSha256()
                    : initialState == TargetDeleteAuthorityStateV1.READ_FENCED_V1
                            ? original.readFence().orElseThrow().attemptIdSha256()
                            : Sha256Digest.hash(CanonicalUtf8.fromString((snapshot.reason()
                                                            == DeleteEligibilitySnapshotV2.ReclamationReason
                                                                    .UNPUBLISHED_ARTIFACT
                                                    ? "NATIVE_UNPUBLISHED_DELETE_V1:"
                                                    : "NATIVE_REPLACED_OFFLOAD_DELETE_V1:")
                                            + ready.mark().clockDomain())
                                    .bytes());
            try {
                reader = connection
                        .physical()
                        .openDeleteIdentityReader(
                                new NativeMetadata(),
                                profile.scopeId(),
                                profile.exclusiveNamespacePrefix(),
                                object.key().substring(0, object.key().lastIndexOf('/') + 1),
                                object,
                                objectResource().immutableIdentity().bytes(),
                                profile.maximumObjectBytes(),
                                64,
                                65536,
                                4_194_304,
                                1024,
                                1000,
                                executor);
            } catch (java.io.IOException failure) {
                throw new java.util.concurrent.CompletionException(failure);
            }
            mutations =
                    new M5TargetDeleteAuthorityCoordinatorV1(new NativeMetadata(), new NativeOwnerVerifier(), reader);
        }

        private VersionedValue applied(M5TargetDeleteAuthorityCoordinatorV1.MutationResultV1 result, boolean terminal) {
            if (!(terminal ? result.exactTerminalIsAuthoritative() : result.exactCandidateIsAuthoritative())) {
                throw new IllegalStateException(
                        "Original Native delete mutation remains unresolved: " + result.outcome());
            }
            return result.observed().orElseThrow();
        }

        private CompletionStage<VersionedValue> refreshOriginal() {
            if (refreshResult == null) {
                return performRefresh();
            }
            // An applied refresh advances the marked authority. Reconcile this original candidate before
            // asking its timer to validate the old predecessor again; a different value never grants progress.
            return route.read(resource.authorityKey()).thenCompose(stored -> {
                if (stored.filter(value -> value.canonicalStoredBytes().equals(refreshResult.exactCandidate()))
                        .isPresent()) {
                    return ready.requireAdmittedCurrent(resource, ready.mark())
                            .thenApply(ignored -> stored.orElseThrow());
                }
                if (stored.equals(Optional.of(open))) {
                    return performRefresh();
                }
                return CompletableFuture.failedFuture(
                        new IllegalStateException("Original Native refresh candidate remains unresolved or changed"));
            });
        }

        private CompletionStage<VersionedValue> performRefresh() {
            var operation = initialState == TargetDeleteAuthorityStateV1.READ_FENCED_V1
                    ? mutations.refreshIdentityRead(open, context, snapshot, ready)
                    : mutations.refreshDispatch(open, context, snapshot, ready);
            return operation.thenApply(result -> {
                refreshResult = result;
                return applied(result, false);
            });
        }

        private CompletionStage<VersionedValue> run() {
            CompletionStage<Void> work = CompletableFuture.completedFuture(null);
            if (initialState == TargetDeleteAuthorityStateV1.READ_FENCED_V1) {
                work = work.thenCompose(ignored -> fenced != null
                        ? CompletableFuture.completedFuture(null)
                        : refreshOriginal().thenAccept(value -> fenced = value));
            } else if (initialState == TargetDeleteAuthorityStateV1.DELETE_INTENT_V1) {
                work = work.thenCompose(ignored -> intent != null
                        ? CompletableFuture.completedFuture(null)
                        : refreshOriginal().thenAccept(value -> intent = value));
            } else {
                if (qualified == null) {
                    work = work.thenCompose(ignored -> mutations.qualifyEligibility(open, snapshot, ready))
                            .thenAccept(result -> qualified = applied(result, false));
                }
                work = work.thenCompose(ignored -> fenced != null
                        ? CompletableFuture.completedFuture(null)
                        : mutations
                                .prepareIdentityRead(qualified, attempt, context, ready)
                                .thenAccept(result -> fenced = applied(result, false)));
            }
            work = work.thenCompose(ignored -> intent != null || external != null
                    ? CompletableFuture.completedFuture(null)
                    : reader.capture(fenced).thenAccept(value -> external = value));
            work = work.thenCompose(ignored -> intent != null
                    ? CompletableFuture.completedFuture(null)
                    : mutations
                            .bindDeleteIntent(fenced, external, attempt, ready)
                            .thenAccept(result -> intent = applied(result, false)));
            work = work.thenCompose(ignored -> done != null
                    ? CompletableFuture.completedFuture(null)
                    : reader.deletePersistedIntent(intent, new NativeOwnerVerifier())
                            .thenAccept(result -> done = applied(result, true)));
            work = work.thenCompose(ignored -> {
                if (compact != null) {
                    return CompletableFuture.completedFuture(null);
                }
                if (com.nereusstream.storage.object.gc.M5TargetDeleteStoredValueV2.decode(done)
                        .compactDone()
                        .isPresent()) {
                    compact = done;
                    return CompletableFuture.completedFuture(null);
                }
                return mutations.compactDone(done).thenAccept(result -> compact = applied(result, true));
            });
            return work.thenCompose(ignored -> route.quota().settle(resource)).thenApply(result -> {
                if (result != M5GcQuotaCoordinatorV2.Result.SETTLED) {
                    throw new IllegalStateException(
                            "Native compact DONE quota settlement remains unresolved: " + result);
                }
                return compact;
            });
        }
    }

    private <T> CompletableFuture<T> own(Supplier<CompletionStage<T>> operation) {
        return track(operation, false);
    }

    private <T> CompletableFuture<T> io(Supplier<CompletionStage<T>> operation) {
        return track(operation, true);
    }

    private <T> CompletableFuture<T> track(Supplier<CompletionStage<T>> operation, boolean io) {
        var terminal = new CompletableFuture<Void>();
        synchronized (this) {
            requireOpen();
            if (io ? activeIo >= 2 : activeCalls >= 2) {
                return CompletableFuture.failedFuture(
                        new IllegalStateException("Native GC operation capacity is occupied"));
            }
            if (io) {
                activeIo++;
            } else {
                activeCalls++;
            }
            accepted.add(terminal);
        }
        var observer = new CompletableFuture<T>();
        CompletionStage<T> original;
        try {
            original = operations.retain(operation);
        } catch (Throwable failure) {
            original = CompletableFuture.failedFuture(failure);
        }
        original.whenComplete((value, failure) -> {
            synchronized (this) {
                accepted.remove(terminal);
                if (io) {
                    activeIo--;
                } else {
                    activeCalls--;
                }
            }
            terminal.complete(null);
            if (failure == null) {
                observer.complete(value);
            } else {
                observer.completeExceptionally(failure);
            }
        });
        return observer;
    }

    public synchronized CompletableFuture<Void> closeAsync() {
        closed = true;
        if (closing == null || closing.isCompletedExceptionally()) {
            closing = CompletableFuture.allOf(accepted.toArray(CompletableFuture[]::new))
                    .thenCompose(ignored -> connection == null
                            ? CompletableFuture.completedFuture(null)
                            : connection.reconcileScopeForClose())
                    .thenRun(() -> {
                        if (deletion != null) {
                            deletion.reader.close();
                        }
                        if (connection != null) {
                            connection.close();
                        }
                    });
        }
        return closing.copy();
    }

    @Override
    public void close() {
        closeAsync().join();
    }
}
