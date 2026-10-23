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

package com.nereusstream.storage.bookkeeper;

import com.nereusstream.domain.bytes.CanonicalBytes;
import com.nereusstream.domain.bytes.Sha256Digest;
import com.nereusstream.storage.api.bookkeeper.BookKeeperCapabilitySnapshotV1;
import com.nereusstream.storage.api.bookkeeper.ExactLedgerEntryV1;
import com.nereusstream.storage.api.bookkeeper.RunLedgerConfigurationV1;
import com.nereusstream.storage.api.bookkeeper.RunLedgerHandleV1;
import com.nereusstream.storage.api.bookkeeper.RunLedgerRecoveryProofV1;
import com.nereusstream.storage.api.lifecycle.PhysicalNamespaceAuthorityBindingV2;
import java.net.URI;
import java.nio.charset.StandardCharsets;
import java.util.Base64;
import java.util.Objects;
import java.util.Optional;
import java.util.Set;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.CompletionStage;
import java.util.concurrent.ConcurrentHashMap;
import org.apache.bookkeeper.client.BookKeeper;
import org.apache.bookkeeper.meta.zk.ZKMetadataClientDriver;

/** Owns the guarded BK client. Create fencing grants no append, publication, cleanup or delete authority. */
public final class M5BookKeeperNativeCreateClientV2 implements AutoCloseable {
    private final BookKeeper client;
    private final BookKeeperCapabilitySnapshotV1 capability;
    private final M5BookKeeperNativeCreateSpecV2 spec;
    private final Set<RealBookKeeperCellSessionV1> headerReaders = ConcurrentHashMap.newKeySet();
    private boolean headerReadsDraining;

    private M5BookKeeperNativeCreateClientV2(
            BookKeeper client, BookKeeperCapabilitySnapshotV1 capability, M5BookKeeperNativeCreateSpecV2 spec) {
        this.client = client;
        this.capability = capability;
        this.spec = spec;
    }

    public static String discoverInstanceId(String metadataServiceUri, BookKeeperCapabilitySnapshotV1 capability)
            throws Exception {
        var configuration = RealBookKeeperClientConfigurationV1.from(metadataServiceUri, capability);
        try (var owned = (BookKeeper) org.apache.bookkeeper.client.api.BookKeeper.newBuilder(configuration)
                .build()) {
            var driver = (ZKMetadataClientDriver) owned.getMetadataClientDriver();
            var instance = new String(
                    driver.getZk().getData(URI.create(metadataServiceUri).getPath() + "/INSTANCEID", false, null),
                    StandardCharsets.US_ASCII);
            M5BookKeeperNativeCreateSpecV2.namespace(instance);
            return instance;
        }
    }

    public static M5BookKeeperNativeCreateClientV2 connect(
            String metadataServiceUri, BookKeeperCapabilitySnapshotV1 capability, M5BookKeeperNativeCreateSpecV2 spec)
            throws Exception {
        return connect(metadataServiceUri, capability, spec, Optional.empty());
    }

    public static M5BookKeeperNativeCreateClientV2 connect(
            String metadataServiceUri,
            BookKeeperCapabilitySnapshotV1 capability,
            M5BookKeeperNativeCreateSpecV2 spec,
            PhysicalNamespaceAuthorityBindingV2 binding)
            throws Exception {
        return connect(metadataServiceUri, capability, spec, Optional.of(binding));
    }

    private static M5BookKeeperNativeCreateClientV2 connect(
            String metadataServiceUri,
            BookKeeperCapabilitySnapshotV1 capability,
            M5BookKeeperNativeCreateSpecV2 spec,
            Optional<PhysicalNamespaceAuthorityBindingV2> binding)
            throws Exception {
        var configuration = RealBookKeeperClientConfigurationV1.from(metadataServiceUri, capability);
        if (!capability.credentialIdentityVersion().equals("bk-k0-no-auth:v1")
                || spec.configurations().stream()
                        .anyMatch(run -> !run.providerScopeId().equals(capability.providerScopeId())
                                || !run.configurationDigest().equals(capability.configurationDigest())
                                || run.ensembleSize() != capability.ensembleSize()
                                || run.writeQuorumSize() != capability.writeQuorumSize()
                                || run.ackQuorumSize() != capability.ackQuorumSize()
                                || run.digestType() != capability.digestType())) {
            throw new IllegalArgumentException("native M5 create scope differs from the admitted no-auth capability");
        }
        binding.ifPresent(value -> {
            if (!value.physicalNamespace().equals(spec.namespace())) {
                throw new IllegalArgumentException("native create binding has another physical namespace");
            }
            configuration.setProperty(
                    M5BookKeeperNativeMetadataDriverV2.NAMESPACE_BINDING_PROPERTY,
                    Base64.getEncoder().encodeToString(value.encode().toByteArray()));
        });
        M5BookKeeperNativeMetadataDriverV2.register();
        configuration.setProperty(
                M5BookKeeperNativeMetadataDriverV2.SPEC_PROPERTY,
                Base64.getEncoder().encodeToString(spec.encode().toByteArray()));
        configuration.setMetadataServiceUri(
                M5BookKeeperNativeMetadataDriverV2.DRIVER_SCHEME + metadataServiceUri.substring(2));
        var client = (BookKeeper) org.apache.bookkeeper.client.api.BookKeeper.newBuilder(configuration)
                .build();
        if (!(client.getMetadataClientDriver() instanceof M5BookKeeperNativeMetadataDriverV2)) {
            client.close();
            throw new IllegalStateException("native M5 scheme resolved to an unguarded metadata driver");
        }
        return new M5BookKeeperNativeCreateClientV2(client, capability, spec);
    }

    public M5BookKeeperNativeCreateSpecV2 spec() {
        return driver().createSpec();
    }

    /** Advances the exact guarded create scope, retaining one current run and its permanent native fence. */
    public M5BookKeeperNativeCreateSpecV2 admitCreateConfiguration(RunLedgerConfigurationV1 configuration)
            throws Exception {
        if (!configuration.equals(RunLedgerConfigurationV1.from(capability, configuration.runId()))) {
            throw new IllegalArgumentException("new run changes the admitted capability");
        }
        var next = M5BookKeeperNativeCreateSpecV2.laterRun(spec, configuration);
        driver().activateCreateScope(next);
        return next;
    }

    public BookKeeperCapabilitySnapshotV1 capabilitySnapshot() {
        return capability;
    }

    /** Reads the permanent assignment through this same owned connection; an unbound client cannot qualify. */
    public CompletionStage<PhysicalNamespaceAuthorityBindingV2> requireNamespaceBinding() {
        return driver().guard().requireNamespaceBinding();
    }

    public RealBookKeeperCellSessionV1 newSession() {
        return new RealBookKeeperCellSessionV1(client, capability, new byte[0]);
    }

    /** Native delete fencing is a low-level primitive; the M5 coordinator must supply current intent/eligibility. */
    public M5BookKeeperNativeDeleteAuthorityV2 deleteAuthority(RunLedgerHandleV1 handle) {
        return deleteAuthority(handle, () -> CompletableFuture.completedFuture(null));
    }

    M5BookKeeperNativeDeleteAuthorityV2 deleteAuthority(
            RunLedgerHandleV1 handle, java.util.function.Supplier<CompletionStage<Void>> beforeDelete) {
        var configuration = RunLedgerConfigurationV1.from(capability, handle.runId());
        if (!spec.configurations().contains(configuration)
                || !handle.providerScopeId().equals(capability.providerScopeId())
                || !handle.configurationDigest().equals(capability.configurationDigest())
                || !(client.getLedgerManager() instanceof org.apache.bookkeeper.meta.CleanupLedgerManager cleanup)
                || !(cleanup.getUnderlying() instanceof M5BookKeeperNativeLedgerManagerV2 manager)) {
            throw new IllegalArgumentException("native delete handle/manager differs from the admitted scope");
        }
        var driver = driver();
        return new M5BookKeeperNativeDeleteAuthorityV2(
                driver.getZk(),
                manager,
                driver.guard(),
                driver.nativeAcls(),
                handle,
                spec.namespace(),
                capability,
                beforeDelete);
    }

    /** Captures sealed native metadata through this same owned connection and admitted capability. */
    public java.util.concurrent.CompletionStage<M5BookKeeperDeleteAdapterV1.CaptureResult> captureExactTarget(
            com.nereusstream.storage.api.bookkeeper.RunLedgerHandleV1 handle) {
        return new M5BookKeeperDeleteAdapterV1(client, capability, new byte[0]).captureExactTarget(handle);
    }

    /**
     * Observes only entry zero without fencing or a reader-LAC requirement. A newly quorum-written header can still
     * have reader LAC -1. These exact bytes are not an ACK/quorum or protocol-owner proof; the lifecycle retains those
     * admission obligations. Every call validates native run metadata and closes its independent read handle.
     */
    public CompletionStage<ExactLedgerEntryV1> readNativeRunHeader(RunLedgerHandleV1 handle) {
        if (!handle.providerScopeId().equals(capability.providerScopeId())
                || !handle.configurationDigest().equals(capability.configurationDigest())) {
            return CompletableFuture.failedFuture(
                    new IllegalArgumentException("header handle is outside native scope"));
        }
        final RealBookKeeperCellSessionV1 reader;
        synchronized (this) {
            if (headerReadsDraining) {
                return CompletableFuture.failedFuture(new IllegalStateException("native header owner is draining"));
            }
            reader = newSession();
            headerReaders.add(reader);
        }
        var terminal = new CompletableFuture<ExactLedgerEntryV1>();
        CompletionStage<ExactLedgerEntryV1> read;
        try {
            read = reader.readNativeRunHeader(handle);
        } catch (Throwable failure) {
            read = CompletableFuture.failedFuture(failure);
        }
        read.whenComplete((entry, failure) -> closeHeaderReader(reader).whenComplete((ignored, closeFailure) -> {
            if (closeFailure != null) {
                if (failure != null && failure != closeFailure) {
                    closeFailure.addSuppressed(failure);
                }
                terminal.completeExceptionally(closeFailure);
            } else if (failure != null) {
                terminal.completeExceptionally(failure);
            } else {
                terminal.complete(entry);
            }
        }));
        return terminal.thenApply(entry -> entry);
    }

    private CompletionStage<Void> closeHeaderReader(RealBookKeeperCellSessionV1 reader) {
        try {
            return reader.closeAsync().thenRun(() -> headerReaders.remove(reader));
        } catch (Throwable failure) {
            return CompletableFuture.failedFuture(failure);
        }
    }

    public CompletableFuture<Void> fenceCreates() {
        return driver().fenceCreates();
    }

    /** Exact existing-task fence only; protocol closure and delete eligibility must be established separately. */
    public CompletionStage<Void> fenceExistingScope(M5BookKeeperNativeCreateSpecV2 expected, RunLedgerHandleV1 handle) {
        return driver().guard().fenceExistingScope(expected, handle, capability);
    }

    /** Raw bytes and native version fields from one getData response; timestamps grant no grace authority. */
    public record NativeRecordFact(
            String nativePath,
            CanonicalBytes rawStoredBytes,
            long nativeCreateZxid,
            long nativeModifyZxid,
            int nativeVersion,
            long nativeCreatedAtMillis) {
        public NativeRecordFact {
            Objects.requireNonNull(nativePath, "nativePath");
            Objects.requireNonNull(rawStoredBytes, "rawStoredBytes");
            if (!nativePath.startsWith("/") || rawStoredBytes.isEmpty() || nativeVersion < 0) {
                throw new IllegalArgumentException("native record path/bytes/version differ");
            }
        }

        public Sha256Digest rawStoredSha256() {
            return Sha256Digest.hash(rawStoredBytes);
        }
    }

    /** Actual native create admission; a fenced create scope alone is not SDK/read termination. */
    public record TaskCreateScopeFact(
            PhysicalNamespaceAuthorityBindingV2 namespaceBinding,
            M5BookKeeperNativeCreateSpecV2 createScope,
            NativeRecordFact instanceIdentity,
            NativeRecordFact taskFence) {
        public TaskCreateScopeFact {
            Objects.requireNonNull(namespaceBinding, "namespaceBinding");
            Objects.requireNonNull(createScope, "createScope");
            Objects.requireNonNull(instanceIdentity, "instanceIdentity");
            Objects.requireNonNull(taskFence, "taskFence");
        }

        public boolean fenced() {
            return taskFence.nativeVersion() == 1;
        }
    }

    /** Read-only full Task observation under the existing bound connection, followed by exact rereads. */
    public CompletionStage<TaskCreateScopeFact> readTaskCreateScopeFact(M5BookKeeperNativeCreateSpecV2 expected) {
        var guard = driver().guard();
        return guard.requireNamespaceBinding()
                .thenCompose(binding -> guard.readInstanceFact()
                        .thenCompose(instance -> guard.readTaskCreateScopeFact(expected, capability)
                                .thenApply(fence -> new TaskCreateScopeFact(binding, expected, instance, fence))))
                .thenCompose(fact -> guard.readTaskCreateScopeFact(expected, capability)
                        .thenCompose(fence -> {
                            if (!fence.equals(fact.taskFence())) {
                                return CompletableFuture.failedFuture(new IllegalStateException(
                                        "native Task create fence changed during fact capture"));
                            }
                            return guard.readInstanceFact().thenCompose(instance -> {
                                if (!instance.equals(fact.instanceIdentity())) {
                                    return CompletableFuture.failedFuture(new IllegalStateException(
                                            "native Task instance changed during fact capture"));
                                }
                                return guard.requireNamespaceBinding().thenApply(binding -> {
                                    if (!binding.equals(fact.namespaceBinding())) {
                                        throw new IllegalStateException(
                                                "native Task namespace changed during fact capture");
                                    }
                                    return fact;
                                });
                            });
                        }));
    }

    /** Permanent original create fence/reservation, readable even after exact ledger deletion. */
    public record FencedLedgerScopeFacts(
            TaskCreateScopeFact scope, RunLedgerHandleV1 handle, NativeRecordFact reservation) {
        public FencedLedgerScopeFacts {
            Objects.requireNonNull(scope, "scope");
            Objects.requireNonNull(handle, "handle");
            Objects.requireNonNull(reservation, "reservation");
            if (!scope.fenced()) {
                throw new IllegalArgumentException("native ledger scope lacks its permanent create fence");
            }
        }
    }

    /** Reads permanent admission only; absence of ledger metadata is not inferred or manufactured here. */
    public CompletionStage<FencedLedgerScopeFacts> readFencedLedgerScopeFacts(
            M5BookKeeperNativeCreateSpecV2 expected, RunLedgerHandleV1 handle) {
        return captureFencedLedgerScopeFacts(expected, handle).thenCompose(first -> captureFencedLedgerScopeFacts(
                        expected, handle)
                .thenApply(current -> {
                    if (!first.equals(current)) {
                        throw new IllegalStateException("native fenced ledger scope changed during fact capture");
                    }
                    return first;
                }));
    }

    private CompletionStage<FencedLedgerScopeFacts> captureFencedLedgerScopeFacts(
            M5BookKeeperNativeCreateSpecV2 expected, RunLedgerHandleV1 handle) {
        return readTaskCreateScopeFact(expected).thenCompose(scope -> driver().guard()
                .readSealedExistingScope(expected, handle, capability)
                .thenApply(fenced -> {
                    if (!scope.taskFence().equals(fenced.taskFence())) {
                        throw new IllegalStateException("native ledger scope differs from its actual Task fence");
                    }
                    return new FencedLedgerScopeFacts(scope, handle, fenced.reservation());
                }));
    }

    /** Exact post-recovery source observations; this value is not task, read-protection or delete eligibility. */
    public record RecoveredScopeFacts(
            PhysicalNamespaceAuthorityBindingV2 namespaceBinding,
            M5BookKeeperNativeCreateSpecV2 createScope,
            RunLedgerRecoveryProofV1 recovery,
            NativeRecordFact instanceIdentity,
            NativeRecordFact taskFence,
            NativeRecordFact reservation,
            NativeRecordFact ledgerMetadata,
            M5BookKeeperDeleteAdapterV1.BookKeeperDeleteTargetV1 closedMetadata) {
        public RecoveredScopeFacts {
            Objects.requireNonNull(namespaceBinding, "namespaceBinding");
            Objects.requireNonNull(createScope, "createScope");
            Objects.requireNonNull(recovery, "recovery");
            Objects.requireNonNull(instanceIdentity, "instanceIdentity");
            Objects.requireNonNull(taskFence, "taskFence");
            Objects.requireNonNull(reservation, "reservation");
            Objects.requireNonNull(ledgerMetadata, "ledgerMetadata");
            Objects.requireNonNull(closedMetadata, "closedMetadata");
        }
    }

    /** Reads only a physically present recovered birth; absence or uncertainty must retain its old writer ticket. */
    public CompletionStage<RecoveredScopeFacts> readRecoveredScopeFacts(
            M5BookKeeperNativeCreateSpecV2 expected, RunLedgerRecoveryProofV1 proof) {
        Objects.requireNonNull(proof, "proof");
        var guard = driver().guard();
        if (!(client.getLedgerManager() instanceof org.apache.bookkeeper.meta.CleanupLedgerManager cleanup)
                || !(cleanup.getUnderlying() instanceof M5BookKeeperNativeLedgerManagerV2 manager)) {
            return CompletableFuture.failedFuture(new IllegalStateException("native ledger manager differs"));
        }
        String path = manager.nativeLedgerPath(proof.handle().ledgerIdentity().ledgerId());
        return guard.readInstanceFact().thenCompose(instance -> guard.requireNamespaceBinding()
                .thenCompose(binding -> guard.readFencedExistingScope(expected, proof.handle(), capability)
                        .thenCompose(scope -> guard.readNativeRecord(path)
                                .thenApply(nativeValue -> new RecoveredScopeFacts(
                                        binding,
                                        expected,
                                        proof,
                                        instance,
                                        scope.taskFence(),
                                        scope.reservation(),
                                        nativeValue,
                                        requireRecoveredMetadata(nativeValue, proof))))));
    }

    /** Actual sealed identity under the owned native connection; it grants no writer, reference or delete closure. */
    public record SealedScopeFacts(
            PhysicalNamespaceAuthorityBindingV2 namespaceBinding,
            M5BookKeeperNativeCreateSpecV2 createScope,
            RunLedgerHandleV1 handle,
            NativeRecordFact instanceIdentity,
            NativeRecordFact taskFence,
            NativeRecordFact reservation,
            NativeRecordFact ledgerMetadata,
            M5BookKeeperDeleteAdapterV1.BookKeeperDeleteTargetV1 closedMetadata) {
        public SealedScopeFacts {
            Objects.requireNonNull(namespaceBinding, "namespaceBinding");
            Objects.requireNonNull(createScope, "createScope");
            Objects.requireNonNull(handle, "handle");
            Objects.requireNonNull(instanceIdentity, "instanceIdentity");
            Objects.requireNonNull(taskFence, "taskFence");
            Objects.requireNonNull(reservation, "reservation");
            Objects.requireNonNull(ledgerMetadata, "ledgerMetadata");
            Objects.requireNonNull(closedMetadata, "closedMetadata");
        }
    }

    /** Exact read-only native scope and CLOSED metadata, including an exact OPEN creating Task. */
    public CompletionStage<SealedScopeFacts> readSealedScopeFacts(
            M5BookKeeperNativeCreateSpecV2 expected, RunLedgerHandleV1 handle) {
        Objects.requireNonNull(expected, "expected");
        Objects.requireNonNull(handle, "handle");
        return captureSealedScopeFacts(expected, handle)
                .thenCompose(first -> captureSealedScopeFacts(expected, handle).thenApply(current -> {
                    if (!current.equals(first)) {
                        throw new IllegalStateException("sealed native scope changed during fact capture");
                    }
                    return first;
                }));
    }

    private CompletionStage<SealedScopeFacts> captureSealedScopeFacts(
            M5BookKeeperNativeCreateSpecV2 expected, RunLedgerHandleV1 handle) {
        var guard = driver().guard();
        if (!(client.getLedgerManager() instanceof org.apache.bookkeeper.meta.CleanupLedgerManager cleanup)
                || !(cleanup.getUnderlying() instanceof M5BookKeeperNativeLedgerManagerV2 manager)) {
            return CompletableFuture.failedFuture(new IllegalStateException("native ledger manager differs"));
        }
        String path = manager.nativeLedgerPath(handle.ledgerIdentity().ledgerId());
        return guard.readInstanceFact().thenCompose(instance -> guard.requireNamespaceBinding()
                .thenCompose(binding -> guard.readSealedExistingScope(expected, handle, capability)
                        .thenCompose(scope -> guard.readNativeRecord(path)
                                .thenApply(nativeValue -> new SealedScopeFacts(
                                        binding,
                                        expected,
                                        handle,
                                        instance,
                                        scope.taskFence(),
                                        scope.reservation(),
                                        nativeValue,
                                        requireClosedMetadata(nativeValue, handle))))));
    }

    private M5BookKeeperDeleteAdapterV1.BookKeeperDeleteTargetV1 requireRecoveredMetadata(
            NativeRecordFact nativeValue, RunLedgerRecoveryProofV1 proof) {
        var closed = requireClosedMetadata(nativeValue, proof.handle());
        if (closed.sealedLastEntryId() != proof.lastAddConfirmed()) {
            throw new IllegalStateException("recovered native ledger LAC differs from recovery proof");
        }
        return closed;
    }

    private M5BookKeeperDeleteAdapterV1.BookKeeperDeleteTargetV1 requireClosedMetadata(
            NativeRecordFact nativeValue, RunLedgerHandleV1 handle) {
        final org.apache.bookkeeper.client.api.LedgerMetadata metadata;
        try {
            metadata = new org.apache.bookkeeper.meta.LedgerMetadataSerDe()
                    .parseConfig(
                            nativeValue.rawStoredBytes().toByteArray(),
                            handle.ledgerIdentity().ledgerId(),
                            Optional.of(nativeValue.nativeCreatedAtMillis()));
        } catch (java.io.IOException failure) {
            throw new IllegalStateException("native ledger metadata cannot be parsed", failure);
        }
        return M5BookKeeperDeleteAdapterV1.exactTarget(metadata, handle, capability, new byte[0])
                .orElseThrow(() -> new IllegalStateException("native ledger is not the exact closed run"));
    }

    M5BookKeeperNativeMetadataDriverV2 driver() {
        return (M5BookKeeperNativeMetadataDriverV2) client.getMetadataClientDriver();
    }

    BookKeeper nativeClient() {
        return client;
    }

    @Override
    public void close() throws Exception {
        final java.util.List<RealBookKeeperCellSessionV1> readers;
        synchronized (this) {
            headerReadsDraining = true;
            readers = java.util.List.copyOf(headerReaders);
        }
        CompletableFuture.allOf(readers.stream()
                        .map(reader -> closeHeaderReader(reader).toCompletableFuture())
                        .toArray(CompletableFuture[]::new))
                .join();
        client.close();
    }
}
