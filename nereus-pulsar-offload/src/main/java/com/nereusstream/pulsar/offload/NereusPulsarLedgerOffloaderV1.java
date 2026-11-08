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

import com.nereusstream.pulsar.offload.PulsarObjectReadHandleV1.ObjectReadException;
import com.nereusstream.pulsar.offload.PulsarSealedLedgerAttemptV1.DeleteState;
import com.nereusstream.pulsar.offload.PulsarSealedLedgerPublisherV1.PreparedAttempt;
import com.nereusstream.pulsar.offload.npd1.Npd1CodecV1;
import com.nereusstream.pulsar.offload.npd1.Npd1CodecV1.CompressionPolicy;
import com.nereusstream.pulsar.offload.npd1.Npd1CodecV1.DataObject;
import com.nereusstream.pulsar.offload.npd1.Npd1CodecV1.EntryPayload;
import com.nereusstream.pulsar.offload.npd1.Npd1CodecV1.StreamingEncoder;
import com.nereusstream.pulsar.offload.npo1.Npo1CodecV1.AttemptKeyEnvelope;
import com.nereusstream.pulsar.offload.npo1.Npo1CodecV1.CustomMetadataValue;
import com.nereusstream.pulsar.offload.npo1.Npo1CodecV1.DigestType;
import com.nereusstream.pulsar.offload.npo1.Npo1CodecV1.EnsembleSegment;
import com.nereusstream.pulsar.offload.npo1.Npo1CodecV1.SealedLedgerSection;
import com.nereusstream.storage.api.lifecycle.MetadataNamespaceIdentityV2;
import com.nereusstream.storage.api.lifecycle.PhysicalNamespaceAuthorityBindingV2;
import com.nereusstream.storage.api.lifecycle.PhysicalResourceIdV2.Namespace;
import java.io.IOException;
import java.nio.file.Files;
import java.nio.file.Path;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.Objects;
import java.util.UUID;
import java.util.concurrent.CancellationException;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.CompletionException;
import java.util.concurrent.CompletionStage;
import java.util.concurrent.Executor;
import java.util.concurrent.atomic.AtomicBoolean;
import java.util.function.BooleanSupplier;
import java.util.function.Supplier;
import javax.crypto.SecretKey;
import org.apache.bookkeeper.client.api.LedgerEntries;
import org.apache.bookkeeper.client.api.LedgerEntry;
import org.apache.bookkeeper.client.api.LedgerMetadata;
import org.apache.bookkeeper.client.api.ReadHandle;
import org.apache.bookkeeper.mledger.SourceSafeLedgerOffloader;
import org.apache.pulsar.common.policies.data.OffloadPolicies;

/** Native Pulsar SourceSafeLedgerOffloader for one admitted NPD1/NPO1 runtime policy. */
public final class NereusPulsarLedgerOffloaderV1 implements SourceSafeLedgerOffloader {
    public static final String DRIVER_NAME = "nereus-v2-npd1-npo1";
    public static final String METADATA_POLICY_ID = "nereus.policyId";
    public static final String METADATA_PROVIDER_SCOPE = "nereus.providerScope";
    public static final String METADATA_KEY_DERIVATION_VERSION = "nereus.keyDerivationVersion";
    public static final String METADATA_RETENTION = "nereus.retention";
    public static final String METADATA_BLOCK_CLASS = "nereus.blockClass";
    public static final String METADATA_BLOCK_TARGET = "nereus.blockTargetBytes";
    public static final String METADATA_COMPRESSION_POLICY = "nereus.compressionPolicy";

    public interface AttemptKeyManager {
        PreparedAttemptKey create(long ledgerId, UUID attemptUuid, Map<String, String> persistedDriverMetadata);

        SecretKey unwrap(
                long ledgerId,
                UUID attemptUuid,
                AttemptKeyEnvelope envelope,
                Map<String, String> persistedDriverMetadata);
    }

    public record PreparedAttemptKey(SecretKey plaintextKey, AttemptKeyEnvelope envelope) {
        public PreparedAttemptKey {
            Objects.requireNonNull(plaintextKey, "plaintextKey");
            Objects.requireNonNull(envelope, "envelope");
            byte[] encoded = plaintextKey.getEncoded();
            if (!"AES".equals(plaintextKey.getAlgorithm()) || encoded == null || encoded.length != 32) {
                throw new IllegalArgumentException("attempt key must be one encodable AES-256 key");
            }
        }
    }

    @FunctionalInterface
    public interface IntegrityFailureObserver {
        void record(long ledgerId, UUID attemptUuid, Throwable failure);
    }

    public record RuntimePolicy(
            String policyId,
            String providerScopePrefix,
            PulsarSealedLedgerAttemptV1.RetentionClass retentionClass,
            PulsarOffloadBlockPolicyV1.BlockClass blockClass,
            CompressionPolicy compressionPolicy,
            PulsarOffloadLimitCandidateV1 limits) {
        public RuntimePolicy {
            Objects.requireNonNull(policyId, "policyId");
            Objects.requireNonNull(providerScopePrefix, "providerScopePrefix");
            Objects.requireNonNull(retentionClass, "retentionClass");
            Objects.requireNonNull(blockClass, "blockClass");
            Objects.requireNonNull(compressionPolicy, "compressionPolicy");
            Objects.requireNonNull(limits, "limits");
            if (!policyId.matches("[a-z0-9][a-z0-9._-]{0,63}")
                    || !limits.blockTargetBytes().contains(blockClass.targetBytes())) {
                throw new IllegalArgumentException("runtime policy identity or block target is not admitted");
            }
            PulsarOffloadKeysV1.derive(providerScopePrefix, 0, new UUID(0, 0));
        }

        Map<String, String> driverMetadata() {
            return Map.of(
                    METADATA_POLICY_ID, policyId,
                    METADATA_PROVIDER_SCOPE, providerScopePrefix,
                    METADATA_KEY_DERIVATION_VERSION, Integer.toString(PulsarOffloadKeysV1.KEY_DERIVATION_VERSION),
                    METADATA_RETENTION, retentionClass.name(),
                    METADATA_BLOCK_CLASS, blockClass.id(),
                    METADATA_BLOCK_TARGET, Integer.toString(blockClass.targetBytes()),
                    METADATA_COMPRESSION_POLICY, compressionPolicy.name());
        }
    }

    Map<String, String> readAdmissionConfiguration() {
        if (!(objectStore instanceof S3PulsarOffloadObjectStoreV1 store)) {
            throw new IllegalStateException("Native M4 admission requires its actual qualified S3 adapter");
        }
        var values = new java.util.TreeMap<String, String>(store.readAdmissionConfiguration());
        values.putAll(policy.driverMetadata());
        values.put("limits", policy.limits().toString());
        values.put("capabilities", objectStore.capabilities().toString());
        return Map.copyOf(values);
    }

    private final PulsarOffloadObjectStoreV1 objectStore;
    private final RuntimePolicy policy;
    private final AttemptKeyManager keyManager;
    private final IntegrityFailureObserver integrityObserver;
    private final OffloadPolicies offloadPolicies;
    private final Executor fileExecutor;
    private final Path stagingDirectory;
    private final PulsarSealedLedgerPublisherV1 publisher;
    private final AtomicBoolean closed = new AtomicBoolean();
    private final Object lifetimeLock = new Object();
    private final CompletableFuture<Void> drained = new CompletableFuture<>();
    private volatile PulsarNativeOffloadBirthAdmissionV2 enrolledOutput;
    private int activeOperations;

    public NereusPulsarLedgerOffloaderV1(
            PulsarOffloadObjectStoreV1 objectStore,
            RuntimePolicy policy,
            AttemptKeyManager keyManager,
            IntegrityFailureObserver integrityObserver,
            OffloadPolicies offloadPolicies,
            Executor fileExecutor,
            Path stagingDirectory) {
        this.objectStore = Objects.requireNonNull(objectStore, "objectStore");
        this.policy = Objects.requireNonNull(policy, "policy");
        this.keyManager = Objects.requireNonNull(keyManager, "keyManager");
        this.integrityObserver = Objects.requireNonNull(integrityObserver, "integrityObserver");
        this.offloadPolicies = Objects.requireNonNull(offloadPolicies, "offloadPolicies");
        this.fileExecutor = Objects.requireNonNull(fileExecutor, "fileExecutor");
        this.stagingDirectory = Objects.requireNonNull(stagingDirectory, "stagingDirectory");
        PulsarOffloadProfileAdmissionV1.requireAdmitted(policy.limits(), objectStore.capabilities());
        this.publisher = new PulsarSealedLedgerPublisherV1(
                objectStore,
                policy.limits(),
                new PulsarPublishedAttemptVerifierV1(
                        objectStore,
                        policy.limits(),
                        (attempt, envelope) -> keyManager.unwrap(
                                attempt.ledgerId(), attempt.attemptUuid(), envelope, policy.driverMetadata())),
                fileExecutor);
    }

    @Override
    public String getOffloadDriverName() {
        return DRIVER_NAME;
    }

    @Override
    public Map<String, String> getOffloadDriverMetadata() {
        return policy.driverMetadata();
    }

    /** Implements the enrolled Native SPI; the pinned historical M5 compile API predates this method. */
    public boolean preservesIncompleteAttempt() {
        return enrolledOutput != null;
    }

    CompletableFuture<Void> drainOriginalOutput() {
        var original = enrolledOutput;
        return original == null ? CompletableFuture.completedFuture(null) : original.drain();
    }

    void enableEnrolledOutput(
            PulsarNativeBindingV1 binding,
            PulsarNativeReadCellBudgetV1 budget,
            S3PulsarOffloadObjectStoreV1.MaterializationConnection connection,
            com.nereusstream.metadata.oxia.v2.retention.OxiaQuotaTargetDeleteStoreV2 route,
            com.nereusstream.storage.object.retention.M5ClosedWriterRegistryV1 writers,
            Supplier<CompletionStage<Void>> requireNativeOwnership) {
        synchronized (lifetimeLock) {
            if (closed.get() || enrolledOutput != null) {
                throw new IllegalStateException("Native offloader already has an enrolled output owner or is closed");
            }
            enrolledOutput = new PulsarNativeOffloadBirthAdmissionV2(
                    binding,
                    budget,
                    connection,
                    route,
                    objectStore,
                    writers,
                    binding.nativeOffloadAttempts(),
                    requireNativeOwnership);
        }
    }

    S3PulsarOffloadObjectStoreV1.MaterializationConnection borrowMaterializationConnection(
            Namespace expected,
            MetadataNamespaceIdentityV2 metadataNamespace,
            com.nereusstream.storage.api.bookkeeper.CellProviderScopeId scope,
            long maximumObjectBytes) {
        synchronized (lifetimeLock) {
            requireOpen();
            activeOperations++;
        }
        boolean delivered = false;
        try {
            if (!(objectStore instanceof S3PulsarOffloadObjectStoreV1 store)) {
                throw new IllegalStateException("Native materialization requires the original qualified S3 connection");
            }
            var connection = store.borrowMaterializationConnection(
                    expected,
                    metadataNamespace,
                    scope,
                    policy.providerScopePrefix(),
                    maximumObjectBytes,
                    this::operationFinished);
            if (closed.get()) {
                connection.close();
                delivered = true; // The connection already returned this original offloader hold.
                throw new IllegalStateException("Native offloader closed during materialization admission");
            }
            delivered = true;
            return connection;
        } finally {
            if (!delivered) {
                operationFinished();
            }
        }
    }

    S3PulsarOffloadObjectStoreV1.MaterializationConnection borrowConfiguredMaterializationConnection(
            PulsarNativeBindingV1 binding, Namespace expected, long maximumObjectBytes) {
        synchronized (lifetimeLock) {
            requireOpen();
            activeOperations++;
        }
        boolean delivered = false;
        try {
            if (!(objectStore instanceof S3PulsarOffloadObjectStoreV1 store)) {
                throw new IllegalStateException(
                        "Native materialization requires its original configured S3 connection");
            }
            var connection = store.borrowConfiguredMaterializationConnection(
                    expected,
                    binding.metadataNamespace(),
                    binding.authority().aggregate().binding().cellIdentity(),
                    policy.providerScopePrefix(),
                    maximumObjectBytes,
                    this::operationFinished);
            if (closed.get()) {
                connection.close();
                delivered = true;
                throw new IllegalStateException("Native offloader closed during configured materialization admission");
            }
            delivered = true;
            return connection;
        } finally {
            if (!delivered) {
                operationFinished();
            }
        }
    }

    @Override
    public CompletableFuture<Void> offload(ReadHandle ledger, UUID attemptUuid, Map<String, String> extraMetadata) {
        return admittedOperation(() -> offloadLedger(ledger, attemptUuid, extraMetadata));
    }

    private CompletableFuture<Void> offloadLedger(
            ReadHandle ledger, UUID attemptUuid, Map<String, String> extraMetadata) {
        PulsarSealedLedgerAttemptV1 attempt;
        SealedLedgerSection sealedLedger;
        try {
            Objects.requireNonNull(ledger, "ledger");
            Objects.requireNonNull(attemptUuid, "attemptUuid");
            Objects.requireNonNull(extraMetadata, "extraMetadata");
            if (!ledger.isClosed()
                    || ledger.getLastAddConfirmed() < 0
                    || ledger.getLedgerMetadata().getLastEntryId() != ledger.getLastAddConfirmed()
                    || ledger.getLength() != ledger.getLedgerMetadata().getLength()
                    || !extraMetadata.containsKey("ManagedLedgerName")) {
                throw new IllegalArgumentException("offload input is not one exact sealed native ledger");
            }
            LedgerMetadata metadata = ledger.getLedgerMetadata();
            attempt = new PulsarSealedLedgerAttemptV1(
                    ledger.getId(),
                    attemptUuid,
                    ledger.getLastAddConfirmed(),
                    Math.addExact(ledger.getLastAddConfirmed(), 1),
                    ledger.getLength(),
                    metadata.getCtime(),
                    0,
                    policy.providerScopePrefix(),
                    policy.retentionClass(),
                    DeleteState.BK_DELETE_NONE,
                    false);
            sealedLedger = sealedLedger(metadata, ledger);
        } catch (Throwable failure) {
            return CompletableFuture.failedFuture(failure);
        }

        var admission = enrolledOutput;
        if (admission == null) {
            return CompletableFuture.completedFuture(null)
                    .thenCompose(unused -> encodeAndPublish(
                            ledger,
                            attempt,
                            sealedLedger,
                            Objects.requireNonNull(
                                    keyManager.create(ledger.getId(), attemptUuid, policy.driverMetadata()),
                                    "attemptKey"),
                            null));
        }
        admission.begin(attemptUuid);
        return CompletableFuture.completedFuture(null)
                .thenCompose(unused -> admission.requireCurrent())
                .thenCompose(unused -> admission
                        .attempts()
                        .open(
                                attempt,
                                sealedLedger,
                                policy.blockClass().targetBytes(),
                                policy.driverMetadata(),
                                keyManager,
                                fileExecutor))
                .thenCompose(original -> admission
                        .open(original)
                        .thenCompose(outputs ->
                                encodeAndPublish(ledger, attempt, sealedLedger, original.keyMaterial(), outputs)))
                .whenComplete((unused, failure) -> admission.finish(attemptUuid, failure))
                .toCompletableFuture();
    }

    private CompletableFuture<Void> encodeAndPublish(
            ReadHandle ledger,
            PulsarSealedLedgerAttemptV1 attempt,
            SealedLedgerSection sealedLedger,
            PreparedAttemptKey attemptKey,
            PulsarSealedLedgerPublisherV1.ImmutableOutputPublisher outputs) {
        return createStagingFile(ledger.getId(), attempt.attemptUuid())
                .thenCompose(target -> encodeLedger(ledger, attempt.attemptUuid(), attemptKey.plaintextKey(), target)
                        .thenCompose(data -> {
                            var prepared = new PreparedAttempt(
                                    attempt,
                                    sealedLedger,
                                    data,
                                    policy.blockClass().targetBytes(),
                                    attemptKey.envelope());
                            return outputs == null ? publisher.publish(prepared) : publisher.publish(prepared, outputs);
                        })
                        .thenAccept(ignored -> {})
                        .whenComplete((ignored, failure) -> deleteStagingFile(target)))
                .toCompletableFuture();
    }

    @Override
    public CompletableFuture<ReadHandle> readOffloaded(
            long ledgerId, UUID attemptUuid, Map<String, String> persistedDriverMetadata) {
        return readOffloaded(ledgerId, attemptUuid, persistedDriverMetadata, null);
    }

    @Override
    public CompletableFuture<ReadHandle> readOffloadedCaptured(
            long ledgerId,
            UUID attemptUuid,
            Map<String, String> persistedDriverMetadata,
            OffloadedReadSourceIdentity expected) {
        if (expected == null
                || expected.ledgerId() != ledgerId
                || !expected.attemptUuid().equals(attemptUuid)) {
            return CompletableFuture.failedFuture(new IllegalArgumentException("published Object attempt differs"));
        }
        return readOffloaded(ledgerId, attemptUuid, persistedDriverMetadata, expected);
    }

    CompletionStage<OffloadedReadSourceIdentity> captureEnrolledOffload(
            long ledgerId, UUID attempt, Map<String, String> persistedDriverMetadata) {
        var admission = Objects.requireNonNull(enrolledOutput, "enrolledOutput");
        var exactPolicy = nativePolicy(persistedDriverMetadata);
        return admission.capture(
                ledgerId, attempt, PulsarOffloadKeysV1.derive(exactPolicy.providerScopePrefix(), ledgerId, attempt));
    }

    private CompletableFuture<ReadHandle> readOffloaded(
            long ledgerId,
            UUID attemptUuid,
            Map<String, String> persistedDriverMetadata,
            OffloadedReadSourceIdentity expected) {
        CompletableFuture<ReadHandle> opening = admittedOperation(() -> {
            if (expected == null && enrolledOutput != null) {
                return captureEnrolledOffload(ledgerId, attemptUuid, persistedDriverMetadata)
                        .thenCompose(
                                captured -> openReadHandle(ledgerId, attemptUuid, persistedDriverMetadata, captured))
                        .toCompletableFuture();
            }
            return openReadHandle(ledgerId, attemptUuid, persistedDriverMetadata, expected);
        });
        CompletableFuture<ReadHandle> observer = new CompletableFuture<>();
        opening.whenComplete((handle, failure) -> {
            if (failure != null) {
                observer.completeExceptionally(failure);
            } else if (!observer.complete(handle)) {
                // No caller received this handle; it has no admitted reads and owns no borrowed source.
                handle.closeAsync();
            }
        });
        return observer;
    }

    private CompletableFuture<ReadHandle> openReadHandle(
            long ledgerId,
            UUID attemptUuid,
            Map<String, String> persistedDriverMetadata,
            OffloadedReadSourceIdentity expected) {
        NativePolicy nativePolicy;
        try {
            nativePolicy = nativePolicy(persistedDriverMetadata);
            if (expected != null) {
                var keys = PulsarOffloadKeysV1.derive(nativePolicy.providerScopePrefix(), ledgerId, attemptUuid);
                if (!keys.rootKey().equals(expected.root().key())
                        || !keys.dataKey().equals(expected.data().key())) {
                    throw new IllegalArgumentException("published Object keys differ from the actual provider attempt");
                }
            }
        } catch (Throwable failure) {
            return CompletableFuture.failedFuture(failure);
        }
        return PulsarObjectReadHandleV1.openNative(
                        objectStore,
                        policy.limits(),
                        ledgerId,
                        attemptUuid,
                        nativePolicy.providerScopePrefix(),
                        nativePolicy.retentionClass(),
                        nativePolicy.blockClass().targetBytes(),
                        nativePolicy.compressionPolicy(),
                        persistedAttempt -> keyManager.unwrap(
                                ledgerId, attemptUuid, persistedAttempt.keyEnvelope(), persistedDriverMetadata),
                        expected == null
                                ? null
                                : new PulsarOffloadObjectStoreV1.ImmutableObject(
                                        expected.root().version(),
                                        expected.root().length(),
                                        expected.root().sha256()),
                        expected == null
                                ? null
                                : new PulsarOffloadObjectStoreV1.ImmutableObject(
                                        expected.data().version(),
                                        expected.data().length(),
                                        expected.data().sha256()))
                .thenCompose(handle -> {
                    if (closed.get()) {
                        return handle.close()
                                .thenCompose(ignored -> CompletableFuture.<ReadHandle>failedFuture(
                                        new IllegalStateException("Nereus Pulsar offloader is closed")));
                    }
                    return CompletableFuture.<ReadHandle>completedFuture(new NereusPulsarReadHandleV1(handle));
                })
                .toCompletableFuture();
    }

    @Override
    public OffloadedReadSourceIdentity describeOffloadedReadSource(ReadHandle handle) {
        if (!(handle instanceof NereusPulsarReadHandleV1 nativeHandle)) {
            throw new IllegalArgumentException("opened source is not this provider's NPO1/NPD1 handle");
        }
        return nativeHandle.describeSource(objectStore);
    }

    @Override
    public SourceSafeLedgerOffloader.OffloadedReadMemoryBounds describeOffloadedReadMemoryBounds(ReadHandle handle) {
        if (!(handle instanceof NereusPulsarReadHandleV1 nativeHandle)) {
            throw new IllegalArgumentException("opened source is not this provider's NPO1/NPD1 handle");
        }
        return nativeHandle.memoryBounds(objectStore);
    }

    @Override
    public CompletableFuture<Void> verifyOpenedOffloadedSource(
            ReadHandle handle,
            long lastAddConfirmed,
            long entryCount,
            long logicalLength,
            BooleanSupplier sourceUseOpen) {
        try {
            if (!(handle instanceof NereusPulsarReadHandleV1 nativeHandle)) {
                throw new IllegalArgumentException("opened verification source belongs to another provider");
            }
            nativeHandle.describeSource(objectStore);
            return admittedOperation(
                    () -> nativeHandle.verifyOpenedSource(lastAddConfirmed, entryCount, logicalLength, sourceUseOpen));
        } catch (Throwable failure) {
            return CompletableFuture.failedFuture(failure);
        }
    }

    /** Uses this exact Native provider connection; endpoint aliases and another store cannot supply its marker. */
    public CompletableFuture<PhysicalNamespaceAuthorityBindingV2> observePhysicalNamespace(
            Namespace expected, MetadataNamespaceIdentityV2 metadataNamespace) {
        return admittedOperation(() -> {
            if (!(objectStore instanceof S3PulsarOffloadObjectStoreV1 store)) {
                return CompletableFuture.failedFuture(
                        new UnsupportedOperationException("native physical Object namespace is unsupported"));
            }
            return store.observePhysicalNamespace(expected, metadataNamespace).toCompletableFuture();
        });
    }

    @Override
    public CompletableFuture<Void> deleteOffloaded(
            long ledgerId, UUID attemptUuid, Map<String, String> persistedDriverMetadata) {
        try {
            requireOpen();
            NativePolicy nativePolicy = nativePolicy(persistedDriverMetadata);
            PulsarOffloadKeysV1.derive(nativePolicy.providerScopePrefix(), ledgerId, attemptUuid);
        } catch (Throwable failure) {
            return CompletableFuture.failedFuture(failure);
        }
        return CompletableFuture.failedFuture(new IllegalStateException(
                "Pulsar offload deletion requires M5 intent, release and native reference-free authority"));
    }

    @Override
    public org.apache.bookkeeper.mledger.SourceSafeLedgerOffloader.RetentionClass getBookKeeperRetentionClass() {
        return policy.retentionClass() == PulsarSealedLedgerAttemptV1.RetentionClass.RETAIN_BK
                ? org.apache.bookkeeper.mledger.SourceSafeLedgerOffloader.RetentionClass.RETAIN_BK
                : org.apache.bookkeeper.mledger.SourceSafeLedgerOffloader.RetentionClass.DELETE_AFTER_VERIFIED;
    }

    @Override
    public org.apache.bookkeeper.mledger.SourceSafeLedgerOffloader.ReadFailureKind classifyOffloadedReadFailure(
            Throwable failure) {
        Throwable unwrapped = unwrap(failure);
        if (unwrapped instanceof ObjectReadException readFailure) {
            return switch (readFailure.kind()) {
                case NOT_FOUND -> SourceSafeLedgerOffloader.ReadFailureKind.NOT_FOUND;
                case TIMEOUT -> SourceSafeLedgerOffloader.ReadFailureKind.TIMEOUT;
                case UNAVAILABLE -> SourceSafeLedgerOffloader.ReadFailureKind.UNAVAILABLE;
                case SHORT_READ -> SourceSafeLedgerOffloader.ReadFailureKind.SHORT_READ;
                case INTEGRITY -> SourceSafeLedgerOffloader.ReadFailureKind.INTEGRITY;
                case FORMAT -> SourceSafeLedgerOffloader.ReadFailureKind.FORMAT;
                case INVALID_RANGE -> SourceSafeLedgerOffloader.ReadFailureKind.INVALID_RANGE;
                case CANCELLED -> SourceSafeLedgerOffloader.ReadFailureKind.CANCELLED;
                case CLOSED -> SourceSafeLedgerOffloader.ReadFailureKind.CLOSED;
            };
        }
        if (unwrapped instanceof CancellationException) {
            return SourceSafeLedgerOffloader.ReadFailureKind.CANCELLED;
        }
        return SourceSafeLedgerOffloader.ReadFailureKind.OTHER;
    }

    @Override
    public void recordOffloadedReadIntegrityFailure(long ledgerId, UUID attemptUuid, Throwable failure) {
        integrityObserver.record(ledgerId, attemptUuid, failure);
    }

    @Override
    public CompletableFuture<Void> revalidateOffloadedForSourceDeletion(
            long ledgerId,
            UUID attemptUuid,
            Map<String, String> persistedDriverMetadata,
            long lastAddConfirmed,
            long entryCount,
            long logicalLength) {
        return admittedOperation(() -> revalidateSource(
                ledgerId, attemptUuid, persistedDriverMetadata, lastAddConfirmed, entryCount, logicalLength));
    }

    private CompletableFuture<Void> revalidateSource(
            long ledgerId,
            UUID attemptUuid,
            Map<String, String> persistedDriverMetadata,
            long lastAddConfirmed,
            long entryCount,
            long logicalLength) {
        NativePolicy nativePolicy;
        try {
            nativePolicy = nativePolicy(persistedDriverMetadata);
        } catch (Throwable failure) {
            return CompletableFuture.failedFuture(failure);
        }
        return PulsarObjectReadHandleV1.openNative(
                        objectStore,
                        policy.limits(),
                        ledgerId,
                        attemptUuid,
                        nativePolicy.providerScopePrefix(),
                        nativePolicy.retentionClass(),
                        nativePolicy.blockClass().targetBytes(),
                        nativePolicy.compressionPolicy(),
                        persistedAttempt -> keyManager.unwrap(
                                ledgerId, attemptUuid, persistedAttempt.keyEnvelope(), persistedDriverMetadata))
                .thenCompose(handle -> {
                    CompletableFuture<Void> result = new CompletableFuture<>();
                    PulsarSealedLedgerAttemptV1 attempt = handle.attempt();
                    if (attempt.lastAddConfirmed() != lastAddConfirmed
                            || attempt.entryCount() != entryCount
                            || attempt.logicalLength() != logicalLength) {
                        handle.close()
                                .whenComplete((ignored, closeFailure) -> result.completeExceptionally(
                                        new IllegalStateException("native deletion facts differ from NPO1")));
                        return result;
                    }
                    handle.verifyCompleteLedger().whenComplete((report, verifyFailure) -> handle.close()
                            .whenComplete((ignored, closeFailure) ->
                                    completeAfterClose(result, verifyFailure, closeFailure)));
                    return result;
                })
                .toCompletableFuture();
    }

    @Override
    public OffloadPolicies getOffloadPolicies() {
        return offloadPolicies;
    }

    @Override
    public void close() {
        closeAsync().join();
    }

    /** Waits for admitted offloads, handle opens and source verification; returned read handles remain borrowed. */
    public CompletableFuture<Void> closeAsync() {
        boolean complete;
        synchronized (lifetimeLock) {
            closed.set(true);
            complete = activeOperations == 0;
        }
        if (complete) {
            drained.complete(null);
        }
        return drained.copy();
    }

    private <T> CompletableFuture<T> admittedOperation(Supplier<CompletableFuture<T>> operation) {
        synchronized (lifetimeLock) {
            if (closed.get()) {
                return CompletableFuture.failedFuture(new IllegalStateException("Nereus Pulsar offloader is closed"));
            }
            activeOperations++;
        }
        CompletableFuture<T> actual;
        try {
            actual = operation.get();
        } catch (Throwable failure) {
            actual = CompletableFuture.failedFuture(failure);
        }
        return actual.whenComplete((ignored, failure) -> operationFinished()).copy();
    }

    private void operationFinished() {
        boolean complete;
        synchronized (lifetimeLock) {
            activeOperations--;
            complete = closed.get() && activeOperations == 0;
        }
        if (complete) {
            drained.complete(null);
        }
    }

    private CompletableFuture<Path> createStagingFile(long ledgerId, UUID attemptUuid) {
        return CompletableFuture.supplyAsync(
                () -> {
                    try {
                        Files.createDirectories(stagingDirectory);
                        return Files.createTempFile(
                                stagingDirectory, "npd1-ledger-" + ledgerId + "-" + attemptUuid + "-", ".tmp");
                    } catch (IOException failure) {
                        throw new CompletionException("cannot create NPD1 staging file", failure);
                    }
                },
                fileExecutor);
    }

    private CompletableFuture<DataObject> encodeLedger(
            ReadHandle ledger, UUID attemptUuid, SecretKey attemptKey, Path target) {
        CompletableFuture<DataObject> result = new CompletableFuture<>();
        CompletableFuture.runAsync(
                        () -> {
                            StreamingEncoder encoder = Npd1CodecV1.openStreaming(
                                    target,
                                    policy.blockClass().targetBytes(),
                                    policy.compressionPolicy(),
                                    attemptKey,
                                    attemptUuid,
                                    policy.limits());
                            CompletableFuture<DataObject> encoded = new CompletableFuture<>();
                            encoded.whenComplete((ignored, failure) -> encoder.close())
                                    .whenComplete((data, failure) -> {
                                        if (failure == null) {
                                            result.complete(data);
                                        } else {
                                            result.completeExceptionally(unwrap(failure));
                                        }
                                    });
                            readNextBatch(ledger, 0, encoder, encoded);
                        },
                        fileExecutor)
                .exceptionally(failure -> {
                    result.completeExceptionally(unwrap(failure));
                    return null;
                });
        return result;
    }

    private void readNextBatch(
            ReadHandle ledger, long firstEntryId, StreamingEncoder encoder, CompletableFuture<DataObject> result) {
        CompletableFuture<LedgerEntries> read;
        try {
            // BookKeeper's range-read fallback ignores maxSize; bound count by the admitted maximum entry size.
            int maximumEntries = (int) Math.min(
                    policy.limits().maxEntriesPerBlock(),
                    policy.limits().maxDecodedBlockBytes() / policy.limits().maxEntryBytes());
            read = Objects.requireNonNull(ledger.batchReadAsync(
                    firstEntryId, maximumEntries, policy.limits().maxDecodedBlockBytes()));
        } catch (Throwable failure) {
            result.completeExceptionally(failure);
            return;
        }
        read.whenComplete((entries, readFailure) -> {
            try {
                fileExecutor.execute(() -> consumeBatch(ledger, firstEntryId, encoder, result, entries, readFailure));
            } catch (Throwable dispatchFailure) {
                Throwable primary = readFailure == null ? dispatchFailure : unwrap(readFailure);
                if (primary != dispatchFailure) {
                    primary.addSuppressed(dispatchFailure);
                }
                result.completeExceptionally(closeEntries(entries, primary));
            }
        });
    }

    private void consumeBatch(
            ReadHandle ledger,
            long firstEntryId,
            StreamingEncoder encoder,
            CompletableFuture<DataObject> result,
            LedgerEntries entries,
            Throwable readFailure) {
        if (readFailure != null) {
            result.completeExceptionally(closeEntries(entries, unwrap(readFailure)));
            return;
        }
        Throwable failure = null;
        long nextEntryId = firstEntryId;
        int batchEntries = 0;
        long batchBytes = 0;
        try {
            if (entries == null) {
                throw new IllegalStateException("native batch read returned null entries");
            }
            for (LedgerEntry entry : entries) {
                if (entry.getLedgerId() != ledger.getId() || entry.getEntryId() != nextEntryId) {
                    throw new IllegalStateException("native batch read is mixed, reordered, or gapped");
                }
                byte[] payload = entry.getEntryBytes();
                batchEntries = Math.addExact(batchEntries, 1);
                batchBytes = Math.addExact(batchBytes, payload.length);
                if (batchEntries > policy.limits().maxEntriesPerBlock()
                        || batchBytes > policy.limits().maxDecodedBlockBytes()) {
                    throw new IllegalStateException("native batch read exceeded the admitted count or byte cap");
                }
                encoder.append(new EntryPayload(entry.getEntryId(), payload));
                nextEntryId = Math.addExact(nextEntryId, 1);
            }
            long terminalEntryId = Math.addExact(ledger.getLastAddConfirmed(), 1);
            if (nextEntryId == firstEntryId || nextEntryId > terminalEntryId) {
                throw new IllegalStateException("native batch read made no progress or crossed LAC");
            }
        } catch (Throwable batchFailure) {
            failure = batchFailure;
        }
        failure = closeEntries(entries, failure);
        if (failure != null) {
            result.completeExceptionally(failure);
        } else if (nextEntryId == Math.addExact(ledger.getLastAddConfirmed(), 1)) {
            try {
                result.complete(encoder.finish());
            } catch (Throwable finishFailure) {
                result.completeExceptionally(finishFailure);
            }
        } else {
            readNextBatch(ledger, nextEntryId, encoder, result);
        }
    }

    private static Throwable closeEntries(LedgerEntries entries, Throwable failure) {
        try {
            if (entries != null) {
                entries.close();
            }
        } catch (Throwable closeFailure) {
            if (failure == null) {
                return closeFailure;
            }
            failure.addSuppressed(closeFailure);
        }
        return failure;
    }

    private NativePolicy nativePolicy(Map<String, String> persisted) {
        Objects.requireNonNull(persisted, "persistedDriverMetadata");
        String policyId = required(persisted, METADATA_POLICY_ID);
        String scope = required(persisted, METADATA_PROVIDER_SCOPE);
        int keyDerivationVersion = Integer.parseInt(required(persisted, METADATA_KEY_DERIVATION_VERSION));
        PulsarSealedLedgerAttemptV1.RetentionClass retention =
                PulsarSealedLedgerAttemptV1.RetentionClass.valueOf(required(persisted, METADATA_RETENTION));
        PulsarOffloadBlockPolicyV1.BlockClass blockClass =
                PulsarOffloadBlockPolicyV1.BlockClass.fromId(required(persisted, METADATA_BLOCK_CLASS));
        int blockTarget = Integer.parseInt(required(persisted, METADATA_BLOCK_TARGET));
        CompressionPolicy compressionPolicy =
                CompressionPolicy.valueOf(required(persisted, METADATA_COMPRESSION_POLICY));
        if (!policy.policyId().equals(policyId)
                || keyDerivationVersion != PulsarOffloadKeysV1.KEY_DERIVATION_VERSION
                || blockTarget != blockClass.targetBytes()
                || !policy.limits().blockTargetBytes().contains(blockTarget)) {
            throw new IllegalArgumentException("persisted Nereus offload policy is not supported by this provider");
        }
        PulsarOffloadKeysV1.derive(scope, 0, new UUID(0, 0));
        return new NativePolicy(scope, retention, blockClass, compressionPolicy);
    }

    private static String required(Map<String, String> metadata, String key) {
        String value = metadata.get(key);
        if (value == null || value.isBlank()) {
            throw new IllegalArgumentException("persisted driver metadata lacks " + key);
        }
        return value;
    }

    private void requireOpen() {
        if (closed.get()) {
            throw new IllegalStateException("Nereus Pulsar offloader is closed");
        }
    }

    private static SealedLedgerSection sealedLedger(LedgerMetadata metadata, ReadHandle ledger) {
        Map<String, CustomMetadataValue> customMetadata = new HashMap<>();
        metadata.getCustomMetadata().forEach((key, value) -> customMetadata.put(key, new CustomMetadataValue(value)));
        List<EnsembleSegment> ensembles = new ArrayList<>();
        metadata.getAllEnsembles()
                .forEach((firstEntryId, bookies) -> ensembles.add(new EnsembleSegment(
                        firstEntryId, bookies.stream().map(Object::toString).toList())));
        return new SealedLedgerSection(
                ledger.getLastAddConfirmed(),
                Math.addExact(ledger.getLastAddConfirmed(), 1),
                ledger.getLength(),
                metadata.getCtime(),
                metadata.getCToken(),
                metadata.getEnsembleSize(),
                metadata.getWriteQuorumSize(),
                metadata.getAckQuorumSize(),
                switch (metadata.getDigestType()) {
                    case CRC32C -> DigestType.CRC32C;
                    case MAC -> DigestType.MAC;
                    case CRC32 -> DigestType.CRC32;
                    case DUMMY -> DigestType.DUMMY;
                },
                customMetadata,
                ensembles);
    }

    private static void completeAfterClose(CompletableFuture<Void> result, Throwable primary, Throwable closeFailure) {
        if (primary != null) {
            if (closeFailure != null) {
                primary.addSuppressed(closeFailure);
            }
            result.completeExceptionally(primary);
        } else if (closeFailure != null) {
            result.completeExceptionally(closeFailure);
        } else {
            result.complete(null);
        }
    }

    private static void deleteStagingFile(Path target) {
        try {
            Files.deleteIfExists(target);
        } catch (IOException ignored) {
            // Published native completion is not revoked by best-effort local staging cleanup.
        }
    }

    private static Throwable unwrap(Throwable failure) {
        if ((failure instanceof CompletionException || failure instanceof java.util.concurrent.ExecutionException)
                && failure.getCause() != null) {
            return failure.getCause();
        }
        return failure;
    }

    private record NativePolicy(
            String providerScopePrefix,
            PulsarSealedLedgerAttemptV1.RetentionClass retentionClass,
            PulsarOffloadBlockPolicyV1.BlockClass blockClass,
            CompressionPolicy compressionPolicy) {}
}
