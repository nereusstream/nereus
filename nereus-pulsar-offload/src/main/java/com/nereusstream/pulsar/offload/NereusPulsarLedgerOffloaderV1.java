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
import com.nereusstream.pulsar.offload.npd1.Npd1CodecV1.CompressionFamily;
import com.nereusstream.pulsar.offload.npd1.Npd1CodecV1.DataObject;
import com.nereusstream.pulsar.offload.npd1.Npd1CodecV1.EntryPayload;
import com.nereusstream.pulsar.offload.npd1.Npd1CodecV1.StreamingEncoder;
import com.nereusstream.pulsar.offload.npo1.Npo1CodecV1.CustomMetadataValue;
import com.nereusstream.pulsar.offload.npo1.Npo1CodecV1.DigestType;
import com.nereusstream.pulsar.offload.npo1.Npo1CodecV1.EnsembleSegment;
import com.nereusstream.pulsar.offload.npo1.Npo1CodecV1.SealedLedgerSection;
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
import java.util.concurrent.Executor;
import java.util.concurrent.atomic.AtomicBoolean;
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
    public static final String METADATA_BLOCK_TARGET = "nereus.blockTargetBytes";
    public static final String METADATA_COMPRESSION = "nereus.compression";

    @FunctionalInterface
    public interface AttemptKeyResolver {
        SecretKey resolve(long ledgerId, UUID attemptUuid, Map<String, String> persistedDriverMetadata);
    }

    @FunctionalInterface
    public interface IntegrityFailureObserver {
        void record(long ledgerId, UUID attemptUuid, Throwable failure);
    }

    public record RuntimePolicy(
            String policyId,
            String providerScopePrefix,
            PulsarSealedLedgerAttemptV1.RetentionClass retentionClass,
            int blockTargetBytes,
            CompressionFamily compressionFamily,
            PulsarOffloadLimitCandidateV1 limits) {
        public RuntimePolicy {
            Objects.requireNonNull(policyId, "policyId");
            Objects.requireNonNull(providerScopePrefix, "providerScopePrefix");
            Objects.requireNonNull(retentionClass, "retentionClass");
            Objects.requireNonNull(compressionFamily, "compressionFamily");
            Objects.requireNonNull(limits, "limits");
            if (!policyId.matches("[a-z0-9][a-z0-9._-]{0,63}")
                    || !limits.blockTargetBytes().contains(blockTargetBytes)) {
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
                    METADATA_BLOCK_TARGET, Integer.toString(blockTargetBytes),
                    METADATA_COMPRESSION, compressionFamily.name());
        }
    }

    private final PulsarOffloadObjectStoreV1 objectStore;
    private final RuntimePolicy policy;
    private final AttemptKeyResolver keyResolver;
    private final IntegrityFailureObserver integrityObserver;
    private final OffloadPolicies offloadPolicies;
    private final Executor fileExecutor;
    private final Path stagingDirectory;
    private final PulsarSealedLedgerPublisherV1 publisher;
    private final AtomicBoolean closed = new AtomicBoolean();

    public NereusPulsarLedgerOffloaderV1(
            PulsarOffloadObjectStoreV1 objectStore,
            RuntimePolicy policy,
            AttemptKeyResolver keyResolver,
            IntegrityFailureObserver integrityObserver,
            OffloadPolicies offloadPolicies,
            Executor fileExecutor,
            Path stagingDirectory) {
        this.objectStore = Objects.requireNonNull(objectStore, "objectStore");
        this.policy = Objects.requireNonNull(policy, "policy");
        this.keyResolver = Objects.requireNonNull(keyResolver, "keyResolver");
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
                        attempt -> keyResolver.resolve(
                                attempt.ledgerId(), attempt.attemptUuid(), policy.driverMetadata())),
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

    @Override
    public CompletableFuture<Void> offload(ReadHandle ledger, UUID attemptUuid, Map<String, String> extraMetadata) {
        SecretKey attemptKey;
        PulsarSealedLedgerAttemptV1 attempt;
        SealedLedgerSection sealedLedger;
        try {
            Objects.requireNonNull(ledger, "ledger");
            Objects.requireNonNull(attemptUuid, "attemptUuid");
            Objects.requireNonNull(extraMetadata, "extraMetadata");
            requireOpen();
            if (!ledger.isClosed()
                    || ledger.getLastAddConfirmed() < 0
                    || ledger.getLedgerMetadata().getLastEntryId() != ledger.getLastAddConfirmed()
                    || ledger.getLength() != ledger.getLedgerMetadata().getLength()
                    || !extraMetadata.containsKey("ManagedLedgerName")) {
                throw new IllegalArgumentException("offload input is not one exact sealed native ledger");
            }
            attemptKey = Objects.requireNonNull(
                    keyResolver.resolve(ledger.getId(), attemptUuid, policy.driverMetadata()), "attemptKey");
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

        return createStagingFile(ledger.getId(), attemptUuid)
                .thenCompose(target -> encodeLedger(ledger, attemptUuid, attemptKey, target)
                        .thenCompose(data -> publisher.publish(
                                new PreparedAttempt(attempt, sealedLedger, data, policy.blockTargetBytes())))
                        .thenAccept(ignored -> {})
                        .whenComplete((ignored, failure) -> deleteStagingFile(target)))
                .toCompletableFuture();
    }

    @Override
    public CompletableFuture<ReadHandle> readOffloaded(
            long ledgerId, UUID attemptUuid, Map<String, String> persistedDriverMetadata) {
        NativePolicy nativePolicy;
        SecretKey attemptKey;
        try {
            requireOpen();
            nativePolicy = nativePolicy(persistedDriverMetadata);
            attemptKey = Objects.requireNonNull(
                    keyResolver.resolve(ledgerId, attemptUuid, persistedDriverMetadata), "attemptKey");
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
                        nativePolicy.blockTargetBytes(),
                        nativePolicy.compressionFamily(),
                        attemptKey)
                .thenApply(handle -> (ReadHandle) new NereusPulsarReadHandleV1(handle))
                .toCompletableFuture();
    }

    @Override
    public CompletableFuture<Void> deleteOffloaded(
            long ledgerId, UUID attemptUuid, Map<String, String> persistedDriverMetadata) {
        NativePolicy nativePolicy;
        try {
            requireOpen();
            nativePolicy = nativePolicy(persistedDriverMetadata);
        } catch (Throwable failure) {
            return CompletableFuture.failedFuture(failure);
        }
        PulsarOffloadKeysV1 keys =
                PulsarOffloadKeysV1.derive(nativePolicy.providerScopePrefix(), ledgerId, attemptUuid);
        return objectStore
                .deleteAndProveAbsent(keys.rootKey())
                .thenCompose(ignored -> objectStore.deleteAndProveAbsent(keys.dataKey()))
                .thenCompose(ignored -> objectStore.cleanupAttemptMultipartResidue(keys.attemptPrefix()))
                .toCompletableFuture();
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
        NativePolicy nativePolicy;
        SecretKey attemptKey;
        try {
            requireOpen();
            nativePolicy = nativePolicy(persistedDriverMetadata);
            attemptKey = Objects.requireNonNull(
                    keyResolver.resolve(ledgerId, attemptUuid, persistedDriverMetadata), "attemptKey");
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
                        nativePolicy.blockTargetBytes(),
                        nativePolicy.compressionFamily(),
                        attemptKey)
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
        closed.set(true);
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
                                    policy.blockTargetBytes(),
                                    policy.compressionFamily(),
                                    attemptKey,
                                    attemptUuid,
                                    policy.limits());
                            readNextBatch(ledger, 0, encoder, result);
                            result.whenComplete((ignored, failure) -> encoder.close());
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
            read = Objects.requireNonNull(ledger.batchReadAsync(
                    firstEntryId,
                    policy.limits().maxEntriesPerBlock(),
                    policy.limits().maxDecodedBlockBytes()));
        } catch (Throwable failure) {
            result.completeExceptionally(failure);
            return;
        }
        read.whenCompleteAsync(
                (entries, readFailure) -> {
                    if (readFailure != null) {
                        result.completeExceptionally(unwrap(readFailure));
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
                                throw new IllegalStateException(
                                        "native batch read exceeded the admitted count or byte cap");
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
                    try {
                        if (entries != null) {
                            entries.close();
                        }
                    } catch (Throwable closeFailure) {
                        if (failure == null) {
                            failure = closeFailure;
                        } else {
                            failure.addSuppressed(closeFailure);
                        }
                    }
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
                },
                fileExecutor);
    }

    private NativePolicy nativePolicy(Map<String, String> persisted) {
        Objects.requireNonNull(persisted, "persistedDriverMetadata");
        String policyId = required(persisted, METADATA_POLICY_ID);
        String scope = required(persisted, METADATA_PROVIDER_SCOPE);
        int keyDerivationVersion = Integer.parseInt(required(persisted, METADATA_KEY_DERIVATION_VERSION));
        PulsarSealedLedgerAttemptV1.RetentionClass retention =
                PulsarSealedLedgerAttemptV1.RetentionClass.valueOf(required(persisted, METADATA_RETENTION));
        int blockTarget = Integer.parseInt(required(persisted, METADATA_BLOCK_TARGET));
        CompressionFamily compression = CompressionFamily.valueOf(required(persisted, METADATA_COMPRESSION));
        if (!policy.policyId().equals(policyId)
                || keyDerivationVersion != PulsarOffloadKeysV1.KEY_DERIVATION_VERSION
                || !policy.limits().blockTargetBytes().contains(blockTarget)) {
            throw new IllegalArgumentException("persisted Nereus offload policy is not supported by this provider");
        }
        PulsarOffloadKeysV1.derive(scope, 0, new UUID(0, 0));
        return new NativePolicy(scope, retention, blockTarget, compression);
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
            int blockTargetBytes,
            CompressionFamily compressionFamily) {}
}
