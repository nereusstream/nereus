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
import com.nereusstream.storage.api.bookkeeper.AppendQuorumProofV1;
import com.nereusstream.storage.api.bookkeeper.BookKeeperCapabilitySnapshotV1;
import com.nereusstream.storage.api.bookkeeper.BookKeeperCellSession;
import com.nereusstream.storage.api.bookkeeper.BookKeeperDigestTypeV1;
import com.nereusstream.storage.api.bookkeeper.BookKeeperLedgerIdentity;
import com.nereusstream.storage.api.bookkeeper.BookKeeperProtocolModeV1;
import com.nereusstream.storage.api.bookkeeper.CellProviderScopeId;
import com.nereusstream.storage.api.bookkeeper.ExactLedgerEntryV1;
import com.nereusstream.storage.api.bookkeeper.ProviderMutationResultV1;
import com.nereusstream.storage.api.bookkeeper.RunLedgerAppendRequestV1;
import com.nereusstream.storage.api.bookkeeper.RunLedgerCloseProofV1;
import com.nereusstream.storage.api.bookkeeper.RunLedgerConfigurationV1;
import com.nereusstream.storage.api.bookkeeper.RunLedgerHandleV1;
import com.nereusstream.storage.api.bookkeeper.RunLedgerOpenOutcomeV1;
import com.nereusstream.storage.api.bookkeeper.RunLedgerOpenResultV1;
import com.nereusstream.storage.api.bookkeeper.RunLedgerReadOutcomeV1;
import com.nereusstream.storage.api.bookkeeper.RunLedgerReadResultV1;
import com.nereusstream.storage.api.bookkeeper.RunLedgerRecoveryProofV1;
import java.nio.ByteBuffer;
import java.util.Arrays;
import java.util.LinkedHashMap;
import java.util.Map;
import java.util.Objects;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.CompletionException;
import java.util.concurrent.CompletionStage;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.ConcurrentMap;
import java.util.concurrent.atomic.AtomicReference;
import org.apache.bookkeeper.client.api.BKException;
import org.apache.bookkeeper.client.api.BookKeeper;
import org.apache.bookkeeper.client.api.DigestType;
import org.apache.bookkeeper.client.api.Handle;
import org.apache.bookkeeper.client.api.LedgerEntries;
import org.apache.bookkeeper.client.api.LedgerEntry;
import org.apache.bookkeeper.client.api.LedgerMetadata;
import org.apache.bookkeeper.client.api.ReadHandle;
import org.apache.bookkeeper.client.api.WriteAdvHandle;

/**
 * Real BookKeeper 4.18 Cell session over a borrowed stateless client transport.
 *
 * <p>The session owns every handle and response-unknown resolver it creates, but never closes the borrowed
 * {@link BookKeeper} client. Run identity is recorded in BookKeeper custom metadata and is revalidated on every open.
 */
public final class RealBookKeeperCellSessionV1 implements BookKeeperCellSession {
    static final String METADATA_SCHEMA = "nereus.schema";
    static final String METADATA_PROVIDER_SCOPE = "nereus.provider-scope";
    static final String METADATA_RUN_ID = "nereus.run-id";
    static final String METADATA_CONFIGURATION = "nereus.configuration";
    static final byte[] SCHEMA_V1 = new byte[] {'N', 'B', 'K', 'R', '1'};

    private final BookKeeper client;
    private final BookKeeperCapabilitySnapshotV1 capability;
    private final byte[] password;
    private final CellSessionOperationRegistry operations = new CellSessionOperationRegistry();
    private final ConcurrentMap<Long, OwnedLedger> ledgers = new ConcurrentHashMap<>();
    private final ConcurrentMap<EntryIdentity, UnknownAppend> unknownAppends = new ConcurrentHashMap<>();
    private final AtomicReference<CompletableFuture<Void>> closeFuture = new AtomicReference<>();

    public RealBookKeeperCellSessionV1(
            BookKeeper borrowedClient, BookKeeperCapabilitySnapshotV1 capability, byte[] password) {
        this.client = Objects.requireNonNull(borrowedClient, "borrowedClient");
        this.capability = Objects.requireNonNull(capability, "capability");
        this.password = Objects.requireNonNull(password, "password").clone();
        requireSupportedCapability(capability);
    }

    @Override
    public CellProviderScopeId providerScopeId() {
        return capability.providerScopeId();
    }

    @Override
    public BookKeeperCapabilitySnapshotV1 capabilitySnapshot() {
        return capability;
    }

    @Override
    public CompletionStage<ProviderMutationResultV1<RunLedgerHandleV1>> createRunLedger(
            RunLedgerConfigurationV1 configuration) {
        requireConfiguration(configuration);
        CellSessionOperationRegistry.OperationLease lease = operations.acceptOperation();
        CompletableFuture<ProviderMutationResultV1<RunLedgerHandleV1>> observer = new CompletableFuture<>();
        try {
            client.newCreateLedgerOp()
                    .withEnsembleSize(configuration.ensembleSize())
                    .withWriteQuorumSize(configuration.writeQuorumSize())
                    .withAckQuorumSize(configuration.ackQuorumSize())
                    .withDigestType(digestType(configuration.digestType()))
                    .withPassword(password)
                    .withCustomMetadata(metadata(configuration))
                    .makeAdv()
                    .execute()
                    .whenComplete((handle, failure) -> {
                        try {
                            if (failure != null) {
                                observer.complete(createFailure(failure));
                                return;
                            }
                            RunLedgerHandleV1 exactHandle = exactHandle(configuration, handle.getId());
                            OwnedLedger previous =
                                    ledgers.putIfAbsent(handle.getId(), new OwnedLedger(exactHandle, handle));
                            if (previous != null) {
                                closeQuietly(handle);
                                observer.complete(ProviderMutationResultV1.fencedOrConflict());
                                return;
                            }
                            observer.complete(ProviderMutationResultV1.appliedExact(exactHandle));
                        } finally {
                            lease.resolveTerminal();
                        }
                    });
        } catch (RuntimeException failure) {
            lease.resolveTerminal();
            observer.completeExceptionally(failure);
        }
        return observer;
    }

    @Override
    public CompletionStage<RunLedgerOpenResultV1> openRunLedger(RunLedgerHandleV1 expectedHandle) {
        requireHandle(expectedHandle);
        CellSessionOperationRegistry.OperationLease lease = operations.acceptOperation();
        CompletableFuture<RunLedgerOpenResultV1> observer = new CompletableFuture<>();
        OwnedLedger cached = ledgers.get(expectedHandle.ledgerIdentity().ledgerId());
        if (cached != null) {
            try {
                boolean exact =
                        cached.handle().equals(expectedHandle) && metadataMatches(cached.sdkHandle(), expectedHandle);
                observer.complete(
                        exact
                                ? RunLedgerOpenResultV1.openedExact(expectedHandle)
                                : RunLedgerOpenResultV1.withoutHandle(RunLedgerOpenOutcomeV1.CONFIGURATION_MISMATCH));
            } finally {
                lease.resolveTerminal();
            }
            return observer;
        }

        try {
            client.newOpenLedgerOp()
                    .withLedgerId(expectedHandle.ledgerIdentity().ledgerId())
                    .withDigestType(digestType(capability.digestType()))
                    .withPassword(password)
                    .withRecovery(false)
                    .execute()
                    .whenComplete((handle, failure) -> {
                        try {
                            if (failure != null) {
                                observer.complete(openFailure(failure));
                                return;
                            }
                            if (!metadataMatches(handle, expectedHandle)) {
                                closeQuietly(handle);
                                observer.complete(RunLedgerOpenResultV1.withoutHandle(
                                        RunLedgerOpenOutcomeV1.CONFIGURATION_MISMATCH));
                                return;
                            }
                            OwnedLedger previous = ledgers.putIfAbsent(
                                    expectedHandle.ledgerIdentity().ledgerId(),
                                    new OwnedLedger(expectedHandle, handle));
                            if (previous != null) {
                                closeQuietly(handle);
                                if (!previous.handle().equals(expectedHandle)) {
                                    observer.complete(RunLedgerOpenResultV1.withoutHandle(
                                            RunLedgerOpenOutcomeV1.CONFIGURATION_MISMATCH));
                                    return;
                                }
                            }
                            observer.complete(RunLedgerOpenResultV1.openedExact(expectedHandle));
                        } finally {
                            lease.resolveTerminal();
                        }
                    });
        } catch (RuntimeException failure) {
            lease.resolveTerminal();
            observer.completeExceptionally(failure);
        }
        return observer;
    }

    @Override
    public CompletionStage<ProviderMutationResultV1<AppendQuorumProofV1>> appendExplicitEntry(
            RunLedgerAppendRequestV1 request) {
        Objects.requireNonNull(request, "request");
        requireHandle(request.handle());
        if (request.payload().readableBytes() > capability.maximumAddPayloadBytes()) {
            throw new IllegalArgumentException("append payload exceeds the admitted BookKeeper frame limit");
        }
        OwnedLedger owned = ledgers.get(request.handle().ledgerIdentity().ledgerId());
        if (owned == null
                || !owned.handle().equals(request.handle())
                || !(owned.sdkHandle() instanceof WriteAdvHandle)) {
            return CompletableFuture.completedFuture(ProviderMutationResultV1.fencedOrConflict());
        }

        CellSessionOperationRegistry.OperationLease lease = operations.acceptAppend(request.payload());
        CompletableFuture<ProviderMutationResultV1<AppendQuorumProofV1>> observer = new CompletableFuture<>();
        CanonicalBytes bytes = canonicalBytes(request.payload().readOnlyBuffer());
        EntryIdentity identity =
                new EntryIdentity(request.handle().ledgerIdentity().ledgerId(), request.expectedEntryId());
        try {
            ((WriteAdvHandle) owned.sdkHandle())
                    .writeAsync(request.expectedEntryId(), bytes.toByteArray())
                    .whenComplete((entryId, failure) -> {
                        if (failure == null && entryId == request.expectedEntryId()) {
                            lease.resolveTerminal();
                            observer.complete(ProviderMutationResultV1.appliedExact(new AppendQuorumProofV1(
                                    request.handle(),
                                    request.expectedEntryId(),
                                    bytes.length(),
                                    Sha256Digest.hash(bytes),
                                    capability.ackQuorumSize())));
                            return;
                        }
                        if (failure == null) {
                            lease.resolveTerminal();
                            observer.complete(ProviderMutationResultV1.fencedOrConflict());
                            return;
                        }
                        ProviderMutationResultV1<AppendQuorumProofV1> classified = appendFailure(failure);
                        if (classified.outcome()
                                == com.nereusstream.storage.api.bookkeeper.ProviderMutationOutcomeV1.OUTCOME_UNKNOWN) {
                            UnknownAppend pending =
                                    new UnknownAppend(request.handle(), bytes, capability.ackQuorumSize(), lease);
                            UnknownAppend previous = unknownAppends.putIfAbsent(identity, pending);
                            if (previous != null) {
                                lease.resolveTerminal();
                                observer.complete(ProviderMutationResultV1.fencedOrConflict());
                                return;
                            }
                        } else {
                            lease.resolveTerminal();
                        }
                        observer.complete(classified);
                    });
        } catch (RuntimeException failure) {
            lease.resolveTerminal();
            observer.completeExceptionally(failure);
        }
        return observer;
    }

    @Override
    public CompletionStage<RunLedgerReadResultV1> readExactEntry(RunLedgerHandleV1 handle, long entryId) {
        requireHandle(handle);
        if (entryId < 0) {
            throw new IllegalArgumentException("entry ID must be non-negative");
        }
        OwnedLedger owned = ledgers.get(handle.ledgerIdentity().ledgerId());
        if (owned == null || !owned.handle().equals(handle)) {
            return CompletableFuture.completedFuture(
                    RunLedgerReadResultV1.withoutEntry(RunLedgerReadOutcomeV1.PROVIDER_FAILURE));
        }
        if (owned.sdkHandle().isClosed() && entryId > owned.sdkHandle().getLastAddConfirmed()) {
            return CompletableFuture.completedFuture(
                    RunLedgerReadResultV1.withoutEntry(RunLedgerReadOutcomeV1.DEFINITIVELY_ABSENT));
        }

        CellSessionOperationRegistry.OperationLease lease = operations.acceptOperation();
        CompletableFuture<RunLedgerReadResultV1> observer = new CompletableFuture<>();
        try {
            owned.sdkHandle().readAsync(entryId, entryId).whenComplete((entries, failure) -> {
                try {
                    if (failure != null) {
                        RunLedgerReadResultV1 result = readFailure(failure);
                        resolveUnknownFromAbsence(handle, entryId, result);
                        observer.complete(result);
                        return;
                    }
                    CanonicalBytes payload;
                    try (LedgerEntries closeableEntries = entries) {
                        LedgerEntry entry = closeableEntries.getEntry(entryId);
                        payload = CanonicalBytes.copyOf(entry.getEntryBytes());
                    }
                    ExactLedgerEntryV1 exact =
                            new ExactLedgerEntryV1(handle, entryId, payload, Sha256Digest.hash(payload));
                    resolveUnknownFromExact(exact);
                    observer.complete(RunLedgerReadResultV1.foundExact(exact));
                } finally {
                    lease.resolveTerminal();
                }
            });
        } catch (RuntimeException failure) {
            lease.resolveTerminal();
            observer.completeExceptionally(failure);
        }
        return observer;
    }

    @Override
    public CompletionStage<ProviderMutationResultV1<RunLedgerRecoveryProofV1>> fenceAndRecoverRunLedger(
            RunLedgerHandleV1 handle) {
        requireHandle(handle);
        CellSessionOperationRegistry.OperationLease lease = operations.acceptOperation();
        CompletableFuture<ProviderMutationResultV1<RunLedgerRecoveryProofV1>> observer = new CompletableFuture<>();
        try {
            client.newOpenLedgerOp()
                    .withLedgerId(handle.ledgerIdentity().ledgerId())
                    .withDigestType(digestType(capability.digestType()))
                    .withPassword(password)
                    .withRecovery(true)
                    .execute()
                    .whenComplete((recovered, failure) -> {
                        if (failure != null) {
                            lease.resolveTerminal();
                            observer.complete(recoveryFailure(failure));
                            return;
                        }
                        if (!metadataMatches(recovered, handle)) {
                            closeQuietly(recovered);
                            lease.resolveTerminal();
                            observer.complete(ProviderMutationResultV1.fencedOrConflict());
                            return;
                        }
                        OwnedLedger replacement = new OwnedLedger(handle, recovered);
                        OwnedLedger previous =
                                ledgers.put(handle.ledgerIdentity().ledgerId(), replacement);
                        if (previous != null && previous.sdkHandle() != recovered) {
                            closeQuietly(previous.sdkHandle());
                        }
                        long lastAddConfirmed = recovered.getLastAddConfirmed();
                        reconcileUnknownAfterRecovery(recovered, handle, lastAddConfirmed)
                                .whenComplete((ignored, error) -> {
                                    lease.resolveTerminal();
                                    if (error != null) {
                                        observer.complete(ProviderMutationResultV1.outcomeUnknown());
                                        return;
                                    }
                                    observer.complete(ProviderMutationResultV1.appliedExact(
                                            new RunLedgerRecoveryProofV1(handle, lastAddConfirmed, true, true)));
                                });
                    });
        } catch (RuntimeException failure) {
            lease.resolveTerminal();
            observer.completeExceptionally(failure);
        }
        return observer;
    }

    @Override
    public CompletionStage<ProviderMutationResultV1<RunLedgerCloseProofV1>> closeRunLedger(RunLedgerHandleV1 handle) {
        requireHandle(handle);
        OwnedLedger owned = ledgers.get(handle.ledgerIdentity().ledgerId());
        if (owned == null || !owned.handle().equals(handle)) {
            return CompletableFuture.completedFuture(ProviderMutationResultV1.fencedOrConflict());
        }
        CellSessionOperationRegistry.OperationLease lease = operations.acceptOperation();
        CompletableFuture<ProviderMutationResultV1<RunLedgerCloseProofV1>> observer = new CompletableFuture<>();
        long lastAddConfirmed = owned.sdkHandle().getLastAddConfirmed();
        try {
            owned.sdkHandle().closeAsync().whenComplete((ignored, failure) -> {
                try {
                    if (failure != null) {
                        observer.complete(closeFailure(failure));
                        return;
                    }
                    ledgers.remove(handle.ledgerIdentity().ledgerId(), owned);
                    observer.complete(
                            ProviderMutationResultV1.appliedExact(new RunLedgerCloseProofV1(handle, lastAddConfirmed)));
                } finally {
                    lease.resolveTerminal();
                }
            });
        } catch (RuntimeException failure) {
            lease.resolveTerminal();
            observer.completeExceptionally(failure);
        }
        return observer;
    }

    @Override
    public CompletionStage<Void> drain() {
        return operations.drain();
    }

    @Override
    public CompletionStage<Void> closeAsync() {
        CompletableFuture<Void> existing = closeFuture.get();
        if (existing != null) {
            return existing;
        }
        CompletableFuture<Void> closing = new CompletableFuture<>();
        if (!closeFuture.compareAndSet(null, closing)) {
            return closeFuture.get();
        }
        operations.closeAsync().whenComplete((ignored, drainFailure) -> {
            if (drainFailure != null) {
                closing.completeExceptionally(unwrap(drainFailure));
                return;
            }
            CompletableFuture<?>[] handleCloses = ledgers.values().stream()
                    .map(OwnedLedger::sdkHandle)
                    .distinct()
                    .map(Handle::closeAsync)
                    .toArray(CompletableFuture[]::new);
            CompletableFuture.allOf(handleCloses).whenComplete((closed, closeFailure) -> {
                if (closeFailure != null) {
                    closing.completeExceptionally(unwrap(closeFailure));
                    return;
                }
                ledgers.clear();
                closing.complete(null);
            });
        });
        return closing;
    }

    int ownedHandleCount() {
        return ledgers.size();
    }

    int unknownAppendCount() {
        return unknownAppends.size();
    }

    static Map<String, byte[]> metadata(RunLedgerConfigurationV1 configuration) {
        Map<String, byte[]> metadata = new LinkedHashMap<>();
        metadata.put(METADATA_SCHEMA, SCHEMA_V1.clone());
        metadata.put(
                METADATA_PROVIDER_SCOPE,
                configuration.providerScopeId().digest().bytes().toByteArray());
        metadata.put(METADATA_RUN_ID, configuration.runId().value().bytes().toByteArray());
        metadata.put(
                METADATA_CONFIGURATION,
                configuration.configurationDigest().bytes().toByteArray());
        return Map.copyOf(metadata);
    }

    static boolean metadataMatches(LedgerMetadata metadata, RunLedgerHandleV1 expected) {
        Map<String, byte[]> actual = metadata.getCustomMetadata();
        return metadata.getLedgerId() == expected.ledgerIdentity().ledgerId()
                && Arrays.equals(actual.get(METADATA_SCHEMA), SCHEMA_V1)
                && Arrays.equals(
                        actual.get(METADATA_PROVIDER_SCOPE),
                        expected.providerScopeId().digest().bytes().toByteArray())
                && Arrays.equals(
                        actual.get(METADATA_RUN_ID),
                        expected.runId().value().bytes().toByteArray())
                && Arrays.equals(
                        actual.get(METADATA_CONFIGURATION),
                        expected.configurationDigest().bytes().toByteArray());
    }

    private boolean metadataMatches(Handle handle, RunLedgerHandleV1 expected) {
        LedgerMetadata metadata = handle.getLedgerMetadata();
        return metadataMatches(metadata, expected)
                && metadata.getEnsembleSize() == capability.ensembleSize()
                && metadata.getWriteQuorumSize() == capability.writeQuorumSize()
                && metadata.getAckQuorumSize() == capability.ackQuorumSize()
                && metadata.getDigestType() == digestType(capability.digestType());
    }

    private CompletionStage<Void> reconcileUnknownAfterRecovery(
            ReadHandle recovered, RunLedgerHandleV1 handle, long lastAddConfirmed) {
        CompletableFuture<?>[] resolvers = unknownAppends.entrySet().stream()
                .filter(entry ->
                        entry.getKey().ledgerId() == handle.ledgerIdentity().ledgerId())
                .map(entry -> resolveRecoveredEntry(recovered, entry.getKey(), entry.getValue(), lastAddConfirmed))
                .toArray(CompletableFuture[]::new);
        return CompletableFuture.allOf(resolvers);
    }

    private CompletableFuture<Void> resolveRecoveredEntry(
            ReadHandle recovered, EntryIdentity identity, UnknownAppend unknown, long lastAddConfirmed) {
        if (identity.entryId() > lastAddConfirmed) {
            unknownAppends.remove(identity, unknown);
            unknown.lease().resolveTerminal();
            return CompletableFuture.completedFuture(null);
        }
        return recovered.readAsync(identity.entryId(), identity.entryId()).thenAccept(entries -> {
            try (LedgerEntries closeableEntries = entries) {
                CanonicalBytes actual = CanonicalBytes.copyOf(
                        closeableEntries.getEntry(identity.entryId()).getEntryBytes());
                unknownAppends.remove(identity, unknown);
                unknown.lease().resolveTerminal();
                if (!actual.equals(unknown.payload())) {
                    throw new CompletionException(
                            new IllegalStateException("response-unknown entry resolved to conflicting bytes"));
                }
            }
        });
    }

    private void resolveUnknownFromExact(ExactLedgerEntryV1 exact) {
        EntryIdentity identity =
                new EntryIdentity(exact.handle().ledgerIdentity().ledgerId(), exact.entryId());
        UnknownAppend unknown = unknownAppends.remove(identity);
        if (unknown != null) {
            unknown.lease().resolveTerminal();
        }
    }

    private void resolveUnknownFromAbsence(RunLedgerHandleV1 handle, long entryId, RunLedgerReadResultV1 result) {
        if (result.outcome() != RunLedgerReadOutcomeV1.DEFINITIVELY_ABSENT) {
            return;
        }
        UnknownAppend unknown =
                unknownAppends.remove(new EntryIdentity(handle.ledgerIdentity().ledgerId(), entryId));
        if (unknown != null) {
            unknown.lease().resolveTerminal();
        }
    }

    private void requireConfiguration(RunLedgerConfigurationV1 configuration) {
        Objects.requireNonNull(configuration, "configuration");
        if (!configuration.providerScopeId().equals(providerScopeId())
                || configuration.ensembleSize() != capability.ensembleSize()
                || configuration.writeQuorumSize() != capability.writeQuorumSize()
                || configuration.ackQuorumSize() != capability.ackQuorumSize()
                || configuration.digestType() != capability.digestType()
                || !configuration.configurationDigest().equals(capability.configurationDigest())) {
            throw new IllegalArgumentException("run configuration differs from the admitted Cell capability");
        }
    }

    private void requireHandle(RunLedgerHandleV1 handle) {
        Objects.requireNonNull(handle, "handle");
        if (!handle.providerScopeId().equals(providerScopeId())
                || !handle.configurationDigest().equals(capability.configurationDigest())) {
            throw new IllegalArgumentException("run handle differs from the admitted Cell capability");
        }
    }

    private static void requireSupportedCapability(BookKeeperCapabilitySnapshotV1 capability) {
        if (capability.protocolMode() != BookKeeperProtocolModeV1.V3) {
            throw new IllegalArgumentException("real M2 provider requires BookKeeper protocol v3");
        }
        int maximum = BookKeeperV3Crc32cAddPayloadLimitV1.maximumAddPayloadBytes(
                capability.clientFrameLimitBytes(), capability.serverFrameLimitBytes());
        if (capability.digestType() != BookKeeperDigestTypeV1.CRC32C
                || capability.maximumAddPayloadBytes() != maximum) {
            throw new IllegalArgumentException("real M2 provider requires the exact v3 CRC32C frame projection");
        }
    }

    private RunLedgerHandleV1 exactHandle(RunLedgerConfigurationV1 configuration, long ledgerId) {
        return new RunLedgerHandleV1(
                configuration.providerScopeId(),
                configuration.runId(),
                new BookKeeperLedgerIdentity(ledgerId),
                configuration.configurationDigest());
    }

    private static CanonicalBytes canonicalBytes(ByteBuffer source) {
        ByteBuffer copy = source.asReadOnlyBuffer();
        byte[] bytes = new byte[copy.remaining()];
        copy.get(bytes);
        return CanonicalBytes.copyOf(bytes);
    }

    private static DigestType digestType(BookKeeperDigestTypeV1 digestType) {
        return switch (digestType) {
            case MAC -> DigestType.MAC;
            case CRC32 -> DigestType.CRC32;
            case CRC32C -> DigestType.CRC32C;
        };
    }

    private static ProviderMutationResultV1<RunLedgerHandleV1> createFailure(Throwable failure) {
        int code = code(failure);
        if (code == BKException.Code.LedgerExistException) {
            return ProviderMutationResultV1.fencedOrConflict();
        }
        if (code == BKException.Code.IncorrectParameterException
                || code == BKException.Code.UnauthorizedAccessException) {
            return ProviderMutationResultV1.definitelyNotApplied();
        }
        return ProviderMutationResultV1.outcomeUnknown();
    }

    private static RunLedgerOpenResultV1 openFailure(Throwable failure) {
        int code = code(failure);
        if (code == BKException.Code.NoSuchLedgerExistsException
                || code == BKException.Code.NoSuchLedgerExistsOnMetadataServerException) {
            return RunLedgerOpenResultV1.withoutHandle(RunLedgerOpenOutcomeV1.ABSENT);
        }
        if (code == BKException.Code.LedgerFencedException || code == BKException.Code.LedgerClosedException) {
            return RunLedgerOpenResultV1.withoutHandle(RunLedgerOpenOutcomeV1.FENCED);
        }
        if (code == BKException.Code.DigestMatchException || code == BKException.Code.UnauthorizedAccessException) {
            return RunLedgerOpenResultV1.withoutHandle(RunLedgerOpenOutcomeV1.CONFIGURATION_MISMATCH);
        }
        return RunLedgerOpenResultV1.withoutHandle(RunLedgerOpenOutcomeV1.PROVIDER_FAILURE);
    }

    private static ProviderMutationResultV1<AppendQuorumProofV1> appendFailure(Throwable failure) {
        int code = code(failure);
        if (code == BKException.Code.LedgerFencedException
                || code == BKException.Code.LedgerClosedException
                || code == BKException.Code.DuplicateEntryIdException
                || code == BKException.Code.UnauthorizedAccessException) {
            return ProviderMutationResultV1.fencedOrConflict();
        }
        if (code == BKException.Code.NoSuchLedgerExistsException
                || code == BKException.Code.NoSuchLedgerExistsOnMetadataServerException
                || code == BKException.Code.IncorrectParameterException) {
            return ProviderMutationResultV1.definitelyNotApplied();
        }
        return ProviderMutationResultV1.outcomeUnknown();
    }

    private static RunLedgerReadResultV1 readFailure(Throwable failure) {
        int code = code(failure);
        if (code == BKException.Code.NoSuchEntryException
                || code == BKException.Code.NoSuchLedgerExistsException
                || code == BKException.Code.NoSuchLedgerExistsOnMetadataServerException) {
            return RunLedgerReadResultV1.withoutEntry(RunLedgerReadOutcomeV1.DEFINITIVELY_ABSENT);
        }
        if (code == BKException.Code.LedgerFencedException || code == BKException.Code.LedgerClosedException) {
            return RunLedgerReadResultV1.withoutEntry(RunLedgerReadOutcomeV1.FENCED);
        }
        return RunLedgerReadResultV1.withoutEntry(RunLedgerReadOutcomeV1.PROVIDER_FAILURE);
    }

    private static ProviderMutationResultV1<RunLedgerRecoveryProofV1> recoveryFailure(Throwable failure) {
        int code = code(failure);
        if (code == BKException.Code.NoSuchLedgerExistsException
                || code == BKException.Code.NoSuchLedgerExistsOnMetadataServerException
                || code == BKException.Code.DigestMatchException
                || code == BKException.Code.UnauthorizedAccessException) {
            return ProviderMutationResultV1.fencedOrConflict();
        }
        return ProviderMutationResultV1.outcomeUnknown();
    }

    private static ProviderMutationResultV1<RunLedgerCloseProofV1> closeFailure(Throwable failure) {
        int code = code(failure);
        if (code == BKException.Code.LedgerFencedException
                || code == BKException.Code.LedgerClosedException
                || code == BKException.Code.UnauthorizedAccessException) {
            return ProviderMutationResultV1.fencedOrConflict();
        }
        if (code == BKException.Code.NoSuchLedgerExistsException
                || code == BKException.Code.NoSuchLedgerExistsOnMetadataServerException) {
            return ProviderMutationResultV1.definitelyNotApplied();
        }
        return ProviderMutationResultV1.outcomeUnknown();
    }

    private static int code(Throwable failure) {
        Throwable unwrapped = unwrap(failure);
        return unwrapped instanceof BKException exception ? exception.getCode() : BKException.Code.UNINITIALIZED;
    }

    private static Throwable unwrap(Throwable failure) {
        Throwable current = failure;
        while ((current instanceof CompletionException || current instanceof java.util.concurrent.ExecutionException)
                && current.getCause() != null) {
            current = current.getCause();
        }
        return current;
    }

    private static void closeQuietly(Handle handle) {
        handle.closeAsync().exceptionally(failure -> null);
    }

    private record OwnedLedger(RunLedgerHandleV1 handle, ReadHandle sdkHandle) {}

    private record EntryIdentity(long ledgerId, long entryId) {}

    private record UnknownAppend(
            RunLedgerHandleV1 handle,
            CanonicalBytes payload,
            int acknowledgedBookies,
            CellSessionOperationRegistry.OperationLease lease) {}
}
