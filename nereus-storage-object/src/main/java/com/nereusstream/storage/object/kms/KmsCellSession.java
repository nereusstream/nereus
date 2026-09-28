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

package com.nereusstream.storage.object.kms;

import com.nereusstream.domain.bytes.CanonicalBytes;
import com.nereusstream.domain.bytes.Sha256Digest;
import com.nereusstream.storage.api.bookkeeper.CellProviderScopeId;
import com.nereusstream.storage.object.control.WalLaneId;
import com.nereusstream.storage.object.control.WalRunObjectSession;
import com.nereusstream.storage.object.control.WalRunRootRecord;
import com.nereusstream.storage.object.nwg1.GroupEncodingPlanV1;
import com.nereusstream.storage.object.nwg1.Nwg1ObjectReaderV1;
import com.nereusstream.storage.object.nwg1.Nwg1ObjectVerifierV1;
import com.nereusstream.storage.object.nwg1.Nwg1RootAuthorityV1;
import com.nereusstream.storage.object.nwg1.Nwg1SealedObjectV1;
import com.nereusstream.storage.object.nwg1.Nwg1VerificationContextV1;
import com.nereusstream.storage.object.nwg1.Nwg1VerificationPathV1;
import com.nereusstream.storage.object.recovery.WalRunLineageRecovery;
import java.io.IOException;
import java.security.SecureRandom;
import java.util.Arrays;
import java.util.LinkedHashMap;
import java.util.LinkedHashSet;
import java.util.Map;
import java.util.Objects;
import java.util.Set;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.ExecutionException;

/**
 * Cell-local bounded multi-run key cache. Each retired admission generation retains its CLOSED run identities
 * until its live leases and actual operations drain. Successors share one cache-slot and operation budget; an old
 * raw facade never routes key operations into its successor. Remote KMS and codec work run outside the state lock.
 */
public final class KmsCellSession implements AutoCloseable {
    public enum State {
        OPEN,
        DRAINING,
        CLOSED
    }

    private enum RunLifecycleOwner {
        NEW_ROOT,
        RECOVERY,
        WAL_RUN,
        CLOSED
    }

    private final KmsTransport transport;
    private final CellProviderScopeId providerScopeId;
    private final String allowedKeyIdentity;
    private final int maximumCachedRunKeys;
    private final SecureRandom random;
    private final CellBudget budget;
    private final Map<RunKeyCacheIdentity, PendingKeyLoad> pending = new LinkedHashMap<>();
    private final Map<RunKeyCacheIdentity, Integer> operations = new LinkedHashMap<>();
    private final Map<RunKeyCacheIdentity, CachedRunKey> cache = new LinkedHashMap<>();
    private final Map<RunKeyCacheIdentity, RunLifecycleOwner> runOwners = new LinkedHashMap<>();
    private State state = State.OPEN;
    private long wrapCalls;
    private long unwrapCalls;

    public KmsCellSession(
            KmsTransport borrowedTransport,
            CellProviderScopeId providerScopeId,
            String allowedKeyIdentity,
            int maximumCachedRunKeys,
            SecureRandom random) {
        this.transport = Objects.requireNonNull(borrowedTransport, "borrowedTransport");
        this.providerScopeId = Objects.requireNonNull(providerScopeId, "providerScopeId");
        if (allowedKeyIdentity == null || allowedKeyIdentity.isBlank() || allowedKeyIdentity.indexOf('\0') >= 0) {
            throw new IllegalArgumentException("allowed KMS key identity is invalid");
        }
        if (maximumCachedRunKeys <= 0) {
            throw new IllegalArgumentException("maximumCachedRunKeys must be positive");
        }
        this.allowedKeyIdentity = allowedKeyIdentity;
        this.maximumCachedRunKeys = maximumCachedRunKeys;
        this.random = Objects.requireNonNull(random, "random");
        this.budget = new CellBudget();
        budget.current = this;
        budget.generations.add(this);
    }

    private KmsCellSession(KmsCellSession previous) {
        transport = previous.transport;
        providerScopeId = previous.providerScopeId;
        allowedKeyIdentity = previous.allowedKeyIdentity;
        maximumCachedRunKeys = previous.maximumCachedRunKeys;
        random = previous.random;
        budget = previous.budget;
    }

    /** State-only monitor used by the existing atomic Provider/KMS authority-transfer barrier. */
    public Object lifecycleMonitor() {
        return budget;
    }

    /** Selects a bounded successor only inside an unforgeable Root authority transfer. */
    private KmsCellSession forRunAdmission() {
        synchronized (budget) {
            requireCellBudgetOpen();
            var current = budget.current;
            if (current.runOwners.size() < maximumCachedRunKeys) {
                return current;
            }
            if ((long) budget.generations.size() >= (long) maximumCachedRunKeys + 1) {
                throw new IllegalStateException(
                        "KMS admission generation capacity reached; admission must backpressure");
            }
            var successor = new KmsCellSession(current);
            budget.current = successor;
            budget.generations.add(successor);
            current.state = State.DRAINING;
            current.finishRetiredGeneration();
            return successor;
        }
    }

    /** Generates one random run key and wraps it once; per-Object wrapping is not exposed. */
    public WrappedRunKeyEnvelope createRunKey(RunKeyCacheIdentity runIdentity) {
        try (var operation = beginOperation(runIdentity, null)) {
            var loading = new PendingKeyLoad(null);
            synchronized (budget) {
                if (cache.containsKey(runIdentity) || pending.containsKey(runIdentity)) {
                    throw new IllegalStateException("WalRun key already exists in this Cell session");
                }
                requireCacheCapacity();
                budget.runSlots++;
                pending.put(runIdentity, loading);
            }
            return wrapNewRunKey(operation, loading);
        }
    }

    /**
     * Reserves one shared run slot before Root publication. The returned handle owns exactly this new key until an
     * exact published Root transfers it to a WalRun lease or the caller abandons the candidate.
     */
    public NewRunKeyCreation beginNewRunKey(RunKeyCacheIdentity runIdentity) {
        Objects.requireNonNull(runIdentity, "runIdentity");
        KmsCellSession admission;
        PendingKeyLoad loading = new PendingKeyLoad(null);
        synchronized (budget) {
            requireCellBudgetOpen();
            budget.current.requireRawRunOwner(runIdentity);
            if (budget.current.cache.containsKey(runIdentity)
                    || budget.current.pending.containsKey(runIdentity)
                    || budget.current.operations.containsKey(runIdentity)) {
                throw new IllegalStateException("new run key requires a pristine run identity");
            }
            requireCacheCapacity();
            if (budget.activeOperations >= maximumCachedRunKeys) {
                throw new IllegalStateException("KMS active operation capacity reached; admission must backpressure");
            }
            admission = forRunAdmission();
            admission.requireOpen();
            admission.requireTransferHistoryCapacity();
            budget.runSlots++;
            admission.runOwners.put(runIdentity, RunLifecycleOwner.NEW_ROOT);
            admission.pending.put(runIdentity, loading);
        }
        try (var operation = admission.beginOperation(runIdentity, RunLifecycleOwner.NEW_ROOT)) {
            return admission.new NewRunKeyCreation(runIdentity, admission.wrapNewRunKey(operation, loading));
        } catch (RuntimeException | Error failure) {
            admission.failKeyLoad(runIdentity, loading, failure);
            synchronized (budget) {
                admission.releaseUnpublishedNewRunKey(runIdentity);
            }
            throw failure;
        }
    }

    private WrappedRunKeyEnvelope wrapNewRunKey(KeyOperation operation, PendingKeyLoad loading) {
        byte[] plaintext = new byte[ObjectKeyDerivationV1.RUN_KEY_BYTES];
        byte[] requestKey = null;
        try {
            random.nextBytes(plaintext);
            requestKey = plaintext.clone();
            synchronized (budget) {
                wrapCalls = Math.incrementExact(wrapCalls);
            }
            var envelope = transport.wrap(allowedKeyIdentity, requestKey);
            requireEnvelopeIdentity(envelope);
            synchronized (budget) {
                operation.requireWrapResultUsable();
                cache.put(operation.runIdentity, new CachedRunKey(envelope, plaintext.clone()));
                pending.remove(operation.runIdentity);
            }
            loading.completion.complete(null);
            return envelope;
        } catch (RuntimeException | Error failure) {
            failKeyLoad(operation.runIdentity, loading, failure);
            throw failure;
        } finally {
            if (requestKey != null) {
                Arrays.fill(requestKey, (byte) 0);
            }
            Arrays.fill(plaintext, (byte) 0);
        }
    }

    public CanonicalBytes deriveObjectKey(
            RunKeyCacheIdentity runIdentity,
            WrappedRunKeyEnvelope envelope,
            Sha256Digest rootSha256,
            WalLaneId laneId,
            long laneSequence) {
        Objects.requireNonNull(rootSha256, "rootSha256");
        Objects.requireNonNull(laneId, "laneId");
        try (var operation = acquireKey(runIdentity, envelope, null)) {
            var result = ObjectKeyDerivationV1.derive(
                    operation.key,
                    rootSha256,
                    runIdentity.shardId(),
                    runIdentity.shardRunEpoch(),
                    laneId,
                    laneSequence);
            operation.requireResultUsable();
            return result;
        }
    }

    /** Seals and self-verifies NWG1 with one operation-owned key copy, erased before return. */
    public Nwg1SealedObjectV1 sealNwg1(
            RunKeyCacheIdentity runIdentity,
            WrappedRunKeyEnvelope envelope,
            GroupEncodingPlanV1 plan,
            long laneSequence,
            Nwg1VerificationContextV1 verificationContext) {
        return sealNwg1Internal(runIdentity, envelope, plan, laneSequence, verificationContext, null);
    }

    private Nwg1SealedObjectV1 sealNwg1Internal(
            RunKeyCacheIdentity runIdentity,
            WrappedRunKeyEnvelope envelope,
            GroupEncodingPlanV1 plan,
            long laneSequence,
            Nwg1VerificationContextV1 verificationContext,
            RunLifecycleOwner owner) {
        Objects.requireNonNull(plan, "plan");
        Objects.requireNonNull(verificationContext, "verificationContext");
        try (var operation = acquireKey(runIdentity, envelope, owner)) {
            var result = com.nereusstream.storage.object.nwg1.Nwg1ObjectWriterV1.seal(
                    plan, laneSequence, operation.key, verificationContext);
            operation.requireResultUsable();
            return result;
        }
    }

    public Nwg1ObjectReaderV1.DecodedObject verifyNwg1(
            RunKeyCacheIdentity runIdentity,
            WrappedRunKeyEnvelope envelope,
            Nwg1VerificationPathV1 path,
            Nwg1RootAuthorityV1 rootAuthority,
            Nwg1VerificationContextV1 verificationContext,
            byte[] relativeLeafUtf8,
            CanonicalBytes canonicalBody,
            long selectedFrameOrdinal) {
        return verifyNwg1Internal(
                runIdentity,
                envelope,
                path,
                rootAuthority,
                verificationContext,
                relativeLeafUtf8,
                canonicalBody,
                selectedFrameOrdinal,
                null);
    }

    private Nwg1ObjectReaderV1.DecodedObject verifyNwg1Internal(
            RunKeyCacheIdentity runIdentity,
            WrappedRunKeyEnvelope envelope,
            Nwg1VerificationPathV1 path,
            Nwg1RootAuthorityV1 rootAuthority,
            Nwg1VerificationContextV1 verificationContext,
            byte[] relativeLeafUtf8,
            CanonicalBytes canonicalBody,
            long selectedFrameOrdinal,
            RunLifecycleOwner owner) {
        Objects.requireNonNull(path, "path");
        Objects.requireNonNull(rootAuthority, "rootAuthority");
        Objects.requireNonNull(verificationContext, "verificationContext");
        Objects.requireNonNull(relativeLeafUtf8, "relativeLeafUtf8");
        Objects.requireNonNull(canonicalBody, "canonicalBody");
        try (var operation = acquireKey(runIdentity, envelope, owner)) {
            var result = Nwg1ObjectVerifierV1.verify(new Nwg1ObjectVerifierV1.Request(
                    path,
                    rootAuthority,
                    verificationContext,
                    relativeLeafUtf8,
                    canonicalBody.toByteArray(),
                    selectedFrameOrdinal,
                    ignoredEnvelope -> operation.key.clone()));
            operation.requireResultUsable();
            return result;
        }
    }

    public void evict(RunKeyCacheIdentity runIdentity) {
        synchronized (budget) {
            requireOpen();
            requireRawRunOwner(runIdentity);
            requireRunIdle(runIdentity);
            CachedRunKey removed = cache.remove(runIdentity);
            if (removed != null) {
                removed.erase();
                budget.runSlots--;
            }
        }
    }

    public void drain() {
        synchronized (budget) {
            closeCellInternal();
        }
    }

    private void closeCellInternal() {
        if (budget.closed) {
            return;
        }
        if (budget.activeOperations != 0) {
            throw new IllegalStateException("KMS Cell retains actual key operations; drain before close");
        }
        for (var generation : budget.generations) {
            generation.requireNoLiveRunLeases();
        }
        budget.closed = true;
        for (var generation : budget.generations) {
            generation.eraseAll();
            generation.runOwners.clear();
            generation.state = State.CLOSED;
        }
        budget.runSlots = 0;
        budget.generations.clear();
    }

    @Override
    public void close() {
        synchronized (budget) {
            closeCellInternal();
        }
    }

    /**
     * Irreversibly transfers one exact run's key-operation authority to a Root-bound WalRun owner facade.
     *
     * <p>The authority value has a private constructor owned by {@link WalRunObjectSession}; callers cannot mint a
     * lease or select its run identity, Root, Provider scope, or wrapped envelope themselves.
     */
    public WalRunLease transferToWalRun(WalRunObjectSession.KmsOwnerAuthority authority) {
        synchronized (budget) {
            return forRunAdmission().transferToWalRunInGeneration(authority);
        }
    }

    private WalRunLease transferToWalRunInGeneration(WalRunObjectSession.KmsOwnerAuthority authority) {
        requireTransferReadyInGeneration(authority);
        WalRunLease lease = new WalRunLease(authority);
        if (!cache.containsKey(authority.runKeyIdentity())) {
            budget.runSlots++;
        }
        runOwners.put(authority.runKeyIdentity(), RunLifecycleOwner.WAL_RUN);
        return lease;
    }

    /** Checks the admission generation inside the Provider+KMS atomic owner-transfer barrier. */
    public void requireTransferReady(WalRunObjectSession.KmsOwnerAuthority authority) {
        synchronized (budget) {
            forRunAdmission().requireTransferReadyInGeneration(authority);
        }
    }

    private void requireTransferReadyInGeneration(WalRunObjectSession.KmsOwnerAuthority authority) {
        requireOpen();
        Objects.requireNonNull(authority, "authority");
        requireRawRunOwner(authority.runKeyIdentity());
        requireRunIdle(authority.runKeyIdentity());
        requireTransferHistoryCapacity();
        if (!providerScopeId.equals(authority.providerScopeId())) {
            throw new IllegalArgumentException("KMS WalRun authority belongs to another Provider scope");
        }
        requireEnvelopeIdentity(authority.wrappedRunKey());
        CachedRunKey existing = cache.get(authority.runKeyIdentity());
        if (existing != null && !existing.envelope.equals(authority.wrappedRunKey())) {
            throw new IllegalArgumentException("KMS WalRun authority rebound an already cached run envelope");
        }
        if (existing == null) {
            requireCacheCapacity();
        }
    }

    /** Checks the admission generation inside the aggregate Provider+KMS recovery-transfer barrier. */
    public void requireRecoveryTransferReady(WalRunLineageRecovery.KmsRecoveryAuthority authority) {
        synchronized (budget) {
            forRunAdmission().requireRecoveryTransferReadyInGeneration(authority);
        }
    }

    private void requireRecoveryTransferReadyInGeneration(WalRunLineageRecovery.KmsRecoveryAuthority authority) {
        requireOpen();
        Objects.requireNonNull(authority, "authority");
        RunKeyCacheIdentity runIdentity = authority.runKeyIdentity();
        requireRawRunOwner(runIdentity);
        requireRunIdle(runIdentity);
        requireTransferHistoryCapacity();
        if (cache.containsKey(runIdentity)) {
            throw new IllegalStateException("KMS recovery transfer requires a pristine target run cache slot");
        }
        requireExactKmsAuthority(
                runIdentity, authority.wrappedRunKey(), authority.rootSha256(), authority.providerScopeId());
        requireCacheCapacity();
    }

    /** Mints the recovery-only lease after the aggregate authority pair has been claimed. */
    public RecoveryLease transferToRecovery(WalRunLineageRecovery.KmsRecoveryTransfer transfer) {
        synchronized (budget) {
            return forRunAdmission().transferToRecoveryInGeneration(transfer);
        }
    }

    private RecoveryLease transferToRecoveryInGeneration(WalRunLineageRecovery.KmsRecoveryTransfer transfer) {
        requireOpen();
        Objects.requireNonNull(transfer, "transfer");
        RunKeyCacheIdentity runIdentity = transfer.runKeyIdentity();
        requireRawRunOwner(runIdentity);
        requireRunIdle(runIdentity);
        requireTransferHistoryCapacity();
        if (cache.containsKey(runIdentity)) {
            throw new IllegalStateException("KMS recovery transfer requires a pristine target run cache slot");
        }
        WrappedRunKeyEnvelope wrappedRunKey = transfer.wrappedRunKey();
        Sha256Digest rootSha256 = transfer.rootSha256();
        CellProviderScopeId exactScope = transfer.providerScopeId();
        requireExactKmsAuthority(runIdentity, wrappedRunKey, rootSha256, exactScope);
        requireCacheCapacity();
        RecoveryLease lease = new RecoveryLease(runIdentity, wrappedRunKey, rootSha256);
        transfer.consumeForLease();
        budget.runSlots++;
        runOwners.put(runIdentity, RunLifecycleOwner.RECOVERY);
        return lease;
    }

    public State state() {
        synchronized (budget) {
            return state;
        }
    }

    public int cachedRunKeyCount() {
        synchronized (budget) {
            return cache.size();
        }
    }

    public long wrapCalls() {
        synchronized (budget) {
            return wrapCalls;
        }
    }

    public long unwrapCalls() {
        synchronized (budget) {
            return unwrapCalls;
        }
    }

    public CellProviderScopeId providerScopeId() {
        return providerScopeId;
    }

    private void requireRawRunOwner(RunKeyCacheIdentity runIdentity) {
        Objects.requireNonNull(runIdentity, "runIdentity");
        for (var generation : budget.generations) {
            var owner = generation.runOwners.get(runIdentity);
            if (owner != null
                    || generation != this
                            && (generation.cache.containsKey(runIdentity)
                                    || generation.pending.containsKey(runIdentity)
                                    || generation.operations.containsKey(runIdentity))) {
                throw new IllegalStateException("KMS run authority was transferred or retired: " + owner);
            }
        }
    }

    private void requireTransferHistoryCapacity() {
        if (runOwners.size() >= maximumCachedRunKeys) {
            throw new IllegalStateException(
                    "KMS run-owner history capacity reached; select the current run-admission generation");
        }
    }

    private void requireExactKmsAuthority(
            RunKeyCacheIdentity runIdentity,
            WrappedRunKeyEnvelope wrappedRunKey,
            Sha256Digest rootSha256,
            CellProviderScopeId exactScope) {
        Objects.requireNonNull(runIdentity, "runIdentity");
        Objects.requireNonNull(rootSha256, "rootSha256");
        if (!providerScopeId.equals(exactScope)) {
            throw new IllegalArgumentException("KMS recovery authority belongs to another Provider scope");
        }
        requireEnvelopeIdentity(Objects.requireNonNull(wrappedRunKey, "wrappedRunKey"));
    }

    private void requireOpen() {
        if (state != State.OPEN) {
            throw new IllegalStateException("KMS Cell session no longer accepts operations: " + state);
        }
    }

    private void requireCacheCapacity() {
        if (budget.runSlots >= maximumCachedRunKeys) {
            throw new IllegalStateException("KMS run-key cache capacity reached; admission must backpressure");
        }
    }

    private void requireLeaseCacheCapacity(RunKeyCacheIdentity runIdentity) {
        if (!isLiveLeaseOwner(runOwners.get(runIdentity)) || budget.runSlots > maximumCachedRunKeys) {
            throw new IllegalStateException("KMS reserved run-key cache capacity was lost");
        }
    }

    private static boolean isLiveLeaseOwner(RunLifecycleOwner owner) {
        return owner == RunLifecycleOwner.NEW_ROOT
                || owner == RunLifecycleOwner.RECOVERY
                || owner == RunLifecycleOwner.WAL_RUN;
    }

    private void requireNoLiveRunLeases() {
        if (runOwners.values().stream().anyMatch(KmsCellSession::isLiveLeaseOwner)) {
            throw new IllegalStateException("KMS Cell cannot drain or close while a transferred run lease is live");
        }
    }

    private void closeRunLease(RunKeyCacheIdentity runIdentity, RunLifecycleOwner expectedOwner) {
        if (runOwners.get(runIdentity) != expectedOwner) {
            throw new IllegalStateException("KMS run lease no longer owns its exact cache identity");
        }
        requireRunIdle(runIdentity);
        CachedRunKey removed = cache.remove(runIdentity);
        if (removed != null) {
            removed.erase();
        }
        budget.runSlots--;
        runOwners.put(runIdentity, RunLifecycleOwner.CLOSED);
        finishRetiredGeneration();
    }

    private void releaseUnpublishedNewRunKey(RunKeyCacheIdentity runIdentity) {
        if (runOwners.get(runIdentity) != RunLifecycleOwner.NEW_ROOT) {
            throw new IllegalStateException("new run key no longer owns its exact cache identity");
        }
        requireRunIdle(runIdentity);
        CachedRunKey removed = cache.remove(runIdentity);
        if (removed != null) {
            removed.erase();
        }
        budget.runSlots--;
        runOwners.remove(runIdentity);
        finishRetiredGeneration();
    }

    private KeyOperation beginOperation(RunKeyCacheIdentity runIdentity, RunLifecycleOwner owner) {
        synchronized (budget) {
            requireOperationOwner(runIdentity, owner);
            if (budget.activeOperations >= maximumCachedRunKeys) {
                throw new IllegalStateException("KMS active operation capacity reached; admission must backpressure");
            }
            budget.activeOperations++;
            operations.merge(runIdentity, 1, Math::addExact);
            return new KeyOperation(runIdentity, owner);
        }
    }

    private KeyOperation acquireKey(
            RunKeyCacheIdentity runIdentity, WrappedRunKeyEnvelope envelope, RunLifecycleOwner owner) {
        var operation = beginOperation(runIdentity, owner);
        try {
            operation.key = loadKeyCopy(operation, envelope);
            return operation;
        } catch (RuntimeException | Error failure) {
            operation.close();
            throw failure;
        }
    }

    private byte[] loadKeyCopy(KeyOperation operation, WrappedRunKeyEnvelope envelope) {
        Objects.requireNonNull(envelope, "envelope");
        requireEnvelopeIdentity(envelope);
        PendingKeyLoad loading;
        boolean loader;
        synchronized (budget) {
            operation.requireResultUsable();
            var cached = cache.get(operation.runIdentity);
            if (cached != null) {
                requireCachedEnvelope(cached, envelope);
                return cached.plaintext.clone();
            }
            loading = pending.get(operation.runIdentity);
            loader = loading == null;
            if (loader) {
                if (operation.owner == null) {
                    requireCacheCapacity();
                    budget.runSlots++;
                } else {
                    requireLeaseCacheCapacity(operation.runIdentity);
                }
                loading = new PendingKeyLoad(envelope);
                pending.put(operation.runIdentity, loading);
                unwrapCalls = Math.incrementExact(unwrapCalls);
            } else if (!envelope.equals(loading.envelope)) {
                throw new IllegalArgumentException("WalRun pending key rebound a different KMS envelope");
            }
        }
        if (loader) {
            byte[] plaintext = null;
            try {
                plaintext = transport.unwrap(envelope);
                if (plaintext == null || plaintext.length != ObjectKeyDerivationV1.RUN_KEY_BYTES) {
                    throw new IllegalStateException("KMS unwrap did not return an exact 256-bit run key");
                }
                synchronized (budget) {
                    operation.requireResultUsable();
                    cache.put(operation.runIdentity, new CachedRunKey(envelope, plaintext.clone()));
                    pending.remove(operation.runIdentity);
                }
                loading.completion.complete(null);
            } catch (RuntimeException | Error failure) {
                failKeyLoad(operation.runIdentity, loading, failure);
                throw failure;
            } finally {
                if (plaintext != null) {
                    Arrays.fill(plaintext, (byte) 0);
                }
            }
        } else {
            try {
                loading.completion.get();
            } catch (InterruptedException failure) {
                Thread.currentThread().interrupt();
                throw new IllegalStateException("KMS key wait interrupted; actual unwrap remains owned", failure);
            } catch (ExecutionException failure) {
                if (failure.getCause() instanceof RuntimeException runtime) {
                    throw runtime;
                }
                if (failure.getCause() instanceof Error error) {
                    throw error;
                }
                throw new IllegalStateException("KMS pending unwrap failed", failure.getCause());
            }
        }
        synchronized (budget) {
            operation.requireResultUsable();
            var cached = Objects.requireNonNull(cache.get(operation.runIdentity), "completed run key");
            requireCachedEnvelope(cached, envelope);
            return cached.plaintext.clone();
        }
    }

    private void failKeyLoad(RunKeyCacheIdentity runIdentity, PendingKeyLoad loading, Throwable failure) {
        synchronized (budget) {
            if (pending.remove(runIdentity, loading) && !isLiveLeaseOwner(runOwners.get(runIdentity))) {
                budget.runSlots--;
            }
        }
        loading.completion.completeExceptionally(failure);
    }

    private void requireCachedEnvelope(CachedRunKey cached, WrappedRunKeyEnvelope envelope) {
        if (!cached.envelope.equals(envelope)) {
            throw new IllegalArgumentException("WalRun cache identity was rebound to a different KMS envelope");
        }
    }

    private void requireOperationOwner(RunKeyCacheIdentity runIdentity, RunLifecycleOwner owner) {
        requireCellBudgetOpen();
        if (owner == null) {
            requireOpen();
            requireRawRunOwner(runIdentity);
        } else if (state == State.CLOSED || runOwners.get(runIdentity) != owner) {
            throw new IllegalStateException("KMS run lease no longer owns its exact run authority");
        }
    }

    private void requireRunIdle(RunKeyCacheIdentity runIdentity) {
        if (operations.containsKey(runIdentity)) {
            throw new IllegalStateException("KMS run retains actual key operations; drain before release or transfer");
        }
    }

    private void requireCellBudgetOpen() {
        if (budget.closed) {
            throw new IllegalStateException("KMS Cell budget is closed");
        }
    }

    private void finishRetiredGeneration() {
        if (state != State.DRAINING) {
            return;
        }
        var iterator = cache.entrySet().iterator();
        while (iterator.hasNext()) {
            var entry = iterator.next();
            if (!isLiveLeaseOwner(runOwners.get(entry.getKey())) && !operations.containsKey(entry.getKey())) {
                entry.getValue().erase();
                iterator.remove();
                budget.runSlots--;
            }
        }
        if (operations.isEmpty()
                && pending.isEmpty()
                && runOwners.values().stream().noneMatch(KmsCellSession::isLiveLeaseOwner)) {
            state = State.CLOSED;
            // Every raw API and old lease is now fenced by this closed generation; no history is discarded live.
            runOwners.clear();
            budget.generations.remove(this);
        }
    }

    ResourceSnapshot resourceSnapshot() {
        synchronized (budget) {
            int history = budget.generations.stream()
                    .mapToInt(cell -> cell.runOwners.size())
                    .sum();
            int loading = budget.generations.stream()
                    .mapToInt(cell -> cell.pending.size())
                    .sum();
            return new ResourceSnapshot(
                    budget.runSlots, budget.activeOperations, loading, budget.generations.size(), history);
        }
    }

    record ResourceSnapshot(int runSlots, int activeOperations, int pendingLoads, int generations, int history) {}

    private static final class CellBudget {
        private final Set<KmsCellSession> generations = new LinkedHashSet<>();
        private KmsCellSession current;
        private int runSlots;
        private int activeOperations;
        private boolean closed;
    }

    private static final class PendingKeyLoad {
        private final WrappedRunKeyEnvelope envelope;
        private final CompletableFuture<Void> completion = new CompletableFuture<>();

        private PendingKeyLoad(WrappedRunKeyEnvelope envelope) {
            this.envelope = envelope;
        }
    }

    private final class KeyOperation implements AutoCloseable {
        private final RunKeyCacheIdentity runIdentity;
        private final RunLifecycleOwner owner;
        private byte[] key;

        private KeyOperation(RunKeyCacheIdentity runIdentity, RunLifecycleOwner owner) {
            this.runIdentity = runIdentity;
            this.owner = owner;
        }

        private void requireResultUsable() {
            synchronized (budget) {
                requireOperationOwner(runIdentity, owner);
            }
        }

        private void requireWrapResultUsable() {
            synchronized (budget) {
                if (owner == null) {
                    // A raw wrap admitted before retirement may complete, but no new raw use can start there.
                    requireCellBudgetOpen();
                    if (state == State.CLOSED) {
                        throw new IllegalStateException("KMS accepted wrap lost its admission generation");
                    }
                    requireRawRunOwner(runIdentity);
                } else {
                    requireOperationOwner(runIdentity, owner);
                }
            }
        }

        @Override
        public void close() {
            if (key != null) {
                Arrays.fill(key, (byte) 0);
            }
            synchronized (budget) {
                int left = operations.get(runIdentity) - 1;
                if (left == 0) {
                    operations.remove(runIdentity);
                } else {
                    operations.put(runIdentity, left);
                }
                budget.activeOperations--;
                finishRetiredGeneration();
            }
        }
    }

    private void requireEnvelopeIdentity(WrappedRunKeyEnvelope envelope) {
        if (!allowedKeyIdentity.equals(envelope.wrappingKeyId())) {
            throw new IllegalArgumentException("KMS envelope lies outside this Cell's allowed key identity");
        }
    }

    private void eraseAll() {
        for (CachedRunKey value : cache.values()) {
            value.erase();
        }
        cache.clear();
    }

    /** One new run's pre-Root key reservation; it never exposes a raw successor Cell or plaintext key. */
    public final class NewRunKeyCreation implements AutoCloseable {
        private final RunKeyCacheIdentity runIdentity;
        private final WrappedRunKeyEnvelope envelope;
        private boolean publicationAttempted;
        private boolean transferred;
        private boolean closed;

        private NewRunKeyCreation(RunKeyCacheIdentity runIdentity, WrappedRunKeyEnvelope envelope) {
            this.runIdentity = runIdentity;
            this.envelope = envelope;
        }

        public WrappedRunKeyEnvelope wrappedRunKey() {
            synchronized (budget) {
                requireCreationOwner();
                return envelope;
            }
        }

        public CellProviderScopeId providerScopeId() {
            return KmsCellSession.this.providerScopeId;
        }

        /** Used only by the existing exact Root-publication and Provider/KMS transfer barriers. */
        public Object lifecycleMonitor() {
            return budget;
        }

        public void requireRootCandidate(WalRunRootRecord root) {
            synchronized (budget) {
                requireCreationOwner();
                Objects.requireNonNull(root, "root");
                if (root.shardId() != runIdentity.shardId()
                        || root.shardRunEpoch() != runIdentity.shardRunEpoch()
                        || !root.providerScopeId().equals(providerScopeId)
                        || !root.wrappedRunKey().equals(envelope)) {
                    throw new IllegalArgumentException("new run key differs from the exact proposed WalRun Root");
                }
            }
        }

        /** Retains this exact candidate through uncertain Root metadata outcomes. */
        public void markRootPublicationAttempt(WalRunRootRecord root) {
            synchronized (budget) {
                requireRootCandidate(root);
                publicationAttempted = true;
            }
        }

        public void requireTransferReady(WalRunObjectSession.KmsOwnerAuthority authority) {
            synchronized (budget) {
                requireCreationOwner();
                Objects.requireNonNull(authority, "authority");
                requireRunIdle(runIdentity);
                if (!runIdentity.equals(authority.runKeyIdentity())
                        || !envelope.equals(authority.wrappedRunKey())
                        || !providerScopeId.equals(authority.providerScopeId())) {
                    throw new IllegalArgumentException("new run key differs from the exact published Root authority");
                }
                CachedRunKey existing = cache.get(runIdentity);
                if (existing == null || !existing.envelope.equals(envelope)) {
                    throw new IllegalStateException("new run key lost its exact reserved cache entry");
                }
            }
        }

        public WalRunLease transferToWalRun(WalRunObjectSession.KmsOwnerAuthority authority) {
            synchronized (budget) {
                requireTransferReady(authority);
                WalRunLease lease = new WalRunLease(authority);
                runOwners.put(runIdentity, RunLifecycleOwner.WAL_RUN);
                transferred = true;
                return lease;
            }
        }

        private void requireCreationOwner() {
            requireCellBudgetOpen();
            if (closed || transferred || runOwners.get(runIdentity) != RunLifecycleOwner.NEW_ROOT) {
                throw new IllegalStateException("new run key creation no longer owns its exact run identity");
            }
        }

        @Override
        public void close() {
            synchronized (budget) {
                if (closed || transferred) {
                    return;
                }
                requireCreationOwner();
                if (publicationAttempted) {
                    closeRunLease(runIdentity, RunLifecycleOwner.NEW_ROOT);
                } else {
                    releaseUnpublishedNewRunKey(runIdentity);
                }
                closed = true;
            }
        }
    }

    /** Recovery-only Root facade that can be irreversibly promoted to the exact final WalRun owner once. */
    public final class RecoveryLease implements AutoCloseable {
        private final RunKeyCacheIdentity runIdentity;
        private final WrappedRunKeyEnvelope wrappedRunKey;
        private final Sha256Digest rootSha256;

        private RecoveryLease(
                RunKeyCacheIdentity runIdentity, WrappedRunKeyEnvelope wrappedRunKey, Sha256Digest rootSha256) {
            this.runIdentity = runIdentity;
            this.wrappedRunKey = wrappedRunKey;
            this.rootSha256 = rootSha256;
        }

        /** Shared state-only monitor for the atomic final Provider/KMS promotion barrier. */
        public Object lifecycleMonitor() {
            return budget;
        }

        public CellProviderScopeId providerScopeId() {
            return KmsCellSession.this.providerScopeId;
        }

        public State state() {
            synchronized (budget) {
                return !budget.closed && runOwners.get(runIdentity) == RunLifecycleOwner.RECOVERY
                        ? State.OPEN
                        : State.CLOSED;
            }
        }

        public int cachedRunKeyCount() {
            synchronized (budget) {
                return cache.containsKey(runIdentity) ? 1 : 0;
            }
        }

        public long unwrapCalls() {
            synchronized (budget) {
                return KmsCellSession.this.unwrapCalls;
            }
        }

        public Nwg1ObjectReaderV1.DecodedObject verifyNwg1(
                Nwg1VerificationPathV1 path,
                Nwg1RootAuthorityV1 rootAuthority,
                Nwg1VerificationContextV1 verificationContext,
                byte[] relativeLeafUtf8,
                CanonicalBytes canonicalBody,
                long selectedFrameOrdinal) {
            synchronized (budget) {
                requireRecoveryOwner();
                requireRootAuthority(rootAuthority);
            }
            return verifyNwg1Internal(
                    runIdentity,
                    wrappedRunKey,
                    path,
                    rootAuthority,
                    verificationContext,
                    relativeLeafUtf8,
                    canonicalBody,
                    selectedFrameOrdinal,
                    RunLifecycleOwner.RECOVERY);
        }

        public Nwg1ObjectReaderV1.AuthenticatedPrefix readAuthenticatedPrefix(
                byte[] exactPrefix, long expectedBodyLength, Nwg1VerificationContextV1 verificationContext) {
            try (var operation = acquireKey(runIdentity, wrappedRunKey, RunLifecycleOwner.RECOVERY)) {
                var prefix = Nwg1ObjectReaderV1.readAuthenticatedPrefix(
                        exactPrefix, expectedBodyLength, verificationContext, operation.key);
                synchronized (budget) {
                    operation.requireResultUsable();
                    requirePrefixAuthority(prefix);
                }
                return prefix;
            }
        }

        /** Streams one append unit without exposing the lease-owned plaintext run key to source or consumer. */
        public Nwg1ObjectReaderV1.VerifiedAppendUnit readSelectedAppendUnitStreaming(
                Nwg1ObjectReaderV1.AuthenticatedPrefix prefix,
                Nwg1ObjectReaderV1.ExactFrameSource exactFrameSource,
                long selectedFrameOrdinal,
                Nwg1VerificationContextV1 verificationContext,
                Nwg1ObjectReaderV1.VerifiedFrameConsumer consumer)
                throws IOException {
            synchronized (budget) {
                requireRecoveryOwner();
                requirePrefixAuthority(prefix);
            }
            try (var operation = acquireKey(runIdentity, wrappedRunKey, RunLifecycleOwner.RECOVERY)) {
                var verified = Nwg1ObjectReaderV1.readSelectedAppendUnitStreaming(
                        prefix, exactFrameSource, selectedFrameOrdinal, verificationContext, operation.key, consumer);
                operation.requireResultUsable();
                return verified;
            }
        }

        public void requireFinalTransferReady(WalRunObjectSession.KmsOwnerAuthority authority) {
            synchronized (budget) {
                requireRecoveryOwner();
                requireRunIdle(runIdentity);
                requireCellBudgetOpen();
                Objects.requireNonNull(authority, "authority");
                if (!runIdentity.equals(authority.runKeyIdentity())
                        || !wrappedRunKey.equals(authority.wrappedRunKey())
                        || !rootSha256.equals(authority.rootSha256())
                        || !providerScopeId.equals(authority.providerScopeId())) {
                    throw new IllegalArgumentException("KMS recovery lease differs from the exact final WalRun Root");
                }
                CachedRunKey existing = cache.get(runIdentity);
                if (existing != null && !existing.envelope.equals(wrappedRunKey)) {
                    throw new IllegalStateException("KMS recovery cache slot rebound the Root envelope");
                }
                if (existing == null) {
                    requireLeaseCacheCapacity(runIdentity);
                }
            }
        }

        public WalRunLease transferToWalRun(WalRunObjectSession.KmsOwnerAuthority authority) {
            synchronized (budget) {
                requireFinalTransferReady(authority);
                WalRunLease lease = new WalRunLease(authority);
                runOwners.put(runIdentity, RunLifecycleOwner.WAL_RUN);
                return lease;
            }
        }

        private void requireRootAuthority(Nwg1RootAuthorityV1 rootAuthority) {
            Objects.requireNonNull(rootAuthority, "rootAuthority");
            if (!Arrays.equals(
                            rootAuthority.cellProviderScopeId(),
                            providerScopeId.digest().bytes().toByteArray())
                    || !Arrays.equals(
                            rootAuthority.walRunRootSha256(), rootSha256.bytes().toByteArray())
                    || !Arrays.equals(
                            rootAuthority.framedEnvelope(),
                            wrappedRunKey.framedBytes().toByteArray())) {
                throw new IllegalArgumentException("NWG1 authority differs from the Root-bound KMS recovery lease");
            }
        }

        private void requirePrefixAuthority(Nwg1ObjectReaderV1.AuthenticatedPrefix prefix) {
            Objects.requireNonNull(prefix, "prefix");
            if (prefix.header().shardId() != runIdentity.shardId()
                    || prefix.header().shardRunEpoch() != runIdentity.shardRunEpoch()
                    || !Arrays.equals(
                            prefix.header().cellProviderScopeId(),
                            providerScopeId.digest().bytes().toByteArray())
                    || !Arrays.equals(
                            prefix.header().walRunRootSha256(),
                            rootSha256.bytes().toByteArray())) {
                throw new IllegalArgumentException("NWG1 prefix differs from the Root-bound KMS recovery lease");
            }
        }

        private void requireRecoveryOwner() {
            if (runOwners.get(runIdentity) != RunLifecycleOwner.RECOVERY) {
                throw new IllegalStateException("KMS recovery lease no longer owns lifecycle authority");
            }
        }

        @Override
        public void close() {
            synchronized (budget) {
                requireRecoveryOwner();
                closeRunLease(runIdentity, RunLifecycleOwner.RECOVERY);
            }
        }
    }

    /** Root-bound facade returned only after an unforgeable owner-authority transfer. */
    public final class WalRunLease implements AutoCloseable {
        private final RunKeyCacheIdentity runIdentity;
        private final WrappedRunKeyEnvelope wrappedRunKey;
        private final Sha256Digest rootSha256;

        private WalRunLease(WalRunObjectSession.KmsOwnerAuthority authority) {
            this.runIdentity = authority.runKeyIdentity();
            this.wrappedRunKey = authority.wrappedRunKey();
            this.rootSha256 = authority.rootSha256();
        }

        public CellProviderScopeId providerScopeId() {
            return KmsCellSession.this.providerScopeId;
        }

        public State state() {
            synchronized (budget) {
                return !budget.closed && runOwners.get(runIdentity) == RunLifecycleOwner.WAL_RUN
                        ? State.OPEN
                        : State.CLOSED;
            }
        }

        public Nwg1SealedObjectV1 sealNwg1(
                GroupEncodingPlanV1 plan, long laneSequence, Nwg1VerificationContextV1 verificationContext) {
            synchronized (budget) {
                requireWalRunOwner();
                requirePlanAuthority(plan);
            }
            return sealNwg1Internal(
                    runIdentity, wrappedRunKey, plan, laneSequence, verificationContext, RunLifecycleOwner.WAL_RUN);
        }

        public Nwg1ObjectReaderV1.DecodedObject verifyNwg1(
                Nwg1VerificationPathV1 path,
                Nwg1RootAuthorityV1 rootAuthority,
                Nwg1VerificationContextV1 verificationContext,
                byte[] relativeLeafUtf8,
                CanonicalBytes canonicalBody,
                long selectedFrameOrdinal) {
            synchronized (budget) {
                requireWalRunOwner();
                requireRootAuthority(rootAuthority);
            }
            return verifyNwg1Internal(
                    runIdentity,
                    wrappedRunKey,
                    path,
                    rootAuthority,
                    verificationContext,
                    relativeLeafUtf8,
                    canonicalBody,
                    selectedFrameOrdinal,
                    RunLifecycleOwner.WAL_RUN);
        }

        public Nwg1ObjectReaderV1.AuthenticatedPrefix readAuthenticatedPrefix(
                byte[] exactPrefix, long expectedBodyLength, Nwg1VerificationContextV1 verificationContext) {
            try (var operation = acquireKey(runIdentity, wrappedRunKey, RunLifecycleOwner.WAL_RUN)) {
                var prefix = Nwg1ObjectReaderV1.readAuthenticatedPrefix(
                        exactPrefix, expectedBodyLength, verificationContext, operation.key);
                synchronized (budget) {
                    operation.requireResultUsable();
                    requirePrefixAuthority(prefix);
                }
                return prefix;
            }
        }

        /** Streams one append unit without exposing the lease-owned plaintext run key to source or consumer. */
        public Nwg1ObjectReaderV1.VerifiedAppendUnit readSelectedAppendUnitStreaming(
                Nwg1ObjectReaderV1.AuthenticatedPrefix prefix,
                Nwg1ObjectReaderV1.ExactFrameSource exactFrameSource,
                long selectedFrameOrdinal,
                Nwg1VerificationContextV1 verificationContext,
                Nwg1ObjectReaderV1.VerifiedFrameConsumer consumer)
                throws IOException {
            synchronized (budget) {
                requireWalRunOwner();
                requirePrefixAuthority(prefix);
            }
            try (var operation = acquireKey(runIdentity, wrappedRunKey, RunLifecycleOwner.WAL_RUN)) {
                var verified = Nwg1ObjectReaderV1.readSelectedAppendUnitStreaming(
                        prefix, exactFrameSource, selectedFrameOrdinal, verificationContext, operation.key, consumer);
                operation.requireResultUsable();
                return verified;
            }
        }

        private void requirePlanAuthority(GroupEncodingPlanV1 plan) {
            Objects.requireNonNull(plan, "plan");
            if (plan.shardId() != runIdentity.shardId()
                    || plan.shardRunEpoch() != runIdentity.shardRunEpoch()
                    || !Arrays.equals(
                            plan.providerScopeId(),
                            providerScopeId.digest().bytes().toByteArray())
                    || !Arrays.equals(plan.rootSha256(), rootSha256.bytes().toByteArray())) {
                throw new IllegalArgumentException("NWG1 plan differs from the Root-bound KMS lease");
            }
        }

        private void requireRootAuthority(Nwg1RootAuthorityV1 rootAuthority) {
            Objects.requireNonNull(rootAuthority, "rootAuthority");
            if (!Arrays.equals(
                            rootAuthority.cellProviderScopeId(),
                            providerScopeId.digest().bytes().toByteArray())
                    || !Arrays.equals(
                            rootAuthority.walRunRootSha256(), rootSha256.bytes().toByteArray())
                    || !Arrays.equals(
                            rootAuthority.framedEnvelope(),
                            wrappedRunKey.framedBytes().toByteArray())) {
                throw new IllegalArgumentException("NWG1 authority differs from the Root-bound KMS lease");
            }
        }

        private void requirePrefixAuthority(Nwg1ObjectReaderV1.AuthenticatedPrefix prefix) {
            Objects.requireNonNull(prefix, "prefix");
            if (prefix.header().shardId() != runIdentity.shardId()
                    || prefix.header().shardRunEpoch() != runIdentity.shardRunEpoch()
                    || !Arrays.equals(
                            prefix.header().cellProviderScopeId(),
                            providerScopeId.digest().bytes().toByteArray())
                    || !Arrays.equals(
                            prefix.header().walRunRootSha256(),
                            rootSha256.bytes().toByteArray())) {
                throw new IllegalArgumentException("NWG1 prefix differs from the Root-bound KMS lease");
            }
        }

        private void requireWalRunOwner() {
            if (runOwners.get(runIdentity) != RunLifecycleOwner.WAL_RUN) {
                throw new IllegalStateException("KMS WalRun lease no longer owns its exact run authority");
            }
        }

        @Override
        public void close() {
            synchronized (budget) {
                requireWalRunOwner();
                closeRunLease(runIdentity, RunLifecycleOwner.WAL_RUN);
            }
        }
    }

    private static final class CachedRunKey {
        private final WrappedRunKeyEnvelope envelope;
        private final byte[] plaintext;

        private CachedRunKey(WrappedRunKeyEnvelope envelope, byte[] plaintext) {
            this.envelope = envelope;
            this.plaintext = plaintext;
        }

        private void erase() {
            Arrays.fill(plaintext, (byte) 0);
        }
    }
}
