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
import java.util.Map;
import java.util.Objects;

/**
 * Cell-local bounded multi-run key cache. A lease owns and erases only its exact run; Cell drain/close erases all raw
 * entries only after every live run lease has closed. Closed run-owner tombstones are retained so a raw alias cannot
 * reacquire a transferred identity. Their total is bounded by {@code maximumCachedRunKeys}; after that many exact run
 * identities have transferred, callers must close a quiescent Cell and create a new one.
 */
public final class KmsCellSession implements AutoCloseable {
    public enum State {
        OPEN,
        DRAINING,
        CLOSED
    }

    private enum RunLifecycleOwner {
        RECOVERY,
        WAL_RUN,
        CLOSED
    }

    private final KmsTransport transport;
    private final CellProviderScopeId providerScopeId;
    private final String allowedKeyIdentity;
    private final int maximumCachedRunKeys;
    private final SecureRandom random;
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
    }

    /** Generates one random run key and wraps it once; per-Object wrapping is not exposed. */
    public synchronized WrappedRunKeyEnvelope createRunKey(RunKeyCacheIdentity runIdentity) {
        requireOpen();
        requireRawRunOwner(runIdentity);
        return createRunKeyInternal(runIdentity);
    }

    private WrappedRunKeyEnvelope createRunKeyInternal(RunKeyCacheIdentity runIdentity) {
        requireOpen();
        Objects.requireNonNull(runIdentity, "runIdentity");
        if (cache.containsKey(runIdentity)) {
            throw new IllegalStateException("WalRun key already exists in this Cell session");
        }
        requireCacheCapacity();
        byte[] plaintext = new byte[ObjectKeyDerivationV1.RUN_KEY_BYTES];
        random.nextBytes(plaintext);
        byte[] requestKey = plaintext.clone();
        try {
            WrappedRunKeyEnvelope envelope = transport.wrap(allowedKeyIdentity, requestKey);
            wrapCalls = Math.incrementExact(wrapCalls);
            requireEnvelopeIdentity(envelope);
            cache.put(runIdentity, new CachedRunKey(envelope, plaintext.clone()));
            return envelope;
        } finally {
            Arrays.fill(requestKey, (byte) 0);
            Arrays.fill(plaintext, (byte) 0);
        }
    }

    public synchronized CanonicalBytes deriveObjectKey(
            RunKeyCacheIdentity runIdentity,
            WrappedRunKeyEnvelope envelope,
            Sha256Digest rootSha256,
            WalLaneId laneId,
            long laneSequence) {
        requireRawRunOwner(runIdentity);
        return deriveObjectKeyInternal(runIdentity, envelope, rootSha256, laneId, laneSequence);
    }

    private CanonicalBytes deriveObjectKeyInternal(
            RunKeyCacheIdentity runIdentity,
            WrappedRunKeyEnvelope envelope,
            Sha256Digest rootSha256,
            WalLaneId laneId,
            long laneSequence) {
        Objects.requireNonNull(rootSha256, "rootSha256");
        Objects.requireNonNull(laneId, "laneId");
        CachedRunKey cached = requireRunKey(runIdentity, envelope);
        return ObjectKeyDerivationV1.derive(
                cached.plaintext, rootSha256, runIdentity.shardId(), runIdentity.shardRunEpoch(), laneId, laneSequence);
    }

    /** Seals and self-verifies NWG1 without exposing or cloning a plaintext run key to the caller. */
    public synchronized Nwg1SealedObjectV1 sealNwg1(
            RunKeyCacheIdentity runIdentity,
            WrappedRunKeyEnvelope envelope,
            GroupEncodingPlanV1 plan,
            long laneSequence,
            Nwg1VerificationContextV1 verificationContext) {
        requireRawRunOwner(runIdentity);
        return sealNwg1Internal(runIdentity, envelope, plan, laneSequence, verificationContext);
    }

    private Nwg1SealedObjectV1 sealNwg1Internal(
            RunKeyCacheIdentity runIdentity,
            WrappedRunKeyEnvelope envelope,
            GroupEncodingPlanV1 plan,
            long laneSequence,
            Nwg1VerificationContextV1 verificationContext) {
        Objects.requireNonNull(plan, "plan");
        Objects.requireNonNull(verificationContext, "verificationContext");
        CachedRunKey cached = requireRunKey(runIdentity, envelope);
        return com.nereusstream.storage.object.nwg1.Nwg1ObjectWriterV1.seal(
                plan, laneSequence, cached.plaintext, verificationContext);
    }

    /** Strictly verifies one Root-bound NWG1 Object; any verifier-owned key copy is erased before return. */
    public synchronized Nwg1ObjectReaderV1.DecodedObject verifyNwg1(
            RunKeyCacheIdentity runIdentity,
            WrappedRunKeyEnvelope envelope,
            Nwg1VerificationPathV1 path,
            Nwg1RootAuthorityV1 rootAuthority,
            Nwg1VerificationContextV1 verificationContext,
            byte[] relativeLeafUtf8,
            CanonicalBytes canonicalBody,
            long selectedFrameOrdinal) {
        requireRawRunOwner(runIdentity);
        return verifyNwg1Internal(
                runIdentity,
                envelope,
                path,
                rootAuthority,
                verificationContext,
                relativeLeafUtf8,
                canonicalBody,
                selectedFrameOrdinal);
    }

    private Nwg1ObjectReaderV1.DecodedObject verifyNwg1Internal(
            RunKeyCacheIdentity runIdentity,
            WrappedRunKeyEnvelope envelope,
            Nwg1VerificationPathV1 path,
            Nwg1RootAuthorityV1 rootAuthority,
            Nwg1VerificationContextV1 verificationContext,
            byte[] relativeLeafUtf8,
            CanonicalBytes canonicalBody,
            long selectedFrameOrdinal) {
        Objects.requireNonNull(path, "path");
        Objects.requireNonNull(rootAuthority, "rootAuthority");
        Objects.requireNonNull(verificationContext, "verificationContext");
        Objects.requireNonNull(relativeLeafUtf8, "relativeLeafUtf8");
        Objects.requireNonNull(canonicalBody, "canonicalBody");
        CachedRunKey cached = requireRunKey(runIdentity, envelope);
        return Nwg1ObjectVerifierV1.verify(new Nwg1ObjectVerifierV1.Request(
                path,
                rootAuthority,
                verificationContext,
                relativeLeafUtf8,
                canonicalBody.toByteArray(),
                selectedFrameOrdinal,
                ignoredEnvelope -> cached.plaintext.clone()));
    }

    public synchronized void evict(RunKeyCacheIdentity runIdentity) {
        requireRawRunOwner(runIdentity);
        CachedRunKey removed = cache.remove(runIdentity);
        if (removed != null) {
            removed.erase();
        }
    }

    public synchronized void drain() {
        closeCellInternal();
    }

    private void closeCellInternal() {
        requireNoLiveRunLeases();
        if (state == State.OPEN) {
            state = State.DRAINING;
        }
        eraseAll();
        state = State.CLOSED;
    }

    @Override
    public synchronized void close() {
        closeCellInternal();
    }

    /**
     * Irreversibly transfers one exact run's key-operation authority to a Root-bound WalRun owner facade.
     *
     * <p>The authority value has a private constructor owned by {@link WalRunObjectSession}; callers cannot mint a
     * lease or select its run identity, Root, Provider scope, or wrapped envelope themselves.
     */
    public synchronized WalRunLease transferToWalRun(WalRunObjectSession.KmsOwnerAuthority authority) {
        requireTransferReady(authority);
        WalRunLease lease = new WalRunLease(authority);
        runOwners.put(authority.runKeyIdentity(), RunLifecycleOwner.WAL_RUN);
        return lease;
    }

    /** Side-effect-free readiness half of the Provider+KMS atomic owner transfer. */
    public synchronized void requireTransferReady(WalRunObjectSession.KmsOwnerAuthority authority) {
        requireOpen();
        Objects.requireNonNull(authority, "authority");
        requireRawRunOwner(authority.runKeyIdentity());
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

    /** Side-effect-free readiness for the aggregate Provider+KMS recovery transfer barrier. */
    public synchronized void requireRecoveryTransferReady(WalRunLineageRecovery.KmsRecoveryAuthority authority) {
        requireOpen();
        Objects.requireNonNull(authority, "authority");
        RunKeyCacheIdentity runIdentity = authority.runKeyIdentity();
        requireRawRunOwner(runIdentity);
        requireTransferHistoryCapacity();
        if (cache.containsKey(runIdentity)) {
            throw new IllegalStateException("KMS recovery transfer requires a pristine target run cache slot");
        }
        requireExactKmsAuthority(
                runIdentity, authority.wrappedRunKey(), authority.rootSha256(), authority.providerScopeId());
        requireCacheCapacity();
    }

    /** Mints the recovery-only lease after the aggregate authority pair has been claimed. */
    public synchronized RecoveryLease transferToRecovery(WalRunLineageRecovery.KmsRecoveryTransfer transfer) {
        requireOpen();
        Objects.requireNonNull(transfer, "transfer");
        RunKeyCacheIdentity runIdentity = transfer.runKeyIdentity();
        requireRawRunOwner(runIdentity);
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
        runOwners.put(runIdentity, RunLifecycleOwner.RECOVERY);
        return lease;
    }

    public synchronized State state() {
        return state;
    }

    public synchronized int cachedRunKeyCount() {
        return cache.size();
    }

    public synchronized long wrapCalls() {
        return wrapCalls;
    }

    public synchronized long unwrapCalls() {
        return unwrapCalls;
    }

    public CellProviderScopeId providerScopeId() {
        return providerScopeId;
    }

    private void requireRawRunOwner(RunKeyCacheIdentity runIdentity) {
        Objects.requireNonNull(runIdentity, "runIdentity");
        RunLifecycleOwner owner = runOwners.get(runIdentity);
        if (owner != null) {
            throw new IllegalStateException("KMS run authority was transferred or retired: " + owner);
        }
    }

    private void requireTransferHistoryCapacity() {
        if (runOwners.size() >= maximumCachedRunKeys) {
            throw new IllegalStateException(
                    "KMS run-owner history capacity reached; close and recreate the quiescent Cell session");
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
        if (cache.size() + reservedEmptyLeaseSlots() >= maximumCachedRunKeys) {
            throw new IllegalStateException("KMS run-key cache capacity reached; admission must backpressure");
        }
    }

    private void requireLeaseCacheCapacity(RunKeyCacheIdentity runIdentity) {
        if (!isLiveLeaseOwner(runOwners.get(runIdentity))) {
            throw new IllegalStateException("KMS run has no live lease slot reservation");
        }
        if (cache.size() + reservedEmptyLeaseSlots() > maximumCachedRunKeys) {
            throw new IllegalStateException("KMS reserved run-key cache capacity was lost");
        }
    }

    private int reservedEmptyLeaseSlots() {
        int reserved = 0;
        for (Map.Entry<RunKeyCacheIdentity, RunLifecycleOwner> entry : runOwners.entrySet()) {
            if (isLiveLeaseOwner(entry.getValue()) && !cache.containsKey(entry.getKey())) {
                reserved = Math.incrementExact(reserved);
            }
        }
        return reserved;
    }

    private static boolean isLiveLeaseOwner(RunLifecycleOwner owner) {
        return owner == RunLifecycleOwner.RECOVERY || owner == RunLifecycleOwner.WAL_RUN;
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
        CachedRunKey removed = cache.remove(runIdentity);
        if (removed != null) {
            removed.erase();
        }
        runOwners.put(runIdentity, RunLifecycleOwner.CLOSED);
    }

    private CachedRunKey requireRunKey(RunKeyCacheIdentity runIdentity, WrappedRunKeyEnvelope envelope) {
        requireOpen();
        Objects.requireNonNull(runIdentity, "runIdentity");
        Objects.requireNonNull(envelope, "envelope");
        requireEnvelopeIdentity(envelope);
        CachedRunKey cached = cache.get(runIdentity);
        if (cached == null) {
            if (isLiveLeaseOwner(runOwners.get(runIdentity))) {
                requireLeaseCacheCapacity(runIdentity);
            } else {
                requireCacheCapacity();
            }
            byte[] plaintext = transport.unwrap(envelope);
            unwrapCalls = Math.incrementExact(unwrapCalls);
            try {
                if (plaintext == null || plaintext.length != ObjectKeyDerivationV1.RUN_KEY_BYTES) {
                    throw new IllegalStateException("KMS unwrap did not return an exact 256-bit run key");
                }
                cached = new CachedRunKey(envelope, plaintext.clone());
                cache.put(runIdentity, cached);
            } finally {
                if (plaintext != null) {
                    Arrays.fill(plaintext, (byte) 0);
                }
            }
        } else if (!cached.envelope.equals(envelope)) {
            throw new IllegalArgumentException("WalRun cache identity was rebound to a different KMS envelope");
        }
        return cached;
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

        public CellProviderScopeId providerScopeId() {
            return KmsCellSession.this.providerScopeId;
        }

        public synchronized State state() {
            synchronized (KmsCellSession.this) {
                return KmsCellSession.this.state;
            }
        }

        public synchronized int cachedRunKeyCount() {
            synchronized (KmsCellSession.this) {
                return cache.containsKey(runIdentity) ? 1 : 0;
            }
        }

        public synchronized long unwrapCalls() {
            synchronized (KmsCellSession.this) {
                return KmsCellSession.this.unwrapCalls;
            }
        }

        public synchronized Nwg1ObjectReaderV1.DecodedObject verifyNwg1(
                Nwg1VerificationPathV1 path,
                Nwg1RootAuthorityV1 rootAuthority,
                Nwg1VerificationContextV1 verificationContext,
                byte[] relativeLeafUtf8,
                CanonicalBytes canonicalBody,
                long selectedFrameOrdinal) {
            synchronized (KmsCellSession.this) {
                requireRecoveryOwner();
                requireRootAuthority(rootAuthority);
                return verifyNwg1Internal(
                        runIdentity,
                        wrappedRunKey,
                        path,
                        rootAuthority,
                        verificationContext,
                        relativeLeafUtf8,
                        canonicalBody,
                        selectedFrameOrdinal);
            }
        }

        public synchronized Nwg1ObjectReaderV1.AuthenticatedPrefix readAuthenticatedPrefix(
                byte[] exactPrefix, long expectedBodyLength, Nwg1VerificationContextV1 verificationContext) {
            synchronized (KmsCellSession.this) {
                requireRecoveryOwner();
                CachedRunKey cached = requireRunKey(runIdentity, wrappedRunKey);
                Nwg1ObjectReaderV1.AuthenticatedPrefix prefix = Nwg1ObjectReaderV1.readAuthenticatedPrefix(
                        exactPrefix, expectedBodyLength, verificationContext, cached.plaintext);
                requirePrefixAuthority(prefix);
                return prefix;
            }
        }

        /** Streams one append unit without exposing the lease-owned plaintext run key to source or consumer. */
        public synchronized Nwg1ObjectReaderV1.VerifiedAppendUnit readSelectedAppendUnitStreaming(
                Nwg1ObjectReaderV1.AuthenticatedPrefix prefix,
                Nwg1ObjectReaderV1.ExactFrameSource exactFrameSource,
                long selectedFrameOrdinal,
                Nwg1VerificationContextV1 verificationContext,
                Nwg1ObjectReaderV1.VerifiedFrameConsumer consumer)
                throws IOException {
            byte[] leasedRunKey;
            synchronized (KmsCellSession.this) {
                requireRecoveryOwner();
                requirePrefixAuthority(prefix);
                CachedRunKey cached = requireRunKey(runIdentity, wrappedRunKey);
                leasedRunKey = cached.plaintext.clone();
            }
            try {
                Nwg1ObjectReaderV1.VerifiedAppendUnit verified = Nwg1ObjectReaderV1.readSelectedAppendUnitStreaming(
                        prefix, exactFrameSource, selectedFrameOrdinal, verificationContext, leasedRunKey, consumer);
                synchronized (KmsCellSession.this) {
                    requireRecoveryOwner();
                }
                return verified;
            } finally {
                Arrays.fill(leasedRunKey, (byte) 0);
            }
        }

        public synchronized void requireFinalTransferReady(WalRunObjectSession.KmsOwnerAuthority authority) {
            synchronized (KmsCellSession.this) {
                requireRecoveryOwner();
                requireOpen();
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

        public synchronized WalRunLease transferToWalRun(WalRunObjectSession.KmsOwnerAuthority authority) {
            synchronized (KmsCellSession.this) {
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
        public synchronized void close() {
            synchronized (KmsCellSession.this) {
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
            synchronized (KmsCellSession.this) {
                return KmsCellSession.this.state;
            }
        }

        public Nwg1SealedObjectV1 sealNwg1(
                GroupEncodingPlanV1 plan, long laneSequence, Nwg1VerificationContextV1 verificationContext) {
            synchronized (KmsCellSession.this) {
                requireWalRunOwner();
                requirePlanAuthority(plan);
                return sealNwg1Internal(runIdentity, wrappedRunKey, plan, laneSequence, verificationContext);
            }
        }

        public Nwg1ObjectReaderV1.DecodedObject verifyNwg1(
                Nwg1VerificationPathV1 path,
                Nwg1RootAuthorityV1 rootAuthority,
                Nwg1VerificationContextV1 verificationContext,
                byte[] relativeLeafUtf8,
                CanonicalBytes canonicalBody,
                long selectedFrameOrdinal) {
            synchronized (KmsCellSession.this) {
                requireWalRunOwner();
                requireRootAuthority(rootAuthority);
                return verifyNwg1Internal(
                        runIdentity,
                        wrappedRunKey,
                        path,
                        rootAuthority,
                        verificationContext,
                        relativeLeafUtf8,
                        canonicalBody,
                        selectedFrameOrdinal);
            }
        }

        public Nwg1ObjectReaderV1.AuthenticatedPrefix readAuthenticatedPrefix(
                byte[] exactPrefix, long expectedBodyLength, Nwg1VerificationContextV1 verificationContext) {
            synchronized (KmsCellSession.this) {
                requireWalRunOwner();
                CachedRunKey cached = requireRunKey(runIdentity, wrappedRunKey);
                Nwg1ObjectReaderV1.AuthenticatedPrefix prefix = Nwg1ObjectReaderV1.readAuthenticatedPrefix(
                        exactPrefix, expectedBodyLength, verificationContext, cached.plaintext);
                requirePrefixAuthority(prefix);
                return prefix;
            }
        }

        /** Streams one append unit without exposing the lease-owned plaintext run key to source or consumer. */
        public synchronized Nwg1ObjectReaderV1.VerifiedAppendUnit readSelectedAppendUnitStreaming(
                Nwg1ObjectReaderV1.AuthenticatedPrefix prefix,
                Nwg1ObjectReaderV1.ExactFrameSource exactFrameSource,
                long selectedFrameOrdinal,
                Nwg1VerificationContextV1 verificationContext,
                Nwg1ObjectReaderV1.VerifiedFrameConsumer consumer)
                throws IOException {
            byte[] leasedRunKey;
            synchronized (KmsCellSession.this) {
                requireWalRunOwner();
                requirePrefixAuthority(prefix);
                CachedRunKey cached = requireRunKey(runIdentity, wrappedRunKey);
                leasedRunKey = cached.plaintext.clone();
            }
            try {
                Nwg1ObjectReaderV1.VerifiedAppendUnit verified = Nwg1ObjectReaderV1.readSelectedAppendUnitStreaming(
                        prefix, exactFrameSource, selectedFrameOrdinal, verificationContext, leasedRunKey, consumer);
                synchronized (KmsCellSession.this) {
                    requireWalRunOwner();
                }
                return verified;
            } finally {
                Arrays.fill(leasedRunKey, (byte) 0);
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
        public synchronized void close() {
            synchronized (KmsCellSession.this) {
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
