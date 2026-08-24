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

package com.nereusstream.storage.object.provider;

import com.nereusstream.domain.bytes.CanonicalBytes;
import com.nereusstream.domain.bytes.Sha256Digest;
import com.nereusstream.storage.api.bookkeeper.CellProviderScopeId;
import com.nereusstream.storage.object.control.WalRunObjectSession;
import com.nereusstream.storage.object.recovery.WalRunLineageRecovery;
import java.io.ByteArrayOutputStream;
import java.io.IOException;
import java.io.InputStream;
import java.nio.charset.StandardCharsets;
import java.security.MessageDigest;
import java.security.NoSuchAlgorithmException;
import java.util.ArrayList;
import java.util.LinkedHashMap;
import java.util.Map;
import java.util.Objects;
import java.util.Optional;
import java.util.concurrent.atomic.AtomicLong;

/**
 * Cell-isolated C1 Provider session: one conditional create, one range prefix, streaming full verification, and
 * strongly consistent paginated LIST/absence. The borrowed transport is never closed by this session.
 */
public final class C1ObjectProviderSession implements AutoCloseable {
    private static final int MAX_PROVIDER_OBJECT_KEY_BYTES = 1024;
    private static final int MAX_PROVIDER_VERSION_TOKEN_BYTES = 65_535;

    public enum State {
        OPEN,
        DRAINING,
        CLOSED
    }

    private enum LifecycleOwner {
        RAW,
        RECOVERY,
        WAL_RUN
    }

    /** Exact caller/Root-owned limits for a non-materializing strong LIST fold. */
    public record StreamingListBounds(
            int maximumPages, long maximumKeys, long maximumCanonicalKeyBytes, int maximumSingleKeyBytes) {
        public StreamingListBounds {
            if (maximumPages <= 0
                    || maximumKeys <= 0
                    || maximumCanonicalKeyBytes <= 0
                    || maximumSingleKeyBytes <= 0
                    || maximumSingleKeyBytes > MAX_PROVIDER_OBJECT_KEY_BYTES) {
                throw new IllegalArgumentException("streaming strong LIST bounds must be positive");
            }
        }
    }

    /** Complete fold counters; this value is returned only after every page and Object has passed validation. */
    public record StreamingListResult(String prefix, int pageCount, long keyCount, long canonicalKeyBytes) {}

    /** Exact verified Object bytes and the optional immutable Provider version observed by the same full GET. */
    public record VerifiedObjectRead(CanonicalBytes canonicalBody, Optional<CanonicalBytes> immutableVersionToken) {
        public VerifiedObjectRead {
            Objects.requireNonNull(canonicalBody, "canonicalBody");
            Objects.requireNonNull(immutableVersionToken, "immutableVersionToken");
            canonicalBody = CanonicalBytes.copyOf(canonicalBody.toByteArray());
            immutableVersionToken = immutableVersionToken.map(value -> {
                if (value.isEmpty() || value.length() > MAX_PROVIDER_VERSION_TOKEN_BYTES) {
                    throw new IllegalArgumentException("immutable Provider version token is empty or oversized");
                }
                return CanonicalBytes.copyOf(value.toByteArray());
            });
        }
    }

    /**
     * Receives one validated Object at a time into caller-owned staging state.
     *
     * <p>The callback must not publish external effects. Its staging state becomes eligible for publication only when
     * {@link #strongListStreaming} returns a complete result; any exception means the entire staging value is
     * discarded.
     */
    @FunctionalInterface
    public interface ListedObjectConsumer {
        void stage(ObjectProviderTransport.ListedObject object) throws IOException;
    }

    private final ObjectProviderTransport transport;
    private final CellProviderScopeId providerScopeId;
    private final String namespacePrefix;
    private final long admittedMaximumObjectBytes;
    private final int admittedMaximumPrefixBytes;
    private final int qualifiedMaximumRangeBytes;
    private final AtomicLong acceptedOperations = new AtomicLong();
    private final Map<String, ObjectIdentity> admittedKeyIdentities = new LinkedHashMap<>();
    private final Map<String, Operation> unknownObjects = new LinkedHashMap<>();
    private State state = State.OPEN;
    private LifecycleOwner lifecycleOwner = LifecycleOwner.RAW;
    private boolean externalWorkObserved;
    private int externalCalls;

    public C1ObjectProviderSession(
            ObjectProviderTransport borrowedTransport,
            CellProviderScopeId providerScopeId,
            String exclusiveNamespacePrefix,
            long admittedMaximumObjectBytes,
            int admittedMaximumPrefixBytes) {
        this.transport = Objects.requireNonNull(borrowedTransport, "borrowedTransport");
        this.providerScopeId = Objects.requireNonNull(providerScopeId, "providerScopeId");
        if (exclusiveNamespacePrefix == null
                || exclusiveNamespacePrefix.isEmpty()
                || exclusiveNamespacePrefix.endsWith("/")
                || exclusiveNamespacePrefix.indexOf('\0') >= 0) {
            throw new IllegalArgumentException("exclusive namespace prefix is invalid");
        }
        requireCanonicalPath(exclusiveNamespacePrefix, MAX_PROVIDER_OBJECT_KEY_BYTES, "namespace prefix");
        this.namespacePrefix = exclusiveNamespacePrefix + "/";
        transport.capabilities().requireC1();
        if (admittedMaximumObjectBytes <= 0
                || admittedMaximumObjectBytes > transport.capabilities().maximumObjectBytes()) {
            throw new IllegalArgumentException("admitted Object cap exceeds qualified Provider capability");
        }
        if (admittedMaximumPrefixBytes <= 0
                || admittedMaximumPrefixBytes > transport.capabilities().maximumRangeBytes()) {
            throw new IllegalArgumentException("admitted prefix cap exceeds qualified Provider capability");
        }
        this.admittedMaximumObjectBytes = admittedMaximumObjectBytes;
        this.admittedMaximumPrefixBytes = admittedMaximumPrefixBytes;
        this.qualifiedMaximumRangeBytes = transport.capabilities().maximumRangeBytes();
    }

    public CellProviderScopeId providerScopeId() {
        return providerScopeId;
    }

    public String exclusiveNamespacePrefix() {
        return namespacePrefix.substring(0, namespacePrefix.length() - 1);
    }

    public long admittedMaximumObjectBytes() {
        return admittedMaximumObjectBytes;
    }

    public int admittedMaximumPrefixBytes() {
        return admittedMaximumPrefixBytes;
    }

    /** The candidate body is proven first; accepted dispatch is UNKNOWN before opening the upload stream. */
    public ProviderObjectResult conditionalCreate(RepeatableObjectBody body) throws IOException {
        beginExternalCall();
        try {
            return conditionalCreateInternal(body);
        } finally {
            endExternalCall();
        }
    }

    private ProviderObjectResult conditionalCreateInternal(RepeatableObjectBody body) throws IOException {
        Objects.requireNonNull(body, "body");
        ObjectIdentity identity = body.identity();
        requireOwned(identity.key());
        requireBodyWithinCap(identity);
        verifyRepeatableBody(body, identity);
        ConditionalCreateClaim claim = claimConditionalCreate(identity);
        if (claim.immediateOutcome() != null) {
            return ProviderObjectResult.outcome(claim.immediateOutcome());
        }
        Operation operation = claim.operation();
        try (IdentityVerifyingInputStream input = new IdentityVerifyingInputStream(body.openStream(), identity)) {
            operation.outcome = ProviderObjectOutcome.OUTCOME_UNKNOWN;
            ObjectProviderTransport.ConditionalCreateResult result = transport.putIfAbsent(identity, input);
            if (result == ObjectProviderTransport.ConditionalCreateResult.CREATED) {
                input.requireVerifiedAtEof();
            }
            ProviderObjectResult resolved =
                    switch (result) {
                        case CREATED -> ProviderObjectResult.outcome(ProviderObjectOutcome.APPLIED_EXACT);
                        case ALREADY_EXISTS -> validateExisting(identity);
                        case DEFINITIVE_CONFLICT ->
                            ProviderObjectResult.outcome(ProviderObjectOutcome.DEFINITIVE_CONFLICT);
                        case RESPONSE_UNKNOWN -> ProviderObjectResult.outcome(ProviderObjectOutcome.OUTCOME_UNKNOWN);
                    };
            operation.outcome = resolved.outcome();
            return resolved;
        } finally {
            finishObjectClaim(identity, operation);
        }
    }

    /** Reconciles response loss through complete strong LIST and an exact streaming full GET; C1 performs no HEAD. */
    public ProviderReconciliationResult reconcileUnknown(
            ObjectIdentity identity,
            String leafPrefix,
            int maximumListPages,
            long maximumListKeys,
            long maximumListKeyBytes,
            int maximumSingleKeyBytes)
            throws IOException {
        beginExternalCall();
        try {
            return reconcileUnknownInternal(
                    identity,
                    leafPrefix,
                    maximumListPages,
                    maximumListKeys,
                    maximumListKeyBytes,
                    maximumSingleKeyBytes);
        } finally {
            endExternalCall();
        }
    }

    private ProviderReconciliationResult reconcileUnknownInternal(
            ObjectIdentity identity,
            String leafPrefix,
            int maximumListPages,
            long maximumListKeys,
            long maximumListKeyBytes,
            int maximumSingleKeyBytes)
            throws IOException {
        Objects.requireNonNull(identity, "identity");
        requireOwned(identity.key());
        requireOwnedPrefix(leafPrefix);
        requireBodyWithinCap(identity);
        if (!requireKeyIdentity(identity)) {
            return ProviderReconciliationResult.localConflict();
        }
        Operation operation = claimUnknownOrAccept(identity);
        if (operation == null) {
            return ProviderReconciliationResult.localConflict();
        }
        try {
            StrongListResult inventory = strongListInternal(
                    leafPrefix, maximumListPages, maximumListKeys, maximumListKeyBytes, maximumSingleKeyBytes);
            boolean exactKeyListed =
                    inventory.objects().stream().anyMatch(value -> value.key().equals(identity.key()));
            Verification verification = verifyFullObject(identity);
            ProviderObjectResult resolved =
                    switch (verification.kind()) {
                        case EXACT ->
                            new ProviderObjectResult(ProviderObjectOutcome.EXISTING_EXACT, verification.versionToken());
                        case MISMATCH -> ProviderObjectResult.outcome(ProviderObjectOutcome.DEFINITIVE_CONFLICT);
                        case NOT_FOUND ->
                            exactKeyListed
                                    ? ProviderObjectResult.outcome(ProviderObjectOutcome.OUTCOME_UNKNOWN)
                                    : ProviderObjectResult.outcome(ProviderObjectOutcome.DEFINITIVELY_NOT_APPLIED);
                        case UNKNOWN -> ProviderObjectResult.outcome(ProviderObjectOutcome.OUTCOME_UNKNOWN);
                    };
            operation.outcome = resolved.outcome();
            return ProviderReconciliationResult.providerWork(resolved, inventory);
        } finally {
            finishObjectClaim(identity, operation);
        }
    }

    /** Returns only the exact inclusive-zero/exclusive-end prefix requested by NWG1 recovery. */
    public CanonicalBytes readDirectoryPrefix(
            ObjectIdentity identity, int directoryPrefixEnd, Optional<CanonicalBytes> versionToken) throws IOException {
        beginExternalCall();
        try {
            return readDirectoryPrefixInternal(identity, directoryPrefixEnd, versionToken);
        } finally {
            endExternalCall();
        }
    }

    private CanonicalBytes readDirectoryPrefixInternal(
            ObjectIdentity identity, int directoryPrefixEnd, Optional<CanonicalBytes> versionToken) throws IOException {
        Objects.requireNonNull(identity, "identity");
        requireOwned(identity.key());
        if (directoryPrefixEnd <= 0
                || directoryPrefixEnd > admittedMaximumPrefixBytes
                || directoryPrefixEnd > identity.bodyLength()) {
            throw new IllegalArgumentException("directory prefix lies outside the admitted bounds");
        }
        Operation operation = acceptReadOperation(identity);
        try (ObjectProviderTransport.StreamingObject response =
                transport.getRange(identity.key(), 0, directoryPrefixEnd, versionToken)) {
            if (response.bodyLength() != identity.bodyLength()
                    || response.inclusiveStart() != 0
                    || response.exclusiveEnd() != directoryPrefixEnd) {
                throw new IOException("Provider returned a different Object/range than requested");
            }
            return readExactBounded(response.body(), directoryPrefixEnd);
        } finally {
            terminal(operation);
        }
    }

    /** Reads one exact immutable Object byte range for a Root-authorized NWG1 frame. */
    public CanonicalBytes readExactRange(
            ObjectIdentity identity, long inclusiveStart, long exclusiveEnd, Optional<CanonicalBytes> versionToken)
            throws IOException {
        beginExternalCall();
        try {
            return readExactRangeInternal(identity, inclusiveStart, exclusiveEnd, versionToken);
        } finally {
            endExternalCall();
        }
    }

    private CanonicalBytes readExactRangeInternal(
            ObjectIdentity identity, long inclusiveStart, long exclusiveEnd, Optional<CanonicalBytes> versionToken)
            throws IOException {
        Objects.requireNonNull(identity, "identity");
        requireOwned(identity.key());
        long rangeBytes;
        try {
            rangeBytes = Math.subtractExact(exclusiveEnd, inclusiveStart);
        } catch (ArithmeticException failure) {
            throw new IllegalArgumentException("range endpoints overflow", failure);
        }
        if (inclusiveStart < 0
                || exclusiveEnd > identity.bodyLength()
                || rangeBytes <= 0
                || rangeBytes > qualifiedMaximumRangeBytes
                || rangeBytes > Integer.MAX_VALUE) {
            throw new IllegalArgumentException("exact range lies outside the qualified Provider/Object bounds");
        }
        Operation operation = acceptReadOperation(identity);
        try (ObjectProviderTransport.StreamingObject response =
                transport.getRange(identity.key(), inclusiveStart, exclusiveEnd, versionToken)) {
            if (response.bodyLength() != identity.bodyLength()
                    || response.inclusiveStart() != inclusiveStart
                    || response.exclusiveEnd() != exclusiveEnd) {
                throw new IOException("Provider returned a different Object/range than requested");
            }
            return readExactBounded(response.body(), Math.toIntExact(rangeBytes));
        } finally {
            terminal(operation);
        }
    }

    /**
     * Reads one bounded control Object (for example NWKCP1), verifying exact length and full-body SHA-256. This API is
     * intentionally not the active-tail prefix path and does not accept ETag as integrity evidence.
     */
    public CanonicalBytes readVerifiedObject(ObjectIdentity identity) throws IOException {
        return readVerifiedObjectWithVersion(identity).canonicalBody();
    }

    public VerifiedObjectRead readVerifiedObjectWithVersion(ObjectIdentity identity) throws IOException {
        beginExternalCall();
        try {
            return readVerifiedObjectWithVersionInternal(identity);
        } finally {
            endExternalCall();
        }
    }

    private VerifiedObjectRead readVerifiedObjectWithVersionInternal(ObjectIdentity identity) throws IOException {
        Objects.requireNonNull(identity, "identity");
        requireOwned(identity.key());
        requireBodyWithinCap(identity);
        if (identity.bodyLength() > Integer.MAX_VALUE) {
            throw new IllegalArgumentException("verified control Object exceeds the in-memory canonical byte cap");
        }
        Operation operation = acceptReadOperation(identity);
        try (ObjectProviderTransport.StreamingObject response = transport.get(identity.key(), Optional.empty())) {
            if (response.bodyLength() != identity.bodyLength()
                    || response.inclusiveStart() != 0
                    || response.exclusiveEnd() != identity.bodyLength()) {
                throw new IOException("Provider returned a different Object than requested");
            }
            CanonicalBytes bytes = readExactBounded(response.body(), Math.toIntExact(identity.bodyLength()));
            if (!Sha256Digest.hash(bytes).equals(identity.bodySha256())) {
                throw new IOException("Provider Object SHA-256 differs from the exact identity");
            }
            return new VerifiedObjectRead(bytes, copyVersionToken(response.immutableVersionToken()));
        } catch (IllegalArgumentException failure) {
            throw new IOException("Provider returned an invalid immutable version token", failure);
        } finally {
            terminal(operation);
        }
    }

    public ProviderAbsenceProof proveAbsent(
            ObjectIdentity identity,
            String leafPrefix,
            int maximumListPages,
            long maximumListKeys,
            long maximumListKeyBytes,
            int maximumSingleKeyBytes)
            throws IOException {
        beginExternalCall();
        try {
            return proveAbsentOwned(
                    identity,
                    leafPrefix,
                    maximumListPages,
                    maximumListKeys,
                    maximumListKeyBytes,
                    maximumSingleKeyBytes);
        } finally {
            endExternalCall();
        }
    }

    private ProviderAbsenceProof proveAbsentOwned(
            ObjectIdentity identity,
            String leafPrefix,
            int maximumListPages,
            long maximumListKeys,
            long maximumListKeyBytes,
            int maximumSingleKeyBytes)
            throws IOException {
        Objects.requireNonNull(identity, "identity");
        requireOwned(identity.key());
        requireOwnedPrefix(leafPrefix);
        requireBodyWithinCap(identity);
        Operation operation = acceptAbsenceOperation(identity);
        try {
            return proveAbsentInternal(
                    identity,
                    leafPrefix,
                    maximumListPages,
                    maximumListKeys,
                    maximumListKeyBytes,
                    maximumSingleKeyBytes);
        } finally {
            terminal(operation);
        }
    }

    /** Complete bounded strong prefix inventory used for uncovered open-lane recovery. */
    public StrongListResult strongList(
            String prefix, int maximumPages, long maximumKeys, long maximumCanonicalKeyBytes, int maximumSingleKeyBytes)
            throws IOException {
        beginExternalCall();
        try {
            return strongListOwned(prefix, maximumPages, maximumKeys, maximumCanonicalKeyBytes, maximumSingleKeyBytes);
        } finally {
            endExternalCall();
        }
    }

    /**
     * Complete bounded strong prefix inventory without retaining the inventory or an unbounded token/key set.
     * Globally strict ASCII key order detects duplicate/replayed pages while retaining only the previous key and the
     * current continuation token; {@code maximumPages} closes empty-page or alternating-token cycles.
     */
    public StreamingListResult strongListStreaming(
            String prefix, StreamingListBounds bounds, ListedObjectConsumer consumer) throws IOException {
        beginExternalCall();
        try {
            return strongListStreamingOwned(prefix, bounds, consumer);
        } finally {
            endExternalCall();
        }
    }

    private StreamingListResult strongListStreamingOwned(
            String prefix, StreamingListBounds bounds, ListedObjectConsumer consumer) throws IOException {
        Operation operation = acceptRecoveryOperation();
        try {
            return strongListStreamingInternal(prefix, bounds, consumer);
        } finally {
            terminal(operation);
        }
    }

    private StrongListResult strongListOwned(
            String prefix, int maximumPages, long maximumKeys, long maximumCanonicalKeyBytes, int maximumSingleKeyBytes)
            throws IOException {
        Operation operation = acceptRecoveryOperation();
        try {
            return strongListInternal(
                    prefix, maximumPages, maximumKeys, maximumCanonicalKeyBytes, maximumSingleKeyBytes);
        } finally {
            terminal(operation);
        }
    }

    private ProviderAbsenceProof proveAbsentInternal(
            ObjectIdentity identity,
            String leafPrefix,
            int maximumListPages,
            long maximumListKeys,
            long maximumListKeyBytes,
            int maximumSingleKeyBytes)
            throws IOException {
        String key = identity.key();
        if (!key.startsWith(leafPrefix)) {
            throw new IllegalArgumentException("absence prefix does not contain the exact key");
        }
        if (maximumListPages <= 0 || maximumListKeys <= 0 || maximumListKeyBytes <= 0) {
            throw new IllegalArgumentException("absence-proof LIST bounds must be positive");
        }
        StrongListResult inventory = strongListInternal(
                leafPrefix, maximumListPages, maximumListKeys, maximumListKeyBytes, maximumSingleKeyBytes);
        if (inventory.objects().stream().anyMatch(value -> value.key().equals(key))) {
            throw new IllegalStateException("Object is present; absence cannot be proven");
        }
        Verification verification = verifyFullObject(identity);
        if (verification.kind() != VerificationKind.NOT_FOUND) {
            throw new IllegalStateException("exact full GET did not prove Object absence: " + verification.kind());
        }
        return new ProviderAbsenceProof(
                key, leafPrefix, inventory.pageCount(), inventory.objects().size(), true, 0);
    }

    public synchronized void drain() {
        requireExternalLifecycleOwner();
        drainInternal();
    }

    private void drainInternal() {
        if (state == State.OPEN) {
            state = State.DRAINING;
        }
    }

    @Override
    public synchronized void close() {
        requireExternalLifecycleOwner();
        closeInternal();
    }

    private void closeInternal() {
        drainInternal();
        if (acceptedOperations.get() != 0 || !unknownObjects.isEmpty()) {
            throw new IllegalStateException("Cell Provider session still owns accepted operations or unknown outcomes");
        }
        state = State.CLOSED;
    }

    /**
     * Performs every fallible transfer validation without changing lifecycle ownership or any Provider state.
     *
     * <p>The authority value has a private constructor owned by {@link WalRunObjectSession}. There is deliberately no
     * package-visible or test-only way to mint one.
     */
    public synchronized void requireTransferReady(WalRunObjectSession.ProviderOwnerAuthority authority) {
        requireRawTransferReady();
        Objects.requireNonNull(authority, "authority");
        requireExactProviderAuthority(
                authority.providerScopeId(),
                authority.exclusiveNamespacePrefix(),
                authority.admittedMaximumObjectBytes(),
                authority.admittedMaximumPrefixBytes());
    }

    private void requireRawTransferReady() {
        requireExternalLifecycleOwner();
        if (state != State.OPEN) {
            throw new IllegalStateException("Cell Provider session no longer accepts ownership transfer: " + state);
        }
        if (externalCalls != 0) {
            throw new IllegalStateException("Cell Provider session has an active external call");
        }
        if (externalWorkObserved
                || acceptedOperations.get() != 0
                || !unknownObjects.isEmpty()
                || !admittedKeyIdentities.isEmpty()) {
            throw new IllegalStateException(
                    "Cell Provider ownership transfer requires a pristine session with no admitted identity or work");
        }
    }

    private void requireExactProviderAuthority(
            CellProviderScopeId exactScope,
            String exactNamespace,
            long exactMaximumObjectBytes,
            int exactMaximumPrefixBytes) {
        if (!providerScopeId.equals(exactScope)
                || !exclusiveNamespacePrefix().equals(exactNamespace)
                || admittedMaximumObjectBytes != exactMaximumObjectBytes
                || admittedMaximumPrefixBytes != exactMaximumPrefixBytes) {
            throw new IllegalArgumentException("Provider WalRun authority differs from the exact Cell scope or caps");
        }
    }

    /**
     * Irreversibly transfers Provider I/O and lifecycle authority after rechecking the complete readiness predicate.
     */
    public synchronized WalRunLease transferToWalRun(WalRunObjectSession.ProviderOwnerAuthority authority) {
        requireTransferReady(authority);
        WalRunLease lease = new WalRunLease(authority);
        lifecycleOwner = LifecycleOwner.WAL_RUN;
        return lease;
    }

    /** Side-effect-free readiness for the aggregate Provider+KMS recovery transfer barrier. */
    public synchronized void requireRecoveryTransferReady(WalRunLineageRecovery.ProviderRecoveryAuthority authority) {
        requireRawTransferReady();
        Objects.requireNonNull(authority, "authority");
        requireExactProviderAuthority(
                authority.providerScopeId(),
                authority.exclusiveNamespacePrefix(),
                authority.admittedMaximumObjectBytes(),
                authority.admittedMaximumPrefixBytes());
        Objects.requireNonNull(authority.rootSha256(), "rootSha256");
    }

    /** Mints the recovery-only lease after the aggregate authority pair has been claimed. */
    public synchronized RecoveryLease transferToRecovery(WalRunLineageRecovery.ProviderRecoveryTransfer transfer) {
        requireRawTransferReady();
        Objects.requireNonNull(transfer, "transfer");
        requireExactProviderAuthority(
                transfer.providerScopeId(),
                transfer.exclusiveNamespacePrefix(),
                transfer.admittedMaximumObjectBytes(),
                transfer.admittedMaximumPrefixBytes());
        Sha256Digest rootSha256 = Objects.requireNonNull(transfer.rootSha256(), "rootSha256");
        RecoveryLease lease = new RecoveryLease(rootSha256);
        transfer.consumeForLease();
        lifecycleOwner = LifecycleOwner.RECOVERY;
        return lease;
    }

    public synchronized State state() {
        return state;
    }

    public long acceptedOperations() {
        return acceptedOperations.get();
    }

    public synchronized int unknownObjectCount() {
        return unknownObjects.size();
    }

    private synchronized void beginExternalCall() {
        requireExternalLifecycleOwner();
        externalWorkObserved = true;
        externalCalls = Math.incrementExact(externalCalls);
    }

    private synchronized void endExternalCall() {
        if (externalCalls <= 0) {
            throw new IllegalStateException("Cell Provider external-call ownership underflow");
        }
        externalCalls--;
    }

    private synchronized void beginRecoveryCall() {
        if (lifecycleOwner != LifecycleOwner.RECOVERY) {
            throw new IllegalStateException("Cell Provider recovery lease no longer owns Provider operations");
        }
        externalCalls = Math.incrementExact(externalCalls);
    }

    private synchronized void endRecoveryCall() {
        if (externalCalls <= 0) {
            throw new IllegalStateException("Cell Provider recovery-call ownership underflow");
        }
        externalCalls--;
    }

    private void requireExternalLifecycleOwner() {
        if (lifecycleOwner != LifecycleOwner.RAW) {
            throw new IllegalStateException("Cell Provider lifecycle authority was transferred from the raw session");
        }
    }

    private ProviderObjectResult validateExisting(ObjectIdentity identity) throws IOException {
        Verification verification = verifyFullObject(identity);
        return switch (verification.kind()) {
            case EXACT -> new ProviderObjectResult(ProviderObjectOutcome.EXISTING_EXACT, verification.versionToken());
            case MISMATCH -> ProviderObjectResult.outcome(ProviderObjectOutcome.DEFINITIVE_CONFLICT);
            case NOT_FOUND, UNKNOWN -> ProviderObjectResult.outcome(ProviderObjectOutcome.OUTCOME_UNKNOWN);
        };
    }

    private Verification verifyFullObject(ObjectIdentity identity) throws IOException {
        try (ObjectProviderTransport.StreamingObject response = transport.get(identity.key(), Optional.empty())) {
            if (response.bodyLength() != identity.bodyLength()
                    || response.inclusiveStart() != 0
                    || response.exclusiveEnd() != identity.bodyLength()) {
                return Verification.mismatch();
            }
            Sha256Digest observed = streamSha256(response.body(), identity.bodyLength());
            return observed.equals(identity.bodySha256())
                    ? Verification.exact(copyVersionToken(response.immutableVersionToken()))
                    : Verification.mismatch();
        } catch (IOException failure) {
            return switch (transport.classifyFailure(failure)) {
                case NOT_FOUND -> Verification.notFound();
                case RETRYABLE, OUTCOME_UNKNOWN -> Verification.unknown();
                case FATAL -> throw failure;
            };
        }
    }

    private StrongListResult strongListInternal(
            String prefix, int maximumPages, long maximumKeys, long maximumCanonicalKeyBytes, int maximumSingleKeyBytes)
            throws IOException {
        requireOwnedPrefix(prefix);
        if (maximumPages <= 0
                || maximumKeys <= 0
                || maximumCanonicalKeyBytes <= 0
                || maximumSingleKeyBytes <= 0
                || maximumSingleKeyBytes > MAX_PROVIDER_OBJECT_KEY_BYTES) {
            throw new IllegalArgumentException("strong LIST call bounds must be positive");
        }
        int pageLimit = transport.capabilities().maximumListPageKeys();
        Optional<CanonicalBytes> continuation = Optional.empty();
        String lastKey = null;
        ArrayList<ObjectProviderTransport.ListedObject> objects = new ArrayList<>();
        int pages = 0;
        long keyBytes = 0;
        do {
            if (pages == maximumPages) {
                throw new IllegalStateException("strong LIST page bound exhausted");
            }
            long remainingKeys = Math.subtractExact(maximumKeys, objects.size());
            if (remainingKeys == 0) {
                throw new IllegalStateException("strong LIST key bound exhausted");
            }
            long remainingKeyBytes = Math.subtractExact(maximumCanonicalKeyBytes, keyBytes);
            long keysAdmissibleByBytes = remainingKeyBytes / maximumSingleKeyBytes;
            if (keysAdmissibleByBytes == 0) {
                throw new IllegalStateException("strong LIST key-byte bound exhausted before the next page");
            }
            int requestedKeys = (int) Math.min(pageLimit, Math.min(remainingKeys, keysAdmissibleByBytes));
            ObjectProviderTransport.ListPage page = transport.list(prefix, continuation, requestedKeys);
            pages = Math.incrementExact(pages);
            if (page.objects().size() > requestedKeys) {
                throw new IOException("Provider LIST page exceeds its qualified key cap");
            }
            for (ObjectProviderTransport.ListedObject object : page.objects()) {
                try {
                    requireCanonicalPath(object.key(), maximumSingleKeyBytes, "listed Object key");
                } catch (IllegalArgumentException failure) {
                    throw new IOException("Provider LIST returned a non-canonical Object key", failure);
                }
                if (!object.key().startsWith(prefix) || (lastKey != null && lastKey.compareTo(object.key()) >= 0)) {
                    throw new IOException("Provider LIST escaped its prefix or violated strict global key order");
                }
                if (object.bodyLength() > admittedMaximumObjectBytes) {
                    throw new IOException("Provider LIST returned an Object above the admitted body cap");
                }
                requireNoListedVersionToken(object);
                if (objects.size() >= maximumKeys) {
                    throw new IllegalStateException("strong LIST key bound exhausted");
                }
                long nextKeyBytes = Math.addExact(keyBytes, object.key().getBytes(StandardCharsets.US_ASCII).length);
                if (nextKeyBytes > maximumCanonicalKeyBytes) {
                    throw new IllegalStateException("strong LIST key-byte bound exhausted");
                }
                keyBytes = nextKeyBytes;
                objects.add(object);
                lastKey = object.key();
            }
            Optional<CanonicalBytes> next = page.nextContinuationToken();
            if (next.isPresent()
                    && (next.orElseThrow().length() > maximumSingleKeyBytes || next.equals(continuation))) {
                throw new IOException("Provider LIST continuation is oversized or did not advance");
            }
            continuation = next;
        } while (continuation.isPresent());
        return new StrongListResult(prefix, objects, pages, keyBytes);
    }

    private StreamingListResult strongListStreamingInternal(
            String prefix, StreamingListBounds bounds, ListedObjectConsumer consumer) throws IOException {
        requireOwnedPrefix(prefix);
        Objects.requireNonNull(bounds, "bounds");
        Objects.requireNonNull(consumer, "consumer");
        int pageLimit = transport.capabilities().maximumListPageKeys();
        Optional<CanonicalBytes> continuation = Optional.empty();
        String lastKey = null;
        int pages = 0;
        long keys = 0;
        long keyBytes = 0;
        do {
            if (pages == bounds.maximumPages()) {
                throw new IllegalStateException("streaming strong LIST page bound exhausted");
            }
            long remainingKeys = Math.subtractExact(bounds.maximumKeys(), keys);
            if (remainingKeys == 0) {
                throw new IllegalStateException("streaming strong LIST key bound exhausted");
            }
            long remainingKeyBytes = Math.subtractExact(bounds.maximumCanonicalKeyBytes(), keyBytes);
            long keysAdmissibleByBytes = remainingKeyBytes / bounds.maximumSingleKeyBytes();
            if (keysAdmissibleByBytes == 0) {
                throw new IllegalStateException("streaming strong LIST key-byte bound exhausted before the next page");
            }
            int requestedKeys = (int) Math.min(pageLimit, Math.min(remainingKeys, keysAdmissibleByBytes));
            ObjectProviderTransport.ListPage page = transport.list(prefix, continuation, requestedKeys);
            pages = Math.incrementExact(pages);
            if (page.objects().size() > requestedKeys) {
                throw new IOException("Provider LIST page exceeds its qualified key cap");
            }
            for (ObjectProviderTransport.ListedObject object : page.objects()) {
                try {
                    requireCanonicalPath(object.key(), bounds.maximumSingleKeyBytes(), "listed Object key");
                } catch (IllegalArgumentException failure) {
                    throw new IOException("Provider LIST returned a non-canonical Object key", failure);
                }
                if (!object.key().startsWith(prefix) || (lastKey != null && lastKey.compareTo(object.key()) >= 0)) {
                    throw new IOException("Provider LIST escaped its prefix or violated strict global key order");
                }
                if (object.bodyLength() > admittedMaximumObjectBytes) {
                    throw new IOException("Provider LIST returned an Object above the admitted body cap");
                }
                requireNoListedVersionToken(object);
                if (keys == bounds.maximumKeys()) {
                    throw new IllegalStateException("streaming strong LIST key bound exhausted");
                }
                long nextKeyBytes = Math.addExact(keyBytes, object.key().getBytes(StandardCharsets.US_ASCII).length);
                if (nextKeyBytes > bounds.maximumCanonicalKeyBytes()) {
                    throw new IllegalStateException("streaming strong LIST key-byte bound exhausted");
                }
                consumer.stage(
                        new ObjectProviderTransport.ListedObject(object.key(), object.bodyLength(), Optional.empty()));
                lastKey = object.key();
                keys = Math.incrementExact(keys);
                keyBytes = nextKeyBytes;
            }
            Optional<CanonicalBytes> next = page.nextContinuationToken();
            if (next.isPresent()
                    && (next.orElseThrow().length() > bounds.maximumSingleKeyBytes() || next.equals(continuation))) {
                throw new IOException("Provider LIST continuation is oversized or did not advance");
            }
            continuation = next;
        } while (continuation.isPresent());
        return new StreamingListResult(prefix, pages, keys, keyBytes);
    }

    private static void requireNoListedVersionToken(ObjectProviderTransport.ListedObject object) throws IOException {
        if (object.immutableVersionToken().isPresent()) {
            throw new IOException("Provider LIST returned a version token while the M3 proof mode is NONE");
        }
    }

    private static Optional<CanonicalBytes> copyVersionToken(Optional<CanonicalBytes> versionToken) throws IOException {
        Objects.requireNonNull(versionToken, "versionToken");
        if (versionToken.isEmpty()) {
            return Optional.empty();
        }
        CanonicalBytes value = versionToken.orElseThrow();
        if (value.isEmpty() || value.length() > MAX_PROVIDER_VERSION_TOKEN_BYTES) {
            throw new IOException("Provider immutable version token is empty or exceeds the canonical cap");
        }
        return Optional.of(CanonicalBytes.copyOf(value.toByteArray()));
    }

    private synchronized Operation acceptOperation() {
        if (state != State.OPEN) {
            throw new IllegalStateException("Cell Provider session no longer accepts operations: " + state);
        }
        acceptedOperations.incrementAndGet();
        return new Operation();
    }

    private synchronized Operation acceptReadOperation(ObjectIdentity identity) {
        if (!requireKeyIdentity(identity)) {
            throw new IllegalStateException("Cell Provider session cannot rebind an admitted Object key");
        }
        return acceptRecoveryOperation();
    }

    private synchronized Operation acceptAbsenceOperation(ObjectIdentity identity) {
        if (state == State.CLOSED) {
            throw new IllegalStateException("Cell Provider session is closed");
        }
        if (!requireKeyIdentity(identity)) {
            throw new IllegalStateException("Cell Provider session cannot rebind an admitted Object key");
        }
        acceptedOperations.incrementAndGet();
        return new Operation();
    }

    private synchronized Operation acceptRecoveryOperation() {
        if (state == State.CLOSED) {
            throw new IllegalStateException("Cell Provider session is closed");
        }
        acceptedOperations.incrementAndGet();
        return new Operation();
    }

    private synchronized ConditionalCreateClaim claimConditionalCreate(ObjectIdentity identity) {
        if (!requireKeyIdentity(identity)) {
            return ConditionalCreateClaim.immediate(ProviderObjectOutcome.DEFINITIVE_CONFLICT);
        }
        if (state != State.OPEN) {
            throw new IllegalStateException(
                    "Cell Provider session no longer accepts conditional PUT and does not dispatch while draining");
        }
        Operation operation = unknownObjects.get(identity.key());
        if (operation != null) {
            if (operation.conditionalCreateDispatches >= 2) {
                throw new IllegalStateException(
                        "C1 frozen failure model permits at most the exact same candidate PUT2 retry");
            }
            operation.claim("same-candidate conditional-create retry");
            operation.conditionalCreateDispatches = Math.incrementExact(operation.conditionalCreateDispatches);
            return ConditionalCreateClaim.dispatch(operation);
        }
        operation = acceptOperation();
        operation.claim("conditional-create");
        operation.conditionalCreateDispatches = 1;
        unknownObjects.put(identity.key(), operation);
        return ConditionalCreateClaim.dispatch(operation);
    }

    private synchronized Operation claimUnknownOrAccept(ObjectIdentity identity) {
        if (!requireKeyIdentity(identity)) {
            return null;
        }
        Operation operation = unknownObjects.get(identity.key());
        if (operation != null) {
            operation.claim("unknown reconciliation");
            return operation;
        }
        operation = acceptRecoveryOperation();
        operation.outcome = ProviderObjectOutcome.OUTCOME_UNKNOWN;
        operation.claim("recovery reconciliation");
        unknownObjects.put(identity.key(), operation);
        return operation;
    }

    private synchronized void finishObjectClaim(ObjectIdentity identity, Operation operation) {
        if (unknownObjects.get(identity.key()) != operation) {
            throw new IllegalStateException("Object operation ownership changed while claimed");
        }
        operation.releaseClaim();
        if (operation.outcome != ProviderObjectOutcome.OUTCOME_UNKNOWN) {
            unknownObjects.remove(identity.key());
            terminal(operation);
        }
    }

    private synchronized boolean requireKeyIdentity(ObjectIdentity identity) {
        ObjectIdentity admitted = admittedKeyIdentities.get(identity.key());
        if (admitted != null) {
            return admitted.equals(identity);
        }
        admittedKeyIdentities.put(identity.key(), identity);
        return true;
    }

    private synchronized void terminal(Operation operation) {
        if (operation.terminal) {
            throw new IllegalStateException("Provider operation resolved twice");
        }
        operation.terminal = true;
        acceptedOperations.decrementAndGet();
    }

    private void requireOwned(String key) {
        requireCanonicalPath(key, MAX_PROVIDER_OBJECT_KEY_BYTES, "Object key");
        if (key == null || !key.startsWith(namespacePrefix) || key.length() == namespacePrefix.length()) {
            throw new IllegalArgumentException("Object key is outside this Cell Provider Scope");
        }
    }

    private void requireOwnedPrefix(String prefix) {
        if (prefix == null || !prefix.startsWith(namespacePrefix)) {
            throw new IllegalArgumentException("LIST prefix is outside this Cell Provider Scope");
        }
        String canonicalPath = prefix.endsWith("/") ? prefix.substring(0, prefix.length() - 1) : prefix;
        requireCanonicalPath(canonicalPath, MAX_PROVIDER_OBJECT_KEY_BYTES, "LIST prefix");
    }

    /** Exclusive Cell namespaces contain only keys admitted through this exact bounded ASCII grammar. */
    private static void requireCanonicalPath(String value, int maximumBytes, String field) {
        if (value == null || value.isEmpty() || value.length() > maximumBytes) {
            throw new IllegalArgumentException(field + " is empty or exceeds the canonical byte cap");
        }
        if (!isAsciiAlphanumeric(value.charAt(0)) || value.endsWith("/") || value.contains("//")) {
            throw new IllegalArgumentException(field + " is not a canonical relative Object path");
        }
        for (String segment : value.split("/", -1)) {
            if (segment.equals(".") || segment.equals("..")) {
                throw new IllegalArgumentException(field + " contains a non-canonical path segment");
            }
            for (int index = 0; index < segment.length(); index++) {
                char character = segment.charAt(index);
                if (!isAsciiAlphanumeric(character) && character != '.' && character != '_' && character != '-') {
                    throw new IllegalArgumentException(field + " contains a non-canonical Object-key character");
                }
            }
        }
    }

    private static boolean isAsciiAlphanumeric(char value) {
        return (value >= '0' && value <= '9') || (value >= 'A' && value <= 'Z') || (value >= 'a' && value <= 'z');
    }

    private void requireBodyWithinCap(ObjectIdentity identity) {
        if (identity.bodyLength() > admittedMaximumObjectBytes) {
            throw new IllegalArgumentException("Object body exceeds the admitted Provider cap");
        }
    }

    private static CanonicalBytes readExactBounded(InputStream input, int exactBytes) throws IOException {
        ByteArrayOutputStream output = new ByteArrayOutputStream(exactBytes);
        byte[] buffer = new byte[Math.min(exactBytes, 8192)];
        int remaining = exactBytes;
        while (remaining > 0) {
            int read = input.read(buffer, 0, Math.min(buffer.length, remaining));
            if (read < 0) {
                throw new IOException("Provider range response is truncated");
            }
            if (read == 0) {
                continue;
            }
            output.write(buffer, 0, read);
            remaining -= read;
        }
        if (input.read() != -1) {
            throw new IOException("Provider range response exceeds the requested range");
        }
        return CanonicalBytes.copyOf(output.toByteArray());
    }

    private static Sha256Digest streamSha256(InputStream input, long exactLength) throws IOException {
        MessageDigest digest = newSha256();
        byte[] buffer = new byte[8192];
        long remaining = exactLength;
        while (remaining > 0) {
            int read = input.read(buffer, 0, (int) Math.min(buffer.length, remaining));
            if (read < 0) {
                throw new IOException("Provider full-Object response is truncated");
            }
            if (read == 0) {
                continue;
            }
            digest.update(buffer, 0, read);
            remaining -= read;
        }
        if (input.read() != -1) {
            throw new IOException("Provider full-Object response exceeds expected length");
        }
        return Sha256Digest.copyOf(digest.digest());
    }

    private static void verifyRepeatableBody(RepeatableObjectBody body, ObjectIdentity identity) throws IOException {
        Sha256Digest observed;
        try (InputStream input = body.openStream()) {
            observed = streamSha256(input, identity.bodyLength());
        } catch (IOException failure) {
            throw new IOException("Repeatable Object body does not have its exact identity length", failure);
        }
        if (!observed.equals(identity.bodySha256())) {
            throw new IOException("Repeatable Object body SHA-256 differs from its exact identity");
        }
    }

    private static MessageDigest newSha256() {
        try {
            return MessageDigest.getInstance("SHA-256");
        } catch (NoSuchAlgorithmException failure) {
            throw new IllegalStateException("JDK has no SHA-256 provider", failure);
        }
    }

    /** Recovery-only facade that can be irreversibly promoted to the exact final WalRun owner once. */
    public final class RecoveryLease implements AutoCloseable {
        private final Sha256Digest rootSha256;

        private RecoveryLease(Sha256Digest rootSha256) {
            this.rootSha256 = rootSha256;
        }

        public CellProviderScopeId providerScopeId() {
            return C1ObjectProviderSession.this.providerScopeId;
        }

        public String exclusiveNamespacePrefix() {
            return C1ObjectProviderSession.this.exclusiveNamespacePrefix();
        }

        public long admittedMaximumObjectBytes() {
            return C1ObjectProviderSession.this.admittedMaximumObjectBytes;
        }

        public int admittedMaximumPrefixBytes() {
            return C1ObjectProviderSession.this.admittedMaximumPrefixBytes;
        }

        public Sha256Digest rootSha256() {
            return rootSha256;
        }

        public State state() {
            synchronized (C1ObjectProviderSession.this) {
                return C1ObjectProviderSession.this.state;
            }
        }

        public long acceptedOperations() {
            return C1ObjectProviderSession.this.acceptedOperations.get();
        }

        public int unknownObjectCount() {
            synchronized (C1ObjectProviderSession.this) {
                return C1ObjectProviderSession.this.unknownObjects.size();
            }
        }

        public synchronized ProviderReconciliationResult reconcileUnknown(
                ObjectIdentity identity,
                String leafPrefix,
                int maximumListPages,
                long maximumListKeys,
                long maximumListKeyBytes,
                int maximumSingleKeyBytes)
                throws IOException {
            beginRecoveryCall();
            try {
                return reconcileUnknownInternal(
                        identity,
                        leafPrefix,
                        maximumListPages,
                        maximumListKeys,
                        maximumListKeyBytes,
                        maximumSingleKeyBytes);
            } finally {
                endRecoveryCall();
            }
        }

        public synchronized CanonicalBytes readDirectoryPrefix(
                ObjectIdentity identity, int directoryPrefixEnd, Optional<CanonicalBytes> versionToken)
                throws IOException {
            beginRecoveryCall();
            try {
                return readDirectoryPrefixInternal(identity, directoryPrefixEnd, versionToken);
            } finally {
                endRecoveryCall();
            }
        }

        public synchronized CanonicalBytes readExactRange(
                ObjectIdentity identity, long inclusiveStart, long exclusiveEnd, Optional<CanonicalBytes> versionToken)
                throws IOException {
            beginRecoveryCall();
            try {
                return readExactRangeInternal(identity, inclusiveStart, exclusiveEnd, versionToken);
            } finally {
                endRecoveryCall();
            }
        }

        public synchronized CanonicalBytes readVerifiedObject(ObjectIdentity identity) throws IOException {
            return readVerifiedObjectWithVersion(identity).canonicalBody();
        }

        public synchronized VerifiedObjectRead readVerifiedObjectWithVersion(ObjectIdentity identity)
                throws IOException {
            beginRecoveryCall();
            try {
                return readVerifiedObjectWithVersionInternal(identity);
            } finally {
                endRecoveryCall();
            }
        }

        public synchronized ProviderAbsenceProof proveAbsent(
                ObjectIdentity identity,
                String leafPrefix,
                int maximumListPages,
                long maximumListKeys,
                long maximumListKeyBytes,
                int maximumSingleKeyBytes)
                throws IOException {
            beginRecoveryCall();
            try {
                return proveAbsentOwned(
                        identity,
                        leafPrefix,
                        maximumListPages,
                        maximumListKeys,
                        maximumListKeyBytes,
                        maximumSingleKeyBytes);
            } finally {
                endRecoveryCall();
            }
        }

        public synchronized StrongListResult strongList(
                String prefix,
                int maximumPages,
                long maximumKeys,
                long maximumCanonicalKeyBytes,
                int maximumSingleKeyBytes)
                throws IOException {
            beginRecoveryCall();
            try {
                return strongListOwned(
                        prefix, maximumPages, maximumKeys, maximumCanonicalKeyBytes, maximumSingleKeyBytes);
            } finally {
                endRecoveryCall();
            }
        }

        public synchronized StreamingListResult strongListStreaming(
                String prefix, StreamingListBounds bounds, ListedObjectConsumer consumer) throws IOException {
            beginRecoveryCall();
            try {
                return strongListStreamingOwned(prefix, bounds, consumer);
            } finally {
                endRecoveryCall();
            }
        }

        public synchronized void requireFinalTransferReady(WalRunObjectSession.ProviderOwnerAuthority authority) {
            synchronized (C1ObjectProviderSession.this) {
                requireRecoveryOwner();
                if (state != State.OPEN || externalCalls != 0 || acceptedOperations.get() != 0) {
                    throw new IllegalStateException("Provider recovery lease retains active work or is not open");
                }
                if (!unknownObjects.isEmpty()) {
                    throw new IllegalStateException("Provider recovery lease retains an unknown Object operation");
                }
                Objects.requireNonNull(authority, "authority");
                requireExactProviderAuthority(
                        authority.providerScopeId(),
                        authority.exclusiveNamespacePrefix(),
                        authority.admittedMaximumObjectBytes(),
                        authority.admittedMaximumPrefixBytes());
                if (!rootSha256.equals(authority.rootSha256())) {
                    throw new IllegalArgumentException("Provider recovery lease belongs to another exact Root");
                }
            }
        }

        public synchronized WalRunLease transferToWalRun(WalRunObjectSession.ProviderOwnerAuthority authority) {
            synchronized (C1ObjectProviderSession.this) {
                requireFinalTransferReady(authority);
                WalRunLease lease = new WalRunLease(authority);
                lifecycleOwner = LifecycleOwner.WAL_RUN;
                return lease;
            }
        }

        public synchronized void drain() {
            synchronized (C1ObjectProviderSession.this) {
                requireRecoveryOwner();
                drainInternal();
            }
        }

        @Override
        public synchronized void close() {
            synchronized (C1ObjectProviderSession.this) {
                requireRecoveryOwner();
                closeInternal();
            }
        }

        private void requireRecoveryOwner() {
            if (lifecycleOwner != LifecycleOwner.RECOVERY) {
                throw new IllegalStateException("Cell Provider recovery lease no longer owns lifecycle authority");
            }
        }
    }

    /** Root-bound facade returned only after an unforgeable owner-authority transfer. */
    public final class WalRunLease implements AutoCloseable {
        private final CellProviderScopeId providerScopeId;
        private final String exclusiveNamespacePrefix;
        private final long admittedMaximumObjectBytes;
        private final int admittedMaximumPrefixBytes;

        private WalRunLease(WalRunObjectSession.ProviderOwnerAuthority authority) {
            this.providerScopeId = authority.providerScopeId();
            this.exclusiveNamespacePrefix = authority.exclusiveNamespacePrefix();
            this.admittedMaximumObjectBytes = authority.admittedMaximumObjectBytes();
            this.admittedMaximumPrefixBytes = authority.admittedMaximumPrefixBytes();
        }

        public CellProviderScopeId providerScopeId() {
            return providerScopeId;
        }

        public String exclusiveNamespacePrefix() {
            return exclusiveNamespacePrefix;
        }

        public long admittedMaximumObjectBytes() {
            return admittedMaximumObjectBytes;
        }

        public int admittedMaximumPrefixBytes() {
            return admittedMaximumPrefixBytes;
        }

        public State state() {
            synchronized (C1ObjectProviderSession.this) {
                return C1ObjectProviderSession.this.state;
            }
        }

        public long acceptedOperations() {
            return C1ObjectProviderSession.this.acceptedOperations.get();
        }

        public int unknownObjectCount() {
            synchronized (C1ObjectProviderSession.this) {
                return C1ObjectProviderSession.this.unknownObjects.size();
            }
        }

        public ProviderObjectResult conditionalCreate(RepeatableObjectBody body) throws IOException {
            return conditionalCreateInternal(body);
        }

        public ProviderReconciliationResult reconcileUnknown(
                ObjectIdentity identity,
                String leafPrefix,
                int maximumListPages,
                long maximumListKeys,
                long maximumListKeyBytes,
                int maximumSingleKeyBytes)
                throws IOException {
            return reconcileUnknownInternal(
                    identity,
                    leafPrefix,
                    maximumListPages,
                    maximumListKeys,
                    maximumListKeyBytes,
                    maximumSingleKeyBytes);
        }

        public CanonicalBytes readDirectoryPrefix(
                ObjectIdentity identity, int directoryPrefixEnd, Optional<CanonicalBytes> versionToken)
                throws IOException {
            return readDirectoryPrefixInternal(identity, directoryPrefixEnd, versionToken);
        }

        public CanonicalBytes readExactRange(
                ObjectIdentity identity, long inclusiveStart, long exclusiveEnd, Optional<CanonicalBytes> versionToken)
                throws IOException {
            return readExactRangeInternal(identity, inclusiveStart, exclusiveEnd, versionToken);
        }

        public CanonicalBytes readVerifiedObject(ObjectIdentity identity) throws IOException {
            return readVerifiedObjectWithVersion(identity).canonicalBody();
        }

        public VerifiedObjectRead readVerifiedObjectWithVersion(ObjectIdentity identity) throws IOException {
            return readVerifiedObjectWithVersionInternal(identity);
        }

        public ProviderAbsenceProof proveAbsent(
                ObjectIdentity identity,
                String leafPrefix,
                int maximumListPages,
                long maximumListKeys,
                long maximumListKeyBytes,
                int maximumSingleKeyBytes)
                throws IOException {
            return proveAbsentOwned(
                    identity,
                    leafPrefix,
                    maximumListPages,
                    maximumListKeys,
                    maximumListKeyBytes,
                    maximumSingleKeyBytes);
        }

        public StrongListResult strongList(
                String prefix,
                int maximumPages,
                long maximumKeys,
                long maximumCanonicalKeyBytes,
                int maximumSingleKeyBytes)
                throws IOException {
            return strongListOwned(prefix, maximumPages, maximumKeys, maximumCanonicalKeyBytes, maximumSingleKeyBytes);
        }

        public StreamingListResult strongListStreaming(
                String prefix, StreamingListBounds bounds, ListedObjectConsumer consumer) throws IOException {
            return strongListStreamingOwned(prefix, bounds, consumer);
        }

        public void drain() {
            synchronized (C1ObjectProviderSession.this) {
                drainInternal();
            }
        }

        @Override
        public void close() {
            synchronized (C1ObjectProviderSession.this) {
                closeInternal();
            }
        }
    }

    /** Makes a CREATED response ineligible for APPLIED_EXACT until the uploaded stream proves its identity. */
    private static final class IdentityVerifyingInputStream extends InputStream {
        private final InputStream delegate;
        private final ObjectIdentity identity;
        private final MessageDigest digest = newSha256();
        private long observedBytes;
        private boolean verifiedAtEof;
        private IOException verificationFailure;

        private IdentityVerifyingInputStream(InputStream delegate, ObjectIdentity identity) {
            this.delegate = Objects.requireNonNull(delegate, "delegate");
            this.identity = identity;
        }

        @Override
        public int read() throws IOException {
            requireNoFailure();
            int value = delegate.read();
            if (value < 0) {
                verifyEof();
                return -1;
            }
            byte[] single = {(byte) value};
            observe(single, 0, 1);
            return value;
        }

        @Override
        public int read(byte[] target, int offset, int length) throws IOException {
            requireNoFailure();
            int read = delegate.read(target, offset, length);
            if (read < 0) {
                verifyEof();
                return -1;
            }
            if (read > 0) {
                observe(target, offset, read);
            }
            return read;
        }

        private void observe(byte[] value, int offset, int length) throws IOException {
            long next;
            try {
                next = Math.addExact(observedBytes, length);
            } catch (ArithmeticException failure) {
                fail("Repeatable Object body length arithmetic overflow");
                return;
            }
            if (next > identity.bodyLength()) {
                fail("Repeatable Object body exceeds its exact identity length");
            }
            digest.update(value, offset, length);
            observedBytes = next;
        }

        private void verifyEof() throws IOException {
            if (verifiedAtEof) {
                return;
            }
            if (observedBytes != identity.bodyLength()) {
                fail("Repeatable Object body is shorter than its exact identity length");
            }
            if (!Sha256Digest.copyOf(digest.digest()).equals(identity.bodySha256())) {
                fail("Repeatable Object body SHA-256 differs from its exact identity");
            }
            verifiedAtEof = true;
        }

        private void requireVerifiedAtEof() throws IOException {
            requireNoFailure();
            if (!verifiedAtEof && observedBytes == identity.bodyLength()) {
                read();
            }
            if (!verifiedAtEof) {
                throw new IOException("Provider returned before consuming and proving the exact Object body");
            }
        }

        private void requireNoFailure() throws IOException {
            if (verificationFailure != null) {
                throw verificationFailure;
            }
        }

        private void fail(String message) throws IOException {
            verificationFailure = new IOException(message);
            throw verificationFailure;
        }

        @Override
        public void close() throws IOException {
            delegate.close();
        }
    }

    private static final class Operation {
        private ProviderObjectOutcome outcome = ProviderObjectOutcome.OUTCOME_UNKNOWN;
        private int conditionalCreateDispatches;
        private boolean claimed;
        private boolean terminal;

        private void claim(String action) {
            if (terminal) {
                throw new IllegalStateException("Provider operation is already terminal");
            }
            if (claimed) {
                throw new IllegalStateException("Provider operation is already claimed for " + action);
            }
            claimed = true;
        }

        private void releaseClaim() {
            if (!claimed) {
                throw new IllegalStateException("Provider operation claim was released twice");
            }
            claimed = false;
        }
    }

    private record ConditionalCreateClaim(Operation operation, ProviderObjectOutcome immediateOutcome) {
        private ConditionalCreateClaim {
            if ((operation == null) == (immediateOutcome == null)) {
                throw new IllegalArgumentException("conditional-create claim must dispatch or resolve immediately");
            }
        }

        private static ConditionalCreateClaim dispatch(Operation operation) {
            return new ConditionalCreateClaim(Objects.requireNonNull(operation, "operation"), null);
        }

        private static ConditionalCreateClaim immediate(ProviderObjectOutcome outcome) {
            return new ConditionalCreateClaim(null, Objects.requireNonNull(outcome, "outcome"));
        }
    }

    private enum VerificationKind {
        EXACT,
        MISMATCH,
        NOT_FOUND,
        UNKNOWN
    }

    private record Verification(VerificationKind kind, Optional<CanonicalBytes> versionToken) {
        private static Verification exact(Optional<CanonicalBytes> versionToken) {
            return new Verification(VerificationKind.EXACT, versionToken);
        }

        private static Verification mismatch() {
            return new Verification(VerificationKind.MISMATCH, Optional.empty());
        }

        private static Verification notFound() {
            return new Verification(VerificationKind.NOT_FOUND, Optional.empty());
        }

        private static Verification unknown() {
            return new Verification(VerificationKind.UNKNOWN, Optional.empty());
        }
    }
}
