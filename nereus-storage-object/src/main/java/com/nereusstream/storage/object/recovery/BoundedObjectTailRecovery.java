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

package com.nereusstream.storage.object.recovery;

import com.nereusstream.domain.bytes.CanonicalBytes;
import com.nereusstream.storage.object.control.CanonicalControlMetadataStore;
import com.nereusstream.storage.object.control.LaneSequenceVector;
import com.nereusstream.storage.object.control.ObjectWalLeafKeyV1;
import com.nereusstream.storage.object.control.ProviderResolvedExtentRowV1;
import com.nereusstream.storage.object.control.ProviderVersionProof;
import com.nereusstream.storage.object.control.WalCheckpointChainVerifier;
import com.nereusstream.storage.object.control.WalCheckpointHeadV1;
import com.nereusstream.storage.object.control.WalLaneId;
import com.nereusstream.storage.object.control.WalRunControlCodec;
import com.nereusstream.storage.object.control.WalRunControlKeys;
import com.nereusstream.storage.object.control.WalRunRootRecord;
import com.nereusstream.storage.object.control.WalRunRuntime;
import com.nereusstream.storage.object.kms.KmsCellSession;
import com.nereusstream.storage.object.nwg1.Nwg1ObjectReaderV1;
import com.nereusstream.storage.object.nwg1.Nwg1VerificationContextV1;
import com.nereusstream.storage.object.provider.C1ObjectProviderSession;
import com.nereusstream.storage.object.provider.ObjectIdentity;
import com.nereusstream.storage.object.provider.ObjectProviderTransport;
import com.nereusstream.storage.object.provider.ProviderObjectOutcome;
import com.nereusstream.storage.object.provider.ProviderObjectResult;
import com.nereusstream.storage.object.provider.ProviderReconciliationResult;
import com.nereusstream.storage.object.provider.StrongListResult;
import java.io.IOException;
import java.nio.charset.StandardCharsets;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.Comparator;
import java.util.HashSet;
import java.util.LinkedHashMap;
import java.util.List;
import java.util.Map;
import java.util.Objects;
import java.util.Optional;
import java.util.Set;
import java.util.function.Consumer;
import java.util.function.LongSupplier;

/** C1 open-tail operations charged to one cumulative envelope; checkpoint fallback receives the same budget object. */
public final class BoundedObjectTailRecovery {
    private final ProviderAccess provider;
    private final CumulativeRecoveryBudget budget;
    private final WalRunRootRecord root;
    private final int exactExtentKeyBytes;
    private boolean compositeWorkingSetActive;
    private final Set<ObjectIdentity> reconciliationAttempts = new HashSet<>();

    BoundedObjectTailRecovery(C1ObjectProviderSession provider, CumulativeRecoveryBudget budget) {
        this.provider = new RawProviderAccess(Objects.requireNonNull(provider, "provider"));
        this.budget = Objects.requireNonNull(budget, "budget");
        this.root = null;
        this.exactExtentKeyBytes = Math.addExact(
                provider.exclusiveNamespacePrefix().getBytes(StandardCharsets.US_ASCII).length,
                ObjectWalLeafKeyV1.ROOT_SUFFIX_BYTES);
    }

    /** Package-local test constructor; production must transfer and use the Root-bound Provider lease. */
    BoundedObjectTailRecovery(C1ObjectProviderSession provider, WalRunRootRecord root, LongSupplier nanoTime) {
        this(
                new RawProviderAccess(Objects.requireNonNull(provider, "provider")),
                root,
                new CumulativeRecoveryBudget(root.recoveryEnvelope(), Objects.requireNonNull(nanoTime, "nanoTime")));
    }

    /** Production constructor: Root-persisted caps plus the transferred Provider lease are the only authorities. */
    public BoundedObjectTailRecovery(
            C1ObjectProviderSession.WalRunLease provider, WalRunRootRecord root, LongSupplier nanoTime) {
        this(
                new LeaseProviderAccess(Objects.requireNonNull(provider, "provider")),
                root,
                new CumulativeRecoveryBudget(root.recoveryEnvelope(), Objects.requireNonNull(nanoTime, "nanoTime")));
    }

    /** Prepares the only fallible new-Root budget work before Provider/KMS ownership is committed. */
    public static PreparedNewRootRecovery prepareNewRoot(WalRunRootRecord root, LongSupplier nanoTime) {
        Objects.requireNonNull(root, "root");
        return new PreparedNewRootRecovery(
                root,
                new CumulativeRecoveryBudget(root.recoveryEnvelope(), Objects.requireNonNull(nanoTime, "nanoTime")));
    }

    /** Binds a prepared new-Root budget to the transferred Provider lease exactly once. */
    public static BoundedObjectTailRecovery fromPreparedNewRoot(
            C1ObjectProviderSession.WalRunLease provider, WalRunRootRecord root, PreparedNewRootRecovery prepared) {
        Objects.requireNonNull(prepared, "prepared");
        return new BoundedObjectTailRecovery(
                new LeaseProviderAccess(Objects.requireNonNull(provider, "provider")), root, prepared.consumeFor(root));
    }

    /**
     * Consumes the one-use current-Root lineage handoff. This is the only production tail constructor that may reuse
     * prior lineage charges rather than start a fresh cumulative envelope.
     */
    public static BoundedObjectTailRecovery fromRecoveredRuntimeCut(
            C1ObjectProviderSession.WalRunLease provider, WalRunRootRecord root, RecoveredWalRunRuntimeCut cut) {
        Objects.requireNonNull(cut, "cut");
        BoundedObjectTailRecovery recovery = new BoundedObjectTailRecovery(
                new LeaseProviderAccess(Objects.requireNonNull(provider, "provider")),
                root,
                cut.consumeBudgetFor(root));
        recovery.compositeWorkingSetActive = true;
        return recovery;
    }

    /** Marks the transferred owner-open composite lease released after its one-use physical row fold. */
    public void finishRecoveredPhysicalRows() {
        if (!compositeWorkingSetActive) {
            throw new IllegalStateException("owner-open composite working set is not active");
        }
        compositeWorkingSetActive = false;
    }

    private BoundedObjectTailRecovery(
            ProviderAccess provider, WalRunRootRecord root, CumulativeRecoveryBudget liveBudget) {
        this.provider = Objects.requireNonNull(provider, "provider");
        this.root = Objects.requireNonNull(root, "root");
        this.budget = Objects.requireNonNull(liveBudget, "liveBudget");
        if (!provider.providerScopeId().equals(root.providerScopeId())
                || !provider.exclusiveNamespacePrefix()
                        .equals(root.providerConfiguration().exclusiveNamespacePrefix())
                || provider.admittedMaximumObjectBytes()
                        != root.providerConfiguration().maxObjectBodyBytes()
                || provider.admittedMaximumPrefixBytes()
                        != root.nwg1AdmissionCaps().maxDirectoryPrefixBytes()) {
            throw new IllegalArgumentException("C1 Provider session differs from the exact WalRun Root authority");
        }
        this.exactExtentKeyBytes = ObjectWalLeafKeyV1.maximumFullKeyBytes(root.providerConfiguration());
    }

    /** One-use prepared budget; it exposes neither counters nor replacement limits. */
    public static final class PreparedNewRootRecovery {
        private final WalRunRootRecord root;
        private CumulativeRecoveryBudget budget;

        private PreparedNewRootRecovery(WalRunRootRecord root, CumulativeRecoveryBudget budget) {
            this.root = Objects.requireNonNull(root, "root");
            this.budget = Objects.requireNonNull(budget, "budget");
        }

        private synchronized CumulativeRecoveryBudget consumeFor(WalRunRootRecord expectedRoot) {
            if (!root.equals(Objects.requireNonNull(expectedRoot, "expectedRoot"))) {
                throw new IllegalArgumentException("prepared recovery budget belongs to another Root");
            }
            if (budget == null) {
                throw new IllegalStateException("prepared recovery budget was already consumed");
            }
            CumulativeRecoveryBudget consumed = budget;
            budget = null;
            return consumed;
        }
    }

    private interface ProviderAccess {
        com.nereusstream.storage.api.bookkeeper.CellProviderScopeId providerScopeId();

        String exclusiveNamespacePrefix();

        long admittedMaximumObjectBytes();

        int admittedMaximumPrefixBytes();

        StrongListResult strongList(
                String prefix,
                int maximumPages,
                long maximumKeys,
                long maximumCanonicalKeyBytes,
                int maximumSingleKeyBytes)
                throws IOException;

        C1ObjectProviderSession.StreamingListResult strongListStreaming(
                String prefix,
                C1ObjectProviderSession.StreamingListBounds bounds,
                C1ObjectProviderSession.ListedObjectConsumer consumer)
                throws IOException;

        ProviderReconciliationResult reconcileUnknown(
                ObjectIdentity identity,
                String leafPrefix,
                int maximumListPages,
                long maximumListKeys,
                long maximumListKeyBytes,
                int maximumSingleKeyBytes)
                throws IOException;

        CanonicalBytes readDirectoryPrefix(
                ObjectIdentity identity, int directoryPrefixEnd, Optional<CanonicalBytes> versionToken)
                throws IOException;

        CanonicalBytes readVerifiedObject(ObjectIdentity identity) throws IOException;

        CanonicalBytes readExactRange(
                ObjectIdentity identity, long inclusiveStart, long exclusiveEnd, Optional<CanonicalBytes> versionToken)
                throws IOException;
    }

    private record RawProviderAccess(C1ObjectProviderSession delegate) implements ProviderAccess {
        private RawProviderAccess {
            Objects.requireNonNull(delegate, "delegate");
        }

        @Override
        public com.nereusstream.storage.api.bookkeeper.CellProviderScopeId providerScopeId() {
            return delegate.providerScopeId();
        }

        @Override
        public String exclusiveNamespacePrefix() {
            return delegate.exclusiveNamespacePrefix();
        }

        @Override
        public long admittedMaximumObjectBytes() {
            return delegate.admittedMaximumObjectBytes();
        }

        @Override
        public int admittedMaximumPrefixBytes() {
            return delegate.admittedMaximumPrefixBytes();
        }

        @Override
        public StrongListResult strongList(
                String prefix,
                int maximumPages,
                long maximumKeys,
                long maximumCanonicalKeyBytes,
                int maximumSingleKeyBytes)
                throws IOException {
            return delegate.strongList(
                    prefix, maximumPages, maximumKeys, maximumCanonicalKeyBytes, maximumSingleKeyBytes);
        }

        @Override
        public C1ObjectProviderSession.StreamingListResult strongListStreaming(
                String prefix,
                C1ObjectProviderSession.StreamingListBounds bounds,
                C1ObjectProviderSession.ListedObjectConsumer consumer)
                throws IOException {
            return delegate.strongListStreaming(prefix, bounds, consumer);
        }

        @Override
        public ProviderReconciliationResult reconcileUnknown(
                ObjectIdentity identity,
                String leafPrefix,
                int maximumListPages,
                long maximumListKeys,
                long maximumListKeyBytes,
                int maximumSingleKeyBytes)
                throws IOException {
            return delegate.reconcileUnknown(
                    identity,
                    leafPrefix,
                    maximumListPages,
                    maximumListKeys,
                    maximumListKeyBytes,
                    maximumSingleKeyBytes);
        }

        @Override
        public CanonicalBytes readDirectoryPrefix(
                ObjectIdentity identity, int directoryPrefixEnd, Optional<CanonicalBytes> versionToken)
                throws IOException {
            return delegate.readDirectoryPrefix(identity, directoryPrefixEnd, versionToken);
        }

        @Override
        public CanonicalBytes readVerifiedObject(ObjectIdentity identity) throws IOException {
            return delegate.readVerifiedObject(identity);
        }

        @Override
        public CanonicalBytes readExactRange(
                ObjectIdentity identity, long inclusiveStart, long exclusiveEnd, Optional<CanonicalBytes> versionToken)
                throws IOException {
            return delegate.readExactRange(identity, inclusiveStart, exclusiveEnd, versionToken);
        }
    }

    private record LeaseProviderAccess(C1ObjectProviderSession.WalRunLease delegate) implements ProviderAccess {
        private LeaseProviderAccess {
            Objects.requireNonNull(delegate, "delegate");
        }

        @Override
        public com.nereusstream.storage.api.bookkeeper.CellProviderScopeId providerScopeId() {
            return delegate.providerScopeId();
        }

        @Override
        public String exclusiveNamespacePrefix() {
            return delegate.exclusiveNamespacePrefix();
        }

        @Override
        public long admittedMaximumObjectBytes() {
            return delegate.admittedMaximumObjectBytes();
        }

        @Override
        public int admittedMaximumPrefixBytes() {
            return delegate.admittedMaximumPrefixBytes();
        }

        @Override
        public StrongListResult strongList(
                String prefix,
                int maximumPages,
                long maximumKeys,
                long maximumCanonicalKeyBytes,
                int maximumSingleKeyBytes)
                throws IOException {
            return delegate.strongList(
                    prefix, maximumPages, maximumKeys, maximumCanonicalKeyBytes, maximumSingleKeyBytes);
        }

        @Override
        public C1ObjectProviderSession.StreamingListResult strongListStreaming(
                String prefix,
                C1ObjectProviderSession.StreamingListBounds bounds,
                C1ObjectProviderSession.ListedObjectConsumer consumer)
                throws IOException {
            return delegate.strongListStreaming(prefix, bounds, consumer);
        }

        @Override
        public ProviderReconciliationResult reconcileUnknown(
                ObjectIdentity identity,
                String leafPrefix,
                int maximumListPages,
                long maximumListKeys,
                long maximumListKeyBytes,
                int maximumSingleKeyBytes)
                throws IOException {
            return delegate.reconcileUnknown(
                    identity,
                    leafPrefix,
                    maximumListPages,
                    maximumListKeys,
                    maximumListKeyBytes,
                    maximumSingleKeyBytes);
        }

        @Override
        public CanonicalBytes readDirectoryPrefix(
                ObjectIdentity identity, int directoryPrefixEnd, Optional<CanonicalBytes> versionToken)
                throws IOException {
            return delegate.readDirectoryPrefix(identity, directoryPrefixEnd, versionToken);
        }

        @Override
        public CanonicalBytes readVerifiedObject(ObjectIdentity identity) throws IOException {
            return delegate.readVerifiedObject(identity);
        }

        @Override
        public CanonicalBytes readExactRange(
                ObjectIdentity identity, long inclusiveStart, long exclusiveEnd, Optional<CanonicalBytes> versionToken)
                throws IOException {
            return delegate.readExactRange(identity, inclusiveStart, exclusiveEnd, versionToken);
        }
    }

    private record RecoveryLeaseProviderAccess(C1ObjectProviderSession.RecoveryLease delegate)
            implements ProviderAccess {
        private RecoveryLeaseProviderAccess {
            Objects.requireNonNull(delegate, "delegate");
        }

        @Override
        public com.nereusstream.storage.api.bookkeeper.CellProviderScopeId providerScopeId() {
            return delegate.providerScopeId();
        }

        @Override
        public String exclusiveNamespacePrefix() {
            return delegate.exclusiveNamespacePrefix();
        }

        @Override
        public long admittedMaximumObjectBytes() {
            return delegate.admittedMaximumObjectBytes();
        }

        @Override
        public int admittedMaximumPrefixBytes() {
            return delegate.admittedMaximumPrefixBytes();
        }

        @Override
        public StrongListResult strongList(
                String prefix,
                int maximumPages,
                long maximumKeys,
                long maximumCanonicalKeyBytes,
                int maximumSingleKeyBytes)
                throws IOException {
            return delegate.strongList(
                    prefix, maximumPages, maximumKeys, maximumCanonicalKeyBytes, maximumSingleKeyBytes);
        }

        @Override
        public C1ObjectProviderSession.StreamingListResult strongListStreaming(
                String prefix,
                C1ObjectProviderSession.StreamingListBounds bounds,
                C1ObjectProviderSession.ListedObjectConsumer consumer)
                throws IOException {
            return delegate.strongListStreaming(prefix, bounds, consumer);
        }

        @Override
        public ProviderReconciliationResult reconcileUnknown(
                ObjectIdentity identity,
                String leafPrefix,
                int maximumListPages,
                long maximumListKeys,
                long maximumListKeyBytes,
                int maximumSingleKeyBytes)
                throws IOException {
            return delegate.reconcileUnknown(
                    identity,
                    leafPrefix,
                    maximumListPages,
                    maximumListKeys,
                    maximumListKeyBytes,
                    maximumSingleKeyBytes);
        }

        @Override
        public CanonicalBytes readDirectoryPrefix(
                ObjectIdentity identity, int directoryPrefixEnd, Optional<CanonicalBytes> versionToken)
                throws IOException {
            return delegate.readDirectoryPrefix(identity, directoryPrefixEnd, versionToken);
        }

        @Override
        public CanonicalBytes readVerifiedObject(ObjectIdentity identity) throws IOException {
            return delegate.readVerifiedObject(identity);
        }

        @Override
        public CanonicalBytes readExactRange(
                ObjectIdentity identity, long inclusiveStart, long exclusiveEnd, Optional<CanonicalBytes> versionToken)
                throws IOException {
            return delegate.readExactRange(identity, inclusiveStart, exclusiveEnd, versionToken);
        }
    }

    StrongListResult discoverUncoveredLane(String lanePrefix, int maximumPages, long maximumKeys, long maximumKeyBytes)
            throws IOException {
        CumulativeRecoveryBudget.ListReservation reservation =
                budget.reserveList(maximumPages, maximumKeys, maximumKeyBytes);
        StrongListResult result = provider.strongList(
                lanePrefix,
                reservation.maximumPages(),
                reservation.maximumKeys(),
                reservation.maximumCanonicalKeyBytes(),
                exactExtentKeyBytes);
        reservation.settle(result.pageCount(), result.objects().size(), result.canonicalKeyBytes());
        return result;
    }

    /** Package-local materializing compatibility path; production recovery uses the streaming fold below. */
    RecoveredLaneInventory discoverUncoveredLane(WalLaneId laneId) throws IOException {
        requireRootBound();
        Objects.requireNonNull(laneId, "laneId");
        String lanePrefix = root.providerConfiguration().exclusiveNamespacePrefix() + "/" + laneId.leafToken() + "/";
        CumulativeRecoveryBudget.ListReservation reservation = budget.reserveRemainingList();
        if (!compositeWorkingSetActive) {
            budget.acquireWorkingSet(reservation.maximumCanonicalKeyBytes());
        }
        try {
            StrongListResult result = provider.strongList(
                    lanePrefix,
                    reservation.maximumPages(),
                    reservation.maximumKeys(),
                    reservation.maximumCanonicalKeyBytes(),
                    exactExtentKeyBytes);
            reservation.settle(result.pageCount(), result.objects().size(), result.canonicalKeyBytes());
            return validateExtentInventory(laneId, result);
        } finally {
            if (!compositeWorkingSetActive) {
                budget.releaseWorkingSet(reservation.maximumCanonicalKeyBytes());
            }
        }
    }

    /**
     * Complete exact-grammar lane inventory folded page-by-page with at most one delayed candidate. The consumer must
     * stage only; its state becomes eligible for publication after this method returns successfully.
     */
    public RecoveredLaneFold discoverUncoveredLaneStreaming(
            WalLaneId laneId, RecoveredExtentConsumer recoveredExtentConsumer) throws IOException {
        requireRootBound();
        Objects.requireNonNull(laneId, "laneId");
        Objects.requireNonNull(recoveredExtentConsumer, "recoveredExtentConsumer");
        String lanePrefix = root.providerConfiguration().exclusiveNamespacePrefix() + "/" + laneId.leafToken() + "/";
        CumulativeRecoveryBudget.ListReservation reservation = budget.reserveRemainingList();
        if (!compositeWorkingSetActive) {
            budget.acquireWorkingSet(reservation.maximumCanonicalKeyBytes());
        }
        RecoveredExtentCandidate[] pending = {null};
        long[] candidates = {0};
        try {
            C1ObjectProviderSession.StreamingListResult result = provider.strongListStreaming(
                    lanePrefix,
                    new C1ObjectProviderSession.StreamingListBounds(
                            reservation.maximumPages(),
                            reservation.maximumKeys(),
                            reservation.maximumCanonicalKeyBytes(),
                            exactExtentKeyBytes),
                    listed -> {
                        RecoveredExtentCandidate candidate = requireListedExtent(laneId, listed);
                        if (pending[0] != null) {
                            if (pending[0].leaf().laneSequence()
                                    == candidate.leaf().laneSequence()) {
                                throw new IOException("strong LIST returned two candidates for one lane sequence");
                            }
                            recoveredExtentConsumer.stage(pending[0]);
                            candidates[0] = Math.incrementExact(candidates[0]);
                        }
                        pending[0] = candidate;
                    });
            if (pending[0] != null) {
                recoveredExtentConsumer.stage(pending[0]);
                candidates[0] = Math.incrementExact(candidates[0]);
            }
            reservation.settle(result.pageCount(), result.keyCount(), result.canonicalKeyBytes());
            return new RecoveredLaneFold(laneId, result.pageCount(), candidates[0], result.canonicalKeyBytes());
        } finally {
            if (!compositeWorkingSetActive) {
                budget.releaseWorkingSet(reservation.maximumCanonicalKeyBytes());
            }
        }
    }

    private RecoveredExtentCandidate requireListedExtent(
            WalLaneId expectedLane, ObjectProviderTransport.ListedObject listed) {
        ObjectWalLeafKeyV1 leaf;
        try {
            leaf = ObjectWalLeafKeyV1.parseFull(root.providerConfiguration(), listed.key());
        } catch (IllegalArgumentException failure) {
            throw new IllegalStateException("strong LIST expanded outside the exact Object WAL inventory", failure);
        }
        if (leaf.laneId() != expectedLane
                || listed.bodyLength() != leaf.bodyLength()
                || leaf.directoryPrefixEnd() > root.nwg1AdmissionCaps().maxDirectoryPrefixBytes()
                || leaf.bodyLength() > root.nwg1AdmissionCaps().maxCanonicalBodyBytes()) {
            throw new IllegalStateException("strong LIST returned a substituted Object WAL extent");
        }
        Optional<CanonicalBytes> versionToken =
                switch (root.providerConfiguration().proofMode()) {
                    case NONE -> Optional.empty();
                    case VERSION_BOUND_FULL_OBJECT_SHA256_V1 -> {
                        CanonicalBytes exact = listed.immutableVersionToken()
                                .orElseThrow(
                                        () -> new IllegalStateException("VERSION LIST entry omits immutable token"));
                        if (exact.isEmpty()
                                || exact.length() > root.providerConfiguration().proofTokenHardCap()) {
                            throw new IllegalStateException("VERSION LIST token exceeds the exact Root proof cap");
                        }
                        yield Optional.of(CanonicalBytes.copyOf(exact.toByteArray()));
                    }
                };
        ObjectIdentity identity = new ObjectIdentity(listed.key(), listed.bodyLength(), leaf.objectSha256());
        return new RecoveredExtentCandidate(leaf, identity, versionToken);
    }

    /** Reconciles one unresolved NWG1 candidate with LIST/full-GET work charged before Provider I/O. */
    public ProviderObjectResult reconcileUnknownExtent(ObjectIdentity identity) throws IOException {
        requireRootBound();
        chargeRetryIfRepeated(identity);
        ObjectWalLeafKeyV1 leaf = requireExactExtentIdentity(identity);
        String lanePrefix = root.providerConfiguration().exclusiveNamespacePrefix()
                + "/"
                + leaf.laneId().leafToken()
                + "/";
        CumulativeRecoveryBudget.ListReservation reservation = budget.reserveRemainingList();
        budget.acquireWorkingSet(reservation.maximumCanonicalKeyBytes());
        try {
            budget.chargeFullGet(identity.bodyLength());
            ProviderReconciliationResult reconciliation = provider.reconcileUnknown(
                    identity,
                    lanePrefix,
                    reservation.maximumPages(),
                    reservation.maximumKeys(),
                    reservation.maximumCanonicalKeyBytes(),
                    exactExtentKeyBytes);
            reconciliation.inventory().ifPresent(inventory -> {
                reservation.settle(inventory.pageCount(), inventory.objects().size(), inventory.canonicalKeyBytes());
                validateExtentInventory(leaf.laneId(), inventory);
            });
            budget.checkWallTime();
            return reconciliation.objectResult();
        } finally {
            budget.releaseWorkingSet(reservation.maximumCanonicalKeyBytes());
        }
    }

    /** Root-bound protocol Object reconciliation; its content-key codec remains protocol-owned. */
    public ProviderObjectResult reconcileUnknownProtocolObject(ObjectIdentity identity) throws IOException {
        requireRootBound();
        Objects.requireNonNull(identity, "identity");
        chargeRetryIfRepeated(identity);
        String rootPrefix = root.providerConfiguration().exclusiveNamespacePrefix() + "/";
        if (!identity.key().startsWith(rootPrefix)) {
            throw new IllegalArgumentException("protocol Object identity lies outside the exact WalRun prefix");
        }
        int slash = identity.key().lastIndexOf('/');
        if (slash < rootPrefix.length()) {
            throw new IllegalArgumentException("protocol Object identity has no Root-bound family prefix");
        }
        int exactKeyBytes = identity.key().getBytes(StandardCharsets.UTF_8).length;
        CumulativeRecoveryBudget.ListReservation reservation = budget.reserveList(1, 1, exactKeyBytes);
        budget.acquireWorkingSet(reservation.maximumCanonicalKeyBytes());
        ProviderReconciliationResult reconciliation;
        try {
            budget.chargeFullGet(identity.bodyLength());
            reconciliation = provider.reconcileUnknown(identity, identity.key(), 1, 1, exactKeyBytes, exactKeyBytes);
            if (reconciliation.inventory().isEmpty()) {
                if (reconciliation.objectResult().outcome() == ProviderObjectOutcome.DEFINITIVE_CONFLICT) {
                    return reconciliation.objectResult();
                }
                throw new IllegalStateException("protocol reconciliation performed no exact-key Provider work");
            }
            StrongListResult inventory = reconciliation.inventory().orElseThrow();
            if (inventory.objects().stream().anyMatch(object -> !object.key().equals(identity.key()))) {
                throw new IllegalStateException(
                        "protocol reconciliation expanded beyond the exact content-addressed key");
            }
            reservation.settle(inventory.pageCount(), inventory.objects().size(), inventory.canonicalKeyBytes());
            budget.checkWallTime();
            return reconciliation.objectResult();
        } finally {
            budget.releaseWorkingSet(reservation.maximumCanonicalKeyBytes());
        }
    }

    /** Charges one Root-persisted retry slot before a frozen same-candidate PUT2 dispatch. */
    public void chargeConditionalCreateRetry() {
        requireRootBound();
        budget.chargeRetry();
    }

    private ProviderReconciliationResult reconcile(
            ObjectIdentity identity, String exactFamilyPrefix, int exactMaximumKeyBytes) throws IOException {
        CumulativeRecoveryBudget.ListReservation reservation = budget.reserveRemainingList();
        budget.chargeFullGet(identity.bodyLength());
        ProviderReconciliationResult reconciliation = provider.reconcileUnknown(
                identity,
                exactFamilyPrefix,
                reservation.maximumPages(),
                reservation.maximumKeys(),
                reservation.maximumCanonicalKeyBytes(),
                exactMaximumKeyBytes);
        reconciliation
                .inventory()
                .ifPresent(inventory -> reservation.settle(
                        inventory.pageCount(), inventory.objects().size(), inventory.canonicalKeyBytes()));
        budget.checkWallTime();
        return reconciliation;
    }

    /** Every supplied current-run extent receives one bounded prefix GET; no whole-Object fallback exists here. */
    public Map<ObjectIdentity, CanonicalBytes> reconstructDirectoryPrefixes(
            Map<ObjectIdentity, Integer> exactPrefixEnds) throws IOException {
        LinkedHashMap<ObjectIdentity, CanonicalBytes> result = new LinkedHashMap<>();
        for (Map.Entry<ObjectIdentity, Integer> entry : exactPrefixEnds.entrySet()) {
            if (root != null) {
                ObjectWalLeafKeyV1 leaf = requireExactExtentIdentity(entry.getKey());
                if (leaf.directoryPrefixEnd() != entry.getValue()) {
                    throw new IllegalArgumentException("requested prefix end differs from the exact Object WAL leaf");
                }
            }
            long workingBytes = entry.getValue();
            budget.acquireWorkingSet(workingBytes);
            try {
                budget.chargeRangeGet(workingBytes);
                CanonicalBytes prefix =
                        provider.readDirectoryPrefix(entry.getKey(), entry.getValue(), java.util.Optional.empty());
                budget.checkWallTime();
                result.put(entry.getKey(), prefix);
            } finally {
                budget.releaseWorkingSet(workingBytes);
            }
        }
        return Map.copyOf(result);
    }

    /** Separate bounded full GET for Root-bound protocol-checkpoint Objects such as NWKCP1. */
    public CanonicalBytes readVerifiedProtocolCheckpoint(ObjectIdentity identity) throws IOException {
        budget.acquireWorkingSet(identity.bodyLength());
        try {
            budget.chargeFullGet(identity.bodyLength());
            CanonicalBytes bytes = provider.readVerifiedObject(identity);
            budget.checkWallTime();
            return bytes;
        } finally {
            budget.releaseWorkingSet(identity.bodyLength());
        }
    }

    /** Exact current-run NWG1 full read, with identity grammar and cumulative work checked before I/O. */
    public CanonicalBytes readVerifiedExtent(ObjectIdentity identity) throws IOException {
        requireRootBound();
        requireExactExtentIdentity(identity);
        return readVerifiedProtocolCheckpoint(identity);
    }

    /** One exact selected-frame range, charged before I/O to the same Root cumulative recovery envelope. */
    public CanonicalBytes readExactFrameRange(ObjectIdentity identity, long inclusiveStart, long exclusiveEnd)
            throws IOException {
        requireRootBound();
        requireExactExtentIdentity(identity);
        long bytes = Math.subtractExact(exclusiveEnd, inclusiveStart);
        if (inclusiveStart < 0
                || exclusiveEnd > identity.bodyLength()
                || bytes <= 0
                || bytes > root.providerConfiguration().maxSingleRangeReadBytes()) {
            throw new IllegalArgumentException("selected frame range lies outside the exact Root/Object bounds");
        }
        if (!compositeWorkingSetActive) {
            budget.acquireWorkingSet(bytes);
        }
        try {
            budget.chargeRangeGet(bytes);
            CanonicalBytes result = provider.readExactRange(identity, inclusiveStart, exclusiveEnd, Optional.empty());
            budget.checkWallTime();
            return result;
        } finally {
            if (!compositeWorkingSetActive) {
                budget.releaseWorkingSet(bytes);
            }
        }
    }

    public CumulativeRecoveryBudget.Snapshot snapshot() {
        return budget.snapshot();
    }

    public void acquireWorkingSet(long exactBytes) {
        budget.acquireWorkingSet(exactBytes);
    }

    public void releaseWorkingSet(long exactBytes) {
        budget.releaseWorkingSet(exactBytes);
    }

    public void growWorkingSet(long exactBytes) {
        budget.growWorkingSet(exactBytes);
    }

    public void shrinkWorkingSet(long exactBytes) {
        budget.shrinkWorkingSet(exactBytes);
    }

    /**
     * Builds the only production owner-open runtime cut. It borrows (but does not consume) the current lineage
     * budget, charges the physical Head before metadata I/O, streams its complete page chain, then performs one
     * strong LIST for every permanent lane. Checkpoint/list overlap is exact and every uncovered suffix is required
     * to be contiguous; therefore neither a missing object nor an unaccounted duplicate can reopen a lane.
     */
    static RecoveredWalRunRuntimeCut recoverCurrentRuntimeCut(
            WalRunRootRecord root,
            CanonicalControlMetadataStore metadata,
            OwnerOpenRecoveryLeasePair recoveryLeases,
            WalRunLineageRecovery.RecoveredLineage recoveredLineage,
            Nwg1VerificationContextV1 verificationContext,
            RecoveredExtentStager recoveredExtentStager)
            throws IOException {
        Objects.requireNonNull(root, "root");
        Objects.requireNonNull(metadata, "metadata");
        Objects.requireNonNull(recoveryLeases, "recoveryLeases");
        Objects.requireNonNull(recoveredLineage, "recoveredLineage");
        Objects.requireNonNull(recoveredExtentStager, "recoveredExtentStager");
        requireVerificationContext(root, verificationContext);
        CumulativeRecoveryBudget sharedBudget = recoveredLineage.borrowForRuntimeCut(root);
        BoundedObjectTailRecovery recovery = new BoundedObjectTailRecovery(
                new RecoveryLeaseProviderAccess(recoveryLeases.provider), root, sharedBudget);
        long maximumRows = root.bounds().maxExtentCount();
        long spoolBytes = Math.multiplyExact(maximumRows, WalRunControlCodec.proofNoneCheckpointRowCanonicalLength());
        long protocolObjectCount = root.protocolCellIdentity().protocolKind().code() == 1 ? 1 : 0;
        long maximumLeafKeyBytes = ObjectWalLeafKeyV1.maximumFullKeyBytes(root.providerConfiguration());
        long listTransientBytes = Math.addExact(
                Math.multiplyExact(Math.addExact(maximumRows, 1), maximumLeafKeyBytes),
                Math.multiplyExact(protocolObjectCount, 1024));
        long transientBytes = Math.max(
                root.checkpointPolicy().maxCanonicalPageBytes(),
                Math.max(
                        root.nwg1AdmissionCaps().maxDirectoryPrefixBytes(),
                        Math.max(
                                Math.addExact(root.nwg1AdmissionCaps().maxDecodedFrameBytes(), 256),
                                Math.max(root.nwg1AdmissionCaps().maxStoredFrameBytes(), listTransientBytes))));
        CumulativeRecoveryBudget.CompositeWorkingSetLease composite =
                sharedBudget.acquireCompositeWorkingSet(Math.addExact(spoolBytes, transientBytes));
        RecoveredPhysicalRowSpool spool = null;
        boolean completed = false;
        try {
            spool = new RecoveredPhysicalRowSpool(maximumRows);
            recovery.compositeWorkingSetActive = true;
            String headKey = WalRunControlKeys.checkpointHeadKey(root.shardId(), root.shardRunEpoch());
            sharedBudget.chargeControlMetadata(1024 * 1024);
            CanonicalBytes headBytes = metadata.get(headKey)
                    .orElseThrow(() -> new IllegalStateException("current physical checkpoint Head is absent"));
            if (headBytes.length() > 1024 * 1024) {
                throw new IllegalStateException("current physical checkpoint Head exceeds the recovery control cap");
            }
            WalCheckpointHeadV1 head = WalRunControlCodec.decodeCheckpointHead(headBytes);
            RecoveredPhysicalRowSpool exactSpool = spool;
            WalCheckpointChainVerifier.StreamingVerification chain =
                    recovery.verifyCheckpointChainStreaming(metadata, head, exactSpool::appendCheckpoint);
            exactSpool.beginStrongListFold();
            long[] resolvedBytes = {0};
            for (WalLaneId lane : WalLaneId.values()) {
                long covered = head.coveredThrough().get(lane);
                recovery.discoverUncoveredLaneStreaming(lane, candidate -> {
                    if (candidate.immutableVersionToken().isPresent()) {
                        throw new IllegalStateException("M3 production strong LIST returned a VERSION proof token");
                    }
                    ProviderResolvedExtentRowV1 listedRow = new ProviderResolvedExtentRowV1(
                            lane,
                            candidate.leaf().laneSequence(),
                            candidate.leaf().directoryPrefixEnd(),
                            candidate.leaf().bodyLength(),
                            candidate.leaf().objectSha256(),
                            ProviderVersionProof.none());
                    exactSpool.acceptListedRow(lane, listedRow, covered);
                    resolvedBytes[0] = Math.addExact(resolvedBytes[0], listedRow.bodyLength());
                });
                exactSpool.endLane(lane, covered);
            }
            exactSpool.finishStrongListFold();
            if (chain.aggregateExtentCount() > exactSpool.rowCount()) {
                throw new IllegalStateException("strong LIST physical inventory is smaller than the checkpoint chain");
            }
            exactSpool.verifyRows(row -> authenticateAndStageRecoveredExtent(
                    recovery, recoveryLeases.kms, root, row, verificationContext, recoveredExtentStager));
            LaneSequenceVector resolved = LaneSequenceVector.of(
                    exactSpool.laneThrough(WalLaneId.OBJECT_LATENCY),
                    exactSpool.laneThrough(WalLaneId.OBJECT_BALANCED),
                    exactSpool.laneThrough(WalLaneId.OBJECT_COST));
            ArrayList<WalRunRuntime.RecoveredLane> recoveredLanes = new ArrayList<>();
            for (WalLaneId lane : WalLaneId.values()) {
                long through = exactSpool.laneThrough(lane);
                if (through >= 0) {
                    recoveredLanes.add(new WalRunRuntime.RecoveredLane(lane, through, Optional.empty(), false));
                }
            }
            long resolvedCount = exactSpool.rowCount();
            WalRunRuntime.RecoveredState runtimeState = new WalRunRuntime.RecoveredState(
                    WalRunRuntime.State.STOPPING,
                    Optional.of(WalRunRuntime.StopReason.OWNER_REQUEST),
                    recoveredLanes,
                    resolvedCount,
                    resolvedBytes[0],
                    resolvedCount,
                    resolvedBytes[0]);
            RecoveredWalRunRuntimeCut result = new RecoveredWalRunRuntimeCut(
                    root,
                    recoveredLineage,
                    headKey,
                    headBytes,
                    head,
                    chain,
                    resolved,
                    runtimeState,
                    sharedBudget,
                    recoveryLeases,
                    exactSpool,
                    composite);
            completed = true;
            return result;
        } finally {
            if (!completed) {
                recovery.compositeWorkingSetActive = false;
                if (spool != null) {
                    spool.close();
                }
                composite.close();
            }
        }
    }

    private static void requireVerificationContext(WalRunRootRecord root, Nwg1VerificationContextV1 context) {
        Objects.requireNonNull(context, "verificationContext");
        if (!context.protocolCell().equals(root.protocolCellIdentity())
                || !Arrays.equals(
                        context.cellProviderScopeId(),
                        root.providerScopeId().digest().bytes().toByteArray())
                || !Arrays.equals(
                        context.walRunRootSha256(),
                        WalRunControlCodec.rootSha256(root).bytes().toByteArray())
                || !Arrays.equals(
                        context.envelope().framedBytes(),
                        root.wrappedRunKey().framedBytes().toByteArray())) {
            throw new IllegalArgumentException("recovery verification context differs from the exact current Root");
        }
    }

    private static void authenticateAndStageRecoveredExtent(
            BoundedObjectTailRecovery recovery,
            KmsCellSession.RecoveryLease kms,
            WalRunRootRecord root,
            ProviderResolvedExtentRowV1 row,
            Nwg1VerificationContextV1 verificationContext,
            RecoveredExtentStager recoveredExtentStager)
            throws IOException {
        ObjectWalLeafKeyV1 leaf = ObjectWalLeafKeyV1.fromRow(row);
        ObjectIdentity identity =
                new ObjectIdentity(leaf.fullKey(root.providerConfiguration()), row.bodyLength(), row.objectSha256());
        recovery.budget.chargeRangeGet(row.directoryPrefixEnd());
        CanonicalBytes exactPrefix =
                recovery.provider.readDirectoryPrefix(identity, row.directoryPrefixEnd(), Optional.empty());
        Nwg1ObjectReaderV1.AuthenticatedPrefix prefix =
                kms.readAuthenticatedPrefix(exactPrefix.toByteArray(), row.bodyLength(), verificationContext);
        requirePrefixMatchesRowAndCaps(prefix, row, root);
        recovery.budget.chargeDecoded(
                prefix.directory().bindings().size(),
                prefix.directory().frames().size(),
                prefix.directory().appendUnits().size());
        int[] nextUnitOrdinal = {0};
        boolean[] callbackLive = {true};
        try {
            recoveredExtentStager.stage(row, prefix, (selectedFrameOrdinal, consumer) -> {
                if (!callbackLive[0]) {
                    throw new IllegalStateException("recovered append-unit reader escaped its fenced callback");
                }
                int frameOrdinal = Math.toIntExact(selectedFrameOrdinal);
                if (frameOrdinal < 0
                        || frameOrdinal >= prefix.directory().frames().size()) {
                    throw new IllegalArgumentException("selected recovery frame ordinal is outside Directory");
                }
                int unitOrdinal = Math.toIntExact(
                        prefix.directory().frames().get(frameOrdinal).appendUnitOrdinal());
                if (unitOrdinal != nextUnitOrdinal[0]) {
                    throw new IllegalStateException(
                            "recovered append units must be consumed once in canonical ordinal order");
                }
                Nwg1ObjectReaderV1.VerifiedAppendUnit verified = kms.readSelectedAppendUnitStreaming(
                        prefix,
                        (range, ignored) -> recovery.readExactFrameRange(
                                        identity, range.inclusiveStart(), range.exclusiveEnd())
                                .toByteArray(),
                        selectedFrameOrdinal,
                        verificationContext,
                        Objects.requireNonNull(consumer, "consumer"));
                nextUnitOrdinal[0] = Math.incrementExact(nextUnitOrdinal[0]);
                return verified;
            });
        } finally {
            callbackLive[0] = false;
        }
        if (nextUnitOrdinal[0] != prefix.directory().appendUnits().size()) {
            throw new IllegalStateException("recovered extent stager omitted an authenticated append unit");
        }
        recovery.budget.checkWallTime();
    }

    private static void requirePrefixMatchesRowAndCaps(
            Nwg1ObjectReaderV1.AuthenticatedPrefix prefix, ProviderResolvedExtentRowV1 row, WalRunRootRecord root) {
        if (prefix.header().laneId() != row.laneId().code()
                || prefix.header().laneSequence() != row.laneSequence()
                || prefix.header().directoryPrefixEnd() != row.directoryPrefixEnd()
                || prefix.header().canonicalBodyLength() != row.bodyLength()) {
            throw new IllegalStateException("authenticated NWG1 prefix differs from the exact physical row");
        }
        var caps = root.nwg1AdmissionCaps();
        if (prefix.header().canonicalBodyLength() > caps.maxCanonicalBodyBytes()
                || prefix.header().directoryPrefixEnd() > caps.maxDirectoryPrefixBytes()
                || prefix.header().directoryPlaintextLength() > caps.maxDirectoryPlaintextBytes()
                || prefix.directory().bindings().size() > caps.maxBindingContexts()
                || prefix.directory().appendUnits().size() > caps.maxAppendUnits()
                || prefix.directory().frames().size() > caps.maxFrames()
                || prefix.header().actualPayloadBytesAtPlanSeal() > caps.maxTotalDecodedPayloadBytes()) {
            throw new IllegalStateException("authenticated NWG1 Directory exceeds the exact Root caps");
        }
        long[] decodedPerUnit = new long[prefix.directory().appendUnits().size()];
        long decodedTotal = 0;
        for (var frame : prefix.directory().frames()) {
            if (frame.decodedPayloadBytes() > caps.maxDecodedFrameBytes()
                    || frame.storedBlockBytes() > caps.maxStoredFrameBytes()
                    || frame.storedBlockBytes() > root.providerConfiguration().maxSingleRangeReadBytes()) {
                throw new IllegalStateException("authenticated NWG1 frame exceeds the exact Root caps");
            }
            int unitOrdinal = Math.toIntExact(frame.appendUnitOrdinal());
            decodedPerUnit[unitOrdinal] = Math.addExact(decodedPerUnit[unitOrdinal], frame.decodedPayloadBytes());
            decodedTotal = Math.addExact(decodedTotal, frame.decodedPayloadBytes());
        }
        for (long decodedBytes : decodedPerUnit) {
            if (decodedBytes > caps.maxDecodedAppendUnitBytes()) {
                throw new IllegalStateException("authenticated NWG1 append unit exceeds the exact Root caps");
            }
        }
        if (decodedTotal != prefix.header().actualPayloadBytesAtPlanSeal()
                || decodedTotal > caps.maxTotalDecodedPayloadBytes()) {
            throw new IllegalStateException("authenticated NWG1 decoded total differs from the exact Root caps");
        }
    }

    private static RecoveredWalRunRuntimeCut.TailRecoveryRejectedException rejected(
            RecoveredWalRunRuntimeCut.TailDisposition disposition, String message) {
        return new RecoveredWalRunRuntimeCut.TailRecoveryRejectedException(disposition, message);
    }

    /**
     * Uses the same live Root-owned budget that paid for lineage and tail recovery; it never accepts a caller budget.
     */
    public WalCheckpointChainVerifier.StreamingVerification verifyCheckpointChainStreaming(
            CanonicalControlMetadataStore metadata,
            WalCheckpointHeadV1 head,
            Consumer<ProviderResolvedExtentRowV1> verifiedRowConsumer) {
        return verifyCheckpointChainStreaming(metadata, head, verifiedRowConsumer, ignored -> {});
    }

    public WalCheckpointChainVerifier.StreamingVerification verifyCheckpointChainStreaming(
            CanonicalControlMetadataStore metadata,
            WalCheckpointHeadV1 head,
            Consumer<ProviderResolvedExtentRowV1> verifiedRowConsumer,
            Consumer<WalCheckpointChainVerifier.VerifiedPageIdentity> verifiedPageConsumer) {
        if (root == null) {
            throw new IllegalStateException("checkpoint-chain recovery requires a Root-bound tail session");
        }
        return new WalCheckpointChainVerifier(metadata, root)
                .verifyStreaming(head, budget, verifiedRowConsumer, verifiedPageConsumer);
    }

    public void chargeRoot(boolean predecessor, long canonicalBytes) {
        budget.chargeRoot(predecessor, canonicalBytes);
    }

    public void chargeControlMetadata(long canonicalBytes) {
        budget.chargeControlMetadata(canonicalBytes);
    }

    public void chargeDecoded(long contexts, long frames, long commitSets) {
        budget.chargeDecoded(contexts, frames, commitSets);
    }

    public CumulativeRecoveryBudget.Snapshot enterFallback() {
        return budget.enterFallback();
    }

    private RecoveredLaneInventory validateExtentInventory(WalLaneId expectedLane, StrongListResult inventory) {
        ArrayList<RecoveredExtentCandidate> candidates =
                new ArrayList<>(inventory.objects().size());
        Set<Long> sequences = new HashSet<>();
        for (ObjectProviderTransport.ListedObject listed : inventory.objects()) {
            ObjectWalLeafKeyV1 leaf;
            try {
                leaf = ObjectWalLeafKeyV1.parseFull(root.providerConfiguration(), listed.key());
            } catch (IllegalArgumentException failure) {
                throw new IllegalStateException("strong LIST expanded outside the exact Object WAL inventory", failure);
            }
            if (leaf.laneId() != expectedLane
                    || listed.bodyLength() != leaf.bodyLength()
                    || leaf.directoryPrefixEnd() > root.nwg1AdmissionCaps().maxDirectoryPrefixBytes()
                    || leaf.bodyLength() > root.nwg1AdmissionCaps().maxCanonicalBodyBytes()
                    || !sequences.add(leaf.laneSequence())) {
                throw new IllegalStateException("strong LIST returned a substituted or duplicate Object WAL extent");
            }
            ObjectIdentity identity = new ObjectIdentity(listed.key(), listed.bodyLength(), leaf.objectSha256());
            candidates.add(new RecoveredExtentCandidate(leaf, identity, listed.immutableVersionToken()));
        }
        candidates.sort(Comparator.comparingLong(value -> value.leaf().laneSequence()));
        return new RecoveredLaneInventory(
                expectedLane, candidates, inventory.pageCount(), inventory.canonicalKeyBytes());
    }

    private ObjectWalLeafKeyV1 requireExactExtentIdentity(ObjectIdentity identity) {
        Objects.requireNonNull(identity, "identity");
        ObjectWalLeafKeyV1 leaf = ObjectWalLeafKeyV1.parseFull(root.providerConfiguration(), identity.key());
        if (identity.bodyLength() != leaf.bodyLength() || !identity.bodySha256().equals(leaf.objectSha256())) {
            throw new IllegalArgumentException("Object identity differs from its exact Object WAL leaf key");
        }
        return leaf;
    }

    private void requireRootBound() {
        if (root == null) {
            throw new IllegalStateException("operation requires the production Root-bound recovery constructor");
        }
    }

    private synchronized void chargeRetryIfRepeated(ObjectIdentity identity) {
        Objects.requireNonNull(identity, "identity");
        if (!reconciliationAttempts.add(identity)) {
            budget.chargeRetry();
        }
    }

    public record RecoveredExtentCandidate(
            ObjectWalLeafKeyV1 leaf, ObjectIdentity identity, Optional<CanonicalBytes> immutableVersionToken) {
        public RecoveredExtentCandidate {
            Objects.requireNonNull(leaf, "leaf");
            Objects.requireNonNull(identity, "identity");
            immutableVersionToken = immutableVersionToken.map(value -> CanonicalBytes.copyOf(value.toByteArray()));
        }
    }

    @FunctionalInterface
    public interface RecoveredExtentConsumer {
        void stage(RecoveredExtentCandidate candidate) throws IOException;
    }

    /**
     * Stack-scoped protocol staging callback invoked once per complete authenticated physical extent while the
     * durable protocol-owner fence is still held. Implementations must stage only and publish after the common
     * factory returns successfully.
     */
    @FunctionalInterface
    public interface RecoveredExtentStager {
        void stage(
                ProviderResolvedExtentRowV1 physicalRow,
                Nwg1ObjectReaderV1.AuthenticatedPrefix authenticatedPrefix,
                SelectedAppendUnitReader appendUnitReader)
                throws IOException;
    }

    /** One-use append-unit reader valid only during its enclosing recovered-extent staging callback. */
    @FunctionalInterface
    public interface SelectedAppendUnitReader {
        Nwg1ObjectReaderV1.VerifiedAppendUnit read(
                long selectedFrameOrdinal, Nwg1ObjectReaderV1.VerifiedFrameConsumer consumer) throws IOException;
    }

    public record RecoveredLaneFold(WalLaneId laneId, int pageCount, long candidateCount, long canonicalKeyBytes) {
        public RecoveredLaneFold {
            Objects.requireNonNull(laneId, "laneId");
            if (pageCount <= 0 || candidateCount < 0 || canonicalKeyBytes < 0) {
                throw new IllegalArgumentException("streamed recovered-lane counters are invalid");
            }
        }
    }

    public record RecoveredLaneInventory(
            WalLaneId laneId, List<RecoveredExtentCandidate> extents, int pageCount, long canonicalKeyBytes) {
        public RecoveredLaneInventory {
            Objects.requireNonNull(laneId, "laneId");
            extents = List.copyOf(extents);
            if (pageCount <= 0 || canonicalKeyBytes < 0) {
                throw new IllegalArgumentException("recovered lane inventory counters are invalid");
            }
        }
    }
}
