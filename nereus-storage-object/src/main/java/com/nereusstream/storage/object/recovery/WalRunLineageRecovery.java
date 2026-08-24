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
import com.nereusstream.domain.bytes.Sha256Digest;
import com.nereusstream.domain.protocol.ProtocolCellIdentity;
import com.nereusstream.storage.api.bookkeeper.CellProviderScopeId;
import com.nereusstream.storage.object.control.CanonicalControlMetadataStore;
import com.nereusstream.storage.object.control.CurrentWalRunPointer;
import com.nereusstream.storage.object.control.TerminalProtocolCheckpointBindingV1;
import com.nereusstream.storage.object.control.TerminalProtocolCheckpointVerifierV1;
import com.nereusstream.storage.object.control.WalCheckpointChainVerifier;
import com.nereusstream.storage.object.control.WalCheckpointHeadV1;
import com.nereusstream.storage.object.control.WalRunControlCodec;
import com.nereusstream.storage.object.control.WalRunControlKeys;
import com.nereusstream.storage.object.control.WalRunPredecessor;
import com.nereusstream.storage.object.control.WalRunReference;
import com.nereusstream.storage.object.control.WalRunRootRecord;
import com.nereusstream.storage.object.control.WalRunSealRecord;
import com.nereusstream.storage.object.kms.RunKeyCacheIdentity;
import java.util.ArrayList;
import java.util.HashSet;
import java.util.List;
import java.util.Objects;
import java.util.Optional;
import java.util.Set;
import java.util.function.LongSupplier;

/** Fail-closed exact Root/Seal lineage walk from the current pointer to a retirement frontier or genesis. */
public final class WalRunLineageRecovery {
    /** Control values are deliberately bootstrap-capped before the current Root reveals its persisted envelope. */
    private static final int MAX_BOOTSTRAP_CONTROL_BYTES = 1024 * 1024;

    private final CanonicalControlMetadataStore metadata;
    private final TerminalProtocolCheckpointVerifierV1 protocolCheckpointVerifier;
    private final TerminalProtocolCheckpointVerifierV1.ProtocolObjectRecoveryReaderFactoryV1
            protocolObjectReaderFactory;

    public WalRunLineageRecovery(CanonicalControlMetadataStore metadata) {
        this(
                metadata,
                TerminalProtocolCheckpointVerifierV1.failClosed(),
                TerminalProtocolCheckpointVerifierV1.ProtocolObjectRecoveryReaderFactoryV1.failClosed());
    }

    public WalRunLineageRecovery(
            CanonicalControlMetadataStore metadata, TerminalProtocolCheckpointVerifierV1 protocolCheckpointVerifier) {
        this(
                metadata,
                protocolCheckpointVerifier,
                TerminalProtocolCheckpointVerifierV1.ProtocolObjectRecoveryReaderFactoryV1.failClosed());
    }

    public WalRunLineageRecovery(
            CanonicalControlMetadataStore metadata,
            TerminalProtocolCheckpointVerifierV1 protocolCheckpointVerifier,
            TerminalProtocolCheckpointVerifierV1.ProtocolObjectRecoveryReaderFactoryV1 protocolObjectReaderFactory) {
        this.metadata = Objects.requireNonNull(metadata, "metadata");
        this.protocolCheckpointVerifier =
                Objects.requireNonNull(protocolCheckpointVerifier, "protocolCheckpointVerifier");
        this.protocolObjectReaderFactory =
                Objects.requireNonNull(protocolObjectReaderFactory, "protocolObjectReaderFactory");
    }

    /**
     * Recovers under the current Root's persisted envelope only.  The caller can provide a monotonic clock for
     * deterministic tests but cannot replace, reset, or widen any Root-owned counter.
     */
    public RecoveredLineage recover(
            String pointerKey,
            ProtocolCellIdentity expectedProtocolCell,
            CellProviderScopeId expectedProviderScope,
            Optional<WalRunReference> retirementFrontier,
            LongSupplier nanoTime) {
        Objects.requireNonNull(pointerKey, "pointerKey");
        Objects.requireNonNull(expectedProtocolCell, "expectedProtocolCell");
        Objects.requireNonNull(expectedProviderScope, "expectedProviderScope");
        Objects.requireNonNull(retirementFrontier, "retirementFrontier");
        Objects.requireNonNull(nanoTime, "nanoTime");

        // There is no Root before the pointer is decoded.  Bootstrap values have a fixed hard cap, then are
        // immediately charged to the verified current Root before any predecessor/control traversal.
        CanonicalBytes pointerBytes = requiredBootstrap(pointerKey, "CurrentWalRunPointer");
        CurrentWalRunPointer pointer = WalRunControlCodec.decodePointer(pointerBytes);
        WalRunControlKeys.requirePointerKey(pointerKey, pointer.current().shardId());
        CanonicalBytes currentRootBytes = requiredBootstrap(pointer.current().rootKey(), "current WalRun Root");
        if (!Sha256Digest.hash(currentRootBytes).equals(pointer.current().rootSha256())) {
            throw new IllegalStateException("WalRun Root digest differs from its exact reference");
        }
        WalRunRootRecord currentRoot = WalRunControlCodec.decodeRoot(currentRootBytes);
        requireReferenceMatchesRoot(pointer.current(), currentRoot);
        CumulativeRecoveryBudget budget = new CumulativeRecoveryBudget(currentRoot.recoveryEnvelope(), nanoTime);
        budget.chargeControlMetadata(pointerBytes.length());
        budget.chargeRoot(false, currentRootBytes.length());

        ArrayList<RecoveredRun> lineage = new ArrayList<>();
        Set<String> identities = new HashSet<>();
        WalRunReference current = pointer.current();
        WalRunRootRecord root = currentRoot;
        int predecessorDepth = 0;
        int rootBound = -1;
        WalRunPredecessor successorLineage = null;
        WalRunSealRecord successorPredecessorSeal = null;
        while (true) {
            String cycleIdentity = current.rootKey() + "#" + current.rootSha256();
            if (!identities.add(cycleIdentity)) {
                throw new IllegalStateException("WalRun lineage cycle detected");
            }
            requireReferenceMatchesRoot(current, root);
            if (!root.protocolCellIdentity().equals(expectedProtocolCell)
                    || !root.providerScopeId().equals(expectedProviderScope)) {
                throw new IllegalStateException("WalRun lineage substituted the expected Protocol Cell/provider scope");
            }
            if (rootBound < 0) {
                rootBound = root.bounds().maxRecoverablePredecessorRuns();
            }
            if (predecessorDepth > rootBound) {
                throw new RecoveryEnvelopeExceededException("Root maxRecoverablePredecessorRuns");
            }
            if (successorLineage != null) {
                verifyExactSealedPhysicalClosure(root, successorPredecessorSeal, budget);
                verifyTerminalProtocolCheckpoint(currentRoot, root, successorPredecessorSeal, successorLineage, budget);
            }
            lineage.add(new RecoveredRun(current, root, Optional.empty()));
            if (retirementFrontier.isPresent() && current.equals(retirementFrontier.orElseThrow())) {
                return new RecoveredLineage(currentRoot, rootSha256(currentRoot), lineage, budget);
            }
            if (root.predecessor().isEmpty()) {
                if (retirementFrontier.isPresent()) {
                    throw new IllegalStateException("WalRun lineage cannot reach the retirement frontier");
                }
                return new RecoveredLineage(currentRoot, rootSha256(currentRoot), lineage, budget);
            }
            if (predecessorDepth >= rootBound) {
                throw new RecoveryEnvelopeExceededException("Root maxRecoverablePredecessorRuns");
            }
            WalRunPredecessor predecessor = root.predecessor().orElseThrow();
            if (predecessor.root().shardId() != pointer.current().shardId()
                    || predecessor.root().shardRunEpoch() >= current.shardRunEpoch()) {
                throw new IllegalStateException("WalRun lineage fork or non-decreasing predecessor epoch");
            }
            budget.chargeControlMetadata(MAX_BOOTSTRAP_CONTROL_BYTES);
            CanonicalBytes sealBytes = requiredCappedControl(predecessor.sealKey(), "WalRun Seal");
            if (!Sha256Digest.hash(sealBytes).equals(predecessor.sealSha256())) {
                throw new IllegalStateException("WalRun Seal digest differs from successor lineage reference");
            }
            WalRunSealRecord seal = WalRunControlCodec.decodeSeal(sealBytes);
            if (!seal.root().equals(predecessor.root())) {
                throw new IllegalStateException("WalRun Seal does not bind the exact predecessor Root");
            }
            int currentIndex = lineage.size() - 1;
            RecoveredRun successor = lineage.get(currentIndex);
            lineage.set(currentIndex, new RecoveredRun(successor.reference(), successor.root(), Optional.of(seal)));
            successorLineage = predecessor;
            successorPredecessorSeal = seal;
            current = predecessor.root();
            budget.chargeRoot(true, MAX_BOOTSTRAP_CONTROL_BYTES);
            CanonicalBytes predecessorRootBytes = requiredCappedControl(current.rootKey(), "WalRun Root");
            if (!Sha256Digest.hash(predecessorRootBytes).equals(current.rootSha256())) {
                throw new IllegalStateException("WalRun Root digest differs from its exact reference");
            }
            root = WalRunControlCodec.decodeRoot(predecessorRootBytes);
            predecessorDepth = Math.incrementExact(predecessorDepth);
        }
    }

    private CanonicalBytes requiredBootstrap(String key, String label) {
        CanonicalBytes value = requiredCappedControl(key, label);
        return value;
    }

    private CanonicalBytes requiredCappedControl(String key, String label) {
        CanonicalBytes value = requiredCharged(key, label);
        if (value.length() > MAX_BOOTSTRAP_CONTROL_BYTES) {
            throw new RecoveryEnvelopeExceededException(label + " canonical bytes");
        }
        return value;
    }

    private CanonicalBytes requiredCharged(String key, String label) {
        return metadata.get(key).orElseThrow(() -> new IllegalStateException(label + " is absent: " + key));
    }

    private static void requireReferenceMatchesRoot(WalRunReference reference, WalRunRootRecord root) {
        if (root.shardId() != reference.shardId() || root.shardRunEpoch() != reference.shardRunEpoch()) {
            throw new IllegalStateException("WalRun Root shard/epoch differs from its exact reference");
        }
    }

    private void verifyExactSealedPhysicalClosure(
            WalRunRootRecord predecessorRoot, WalRunSealRecord predecessorSeal, CumulativeRecoveryBudget budget) {
        budget.chargeControlMetadata(MAX_BOOTSTRAP_CONTROL_BYTES);
        CanonicalBytes exactHeadValue =
                requiredCappedControl(predecessorSeal.finalCheckpointHeadKey(), "final physical checkpoint Head");
        if (!Sha256Digest.hash(exactHeadValue).equals(predecessorSeal.finalCheckpointHeadSha256())) {
            throw new IllegalStateException("final physical checkpoint Head digest differs from predecessor Seal");
        }
        WalCheckpointHeadV1 head = WalRunControlCodec.decodeCheckpointHead(exactHeadValue);
        if (!head.rootSha256().equals(WalRunControlCodec.rootSha256(predecessorRoot))
                || head.shardRunEpoch() != predecessorRoot.shardRunEpoch()
                || !head.coveredThrough().equals(predecessorSeal.terminalSequence())) {
            throw new IllegalStateException("final physical checkpoint Head differs from exact predecessor Seal/Root");
        }
        WalCheckpointChainVerifier.StreamingVerification verified =
                new WalCheckpointChainVerifier(metadata, predecessorRoot).verifyStreaming(head, budget);
        if (verified.aggregateExtentCount() != predecessorSeal.aggregateExtentCount()
                || verified.aggregateCanonicalBodyBytes() != predecessorSeal.aggregateCanonicalBodyBytes()
                || !verified.coveredThrough().equals(predecessorSeal.terminalSequence())) {
            throw new IllegalStateException(
                    "predecessor Seal aggregate differs from complete physical checkpoint chain");
        }
    }

    private void verifyTerminalProtocolCheckpoint(
            WalRunRootRecord budgetOwnerRoot,
            WalRunRootRecord predecessorRoot,
            WalRunSealRecord predecessorSeal,
            WalRunPredecessor successorLineage,
            CumulativeRecoveryBudget budget) {
        if (successorLineage.terminalProtocolCheckpoint().isEmpty()) {
            return;
        }
        TerminalProtocolCheckpointBindingV1 binding =
                successorLineage.terminalProtocolCheckpoint().orElseThrow();
        if (binding.protocolKind() != predecessorRoot.protocolCellIdentity().protocolKind()) {
            throw new IllegalStateException(
                    "WalRun lineage terminal protocol Head kind differs from its Protocol Cell");
        }
        budget.chargeControlMetadata(MAX_BOOTSTRAP_CONTROL_BYTES);
        CanonicalBytes exactHeadValue =
                requiredCappedControl(binding.terminalHeadKey(), "terminal protocol checkpoint Head");
        if (!Sha256Digest.hash(exactHeadValue).equals(binding.terminalHeadValueSha256())) {
            throw new IllegalStateException("terminal protocol checkpoint Head digest differs from successor lineage");
        }
        protocolCheckpointVerifier.verifyTerminal(
                predecessorRoot,
                predecessorSeal,
                binding,
                exactHeadValue,
                new TerminalProtocolCheckpointVerifierV1.RecoveryContext(
                        budgetOwnerRoot, predecessorRoot, budget, protocolObjectReaderFactory));
    }

    /** Seal is the exact predecessor Seal named by this run's successor Root, when a predecessor exists. */
    public record RecoveredRun(
            WalRunReference reference, WalRunRootRecord root, Optional<WalRunSealRecord> predecessorSeal) {
        public RecoveredRun {
            Objects.requireNonNull(reference, "reference");
            Objects.requireNonNull(root, "root");
            Objects.requireNonNull(predecessorSeal, "predecessorSeal");
        }
    }

    /**
     * One-use authority that carries the only live current-Root budget from lineage into owner-open tail recovery.
     * It cannot be constructed outside this verifier and never exposes the budget outside the recovery package.
     */
    public static final class RecoveredLineage {
        private final WalRunRootRecord currentRoot;
        private final Sha256Digest currentRootSha256;
        private final List<RecoveredRun> runs;
        private CumulativeRecoveryBudget liveBudget;

        private RecoveredLineage(
                WalRunRootRecord currentRoot,
                Sha256Digest currentRootSha256,
                List<RecoveredRun> runs,
                CumulativeRecoveryBudget liveBudget) {
            this.currentRoot = Objects.requireNonNull(currentRoot, "currentRoot");
            this.currentRootSha256 = Objects.requireNonNull(currentRootSha256, "currentRootSha256");
            this.runs = List.copyOf(runs);
            this.liveBudget = Objects.requireNonNull(liveBudget, "liveBudget");
        }

        public List<RecoveredRun> runs() {
            return runs;
        }

        /**
         * Side-effect-free readiness check for a session factory. It lets the factory validate its runtime,
         * Provider, KMS and envelope authorities before the final one-use budget transfer.
         */
        public synchronized void requireConsumableFor(WalRunRootRecord root) {
            requireExactCurrentRoot(root);
            if (liveBudget == null) {
                throw new IllegalStateException("lineage recovery authority was already consumed");
            }
        }

        synchronized CumulativeRecoveryBudget consumeFor(WalRunRootRecord root) {
            requireConsumableFor(root);
            CumulativeRecoveryBudget consumed = liveBudget;
            liveBudget = null;
            return consumed;
        }

        /**
         * Package-private borrow for the one common owner-open cut builder; it cannot replace or consume the budget.
         */
        synchronized CumulativeRecoveryBudget borrowForRuntimeCut(WalRunRootRecord root) {
            requireConsumableFor(root);
            return liveBudget;
        }

        /** Mints the two recovery-transfer authorities once; neither raw Cell session can be reopened afterward. */
        public synchronized OwnerOpenRecoveryAuthorities prepareOwnerOpenTransfers(WalRunRootRecord root) {
            requireConsumableFor(root);
            return new OwnerOpenRecoveryAuthorities(
                    this,
                    new ProviderRecoveryAuthority(currentRoot, currentRootSha256),
                    new KmsRecoveryAuthority(currentRoot, currentRootSha256));
        }

        private boolean transferAuthoritiesPrepared;

        private void requireExactCurrentRoot(WalRunRootRecord root) {
            Objects.requireNonNull(root, "root");
            if (!currentRoot.equals(root) || !currentRootSha256.equals(rootSha256(root))) {
                throw new IllegalArgumentException("lineage recovery authority differs from the current Root");
            }
        }
    }

    /** Opaque pair passed only to raw Provider/KMS recovery-transfer APIs. */
    public static final class OwnerOpenRecoveryAuthorities {
        private final ProviderRecoveryAuthority providerAuthority;
        private final KmsRecoveryAuthority kmsAuthority;
        private final RecoveredLineage lineage;
        private boolean claimed;

        private OwnerOpenRecoveryAuthorities(
                RecoveredLineage lineage,
                ProviderRecoveryAuthority providerAuthority,
                KmsRecoveryAuthority kmsAuthority) {
            this.lineage = lineage;
            Objects.requireNonNull(providerAuthority, "providerAuthority");
            Objects.requireNonNull(kmsAuthority, "kmsAuthority");
            this.providerAuthority = providerAuthority;
            this.kmsAuthority = kmsAuthority;
        }

        public ProviderRecoveryAuthority providerAuthority() {
            return providerAuthority;
        }

        public KmsRecoveryAuthority kmsAuthority() {
            return kmsAuthority;
        }

        /** Called only after raw Provider and KMS have both completed side-effect-free readiness checks. */
        synchronized ClaimedOwnerOpenRecoveryAuthorities claimForTransfers() {
            synchronized (lineage) {
                if (claimed || lineage.transferAuthoritiesPrepared) {
                    throw new IllegalStateException("owner-open recovery transfer pair was already claimed");
                }
                lineage.transferAuthoritiesPrepared = true;
            }
            providerAuthority.requireLive();
            kmsAuthority.requireLive();
            claimed = true;
            return new ClaimedOwnerOpenRecoveryAuthorities(
                    new ProviderRecoveryTransfer(providerAuthority), new KmsRecoveryTransfer(kmsAuthority));
        }
    }

    /** Aggregate claim whose two sub-tokens retain exact snapshots until their corresponding lease mint. */
    public record ClaimedOwnerOpenRecoveryAuthorities(
            ProviderRecoveryTransfer providerTransfer, KmsRecoveryTransfer kmsTransfer) {}

    public static final class ProviderRecoveryTransfer {
        private final ProviderRecoveryAuthority authority;
        private boolean consumed;

        private ProviderRecoveryTransfer(ProviderRecoveryAuthority authority) {
            this.authority = authority;
        }

        public com.nereusstream.storage.api.bookkeeper.CellProviderScopeId providerScopeId() {
            return authority.providerScopeId();
        }

        public String exclusiveNamespacePrefix() {
            return authority.exclusiveNamespacePrefix();
        }

        public long admittedMaximumObjectBytes() {
            return authority.admittedMaximumObjectBytes();
        }

        public int admittedMaximumPrefixBytes() {
            return authority.admittedMaximumPrefixBytes();
        }

        public Sha256Digest rootSha256() {
            return authority.rootSha256();
        }

        public synchronized void consumeForLease() {
            if (consumed) {
                throw new IllegalStateException("Provider recovery transfer was consumed");
            }
            authority.consume();
            consumed = true;
        }
    }

    public static final class KmsRecoveryTransfer {
        private final KmsRecoveryAuthority authority;
        private boolean consumed;

        private KmsRecoveryTransfer(KmsRecoveryAuthority authority) {
            this.authority = authority;
        }

        public RunKeyCacheIdentity runKeyIdentity() {
            return authority.runKeyIdentity();
        }

        public com.nereusstream.storage.object.kms.WrappedRunKeyEnvelope wrappedRunKey() {
            return authority.wrappedRunKey();
        }

        public Sha256Digest rootSha256() {
            return authority.rootSha256();
        }

        public com.nereusstream.storage.api.bookkeeper.CellProviderScopeId providerScopeId() {
            return authority.providerScopeId();
        }

        public synchronized void consumeForLease() {
            if (consumed) {
                throw new IllegalStateException("KMS recovery transfer was consumed");
            }
            authority.consume();
            consumed = true;
        }
    }

    /** Exact current-Root capability consumed once by C1 transferToRecovery. */
    public static final class ProviderRecoveryAuthority {
        private final WalRunRootRecord root;
        private final Sha256Digest rootSha256;
        private boolean consumed;

        private ProviderRecoveryAuthority(WalRunRootRecord root, Sha256Digest rootSha256) {
            this.root = root;
            this.rootSha256 = rootSha256;
        }

        public synchronized com.nereusstream.storage.api.bookkeeper.CellProviderScopeId providerScopeId() {
            requireLive();
            return root.providerScopeId();
        }

        public synchronized String exclusiveNamespacePrefix() {
            requireLive();
            return root.providerConfiguration().exclusiveNamespacePrefix();
        }

        public synchronized long admittedMaximumObjectBytes() {
            requireLive();
            return root.providerConfiguration().maxObjectBodyBytes();
        }

        public synchronized int admittedMaximumPrefixBytes() {
            requireLive();
            return root.nwg1AdmissionCaps().maxDirectoryPrefixBytes();
        }

        public synchronized Sha256Digest rootSha256() {
            requireLive();
            return rootSha256;
        }

        public synchronized void consume() {
            requireLive();
            consumed = true;
        }

        private void requireLive() {
            if (consumed) {
                throw new IllegalStateException("Provider recovery authority was consumed");
            }
        }
    }

    /** Exact current-Root capability consumed once by KMS transferToRecovery. */
    public static final class KmsRecoveryAuthority {
        private final WalRunRootRecord root;
        private final Sha256Digest rootSha256;
        private boolean consumed;

        private KmsRecoveryAuthority(WalRunRootRecord root, Sha256Digest rootSha256) {
            this.root = root;
            this.rootSha256 = rootSha256;
        }

        public synchronized RunKeyCacheIdentity runKeyIdentity() {
            requireLive();
            return new RunKeyCacheIdentity(root.shardId(), root.shardRunEpoch());
        }

        public synchronized com.nereusstream.storage.object.kms.WrappedRunKeyEnvelope wrappedRunKey() {
            requireLive();
            return root.wrappedRunKey();
        }

        public synchronized Sha256Digest rootSha256() {
            requireLive();
            return rootSha256;
        }

        public synchronized com.nereusstream.storage.api.bookkeeper.CellProviderScopeId providerScopeId() {
            requireLive();
            return root.providerScopeId();
        }

        public synchronized void consume() {
            requireLive();
            consumed = true;
        }

        private void requireLive() {
            if (consumed) {
                throw new IllegalStateException("KMS recovery authority was consumed");
            }
        }
    }

    private static Sha256Digest rootSha256(WalRunRootRecord root) {
        return WalRunControlCodec.rootSha256(root);
    }
}
