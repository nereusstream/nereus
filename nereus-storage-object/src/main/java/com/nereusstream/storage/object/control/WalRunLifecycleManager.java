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

package com.nereusstream.storage.object.control;

import com.nereusstream.domain.bytes.CanonicalBytes;
import com.nereusstream.domain.bytes.Sha256Digest;
import com.nereusstream.storage.object.recovery.CumulativeRecoveryBudget;
import com.nereusstream.storage.object.recovery.RecoveryEnvelopeExceededException;
import java.util.HashSet;
import java.util.Objects;
import java.util.Optional;
import java.util.Set;

/** Exact-response-loss reconciliation for immutable Root/Seal creation and successor-before-pointer publication. */
public final class WalRunLifecycleManager {
    private static final int MAX_BOOTSTRAP_CONTROL_BYTES = 1024 * 1024;

    public enum SealedPointerCompletionOutcome {
        POINTER_ON_UNSEALED_ROOT,
        NEED_EXACT_SUCCESSOR_CANDIDATE,
        ADVANCED_EXACT,
        ALREADY_ADVANCED_EXACT
    }

    private final CanonicalControlMetadataStore metadata;
    private final TerminalProtocolCheckpointVerifierV1 protocolCheckpointVerifier;
    private final TerminalProtocolCheckpointVerifierV1.ProtocolObjectRecoveryReaderFactoryV1
            protocolObjectReaderFactory;

    public WalRunLifecycleManager(CanonicalControlMetadataStore metadata) {
        this(
                metadata,
                TerminalProtocolCheckpointVerifierV1.failClosed(),
                TerminalProtocolCheckpointVerifierV1.ProtocolObjectRecoveryReaderFactoryV1.failClosed());
    }

    public WalRunLifecycleManager(
            CanonicalControlMetadataStore metadata, TerminalProtocolCheckpointVerifierV1 protocolCheckpointVerifier) {
        this(
                metadata,
                protocolCheckpointVerifier,
                TerminalProtocolCheckpointVerifierV1.ProtocolObjectRecoveryReaderFactoryV1.failClosed());
    }

    public WalRunLifecycleManager(
            CanonicalControlMetadataStore metadata,
            TerminalProtocolCheckpointVerifierV1 protocolCheckpointVerifier,
            TerminalProtocolCheckpointVerifierV1.ProtocolObjectRecoveryReaderFactoryV1 protocolObjectReaderFactory) {
        this.metadata = Objects.requireNonNull(metadata, "metadata");
        this.protocolCheckpointVerifier =
                Objects.requireNonNull(protocolCheckpointVerifier, "protocolCheckpointVerifier");
        this.protocolObjectReaderFactory =
                Objects.requireNonNull(protocolObjectReaderFactory, "protocolObjectReaderFactory");
    }

    public WalRunReference createRoot(String rootKey, WalRunRootRecord root) {
        root.requireM3ProductionProviderProofMode();
        WalRunControlKeys.requireRootKey(rootKey, root.shardId(), root.shardRunEpoch());
        CanonicalBytes value = WalRunControlCodec.encodeRoot(root);
        createImmutableExact(rootKey, value, "WalRun Root");
        return new WalRunReference(rootKey, WalRunControlCodec.rootSha256(root), root.shardId(), root.shardRunEpoch());
    }

    /**
     * Genesis is fresh-open only when this invocation definitively creates both the immutable Root and the empty
     * current-pointer CAS. Any exact adoption, response loss, or pointer conflict is recover-only, including an
     * immutable Root that this invocation just created but failed to make current.
     */
    public FreshRootPublication createRootAndInitializePointer(String rootKey, WalRunRootRecord root) {
        root.requireM3ProductionProviderProofMode();
        WalRunControlKeys.requireRootKey(rootKey, root.shardId(), root.shardRunEpoch());
        CanonicalBytes candidate = WalRunControlCodec.encodeRoot(root);
        ControlMutationOutcome rootOutcome = metadata.putIfAbsent(rootKey, candidate);
        WalRunReference reference =
                new WalRunReference(rootKey, WalRunControlCodec.rootSha256(root), root.shardId(), root.shardRunEpoch());
        if (rootOutcome != ControlMutationOutcome.APPLIED) {
            Optional<CanonicalBytes> observed = metadata.get(rootKey);
            if (observed.isEmpty() || !observed.orElseThrow().equals(candidate)) {
                throw new IllegalStateException("WalRun Root did not converge to exact candidate: " + rootOutcome);
            }
            return new FreshRootPublication(reference, Optional.empty());
        }
        String pointerKey = WalRunControlKeys.pointerKey(root.shardId());
        CanonicalBytes pointer = WalRunControlCodec.encodePointer(new CurrentWalRunPointer(reference));
        ControlMutationOutcome pointerOutcome = metadata.compareAndSet(pointerKey, Optional.empty(), pointer);
        if (pointerOutcome != ControlMutationOutcome.APPLIED) {
            return new FreshRootPublication(reference, Optional.empty());
        }
        return new FreshRootPublication(reference, Optional.of(new NewWalRunOwnerAuthority(reference, root)));
    }

    public record FreshRootPublication(WalRunReference reference, Optional<NewWalRunOwnerAuthority> ownerAuthority) {
        public FreshRootPublication {
            Objects.requireNonNull(reference, "reference");
            Objects.requireNonNull(ownerAuthority, "ownerAuthority");
        }
    }

    /** One-use lifecycle publication capability; raw Roots cannot open production Provider/KMS ownership. */
    public static final class NewWalRunOwnerAuthority {
        private final WalRunReference reference;
        private final WalRunRootRecord root;
        private boolean consumed;

        private NewWalRunOwnerAuthority(WalRunReference reference, WalRunRootRecord root) {
            this.reference = reference;
            this.root = root;
        }

        synchronized WalRunRootRecord requireConsumableRoot() {
            if (consumed) {
                throw new IllegalStateException("new WalRun owner authority was already consumed");
            }
            return root;
        }

        synchronized void consumeFor(WalRunRootRecord expectedRoot) {
            if (!requireConsumableRoot().equals(expectedRoot)
                    || !reference.rootSha256().equals(WalRunControlCodec.rootSha256(expectedRoot))
                    || !reference
                            .rootKey()
                            .equals(WalRunControlKeys.rootKey(expectedRoot.shardId(), expectedRoot.shardRunEpoch()))
                    || !root.walRunSessionId().equals(expectedRoot.walRunSessionId())
                    || !root.providerConfiguration()
                            .exclusiveNamespacePrefix()
                            .equals(expectedRoot.providerConfiguration().exclusiveNamespacePrefix())) {
                throw new IllegalArgumentException("new WalRun owner authority differs from the exact published Root");
            }
            consumed = true;
        }
    }

    WalRunTerminalClosureProofV1 publishSeal(String sealKey, WalRunSealRecord seal, WalRunRuntime sealedRuntime) {
        WalRunControlKeys.requireSealKey(
                sealKey, seal.root().shardId(), seal.root().shardRunEpoch());
        WalRunRootRecord root = verifySealRecord(seal);
        Objects.requireNonNull(sealedRuntime, "sealedRuntime");
        if (sealedRuntime.state() != WalRunRuntime.State.SEALED
                || !sealedRuntime.rootRecord().equals(root)
                || !sealedRuntime.resolvedVector().equals(seal.terminalSequence())
                || sealedRuntime.resolvedExtentCount() != seal.aggregateExtentCount()
                || sealedRuntime.resolvedCanonicalBodyBytes() != seal.aggregateCanonicalBodyBytes()) {
            throw new IllegalStateException(
                    "physical Seal publication requires the exact stopped/sealed runtime closure");
        }
        createImmutableExact(sealKey, WalRunControlCodec.encodeSeal(seal), "WalRun Seal");
        return new WalRunTerminalClosureProofV1(sealKey, seal);
    }

    public WalRunTerminalClosureProofV1 publishSeal(
            String sealKey, WalRunSealRecord seal, WalRunObjectSession sealedSession) {
        WalRunControlKeys.requireSealKey(
                sealKey, seal.root().shardId(), seal.root().shardRunEpoch());
        WalRunObjectSession session = Objects.requireNonNull(sealedSession, "sealedSession");
        session.requireTerminalClosable();
        WalRunRootRecord root = verifySealRecord(seal);
        session.requireExactSealedClosure(root, seal);
        createImmutableExact(sealKey, WalRunControlCodec.encodeSeal(seal), "WalRun Seal");
        return new WalRunTerminalClosureProofV1(sealKey, seal);
    }

    private WalRunRootRecord verifySealRecord(WalRunSealRecord seal) {
        return verifySealRecord(seal, loadRootForSeal(seal), null);
    }

    /**
     * Validates a Seal against an already snapshotted Root. Successor preflight must use this overload so every
     * retained Root is read and charged exactly once under the prospective Root's cumulative envelope.
     */
    private WalRunRootRecord verifySealRecord(
            WalRunSealRecord seal, WalRunRootRecord alreadyVerifiedRoot, CumulativeRecoveryBudget recoveryBudget) {
        WalRunRootRecord root = Objects.requireNonNull(alreadyVerifiedRoot, "alreadyVerifiedRoot");
        if (!WalRunControlCodec.rootSha256(root).equals(seal.root().rootSha256())
                || root.shardId() != seal.root().shardId()
                || root.shardRunEpoch() != seal.root().shardRunEpoch()) {
            throw new IllegalStateException("sealed WalRun Root differs from the already verified exact reference");
        }
        if (recoveryBudget != null) {
            recoveryBudget.chargeControlMetadata(MAX_BOOTSTRAP_CONTROL_BYTES);
        }
        CanonicalBytes checkpointHeadBytes = metadata.get(seal.finalCheckpointHeadKey())
                .orElseThrow(() -> new IllegalStateException("final checkpoint head is absent"));
        if (recoveryBudget != null && checkpointHeadBytes.length() > MAX_BOOTSTRAP_CONTROL_BYTES) {
            throw new IllegalStateException("final checkpoint head exceeds the successor recovery control cap");
        }
        if (!com.nereusstream.domain.bytes.Sha256Digest.hash(checkpointHeadBytes)
                .equals(seal.finalCheckpointHeadSha256())) {
            throw new IllegalStateException("final checkpoint head SHA-256 differs from the Seal");
        }
        WalCheckpointHeadV1 checkpointHead = WalRunControlCodec.decodeCheckpointHead(checkpointHeadBytes);
        if (!checkpointHead.rootSha256().equals(seal.root().rootSha256())
                || checkpointHead.shardRunEpoch() != seal.root().shardRunEpoch()
                || !checkpointHead.coveredThrough().equals(seal.terminalSequence())) {
            throw new IllegalStateException("final checkpoint head does not exactly cover the sealed Root/vector");
        }
        long aggregateExtentCount;
        long aggregateCanonicalBodyBytes;
        if (recoveryBudget == null) {
            WalCheckpointChainVerifier.Verification verified =
                    new WalCheckpointChainVerifier(metadata, root).verify(checkpointHead);
            aggregateExtentCount = verified.aggregateExtentCount();
            aggregateCanonicalBodyBytes = verified.aggregateCanonicalBodyBytes();
        } else {
            WalCheckpointChainVerifier.StreamingVerification verified =
                    new WalCheckpointChainVerifier(metadata, root).verifyStreaming(checkpointHead, recoveryBudget);
            aggregateExtentCount = verified.aggregateExtentCount();
            aggregateCanonicalBodyBytes = verified.aggregateCanonicalBodyBytes();
        }
        if (aggregateExtentCount != seal.aggregateExtentCount()
                || aggregateCanonicalBodyBytes != seal.aggregateCanonicalBodyBytes()) {
            throw new IllegalStateException("Seal aggregate facts differ from the complete checkpoint page chain");
        }
        return root;
    }

    private WalRunRootRecord loadRootForSeal(WalRunSealRecord seal) {
        CanonicalBytes rootBytes = metadata.get(seal.root().rootKey())
                .orElseThrow(() -> new IllegalStateException("sealed WalRun Root is absent"));
        if (!com.nereusstream.domain.bytes.Sha256Digest.hash(rootBytes)
                .equals(seal.root().rootSha256())) {
            throw new IllegalStateException("sealed WalRun Root SHA-256 differs from the exact reference");
        }
        WalRunRootRecord root = WalRunControlCodec.decodeRoot(rootBytes);
        if (root.shardId() != seal.root().shardId()
                || root.shardRunEpoch() != seal.root().shardRunEpoch()) {
            throw new IllegalStateException("sealed WalRun Root shard/epoch differs from the exact reference");
        }
        return root;
    }

    public void initializePointer(String pointerKey, CurrentWalRunPointer candidate) {
        WalRunControlKeys.requirePointerKey(pointerKey, candidate.current().shardId());
        loadExactRoot(candidate.current());
        CanonicalBytes candidateBytes = WalRunControlCodec.encodePointer(candidate);
        reconcileMutation(
                pointerKey,
                candidateBytes,
                metadata.compareAndSet(pointerKey, Optional.empty(), candidateBytes),
                "initial WalRun pointer");
    }

    /**
     * Publishes the immutable successor first, then exact-CASes the pointer. No call here is part of normal append.
     */
    public WalRunReference publishSuccessorAndAdvance(
            String pointerKey,
            CurrentWalRunPointer expectedPointer,
            String predecessorSealKey,
            WalRunSealRecord predecessorSeal,
            String successorRootKey,
            WalRunRootRecord successorRoot) {
        successorRoot.requireM3ProductionProviderProofMode();
        WalRunControlKeys.requirePointerKey(
                pointerKey, expectedPointer.current().shardId());
        ProspectiveSuccessorSnapshot snapshot =
                prepareProspectiveSuccessor(expectedPointer, predecessorSealKey, predecessorSeal, successorRoot);
        validatePreparedSuccessor(
                expectedPointer,
                predecessorSealKey,
                predecessorSeal,
                snapshot.predecessorRoot(),
                successorRoot,
                snapshot);
        WalRunReference successor = createOrAdoptExactRootUnderBudget(successorRootKey, successorRoot, snapshot);
        CanonicalBytes expectedBytes = WalRunControlCodec.encodePointer(expectedPointer);
        CanonicalBytes candidateBytes = WalRunControlCodec.encodePointer(new CurrentWalRunPointer(successor));
        ControlMutationOutcome outcome = metadata.compareAndSet(pointerKey, Optional.of(expectedBytes), candidateBytes);
        if (outcome == ControlMutationOutcome.APPLIED) {
            return successor;
        }
        CurrentWalRunPointer observed = readPointerUnderPreparedBudget(pointerKey, snapshot.budget());
        if (observed.current().equals(successor)) {
            return successor;
        }
        throw differentWinnerRequiresOwnerOpen(observed);
    }

    /** Fresh successor ownership exists only for this call's definitive Root creation and pointer-CAS win. */
    public SuccessorPublication publishSuccessorAndAdvanceWithOwnerAuthority(
            String pointerKey,
            CurrentWalRunPointer expectedPointer,
            String predecessorSealKey,
            WalRunSealRecord predecessorSeal,
            String successorRootKey,
            WalRunRootRecord successorRoot) {
        successorRoot.requireM3ProductionProviderProofMode();
        WalRunControlKeys.requirePointerKey(
                pointerKey, expectedPointer.current().shardId());
        ProspectiveSuccessorSnapshot snapshot =
                prepareProspectiveSuccessor(expectedPointer, predecessorSealKey, predecessorSeal, successorRoot);
        validatePreparedSuccessor(
                expectedPointer,
                predecessorSealKey,
                predecessorSeal,
                snapshot.predecessorRoot(),
                successorRoot,
                snapshot);
        CanonicalBytes rootBytes = WalRunControlCodec.encodeRoot(successorRoot);
        WalRunReference successor = new WalRunReference(
                successorRootKey,
                WalRunControlCodec.rootSha256(successorRoot),
                successorRoot.shardId(),
                successorRoot.shardRunEpoch());
        boolean definitivelyCreated =
                metadata.putIfAbsent(successorRootKey, rootBytes) == ControlMutationOutcome.APPLIED;
        if (!definitivelyCreated) {
            requireExistingExactRootUnderBudget(successorRootKey, rootBytes, snapshot);
        }
        CanonicalBytes expected = WalRunControlCodec.encodePointer(expectedPointer);
        CanonicalBytes candidate = WalRunControlCodec.encodePointer(new CurrentWalRunPointer(successor));
        if (metadata.compareAndSet(pointerKey, Optional.of(expected), candidate) == ControlMutationOutcome.APPLIED) {
            return new SuccessorPublication(
                    successor,
                    definitivelyCreated
                            ? Optional.of(new NewWalRunOwnerAuthority(successor, successorRoot))
                            : Optional.empty());
        }
        CurrentWalRunPointer observed = readPointerUnderPreparedBudget(pointerKey, snapshot.budget());
        if (!observed.current().equals(successor)) {
            throw differentWinnerRequiresOwnerOpen(observed);
        }
        return new SuccessorPublication(observed.current(), Optional.empty());
    }

    public record SuccessorPublication(WalRunReference reference, Optional<NewWalRunOwnerAuthority> ownerAuthority) {
        public SuccessorPublication {
            Objects.requireNonNull(reference, "reference");
            Objects.requireNonNull(ownerAuthority, "ownerAuthority");
        }
    }

    /**
     * Owner-open crash completion. A pointer that still names a sealed Root is never returned as admitting.
     */
    public SealedPointerCompletion completeSealedPointer(
            String pointerKey,
            WalRunReference expectedSealedRoot,
            Optional<SuccessorCandidate> exactSuccessorCandidate) {
        Objects.requireNonNull(expectedSealedRoot, "expectedSealedRoot");
        Objects.requireNonNull(exactSuccessorCandidate, "exactSuccessorCandidate");
        WalRunControlKeys.requirePointerKey(pointerKey, expectedSealedRoot.shardId());
        if (exactSuccessorCandidate.isPresent()) {
            return completeSealedPointerWithCandidate(
                    pointerKey, expectedSealedRoot, exactSuccessorCandidate.orElseThrow());
        }
        // Bootstrap the current pointer/Root under fixed caps, then charge them immediately to that Root's sole
        // cumulative envelope. Every later Seal/Head/page/retained-lineage read stays on this same budget.
        CanonicalBytes pointerBytes = requiredBootstrapControl(pointerKey, "current WalRun pointer");
        CurrentWalRunPointer observedPointer = WalRunControlCodec.decodePointer(pointerBytes);
        WalRunControlKeys.requirePointerKey(
                pointerKey, observedPointer.current().shardId());
        CanonicalBytes currentRootBytes =
                requiredBootstrapControl(observedPointer.current().rootKey(), "current WalRun Root");
        if (!Sha256Digest.hash(currentRootBytes)
                .equals(observedPointer.current().rootSha256())) {
            throw new IllegalStateException("current WalRun Root digest differs from the exact pointer reference");
        }
        WalRunRootRecord currentRoot = WalRunControlCodec.decodeRoot(currentRootBytes);
        if (currentRoot.shardId() != observedPointer.current().shardId()
                || currentRoot.shardRunEpoch() != observedPointer.current().shardRunEpoch()) {
            throw new IllegalStateException("current WalRun Root shard/epoch differs from the exact pointer reference");
        }
        CumulativeRecoveryBudget budget =
                new CumulativeRecoveryBudget(currentRoot.recoveryEnvelope(), System::nanoTime);
        budget.chargeControlMetadata(pointerBytes.length());
        budget.chargeRoot(false, currentRootBytes.length());
        WalRunRootRecord predecessorRoot = observedPointer.current().equals(expectedSealedRoot)
                ? currentRoot
                : loadExactRootUnderBudget(expectedSealedRoot, budget);
        String sealKey = WalRunControlKeys.sealKey(expectedSealedRoot.shardId(), expectedSealedRoot.shardRunEpoch());
        budget.chargeControlMetadata(MAX_BOOTSTRAP_CONTROL_BYTES);
        Optional<CanonicalBytes> sealBytes = metadata.get(sealKey);
        if (sealBytes.isEmpty()) {
            if (!observedPointer.current().equals(expectedSealedRoot)) {
                throw new IllegalStateException("pointer advanced from an expected Root that has no exact Seal");
            }
            return new SealedPointerCompletion(
                    SealedPointerCompletionOutcome.POINTER_ON_UNSEALED_ROOT, observedPointer.current());
        }
        if (sealBytes.orElseThrow().length() > MAX_BOOTSTRAP_CONTROL_BYTES) {
            throw new RecoveryEnvelopeExceededException("WalRun Seal canonical bytes");
        }
        WalRunSealRecord seal = WalRunControlCodec.decodeSeal(sealBytes.orElseThrow());
        if (!seal.root().equals(expectedSealedRoot)) {
            throw new IllegalStateException("derived Seal does not bind the expected pointer Root");
        }
        if (observedPointer.current().equals(expectedSealedRoot)) {
            verifySealRecord(seal, predecessorRoot, budget);
        } else {
            ProspectiveSuccessorSnapshot snapshot = new ProspectiveSuccessorSnapshot(budget, seal, predecessorRoot);
            validatePreparedSuccessor(
                    new CurrentWalRunPointer(expectedSealedRoot),
                    sealKey,
                    seal,
                    predecessorRoot,
                    currentRoot,
                    snapshot);
            return new SealedPointerCompletion(
                    SealedPointerCompletionOutcome.ALREADY_ADVANCED_EXACT, observedPointer.current());
        }
        return new SealedPointerCompletion(
                SealedPointerCompletionOutcome.NEED_EXACT_SUCCESSOR_CANDIDATE, expectedSealedRoot);
    }

    private SealedPointerCompletion completeSealedPointerWithCandidate(
            String pointerKey, WalRunReference expectedSealedRoot, SuccessorCandidate supplied) {
        CurrentWalRunPointer expectedPointer = new CurrentWalRunPointer(expectedSealedRoot);
        String sealKey = WalRunControlKeys.sealKey(expectedSealedRoot.shardId(), expectedSealedRoot.shardRunEpoch());
        ProspectiveSuccessorSnapshot snapshot =
                prepareProspectiveSuccessorFromStore(expectedPointer, sealKey, supplied.root());
        validatePreparedSuccessor(
                expectedPointer, sealKey, snapshot.exactSeal(), snapshot.predecessorRoot(), supplied.root(), snapshot);
        WalRunReference successor = createOrAdoptExactRootUnderBudget(supplied.rootKey(), supplied.root(), snapshot);
        CanonicalBytes expectedBytes = WalRunControlCodec.encodePointer(expectedPointer);
        CanonicalBytes candidateBytes = WalRunControlCodec.encodePointer(new CurrentWalRunPointer(successor));
        ControlMutationOutcome outcome = metadata.compareAndSet(pointerKey, Optional.of(expectedBytes), candidateBytes);
        if (outcome == ControlMutationOutcome.APPLIED) {
            return new SealedPointerCompletion(SealedPointerCompletionOutcome.ADVANCED_EXACT, successor);
        }
        CurrentWalRunPointer winner = readPointerUnderPreparedBudget(pointerKey, snapshot.budget());
        if (winner.current().equals(successor)) {
            return new SealedPointerCompletion(SealedPointerCompletionOutcome.ADVANCED_EXACT, successor);
        }
        throw differentWinnerRequiresOwnerOpen(winner);
    }

    public CurrentWalRunPointer readPointer(String pointerKey) {
        CanonicalBytes value = metadata.get(pointerKey)
                .orElseThrow(() -> new IllegalStateException("current WalRun pointer is absent"));
        CurrentWalRunPointer pointer = WalRunControlCodec.decodePointer(value);
        WalRunControlKeys.requirePointerKey(pointerKey, pointer.current().shardId());
        return pointer;
    }

    private void createImmutableExact(String key, CanonicalBytes value, String label) {
        reconcileMutation(key, value, metadata.putIfAbsent(key, value), label);
    }

    private void reconcileMutation(
            String key, CanonicalBytes exactCandidate, ControlMutationOutcome outcome, String label) {
        if (outcome == ControlMutationOutcome.APPLIED) {
            return;
        }
        Optional<CanonicalBytes> observed = metadata.get(key);
        if (observed.isPresent() && observed.orElseThrow().equals(exactCandidate)) {
            return;
        }
        throw new IllegalStateException(label + " did not converge to exact candidate: " + outcome);
    }

    /**
     * Captures exactly one prospective successor view. The budget is created from the candidate Root before any
     * metadata read; each later read consumes a precharged part of this same view and is never repeated downstream.
     */
    private ProspectiveSuccessorSnapshot prepareProspectiveSuccessor(
            CurrentWalRunPointer expectedPointer,
            String predecessorSealKey,
            WalRunSealRecord suppliedSeal,
            WalRunRootRecord successorRoot) {
        CumulativeRecoveryBudget budget =
                new CumulativeRecoveryBudget(successorRoot.recoveryEnvelope(), System::nanoTime);
        // Reserve the single pointer-control slot now. It is used only if a later CAS conflict requires one winner
        // read, so no extra metadata allowance is invented after the candidate Root has been accepted.
        budget.chargeControlMetadata(MAX_BOOTSTRAP_CONTROL_BYTES);
        budget.chargeRoot(false, WalRunControlCodec.encodeRoot(successorRoot).length());
        budget.chargeControlMetadata(MAX_BOOTSTRAP_CONTROL_BYTES);
        CanonicalBytes exactSealBytes = metadata.get(predecessorSealKey)
                .orElseThrow(() -> new IllegalStateException("successor predecessor Seal is absent"));
        if (exactSealBytes.length() > MAX_BOOTSTRAP_CONTROL_BYTES
                || !exactSealBytes.equals(WalRunControlCodec.encodeSeal(suppliedSeal))) {
            throw new IllegalStateException("successor predecessor Seal is absent or differs from the exact record");
        }
        WalRunSealRecord exactSeal = WalRunControlCodec.decodeSeal(exactSealBytes);
        if (!exactSeal.equals(suppliedSeal) || !exactSeal.root().equals(expectedPointer.current())) {
            throw new IllegalStateException("successor predecessor Seal differs from the exact pointer Root");
        }
        WalRunRootRecord predecessorRoot = loadExactRootUnderBudget(exactSeal.root(), budget);
        return new ProspectiveSuccessorSnapshot(budget, exactSeal, predecessorRoot);
    }

    private ProspectiveSuccessorSnapshot prepareProspectiveSuccessorFromStore(
            CurrentWalRunPointer expectedPointer, String predecessorSealKey, WalRunRootRecord successorRoot) {
        CumulativeRecoveryBudget budget =
                new CumulativeRecoveryBudget(successorRoot.recoveryEnvelope(), System::nanoTime);
        budget.chargeControlMetadata(MAX_BOOTSTRAP_CONTROL_BYTES);
        budget.chargeRoot(false, WalRunControlCodec.encodeRoot(successorRoot).length());
        budget.chargeControlMetadata(MAX_BOOTSTRAP_CONTROL_BYTES);
        CanonicalBytes exactSealBytes = metadata.get(predecessorSealKey)
                .orElseThrow(() -> new IllegalStateException("owner-open predecessor Seal is absent"));
        if (exactSealBytes.length() > MAX_BOOTSTRAP_CONTROL_BYTES) {
            throw new IllegalStateException("owner-open predecessor Seal exceeds the prospective recovery control cap");
        }
        WalRunSealRecord exactSeal = WalRunControlCodec.decodeSeal(exactSealBytes);
        if (!exactSeal.root().equals(expectedPointer.current())) {
            throw new IllegalStateException("owner-open predecessor Seal differs from the exact pointer Root");
        }
        return new ProspectiveSuccessorSnapshot(budget, exactSeal, loadExactRootUnderBudget(exactSeal.root(), budget));
    }

    private void validatePreparedSuccessor(
            CurrentWalRunPointer expectedPointer,
            String predecessorSealKey,
            WalRunSealRecord predecessorSeal,
            WalRunRootRecord predecessorRoot,
            WalRunRootRecord successorRoot,
            ProspectiveSuccessorSnapshot snapshot) {
        WalRunReference predecessor = expectedPointer.current();
        if (!predecessorSeal.root().equals(predecessor)) {
            throw new IllegalArgumentException("Seal does not bind the exact pointer predecessor");
        }
        if (successorRoot.predecessor().isEmpty()) {
            throw new IllegalArgumentException("successor Root does not bind the exact predecessor Root and Seal");
        }
        WalRunPredecessor actualLineage = successorRoot.predecessor().orElseThrow();
        if (!actualLineage.root().equals(predecessor)
                || !actualLineage.sealKey().equals(predecessorSealKey)
                || !actualLineage.sealSha256().equals(WalRunControlCodec.sealSha256(predecessorSeal))) {
            throw new IllegalArgumentException("successor Root does not bind the exact predecessor Root and Seal");
        }
        if (successorRoot.shardId() != predecessor.shardId()
                || successorRoot.shardRunEpoch() <= predecessor.shardRunEpoch()) {
            throw new IllegalArgumentException("successor Root must advance the same shard epoch");
        }
        if (!successorRoot.protocolCellIdentity().equals(predecessorRoot.protocolCellIdentity())
                || !successorRoot.providerScopeId().equals(predecessorRoot.providerScopeId())) {
            throw new IllegalArgumentException("successor Root substituted the Protocol Cell/provider scope");
        }
        if (successorRoot.walRunSessionId().equals(predecessorRoot.walRunSessionId())
                || successorRoot.openedAtMillis() < predecessorRoot.openedAtMillis()
                || successorRoot
                        .providerConfiguration()
                        .exclusiveNamespacePrefix()
                        .equals(predecessorRoot.providerConfiguration().exclusiveNamespacePrefix())
                || successorRoot.wrappedRunKey().equals(predecessorRoot.wrappedRunKey())) {
            throw new IllegalArgumentException(
                    "successor Root reused run session/prefix/key or regressed creation time");
        }
        if (!snapshot.predecessorRoot().equals(predecessorRoot)
                || !snapshot.exactSeal().equals(predecessorSeal)) {
            throw new IllegalStateException("successor validation received a substituted prospective snapshot");
        }
        verifyRetainedLineageClosure(successorRoot, predecessorRoot, predecessorSeal, actualLineage, snapshot.budget());
    }

    private void verifyTerminalProtocolCheckpoint(
            WalRunRootRecord budgetOwnerRoot,
            WalRunRootRecord predecessorRoot,
            WalRunSealRecord predecessorSeal,
            WalRunPredecessor successorLineage,
            CumulativeRecoveryBudget recoveryBudget) {
        if (successorLineage.terminalProtocolCheckpoint().isEmpty()) {
            if (predecessorRoot.protocolCellIdentity().protocolKind()
                    == com.nereusstream.domain.protocol.ProtocolKindV1.KAFKA) {
                throw new IllegalArgumentException("Kafka successor lineage requires the exact terminal protocol Head");
            }
            return;
        }
        TerminalProtocolCheckpointBindingV1 binding =
                successorLineage.terminalProtocolCheckpoint().orElseThrow();
        if (binding.protocolKind() != predecessorRoot.protocolCellIdentity().protocolKind()) {
            throw new IllegalArgumentException(
                    "terminal protocol Head kind differs from the predecessor Protocol Cell");
        }
        recoveryBudget.chargeControlMetadata(MAX_BOOTSTRAP_CONTROL_BYTES);
        CanonicalBytes exactHeadValue = metadata.get(binding.terminalHeadKey())
                .orElseThrow(() -> new IllegalStateException("terminal protocol checkpoint Head is absent"));
        if (exactHeadValue.length() > MAX_BOOTSTRAP_CONTROL_BYTES) {
            throw new IllegalStateException(
                    "terminal protocol checkpoint Head exceeds the successor recovery control cap");
        }
        if (!com.nereusstream.domain.bytes.Sha256Digest.hash(exactHeadValue)
                .equals(binding.terminalHeadValueSha256())) {
            throw new IllegalStateException("terminal protocol checkpoint Head SHA-256 differs from successor lineage");
        }
        protocolCheckpointVerifier.verifyTerminal(
                predecessorRoot,
                predecessorSeal,
                binding,
                exactHeadValue,
                new TerminalProtocolCheckpointVerifierV1.RecoveryContext(
                        budgetOwnerRoot, predecessorRoot, recoveryBudget, protocolObjectReaderFactory));
    }

    /**
     * Replays the exact retained predecessor chain under the prospective successor's one persisted envelope.  This
     * prevents a successor from making a direct tuple look valid while hiding an older Seal/Head/page chain that it
     * cannot recover after its pointer CAS wins.
     */
    private void verifyRetainedLineageClosure(
            WalRunRootRecord successorRoot,
            WalRunRootRecord directPredecessorRoot,
            WalRunSealRecord directPredecessorSeal,
            WalRunPredecessor directLineage,
            CumulativeRecoveryBudget recoveryBudget) {
        WalRunRootRecord retainedRoot = directPredecessorRoot;
        WalRunSealRecord retainedSeal = directPredecessorSeal;
        WalRunPredecessor retainedLineage = directLineage;
        Set<String> identities = new HashSet<>();
        int predecessorDepth = 1;
        while (true) {
            if (predecessorDepth > successorRoot.bounds().maxRecoverablePredecessorRuns()) {
                throw new RecoveryEnvelopeExceededException("successor Root maxRecoverablePredecessorRuns");
            }
            String identity = retainedLineage.root().rootKey() + "#"
                    + retainedLineage.root().rootSha256();
            if (!identities.add(identity)) {
                throw new IllegalStateException("successor retained lineage contains a Root cycle");
            }
            WalRunRootRecord verified = verifySealRecord(retainedSeal, retainedRoot, recoveryBudget);
            if (!verified.equals(retainedRoot)) {
                throw new IllegalStateException("retained Seal resolves a substituted predecessor Root");
            }
            if (!retainedRoot.protocolCellIdentity().equals(successorRoot.protocolCellIdentity())
                    || !retainedRoot.providerScopeId().equals(successorRoot.providerScopeId())) {
                throw new IllegalStateException(
                        "retained lineage substituted the successor Protocol Cell/provider scope");
            }
            verifyTerminalProtocolCheckpoint(
                    successorRoot, retainedRoot, retainedSeal, retainedLineage, recoveryBudget);
            if (retainedRoot.predecessor().isEmpty()) {
                return;
            }
            WalRunPredecessor olderLineage = retainedRoot.predecessor().orElseThrow();
            if (olderLineage.root().shardId() != successorRoot.shardId()
                    || olderLineage.root().shardRunEpoch() >= retainedRoot.shardRunEpoch()) {
                throw new IllegalStateException("retained lineage fork or non-decreasing predecessor epoch");
            }
            recoveryBudget.chargeControlMetadata(MAX_BOOTSTRAP_CONTROL_BYTES);
            CanonicalBytes olderSealBytes = metadata.get(olderLineage.sealKey())
                    .orElseThrow(() -> new IllegalStateException("retained predecessor Seal is absent"));
            if (olderSealBytes.length() > MAX_BOOTSTRAP_CONTROL_BYTES
                    || !Sha256Digest.hash(olderSealBytes).equals(olderLineage.sealSha256())) {
                throw new IllegalStateException("retained predecessor Seal cap or SHA-256 differs");
            }
            WalRunSealRecord olderSeal = WalRunControlCodec.decodeSeal(olderSealBytes);
            if (!olderSeal.root().equals(olderLineage.root())) {
                throw new IllegalStateException("retained predecessor Seal does not bind its exact Root");
            }
            retainedRoot = loadExactRootUnderBudget(olderLineage.root(), recoveryBudget);
            retainedSeal = olderSeal;
            retainedLineage = olderLineage;
            predecessorDepth = Math.incrementExact(predecessorDepth);
        }
    }

    private WalRunRootRecord loadExactRootUnderBudget(
            WalRunReference reference, CumulativeRecoveryBudget recoveryBudget) {
        recoveryBudget.chargeRoot(true, MAX_BOOTSTRAP_CONTROL_BYTES);
        CanonicalBytes bytes = metadata.get(reference.rootKey())
                .orElseThrow(() -> new IllegalStateException("retained WalRun Root is absent: " + reference.rootKey()));
        if (bytes.length() > MAX_BOOTSTRAP_CONTROL_BYTES
                || !Sha256Digest.hash(bytes).equals(reference.rootSha256())) {
            throw new IllegalStateException("retained WalRun Root cap or digest differs from its exact reference");
        }
        WalRunRootRecord root = WalRunControlCodec.decodeRoot(bytes);
        if (root.shardId() != reference.shardId() || root.shardRunEpoch() != reference.shardRunEpoch()) {
            throw new IllegalStateException("retained WalRun Root shard/epoch differs from its exact reference");
        }
        return root;
    }

    private WalRunRootRecord loadExactRoot(WalRunReference reference) {
        CanonicalBytes bytes = metadata.get(reference.rootKey())
                .orElseThrow(() -> new IllegalStateException("WalRun Root is absent: " + reference.rootKey()));
        if (!com.nereusstream.domain.bytes.Sha256Digest.hash(bytes).equals(reference.rootSha256())) {
            throw new IllegalStateException("WalRun Root digest differs from its exact reference");
        }
        WalRunRootRecord root = WalRunControlCodec.decodeRoot(bytes);
        if (root.shardId() != reference.shardId() || root.shardRunEpoch() != reference.shardRunEpoch()) {
            throw new IllegalStateException("WalRun Root shard/epoch differs from its exact reference");
        }
        return root;
    }

    private WalRunReference createOrAdoptExactRootUnderBudget(
            String rootKey, WalRunRootRecord root, ProspectiveSuccessorSnapshot snapshot) {
        WalRunControlKeys.requireRootKey(rootKey, root.shardId(), root.shardRunEpoch());
        CanonicalBytes candidate = WalRunControlCodec.encodeRoot(root);
        ControlMutationOutcome outcome = metadata.putIfAbsent(rootKey, candidate);
        if (outcome == ControlMutationOutcome.APPLIED) {
            return new WalRunReference(
                    rootKey, WalRunControlCodec.rootSha256(root), root.shardId(), root.shardRunEpoch());
        }
        // The candidate Root has already occupied the current-Root slot. Its one response-loss observation reuses
        // that exact Root slot; charging a second C here would contradict the persisted canonical closure formula.
        snapshot.claimCandidateRootObservation();
        CanonicalBytes observed = metadata.get(rootKey)
                .orElseThrow(() -> new IllegalStateException("successor Root response did not converge"));
        if (observed.length() > MAX_BOOTSTRAP_CONTROL_BYTES || !observed.equals(candidate)) {
            throw new IllegalStateException("successor Root did not converge to its exact immutable candidate");
        }
        return new WalRunReference(rootKey, WalRunControlCodec.rootSha256(root), root.shardId(), root.shardRunEpoch());
    }

    private void requireExistingExactRootUnderBudget(
            String rootKey, CanonicalBytes exactCandidate, ProspectiveSuccessorSnapshot snapshot) {
        snapshot.claimCandidateRootObservation();
        CanonicalBytes observed = metadata.get(rootKey)
                .orElseThrow(() -> new IllegalStateException("successor Root response did not converge"));
        if (observed.length() > MAX_BOOTSTRAP_CONTROL_BYTES || !observed.equals(exactCandidate)) {
            throw new IllegalStateException("successor Root did not converge to its exact immutable candidate");
        }
    }

    private CurrentWalRunPointer readPointerUnderPreparedBudget(String pointerKey, CumulativeRecoveryBudget budget) {
        CanonicalBytes value = metadata.get(pointerKey)
                .orElseThrow(() -> new IllegalStateException("current WalRun pointer is absent"));
        if (value.length() > MAX_BOOTSTRAP_CONTROL_BYTES) {
            throw new IllegalStateException("current WalRun pointer exceeds the prospective recovery control cap");
        }
        CurrentWalRunPointer pointer = WalRunControlCodec.decodePointer(value);
        WalRunControlKeys.requirePointerKey(pointerKey, pointer.current().shardId());
        return pointer;
    }

    private CanonicalBytes requiredBootstrapControl(String key, String label) {
        CanonicalBytes value = metadata.get(key).orElseThrow(() -> new IllegalStateException(label + " is absent"));
        if (value.length() > MAX_BOOTSTRAP_CONTROL_BYTES) {
            throw new RecoveryEnvelopeExceededException(label + " canonical bytes");
        }
        return value;
    }

    private static IllegalStateException differentWinnerRequiresOwnerOpen(CurrentWalRunPointer winner) {
        return new IllegalStateException(
                "a different successor won the pointer CAS; retry owner-open under the exact winner Root envelope: "
                        + winner.current().rootKey());
    }

    private static final class ProspectiveSuccessorSnapshot {
        private final CumulativeRecoveryBudget budget;
        private final WalRunSealRecord exactSeal;
        private final WalRunRootRecord predecessorRoot;
        private boolean candidateRootObserved;

        private ProspectiveSuccessorSnapshot(
                CumulativeRecoveryBudget budget, WalRunSealRecord exactSeal, WalRunRootRecord predecessorRoot) {
            this.budget = Objects.requireNonNull(budget, "budget");
            this.exactSeal = Objects.requireNonNull(exactSeal, "exactSeal");
            this.predecessorRoot = Objects.requireNonNull(predecessorRoot, "predecessorRoot");
        }

        private CumulativeRecoveryBudget budget() {
            return budget;
        }

        private WalRunSealRecord exactSeal() {
            return exactSeal;
        }

        private WalRunRootRecord predecessorRoot() {
            return predecessorRoot;
        }

        private void claimCandidateRootObservation() {
            if (candidateRootObserved) {
                throw new IllegalStateException("prospective successor Root observation was already consumed");
            }
            candidateRootObserved = true;
        }
    }

    public record SuccessorCandidate(String rootKey, WalRunRootRecord root) {
        public SuccessorCandidate {
            Objects.requireNonNull(rootKey, "rootKey");
            Objects.requireNonNull(root, "root");
            WalRunControlKeys.requireRootKey(rootKey, root.shardId(), root.shardRunEpoch());
        }
    }

    public record SealedPointerCompletion(SealedPointerCompletionOutcome outcome, WalRunReference current) {
        public SealedPointerCompletion {
            Objects.requireNonNull(outcome, "outcome");
            Objects.requireNonNull(current, "current");
        }
    }
}
