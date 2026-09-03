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

package com.nereusstream.storage.object.read.control;

import com.nereusstream.domain.bytes.CanonicalBytes;
import com.nereusstream.domain.bytes.Sha256Digest;
import com.nereusstream.storage.object.control.CanonicalControlMetadataStore;
import com.nereusstream.storage.object.control.ControlMutationOutcome;
import com.nereusstream.storage.object.read.BindingReadHazardPoolV1;
import com.nereusstream.storage.object.read.control.M4ReadControlRecordsV1.AdmissionState;
import com.nereusstream.storage.object.read.control.M4ReadControlRecordsV1.BindingIdentity;
import com.nereusstream.storage.object.read.control.M4ReadControlRecordsV1.BindingReadSelector;
import com.nereusstream.storage.object.read.control.M4ReadControlRecordsV1.CapabilityBinding;
import com.nereusstream.storage.object.read.control.M4ReadControlRecordsV1.CapabilityEvidence;
import com.nereusstream.storage.object.read.control.M4ReadControlRecordsV1.CapabilityKind;
import com.nereusstream.storage.object.read.control.M4ReadControlRecordsV1.CapabilityState;
import com.nereusstream.storage.object.read.control.M4ReadControlRecordsV1.ClosureAnchor;
import com.nereusstream.storage.object.read.control.M4ReadControlRecordsV1.ProofEntry;
import com.nereusstream.storage.object.read.control.M4ReadControlRecordsV1.ProofFold;
import com.nereusstream.storage.object.read.control.M4ReadControlRecordsV1.ProtectionState;
import com.nereusstream.storage.object.read.control.M4ReadControlRecordsV1.QuiescenceProofHead;
import com.nereusstream.storage.object.read.control.M4ReadControlRecordsV1.ReadAdmissionEpochTerminalCut;
import com.nereusstream.storage.object.read.control.M4ReadControlRecordsV1.ReadQuiescenceProof;
import com.nereusstream.storage.object.read.control.M4ReadControlRecordsV1.SelectorMode;
import com.nereusstream.storage.object.read.control.M4ReadControlRecordsV1.SourceProtection;
import com.nereusstream.storage.object.read.control.M4ReadControlRecordsV1.SourceProtectionIdentity;
import com.nereusstream.storage.object.read.control.M4ReadControlRecordsV1.SourceRetirementBatch;
import com.nereusstream.storage.object.read.control.M4ReadControlRecordsV1.TerminalKind;
import com.nereusstream.storage.object.retention.M5BindingAuthorityControlMetadataStoreV1;
import java.io.ByteArrayOutputStream;
import java.io.DataOutputStream;
import java.io.IOException;
import java.util.ArrayList;
import java.util.Comparator;
import java.util.LinkedHashMap;
import java.util.List;
import java.util.Map;
import java.util.Objects;
import java.util.Optional;

/** Low-frequency M4 selector, proof, fold, and exact source-protection release authority. */
public final class M4ReadControlCoordinatorV1 {
    public enum Outcome {
        APPLIED,
        EXISTING_EXACT,
        ADOPTED_DIFFERENT_VALID_TERMINAL,
        RETRY_EXACT_PREDECESSOR,
        STOPPED,
        RETAIN,
        CONFLICT,
        QUARANTINED_INVALID_OCCUPANT
    }

    private final CanonicalControlMetadataStore metadata;
    private final BindingIdentity binding;
    private final M4ReadControlKeysV1 keys;

    public M4ReadControlCoordinatorV1(CanonicalControlMetadataStore metadata, int shardId, BindingIdentity binding) {
        this.binding = Objects.requireNonNull(binding, "binding");
        keys = new M4ReadControlKeysV1(shardId, binding);
        this.metadata = new M5BindingAuthorityControlMetadataStoreV1(
                Objects.requireNonNull(metadata, "metadata"), keys.selector());
    }

    public Outcome createCapability(CapabilityEvidence capability) {
        requireBinding(capability.binding());
        if (capability.state() != CapabilityState.ADMITTED) {
            throw new IllegalArgumentException("new capability evidence must be admitted");
        }
        CanonicalBytes candidate = M4ReadControlCodecV1.encodeCapability(capability);
        return reconcileCreate(keys.capability(capability.generation()), candidate, ExistingKind.EXACT_ONLY);
    }

    public Outcome revokeCapability(long generation) {
        String key = keys.capability(generation);
        Optional<CanonicalBytes> currentBytes = metadata.get(key);
        if (currentBytes.isEmpty()) {
            return Outcome.RETAIN;
        }
        CapabilityEvidence current = M4ReadControlCodecV1.decodeCapability(currentBytes.orElseThrow());
        requireBinding(current.binding());
        if (current.state() == CapabilityState.REVOKED) {
            return Outcome.EXISTING_EXACT;
        }
        CanonicalBytes candidate = M4ReadControlCodecV1.encodeCapability(current.revoke());
        return reconcileCas(key, currentBytes.orElseThrow(), candidate);
    }

    public Outcome createSelector(BindingReadSelector selector) {
        validateSelector(selector);
        CanonicalBytes candidate = M4ReadControlCodecV1.encodeSelector(selector);
        requireAdmissionCapacity(selector, candidate);
        return reconcileCreate(keys.selector(), candidate, ExistingKind.EXACT_ONLY);
    }

    public Optional<BindingReadSelector> readSelector() {
        return metadata.get(keys.selector())
                .map(M4ReadControlCodecV1::decodeSelector)
                .map(value -> {
                    validateSelector(value);
                    return value;
                });
    }

    /**
     * Grants a new owner through the same selector CAS that competes with fallback closure. A
     * fallback-bearing predecessor closes its epoch and persists an inline closure anchor; a
     * preferred-only predecessor creates no proof liability.
     */
    public Outcome grantTakeover(
            BindingReadSelector expected,
            Sha256Digest successorViewSha256,
            long successorOwnerEpoch,
            long successorSourceGeneration) {
        validateSelector(expected);
        Objects.requireNonNull(successorViewSha256, "successorViewSha256");
        if (expected.admissionState() != AdmissionState.ADMITTING
                || successorOwnerEpoch <= expected.ownerEpoch()
                || successorSourceGeneration <= expected.sourceGeneration()) {
            throw new IllegalArgumentException("takeover requires a newer owner and source generation");
        }
        BindingReadSelector successorCore = selectorCore(
                expected,
                successorViewSha256,
                successorOwnerEpoch,
                Math.addExact(expected.readAdmissionEpoch(), 1),
                successorSourceGeneration,
                expected.mode(),
                AdmissionState.ADMITTING,
                expected.fallbackSetSha256());
        List<ClosureAnchor> anchors = expected.pendingAnchors();
        if (expected.mode() == SelectorMode.PREFERRED_WITH_FALLBACK) {
            ClosureAnchor anchor = closureAnchor(
                    expected,
                    successorCore,
                    hashLongsAndDigests(
                            "M4-TAKEOVER-PWF-V1",
                            List.of(
                                    M4ReadControlCodecV1.selectorCoreSha256(expected),
                                    M4ReadControlCodecV1.selectorCoreSha256(successorCore),
                                    expected.fallbackSetSha256().orElseThrow())));
            anchors = append(anchors, anchor);
        }
        BindingReadSelector candidate;
        CanonicalBytes candidateBytes;
        try {
            candidate = withInline(successorCore, anchors, expected.activeBatches());
            candidateBytes = M4ReadControlCodecV1.encodeSelector(candidate);
            requireAdmissionCapacity(candidate, candidateBytes);
        } catch (IllegalArgumentException capExceeded) {
            return stopForCapacity(expected);
        }
        return reconcileSelectorCas(M4ReadControlCodecV1.encodeSelector(expected), candidateBytes, false);
    }

    /** Introduces an exact precreated protected fallback set and advances to a fresh epoch. */
    public Outcome introduceFallback(
            BindingReadSelector expected,
            Sha256Digest fallbackViewSha256,
            long successorSourceGeneration,
            List<SourceProtectionIdentity> unsortedSources) {
        validateSelector(expected);
        Objects.requireNonNull(fallbackViewSha256, "fallbackViewSha256");
        if (expected.mode() != SelectorMode.PREFERRED_ONLY
                || expected.admissionState() != AdmissionState.ADMITTING
                || successorSourceGeneration <= expected.sourceGeneration()) {
            throw new IllegalArgumentException(
                    "fallback introduction requires one admitting preferred-only predecessor");
        }
        List<SourceProtectionIdentity> sources = canonicalProtectedSources(unsortedSources, expected.capability());
        BindingReadSelector candidate = new BindingReadSelector(
                binding,
                fallbackViewSha256,
                expected.ownerEpoch(),
                Math.addExact(expected.readAdmissionEpoch(), 1),
                successorSourceGeneration,
                SelectorMode.PREFERRED_WITH_FALLBACK,
                AdmissionState.ADMITTING,
                Optional.of(M4ReadControlCodecV1.calculateFallbackSetSha256(sources)),
                expected.capability(),
                expected.pendingAnchors(),
                expected.activeBatches());
        CanonicalBytes candidateBytes = M4ReadControlCodecV1.encodeSelector(candidate);
        requireAdmissionCapacity(candidate, candidateBytes);
        return reconcileSelectorCas(M4ReadControlCodecV1.encodeSelector(expected), candidateBytes, false);
    }

    /** Changes only the captured view/source generation while retaining exact epoch membership. */
    public Outcome updateMembershipNeutralView(
            BindingReadSelector expected,
            Sha256Digest successorViewSha256,
            long successorSourceGeneration,
            List<SourceProtectionIdentity> exactFallbackSources) {
        validateSelector(expected);
        Objects.requireNonNull(successorViewSha256, "successorViewSha256");
        if (expected.admissionState() != AdmissionState.ADMITTING
                || successorSourceGeneration <= expected.sourceGeneration()) {
            throw new IllegalArgumentException("view update requires an admitting predecessor and newer generation");
        }
        Objects.requireNonNull(exactFallbackSources, "exactFallbackSources");
        List<SourceProtectionIdentity> sources = exactFallbackSources.isEmpty()
                ? List.of()
                : canonicalProtectedSources(exactFallbackSources, expected.capability());
        Optional<Sha256Digest> suppliedSet = sources.isEmpty()
                ? Optional.empty()
                : Optional.of(M4ReadControlCodecV1.calculateFallbackSetSha256(sources));
        if (!suppliedSet.equals(expected.fallbackSetSha256())) {
            throw new IllegalArgumentException("membership-neutral update changed the exact fallback identities");
        }
        BindingReadSelector candidate = new BindingReadSelector(
                binding,
                successorViewSha256,
                expected.ownerEpoch(),
                expected.readAdmissionEpoch(),
                successorSourceGeneration,
                expected.mode(),
                AdmissionState.ADMITTING,
                expected.fallbackSetSha256(),
                expected.capability(),
                expected.pendingAnchors(),
                expected.activeBatches());
        CanonicalBytes candidateBytes = M4ReadControlCodecV1.encodeSelector(candidate);
        requireAdmissionCapacity(candidate, candidateBytes);
        return reconcileSelectorCas(M4ReadControlCodecV1.encodeSelector(expected), candidateBytes, false);
    }

    public Outcome closeFallback(
            BindingReadSelector expected,
            Sha256Digest preferredOnlyViewSha256,
            long successorSourceGeneration,
            List<SourceProtectionIdentity> unsortedSources) {
        requireBinding(expected.binding());
        Objects.requireNonNull(preferredOnlyViewSha256, "preferredOnlyViewSha256");
        validateSelector(expected);
        if (expected.mode() != SelectorMode.PREFERRED_WITH_FALLBACK
                || expected.admissionState() != AdmissionState.ADMITTING
                || successorSourceGeneration <= expected.sourceGeneration()) {
            throw new IllegalArgumentException("fused closure requires one admitting fallback-bearing predecessor");
        }
        requireCapabilityAdmitted(expected.capability());
        List<SourceProtectionIdentity> sources = canonicalProtectedSources(unsortedSources, expected.capability());
        Sha256Digest fallbackSet = M4ReadControlCodecV1.calculateFallbackSetSha256(sources);
        if (!expected.fallbackSetSha256().orElseThrow().equals(fallbackSet)) {
            throw new IllegalArgumentException("closure source set differs from the selected fallback authority");
        }

        BindingReadSelector successorCore = new BindingReadSelector(
                binding,
                preferredOnlyViewSha256,
                expected.ownerEpoch(),
                Math.addExact(expected.readAdmissionEpoch(), 1),
                successorSourceGeneration,
                SelectorMode.PREFERRED_ONLY,
                AdmissionState.ADMITTING,
                Optional.empty(),
                expected.capability(),
                List.of(),
                List.of());
        Sha256Digest predecessorCoreSha = M4ReadControlCodecV1.selectorCoreSha256(expected);
        Sha256Digest successorCoreSha = M4ReadControlCodecV1.selectorCoreSha256(successorCore);
        Sha256Digest transitionSha = transitionSha(predecessorCoreSha, successorCoreSha, fallbackSet);
        ClosureAnchor anchor = new ClosureAnchor(
                expected.readAdmissionEpoch(),
                expected.ownerEpoch(),
                predecessorCoreSha,
                successorCoreSha,
                transitionSha,
                expected.capability());
        SourceRetirementBatch batch = batch(
                predecessorCoreSha,
                successorCoreSha,
                transitionSha,
                fallbackSet,
                expected.readAdmissionEpoch(),
                expected.capability(),
                sources);

        CanonicalBytes candidateBytes;
        try {
            List<ClosureAnchor> anchors = append(expected.pendingAnchors(), anchor);
            List<SourceRetirementBatch> batches = append(expected.activeBatches(), batch);
            BindingReadSelector candidate = new BindingReadSelector(
                    binding,
                    preferredOnlyViewSha256,
                    expected.ownerEpoch(),
                    Math.addExact(expected.readAdmissionEpoch(), 1),
                    successorSourceGeneration,
                    SelectorMode.PREFERRED_ONLY,
                    AdmissionState.ADMITTING,
                    Optional.empty(),
                    expected.capability(),
                    anchors,
                    batches);
            candidateBytes = M4ReadControlCodecV1.encodeSelector(candidate);
            requireAdmissionCapacity(candidate, candidateBytes);
        } catch (IllegalArgumentException capExceeded) {
            return stopForCapacity(expected);
        }
        CanonicalBytes expectedBytes = M4ReadControlCodecV1.encodeSelector(expected);
        return reconcileSelectorCas(expectedBytes, candidateBytes, false);
    }

    public Outcome publishTerminal(ReadAdmissionEpochTerminalCut terminal) {
        requireBinding(terminal.binding());
        CanonicalBytes candidate = M4ReadControlCodecV1.encodeTerminal(terminal);
        Optional<CanonicalBytes> existing = metadata.get(keys.terminal(terminal.readAdmissionEpoch()));
        if (existing.isPresent() && existing.orElseThrow().equals(candidate)) {
            return Outcome.EXISTING_EXACT;
        }
        BindingReadSelector selector = readSelector().orElseThrow();
        ClosureAnchor anchor = findAnchor(selector, terminal.readAdmissionEpoch());
        validateTerminal(anchor, terminal);
        return reconcileTerminalCreate(keys.terminal(terminal.readAdmissionEpoch()), candidate, anchor);
    }

    public Outcome publishProof(ReadQuiescenceProof proof) {
        requireBinding(proof.binding());
        requireCapabilityAdmitted(proof.capability());
        readSelector().orElseThrow(() -> new IllegalStateException("quiescence proof lacks its Binding selector"));
        CanonicalBytes terminalBytes = metadata.get(keys.terminal(proof.readAdmissionEpoch()))
                .orElseThrow(() -> new IllegalStateException("quiescence proof lacks its exact terminal cut"));
        ReadAdmissionEpochTerminalCut terminal = M4ReadControlCodecV1.decodeTerminal(terminalBytes);
        validateTerminalEvidence(terminal);
        if (!terminal.binding().equals(binding)
                || !M4ReadControlCodecV1.terminalSha256(terminal).equals(proof.terminalCutSha256())
                || proof.readAdmissionEpoch() != terminal.readAdmissionEpoch()
                || proof.drainedThroughReadViewGeneration() < terminal.lastAdmittedAndDrainedReadViewGeneration()
                || proof.safeAfterAuthorityTimeMillis() < terminal.safeAfterAuthorityTimeMillis()
                || !proof.capability().equals(terminal.capability())
                || proof.kind() != terminal.kind()) {
            throw new IllegalArgumentException("quiescence proof differs from the exact verified terminal cut");
        }
        CanonicalBytes candidate = M4ReadControlCodecV1.encodeProof(proof);
        Outcome created = reconcileCreate(keys.proof(proof.readAdmissionEpoch()), candidate, ExistingKind.EXACT_ONLY);
        if (created != Outcome.APPLIED && created != Outcome.EXISTING_EXACT) {
            return created;
        }
        return appendProofHead(proof, Sha256Digest.hash(candidate));
    }

    public Outcome createProtection(SourceProtection protection) {
        requireBinding(protection.binding());
        if (protection.state() != ProtectionState.PROTECTED
                || protection.releasedByBatchSha256().isPresent()
                || protection.releaseProofHeadSha256().isPresent()) {
            throw new IllegalArgumentException("new source protection must be exactly PROTECTED");
        }
        requireCapabilityAdmitted(protection.identity().capability());
        CanonicalBytes candidate = M4ReadControlCodecV1.encodeProtection(protection);
        return reconcileCreate(
                keys.protection(
                        protection.identity().sourceIdentitySha256(),
                        protection.identity().protectionGeneration()),
                candidate,
                ExistingKind.EXACT_ONLY);
    }

    /**
     * Removes terminal-backed closure anchors in one bounded selector CAS. Terminal and proof
     * records remain immutable durable authority; pruning changes no quiescence fact.
     */
    public Outcome pruneTerminalBackedAnchors(BindingReadSelector expected, int maximumToPrune) {
        requireBinding(expected.binding());
        if (maximumToPrune <= 0) {
            throw new IllegalArgumentException("maximum prune count must be positive");
        }
        List<ClosureAnchor> retained = new ArrayList<>();
        int pruned = 0;
        for (ClosureAnchor anchor : expected.pendingAnchors()) {
            if (pruned < maximumToPrune && hasValidTerminal(anchor)) {
                pruned++;
            } else {
                retained.add(anchor);
            }
        }
        if (pruned == 0) {
            return Outcome.EXISTING_EXACT;
        }
        BindingReadSelector candidate = new BindingReadSelector(
                binding,
                expected.selectedViewSha256(),
                expected.ownerEpoch(),
                expected.readAdmissionEpoch(),
                expected.sourceGeneration(),
                expected.mode(),
                expected.admissionState(),
                expected.fallbackSetSha256(),
                expected.capability(),
                retained,
                expected.activeBatches());
        CanonicalBytes expectedBytes = M4ReadControlCodecV1.encodeSelector(expected);
        CanonicalBytes candidateBytes = M4ReadControlCodecV1.encodeSelector(candidate);
        if (candidate.admissionState() == AdmissionState.ADMITTING) {
            requireAdmissionCapacity(candidate, candidateBytes);
        }
        return reconcileSelectorCas(expectedBytes, candidateBytes, false);
    }

    /** Recovers a STOPPED selector only by granting a fresh, never-reused epoch. */
    public Outcome resumeStopped(BindingReadSelector expected, long ownerEpoch) {
        requireBinding(expected.binding());
        if (expected.admissionState() != AdmissionState.STOPPED || ownerEpoch < expected.ownerEpoch()) {
            throw new IllegalArgumentException("resume requires an exact STOPPED predecessor and non-stale owner");
        }
        requireCapabilityAdmitted(expected.capability());
        BindingReadSelector candidate = new BindingReadSelector(
                binding,
                expected.selectedViewSha256(),
                ownerEpoch,
                Math.addExact(expected.readAdmissionEpoch(), 1),
                expected.sourceGeneration(),
                expected.mode(),
                AdmissionState.ADMITTING,
                expected.fallbackSetSha256(),
                expected.capability(),
                expected.pendingAnchors(),
                expected.activeBatches());
        CanonicalBytes candidateBytes = M4ReadControlCodecV1.encodeSelector(candidate);
        requireAdmissionCapacity(candidate, candidateBytes);
        return reconcileSelectorCas(M4ReadControlCodecV1.encodeSelector(expected), candidateBytes, false);
    }

    public Outcome releaseProtection(
            SourceRetirementBatch batch, SourceProtectionIdentity source, BindingReadHazardPoolV1 hazardPool) {
        Objects.requireNonNull(hazardPool, "hazardPool");
        requireBinding(batch.binding());
        if (!batch.sources().contains(source)) {
            throw new IllegalArgumentException("release source is not an exact batch member");
        }
        BindingReadSelector selector;
        try {
            selector = readSelector().orElseThrow();
        } catch (RuntimeException invalidSelector) {
            return Outcome.RETAIN;
        }
        if (selector.mode() != SelectorMode.PREFERRED_ONLY
                || !selector.activeBatches().contains(batch)) {
            return Outcome.RETAIN;
        }
        requireCapabilityAdmitted(source.capability());
        if (hazardPool.scan(binding.bindingId(), source.fallbackSourceGeneration())
                != BindingReadHazardPoolV1.ScanOutcome.CLEAN) {
            return Outcome.RETAIN;
        }
        QuiescenceProofHead head = metadata.get(keys.proofHead())
                .map(M4ReadControlCodecV1::decodeHead)
                .orElseThrow(() -> new IllegalStateException("protection release lacks a proof head"));
        if (!verifyInterval(
                head,
                source.firstFallbackCapableReadAdmissionEpoch(),
                batch.sharedLastFallbackCapableReadAdmissionEpoch())) {
            return Outcome.RETAIN;
        }
        String key = keys.protection(source.sourceIdentitySha256(), source.protectionGeneration());
        Optional<CanonicalBytes> currentBytes = metadata.get(key);
        if (currentBytes.isEmpty()) {
            return Outcome.RETAIN;
        }
        SourceProtection current = M4ReadControlCodecV1.decodeProtection(currentBytes.orElseThrow());
        if (!current.binding().equals(binding) || !current.identity().equals(source)) {
            return Outcome.RETAIN;
        }
        Sha256Digest headSha = Sha256Digest.hash(M4ReadControlCodecV1.encodeHead(head));
        if (current.state() == ProtectionState.RELEASED) {
            return current.releasedByBatchSha256().equals(Optional.of(batch.batchIdSha256()))
                            && current.releaseProofHeadSha256().equals(Optional.of(headSha))
                    ? Outcome.EXISTING_EXACT
                    : Outcome.RETAIN;
        }
        SourceProtection released = new SourceProtection(
                binding, source, ProtectionState.RELEASED, Optional.of(batch.batchIdSha256()), Optional.of(headSha));
        return reconcileCas(key, currentBytes.orElseThrow(), M4ReadControlCodecV1.encodeProtection(released));
    }

    public boolean verifyInterval(QuiescenceProofHead head, long firstEpoch, long lastEpoch) {
        requireBinding(head.binding());
        if (firstEpoch <= 0
                || lastEpoch < firstEpoch
                || lastEpoch - firstEpoch >= M4ReadControlRecordsV1.MAX_PROOF_INTERVAL_EPOCHS) {
            return false;
        }
        Map<Long, ProofEntry> indexed = new LinkedHashMap<>();
        for (ProofEntry entry : head.window()) {
            indexed.put(entry.readAdmissionEpoch(), entry);
        }
        for (ProofFold fold : head.folds()) {
            if (fold.lastEpoch() < firstEpoch || fold.firstEpoch() > lastEpoch) {
                continue;
            }
            List<Sha256Digest> ordered = new ArrayList<>();
            for (long epoch = fold.firstEpoch(); epoch <= fold.lastEpoch(); epoch++) {
                Optional<VerifiedProof> verified = readVerifiedProof(epoch);
                if (verified.isEmpty()) {
                    return false;
                }
                ordered.add(verified.orElseThrow().proofSha256());
                indexed.put(epoch, verified.orElseThrow().entry());
            }
            if (!fold.orderedProofsSha256().equals(orderedProofsSha(ordered))) {
                return false;
            }
        }
        for (long epoch = firstEpoch; epoch <= lastEpoch; epoch++) {
            ProofEntry entry = indexed.get(epoch);
            if (entry == null) {
                return false;
            }
            Optional<VerifiedProof> verified = readVerifiedProof(epoch);
            if (verified.isEmpty() || !verified.orElseThrow().entry().equals(entry)) {
                return false;
            }
        }
        return true;
    }

    private Outcome appendProofHead(ReadQuiescenceProof proof, Sha256Digest proofSha) {
        String key = keys.proofHead();
        Optional<CanonicalBytes> currentBytes = metadata.get(key);
        QuiescenceProofHead current = currentBytes
                .map(M4ReadControlCodecV1::decodeHead)
                .orElse(new QuiescenceProofHead(binding, 1, List.of(), List.of()));
        ProofEntry newEntry =
                new ProofEntry(proof.readAdmissionEpoch(), proofSha, proof.terminalCutSha256(), proof.capability());
        Optional<ProofEntry> duplicate = current.window().stream()
                .filter(entry -> entry.readAdmissionEpoch() == proof.readAdmissionEpoch())
                .findFirst();
        if (duplicate.isPresent()) {
            return duplicate.orElseThrow().equals(newEntry) ? Outcome.EXISTING_EXACT : Outcome.CONFLICT;
        }
        boolean alreadyFolded = current.folds().stream()
                .anyMatch(value -> proof.readAdmissionEpoch() >= value.firstEpoch()
                        && proof.readAdmissionEpoch() <= value.lastEpoch());
        if (alreadyFolded) {
            return readVerifiedProof(proof.readAdmissionEpoch())
                            .filter(value -> value.entry().equals(newEntry))
                            .isPresent()
                    ? Outcome.EXISTING_EXACT
                    : Outcome.CONFLICT;
        }
        if (!current.window().isEmpty()
                && proof.readAdmissionEpoch()
                        <= current.window().get(current.window().size() - 1).readAdmissionEpoch()) {
            return Outcome.CONFLICT;
        }
        List<ProofFold> folds = new ArrayList<>(current.folds());
        List<ProofEntry> window = new ArrayList<>(current.window());
        if (window.size() == M4ReadControlRecordsV1.MAX_PROOF_WINDOW) {
            if (folds.size() == M4ReadControlRecordsV1.MAX_PROOF_FOLDS
                    || !isContiguous(window.subList(0, M4ReadControlRecordsV1.PROOF_FOLD_ENTRIES))) {
                return stopSelectorForProofCapacity();
            }
            List<ProofEntry> folded = List.copyOf(window.subList(0, M4ReadControlRecordsV1.PROOF_FOLD_ENTRIES));
            folds.add(new ProofFold(
                    folded.get(0).readAdmissionEpoch(),
                    folded.get(folded.size() - 1).readAdmissionEpoch(),
                    orderedProofsSha(
                            folded.stream().map(ProofEntry::proofSha256).toList())));
            window = new ArrayList<>(window.subList(M4ReadControlRecordsV1.PROOF_FOLD_ENTRIES, window.size()));
        }
        window.add(newEntry);
        long nextGeneration = currentBytes.isEmpty() ? 1 : Math.addExact(current.generation(), 1);
        QuiescenceProofHead candidate = new QuiescenceProofHead(binding, nextGeneration, folds, window);
        CanonicalBytes candidateBytes = M4ReadControlCodecV1.encodeHead(candidate);
        if (currentBytes.isEmpty()) {
            return reconcileCreate(key, candidateBytes, ExistingKind.EXACT_ONLY);
        }
        return reconcileCas(key, currentBytes.orElseThrow(), candidateBytes);
    }

    private Optional<VerifiedProof> readVerifiedProof(long epoch) {
        Optional<CanonicalBytes> proofBytes = metadata.get(keys.proof(epoch));
        if (proofBytes.isEmpty()) {
            return Optional.empty();
        }
        ReadQuiescenceProof proof;
        try {
            proof = M4ReadControlCodecV1.decodeProof(proofBytes.orElseThrow());
            requireCapabilityAdmitted(proof.capability());
        } catch (RuntimeException invalid) {
            return Optional.empty();
        }
        if (!proof.binding().equals(binding) || proof.readAdmissionEpoch() != epoch) {
            return Optional.empty();
        }
        Optional<CanonicalBytes> terminalBytes = metadata.get(keys.terminal(epoch));
        if (terminalBytes.isEmpty()) {
            return Optional.empty();
        }
        ReadAdmissionEpochTerminalCut terminal;
        try {
            terminal = M4ReadControlCodecV1.decodeTerminal(terminalBytes.orElseThrow());
            validateTerminalEvidence(terminal);
        } catch (RuntimeException invalid) {
            return Optional.empty();
        }
        if (!Sha256Digest.hash(terminalBytes.orElseThrow()).equals(proof.terminalCutSha256())
                || !terminal.binding().equals(binding)
                || terminal.readAdmissionEpoch() != epoch
                || !terminal.capability().equals(proof.capability())
                || terminal.kind() != proof.kind()
                || proof.drainedThroughReadViewGeneration() < terminal.lastAdmittedAndDrainedReadViewGeneration()
                || proof.safeAfterAuthorityTimeMillis() < terminal.safeAfterAuthorityTimeMillis()) {
            return Optional.empty();
        }
        Sha256Digest proofSha = Sha256Digest.hash(proofBytes.orElseThrow());
        return Optional.of(new VerifiedProof(
                proofSha, new ProofEntry(epoch, proofSha, proof.terminalCutSha256(), proof.capability())));
    }

    private Outcome reconcileTerminalCreate(String key, CanonicalBytes candidate, ClosureAnchor anchor) {
        ControlMutationOutcome outcome = metadata.putIfAbsent(key, candidate);
        Optional<CanonicalBytes> observed = metadata.get(key);
        if (observed.isPresent() && observed.orElseThrow().equals(candidate)) {
            return outcome == ControlMutationOutcome.APPLIED ? Outcome.APPLIED : Outcome.EXISTING_EXACT;
        }
        if (observed.isEmpty()) {
            return outcome == ControlMutationOutcome.DEFINITIVE_CONFLICT ? Outcome.CONFLICT : Outcome.RETAIN;
        }
        try {
            ReadAdmissionEpochTerminalCut different = M4ReadControlCodecV1.decodeTerminal(observed.orElseThrow());
            validateTerminal(anchor, different);
            return Outcome.ADOPTED_DIFFERENT_VALID_TERMINAL;
        } catch (RuntimeException invalid) {
            return Outcome.QUARANTINED_INVALID_OCCUPANT;
        }
    }

    private void validateTerminal(ClosureAnchor anchor, ReadAdmissionEpochTerminalCut terminal) {
        if (!terminal.closureAnchorSha256().equals(M4ReadControlCodecV1.anchorSha256(anchor))
                || terminal.readAdmissionEpoch() != anchor.closedReadAdmissionEpoch()
                || terminal.ownerEpoch() != anchor.ownerEpoch()
                || !terminal.capability().equals(anchor.capability())) {
            throw new IllegalArgumentException("terminal cut differs from its exact closure anchor");
        }
        validateTerminalEvidence(terminal);
    }

    private void validateTerminalEvidence(ReadAdmissionEpochTerminalCut terminal) {
        requireBinding(terminal.binding());
        requireCapabilityAdmitted(terminal.capability());
        CapabilityEvidence capability = readCapability(terminal.capability());
        if (terminal.kind() == TerminalKind.QUALIFIED_EXPIRY
                && capability.kind() != CapabilityKind.AUTHORITY_EXPIRY_V1) {
            throw new IllegalArgumentException("qualified expiry is not admitted by the bound capability");
        }
        if (terminal.kind() == TerminalKind.QUALIFIED_EXPIRY) {
            long safeAfter =
                    Math.addExact(terminal.authorityNotAfterMillis(), capability.maximumSourceAccessLifetimeMillis());
            safeAfter = Math.addExact(safeAfter, capability.maximumClockSkewMillis());
            safeAfter = Math.addExact(safeAfter, capability.propagationGraceMillis());
            if (terminal.safeAfterAuthorityTimeMillis() != safeAfter
                    || terminal.observedAuthorityTimeMillis() < safeAfter) {
                throw new IllegalArgumentException("qualified expiry does not satisfy the exact capability time bound");
            }
        }
    }

    private void validateSelector(BindingReadSelector selector) {
        requireBinding(selector.binding());
        requireCapabilityAdmitted(selector.capability());
        for (ClosureAnchor anchor : selector.pendingAnchors()) {
            requireCapabilityAdmitted(anchor.capability());
        }
        long distinctBatches = selector.activeBatches().stream()
                .map(SourceRetirementBatch::batchIdSha256)
                .distinct()
                .count();
        if (distinctBatches != selector.activeBatches().size()) {
            throw new IllegalArgumentException("selector contains a duplicate retirement batch");
        }
        for (SourceRetirementBatch batch : selector.activeBatches()) {
            requireBinding(batch.binding());
            requireCapabilityAdmitted(batch.capability());
            M4ReadControlCodecV1.encodeBatch(batch);
        }
    }

    private boolean hasValidTerminal(ClosureAnchor anchor) {
        Optional<CanonicalBytes> terminalBytes = metadata.get(keys.terminal(anchor.closedReadAdmissionEpoch()));
        if (terminalBytes.isEmpty()) {
            return false;
        }
        try {
            validateTerminal(anchor, M4ReadControlCodecV1.decodeTerminal(terminalBytes.orElseThrow()));
            return true;
        } catch (RuntimeException invalid) {
            return false;
        }
    }

    private Outcome stopForCapacity(BindingReadSelector expected) {
        BindingReadSelector stoppedCore = new BindingReadSelector(
                binding,
                expected.selectedViewSha256(),
                expected.ownerEpoch(),
                Math.addExact(expected.readAdmissionEpoch(), 1),
                expected.sourceGeneration(),
                SelectorMode.PREFERRED_WITH_FALLBACK,
                AdmissionState.STOPPED,
                expected.fallbackSetSha256(),
                expected.capability(),
                List.of(),
                List.of());
        Sha256Digest predecessorCore = M4ReadControlCodecV1.selectorCoreSha256(expected);
        Sha256Digest stoppedCoreSha = M4ReadControlCodecV1.selectorCoreSha256(stoppedCore);
        ClosureAnchor stoppedAnchor = new ClosureAnchor(
                expected.readAdmissionEpoch(),
                expected.ownerEpoch(),
                predecessorCore,
                stoppedCoreSha,
                hashLongsAndDigests(
                        "M4-CAPACITY-STOPPED-V1",
                        List.of(
                                predecessorCore,
                                stoppedCoreSha,
                                expected.fallbackSetSha256().orElseThrow())),
                expected.capability());
        BindingReadSelector stopped;
        try {
            stopped = new BindingReadSelector(
                    binding,
                    expected.selectedViewSha256(),
                    expected.ownerEpoch(),
                    Math.addExact(expected.readAdmissionEpoch(), 1),
                    expected.sourceGeneration(),
                    SelectorMode.PREFERRED_WITH_FALLBACK,
                    AdmissionState.STOPPED,
                    expected.fallbackSetSha256(),
                    expected.capability(),
                    append(expected.pendingAnchors(), stoppedAnchor),
                    expected.activeBatches());
        } catch (IllegalArgumentException exhausted) {
            return Outcome.RETAIN;
        }
        CanonicalBytes stoppedBytes;
        try {
            stoppedBytes = M4ReadControlCodecV1.encodeSelector(stopped);
        } catch (IllegalArgumentException exhausted) {
            return Outcome.RETAIN;
        }
        Outcome outcome = reconcileSelectorCas(M4ReadControlCodecV1.encodeSelector(expected), stoppedBytes, true);
        return outcome == Outcome.APPLIED || outcome == Outcome.EXISTING_EXACT ? Outcome.STOPPED : outcome;
    }

    /** Closes read admission when the bounded durable proof authority cannot accept another epoch. */
    private Outcome stopSelectorForProofCapacity() {
        BindingReadSelector expected;
        try {
            expected = readSelector().orElseThrow();
        } catch (RuntimeException missingOrInvalidSelector) {
            return Outcome.RETAIN;
        }
        if (expected.admissionState() == AdmissionState.STOPPED) {
            return Outcome.STOPPED;
        }
        BindingReadSelector stoppedCore = new BindingReadSelector(
                binding,
                expected.selectedViewSha256(),
                expected.ownerEpoch(),
                Math.addExact(expected.readAdmissionEpoch(), 1),
                expected.sourceGeneration(),
                expected.mode(),
                AdmissionState.STOPPED,
                expected.fallbackSetSha256(),
                expected.capability(),
                List.of(),
                List.of());
        List<ClosureAnchor> anchors = expected.pendingAnchors();
        if (expected.mode() == SelectorMode.PREFERRED_WITH_FALLBACK) {
            Sha256Digest predecessorCore = M4ReadControlCodecV1.selectorCoreSha256(expected);
            Sha256Digest stoppedCoreSha = M4ReadControlCodecV1.selectorCoreSha256(stoppedCore);
            ClosureAnchor stoppedAnchor = new ClosureAnchor(
                    expected.readAdmissionEpoch(),
                    expected.ownerEpoch(),
                    predecessorCore,
                    stoppedCoreSha,
                    hashLongsAndDigests(
                            "M4-PROOF-CAPACITY-STOPPED-V1",
                            List.of(
                                    predecessorCore,
                                    stoppedCoreSha,
                                    expected.fallbackSetSha256().orElseThrow())),
                    expected.capability());
            try {
                anchors = append(anchors, stoppedAnchor);
            } catch (IllegalArgumentException exhausted) {
                return Outcome.RETAIN;
            }
        }
        BindingReadSelector stopped;
        CanonicalBytes stoppedBytes;
        try {
            stopped = new BindingReadSelector(
                    binding,
                    expected.selectedViewSha256(),
                    expected.ownerEpoch(),
                    Math.addExact(expected.readAdmissionEpoch(), 1),
                    expected.sourceGeneration(),
                    expected.mode(),
                    AdmissionState.STOPPED,
                    expected.fallbackSetSha256(),
                    expected.capability(),
                    anchors,
                    expected.activeBatches());
            stoppedBytes = M4ReadControlCodecV1.encodeSelector(stopped);
        } catch (IllegalArgumentException exhausted) {
            return Outcome.RETAIN;
        }
        Outcome outcome = reconcileSelectorCas(M4ReadControlCodecV1.encodeSelector(expected), stoppedBytes, true);
        return outcome == Outcome.APPLIED || outcome == Outcome.EXISTING_EXACT ? Outcome.STOPPED : outcome;
    }

    private void requireAdmissionCapacity(BindingReadSelector selector, CanonicalBytes encoded) {
        if (selector.admissionState() != AdmissionState.ADMITTING) {
            return;
        }
        if (selector.pendingAnchors().size() >= M4ReadControlRecordsV1.MAX_PENDING_ANCHORS
                || selector.activeBatches().size() >= M4ReadControlRecordsV1.MAX_ACTIVE_BATCHES
                || encoded.length() + M4ReadControlCodecV1.EMERGENCY_STOPPED_RESERVE_BYTES
                        > M4ReadControlCodecV1.MAX_SELECTOR_BYTES) {
            throw new IllegalArgumentException("ADMITTING selector consumes its emergency STOPPED envelope");
        }
        if (metadata instanceof M5BindingAuthorityControlMetadataStoreV1 authorityStore) {
            authorityStore.requireSelectorCapacity(selector);
        }
    }

    private Outcome reconcileSelectorCas(CanonicalBytes expected, CanonicalBytes candidate, boolean stopped) {
        ControlMutationOutcome outcome = metadata.compareAndSet(keys.selector(), Optional.of(expected), candidate);
        Optional<CanonicalBytes> observed = metadata.get(keys.selector());
        if (observed.isPresent() && observed.orElseThrow().equals(candidate)) {
            return outcome == ControlMutationOutcome.APPLIED ? Outcome.APPLIED : Outcome.EXISTING_EXACT;
        }
        if (observed.isPresent() && observed.orElseThrow().equals(expected)) {
            return outcome == ControlMutationOutcome.DEFINITIVE_CONFLICT
                    ? Outcome.CONFLICT
                    : Outcome.RETRY_EXACT_PREDECESSOR;
        }
        return stopped && observed.isEmpty() ? Outcome.RETAIN : Outcome.CONFLICT;
    }

    private Outcome reconcileCreate(String key, CanonicalBytes candidate, ExistingKind existingKind) {
        ControlMutationOutcome outcome = metadata.putIfAbsent(key, candidate);
        Optional<CanonicalBytes> observed = metadata.get(key);
        if (observed.isPresent() && observed.orElseThrow().equals(candidate)) {
            return outcome == ControlMutationOutcome.APPLIED ? Outcome.APPLIED : Outcome.EXISTING_EXACT;
        }
        if (observed.isEmpty()) {
            return outcome == ControlMutationOutcome.DEFINITIVE_CONFLICT ? Outcome.CONFLICT : Outcome.RETAIN;
        }
        return existingKind == ExistingKind.EXACT_ONLY ? Outcome.CONFLICT : Outcome.RETAIN;
    }

    private Outcome reconcileCas(String key, CanonicalBytes expected, CanonicalBytes candidate) {
        ControlMutationOutcome outcome = metadata.compareAndSet(key, Optional.of(expected), candidate);
        Optional<CanonicalBytes> observed = metadata.get(key);
        if (observed.isPresent() && observed.orElseThrow().equals(candidate)) {
            return outcome == ControlMutationOutcome.APPLIED ? Outcome.APPLIED : Outcome.EXISTING_EXACT;
        }
        if (observed.isPresent() && observed.orElseThrow().equals(expected)) {
            return outcome == ControlMutationOutcome.DEFINITIVE_CONFLICT
                    ? Outcome.CONFLICT
                    : Outcome.RETRY_EXACT_PREDECESSOR;
        }
        return Outcome.CONFLICT;
    }

    private CapabilityEvidence readCapability(CapabilityBinding capability) {
        CapabilityEvidence value = metadata.get(keys.capability(capability.generation()))
                .map(M4ReadControlCodecV1::decodeCapability)
                .orElseThrow(() -> new IllegalStateException("bound capability evidence is missing"));
        requireBinding(value.binding());
        if (!M4ReadControlCodecV1.capabilityEvidenceSha256(value).equals(capability.evidenceSha256())) {
            throw new IllegalStateException("bound capability evidence digest differs");
        }
        return value;
    }

    private void requireCapabilityAdmitted(CapabilityBinding capability) {
        if (readCapability(capability).state() != CapabilityState.ADMITTED) {
            throw new IllegalStateException("bound quiescence capability is revoked");
        }
    }

    private ClosureAnchor findAnchor(BindingReadSelector selector, long epoch) {
        return selector.pendingAnchors().stream()
                .filter(value -> value.closedReadAdmissionEpoch() == epoch)
                .findFirst()
                .orElseThrow(() -> new IllegalArgumentException("selector has no exact pending closure anchor"));
    }

    private SourceRetirementBatch batch(
            Sha256Digest predecessor,
            Sha256Digest successor,
            Sha256Digest transition,
            Sha256Digest fallbackSet,
            long sharedLast,
            CapabilityBinding capability,
            List<SourceProtectionIdentity> sources) {
        long minimum = sources.stream()
                .mapToLong(SourceProtectionIdentity::firstFallbackCapableReadAdmissionEpoch)
                .min()
                .orElseThrow();
        Sha256Digest placeholder = Sha256Digest.hash(CanonicalBytes.copyOf(new byte[] {1}));
        SourceRetirementBatch draft = new SourceRetirementBatch(
                binding,
                placeholder,
                predecessor,
                successor,
                transition,
                fallbackSet,
                sharedLast,
                minimum,
                capability,
                sources);
        return new SourceRetirementBatch(
                binding,
                M4ReadControlCodecV1.calculateBatchId(draft),
                predecessor,
                successor,
                transition,
                fallbackSet,
                sharedLast,
                minimum,
                capability,
                sources);
    }

    private List<SourceProtectionIdentity> canonicalProtectedSources(
            List<SourceProtectionIdentity> unsortedSources, CapabilityBinding requiredCapability) {
        Objects.requireNonNull(unsortedSources, "sources");
        if (unsortedSources.isEmpty()
                || unsortedSources.size() > M4ReadControlRecordsV1.MAX_SOURCES_PER_BATCH
                || unsortedSources.stream().anyMatch(Objects::isNull)) {
            throw new IllegalArgumentException("fallback source set is outside its closed hard cap");
        }
        List<SourceProtectionIdentity> sources = unsortedSources.stream()
                .sorted(Comparator.comparing(row -> row.sourceIdentitySha256().toHex()))
                .toList();
        if (sources.stream().distinct().count() != sources.size()
                || sources.stream().anyMatch(source -> !source.capability().equals(requiredCapability))) {
            throw new IllegalArgumentException("fallback sources are not unique or capability-exact");
        }
        for (SourceProtectionIdentity source : sources) {
            String key = keys.protection(source.sourceIdentitySha256(), source.protectionGeneration());
            Optional<CanonicalBytes> bytes = metadata.get(key);
            if (bytes.isEmpty()) {
                throw new IllegalStateException("fallback source protection is missing");
            }
            SourceProtection protection = M4ReadControlCodecV1.decodeProtection(bytes.orElseThrow());
            if (!protection.binding().equals(binding)
                    || !protection.identity().equals(source)
                    || protection.state() != ProtectionState.PROTECTED) {
                throw new IllegalStateException("fallback source protection is not exact PROTECTED authority");
            }
        }
        return sources;
    }

    private BindingReadSelector selectorCore(
            BindingReadSelector expected,
            Sha256Digest selectedViewSha256,
            long ownerEpoch,
            long readAdmissionEpoch,
            long sourceGeneration,
            SelectorMode mode,
            AdmissionState admissionState,
            Optional<Sha256Digest> fallbackSetSha256) {
        return new BindingReadSelector(
                binding,
                selectedViewSha256,
                ownerEpoch,
                readAdmissionEpoch,
                sourceGeneration,
                mode,
                admissionState,
                fallbackSetSha256,
                expected.capability(),
                List.of(),
                List.of());
    }

    private static BindingReadSelector withInline(
            BindingReadSelector core, List<ClosureAnchor> anchors, List<SourceRetirementBatch> batches) {
        return new BindingReadSelector(
                core.binding(),
                core.selectedViewSha256(),
                core.ownerEpoch(),
                core.readAdmissionEpoch(),
                core.sourceGeneration(),
                core.mode(),
                core.admissionState(),
                core.fallbackSetSha256(),
                core.capability(),
                anchors,
                batches);
    }

    private static ClosureAnchor closureAnchor(
            BindingReadSelector predecessor, BindingReadSelector successorCore, Sha256Digest transitionSha256) {
        return new ClosureAnchor(
                predecessor.readAdmissionEpoch(),
                predecessor.ownerEpoch(),
                M4ReadControlCodecV1.selectorCoreSha256(predecessor),
                M4ReadControlCodecV1.selectorCoreSha256(successorCore),
                transitionSha256,
                predecessor.capability());
    }

    private void requireBinding(BindingIdentity value) {
        if (!binding.equals(value)) {
            throw new IllegalArgumentException("M4 control value belongs to another Binding identity");
        }
    }

    private static <T> List<T> append(List<T> previous, T value) {
        List<T> result = new ArrayList<>(previous);
        result.add(value);
        return List.copyOf(result);
    }

    private static Sha256Digest transitionSha(
            Sha256Digest predecessor, Sha256Digest successor, Sha256Digest fallbackSet) {
        return hashLongsAndDigests("M4-FUSED-PWF-TO-PO-V1", List.of(predecessor, successor, fallbackSet));
    }

    private static Sha256Digest orderedProofsSha(List<Sha256Digest> proofs) {
        return hashLongsAndDigests("M4-ORDERED-PROOF-FOLD-V1", proofs);
    }

    private static Sha256Digest hashLongsAndDigests(String domain, List<Sha256Digest> digests) {
        try {
            ByteArrayOutputStream bytes = new ByteArrayOutputStream();
            try (DataOutputStream output = new DataOutputStream(bytes)) {
                output.writeUTF(domain);
                output.writeInt(digests.size());
                for (Sha256Digest digest : digests) {
                    output.write(digest.bytes().toByteArray());
                }
            }
            return Sha256Digest.hash(CanonicalBytes.copyOf(bytes.toByteArray()));
        } catch (IOException impossible) {
            throw new IllegalStateException("in-memory M4 digest encoding failed", impossible);
        }
    }

    private static boolean isContiguous(List<ProofEntry> entries) {
        for (int index = 1; index < entries.size(); index++) {
            if (entries.get(index).readAdmissionEpoch()
                    != entries.get(index - 1).readAdmissionEpoch() + 1) {
                return false;
            }
        }
        return true;
    }

    private enum ExistingKind {
        EXACT_ONLY
    }

    private record VerifiedProof(Sha256Digest proofSha256, ProofEntry entry) {}
}
