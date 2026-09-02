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
import com.nereusstream.domain.identity.TopicBindingId;
import java.util.Comparator;
import java.util.List;
import java.util.Objects;
import java.util.Optional;

/** Closed immutable M4 control-plane values. Evidence receipts are deliberately absent. */
public final class M4ReadControlRecordsV1 {
    public static final int MAX_PENDING_ANCHORS = 8;
    public static final int MAX_ACTIVE_BATCHES = 8;
    public static final int MAX_SOURCES_PER_BATCH = 64;
    public static final int MAX_PROOF_WINDOW = 64;
    public static final int PROOF_FOLD_ENTRIES = 32;
    public static final int MAX_PROOF_FOLDS = 64;
    public static final long MAX_PROOF_INTERVAL_EPOCHS = 4_096;

    private M4ReadControlRecordsV1() {}

    public enum CapabilityKind {
        DURABLE_DRAIN_ONLY_V1,
        AUTHORITY_EXPIRY_V1
    }

    public enum CapabilityState {
        ADMITTED,
        REVOKED
    }

    public enum SelectorMode {
        PREFERRED_WITH_FALLBACK,
        PREFERRED_ONLY
    }

    public enum AdmissionState {
        ADMITTING,
        STOPPED
    }

    public enum TerminalKind {
        PLANNED_DRAIN,
        QUALIFIED_EXPIRY
    }

    public enum ProtectionState {
        PROTECTED,
        RELEASED
    }

    public record BindingIdentity(
            TopicBindingId bindingId, Sha256Digest incarnationSha256, Sha256Digest storageEpochSha256) {
        public BindingIdentity {
            Objects.requireNonNull(bindingId, "bindingId");
            Objects.requireNonNull(incarnationSha256, "incarnationSha256");
            Objects.requireNonNull(storageEpochSha256, "storageEpochSha256");
            if (bindingId.digest().isZero() || incarnationSha256.isZero() || storageEpochSha256.isZero()) {
                throw new IllegalArgumentException("M4 Binding identity contains a zero digest");
            }
        }
    }

    public record CapabilityEvidence(
            BindingIdentity binding,
            long generation,
            long backendAdmissionGeneration,
            CapabilityKind kind,
            CapabilityState state,
            Sha256Digest backendAdapterSha256,
            Sha256Digest backendProtocolConfigurationSha256,
            Sha256Digest readAdmissionContractSha256,
            Sha256Digest verifierSha256,
            Sha256Digest conformanceReceiptIdentitySha256,
            Sha256Digest conformanceReceiptSha256,
            Sha256Digest authorityTimeSemanticsSha256,
            long maximumSourceAccessLifetimeMillis,
            long maximumClockSkewMillis,
            long propagationGraceMillis) {
        public CapabilityEvidence {
            Objects.requireNonNull(binding, "binding");
            Objects.requireNonNull(kind, "kind");
            Objects.requireNonNull(state, "state");
            requireDigest(backendAdapterSha256, "backendAdapterSha256");
            requireDigest(backendProtocolConfigurationSha256, "backendProtocolConfigurationSha256");
            requireDigest(readAdmissionContractSha256, "readAdmissionContractSha256");
            requireDigest(verifierSha256, "verifierSha256");
            requireDigest(conformanceReceiptIdentitySha256, "conformanceReceiptIdentitySha256");
            requireDigest(conformanceReceiptSha256, "conformanceReceiptSha256");
            requireDigest(authorityTimeSemanticsSha256, "authorityTimeSemanticsSha256");
            if (generation <= 0
                    || backendAdmissionGeneration <= 0
                    || maximumSourceAccessLifetimeMillis <= 0
                    || maximumClockSkewMillis < 0
                    || propagationGraceMillis < 0) {
                throw new IllegalArgumentException("capability generation/time bounds are outside their domains");
            }
            if (kind == CapabilityKind.DURABLE_DRAIN_ONLY_V1
                    && (maximumClockSkewMillis != 0 || propagationGraceMillis != 0)) {
                throw new IllegalArgumentException("durable-drain-only capability cannot carry expiry allowances");
            }
        }

        public CapabilityEvidence revoke() {
            if (state == CapabilityState.REVOKED) {
                return this;
            }
            return new CapabilityEvidence(
                    binding,
                    generation,
                    backendAdmissionGeneration,
                    kind,
                    CapabilityState.REVOKED,
                    backendAdapterSha256,
                    backendProtocolConfigurationSha256,
                    readAdmissionContractSha256,
                    verifierSha256,
                    conformanceReceiptIdentitySha256,
                    conformanceReceiptSha256,
                    authorityTimeSemanticsSha256,
                    maximumSourceAccessLifetimeMillis,
                    maximumClockSkewMillis,
                    propagationGraceMillis);
        }
    }

    public record CapabilityBinding(long generation, Sha256Digest evidenceSha256) {
        public CapabilityBinding {
            requireDigest(evidenceSha256, "evidenceSha256");
            if (generation <= 0) {
                throw new IllegalArgumentException("capability binding generation must be positive");
            }
        }
    }

    public record ClosureAnchor(
            long closedReadAdmissionEpoch,
            long ownerEpoch,
            Sha256Digest predecessorSelectorCoreSha256,
            Sha256Digest successorSelectorCoreSha256,
            Sha256Digest transitionSha256,
            CapabilityBinding capability) {
        public ClosureAnchor {
            requireDigest(predecessorSelectorCoreSha256, "predecessorSelectorCoreSha256");
            requireDigest(successorSelectorCoreSha256, "successorSelectorCoreSha256");
            requireDigest(transitionSha256, "transitionSha256");
            Objects.requireNonNull(capability, "capability");
            if (closedReadAdmissionEpoch <= 0 || ownerEpoch <= 0) {
                throw new IllegalArgumentException("closure anchor epoch is outside its domain");
            }
        }
    }

    public record SourceProtectionIdentity(
            Sha256Digest sourceIdentitySha256,
            long protectionGeneration,
            long firstFallbackCapableReadAdmissionEpoch,
            long fallbackSourceGeneration,
            CapabilityBinding capability) {
        public SourceProtectionIdentity {
            requireDigest(sourceIdentitySha256, "sourceIdentitySha256");
            Objects.requireNonNull(capability, "capability");
            if (protectionGeneration <= 0
                    || firstFallbackCapableReadAdmissionEpoch <= 0
                    || fallbackSourceGeneration <= 0) {
                throw new IllegalArgumentException("source-protection identity is outside its domain");
            }
        }
    }

    public record SourceRetirementBatch(
            BindingIdentity binding,
            Sha256Digest batchIdSha256,
            Sha256Digest predecessorSelectorCoreSha256,
            Sha256Digest successorSelectorCoreSha256,
            Sha256Digest transitionSha256,
            Sha256Digest fallbackSetSha256,
            long sharedLastFallbackCapableReadAdmissionEpoch,
            long minimumFirstEpochSummary,
            CapabilityBinding capability,
            List<SourceProtectionIdentity> sources) {
        public SourceRetirementBatch {
            Objects.requireNonNull(binding, "binding");
            requireDigest(batchIdSha256, "batchIdSha256");
            requireDigest(predecessorSelectorCoreSha256, "predecessorSelectorCoreSha256");
            requireDigest(successorSelectorCoreSha256, "successorSelectorCoreSha256");
            requireDigest(transitionSha256, "transitionSha256");
            requireDigest(fallbackSetSha256, "fallbackSetSha256");
            Objects.requireNonNull(capability, "capability");
            sources = List.copyOf(Objects.requireNonNull(sources, "sources"));
            if (sources.isEmpty() || sources.size() > MAX_SOURCES_PER_BATCH) {
                throw new IllegalArgumentException("source-retirement batch source count is outside its hard cap");
            }
            List<SourceProtectionIdentity> sorted = sources.stream()
                    .sorted(Comparator.comparing(
                            row -> row.sourceIdentitySha256().toHex()))
                    .toList();
            if (!sources.equals(sorted) || sources.stream().distinct().count() != sources.size()) {
                throw new IllegalArgumentException("source-retirement batch sources are not sorted unique");
            }
            long calculatedMinimum = sources.stream()
                    .mapToLong(SourceProtectionIdentity::firstFallbackCapableReadAdmissionEpoch)
                    .min()
                    .orElseThrow();
            if (sharedLastFallbackCapableReadAdmissionEpoch <= 0
                    || minimumFirstEpochSummary != calculatedMinimum
                    || sources.stream()
                            .anyMatch(row -> row.firstFallbackCapableReadAdmissionEpoch()
                                    > sharedLastFallbackCapableReadAdmissionEpoch)
                    || sources.stream().anyMatch(row -> !row.capability().equals(capability))) {
                throw new IllegalArgumentException("source-retirement interval/capability summary is invalid");
            }
        }
    }

    public record BindingReadSelector(
            BindingIdentity binding,
            Sha256Digest selectedViewSha256,
            long ownerEpoch,
            long readAdmissionEpoch,
            long sourceGeneration,
            SelectorMode mode,
            AdmissionState admissionState,
            Optional<Sha256Digest> fallbackSetSha256,
            CapabilityBinding capability,
            List<ClosureAnchor> pendingAnchors,
            List<SourceRetirementBatch> activeBatches) {
        public BindingReadSelector {
            Objects.requireNonNull(binding, "binding");
            requireDigest(selectedViewSha256, "selectedViewSha256");
            Objects.requireNonNull(mode, "mode");
            Objects.requireNonNull(admissionState, "admissionState");
            fallbackSetSha256 = Objects.requireNonNull(fallbackSetSha256, "fallbackSetSha256");
            fallbackSetSha256.ifPresent(value -> requireDigest(value, "fallbackSetSha256"));
            Objects.requireNonNull(capability, "capability");
            pendingAnchors = List.copyOf(Objects.requireNonNull(pendingAnchors, "pendingAnchors"));
            activeBatches = List.copyOf(Objects.requireNonNull(activeBatches, "activeBatches"));
            if (ownerEpoch <= 0 || readAdmissionEpoch <= 0 || sourceGeneration <= 0) {
                throw new IllegalArgumentException("selector epoch/generation is outside its domain");
            }
            if ((mode == SelectorMode.PREFERRED_WITH_FALLBACK) != fallbackSetSha256.isPresent()) {
                throw new IllegalArgumentException("selector fallback mode and exact fallback set disagree");
            }
            if (pendingAnchors.size() > MAX_PENDING_ANCHORS || activeBatches.size() > MAX_ACTIVE_BATCHES) {
                throw new IllegalArgumentException("selector inline authority exceeds its hard count caps");
            }
            requireStrictEpochOrder(pendingAnchors);
        }
    }

    public record ReadAdmissionEpochTerminalCut(
            BindingIdentity binding,
            Sha256Digest closureAnchorSha256,
            long readAdmissionEpoch,
            long ownerEpoch,
            long lastAdmittedAndDrainedReadViewGeneration,
            long safeAfterAuthorityTimeMillis,
            CapabilityBinding capability,
            TerminalKind kind,
            Sha256Digest admissionClosedOrOwnerFenceSha256,
            Sha256Digest terminalEvidenceSha256,
            long authorityNotAfterMillis,
            long observedAuthorityTimeMillis,
            long reconcilerEpoch) {
        public ReadAdmissionEpochTerminalCut {
            Objects.requireNonNull(binding, "binding");
            requireDigest(closureAnchorSha256, "closureAnchorSha256");
            Objects.requireNonNull(capability, "capability");
            Objects.requireNonNull(kind, "kind");
            requireDigest(admissionClosedOrOwnerFenceSha256, "admissionClosedOrOwnerFenceSha256");
            requireDigest(terminalEvidenceSha256, "terminalEvidenceSha256");
            if (readAdmissionEpoch <= 0
                    || ownerEpoch <= 0
                    || lastAdmittedAndDrainedReadViewGeneration <= 0
                    || safeAfterAuthorityTimeMillis <= 0
                    || reconcilerEpoch <= 0) {
                throw new IllegalArgumentException("terminal cut epoch/time is outside its domain");
            }
            if (kind == TerminalKind.PLANNED_DRAIN
                    && (authorityNotAfterMillis != 0 || observedAuthorityTimeMillis != 0)) {
                throw new IllegalArgumentException("planned drain cannot carry qualified-expiry timestamps");
            }
            if (kind == TerminalKind.QUALIFIED_EXPIRY
                    && (authorityNotAfterMillis <= 0 || observedAuthorityTimeMillis <= 0)) {
                throw new IllegalArgumentException("qualified expiry lacks authority timestamps");
            }
        }
    }

    public record ReadQuiescenceProof(
            BindingIdentity binding,
            long readAdmissionEpoch,
            Sha256Digest terminalCutSha256,
            long drainedThroughReadViewGeneration,
            long safeAfterAuthorityTimeMillis,
            CapabilityBinding capability,
            TerminalKind kind,
            Sha256Digest proofIdentitySha256) {
        public ReadQuiescenceProof {
            Objects.requireNonNull(binding, "binding");
            requireDigest(terminalCutSha256, "terminalCutSha256");
            Objects.requireNonNull(capability, "capability");
            Objects.requireNonNull(kind, "kind");
            requireDigest(proofIdentitySha256, "proofIdentitySha256");
            if (readAdmissionEpoch <= 0 || drainedThroughReadViewGeneration <= 0 || safeAfterAuthorityTimeMillis <= 0) {
                throw new IllegalArgumentException("quiescence proof epoch/time is outside its domain");
            }
        }
    }

    public record ProofEntry(
            long readAdmissionEpoch,
            Sha256Digest proofSha256,
            Sha256Digest terminalCutSha256,
            CapabilityBinding capability) {
        public ProofEntry {
            requireDigest(proofSha256, "proofSha256");
            requireDigest(terminalCutSha256, "terminalCutSha256");
            Objects.requireNonNull(capability, "capability");
            if (readAdmissionEpoch <= 0) {
                throw new IllegalArgumentException("proof-entry epoch must be positive");
            }
        }
    }

    public record ProofFold(long firstEpoch, long lastEpoch, Sha256Digest orderedProofsSha256) {
        public ProofFold {
            requireDigest(orderedProofsSha256, "orderedProofsSha256");
            if (firstEpoch <= 0
                    || lastEpoch < firstEpoch
                    || lastEpoch - firstEpoch + 1 != PROOF_FOLD_ENTRIES
                    || lastEpoch - firstEpoch >= MAX_PROOF_INTERVAL_EPOCHS) {
                throw new IllegalArgumentException("proof fold interval is outside its hard bound");
            }
        }
    }

    public record QuiescenceProofHead(
            BindingIdentity binding, long generation, List<ProofFold> folds, List<ProofEntry> window) {
        public QuiescenceProofHead {
            Objects.requireNonNull(binding, "binding");
            folds = List.copyOf(Objects.requireNonNull(folds, "folds"));
            window = List.copyOf(Objects.requireNonNull(window, "window"));
            if (generation <= 0 || folds.size() > MAX_PROOF_FOLDS || window.size() > MAX_PROOF_WINDOW) {
                throw new IllegalArgumentException("proof head generation/count exceeds its hard cap");
            }
            requireOrderedNonoverlappingFolds(folds);
            requireOrderedEntries(window);
            if (!folds.isEmpty()
                    && !window.isEmpty()
                    && folds.get(folds.size() - 1).lastEpoch() >= window.get(0).readAdmissionEpoch()) {
                throw new IllegalArgumentException("proof fold and live window overlap");
            }
        }
    }

    public record SourceProtection(
            BindingIdentity binding,
            SourceProtectionIdentity identity,
            ProtectionState state,
            Optional<Sha256Digest> releasedByBatchSha256,
            Optional<Sha256Digest> releaseProofHeadSha256) {
        public SourceProtection {
            Objects.requireNonNull(binding, "binding");
            Objects.requireNonNull(identity, "identity");
            Objects.requireNonNull(state, "state");
            releasedByBatchSha256 = Objects.requireNonNull(releasedByBatchSha256, "releasedByBatchSha256");
            releaseProofHeadSha256 = Objects.requireNonNull(releaseProofHeadSha256, "releaseProofHeadSha256");
            releasedByBatchSha256.ifPresent(value -> requireDigest(value, "releasedByBatchSha256"));
            releaseProofHeadSha256.ifPresent(value -> requireDigest(value, "releaseProofHeadSha256"));
            boolean releasedFields = releasedByBatchSha256.isPresent() && releaseProofHeadSha256.isPresent();
            if ((state == ProtectionState.RELEASED) != releasedFields) {
                throw new IllegalArgumentException("source-protection state and exact release bindings disagree");
            }
        }

        public SourceProtection release(Sha256Digest batchSha256, Sha256Digest proofHeadSha256) {
            if (state == ProtectionState.RELEASED) {
                return this;
            }
            return new SourceProtection(
                    binding,
                    identity,
                    ProtectionState.RELEASED,
                    Optional.of(batchSha256),
                    Optional.of(proofHeadSha256));
        }
    }

    public static Sha256Digest digest(CanonicalBytes bytes) {
        return Sha256Digest.hash(bytes);
    }

    private static void requireDigest(Sha256Digest digest, String name) {
        Objects.requireNonNull(digest, name);
        if (digest.isZero()) {
            throw new IllegalArgumentException(name + " is zero");
        }
    }

    private static void requireStrictEpochOrder(List<ClosureAnchor> anchors) {
        long previous = 0;
        for (ClosureAnchor anchor : anchors) {
            Objects.requireNonNull(anchor, "anchor");
            if (anchor.closedReadAdmissionEpoch() <= previous) {
                throw new IllegalArgumentException("closure anchors are not strictly epoch ordered");
            }
            previous = anchor.closedReadAdmissionEpoch();
        }
    }

    private static void requireOrderedNonoverlappingFolds(List<ProofFold> folds) {
        long previous = 0;
        for (ProofFold fold : folds) {
            Objects.requireNonNull(fold, "fold");
            if (fold.firstEpoch() <= previous) {
                throw new IllegalArgumentException("proof folds overlap or are not ordered");
            }
            previous = fold.lastEpoch();
        }
    }

    private static void requireOrderedEntries(List<ProofEntry> entries) {
        long previous = 0;
        for (ProofEntry entry : entries) {
            Objects.requireNonNull(entry, "entry");
            if (entry.readAdmissionEpoch() <= previous) {
                throw new IllegalArgumentException("proof window entries are not strictly ordered");
            }
            previous = entry.readAdmissionEpoch();
        }
    }
}
