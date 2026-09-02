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
import com.nereusstream.storage.object.read.control.M4ReadControlRecordsV1.BindingIdentity;
import com.nereusstream.storage.object.read.control.M4ReadControlRecordsV1.BindingReadSelector;
import com.nereusstream.storage.object.read.control.M4ReadControlRecordsV1.CapabilityEvidence;
import com.nereusstream.storage.object.read.control.M4ReadControlRecordsV1.CapabilityKind;
import com.nereusstream.storage.object.read.control.M4ReadControlRecordsV1.CapabilityState;
import com.nereusstream.storage.object.read.control.M4ReadControlRecordsV1.ProofEntry;
import com.nereusstream.storage.object.read.control.M4ReadControlRecordsV1.ProofFold;
import com.nereusstream.storage.object.read.control.M4ReadControlRecordsV1.QuiescenceProofHead;
import com.nereusstream.storage.object.read.control.M4ReadControlRecordsV1.ReadAdmissionEpochTerminalCut;
import com.nereusstream.storage.object.read.control.M4ReadControlRecordsV1.ReadQuiescenceProof;
import com.nereusstream.storage.object.read.control.M4ReadControlRecordsV1.SourceRetirementBatch;
import com.nereusstream.storage.object.read.control.M4ReadControlRecordsV1.TerminalKind;
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

/**
 * Pure M4 terminal/proof/fold cleanup predicate.
 *
 * <p>The planner emits only an exact predecessor/successor head pair and exact immutable rows. A
 * shared scheduler may conditionally install and delete that plan only while the supplied selector
 * and every external reference generation remain fenced. The plan itself is never release or GC
 * authority.
 */
public final class M4ProofCleanupPlannerV1 {
    public static final int MAX_CLEANUP_RECORDS = M4ReadControlRecordsV1.PROOF_FOLD_ENTRIES;
    private static final int MAX_REFERENCE_ROWS = 4_096;

    private M4ProofCleanupPlannerV1() {}

    public record EpochInterval(long firstEpoch, long lastEpoch) {
        public EpochInterval {
            if (firstEpoch <= 0
                    || lastEpoch < firstEpoch
                    || lastEpoch - firstEpoch >= M4ReadControlRecordsV1.MAX_PROOF_INTERVAL_EPOCHS) {
                throw new IllegalArgumentException("cleanup reference interval is outside its hard bound");
            }
        }

        boolean contains(long epoch) {
            return epoch >= firstEpoch && epoch <= lastEpoch;
        }
    }

    /** Exact non-selector references captured under the scheduler's publication fence. */
    public record ReferenceSnapshot(
            BindingIdentity binding,
            long generation,
            Sha256Digest publicationFenceSha256,
            List<EpochInterval> activeIntervals,
            List<Long> recoveryEpochs,
            List<Long> responseLossEpochs,
            List<Long> auditEpochs) {
        public ReferenceSnapshot {
            Objects.requireNonNull(binding, "binding");
            Objects.requireNonNull(publicationFenceSha256, "publicationFenceSha256");
            if (generation <= 0) {
                throw new IllegalArgumentException("cleanup reference generation must be positive");
            }
            activeIntervals = copyIntervals(activeIntervals);
            recoveryEpochs = copyEpochs(recoveryEpochs, "recoveryEpochs");
            responseLossEpochs = copyEpochs(responseLossEpochs, "responseLossEpochs");
            auditEpochs = copyEpochs(auditEpochs, "auditEpochs");
            long total = Math.addExact(
                    Math.addExact(activeIntervals.size(), recoveryEpochs.size()),
                    Math.addExact(responseLossEpochs.size(), auditEpochs.size()));
            if (total > MAX_REFERENCE_ROWS) {
                throw new IllegalArgumentException("cleanup reference snapshot exceeds its hard cap");
            }
        }

        boolean references(long epoch) {
            return activeIntervals.stream().anyMatch(interval -> interval.contains(epoch))
                    || recoveryEpochs.contains(epoch)
                    || responseLossEpochs.contains(epoch)
                    || auditEpochs.contains(epoch);
        }
    }

    public record ExactRows(
            long readAdmissionEpoch,
            CanonicalBytes terminalBytes,
            CanonicalBytes proofBytes,
            Sha256Digest terminalSha256,
            Sha256Digest proofSha256) {
        public ExactRows {
            Objects.requireNonNull(terminalBytes, "terminalBytes");
            Objects.requireNonNull(proofBytes, "proofBytes");
            Objects.requireNonNull(terminalSha256, "terminalSha256");
            Objects.requireNonNull(proofSha256, "proofSha256");
            if (readAdmissionEpoch <= 0
                    || !Sha256Digest.hash(terminalBytes).equals(terminalSha256)
                    || !Sha256Digest.hash(proofBytes).equals(proofSha256)) {
                throw new IllegalArgumentException("cleanup row identity differs from its exact bytes");
            }
        }
    }

    public record CleanupPlan(
            BindingIdentity binding,
            CanonicalBytes selectorBytes,
            ReferenceSnapshot references,
            CanonicalBytes expectedHeadBytes,
            CanonicalBytes successorHeadBytes,
            List<ExactRows> rows) {
        public CleanupPlan {
            Objects.requireNonNull(binding, "binding");
            Objects.requireNonNull(selectorBytes, "selectorBytes");
            Objects.requireNonNull(references, "references");
            Objects.requireNonNull(expectedHeadBytes, "expectedHeadBytes");
            Objects.requireNonNull(successorHeadBytes, "successorHeadBytes");
            rows = List.copyOf(Objects.requireNonNull(rows, "rows"));
            if (rows.isEmpty() || rows.size() > MAX_CLEANUP_RECORDS) {
                throw new IllegalArgumentException("cleanup plan row count is outside its hard cap");
            }
            long previous = 0;
            for (ExactRows row : rows) {
                if (row.readAdmissionEpoch() <= previous) {
                    throw new IllegalArgumentException("cleanup rows are not strictly epoch ordered");
                }
                previous = row.readAdmissionEpoch();
            }
        }
    }

    public static Optional<CleanupPlan> plan(
            BindingReadSelector selector,
            QuiescenceProofHead head,
            List<ReadAdmissionEpochTerminalCut> terminals,
            List<ReadQuiescenceProof> proofs,
            List<CapabilityEvidence> capabilities,
            ReferenceSnapshot references,
            int maximumRecords) {
        Objects.requireNonNull(selector, "selector");
        Objects.requireNonNull(head, "head");
        Objects.requireNonNull(references, "references");
        if (!selector.binding().equals(head.binding())) {
            throw new IllegalArgumentException("cleanup selector/head Binding differs");
        }
        if (!references.binding().equals(head.binding())) {
            throw new IllegalArgumentException("cleanup reference snapshot Binding differs");
        }
        if (maximumRecords <= 0 || maximumRecords > MAX_CLEANUP_RECORDS) {
            throw new IllegalArgumentException("cleanup batch size is outside its hard cap");
        }
        Map<Long, CapabilityEvidence> admitted = admittedCapabilities(head.binding(), capabilities);
        Map<Long, ReadAdmissionEpochTerminalCut> terminalByEpoch = uniqueTerminals(head.binding(), terminals);
        Map<Long, ReadQuiescenceProof> proofByEpoch = uniqueProofs(head.binding(), proofs);

        List<Long> candidates = candidates(selector, head, references, maximumRecords);
        if (candidates.isEmpty()) {
            return Optional.empty();
        }
        List<ExactRows> rows = new ArrayList<>(candidates.size());
        List<ProofEntry> entries = new ArrayList<>(candidates.size());
        for (long epoch : candidates) {
            VerifiedRows verified =
                    verifyRows(head.binding(), epoch, terminalByEpoch.get(epoch), proofByEpoch.get(epoch), admitted);
            rows.add(verified.rows());
            entries.add(verified.entry());
        }

        List<ProofFold> successorFolds = new ArrayList<>(head.folds());
        List<ProofEntry> successorWindow = new ArrayList<>(head.window());
        if (!head.folds().isEmpty()) {
            ProofFold oldest = head.folds().get(0);
            if (candidates.get(0) != oldest.firstEpoch()
                    || candidates.get(candidates.size() - 1) != oldest.lastEpoch()
                    || !oldest.orderedProofsSha256()
                            .equals(orderedProofsSha(entries.stream()
                                    .map(ProofEntry::proofSha256)
                                    .toList()))) {
                throw new IllegalArgumentException("cleanup fold differs from its exact ordered proof rows");
            }
            successorFolds.remove(0);
        } else {
            if (!head.window().subList(0, candidates.size()).equals(entries)) {
                throw new IllegalArgumentException("cleanup window prefix differs from its exact proof rows");
            }
            successorWindow.subList(0, candidates.size()).clear();
        }
        QuiescenceProofHead successor = new QuiescenceProofHead(
                head.binding(), Math.addExact(head.generation(), 1), successorFolds, successorWindow);
        return Optional.of(new CleanupPlan(
                head.binding(),
                M4ReadControlCodecV1.encodeSelector(selector),
                references,
                M4ReadControlCodecV1.encodeHead(head),
                M4ReadControlCodecV1.encodeHead(successor),
                rows));
    }

    private static List<Long> candidates(
            BindingReadSelector selector, QuiescenceProofHead head, ReferenceSnapshot references, int maximumRecords) {
        List<Long> candidates = new ArrayList<>();
        if (!head.folds().isEmpty()) {
            ProofFold oldest = head.folds().get(0);
            long count = Math.addExact(Math.subtractExact(oldest.lastEpoch(), oldest.firstEpoch()), 1);
            if (count > maximumRecords) {
                return List.of();
            }
            for (long epoch = oldest.firstEpoch(); epoch <= oldest.lastEpoch(); epoch++) {
                if (referenced(selector, references, epoch)) {
                    return List.of();
                }
                candidates.add(epoch);
            }
            return List.copyOf(candidates);
        }
        for (ProofEntry entry : head.window()) {
            if (candidates.size() == maximumRecords || referenced(selector, references, entry.readAdmissionEpoch())) {
                break;
            }
            candidates.add(entry.readAdmissionEpoch());
        }
        return List.copyOf(candidates);
    }

    private static boolean referenced(BindingReadSelector selector, ReferenceSnapshot references, long epoch) {
        if (selector.pendingAnchors().stream().anyMatch(anchor -> anchor.closedReadAdmissionEpoch() == epoch)) {
            return true;
        }
        for (SourceRetirementBatch batch : selector.activeBatches()) {
            for (M4ReadControlRecordsV1.SourceProtectionIdentity source : batch.sources()) {
                if (epoch >= source.firstFallbackCapableReadAdmissionEpoch()
                        && epoch <= batch.sharedLastFallbackCapableReadAdmissionEpoch()) {
                    return true;
                }
            }
        }
        return references.references(epoch);
    }

    private static VerifiedRows verifyRows(
            BindingIdentity binding,
            long epoch,
            ReadAdmissionEpochTerminalCut terminal,
            ReadQuiescenceProof proof,
            Map<Long, CapabilityEvidence> admitted) {
        if (terminal == null || proof == null) {
            throw new IllegalArgumentException("cleanup candidate lacks its exact terminal/proof rows");
        }
        CanonicalBytes terminalBytes = M4ReadControlCodecV1.encodeTerminal(terminal);
        CanonicalBytes proofBytes = M4ReadControlCodecV1.encodeProof(proof);
        Sha256Digest terminalSha = Sha256Digest.hash(terminalBytes);
        Sha256Digest proofSha = Sha256Digest.hash(proofBytes);
        if (!terminal.binding().equals(binding)
                || !proof.binding().equals(binding)
                || terminal.readAdmissionEpoch() != epoch
                || proof.readAdmissionEpoch() != epoch
                || !proof.terminalCutSha256().equals(terminalSha)
                || !proof.capability().equals(terminal.capability())
                || proof.kind() != terminal.kind()
                || proof.drainedThroughReadViewGeneration() < terminal.lastAdmittedAndDrainedReadViewGeneration()
                || proof.safeAfterAuthorityTimeMillis() < terminal.safeAfterAuthorityTimeMillis()) {
            throw new IllegalArgumentException("cleanup terminal/proof binding differs");
        }
        CapabilityEvidence capability = admitted.get(proof.capability().generation());
        if (capability == null
                || !M4ReadControlCodecV1.capabilityEvidenceSha256(capability)
                        .equals(proof.capability().evidenceSha256())) {
            throw new IllegalArgumentException("cleanup proof capability is missing, revoked, or mismatched");
        }
        validateTerminalCapability(terminal, capability);
        ProofEntry entry = new ProofEntry(epoch, proofSha, terminalSha, proof.capability());
        return new VerifiedRows(entry, new ExactRows(epoch, terminalBytes, proofBytes, terminalSha, proofSha));
    }

    private static void validateTerminalCapability(
            ReadAdmissionEpochTerminalCut terminal, CapabilityEvidence capability) {
        if (terminal.kind() == TerminalKind.QUALIFIED_EXPIRY) {
            if (capability.kind() != CapabilityKind.AUTHORITY_EXPIRY_V1) {
                throw new IllegalArgumentException("cleanup expiry terminal lacks expiry capability");
            }
            long safeAfter = Math.addExact(
                    Math.addExact(
                            Math.addExact(
                                    terminal.authorityNotAfterMillis(), capability.maximumSourceAccessLifetimeMillis()),
                            capability.maximumClockSkewMillis()),
                    capability.propagationGraceMillis());
            if (terminal.safeAfterAuthorityTimeMillis() != safeAfter
                    || terminal.observedAuthorityTimeMillis() < safeAfter) {
                throw new IllegalArgumentException("cleanup expiry terminal violates its capability time bound");
            }
        }
    }

    private static Map<Long, CapabilityEvidence> admittedCapabilities(
            BindingIdentity binding, List<CapabilityEvidence> capabilities) {
        Objects.requireNonNull(capabilities, "capabilities");
        Map<Long, CapabilityEvidence> result = new LinkedHashMap<>();
        for (CapabilityEvidence capability : capabilities) {
            Objects.requireNonNull(capability, "capability");
            if (!capability.binding().equals(binding) || capability.state() != CapabilityState.ADMITTED) {
                continue;
            }
            if (result.putIfAbsent(capability.generation(), capability) != null) {
                throw new IllegalArgumentException("cleanup capability generation is duplicated");
            }
        }
        return result;
    }

    private static Map<Long, ReadAdmissionEpochTerminalCut> uniqueTerminals(
            BindingIdentity binding, List<ReadAdmissionEpochTerminalCut> terminals) {
        Objects.requireNonNull(terminals, "terminals");
        Map<Long, ReadAdmissionEpochTerminalCut> result = new LinkedHashMap<>();
        for (ReadAdmissionEpochTerminalCut terminal : terminals) {
            Objects.requireNonNull(terminal, "terminal");
            if (!terminal.binding().equals(binding)
                    || result.putIfAbsent(terminal.readAdmissionEpoch(), terminal) != null) {
                throw new IllegalArgumentException("cleanup terminal Binding/epoch inventory differs");
            }
        }
        return result;
    }

    private static Map<Long, ReadQuiescenceProof> uniqueProofs(
            BindingIdentity binding, List<ReadQuiescenceProof> proofs) {
        Objects.requireNonNull(proofs, "proofs");
        Map<Long, ReadQuiescenceProof> result = new LinkedHashMap<>();
        for (ReadQuiescenceProof proof : proofs) {
            Objects.requireNonNull(proof, "proof");
            if (!proof.binding().equals(binding) || result.putIfAbsent(proof.readAdmissionEpoch(), proof) != null) {
                throw new IllegalArgumentException("cleanup proof Binding/epoch inventory differs");
            }
        }
        return result;
    }

    private static List<EpochInterval> copyIntervals(List<EpochInterval> values) {
        List<EpochInterval> result = new ArrayList<>(Objects.requireNonNull(values, "activeIntervals"));
        if (result.stream().anyMatch(Objects::isNull)) {
            throw new IllegalArgumentException("cleanup active interval contains null");
        }
        result.sort(Comparator.comparingLong(EpochInterval::firstEpoch).thenComparingLong(EpochInterval::lastEpoch));
        if (result.stream().distinct().count() != result.size()) {
            throw new IllegalArgumentException("cleanup active interval is duplicated");
        }
        return List.copyOf(result);
    }

    private static List<Long> copyEpochs(List<Long> values, String name) {
        List<Long> result = new ArrayList<>(Objects.requireNonNull(values, name));
        if (result.stream().anyMatch(value -> value == null || value <= 0)) {
            throw new IllegalArgumentException("cleanup " + name + " contains an invalid epoch");
        }
        result.sort(Long::compareTo);
        if (result.stream().distinct().count() != result.size()) {
            throw new IllegalArgumentException("cleanup " + name + " contains a duplicate epoch");
        }
        return List.copyOf(result);
    }

    private static Sha256Digest orderedProofsSha(List<Sha256Digest> proofs) {
        try {
            ByteArrayOutputStream bytes = new ByteArrayOutputStream();
            try (DataOutputStream output = new DataOutputStream(bytes)) {
                output.writeUTF("M4-ORDERED-PROOF-FOLD-V1");
                output.writeInt(proofs.size());
                for (Sha256Digest digest : proofs) {
                    output.write(digest.bytes().toByteArray());
                }
            }
            return Sha256Digest.hash(CanonicalBytes.copyOf(bytes.toByteArray()));
        } catch (IOException impossible) {
            throw new IllegalStateException("in-memory M4 cleanup digest encoding failed", impossible);
        }
    }

    private record VerifiedRows(ProofEntry entry, ExactRows rows) {}
}
