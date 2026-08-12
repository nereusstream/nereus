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

package com.nereusstream.domain.receipt;

import com.nereusstream.domain.receipt.M1FinalIndexV1.FinalRejectedException;
import com.nereusstream.domain.receipt.M1FinalIndexV1.GateId;
import com.nereusstream.domain.receipt.M1FinalIndexV1.GateOutcome;
import com.nereusstream.domain.receipt.M1FinalIndexV1.RejectionCode;
import java.io.IOException;
import java.nio.file.Files;
import java.nio.file.LinkOption;
import java.nio.file.Path;
import java.nio.file.attribute.BasicFileAttributes;
import java.util.ArrayList;
import java.util.Comparator;
import java.util.EnumSet;
import java.util.HashMap;
import java.util.HashSet;
import java.util.List;
import java.util.Map;
import java.util.Objects;
import java.util.Set;

/** Resolves one Final index without rerunning any referenced gate or suite. */
public final class M1FinalResolverV1 {
    private M1FinalResolverV1() {}

    public record ScenarioRequirement(
            String scenarioId, VirtualLedgerReceiptV1.ReceiptKind receiptKind, Set<String> requiredSuiteIds) {
        public ScenarioRequirement {
            Objects.requireNonNull(scenarioId, "scenarioId");
            Objects.requireNonNull(receiptKind, "receiptKind");
            requiredSuiteIds = Set.copyOf(requiredSuiteIds);
            if (requiredSuiteIds.isEmpty()) {
                throw new IllegalArgumentException("each promoted scenario requires at least one named suite");
            }
        }
    }

    public record PromotionPolicy(List<ScenarioRequirement> scenarios) {
        public PromotionPolicy {
            scenarios = List.copyOf(scenarios);
            if (scenarios.isEmpty()) {
                throw new IllegalArgumentException("promotion policy is empty");
            }
            String previous = null;
            for (ScenarioRequirement scenario : scenarios) {
                if (previous != null && previous.compareTo(scenario.scenarioId()) >= 0) {
                    throw new IllegalArgumentException("promotion-policy scenarios must be sorted and unique");
                }
                previous = scenario.scenarioId();
            }
        }
    }

    public record Resolution(
            VirtualLedgerReceiptV1.SourceTuple sourceTuple,
            String sourceTupleSha,
            Set<GateId> passedGates,
            Set<String> passedScenarios,
            List<String> receiptPaths) {
        public Resolution {
            passedGates = Set.copyOf(passedGates);
            passedScenarios = Set.copyOf(passedScenarios);
            receiptPaths = List.copyOf(receiptPaths);
        }
    }

    public static Resolution resolve(Path finalIndexFile, PromotionPolicy policy) {
        Objects.requireNonNull(finalIndexFile, "finalIndexFile");
        Objects.requireNonNull(policy, "policy");
        M1FinalIndexV1.Index index = M1FinalIndexV1.parseCanonicalFile(finalIndexFile);
        Path root = verifiedRoot(finalIndexFile.toAbsolutePath().getParent());

        EnumSet<GateId> requiredGates = EnumSet.allOf(GateId.class);
        EnumSet<GateId> passedGates = EnumSet.noneOf(GateId.class);
        for (M1FinalIndexV1.GateRef reference : index.requiredGateRefs()) {
            byte[] bytes = readReference(root, reference.path(), reference.length(), reference.sha256());
            M1FinalIndexV1.GateResult result = M1FinalIndexV1.parseCanonicalGateResult(bytes);
            if (result.gateId() != reference.gateId()) {
                throw reject(RejectionCode.FINAL_REFERENCE_INVALID, "gate reference and gate result differ");
            }
            if (!result.sourceTupleSha().equals(index.sourceTupleSha())) {
                throw reject(RejectionCode.FINAL_RECEIPT_SOURCE_TUPLE_MISMATCH, "gate source tuple differs");
            }
            if (result.outcome() != GateOutcome.PASS) {
                throw reject(RejectionCode.FINAL_GATE_NOT_PASS, "required gate did not pass: " + result.gateId());
            }
            passedGates.add(result.gateId());
        }
        if (!passedGates.equals(requiredGates)) {
            throw reject(RejectionCode.FINAL_REQUIRED_GATE_MISSING, "Final index lacks an exact required gate");
        }

        Map<String, ScenarioRequirement> requirements = new HashMap<>();
        for (ScenarioRequirement row : policy.scenarios()) {
            requirements.put(row.scenarioId(), row);
        }
        Set<String> seenScenarios = new HashSet<>();
        List<String> receiptPaths = new ArrayList<>();
        VirtualLedgerReceiptV1.SourceTuple commonSource = null;

        for (M1FinalIndexV1.ReceiptRef reference : index.receiptRefs()) {
            Path receiptPath = resolvePath(root, reference.path());
            byte[] bytes = readReference(root, reference.path(), reference.length(), reference.sha256());
            VirtualLedgerReceiptV1.ReceiptRoot receipt = VirtualLedgerReceiptV1.parseCanonical(bytes);
            if (receipt.kind() != reference.kind()) {
                throw reject(RejectionCode.FINAL_RECEIPT_KIND_MISMATCH, "receipt kind and typed reference differ");
            }
            String tupleSha = VirtualLedgerReceiptV1.sourceTupleSha256(receipt.sourceTuple());
            if (!tupleSha.equals(index.sourceTupleSha())) {
                throw reject(RejectionCode.FINAL_RECEIPT_SOURCE_TUPLE_MISMATCH, "receipt source tuple differs");
            }
            if (commonSource == null) {
                commonSource = receipt.sourceTuple();
            } else if (!commonSource.equals(receipt.sourceTuple())) {
                throw reject(
                        RejectionCode.FINAL_RECEIPT_SOURCE_TUPLE_MISMATCH, "receipts bind different source tuples");
            }

            for (VirtualLedgerReceiptV1.ScenarioResult scenario : receipt.scenarios()) {
                ScenarioRequirement requirement = requirements.get(scenario.scenarioId());
                if (requirement == null) {
                    throw reject(
                            RejectionCode.FINAL_RECEIPT_SET_INCOMPLETE, "receipt contains an unrequested scenario");
                }
                if (requirement.receiptKind() != receipt.kind()) {
                    throw reject(
                            RejectionCode.FINAL_RECEIPT_KIND_MISMATCH, "scenario requires a different receipt kind");
                }
                if (!seenScenarios.add(scenario.scenarioId())) {
                    throw reject(
                            RejectionCode.FINAL_RECEIPT_SCENARIO_DUPLICATE, "scenario appears in multiple receipts");
                }
                Set<String> actualSuites = new HashSet<>();
                for (VirtualLedgerReceiptV1.SuiteResult suite : scenario.suites()) {
                    actualSuites.add(suite.suiteId());
                }
                if (!actualSuites.containsAll(requirement.requiredSuiteIds())) {
                    throw reject(
                            RejectionCode.FINAL_RECEIPT_SET_INCOMPLETE,
                            "scenario lacks a required suite: " + scenario.scenarioId());
                }
            }
            VirtualLedgerReceiptV1.requireMandatoryPass(receipt, Set.of());
            VirtualLedgerReceiptV1.verifyAttachments(receiptPath.getParent(), receipt);
            receiptPaths.add(reference.path());
        }

        if (!seenScenarios.equals(requirements.keySet()) || commonSource == null) {
            throw reject(RejectionCode.FINAL_RECEIPT_SET_INCOMPLETE, "Final receipt set does not cover exact policy");
        }
        receiptPaths.sort(Comparator.naturalOrder());
        return new Resolution(commonSource, index.sourceTupleSha(), passedGates, seenScenarios, receiptPaths);
    }

    private static Path verifiedRoot(Path root) {
        if (root == null || Files.isSymbolicLink(root)) {
            throw reject(RejectionCode.FINAL_ROOT_NOT_REGULAR, "Final index directory is missing or a symlink");
        }
        try {
            Path real = root.toRealPath(LinkOption.NOFOLLOW_LINKS);
            BasicFileAttributes attributes =
                    Files.readAttributes(real, BasicFileAttributes.class, LinkOption.NOFOLLOW_LINKS);
            if (!attributes.isDirectory() || attributes.isSymbolicLink()) {
                throw reject(RejectionCode.FINAL_ROOT_NOT_REGULAR, "Final index root is not a directory");
            }
            return real;
        } catch (IOException error) {
            throw new FinalRejectedException(RejectionCode.FINAL_ROOT_NOT_REGULAR, "cannot resolve Final root", error);
        }
    }

    private static byte[] readReference(Path root, String relative, long expectedLength, String expectedSha) {
        try {
            return VirtualLedgerReceiptV1.readVerifiedFile(root, relative, expectedLength, expectedSha);
        } catch (VirtualLedgerReceiptV1.ReceiptRejectedException error) {
            RejectionCode code =
                    switch (error.code()) {
                        case RECEIPT_ATTACHMENT_LENGTH_MISMATCH -> RejectionCode.FINAL_REFERENCE_LENGTH_MISMATCH;
                        case RECEIPT_ATTACHMENT_DIGEST_MISMATCH -> RejectionCode.FINAL_REFERENCE_DIGEST_MISMATCH;
                        case RECEIPT_PATH_INVALID, RECEIPT_PATH_BYTES_EXCEEDED, RECEIPT_PATH_SEGMENTS_EXCEEDED ->
                            RejectionCode.FINAL_REFERENCE_INVALID;
                        default -> RejectionCode.FINAL_REFERENCE_NOT_REGULAR;
                    };
            throw new FinalRejectedException(code, "referenced evidence failed secure verification", error);
        }
    }

    private static Path resolvePath(Path root, String relative) {
        List<String> segments;
        try {
            segments = VirtualLedgerReceiptV1.validatePath(relative);
        } catch (VirtualLedgerReceiptV1.ReceiptRejectedException error) {
            throw new FinalRejectedException(RejectionCode.FINAL_REFERENCE_INVALID, "invalid reference path", error);
        }
        Path current = root;
        for (int index = 0; index < segments.size(); index++) {
            current = current.resolve(segments.get(index));
            BasicFileAttributes attributes = attributes(current);
            if (attributes.isSymbolicLink() || Files.isSymbolicLink(current)) {
                throw reject(RejectionCode.FINAL_REFERENCE_NOT_REGULAR, "reference path contains a symlink");
            }
            if (index + 1 < segments.size() && !attributes.isDirectory()) {
                throw reject(RejectionCode.FINAL_REFERENCE_NOT_REGULAR, "reference ancestor is not a directory");
            }
        }
        Path normalized = current.normalize();
        if (!normalized.startsWith(root)) {
            throw reject(RejectionCode.FINAL_REFERENCE_INVALID, "reference leaves Final root");
        }
        return normalized;
    }

    private static BasicFileAttributes attributes(Path path) {
        try {
            return Files.readAttributes(path, BasicFileAttributes.class, LinkOption.NOFOLLOW_LINKS);
        } catch (IOException error) {
            throw new FinalRejectedException(
                    RejectionCode.FINAL_REFERENCE_NOT_REGULAR, "cannot read reference attributes", error);
        }
    }

    private static FinalRejectedException reject(RejectionCode code, String detail) {
        return new FinalRejectedException(code, detail);
    }
}
