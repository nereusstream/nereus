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

package com.nereusstream.kafka.bookkeeper.evidence;

import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.assertThatThrownBy;
import com.nereusstream.kafka.bookkeeper.evidence.KafkaM2FinalReceiptV1.AttachmentKind;
import com.nereusstream.kafka.bookkeeper.evidence.KafkaM2FinalReceiptV1.AttachmentRef;
import com.nereusstream.kafka.bookkeeper.evidence.KafkaM2FinalReceiptV1.Receipt;
import com.nereusstream.kafka.bookkeeper.evidence.KafkaM2FinalReceiptV1.ReceiptKind;
import com.nereusstream.kafka.bookkeeper.evidence.KafkaM2FinalReceiptV1.ReceiptRejectedException;
import com.nereusstream.kafka.bookkeeper.evidence.KafkaM2FinalReceiptV1.ReceiptResult;
import com.nereusstream.kafka.bookkeeper.evidence.KafkaM2FinalReceiptV1.RejectionCode;
import com.nereusstream.kafka.bookkeeper.evidence.KafkaM2FinalReceiptV1.ScenarioResult;
import com.nereusstream.kafka.bookkeeper.evidence.KafkaM2FinalReceiptV1.SourceTuple;
import com.nereusstream.kafka.bookkeeper.evidence.KafkaM2FinalReceiptV1.SuiteResult;
import java.nio.charset.StandardCharsets;
import java.nio.file.Files;
import java.nio.file.Path;
import java.security.MessageDigest;
import java.util.ArrayList;
import java.util.Comparator;
import java.util.HashMap;
import java.util.HashSet;
import java.util.List;
import java.util.Map;
import java.util.Set;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.io.TempDir;

class KafkaM2FinalResolverV1Test {
    @TempDir
    Path temporaryDirectory;

    @Test
    void canonicalRoundTripAndExactPolicyResolve() throws Exception {
        Fixture fixture = fixture();
        byte[] canonical = KafkaM2FinalReceiptV1.canonicalBytes(fixture.receipt());
        assertThat(KafkaM2FinalReceiptV1.parseCanonical(canonical)).isEqualTo(fixture.receipt());

        Path receiptFile = writeReceipt(fixture.receipt());
        KafkaM2FinalResolverV1.Resolution resolution = KafkaM2FinalResolverV1.resolve(temporaryDirectory, receiptFile);

        assertThat(resolution.promotedScenarios())
                .containsExactlyInAnyOrderElementsOf(KafkaM2PromotionPolicyV1.policy().stream()
                        .map(KafkaM2PromotionPolicyV1.ScenarioRequirement::scenarioId)
                        .toList());
        assertThat(resolution.scenarioSuiteReferences()).isEqualTo(40);
        assertThat(resolution.uniqueAttachments())
                .isEqualTo(fixture.receipt().attachments().size());
    }

    @Test
    void rejectsNonCanonicalBytesAndNonPassSuite() throws Exception {
        Fixture fixture = fixture();
        byte[] canonical = KafkaM2FinalReceiptV1.canonicalBytes(fixture.receipt());
        byte[] withNewline = (new String(canonical, StandardCharsets.UTF_8) + "\n").getBytes(StandardCharsets.UTF_8);
        assertRejected(
                () -> KafkaM2FinalReceiptV1.parseCanonical(withNewline), RejectionCode.MALFORMED_OR_NON_CANONICAL);

        ScenarioResult first = fixture.receipt().scenarios().get(0);
        List<SuiteResult> changedSuites = new ArrayList<>(first.suites());
        SuiteResult original = changedSuites.get(0);
        changedSuites.set(0, new SuiteResult(original.attachmentPath(), 0, 0, 1, original.suiteId(), original.tests()));
        Receipt changed = replaceScenario(fixture.receipt(), 0, new ScenarioResult(first.scenarioId(), changedSuites));
        assertRejected(() -> KafkaM2FinalReceiptV1.canonicalBytes(changed), RejectionCode.MANDATORY_RESULT_NOT_PASS);
    }

    @Test
    void rejectsMissingOrUnownedScenarioAndSuite() throws Exception {
        Fixture fixture = fixture();
        ScenarioResult first = fixture.receipt().scenarios().get(0);
        SuiteResult removedSuite = first.suites().get(first.suites().size() - 1);
        ScenarioResult missingSuite = new ScenarioResult(
                first.scenarioId(), first.suites().subList(0, first.suites().size() - 1));
        Receipt missing = replaceScenario(fixture.receipt(), 0, missingSuite);
        missing = removeAttachmentIfUnused(missing, removedSuite.attachmentPath());
        Path missingFile = writeReceipt(missing);
        assertRejected(
                () -> KafkaM2FinalResolverV1.resolve(temporaryDirectory, missingFile), RejectionCode.SUITE_SET_INVALID);

        List<ScenarioResult> scenarios = new ArrayList<>(fixture.receipt().scenarios());
        scenarios.set(0, new ScenarioResult("V2-BK-001", scenarios.get(0).suites()));
        Receipt unowned = new Receipt(
                fixture.receipt().attachments(),
                fixture.receipt().kind(),
                true,
                fixture.receipt().result(),
                scenarios,
                fixture.receipt().schema(),
                fixture.receipt().sourceTuple());
        Path unownedFile = writeReceipt(unowned);
        assertRejected(
                () -> KafkaM2FinalResolverV1.resolve(temporaryDirectory, unownedFile),
                RejectionCode.SCENARIO_SET_INVALID);
    }

    @Test
    void rejectsDuplicateAndUnsortedIds() throws Exception {
        Fixture fixture = fixture();
        List<ScenarioResult> scenarios = new ArrayList<>(fixture.receipt().scenarios());
        scenarios.set(
                1,
                new ScenarioResult(
                        scenarios.get(0).scenarioId(), scenarios.get(1).suites()));
        Receipt duplicateScenario = copy(
                fixture.receipt(),
                fixture.receipt().attachments(),
                scenarios,
                fixture.receipt().sourceTuple());
        assertRejected(
                () -> KafkaM2FinalReceiptV1.canonicalBytes(duplicateScenario), RejectionCode.SCENARIO_SET_INVALID);

        ScenarioResult first = fixture.receipt().scenarios().get(0);
        List<SuiteResult> suites = new ArrayList<>(first.suites());
        SuiteResult swap = suites.get(0);
        suites.set(0, suites.get(1));
        suites.set(1, swap);
        Receipt unsortedSuite = replaceScenario(fixture.receipt(), 0, new ScenarioResult(first.scenarioId(), suites));
        assertRejected(() -> KafkaM2FinalReceiptV1.canonicalBytes(unsortedSuite), RejectionCode.SUITE_SET_INVALID);
    }

    @Test
    void rejectsTamperedAndSymlinkAttachments() throws Exception {
        Fixture fixture = fixture();
        Path tamperedReceiptFile = writeReceipt(fixture.receipt());
        AttachmentRef target = fixture.receipt().attachments().stream()
                .filter(row -> row.kind() == AttachmentKind.LOCAL_JUNIT_REPORT)
                .findFirst()
                .orElseThrow();
        Path targetFile = temporaryDirectory.resolve(target.path());
        Files.writeString(targetFile, "tampered");
        assertRejected(
                () -> KafkaM2FinalResolverV1.resolve(temporaryDirectory, tamperedReceiptFile),
                RejectionCode.ATTACHMENT_LENGTH_MISMATCH);

        fixture = fixture();
        Path symlinkReceiptFile = writeReceipt(fixture.receipt());
        target = fixture.receipt().attachments().stream()
                .filter(row -> row.kind() == AttachmentKind.LOCAL_JUNIT_REPORT)
                .findFirst()
                .orElseThrow();
        targetFile = temporaryDirectory.resolve(target.path());
        Path regular = targetFile.resolveSibling("regular-target.xml");
        Files.write(regular, Files.readAllBytes(targetFile));
        Files.delete(targetFile);
        Files.createSymbolicLink(targetFile, regular.getFileName());
        Path finalReceiptFile = symlinkReceiptFile;
        assertRejected(
                () -> KafkaM2FinalResolverV1.resolve(temporaryDirectory, finalReceiptFile),
                RejectionCode.ATTACHMENT_NOT_REGULAR);
    }

    @Test
    void rejectsWrongPrerequisiteBindingAndUnreferencedAttachment() throws Exception {
        Fixture fixture = fixture();
        SourceTuple source = fixture.receipt().sourceTuple();
        SourceTuple wrong = new SourceTuple(
                source.bookKeeperSourceCommit(),
                "f".repeat(64),
                source.kafkaForkCommit(),
                source.kafkaInputsReceiptSha256(),
                source.nereusCommit(),
                source.sourceLocksSha256());
        Receipt wrongBinding = copy(
                fixture.receipt(),
                fixture.receipt().attachments(),
                fixture.receipt().scenarios(),
                wrong);
        assertRejected(
                () -> KafkaM2FinalReceiptV1.canonicalBytes(wrongBinding), RejectionCode.PREREQUISITE_BINDING_INVALID);

        byte[] extraBytes = "extra".getBytes(StandardCharsets.UTF_8);
        String extraPath = "evidence/unreferenced.json";
        write(extraPath, extraBytes);
        List<AttachmentRef> attachments = new ArrayList<>(fixture.receipt().attachments());
        attachments.add(
                new AttachmentRef(AttachmentKind.LOCAL_JUNIT_REPORT, extraPath, extraBytes.length, sha(extraBytes)));
        attachments.sort(Comparator.comparing(AttachmentRef::path));
        Receipt extra = copy(fixture.receipt(), attachments, fixture.receipt().scenarios(), source);
        assertRejected(() -> KafkaM2FinalReceiptV1.canonicalBytes(extra), RejectionCode.ATTACHMENT_SET_INVALID);
    }

    @Test
    void rejectsPathEscapeAndAttachmentCaps() throws Exception {
        Fixture fixture = fixture();
        List<AttachmentRef> attachments = new ArrayList<>(fixture.receipt().attachments());
        AttachmentRef first = attachments.get(0);
        attachments.set(0, new AttachmentRef(first.kind(), "../escape", first.bytes(), first.sha256()));
        Receipt escaped = copy(
                fixture.receipt(),
                attachments,
                fixture.receipt().scenarios(),
                fixture.receipt().sourceTuple());
        assertRejected(() -> KafkaM2FinalReceiptV1.canonicalBytes(escaped), RejectionCode.PATH_INVALID);

        attachments = new ArrayList<>(fixture.receipt().attachments());
        first = attachments.get(0);
        attachments.set(
                0,
                new AttachmentRef(
                        first.kind(),
                        first.path(),
                        KafkaM2FinalReceiptV1.MAX_SINGLE_ATTACHMENT_BYTES + 1L,
                        first.sha256()));
        Receipt oversized = copy(
                fixture.receipt(),
                attachments,
                fixture.receipt().scenarios(),
                fixture.receipt().sourceTuple());
        assertRejected(() -> KafkaM2FinalReceiptV1.canonicalBytes(oversized), RejectionCode.ATTACHMENT_BYTES_EXCEEDED);
    }

    @Test
    void canonicalRootRejectsSymlinkAndOversize() throws Exception {
        Fixture fixture = fixture();
        Path receiptFile = writeReceipt(fixture.receipt());
        Path link = temporaryDirectory.resolve("receipt-link.json");
        Files.createSymbolicLink(link, receiptFile.getFileName());
        assertRejected(() -> KafkaM2FinalReceiptV1.parseCanonicalFile(link), RejectionCode.ROOT_NOT_REGULAR);

        Path oversized = temporaryDirectory.resolve("oversized.json");
        Files.write(oversized, new byte[KafkaM2FinalReceiptV1.MAX_CANONICAL_BYTES + 1]);
        assertRejected(() -> KafkaM2FinalReceiptV1.parseCanonicalFile(oversized), RejectionCode.ROOT_BYTES_EXCEEDED);
    }

    @Test
    void policyContainsOnlyTheTenExactlyM2Scenarios() {
        List<String> actual = KafkaM2PromotionPolicyV1.policy().stream()
                .map(KafkaM2PromotionPolicyV1.ScenarioRequirement::scenarioId)
                .toList();
        assertThat(actual)
                .containsExactly(
                        "V2-BK-003",
                        "V2-BK-014",
                        "V2-BK-015",
                        "V2-BK-016",
                        "V2-BK-017",
                        "V2-KAF-DATA-001",
                        "V2-KAF-DATA-002",
                        "V2-KAF-DATA-004",
                        "V2-KAF-DATA-005",
                        "V2-KAF-DATA-014")
                .doesNotContain("V2-KAF-DATA-003", "V2-KAF-DATA-006", "V2-BK-013");
    }

    private Fixture fixture() throws Exception {
        Map<String, String> suitePaths = new HashMap<>();
        Set<String> uniqueSuites = new HashSet<>();
        for (KafkaM2PromotionPolicyV1.ScenarioRequirement scenario : KafkaM2PromotionPolicyV1.policy()) {
            uniqueSuites.addAll(scenario.requiredSuiteIds());
        }
        List<String> sortedSuites = uniqueSuites.stream().sorted().toList();
        List<AttachmentRef> attachments = new ArrayList<>();
        for (int index = 0; index < sortedSuites.size(); index++) {
            String suite = sortedSuites.get(index);
            String path = "evidence/suite-" + String.format("%02d", index) + ".xml";
            byte[] bytes = ("suite=" + suite).getBytes(StandardCharsets.UTF_8);
            write(path, bytes);
            AttachmentKind kind = suite.startsWith("nereus.kafka.m2.k9.scale.")
                    ? AttachmentKind.SCALE_RESULT
                    : suite.startsWith("org.apache.kafka.")
                            ? AttachmentKind.EXACT_KAFKA_RESULT
                            : suite.contains("RealBookKeeper")
                                    ? AttachmentKind.REAL_BOOKKEEPER_JUNIT_REPORT
                                    : AttachmentKind.LOCAL_JUNIT_REPORT;
            attachments.add(new AttachmentRef(kind, path, bytes.length, sha(bytes)));
            suitePaths.put(suite, path);
        }

        byte[] k0Bytes = "{\"result\":\"PASS_KAFKA_M2_INPUTS_ONLY\"}".getBytes(StandardCharsets.UTF_8);
        byte[] k9Bytes = "{\"result\":\"PASS_KAFKA_M2_K9_REAL_BOOKKEEPER_EVIDENCE\"}".getBytes(StandardCharsets.UTF_8);
        write(KafkaM2FinalReceiptV1.K0_INPUTS_PATH, k0Bytes);
        write(KafkaM2FinalReceiptV1.K9_EVIDENCE_PATH, k9Bytes);
        attachments.add(new AttachmentRef(
                AttachmentKind.K0_INPUTS_RECEIPT, KafkaM2FinalReceiptV1.K0_INPUTS_PATH, k0Bytes.length, sha(k0Bytes)));
        attachments.add(new AttachmentRef(
                AttachmentKind.K9_EVIDENCE_RECEIPT,
                KafkaM2FinalReceiptV1.K9_EVIDENCE_PATH,
                k9Bytes.length,
                sha(k9Bytes)));
        attachments.sort(Comparator.comparing(AttachmentRef::path));

        List<ScenarioResult> scenarios = new ArrayList<>();
        for (KafkaM2PromotionPolicyV1.ScenarioRequirement requirement : KafkaM2PromotionPolicyV1.policy()) {
            List<SuiteResult> suites = requirement.requiredSuiteIds().stream()
                    .sorted()
                    .map(suite -> new SuiteResult(suitePaths.get(suite), 0, 0, 0, suite, 1))
                    .toList();
            scenarios.add(new ScenarioResult(requirement.scenarioId(), suites));
        }
        SourceTuple source = new SourceTuple(
                "c".repeat(40), sha(k9Bytes), "b".repeat(40), sha(k0Bytes), "a".repeat(40), "d".repeat(64));
        return new Fixture(new Receipt(
                attachments,
                ReceiptKind.KAFKA_M2_FINAL,
                true,
                ReceiptResult.PASS_KAFKA_M2_FINAL,
                scenarios,
                KafkaM2FinalReceiptV1.SCHEMA,
                source));
    }

    private Path writeReceipt(Receipt receipt) throws Exception {
        Path path = temporaryDirectory.resolve("kafka-final.json");
        Files.write(path, KafkaM2FinalReceiptV1.canonicalBytes(receipt));
        return path;
    }

    private void write(String relative, byte[] bytes) throws Exception {
        Path path = temporaryDirectory.resolve(relative);
        Files.createDirectories(path.getParent());
        Files.write(path, bytes);
    }

    private static String sha(byte[] bytes) throws Exception {
        return java.util.HexFormat.of()
                .formatHex(MessageDigest.getInstance("SHA-256").digest(bytes));
    }

    private static Receipt replaceScenario(Receipt receipt, int index, ScenarioResult scenario) {
        List<ScenarioResult> scenarios = new ArrayList<>(receipt.scenarios());
        scenarios.set(index, scenario);
        return copy(receipt, receipt.attachments(), scenarios, receipt.sourceTuple());
    }

    private static Receipt removeAttachmentIfUnused(Receipt receipt, String path) {
        boolean stillUsed = receipt.scenarios().stream()
                .flatMap(scenario -> scenario.suites().stream())
                .anyMatch(suite -> suite.attachmentPath().equals(path));
        if (stillUsed) {
            return receipt;
        }
        return copy(
                receipt,
                receipt.attachments().stream()
                        .filter(row -> !row.path().equals(path))
                        .toList(),
                receipt.scenarios(),
                receipt.sourceTuple());
    }

    private static Receipt copy(
            Receipt receipt, List<AttachmentRef> attachments, List<ScenarioResult> scenarios, SourceTuple source) {
        return new Receipt(
                attachments,
                receipt.kind(),
                receipt.promotionEligible(),
                receipt.result(),
                scenarios,
                receipt.schema(),
                source);
    }

    private static void assertRejected(ThrowingAction action, RejectionCode code) {
        assertThatThrownBy(action::run)
                .isInstanceOfSatisfying(ReceiptRejectedException.class, rejection -> assertThat(rejection.code())
                        .isEqualTo(code));
    }

    @FunctionalInterface
    private interface ThrowingAction {
        void run() throws Exception;
    }

    private record Fixture(Receipt receipt) {}
}
