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

package com.nereusstream.pulsar.offload.evidence;

import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.assertThatThrownBy;
import com.nereusstream.pulsar.offload.evidence.PulsarM2FinalReceiptV1.AttachmentKind;
import com.nereusstream.pulsar.offload.evidence.PulsarM2FinalReceiptV1.AttachmentRef;
import com.nereusstream.pulsar.offload.evidence.PulsarM2FinalReceiptV1.Receipt;
import com.nereusstream.pulsar.offload.evidence.PulsarM2FinalReceiptV1.ReceiptKind;
import com.nereusstream.pulsar.offload.evidence.PulsarM2FinalReceiptV1.ReceiptRejectedException;
import com.nereusstream.pulsar.offload.evidence.PulsarM2FinalReceiptV1.ReceiptResult;
import com.nereusstream.pulsar.offload.evidence.PulsarM2FinalReceiptV1.RejectionCode;
import com.nereusstream.pulsar.offload.evidence.PulsarM2FinalReceiptV1.ScenarioResult;
import com.nereusstream.pulsar.offload.evidence.PulsarM2FinalReceiptV1.SourceTuple;
import com.nereusstream.pulsar.offload.evidence.PulsarM2FinalReceiptV1.SuiteResult;
import java.nio.charset.StandardCharsets;
import java.nio.file.Files;
import java.nio.file.Path;
import java.security.MessageDigest;
import java.util.ArrayList;
import java.util.Comparator;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.io.TempDir;

class PulsarM2FinalResolverV1Test {
    private static final String LOCAL_PATH = "evidence/pulsar-local.json";
    private static final String NATIVE_PATH = "evidence/pulsar-native.json";
    private static final String PROVIDER_PATH = "evidence/pulsar-provider.json";
    private static final String CANDIDATE_PATH = "docs/v2/evidence/v2-m2/pulsar/p6/candidate-matrix.json";
    private static final String NATIVE_BASELINE_PATH = "docs/v2/evidence/v2-m2/pulsar/p6/native-baseline.json";
    private static final String MINIO_PATH = "docs/v2/evidence/v2-m2/pulsar/p6/minio-provider.json";

    @TempDir
    Path temporaryDirectory;

    @Test
    void canonicalRoundTripAndExactPolicyResolve() throws Exception {
        Fixture fixture = fixture();
        byte[] canonical = PulsarM2FinalReceiptV1.canonicalBytes(fixture.receipt());
        assertThat(PulsarM2FinalReceiptV1.parseCanonical(canonical)).isEqualTo(fixture.receipt());

        PulsarM2FinalResolverV1.Resolution resolution =
                PulsarM2FinalResolverV1.resolve(temporaryDirectory, writeReceipt(fixture.receipt()));

        assertThat(resolution.promotedScenarios())
                .containsExactlyInAnyOrderElementsOf(PulsarM2PromotionPolicyV1.policy().stream()
                        .map(PulsarM2PromotionPolicyV1.ScenarioRequirement::scenarioId)
                        .toList());
        assertThat(resolution.scenarioSuiteReferences()).isEqualTo(32);
        assertThat(resolution.uniqueAttachments()).isEqualTo(8);
    }

    @Test
    void rejectsNonCanonicalBytesAndNonPassSuite() throws Exception {
        Fixture fixture = fixture();
        byte[] canonical = PulsarM2FinalReceiptV1.canonicalBytes(fixture.receipt());
        byte[] withNewline = (new String(canonical, StandardCharsets.UTF_8) + "\n").getBytes(StandardCharsets.UTF_8);
        assertRejected(
                () -> PulsarM2FinalReceiptV1.parseCanonical(withNewline), RejectionCode.MALFORMED_OR_NON_CANONICAL);

        ScenarioResult first = fixture.receipt().scenarios().get(0);
        List<SuiteResult> changedSuites = new ArrayList<>(first.suites());
        SuiteResult original = changedSuites.get(0);
        changedSuites.set(0, new SuiteResult(original.attachmentPath(), 0, 0, 1, original.suiteId(), original.tests()));
        Receipt changed = replaceScenario(fixture.receipt(), 0, new ScenarioResult(first.scenarioId(), changedSuites));
        assertRejected(() -> PulsarM2FinalReceiptV1.canonicalBytes(changed), RejectionCode.MANDATORY_RESULT_NOT_PASS);
    }

    @Test
    void rejectsMissingOrUnownedScenarioAndSuite() throws Exception {
        Fixture fixture = fixture();
        ScenarioResult first = fixture.receipt().scenarios().get(0);
        ScenarioResult missingSuite = new ScenarioResult(
                first.scenarioId(), first.suites().subList(0, first.suites().size() - 1));
        Path missingFile = writeReceipt(replaceScenario(fixture.receipt(), 0, missingSuite));
        assertRejected(
                () -> PulsarM2FinalResolverV1.resolve(temporaryDirectory, missingFile),
                RejectionCode.SUITE_SET_INVALID);

        List<ScenarioResult> scenarios = new ArrayList<>(fixture.receipt().scenarios());
        scenarios.set(0, new ScenarioResult("V2-BK-003", scenarios.get(0).suites()));
        Receipt unowned = copy(
                fixture.receipt(),
                fixture.receipt().attachments(),
                scenarios,
                fixture.receipt().sourceTuple());
        assertRejected(
                () -> PulsarM2FinalResolverV1.resolve(temporaryDirectory, writeReceipt(unowned)),
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
        Receipt duplicate = copy(
                fixture.receipt(),
                fixture.receipt().attachments(),
                scenarios,
                fixture.receipt().sourceTuple());
        assertRejected(() -> PulsarM2FinalReceiptV1.canonicalBytes(duplicate), RejectionCode.SCENARIO_SET_INVALID);

        ScenarioResult first = fixture.receipt().scenarios().get(0);
        List<SuiteResult> suites = new ArrayList<>(first.suites());
        SuiteResult swap = suites.get(0);
        suites.set(0, suites.get(1));
        suites.set(1, swap);
        Receipt unsorted = replaceScenario(fixture.receipt(), 0, new ScenarioResult(first.scenarioId(), suites));
        assertRejected(() -> PulsarM2FinalReceiptV1.canonicalBytes(unsorted), RejectionCode.SUITE_SET_INVALID);
    }

    @Test
    void rejectsTamperedAndSymlinkAttachments() throws Exception {
        Fixture fixture = fixture();
        Path firstReceiptFile = writeReceipt(fixture.receipt());
        Path target = temporaryDirectory.resolve(LOCAL_PATH);
        Files.writeString(target, "tampered");
        assertRejected(
                () -> PulsarM2FinalResolverV1.resolve(temporaryDirectory, firstReceiptFile),
                RejectionCode.ATTACHMENT_LENGTH_MISMATCH);

        fixture = fixture();
        Path receiptFile = writeReceipt(fixture.receipt());
        target = temporaryDirectory.resolve(LOCAL_PATH);
        Path regular = target.resolveSibling("regular-target.json");
        Files.write(regular, Files.readAllBytes(target));
        Files.delete(target);
        Files.createSymbolicLink(target, regular.getFileName());
        Path finalReceiptFile = receiptFile;
        assertRejected(
                () -> PulsarM2FinalResolverV1.resolve(temporaryDirectory, finalReceiptFile),
                RejectionCode.ATTACHMENT_NOT_REGULAR);
    }

    @Test
    void rejectsWrongPrerequisiteBindingAndUnreferencedAttachment() throws Exception {
        Fixture fixture = fixture();
        SourceTuple source = fixture.receipt().sourceTuple();
        SourceTuple wrong = new SourceTuple(
                "f".repeat(64),
                source.nereusCommit(),
                source.p6ExecutionReceiptSha256(),
                source.pulsarForkCommit(),
                source.sourceLocksSha256());
        Receipt wrongBinding = copy(
                fixture.receipt(),
                fixture.receipt().attachments(),
                fixture.receipt().scenarios(),
                wrong);
        assertRejected(
                () -> PulsarM2FinalReceiptV1.canonicalBytes(wrongBinding), RejectionCode.PREREQUISITE_BINDING_INVALID);

        byte[] extraBytes = "extra".getBytes(StandardCharsets.UTF_8);
        String extraPath = "evidence/unreferenced.json";
        write(extraPath, extraBytes);
        List<AttachmentRef> attachments = new ArrayList<>(fixture.receipt().attachments());
        attachments.add(
                new AttachmentRef(AttachmentKind.LOCAL_JUNIT_SUMMARY, extraPath, extraBytes.length, sha(extraBytes)));
        attachments.sort(Comparator.comparing(AttachmentRef::path));
        Receipt extra = copy(fixture.receipt(), attachments, fixture.receipt().scenarios(), source);
        assertRejected(() -> PulsarM2FinalReceiptV1.canonicalBytes(extra), RejectionCode.ATTACHMENT_SET_INVALID);
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
        assertRejected(() -> PulsarM2FinalReceiptV1.canonicalBytes(escaped), RejectionCode.PATH_INVALID);

        attachments = new ArrayList<>(fixture.receipt().attachments());
        first = attachments.get(0);
        attachments.set(
                0,
                new AttachmentRef(
                        first.kind(),
                        first.path(),
                        PulsarM2FinalReceiptV1.MAX_SINGLE_ATTACHMENT_BYTES + 1L,
                        first.sha256()));
        Receipt oversized = copy(
                fixture.receipt(),
                attachments,
                fixture.receipt().scenarios(),
                fixture.receipt().sourceTuple());
        assertRejected(() -> PulsarM2FinalReceiptV1.canonicalBytes(oversized), RejectionCode.ATTACHMENT_BYTES_EXCEEDED);
    }

    @Test
    void canonicalRootRejectsSymlinkAndOversize() throws Exception {
        Fixture fixture = fixture();
        Path receiptFile = writeReceipt(fixture.receipt());
        Path link = temporaryDirectory.resolve("receipt-link.json");
        Files.createSymbolicLink(link, receiptFile.getFileName());
        assertRejected(() -> PulsarM2FinalReceiptV1.parseCanonicalFile(link), RejectionCode.ROOT_NOT_REGULAR);

        Path oversized = temporaryDirectory.resolve("oversized.json");
        Files.write(oversized, new byte[PulsarM2FinalReceiptV1.MAX_CANONICAL_BYTES + 1]);
        assertRejected(() -> PulsarM2FinalReceiptV1.parseCanonicalFile(oversized), RejectionCode.ROOT_BYTES_EXCEEDED);
    }

    @Test
    void policyContainsOnlyTheElevenExactlyM2BookKeeperScenarios() {
        List<String> actual = PulsarM2PromotionPolicyV1.policy().stream()
                .map(PulsarM2PromotionPolicyV1.ScenarioRequirement::scenarioId)
                .toList();
        assertThat(actual)
                .containsExactly(
                        "V2-BK-001",
                        "V2-BK-002",
                        "V2-BK-004",
                        "V2-BK-005",
                        "V2-BK-006",
                        "V2-BK-007",
                        "V2-BK-008",
                        "V2-BK-009",
                        "V2-BK-010",
                        "V2-BK-012",
                        "V2-BK-013")
                .doesNotContain("V2-BK-003", "V2-BK-011", "V2-BK-014", "V2-PUL-001");
    }

    private Fixture fixture() throws Exception {
        Map<String, String> paths = suitePaths();
        Map<String, AttachmentKind> kinds = Map.of(
                LOCAL_PATH,
                AttachmentKind.LOCAL_JUNIT_SUMMARY,
                NATIVE_PATH,
                AttachmentKind.NATIVE_JUNIT_SUMMARY,
                PROVIDER_PATH,
                AttachmentKind.P6_PROVIDER_JUNIT_SUMMARY,
                CANDIDATE_PATH,
                AttachmentKind.P6_CANDIDATE_MATRIX,
                NATIVE_BASELINE_PATH,
                AttachmentKind.P6_NATIVE_BASELINE,
                MINIO_PATH,
                AttachmentKind.P6_REAL_PROVIDER,
                PulsarM2FinalReceiptV1.KAFKA_FINAL_PATH,
                AttachmentKind.KAFKA_FINAL_RECEIPT,
                PulsarM2FinalReceiptV1.P6_EXECUTION_PATH,
                AttachmentKind.P6_EXECUTION_RECEIPT);
        List<AttachmentRef> attachments = new ArrayList<>();
        Map<String, String> attachmentSha = new HashMap<>();
        for (Map.Entry<String, AttachmentKind> row : kinds.entrySet()) {
            byte[] bytes = ("attachment=" + row.getKey()).getBytes(StandardCharsets.UTF_8);
            write(row.getKey(), bytes);
            String digest = sha(bytes);
            attachments.add(new AttachmentRef(row.getValue(), row.getKey(), bytes.length, digest));
            attachmentSha.put(row.getKey(), digest);
        }
        attachments.sort(Comparator.comparing(AttachmentRef::path));

        List<ScenarioResult> scenarios = new ArrayList<>();
        for (PulsarM2PromotionPolicyV1.ScenarioRequirement requirement : PulsarM2PromotionPolicyV1.policy()) {
            List<SuiteResult> suites = requirement.requiredSuiteIds().stream()
                    .sorted()
                    .map(suite -> new SuiteResult(paths.get(suite), 0, 0, 0, suite, 1))
                    .toList();
            scenarios.add(new ScenarioResult(requirement.scenarioId(), suites));
        }
        SourceTuple source = new SourceTuple(
                attachmentSha.get(PulsarM2FinalReceiptV1.KAFKA_FINAL_PATH),
                "a".repeat(40),
                attachmentSha.get(PulsarM2FinalReceiptV1.P6_EXECUTION_PATH),
                "b".repeat(40),
                "c".repeat(64));
        return new Fixture(new Receipt(
                attachments,
                ReceiptKind.PULSAR_M2_FINAL,
                true,
                ReceiptResult.PASS_PULSAR_M2_FINAL,
                scenarios,
                PulsarM2FinalReceiptV1.SCHEMA,
                source));
    }

    private static Map<String, String> suitePaths() {
        Map<String, String> paths = new HashMap<>();
        for (PulsarM2PromotionPolicyV1.ScenarioRequirement scenario : PulsarM2PromotionPolicyV1.policy()) {
            for (String suite : scenario.requiredSuiteIds()) {
                String path;
                if (suite.equals("nereus.kafka.m2.final")) {
                    path = PulsarM2FinalReceiptV1.KAFKA_FINAL_PATH;
                } else if (suite.equals("nereus.pulsar.m2.p6.candidate-matrix")) {
                    path = CANDIDATE_PATH;
                } else if (suite.equals("nereus.pulsar.m2.p6.native-baseline")) {
                    path = NATIVE_BASELINE_PATH;
                } else if (suite.equals("nereus.pulsar.m2.p6.minio-provider")) {
                    path = MINIO_PATH;
                } else if (suite.startsWith("org.apache.bookkeeper.")) {
                    path = NATIVE_PATH;
                } else if (suite.contains("P6") || suite.contains("S3Pulsar")) {
                    path = PROVIDER_PATH;
                } else {
                    path = LOCAL_PATH;
                }
                paths.put(suite, path);
            }
        }
        return paths;
    }

    private Path writeReceipt(Receipt receipt) throws Exception {
        Path path = temporaryDirectory.resolve("pulsar-final.json");
        Files.write(path, PulsarM2FinalReceiptV1.canonicalBytes(receipt));
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
