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

import static com.nereusstream.domain.receipt.VirtualLedgerReceiptV1.MAX_ATTACHMENTS;
import static com.nereusstream.domain.receipt.VirtualLedgerReceiptV1.MAX_CANONICAL_ROOT_BYTES;
import static com.nereusstream.domain.receipt.VirtualLedgerReceiptV1.MAX_EXACT_JSON_INTEGER;
import static com.nereusstream.domain.receipt.VirtualLedgerReceiptV1.MAX_PATH_BYTES;
import static com.nereusstream.domain.receipt.VirtualLedgerReceiptV1.MAX_PATH_SEGMENTS;
import static com.nereusstream.domain.receipt.VirtualLedgerReceiptV1.MAX_SANITIZED_LOG_BYTES;
import static com.nereusstream.domain.receipt.VirtualLedgerReceiptV1.MAX_SCENARIOS;
import static com.nereusstream.domain.receipt.VirtualLedgerReceiptV1.MAX_SINGLE_ATTACHMENT_BYTES;
import static com.nereusstream.domain.receipt.VirtualLedgerReceiptV1.MAX_SUITES_PER_SCENARIO;
import static com.nereusstream.domain.receipt.VirtualLedgerReceiptV1.MAX_TOTAL_ATTACHMENT_BYTES;
import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.assertThatThrownBy;
import com.nereusstream.domain.receipt.ReceiptV1CapacitySamples.EvidenceReport;
import com.nereusstream.domain.receipt.ReceiptV1CapacitySamples.Sample;
import com.nereusstream.domain.receipt.VirtualLedgerReceiptV1.AttachmentKind;
import com.nereusstream.domain.receipt.VirtualLedgerReceiptV1.AttachmentRef;
import com.nereusstream.domain.receipt.VirtualLedgerReceiptV1.ReceiptKind;
import com.nereusstream.domain.receipt.VirtualLedgerReceiptV1.ReceiptRejectedException;
import com.nereusstream.domain.receipt.VirtualLedgerReceiptV1.ReceiptRoot;
import com.nereusstream.domain.receipt.VirtualLedgerReceiptV1.RejectionCode;
import com.nereusstream.domain.receipt.VirtualLedgerReceiptV1.ScenarioResult;
import com.nereusstream.domain.receipt.VirtualLedgerReceiptV1.SuiteResult;
import java.io.IOException;
import java.nio.charset.StandardCharsets;
import java.nio.file.Files;
import java.nio.file.Path;
import java.util.Arrays;
import java.util.HashSet;
import java.util.LinkedHashMap;
import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.TreeMap;
import org.assertj.core.api.ThrowableAssert.ThrowingCallable;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.io.TempDir;

class ReceiptV1CapacityEvidenceTest {
    private static final String FIXTURE_SOURCE = ReceiptV1CapacitySamples.REQUIRED_BASELINE_COMMIT;
    private static final String FIXTURE_SOURCE_LOCKS = "a".repeat(64);

    @Test
    void inventoryFreezesMeasuredRequiredBaselineJUnitFacts() {
        var inventory = ReceiptV1CapacitySamples.inventory();

        assertThat(inventory).hasSize(ReceiptV1CapacitySamples.EXPECTED_PREEXISTING_SUITES);
        assertThat(inventory.stream().mapToInt(row -> row.tests()).sum())
                .isEqualTo(ReceiptV1CapacitySamples.EXPECTED_PREEXISTING_TESTS);
        assertThat(inventory.stream().mapToInt(row -> row.xmlBytes()).sum())
                .isEqualTo(ReceiptV1CapacitySamples.EXPECTED_PREEXISTING_XML_BYTES);
        assertThat(inventory.stream().allMatch(row -> row.failures() == 0 && row.errors() == 0 && row.skipped() == 0))
                .isTrue();
        assertThat(inventory.stream().map(row -> row.module()).distinct())
                .containsExactly("nereus-domain", "nereus-metadata-spi", "nereus-metadata-oxia");
    }

    @Test
    void sampleSetCoversEveryRequiredReceiptShape() {
        List<Sample> samples = samples();

        assertThat(samples)
                .extracting(Sample::id)
                .containsExactly(
                        "fault-cut",
                        "foundation",
                        "maximum-failure",
                        "multi-scenario-harness",
                        "multi-scenario-registry",
                        "multi-suite",
                        "nta1",
                        "o1",
                        "o2",
                        "registry-readiness",
                        "representative-all-pass");
        assertThat(samples)
                .extracting(sample -> sample.root().kind())
                .contains(ReceiptKind.REGISTRY_CONFORMANCE, ReceiptKind.HARNESS_CONFORMANCE_ONLY);
    }

    @Test
    void everySampleHasAStableCanonicalRoundTrip() {
        for (Sample sample : samples()) {
            byte[] first = VirtualLedgerReceiptV1.canonicalBytes(sample.root());
            ReceiptRoot parsed = VirtualLedgerReceiptV1.parseCanonical(first);
            byte[] second = VirtualLedgerReceiptV1.canonicalBytes(parsed);

            assertThat(parsed).as(sample.id()).isEqualTo(sample.root());
            assertThat(second).as(sample.id()).containsExactly(first);
        }
    }

    @Test
    void evidenceGenerationIsByteDeterministic(@TempDir Path temporaryDirectory) throws IOException {
        Path first = temporaryDirectory.resolve("first");
        Path second = temporaryDirectory.resolve("second");

        EvidenceReport firstReport = ReceiptV1CapacitySamples.generateEvidence(first);
        EvidenceReport secondReport = ReceiptV1CapacitySamples.generateEvidence(second);

        assertThat(secondReport).isEqualTo(firstReport);
        assertThat(treeDigests(second)).isEqualTo(treeDigests(first));
    }

    @Test
    void generatedReportFreezesCapsAndReadinessOnlyBoundary() {
        Path reportRoot = Path.of("build/reports/v2-m1-receipt-caps");
        EvidenceReport report = ReceiptV1CapacitySamples.generateEvidence(reportRoot);

        assertThat(report.samples()).hasSize(11);
        assertThat(report.json())
                .contains("\"result\": \"RECEIPT_CAPACITY_READINESS_ONLY\"")
                .contains("\"promotionEligible\": false")
                .contains("\"productionReceiptParserImplemented\": false")
                .contains("\"scenarioPromotion\": false")
                .contains("\"m1Final\": false")
                .contains("\"sampleRootsAreTestVectors\": true")
                .contains("\"canonicalRootBytes\": 65536")
                .contains("\"expectedFocusedTests\": 36");
        assertThat(report.markdown()).contains(report.jsonSha256(), "91 suites / 386 tests / 96,248 bytes");
    }

    @Test
    void canonicalRootByteBoundaryIsInclusive() {
        VirtualLedgerReceiptV1.requireRootBytes(MAX_CANONICAL_ROOT_BYTES);

        assertRejected(
                RejectionCode.RECEIPT_ROOT_BYTES_EXCEEDED,
                () -> VirtualLedgerReceiptV1.requireRootBytes((long) MAX_CANONICAL_ROOT_BYTES + 1));
    }

    @Test
    void scenarioCountBoundaryIsInclusiveAndNonZero() {
        VirtualLedgerReceiptV1.requireScenarioCount(1);
        VirtualLedgerReceiptV1.requireScenarioCount(MAX_SCENARIOS);
        String oversized =
                replaceArrayWithCopies(canonicalString(sample("foundation").root()), "scenarios", 17);

        assertRejected(
                RejectionCode.RECEIPT_SCENARIO_COUNT_EXCEEDED, () -> VirtualLedgerReceiptV1.requireScenarioCount(0));
        assertRejected(
                RejectionCode.RECEIPT_SCENARIO_COUNT_EXCEEDED,
                () -> VirtualLedgerReceiptV1.requireScenarioCount(MAX_SCENARIOS + 1));
        assertRejected(
                RejectionCode.RECEIPT_SCENARIO_COUNT_EXCEEDED,
                () -> VirtualLedgerReceiptV1.parseCanonical(oversized.getBytes(StandardCharsets.UTF_8)));
    }

    @Test
    void suiteCountBoundaryIsInclusiveAndNonZero() {
        VirtualLedgerReceiptV1.requireSuiteCount(1);
        VirtualLedgerReceiptV1.requireSuiteCount(MAX_SUITES_PER_SCENARIO);
        String oversized = replaceArrayWithCopies(
                canonicalString(rootWithSuite(new SuiteResult("suite.one", 1, 1, 1, 0, 0, 0))), "suites", 129);

        assertRejected(RejectionCode.RECEIPT_SUITE_COUNT_EXCEEDED, () -> VirtualLedgerReceiptV1.requireSuiteCount(0));
        assertRejected(
                RejectionCode.RECEIPT_SUITE_COUNT_EXCEEDED,
                () -> VirtualLedgerReceiptV1.requireSuiteCount(MAX_SUITES_PER_SCENARIO + 1));
        assertRejected(
                RejectionCode.RECEIPT_SUITE_COUNT_EXCEEDED,
                () -> VirtualLedgerReceiptV1.parseCanonical(oversized.getBytes(StandardCharsets.UTF_8)));
    }

    @Test
    void attachmentCountBoundaryAllowsZeroAndIsInclusive() {
        VirtualLedgerReceiptV1.requireAttachmentCount(0);
        VirtualLedgerReceiptV1.requireAttachmentCount(MAX_ATTACHMENTS);
        String oversized =
                replaceArrayWithCopies(canonicalString(rootWithAttachment("one.txt", new byte[0])), "attachments", 33);

        assertRejected(
                RejectionCode.RECEIPT_ATTACHMENT_COUNT_EXCEEDED,
                () -> VirtualLedgerReceiptV1.requireAttachmentCount(MAX_ATTACHMENTS + 1));
        assertRejected(
                RejectionCode.RECEIPT_ATTACHMENT_COUNT_EXCEEDED,
                () -> VirtualLedgerReceiptV1.parseCanonical(oversized.getBytes(StandardCharsets.UTF_8)));
    }

    @Test
    void singleAttachmentByteBoundaryIsInclusive() {
        VirtualLedgerReceiptV1.requireSingleAttachmentBytes(0);
        VirtualLedgerReceiptV1.requireSingleAttachmentBytes(MAX_SINGLE_ATTACHMENT_BYTES);

        assertRejected(
                RejectionCode.RECEIPT_ATTACHMENT_BYTES_EXCEEDED,
                () -> VirtualLedgerReceiptV1.requireSingleAttachmentBytes((long) MAX_SINGLE_ATTACHMENT_BYTES + 1));
    }

    @Test
    void totalAttachmentByteBoundaryIsInclusive() {
        VirtualLedgerReceiptV1.requireTotalAttachmentBytes(0);
        VirtualLedgerReceiptV1.requireTotalAttachmentBytes(MAX_TOTAL_ATTACHMENT_BYTES);

        assertRejected(
                RejectionCode.RECEIPT_ATTACHMENT_TOTAL_BYTES_EXCEEDED,
                () -> VirtualLedgerReceiptV1.requireTotalAttachmentBytes((long) MAX_TOTAL_ATTACHMENT_BYTES + 1));
    }

    @Test
    void sanitizedLogByteBoundaryIsIndependentAndInclusive() {
        VirtualLedgerReceiptV1.requireSanitizedLogBytes(0);
        VirtualLedgerReceiptV1.requireSanitizedLogBytes(MAX_SANITIZED_LOG_BYTES);

        assertRejected(
                RejectionCode.RECEIPT_SANITIZED_LOG_BYTES_EXCEEDED,
                () -> VirtualLedgerReceiptV1.requireSanitizedLogBytes((long) MAX_SANITIZED_LOG_BYTES + 1));
    }

    @Test
    void relativePathByteBoundaryIsInclusive() {
        assertThat(VirtualLedgerReceiptV1.validatePath("a".repeat(MAX_PATH_BYTES)))
                .containsExactly("a".repeat(256));

        assertRejected(
                RejectionCode.RECEIPT_PATH_BYTES_EXCEEDED,
                () -> VirtualLedgerReceiptV1.validatePath("a".repeat(MAX_PATH_BYTES + 1)));
    }

    @Test
    void relativePathSegmentBoundaryIsInclusive() {
        String accepted = String.join("/", java.util.Collections.nCopies(MAX_PATH_SEGMENTS, "a"));
        String rejected = String.join("/", java.util.Collections.nCopies(MAX_PATH_SEGMENTS + 1, "a"));

        assertThat(VirtualLedgerReceiptV1.validatePath(accepted)).hasSize(MAX_PATH_SEGMENTS);
        assertRejected(
                RejectionCode.RECEIPT_PATH_SEGMENTS_EXCEEDED, () -> VirtualLedgerReceiptV1.validatePath(rejected));
    }

    @Test
    void pathTraversalAbsolutePlatformAndNonAsciiFormsFailClosed() {
        for (String path :
                List.of("/root", "tail/", "a//b", ".", "..", "a/../b", "a/./b", "a\\b", "C:a", "_a", "a/\u03b1")) {
            assertRejected(RejectionCode.RECEIPT_PATH_INVALID, () -> VirtualLedgerReceiptV1.validatePath(path));
        }
    }

    @Test
    void checkedArithmeticRejectsAdditionAndMultiplicationOverflow() {
        assertThat(VirtualLedgerReceiptV1.checkedAdd(1, 2, 3)).isEqualTo(6);
        assertThat(VirtualLedgerReceiptV1.checkedMultiply(7, 9)).isEqualTo(63);

        assertRejected(
                RejectionCode.RECEIPT_CHECKED_ARITHMETIC_OVERFLOW,
                () -> VirtualLedgerReceiptV1.checkedAdd(Long.MAX_VALUE, 1));
        assertRejected(
                RejectionCode.RECEIPT_CHECKED_ARITHMETIC_OVERFLOW,
                () -> VirtualLedgerReceiptV1.checkedMultiply(Long.MAX_VALUE, 2));
    }

    @Test
    void junitNormalizationAddsFailuresAndErrorsExactly() {
        SuiteResult normalized = VirtualLedgerReceiptV1.normalizeJUnit("suite.normalized", 11, 2, 3, 1, 1);

        assertThat(normalized).isEqualTo(new SuiteResult("suite.normalized", 11, 10, 4, 5, 1, 1));
    }

    @Test
    void exactIntegerAndAccountingBoundariesFailClosed() {
        SuiteResult exact = new SuiteResult(
                "suite.exact", MAX_EXACT_JSON_INTEGER, MAX_EXACT_JSON_INTEGER, MAX_EXACT_JSON_INTEGER, 0, 0, 0);
        VirtualLedgerReceiptV1.validate(rootWithSuite(exact));

        assertRejected(
                RejectionCode.RECEIPT_WRONG_TYPE_OR_NUMBER,
                () -> VirtualLedgerReceiptV1.validate(
                        rootWithSuite(new SuiteResult("suite.tooLarge", MAX_EXACT_JSON_INTEGER + 1, 1, 1, 0, 0, 0))));
        assertRejected(
                RejectionCode.RECEIPT_ACCOUNTING_INVALID,
                () -> VirtualLedgerReceiptV1.validate(
                        rootWithSuite(new SuiteResult("suite.badAccounting", 2, 2, 1, 0, 0, 0))));
        assertRejected(
                RejectionCode.RECEIPT_ACCOUNTING_INVALID,
                () -> VirtualLedgerReceiptV1.normalizeJUnit("suite.negativePass", 1, 1, 1, 0, 0));
    }

    @Test
    void zeroTestMandatoryResultFails() {
        ReceiptRoot root = rootWithSuite(new SuiteResult("suite.zero", 0, 0, 0, 0, 0, 0));

        assertRejected(
                RejectionCode.RECEIPT_MANDATORY_RESULT_NOT_PASS,
                () -> VirtualLedgerReceiptV1.requireMandatoryPass(root, Set.of("suite.zero")));
    }

    @Test
    void skippedMandatoryResultFails() {
        ReceiptRoot root = rootWithSuite(new SuiteResult("suite.skipped", 2, 1, 1, 0, 1, 0));

        assertRejected(
                RejectionCode.RECEIPT_MANDATORY_RESULT_NOT_PASS,
                () -> VirtualLedgerReceiptV1.requireMandatoryPass(root, Set.of("suite.skipped")));
    }

    @Test
    void failedMandatoryResultFails() {
        ReceiptRoot root = rootWithSuite(new SuiteResult("suite.failed", 2, 2, 1, 1, 0, 0));

        assertRejected(
                RejectionCode.RECEIPT_MANDATORY_RESULT_NOT_PASS,
                () -> VirtualLedgerReceiptV1.requireMandatoryPass(root, Set.of("suite.failed")));
    }

    @Test
    void abortedMandatoryResultFails() {
        ReceiptRoot root = rootWithSuite(new SuiteResult("suite.aborted", 2, 2, 1, 0, 0, 1));

        assertRejected(
                RejectionCode.RECEIPT_MANDATORY_RESULT_NOT_PASS,
                () -> VirtualLedgerReceiptV1.requireMandatoryPass(root, Set.of("suite.aborted")));
    }

    @Test
    void missingMandatorySuiteFails() {
        ReceiptRoot root = rootWithSuite(new SuiteResult("suite.present", 1, 1, 1, 0, 0, 0));

        assertRejected(
                RejectionCode.RECEIPT_REQUIRED_SUITE_MISSING,
                () -> VirtualLedgerReceiptV1.requireMandatoryPass(root, Set.of("suite.absent")));
    }

    @Test
    void duplicateAndUnsortedScenarioSuiteAndAttachmentIdsFail() {
        ReceiptRoot valid = validRoot();
        ScenarioResult scenario = valid.scenarios().get(0);
        ReceiptRoot duplicateScenarios = new ReceiptRoot(
                valid.schema(), valid.kind(), valid.sourceTuple(), List.of(scenario, scenario), List.of());
        ScenarioResult duplicateSuites = new ScenarioResult(
                scenario.scenarioId(),
                List.of(scenario.suites().get(0), scenario.suites().get(0)));
        AttachmentRef attachment =
                sample("registry-readiness").root().attachments().get(0);
        ReceiptRoot duplicateAttachments = new ReceiptRoot(
                valid.schema(), valid.kind(), valid.sourceTuple(), valid.scenarios(), List.of(attachment, attachment));

        assertRejected(
                RejectionCode.RECEIPT_DUPLICATE_OR_UNSORTED_ID,
                () -> VirtualLedgerReceiptV1.validate(duplicateScenarios));
        assertRejected(
                RejectionCode.RECEIPT_DUPLICATE_OR_UNSORTED_ID,
                () -> VirtualLedgerReceiptV1.validate(new ReceiptRoot(
                        valid.schema(), valid.kind(), valid.sourceTuple(), List.of(duplicateSuites), List.of())));
        assertRejected(
                RejectionCode.RECEIPT_DUPLICATE_OR_UNSORTED_ID,
                () -> VirtualLedgerReceiptV1.validate(duplicateAttachments));
    }

    @Test
    void unknownAndMissingFieldsFailClosed() {
        String canonical = canonicalString(validRoot());
        String unknown = canonical.substring(0, canonical.length() - 1) + ",\"unknown\":0}";
        String missing = canonical.replaceFirst(",\"kind\":\"HARNESS_CONFORMANCE_ONLY\"", "");

        assertRejected(
                RejectionCode.RECEIPT_UNKNOWN_OR_MISSING_FIELD,
                () -> VirtualLedgerReceiptV1.parseCanonical(unknown.getBytes(StandardCharsets.UTF_8)));
        assertRejected(
                RejectionCode.RECEIPT_UNKNOWN_OR_MISSING_FIELD,
                () -> VirtualLedgerReceiptV1.parseCanonical(missing.getBytes(StandardCharsets.UTF_8)));
    }

    @Test
    void duplicateJsonFieldsHaveAStableCategory() {
        String duplicate = canonicalString(validRoot())
                .replaceFirst("\\{\"attachments\":", "{\"attachments\":[],\"attachments\":");

        assertRejected(
                RejectionCode.RECEIPT_DUPLICATE_FIELD,
                () -> VirtualLedgerReceiptV1.parseCanonical(duplicate.getBytes(StandardCharsets.UTF_8)));
    }

    @Test
    void wrongJsonTypesFractionsExponentsSignsAndLeadingZerosFail() {
        String canonical = canonicalString(validRoot());
        List<String> invalid = List.of(
                canonical.replace("\"kind\":\"HARNESS_CONFORMANCE_ONLY\"", "\"kind\":1"),
                canonical.replaceFirst("\"aborted\":0", "\"aborted\":1.0"),
                canonical.replaceFirst("\"aborted\":0", "\"aborted\":1e0"),
                canonical.replaceFirst("\"aborted\":0", "\"aborted\":-1"),
                canonical.replaceFirst("\"aborted\":0", "\"aborted\":01"));

        for (String json : invalid) {
            assertRejected(
                    RejectionCode.RECEIPT_WRONG_TYPE_OR_NUMBER,
                    () -> VirtualLedgerReceiptV1.parseCanonical(json.getBytes(StandardCharsets.UTF_8)));
        }
    }

    @Test
    void malformedUtf8BomAndTrailingDataFailBeforeCanonicality() {
        byte[] canonical = VirtualLedgerReceiptV1.canonicalBytes(validRoot());
        byte[] bom = new byte[canonical.length + 3];
        System.arraycopy(new byte[] {(byte) 0xef, (byte) 0xbb, (byte) 0xbf}, 0, bom, 0, 3);
        System.arraycopy(canonical, 0, bom, 3, canonical.length);

        assertRejected(
                RejectionCode.RECEIPT_MALFORMED_JSON,
                () -> VirtualLedgerReceiptV1.parseCanonical(new byte[] {(byte) 0xff}));
        assertRejected(RejectionCode.RECEIPT_MALFORMED_JSON, () -> VirtualLedgerReceiptV1.parseCanonical(bom));
        assertRejected(
                RejectionCode.RECEIPT_MALFORMED_JSON,
                () -> VirtualLedgerReceiptV1.parseCanonical(
                        (new String(canonical, StandardCharsets.UTF_8) + "x").getBytes(StandardCharsets.UTF_8)));
    }

    @Test
    void semanticallyEquivalentNonCanonicalJsonFails() {
        String canonical = canonicalString(validRoot());
        String escapedDigit = canonical.replace("V2-POSITION-010", "V2-POSITION-01" + Character.toString(92) + "u0030");
        String canonicalSuite = "{\"aborted\":0,\"discovered\":12,\"executed\":12,\"failed\":0,"
                + "\"passed\":12,\"skipped\":0,\"suiteId\":\"harness.conformance.cuts\"}";
        String reorderedSuite = "{\"suiteId\":\"harness.conformance.cuts\",\"aborted\":0,"
                + "\"discovered\":12,\"executed\":12,\"failed\":0,\"passed\":12,\"skipped\":0}";

        assertRejected(
                RejectionCode.RECEIPT_NON_CANONICAL_JSON,
                () -> VirtualLedgerReceiptV1.parseCanonical((" " + canonical).getBytes(StandardCharsets.UTF_8)));
        assertRejected(
                RejectionCode.RECEIPT_NON_CANONICAL_JSON,
                () -> VirtualLedgerReceiptV1.parseCanonical(escapedDigit.getBytes(StandardCharsets.UTF_8)));
        assertRejected(
                RejectionCode.RECEIPT_NON_CANONICAL_JSON,
                () -> VirtualLedgerReceiptV1.parseCanonical(
                        canonical.replace(canonicalSuite, reorderedSuite).getBytes(StandardCharsets.UTF_8)));
    }

    @Test
    void schemaKindSourceTupleAndAttachmentKindAreClosed() {
        String canonical = canonicalString(validRoot());
        String wrongSchema = canonical.replace(VirtualLedgerReceiptV1.SCHEMA, "NEREUS_RECEIPT_V2");
        String wrongKind = canonical.replace("HARNESS_CONFORMANCE_ONLY", "UNKNOWN_KIND");
        String wrongSource = canonical.replace(FIXTURE_SOURCE, "g".repeat(40));
        String attachment = canonicalString(sample("registry-readiness").root());
        String wrongAttachmentKind = attachment.replaceFirst("TEST_REPORT", "UNKNOWN_ATTACHMENT");

        assertRejected(
                RejectionCode.RECEIPT_SCHEMA_OR_KIND_INVALID,
                () -> VirtualLedgerReceiptV1.parseCanonical(wrongSchema.getBytes(StandardCharsets.UTF_8)));
        assertRejected(
                RejectionCode.RECEIPT_SCHEMA_OR_KIND_INVALID,
                () -> VirtualLedgerReceiptV1.parseCanonical(wrongKind.getBytes(StandardCharsets.UTF_8)));
        assertRejected(
                RejectionCode.RECEIPT_SOURCE_TUPLE_INVALID,
                () -> VirtualLedgerReceiptV1.parseCanonical(wrongSource.getBytes(StandardCharsets.UTF_8)));
        assertRejected(
                RejectionCode.RECEIPT_SCHEMA_OR_KIND_INVALID,
                () -> VirtualLedgerReceiptV1.parseCanonical(wrongAttachmentKind.getBytes(StandardCharsets.UTF_8)));
    }

    @Test
    void registryFixtureIsExactDeterministicAndContainsDistinctRows() {
        byte[] first = ReceiptV1CapacitySamples.maximumRegistryFixture();
        byte[] second = ReceiptV1CapacitySamples.maximumRegistryFixture();

        assertThat(first)
                .hasSize(ReceiptV1CapacitySamples.MAX_REGISTRY_FIXTURE_BYTES)
                .containsExactly(second);
        assertThat(Arrays.copyOfRange(first, 0, 4)).containsExactly('N', 'V', 'R', '1');
        Set<String> assignmentRows = new HashSet<>();
        int assignmentStart = 184 + 14 * 120;
        for (int index = 0; index < 256; index++) {
            byte[] row = Arrays.copyOfRange(first, assignmentStart + index * 192, assignmentStart + (index + 1) * 192);
            assignmentRows.add(VirtualLedgerReceiptV1.sha256(row));
        }
        assertThat(assignmentRows).hasSize(256);
    }

    @Test
    void failureAndSanitizedLogFixturesNameEveryDistinctBoundary() {
        String failures = new String(ReceiptV1CapacitySamples.maximumFailureReport(), StandardCharsets.UTF_8);
        String log = new String(ReceiptV1CapacitySamples.sanitizedLogExcerpt(), StandardCharsets.UTF_8);

        for (RejectionCode code : RejectionCode.values()) {
            assertThat(failures).contains(code.name());
            assertThat(log).contains(code.name());
        }
        for (String cut : ReceiptV1CapacitySamples.faultCuts()) {
            assertThat(log).contains(cut);
        }
        assertThat(countOccurrences(failures, "<testcase ")).isEqualTo(RejectionCode.values().length);
        assertThat(log.lines())
                .hasSize(RejectionCode.values().length
                        + ReceiptV1CapacitySamples.faultCuts().size());
        assertThat(log).doesNotContain("secret=", "token=", "payload=");
    }

    @Test
    void attachmentVerifierChecksExactLengthAndDigest(@TempDir Path temporaryDirectory) throws IOException {
        byte[] content = "named deterministic test attachment\n".getBytes(StandardCharsets.UTF_8);
        Path target = temporaryDirectory.resolve("attachments/test-reports/result.txt");
        Files.createDirectories(target.getParent());
        Files.write(target, content);
        ReceiptRoot valid = rootWithAttachment("attachments/test-reports/result.txt", content);

        assertThat(VirtualLedgerReceiptV1.verifyAttachments(temporaryDirectory, valid))
                .containsEntry(AttachmentKind.TEST_REPORT, (long) content.length);
        assertRejected(
                RejectionCode.RECEIPT_ATTACHMENT_LENGTH_MISMATCH,
                () -> VirtualLedgerReceiptV1.verifyAttachments(
                        temporaryDirectory,
                        replaceAttachment(valid, content.length + 1, VirtualLedgerReceiptV1.sha256(content))));
        assertRejected(
                RejectionCode.RECEIPT_ATTACHMENT_DIGEST_MISMATCH,
                () -> VirtualLedgerReceiptV1.verifyAttachments(
                        temporaryDirectory, replaceAttachment(valid, content.length, "0".repeat(64))));
    }

    @Test
    void attachmentVerifierRejectsTargetAndAncestorSymlinks(@TempDir Path temporaryDirectory) throws IOException {
        byte[] content = "symlink target\n".getBytes(StandardCharsets.UTF_8);
        Path real = temporaryDirectory.resolve("real.txt");
        Files.write(real, content);
        Path targetLink = temporaryDirectory.resolve("target-link.txt");
        Files.createSymbolicLink(targetLink, real.getFileName());

        assertRejected(
                RejectionCode.RECEIPT_ATTACHMENT_SYMLINK,
                () -> VirtualLedgerReceiptV1.verifyAttachments(
                        temporaryDirectory, rootWithAttachment("target-link.txt", content)));

        Path realDirectory = temporaryDirectory.resolve("real-directory");
        Files.createDirectory(realDirectory);
        Files.write(realDirectory.resolve("data.txt"), content);
        Path ancestorLink = temporaryDirectory.resolve("ancestor-link");
        Files.createSymbolicLink(ancestorLink, realDirectory.getFileName());
        assertRejected(
                RejectionCode.RECEIPT_ATTACHMENT_SYMLINK,
                () -> VirtualLedgerReceiptV1.verifyAttachments(
                        temporaryDirectory, rootWithAttachment("ancestor-link/data.txt", content)));
    }

    @Test
    void attachmentVerifierRejectsMissingAndNonRegularFiles(@TempDir Path temporaryDirectory) throws IOException {
        byte[] empty = new byte[0];
        Files.createDirectory(temporaryDirectory.resolve("directory.txt"));

        assertRejected(
                RejectionCode.RECEIPT_ATTACHMENT_NOT_REGULAR,
                () -> VirtualLedgerReceiptV1.verifyAttachments(
                        temporaryDirectory, rootWithAttachment("directory.txt", empty)));
        assertRejected(
                RejectionCode.RECEIPT_ATTACHMENT_NOT_REGULAR,
                () -> VirtualLedgerReceiptV1.verifyAttachments(
                        temporaryDirectory, rootWithAttachment("missing.txt", empty)));
    }

    @Test
    void receiptRootFileRequiresARegularNoFollowInput(@TempDir Path temporaryDirectory) throws IOException {
        byte[] canonical = VirtualLedgerReceiptV1.canonicalBytes(validRoot());
        Path regular = temporaryDirectory.resolve("receipt.json");
        Files.write(regular, canonical);
        assertThat(VirtualLedgerReceiptV1.parseCanonicalFile(regular)).isEqualTo(validRoot());

        Path link = temporaryDirectory.resolve("receipt-link.json");
        Files.createSymbolicLink(link, regular.getFileName());
        assertRejected(RejectionCode.RECEIPT_ROOT_NOT_REGULAR, () -> VirtualLedgerReceiptV1.parseCanonicalFile(link));
        assertRejected(
                RejectionCode.RECEIPT_ROOT_NOT_REGULAR,
                () -> VirtualLedgerReceiptV1.parseCanonicalFile(temporaryDirectory));

        Path oversized = temporaryDirectory.resolve("oversized.json");
        Files.write(oversized, new byte[MAX_CANONICAL_ROOT_BYTES + 1]);
        assertRejected(
                RejectionCode.RECEIPT_ROOT_BYTES_EXCEEDED, () -> VirtualLedgerReceiptV1.parseCanonicalFile(oversized));
    }

    private static List<Sample> samples() {
        return ReceiptV1CapacitySamples.samples(FIXTURE_SOURCE, FIXTURE_SOURCE_LOCKS);
    }

    private static Sample sample(String id) {
        return samples().stream()
                .filter(sample -> sample.id().equals(id))
                .findFirst()
                .orElseThrow();
    }

    private static ReceiptRoot validRoot() {
        return sample("multi-scenario-harness").root();
    }

    private static ReceiptRoot rootWithSuite(SuiteResult suite) {
        ReceiptRoot valid = validRoot();
        return new ReceiptRoot(
                valid.schema(),
                valid.kind(),
                valid.sourceTuple(),
                List.of(new ScenarioResult("V2-POSITION-010", List.of(suite))),
                List.of());
    }

    private static ReceiptRoot rootWithAttachment(String path, byte[] content) {
        ReceiptRoot valid = validRoot();
        AttachmentRef attachment = new AttachmentRef(
                AttachmentKind.TEST_REPORT, path, content.length, VirtualLedgerReceiptV1.sha256(content));
        return new ReceiptRoot(
                valid.schema(), valid.kind(), valid.sourceTuple(), valid.scenarios(), List.of(attachment));
    }

    private static ReceiptRoot replaceAttachment(ReceiptRoot root, long length, String sha256) {
        AttachmentRef current = root.attachments().get(0);
        AttachmentRef replacement = new AttachmentRef(current.attachmentKind(), current.path(), length, sha256);
        return new ReceiptRoot(root.schema(), root.kind(), root.sourceTuple(), root.scenarios(), List.of(replacement));
    }

    private static String canonicalString(ReceiptRoot root) {
        return new String(VirtualLedgerReceiptV1.canonicalBytes(root), StandardCharsets.UTF_8);
    }

    private static String replaceArrayWithCopies(String json, String field, int copies) {
        String marker = "\"" + field + "\":[";
        int contentStart = json.indexOf(marker);
        if (contentStart < 0) {
            throw new IllegalArgumentException("missing array field " + field);
        }
        contentStart += marker.length();
        int depth = 1;
        boolean inString = false;
        boolean escaped = false;
        int contentEnd = -1;
        for (int index = contentStart; index < json.length(); index++) {
            char value = json.charAt(index);
            if (inString) {
                if (escaped) {
                    escaped = false;
                } else if (value == '\\') {
                    escaped = true;
                } else if (value == '"') {
                    inString = false;
                }
            } else if (value == '"') {
                inString = true;
            } else if (value == '[') {
                depth++;
            } else if (value == ']' && --depth == 0) {
                contentEnd = index;
                break;
            }
        }
        if (contentEnd < 0) {
            throw new IllegalArgumentException("unterminated array field " + field);
        }
        String element = json.substring(contentStart, contentEnd);
        return json.substring(0, contentStart)
                + String.join(",", java.util.Collections.nCopies(copies, element))
                + json.substring(contentEnd);
    }

    private static Map<String, String> treeDigests(Path root) throws IOException {
        Map<String, String> result = new TreeMap<>();
        try (var paths = Files.walk(root)) {
            for (Path path : paths.filter(Files::isRegularFile).toList()) {
                result.put(root.relativize(path).toString(), VirtualLedgerReceiptV1.sha256(Files.readAllBytes(path)));
            }
        }
        return new LinkedHashMap<>(result);
    }

    private static int countOccurrences(String value, String token) {
        int count = 0;
        int offset = 0;
        while ((offset = value.indexOf(token, offset)) >= 0) {
            count++;
            offset += token.length();
        }
        return count;
    }

    private static void assertRejected(RejectionCode code, ThrowingCallable action) {
        assertThatThrownBy(action).isInstanceOf(ReceiptRejectedException.class).satisfies(error -> assertThat(
                        ((ReceiptRejectedException) error).code())
                .isEqualTo(code));
    }
}
