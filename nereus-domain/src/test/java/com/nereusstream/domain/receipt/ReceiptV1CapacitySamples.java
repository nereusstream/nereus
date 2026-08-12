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
import static com.nereusstream.domain.receipt.VirtualLedgerReceiptV1.MAX_PATH_BYTES;
import static com.nereusstream.domain.receipt.VirtualLedgerReceiptV1.MAX_PATH_SEGMENTS;
import static com.nereusstream.domain.receipt.VirtualLedgerReceiptV1.MAX_SANITIZED_LOG_BYTES;
import static com.nereusstream.domain.receipt.VirtualLedgerReceiptV1.MAX_SCENARIOS;
import static com.nereusstream.domain.receipt.VirtualLedgerReceiptV1.MAX_SINGLE_ATTACHMENT_BYTES;
import static com.nereusstream.domain.receipt.VirtualLedgerReceiptV1.MAX_SUITES_PER_SCENARIO;
import static com.nereusstream.domain.receipt.VirtualLedgerReceiptV1.MAX_TOTAL_ATTACHMENT_BYTES;
import com.nereusstream.domain.receipt.VirtualLedgerReceiptV1.AttachmentKind;
import com.nereusstream.domain.receipt.VirtualLedgerReceiptV1.AttachmentRef;
import com.nereusstream.domain.receipt.VirtualLedgerReceiptV1.ReceiptKind;
import com.nereusstream.domain.receipt.VirtualLedgerReceiptV1.ReceiptRoot;
import com.nereusstream.domain.receipt.VirtualLedgerReceiptV1.RejectionCode;
import com.nereusstream.domain.receipt.VirtualLedgerReceiptV1.ScenarioResult;
import com.nereusstream.domain.receipt.VirtualLedgerReceiptV1.SourceTuple;
import com.nereusstream.domain.receipt.VirtualLedgerReceiptV1.SuiteResult;
import java.io.BufferedReader;
import java.io.IOException;
import java.io.InputStream;
import java.io.InputStreamReader;
import java.nio.ByteBuffer;
import java.nio.charset.StandardCharsets;
import java.nio.file.Files;
import java.nio.file.Path;
import java.nio.file.StandardOpenOption;
import java.util.ArrayList;
import java.util.Comparator;
import java.util.EnumMap;
import java.util.List;
import java.util.Locale;
import java.util.Map;
import java.util.Objects;
import java.util.regex.Pattern;
import java.util.stream.Collectors;

/** Deterministic representative roots and attachments for M1-2. */
final class ReceiptV1CapacitySamples {
    static final String REQUIRED_BASELINE_COMMIT = "7ede023e19774309268350a866932804787a52a7";
    static final int EXPECTED_PREEXISTING_SUITES = 91;
    static final int EXPECTED_PREEXISTING_TESTS = 386;
    static final int EXPECTED_PREEXISTING_XML_BYTES = 96_248;
    static final int EXPECTED_O1_EXACT_RUNTIME_XML_BYTES = 10_058;
    static final int EXPECTED_CLOSED_M1_POSITION_SCENARIOS = 9;
    static final int MAX_REGISTRY_FIXTURE_BYTES = 51_016;
    static final int EXPECTED_FOCUSED_TESTS = 36;

    private static final Pattern SHA256 = Pattern.compile("[0-9a-f]{64}");
    private static final String KAFKA_COMMIT = "76f62f3b83e882105219b6c7687dbde594a8b8a2";
    private static final String PULSAR_COMMIT = "11d7ab15291ca4bbc9cc29dedd7878c4e1311ec9";
    private static final String OXIA_CLIENT_COMMIT = "091a42c2780d92da56e9ec1f02ce1c3d988adc16";
    private static final String OXIA_SERVER_COMMIT = "37a17bef17202d5fd6e23282da5fd26d94865484";
    private static final String OXIA_CLIENT_JAR_SHA =
            "0ca719e6d11bd2ee2c2e7e94b42c6843e60f776bea12f7b5814cff9928e2e4c5";
    private static final String OXIA_CLIENT_POM_SHA =
            "b48db12a661e7c4510a30cc816c6b19c5af623dbe5245f8fb8c34ff6afec8659";
    private static final String DOMAIN_JAR_SHA = "2c605ef675c388953f3d2046e02f17bff6b7273a04e4ab8d09cf60be59095600";
    private static final String DOMAIN_POM_SHA = "6edf091863f53ca3bd3d0faa8a8416c764991b335fb5ce0d16d38cdf0e40175d";
    private static final String OXIA_SERVER_IMAGE_DIGEST =
            "sha256:5aa715e4f19091931743e5af489af5f8d6ee15efcce6430a908c6f65cc6d6516";

    private static final Path EVIDENCE_ROOT = Path.of("..", "docs", "v2", "evidence", "v2-m0", "m1-2-receipt-caps")
            .normalize();
    private static final Path SOURCE_COMMIT_PATH = EVIDENCE_ROOT.resolve("source-commit.txt");
    private static final Path SOURCE_LOCKS_INPUT_SHA_PATH = EVIDENCE_ROOT.resolve("source-locks-input.sha256");
    private static final Path CURRENT_SOURCE_LOCKS =
            Path.of("..", "docs", "v2", "source-locks.json").normalize();

    private ReceiptV1CapacitySamples() {}

    record InventoryRow(
            String module, String suiteId, int tests, int failures, int errors, int skipped, int xmlBytes) {}

    record AttachmentData(AttachmentKind kind, String path, byte[] bytes) {
        AttachmentData {
            bytes = bytes.clone();
        }

        @Override
        public byte[] bytes() {
            return bytes.clone();
        }
    }

    record Sample(String id, String generator, ReceiptRoot root, List<AttachmentData> attachments) {
        Sample {
            attachments = List.copyOf(attachments);
        }
    }

    record SampleMetrics(
            String id,
            String generator,
            int rootBytes,
            String rootSha256,
            int scenarios,
            int maxSuitesPerScenario,
            int attachments,
            long maxSingleAttachmentBytes,
            long totalAttachmentBytes,
            int maxPathBytes,
            int maxPathSegments,
            long sanitizedLogBytes,
            Map<AttachmentKind, Long> bytesByKind) {}

    record EvidenceReport(
            String json,
            String markdown,
            List<SampleMetrics> samples,
            String jsonSha256,
            String sourceCommit,
            String sourceLocksInputSha256) {}

    static List<InventoryRow> inventory() {
        InputStream input =
                ReceiptV1CapacitySamples.class.getResourceAsStream("/receipt/m1-current-junit-inventory.tsv");
        if (input == null) {
            throw new IllegalStateException("missing current JUnit inventory fixture");
        }
        try (BufferedReader reader = new BufferedReader(new InputStreamReader(input, StandardCharsets.UTF_8))) {
            List<String> lines = reader.lines().toList();
            if (lines.isEmpty()
                    || !lines.get(0).equals("module\tsuiteId\ttests\tfailures\terrors\tskipped\txmlBytes")) {
                throw new IllegalStateException("unexpected JUnit inventory header");
            }
            List<InventoryRow> rows = new ArrayList<>();
            for (String line : lines.subList(1, lines.size())) {
                String[] fields = line.split("\t", -1);
                if (fields.length != 7) {
                    throw new IllegalStateException("invalid JUnit inventory row: " + line);
                }
                rows.add(new InventoryRow(
                        fields[0],
                        fields[1],
                        Integer.parseInt(fields[2]),
                        Integer.parseInt(fields[3]),
                        Integer.parseInt(fields[4]),
                        Integer.parseInt(fields[5]),
                        Integer.parseInt(fields[6])));
            }
            return List.copyOf(rows);
        } catch (IOException error) {
            throw new IllegalStateException("cannot load JUnit inventory", error);
        }
    }

    static List<Sample> samples(String sourceCommit, String sourceLocksSha256) {
        SourceTuple source = sourceTuple(sourceCommit, sourceLocksSha256);
        List<InventoryRow> inventory = inventory();
        List<Sample> samples = new ArrayList<>();
        samples.add(foundationSample(source, inventory));
        samples.add(o1Sample(source));
        samples.add(o2Sample(source, inventory));
        samples.add(nta1Sample(source));
        samples.add(registryReadinessSample(source, inventory));
        samples.add(representativeAllPassSample(source, inventory));
        samples.add(multiScenarioRegistrySample(source));
        samples.add(multiScenarioHarnessSample(source));
        samples.add(multiSuiteSample(source, inventory));
        samples.add(maximumFailureSample(source));
        samples.add(faultCutSample(source));
        samples.sort(Comparator.comparing(Sample::id));
        return List.copyOf(samples);
    }

    static EvidenceReport generateEvidence(Path reportRoot) {
        Objects.requireNonNull(reportRoot, "reportRoot");
        String sourceCommit = evidenceSourceCommit();
        String sourceLocksInputSha = sourceLocksInputSha256();
        List<Sample> samples = samples(sourceCommit, sourceLocksInputSha);
        List<SampleMetrics> metrics = new ArrayList<>();
        try {
            Files.createDirectories(reportRoot);
            for (Sample sample : samples) {
                Path sampleRoot = reportRoot.resolve("samples").resolve(sample.id());
                Files.createDirectories(sampleRoot);
                for (AttachmentData attachment : sample.attachments()) {
                    Path target = sampleRoot.resolve(attachment.path());
                    Files.createDirectories(target.getParent());
                    Files.write(
                            target,
                            attachment.bytes(),
                            StandardOpenOption.CREATE,
                            StandardOpenOption.TRUNCATE_EXISTING,
                            StandardOpenOption.WRITE);
                }
                byte[] rootBytes = VirtualLedgerReceiptV1.canonicalBytes(sample.root());
                Files.write(
                        sampleRoot.resolve("receipt.json"),
                        rootBytes,
                        StandardOpenOption.CREATE,
                        StandardOpenOption.TRUNCATE_EXISTING,
                        StandardOpenOption.WRITE);
                ReceiptRoot parsed = VirtualLedgerReceiptV1.parseCanonical(rootBytes);
                if (!parsed.equals(sample.root())) {
                    throw new IllegalStateException("sample canonical round trip differs: " + sample.id());
                }
                VirtualLedgerReceiptV1.verifyAttachments(sampleRoot, parsed);
                metrics.add(metrics(sample));
            }
            String json = renderEvidenceJson(sourceCommit, sourceLocksInputSha, metrics);
            String jsonSha = VirtualLedgerReceiptV1.sha256(json.getBytes(StandardCharsets.UTF_8));
            String markdown = renderEvidenceMarkdown(sourceCommit, sourceLocksInputSha, jsonSha, metrics);
            Files.writeString(
                    reportRoot.resolve("receipt-caps.json"),
                    json,
                    StandardCharsets.UTF_8,
                    StandardOpenOption.CREATE,
                    StandardOpenOption.TRUNCATE_EXISTING,
                    StandardOpenOption.WRITE);
            Files.writeString(
                    reportRoot.resolve("README.md"),
                    markdown,
                    StandardCharsets.UTF_8,
                    StandardOpenOption.CREATE,
                    StandardOpenOption.TRUNCATE_EXISTING,
                    StandardOpenOption.WRITE);
            return new EvidenceReport(json, markdown, List.copyOf(metrics), jsonSha, sourceCommit, sourceLocksInputSha);
        } catch (IOException error) {
            throw new IllegalStateException("cannot generate receipt-cap evidence", error);
        }
    }

    static byte[] maximumRegistryFixture() {
        ByteBuffer output = ByteBuffer.allocate(MAX_REGISTRY_FIXTURE_BYTES);
        output.put("NVR1".getBytes(StandardCharsets.US_ASCII));
        output.putShort((short) 1);
        output.put(digestPrefix("deployment", 16));
        output.put(digestPrefix("reservation-domain", 16));
        output.put("123e4567-e89b-12d3-a456-426614174000".getBytes(StandardCharsets.US_ASCII));
        output.put(digest("ledger-namespace"));
        output.putLong(1L << 62);
        output.putLong(Long.MAX_VALUE - 1);
        output.putShort((short) 40);
        output.putInt(65_536);
        output.putShort((short) 256);
        output.putShort((short) 192);
        output.putShort((short) 14);
        output.putShort((short) 120);
        output.putLong(1);
        output.putShort((short) 1);
        output.putShort((short) 1);
        output.put(digest("registry-admission-evidence"));
        output.putShort((short) 14);
        output.putShort((short) 256);
        if (output.position() != 184) {
            throw new IllegalStateException("Registry fixture header differs from R0: " + output.position());
        }

        for (int index = 0; index < 14; index++) {
            int kind = index < 7 ? 1 : 2;
            long generation = index + 1L;
            output.putShort((short) kind);
            output.putShort((short) 1);
            output.putLong(generation);
            output.put(digest("principal/" + index));
            output.putLong(generation);
            output.put(digest("interlock/" + index));
            output.putShort((short) 1);
            output.putShort((short) 1);
            output.put(digest("cohort-evidence/" + index));
        }
        if (output.position() != 184 + 14 * 120) {
            throw new IllegalStateException("Registry fixture writer rows differ from R0");
        }

        long sliceSize = 1L << 40;
        for (int index = 0; index < 256; index++) {
            long start = (1L << 62) + index * sliceSize;
            output.putLong(start);
            output.putLong(start + sliceSize - 1);
            output.putInt(index);
            output.putShort((short) 1);
            output.putShort((short) 1);
            output.put(digestPrefix("cell/" + index, 16));
            output.put(digest("assignment/key/" + index));
            output.put(digest("assignment/value/" + index));
            output.put(digest("assignment/owner/" + index));
            output.put(digest("assignment/evidence/" + index));
            output.put(digestPrefix("assignment/deployment/" + index, 16));
            output.putLong(index + 1L);
        }
        if (output.hasRemaining()) {
            throw new IllegalStateException("Registry fixture did not fill 51,016 bytes");
        }
        return output.array();
    }

    static byte[] sanitizedLogExcerpt() {
        StringBuilder output = new StringBuilder();
        int ordinal = 1;
        for (RejectionCode code : RejectionCode.values()) {
            appendLogLine(
                    output,
                    ordinal++,
                    "PARSER_BOUNDARY",
                    code.name(),
                    "closed receipt field/path/count boundary rejected before authority or allocation");
        }
        for (String cut : faultCuts()) {
            appendLogLine(
                    output,
                    ordinal++,
                    "FAULT_CUT",
                    cut,
                    "deterministic cut retained one non-retried outcome and no secret/provider payload");
        }
        return output.toString().getBytes(StandardCharsets.UTF_8);
    }

    static byte[] maximumFailureReport() {
        StringBuilder xml = new StringBuilder();
        xml.append("<?xml version=\"1.0\" encoding=\"UTF-8\"?>\n");
        xml.append("<testsuite name=\"m1.receipt.maximum.failure\" tests=\"")
                .append(RejectionCode.values().length)
                .append("\" failures=\"")
                .append(RejectionCode.values().length)
                .append("\" errors=\"0\" skipped=\"0\">\n");
        int ordinal = 1;
        for (RejectionCode code : RejectionCode.values()) {
            xml.append("  <testcase classname=\"m1.receipt.boundary.")
                    .append(String.format(Locale.ROOT, "%02d", ordinal++))
                    .append("\" name=\"")
                    .append(code.name())
                    .append("\"><failure message=\"")
                    .append(code.name())
                    .append("\">stable-category=")
                    .append(code.name())
                    .append("; precedence=closed-parser-order; retry=false; authorityGranted=false; ")
                    .append("input=independently-named-boundary-vector</failure></testcase>\n");
        }
        xml.append("</testsuite>\n");
        return xml.toString().getBytes(StandardCharsets.UTF_8);
    }

    static List<String> faultCuts() {
        return List.of(
                "O1_NOTIFICATION_ERROR_BEFORE_READY",
                "O1_NOTIFICATION_ERROR_AFTER_READY",
                "O1_NOTIFICATION_COMPLETED",
                "O1_ASSIGNMENT_STREAM_LOSS",
                "O1_ASSIGNMENT_RESTORE_NO_DELTA",
                "O1_REASSIGNMENT",
                "O1_OLD_GENERATION_LATE_BATCH",
                "O1_CLIENT_CLOSE",
                "O2_CREATE_RESPONSE_UNKNOWN",
                "O2_CAS_RESPONSE_UNKNOWN",
                "O2_EXACT_REREAD_MISMATCH",
                "O2_CONTINUITY_LOSS_BEFORE_INSTALL",
                "O2_STALE_INSTALL_AFTER_READY",
                "R0_ADD_BEFORE_START",
                "R0_FENCE_BEFORE_DRAIN",
                "R0_DRAIN_BEFORE_REVOKE",
                "R0_REVOKE_BEFORE_REMOVE",
                "R0_FENCED_RESIDUE_RETAINED",
                "R0_OMITTED_AUTHORIZED_WRITER",
                "R0_UNAUTHORIZED_WRITER",
                "HARNESS_RESERVE_RESPONSE_UNKNOWN",
                "HARNESS_INSTALL_RESPONSE_UNKNOWN",
                "HARNESS_STALE_OWNER_AFTER_TAKEOVER",
                "HARNESS_BACKGROUND_CLEAR_DELAYED");
    }

    private static Sample foundationSample(SourceTuple source, List<InventoryRow> inventory) {
        List<InventoryRow> rows = inventory.stream()
                .filter(row -> !row.module().equals("nereus-metadata-oxia"))
                .toList();
        byte[] report = deterministicJUnitXml("m1.foundation.current", rows);
        return sample(
                "foundation",
                "REQUIRED_BASELINE_DOMAIN_AND_METADATA_SPI_JUNIT_INVENTORY",
                ReceiptKind.HARNESS_CONFORMANCE_ONLY,
                List.of(new ScenarioResult("V2-POSITION-010", suites(rows))),
                List.of(new AttachmentData(
                        AttachmentKind.TEST_REPORT,
                        "attachments/test-reports/foundation/current-domain-spi-junit.xml",
                        report)),
                source);
    }

    private static Sample o1Sample(SourceTuple source) {
        List<SuiteResult> suites = List.of(
                        VirtualLedgerReceiptV1.normalizeJUnit("o1.exactRuntimeCompatibility", 1, 0, 0, 0, 0),
                        VirtualLedgerReceiptV1.normalizeJUnit("o1.focusedLifecycleAndApi", 88, 0, 0, 0, 0))
                .stream()
                .sorted(Comparator.comparing(SuiteResult::suiteId))
                .toList();
        byte[] report = ("O1 focused counts: focused=88/88/88/0/0/0; full=365/365/365/0/0/0; "
                        + "exactRuntime=1/1/1/0/0/0; rawExactRuntimeXmlBytes=10058; "
                        + "clientCommit="
                        + OXIA_CLIENT_COMMIT
                        + "; serverCommit="
                        + OXIA_SERVER_COMMIT
                        + "\n")
                .getBytes(StandardCharsets.UTF_8);
        return sample(
                "o1",
                "LOCKED_O1_RECEIPT_COUNTS_AND_10058_BYTE_XML_FACT",
                ReceiptKind.HARNESS_CONFORMANCE_ONLY,
                List.of(new ScenarioResult("V2-POSITION-010", suites)),
                List.of(new AttachmentData(
                        AttachmentKind.TEST_REPORT,
                        "attachments/test-reports/oxia-client/exact-runtime/"
                                + "TEST-io.oxia.client.it.NotificationContinuityCompatibilityIT.xml",
                        report)),
                source);
    }

    private static Sample o2Sample(SourceTuple source, List<InventoryRow> inventory) {
        List<InventoryRow> rows = inventory.stream()
                .filter(row -> row.module().equals("nereus-metadata-oxia"))
                .toList();
        return sample(
                "o2",
                "REQUIRED_BASELINE_73_SUITE_303_TEST_METADATA_OXIA_INVENTORY",
                ReceiptKind.HARNESS_CONFORMANCE_ONLY,
                List.of(new ScenarioResult("V2-POSITION-010", suites(rows))),
                List.of(new AttachmentData(
                        AttachmentKind.TEST_REPORT,
                        "attachments/test-reports/o2/current-whole-module-junit.xml",
                        deterministicJUnitXml("m1.o2.current", rows))),
                source);
    }

    private static Sample nta1Sample(SourceTuple source) {
        List<SuiteResult> suites = List.of(
                        VirtualLedgerReceiptV1.normalizeJUnit("nta1.domainAtReceiptClose", 55, 0, 0, 0, 0),
                        VirtualLedgerReceiptV1.normalizeJUnit("nta1.metadataOxiaV2AtReceiptClose", 73, 0, 0, 0, 0),
                        VirtualLedgerReceiptV1.normalizeJUnit("nta1.metadataOxiaWholeAtReceiptClose", 303, 0, 0, 0, 0))
                .stream()
                .sorted(Comparator.comparing(SuiteResult::suiteId))
                .toList();
        String report = "NTA1 exact-local counts=55,73,303; failures=0; errors=0; skipped=0; "
                + "goldens=30b12e545168aa1b0e21a7af895e33e2408265a154be3d70df95e4b5ff27879b,"
                + "48a6db09d4d7708984501f6b5c5c2a7385cd77a2181af58b154854a9b8512979,"
                + "bca9855a872fc8a3ae14fd11c080c2e3bccdb057aadfcd431a660f98e8c3ae95,"
                + "14e04858189f8bf44379106b143cb9cad12c86145583c38b2220c3196dc87fad,"
                + "c678ac0be4215fbada2dce86a9b2a95572ccef453d90a8f8cd4f1bc38fd3d6e8,"
                + "90f45a1d09d59a6a677b40c261da913ea4166b66e3d387823c9c1c4ce0f2aaae\n";
        return sample(
                "nta1",
                "LOCKED_M1_1B_COUNTS_AND_SIX_GOLDEN_DIGESTS",
                ReceiptKind.HARNESS_CONFORMANCE_ONLY,
                List.of(new ScenarioResult("V2-POSITION-010", suites)),
                List.of(new AttachmentData(
                        AttachmentKind.TEST_REPORT,
                        "attachments/test-reports/nta1/exact-local-summary.txt",
                        report.getBytes(StandardCharsets.UTF_8))),
                source);
    }

    private static Sample registryReadinessSample(SourceTuple source, List<InventoryRow> inventory) {
        List<AttachmentData> attachments = List.of(
                new AttachmentData(
                        AttachmentKind.TEST_REPORT,
                        "attachments/test-reports/registry-readiness/all-current-m1-junit.xml",
                        deterministicJUnitXml("m1.registry.kind.complete", inventory)),
                new AttachmentData(
                        AttachmentKind.REGISTRY_BYTES,
                        "attachments/registry/maximum-nvr1-capacity-fixture.bin",
                        maximumRegistryFixture()),
                new AttachmentData(
                        AttachmentKind.REGISTRY_ADMISSION_EVIDENCE,
                        "attachments/registry-admission/maximum-cohort-evidence.json",
                        registryAdmissionEvidence(source.nereusCommit())),
                new AttachmentData(
                        AttachmentKind.WRITER_INTERLOCK_SNAPSHOT,
                        "attachments/writer-interlock/maximum-principal-snapshot.json",
                        writerInterlockSnapshot()),
                new AttachmentData(
                        AttachmentKind.SANITIZED_LOG_EXCERPT,
                        "attachments/sanitized-logs/maximum-named-fault-and-error-excerpt.jsonl",
                        sanitizedLogExcerpt()));
        return sample(
                "registry-readiness",
                "R0_184_PLUS_14X120_PLUS_256X192_AND_KIND_COMPLETE_BUNDLE",
                ReceiptKind.REGISTRY_CONFORMANCE,
                List.of(new ScenarioResult(
                        "V2-POSITION-003",
                        List.of(VirtualLedgerReceiptV1.normalizeJUnit("registry.capacity.readiness", 18, 0, 0, 0, 0)))),
                attachments,
                source);
    }

    private static Sample representativeAllPassSample(SourceTuple source, List<InventoryRow> inventory) {
        List<InventoryRow> oxiaRows = inventory.stream()
                .filter(row -> row.module().equals("nereus-metadata-oxia"))
                .toList();
        List<AttachmentData> attachments = splitReports(oxiaRows, 20);
        return sample(
                "representative-all-pass",
                "REQUIRED_BASELINE_O2_73_SUITES_SPLIT_INTO_20_NAMED_REPORTS",
                ReceiptKind.HARNESS_CONFORMANCE_ONLY,
                List.of(new ScenarioResult("V2-POSITION-010", suites(oxiaRows))),
                attachments,
                source);
    }

    private static Sample multiScenarioRegistrySample(SourceTuple source) {
        List<ScenarioResult> scenarios = new ArrayList<>();
        for (int ordinal = 3; ordinal <= 9; ordinal++) {
            scenarios.add(new ScenarioResult(
                    String.format(Locale.ROOT, "V2-POSITION-%03d", ordinal),
                    List.of(VirtualLedgerReceiptV1.normalizeJUnit(
                            String.format(Locale.ROOT, "registry.scenario.%03d", ordinal), 4 + ordinal, 0, 0, 0, 0))));
        }
        return sample(
                "multi-scenario-registry",
                "SEVEN_REGISTRY_CONFORMANCE_ROWS_WITHOUT_HARNESS_SUBSTITUTION",
                ReceiptKind.REGISTRY_CONFORMANCE,
                scenarios,
                List.of(),
                source);
    }

    private static Sample multiScenarioHarnessSample(SourceTuple source) {
        return sample(
                "multi-scenario-harness",
                "TWO_HARNESS_ROWS_WITH_SCHEMA_DERIVED_NON_SELECTION",
                ReceiptKind.HARNESS_CONFORMANCE_ONLY,
                List.of(
                        new ScenarioResult(
                                "V2-POSITION-010",
                                List.of(VirtualLedgerReceiptV1.normalizeJUnit(
                                        "harness.conformance.cuts", 12, 0, 0, 0, 0))),
                        new ScenarioResult(
                                "V2-POSITION-011",
                                List.of(VirtualLedgerReceiptV1.normalizeJUnit(
                                        "harness.range.takeover", 16, 0, 0, 0, 0)))),
                List.of(),
                source);
    }

    private static Sample multiSuiteSample(SourceTuple source, List<InventoryRow> inventory) {
        List<InventoryRow> oxiaRows = inventory.stream()
                .filter(row -> row.module().equals("nereus-metadata-oxia"))
                .toList();
        return sample(
                "multi-suite",
                "EXACT_73_REQUIRED_BASELINE_O2_SUITE_IDS_AND_COUNTS",
                ReceiptKind.HARNESS_CONFORMANCE_ONLY,
                List.of(new ScenarioResult("V2-POSITION-010", suites(oxiaRows))),
                List.of(),
                source);
    }

    private static Sample maximumFailureSample(SourceTuple source) {
        List<SuiteResult> suites = java.util.Arrays.stream(RejectionCode.values())
                .map(code -> new SuiteResult("failure." + code.name(), 1, 1, 0, 1, 0, 0))
                .sorted(Comparator.comparing(SuiteResult::suiteId))
                .toList();
        return sample(
                "maximum-failure",
                "ONE_DISTINCT_FAILED_CASE_PER_STABLE_REJECTION_CATEGORY",
                ReceiptKind.HARNESS_CONFORMANCE_ONLY,
                List.of(new ScenarioResult("V2-POSITION-010", suites)),
                List.of(new AttachmentData(
                        AttachmentKind.TEST_REPORT,
                        "attachments/test-reports/receipt-parser/maximum-distinct-failures.xml",
                        maximumFailureReport())),
                source);
    }

    private static Sample faultCutSample(SourceTuple source) {
        List<SuiteResult> suites = faultCuts().stream()
                .map(cut -> new SuiteResult("fault." + cut, 1, 1, 0, 0, 0, 1))
                .sorted(Comparator.comparing(SuiteResult::suiteId))
                .toList();
        return sample(
                "fault-cut",
                "ONE_DISTINCT_ABORTED_CASE_PER_NAMED_CONTINUITY_RESPONSE_AND_ROLLOUT_CUT",
                ReceiptKind.HARNESS_CONFORMANCE_ONLY,
                List.of(new ScenarioResult("V2-POSITION-010", suites)),
                List.of(new AttachmentData(
                        AttachmentKind.SANITIZED_LOG_EXCERPT,
                        "attachments/sanitized-logs/fault-cuts/complete-named-cut-excerpt.jsonl",
                        sanitizedLogExcerpt())),
                source);
    }

    private static Sample sample(
            String id,
            String generator,
            ReceiptKind kind,
            List<ScenarioResult> scenarios,
            List<AttachmentData> attachments,
            SourceTuple source) {
        List<ScenarioResult> sortedScenarios = scenarios.stream()
                .map(scenario -> new ScenarioResult(
                        scenario.scenarioId(),
                        scenario.suites().stream()
                                .sorted(Comparator.comparing(SuiteResult::suiteId))
                                .toList()))
                .sorted(Comparator.comparing(ScenarioResult::scenarioId))
                .toList();
        List<AttachmentData> sortedAttachments = attachments.stream()
                .sorted(Comparator.comparing(AttachmentData::path))
                .toList();
        List<AttachmentRef> references = sortedAttachments.stream()
                .map(attachment -> new AttachmentRef(
                        attachment.kind(),
                        attachment.path(),
                        attachment.bytes().length,
                        VirtualLedgerReceiptV1.sha256(attachment.bytes())))
                .toList();
        ReceiptRoot root = new ReceiptRoot(VirtualLedgerReceiptV1.SCHEMA, kind, source, sortedScenarios, references);
        VirtualLedgerReceiptV1.validate(root);
        return new Sample(id, generator, root, sortedAttachments);
    }

    private static List<SuiteResult> suites(List<InventoryRow> rows) {
        return rows.stream()
                .map(row -> VirtualLedgerReceiptV1.normalizeJUnit(
                        row.suiteId(), row.tests(), row.failures(), row.errors(), row.skipped(), 0))
                .sorted(Comparator.comparing(SuiteResult::suiteId))
                .toList();
    }

    private static List<AttachmentData> splitReports(List<InventoryRow> rows, int groups) {
        List<List<InventoryRow>> partitions = new ArrayList<>();
        for (int index = 0; index < groups; index++) {
            partitions.add(new ArrayList<>());
        }
        for (int index = 0; index < rows.size(); index++) {
            partitions.get(index % groups).add(rows.get(index));
        }
        List<AttachmentData> attachments = new ArrayList<>();
        for (int index = 0; index < partitions.size(); index++) {
            String ordinal = String.format(Locale.ROOT, "%02d", index + 1);
            attachments.add(new AttachmentData(
                    AttachmentKind.TEST_REPORT,
                    "attachments/test-reports/all-pass/gate-" + ordinal + "/current-junit.xml",
                    deterministicJUnitXml("m1.all.pass.gate." + ordinal, partitions.get(index))));
        }
        return List.copyOf(attachments);
    }

    private static byte[] deterministicJUnitXml(String name, List<InventoryRow> rows) {
        int tests = rows.stream().mapToInt(InventoryRow::tests).sum();
        int failures = rows.stream().mapToInt(InventoryRow::failures).sum();
        int errors = rows.stream().mapToInt(InventoryRow::errors).sum();
        int skipped = rows.stream().mapToInt(InventoryRow::skipped).sum();
        StringBuilder xml = new StringBuilder();
        xml.append("<?xml version=\"1.0\" encoding=\"UTF-8\"?>\n")
                .append("<testsuites name=\"")
                .append(name)
                .append("\" tests=\"")
                .append(tests)
                .append("\" failures=\"")
                .append(failures)
                .append("\" errors=\"")
                .append(errors)
                .append("\" skipped=\"")
                .append(skipped)
                .append("\">\n");
        for (InventoryRow row : rows) {
            xml.append("  <testsuite name=\"")
                    .append(row.suiteId())
                    .append("\" module=\"")
                    .append(row.module())
                    .append("\" tests=\"")
                    .append(row.tests())
                    .append("\" failures=\"")
                    .append(row.failures())
                    .append("\" errors=\"")
                    .append(row.errors())
                    .append("\" skipped=\"")
                    .append(row.skipped())
                    .append("\" observedXmlBytes=\"")
                    .append(row.xmlBytes())
                    .append("\">\n");
            for (int invocation = 1; invocation <= row.tests(); invocation++) {
                xml.append("    <testcase classname=\"")
                        .append(row.suiteId())
                        .append("\" name=\"observed-invocation-")
                        .append(String.format(Locale.ROOT, "%03d", invocation))
                        .append("\" source=\"current-junit-inventory\" retry=\"false\"/>\n");
            }
            xml.append("  </testsuite>\n");
        }
        xml.append("</testsuites>\n");
        return xml.toString().getBytes(StandardCharsets.UTF_8);
    }

    private static byte[] registryAdmissionEvidence(String sourceCommit) {
        StringBuilder json = new StringBuilder();
        json.append("{\"schema\":\"RegistryAdmissionEvidenceV1.capacity-fixture\",\"sourceCommit\":\"")
                .append(sourceCommit)
                .append("\",\"cohorts\":[");
        for (int index = 0; index < 14; index++) {
            if (index != 0) {
                json.append(',');
            }
            json.append("{\"cohortId\":\"")
                    .append(String.format(Locale.ROOT, "cohort-%02d", index + 1))
                    .append("\",\"writerKind\":\"")
                    .append(index < 7 ? "NATIVE_BOOKKEEPER_LEDGER_ID" : "NEREUS_VIRTUAL_LEDGER_ID")
                    .append("\",\"principalGeneration\":")
                    .append(index + 1)
                    .append(",\"principalSha256\":\"")
                    .append(VirtualLedgerReceiptV1.sha256(("principal/" + index).getBytes(StandardCharsets.UTF_8)))
                    .append("\",\"interlockSha256\":\"")
                    .append(VirtualLedgerReceiptV1.sha256(("interlock/" + index).getBytes(StandardCharsets.UTF_8)))
                    .append("\",\"proofs\":[\"source-qualified\",\"independently-revocable\","
                            + "\"negative-allocation\"]}");
        }
        json.append("],\"rawLogsEmbedded\":false,\"allocationAuthority\":false}\n");
        return json.toString().getBytes(StandardCharsets.UTF_8);
    }

    private static byte[] writerInterlockSnapshot() {
        StringBuilder json = new StringBuilder();
        json.append("{\"schema\":\"WriterInterlockSnapshotV1.capacity-fixture\",\"principals\":[");
        for (int index = 0; index < 14; index++) {
            if (index != 0) {
                json.append(',');
            }
            json.append("{\"principalId\":\"")
                    .append(String.format(Locale.ROOT, "principal-%02d", index + 1))
                    .append("\",\"generation\":")
                    .append(index + 1)
                    .append(",\"sourceQualified\":true,\"allocationCapable\":true,\"lifecycle\":\"")
                    .append(index < 10 ? "ACTIVE" : "FENCED_DRAINING")
                    .append("\",\"revocationIndependent\":true,\"interlockDigest\":\"")
                    .append(VirtualLedgerReceiptV1.sha256(
                            ("interlock/snapshot/" + index).getBytes(StandardCharsets.UTF_8)))
                    .append("\"}");
        }
        json.append("],\"rootMutationForbidden\":true,\"containsSecret\":false}\n");
        return json.toString().getBytes(StandardCharsets.UTF_8);
    }

    private static void appendLogLine(StringBuilder output, int ordinal, String family, String event, String detail) {
        output.append("{\"ordinal\":")
                .append(ordinal)
                .append(",\"family\":\"")
                .append(family)
                .append("\",\"event\":\"")
                .append(event)
                .append("\",\"detail\":\"")
                .append(detail)
                .append("; event-specific-id=")
                .append(VirtualLedgerReceiptV1.sha256((family + "/" + event).getBytes(StandardCharsets.UTF_8))
                        .substring(0, 16))
                .append("; sourceTuple=v2-m0; retry=false; leafPayload=false; secretFields=absent\","
                        + "\"outcome\":\"FAIL_CLOSED\"}\n");
    }

    private static SampleMetrics metrics(Sample sample) {
        byte[] rootBytes = VirtualLedgerReceiptV1.canonicalBytes(sample.root());
        long total = 0;
        long maxSingle = 0;
        int maxPathBytes = 0;
        int maxPathSegments = 0;
        long sanitizedLogBytes = 0;
        Map<AttachmentKind, Long> byKind = new EnumMap<>(AttachmentKind.class);
        for (AttachmentData attachment : sample.attachments()) {
            int bytes = attachment.bytes().length;
            total = VirtualLedgerReceiptV1.checkedAdd(total, bytes);
            maxSingle = Math.max(maxSingle, bytes);
            maxPathBytes = Math.max(maxPathBytes, attachment.path().length());
            maxPathSegments = Math.max(maxPathSegments, attachment.path().split("/", -1).length);
            if (attachment.kind() == AttachmentKind.SANITIZED_LOG_EXCERPT) {
                sanitizedLogBytes = Math.max(sanitizedLogBytes, bytes);
            }
            byKind.merge(attachment.kind(), (long) bytes, Long::sum);
        }
        int maxSuites = sample.root().scenarios().stream()
                .mapToInt(scenario -> scenario.suites().size())
                .max()
                .orElse(0);
        return new SampleMetrics(
                sample.id(),
                sample.generator(),
                rootBytes.length,
                VirtualLedgerReceiptV1.sha256(rootBytes),
                sample.root().scenarios().size(),
                maxSuites,
                sample.root().attachments().size(),
                maxSingle,
                total,
                maxPathBytes,
                maxPathSegments,
                sanitizedLogBytes,
                Map.copyOf(byKind));
    }

    private static String renderEvidenceJson(
            String sourceCommit, String sourceLocksInputSha, List<SampleMetrics> samples) {
        ObservedMaxima maxima = observedMaxima(samples);
        verifyDerivation(maxima);
        String sampleJson = samples.stream()
                .map(ReceiptV1CapacitySamples::renderSampleJson)
                .collect(Collectors.joining(",\n      "));
        return """
                {
                  "schemaVersion": 1,
                  "sourceTupleId": "v2-m0",
                  "result": "RECEIPT_CAPACITY_READINESS_ONLY",
                  "promotionEligible": false,
                  "productionReceiptParserImplemented": false,
                  "runtimeActivated": false,
                  "scenarioPromotion": false,
                  "m1Final": false,
                  "sampleRootsAreTestVectors": true,
                  "source": {
                    "nereusCommit": "%s",
                    "requiredBaselineCommit": "%s",
                    "sourceLocksInputSha256": "%s"
                  },
                  "caps": {
                    "canonicalRootBytes": 65536,
                    "scenariosPerReceipt": 16,
                    "suitesPerScenario": 128,
                    "attachmentsPerReceipt": 32,
                    "singleAttachmentBytes": 262144,
                    "totalAttachmentBytes": 524288,
                    "relativePathBytes": 256,
                    "relativePathSegments": 16,
                    "sanitizedLogBytes": 65536
                  },
                  "observations": {
                    "preM1_2BaselineJUnitXml": {
                      "suites": 91,
                      "tests": 386,
                      "bytes": 96248,
                      "largestLockedExternalXmlBytes": 10058
                    },
                    "closedM1VirtualLedgerScenarioRows": 9,
                    "registryMaximumBytes": 51016,
                    "maxima": {
                      "canonicalRootBytes": %d,
                      "scenariosInOneKindSpecificReceipt": %d,
                      "suitesPerScenario": %d,
                      "attachmentsPerReceipt": %d,
                      "generatedSingleAttachmentBytes": %d,
                      "kindCompleteBundleBytes": %d,
                      "relativePathBytes": %d,
                      "relativePathSegments": %d,
                      "sanitizedLogBytes": %d
                    },
                    "samples": [
                      %s
                    ]
                  },
                  "formulas": {
                    "canonicalRootBytes": "nextPowerOfTwoAtLeast(4 * observedRootBytes)",
                    "scenariosPerReceipt": "nextPowerOfTwoAtLeast(ceil(9 * 1.5))",
                    "suitesPerScenario": "nextPowerOfTwoAtLeast(ceil(73 * 1.5))",
                    "attachmentsPerReceipt": "nextPowerOfTwoAtLeast(5 closed kinds * 4 refs)",
                    "singleAttachmentBytes": "nextPowerOfTwoAtLeast(2 * max(96248, 51016))",
                    "totalAttachmentBytes": "%s",
                    "relativePathBytes": "nextPowerOfTwoAtLeast(2 * observedPathBytes)",
                    "relativePathSegments": "nextPowerOfTwoAtLeast(2 * observedSegments)",
                    "sanitizedLogBytes": "nextPowerOfTwoAtLeast(4 * observedSanitizedLogBytes)"
                  },
                  "testEvidence": {
                    "expectedFocusedTests": %d,
                    "expectedFailures": 0,
                    "expectedErrors": 0,
                    "expectedSkipped": 0,
                    "dynamicTests": false,
                    "internalRetries": false
                  },
                  "rejectionCodes": [%s],
                  "modelSha256": "%s",
                  "limitations": [
                    "NO_PRODUCTION_RECEIPT_PARSER_OR_G1_FINAL_GATE",
                    "NO_N1_K1_P1_OR_R1",
                    "NO_REAL_OXIA_OR_REGISTRY_CONFORMANCE",
                    "NO_SCENARIO_PROMOTION_OR_N3_RECEIPT",
                    "NO_10K_OR_100K_SCALE_BENCHMARK"
                  ]
                }
                """.formatted(
                        sourceCommit,
                        REQUIRED_BASELINE_COMMIT,
                        sourceLocksInputSha,
                        maxima.rootBytes(),
                        maxima.scenarios(),
                        maxima.suites(),
                        maxima.attachments(),
                        maxima.singleAttachmentBytes(),
                        maxima.totalAttachmentBytes(),
                        maxima.pathBytes(),
                        maxima.pathSegments(),
                        maxima.logBytes(),
                        sampleJson,
                        "max(nextPowerOfTwoAtLeast(2 * kindCompleteBundleBytes), 2 * singleAttachmentBytes)",
                        EXPECTED_FOCUSED_TESTS,
                        rejectionCodesJson(),
                        modelSha256(samples));
    }

    private static String renderEvidenceMarkdown(
            String sourceCommit, String sourceLocksInputSha, String jsonSha, List<SampleMetrics> samples) {
        ObservedMaxima maxima = observedMaxima(samples);
        return """
                # M1-2 receipt/parser capacity readiness evidence

                ## Result boundary

                `RECEIPT_CAPACITY_READINESS_ONLY`; `promotionEligible=false`;
                `productionReceiptParserImplemented=false`; `m1Final=false`; `scenarioPromotion=false`.

                This deterministic test/evidence-only artifact binds Nereus `%s`, the source-lock input SHA-256
                `%s`, %d focused tests, eleven named receipt samples, and exact root/attachment/path/log formulas. It
                does not implement G1, publish N1, enter K1/P1/R1, run real Oxia, promote a scenario, or publish
                any generated `REGISTRY_CONFORMANCE` / `HARNESS_CONFORMANCE_ONLY` test vector as an authoritative
                N2, N3, or M1 Final receipt.

                ## Authority and generation rules

                ADR 0084 is the sole normative cap table. `receipt-caps.json` is its machine-checked evidence
                projection; this Markdown does not maintain another numeric cap table.

                The baseline inventory is the sorted, exact module/suite count and XML-byte snapshot from the three
                focused Gradle test tasks at the required baseline. Representative reports retain every named suite
                and normalized count. The Registry attachment is the structured R0 `184 + 14*120 + 256*192` layout;
                maximum-failure and sanitized-log artifacts emit one distinct semantic row per stable rejection or
                named fault cut. No artifact is enlarged with an anonymous repeated string.

                The JSON records the executable formulas. Root headroom is fourfold because the largest sample does
                not simultaneously maximize the independently closed scenario axis; report/Registry, bundle, path,
                segment, and log margins use their stated twofold/fourfold or closed-kind composition rules.

                Observed maxima were %d root bytes, %d scenarios in one kind-specific root, %d suites, %d attachment
                references, %d generated single-attachment bytes, %d kind-complete bundle bytes, %d path bytes, %d
                segments, and %d sanitized-log bytes. The actual pre-M1-2 JUnit XML corpus at the required baseline is
                91 suites / 386 tests / 96,248 bytes; R0's structured Registry boundary is exactly 51,016 bytes.

                ## Artifact identity

                - JSON: `receipt-caps.json`
                - JSON SHA-256: `%s`
                - required baseline: `%s`
                - generated and committed JSON/Markdown bytes must be identical
                """.formatted(
                        sourceCommit,
                        sourceLocksInputSha,
                        EXPECTED_FOCUSED_TESTS,
                        maxima.rootBytes(),
                        maxima.scenarios(),
                        maxima.suites(),
                        maxima.attachments(),
                        maxima.singleAttachmentBytes(),
                        maxima.totalAttachmentBytes(),
                        maxima.pathBytes(),
                        maxima.pathSegments(),
                        maxima.logBytes(),
                        jsonSha,
                        REQUIRED_BASELINE_COMMIT);
    }

    private static String renderSampleJson(SampleMetrics sample) {
        String kindBytes = sample.bytesByKind().entrySet().stream()
                .sorted(Map.Entry.comparingByKey())
                .map(entry -> "\"" + entry.getKey().name() + "\":" + entry.getValue())
                .collect(Collectors.joining(","));
        return ("{\"id\":\"%s\",\"generator\":\"%s\",\"rootBytes\":%d,\"rootSha256\":\"%s\","
                        + "\"scenarios\":%d,\"maxSuitesPerScenario\":%d,\"attachments\":%d,"
                        + "\"maxSingleAttachmentBytes\":%d,\"totalAttachmentBytes\":%d,"
                        + "\"maxPathBytes\":%d,\"maxPathSegments\":%d,\"sanitizedLogBytes\":%d,"
                        + "\"bytesByKind\":{%s}}")
                .formatted(
                        sample.id(),
                        sample.generator(),
                        sample.rootBytes(),
                        sample.rootSha256(),
                        sample.scenarios(),
                        sample.maxSuitesPerScenario(),
                        sample.attachments(),
                        sample.maxSingleAttachmentBytes(),
                        sample.totalAttachmentBytes(),
                        sample.maxPathBytes(),
                        sample.maxPathSegments(),
                        sample.sanitizedLogBytes(),
                        kindBytes);
    }

    private record ObservedMaxima(
            int rootBytes,
            int scenarios,
            int suites,
            int attachments,
            long singleAttachmentBytes,
            long totalAttachmentBytes,
            int pathBytes,
            int pathSegments,
            long logBytes) {}

    private static ObservedMaxima observedMaxima(List<SampleMetrics> samples) {
        return new ObservedMaxima(
                samples.stream().mapToInt(SampleMetrics::rootBytes).max().orElseThrow(),
                samples.stream().mapToInt(SampleMetrics::scenarios).max().orElseThrow(),
                samples.stream()
                        .mapToInt(SampleMetrics::maxSuitesPerScenario)
                        .max()
                        .orElseThrow(),
                samples.stream().mapToInt(SampleMetrics::attachments).max().orElseThrow(),
                samples.stream()
                        .mapToLong(SampleMetrics::maxSingleAttachmentBytes)
                        .max()
                        .orElseThrow(),
                samples.stream()
                        .mapToLong(SampleMetrics::totalAttachmentBytes)
                        .max()
                        .orElseThrow(),
                samples.stream().mapToInt(SampleMetrics::maxPathBytes).max().orElseThrow(),
                samples.stream().mapToInt(SampleMetrics::maxPathSegments).max().orElseThrow(),
                samples.stream()
                        .mapToLong(SampleMetrics::sanitizedLogBytes)
                        .max()
                        .orElseThrow());
    }

    private static void verifyDerivation(ObservedMaxima maxima) {
        if (nextPowerOfTwo(Math.multiplyExact(maxima.rootBytes(), 4)) != MAX_CANONICAL_ROOT_BYTES
                || nextPowerOfTwo(14) != MAX_SCENARIOS
                || nextPowerOfTwo(110) != MAX_SUITES_PER_SCENARIO
                || nextPowerOfTwo(20) != MAX_ATTACHMENTS
                || nextPowerOfTwo(2 * Math.max(EXPECTED_PREEXISTING_XML_BYTES, MAX_REGISTRY_FIXTURE_BYTES))
                        != MAX_SINGLE_ATTACHMENT_BYTES
                || Math.max(
                                nextPowerOfTwo(Math.multiplyExact((int) maxima.totalAttachmentBytes(), 2)),
                                2 * MAX_SINGLE_ATTACHMENT_BYTES)
                        != MAX_TOTAL_ATTACHMENT_BYTES
                || nextPowerOfTwo(Math.multiplyExact(maxima.pathBytes(), 2)) != MAX_PATH_BYTES
                || nextPowerOfTwo(Math.multiplyExact(maxima.pathSegments(), 2)) != MAX_PATH_SEGMENTS
                || nextPowerOfTwo(Math.multiplyExact((int) maxima.logBytes(), 4)) != MAX_SANITIZED_LOG_BYTES) {
            throw new IllegalStateException(
                    "observed receipt samples no longer derive the selected v1 cap table: " + maxima);
        }
    }

    private static int nextPowerOfTwo(int value) {
        if (value <= 0 || value > (1 << 30)) {
            throw new IllegalArgumentException("power-of-two input out of range");
        }
        return 1 << (32 - Integer.numberOfLeadingZeros(value - 1));
    }

    private static String modelSha256(List<SampleMetrics> samples) {
        String sampleIdentity = samples.stream()
                .map(sample -> sample.id() + ":" + sample.rootBytes() + ":" + sample.rootSha256() + ":"
                        + sample.totalAttachmentBytes())
                .collect(Collectors.joining("|"));
        String errors =
                java.util.Arrays.stream(RejectionCode.values()).map(Enum::name).collect(Collectors.joining("|"));
        String caps = "root=65536;scenarios=16;suites=128;attachments=32;single=262144;total=524288;"
                + "path=256;segments=16;log=65536";
        return VirtualLedgerReceiptV1.sha256(
                (caps + ";samples=" + sampleIdentity + ";errors=" + errors).getBytes(StandardCharsets.UTF_8));
    }

    private static String rejectionCodesJson() {
        return java.util.Arrays.stream(RejectionCode.values())
                .map(code -> "\"" + code.name() + "\"")
                .collect(Collectors.joining(","));
    }

    private static SourceTuple sourceTuple(String sourceCommit, String sourceLocksSha256) {
        if (!Pattern.matches("[0-9a-f]{40}", sourceCommit)
                || !SHA256.matcher(sourceLocksSha256).matches()) {
            throw new IllegalArgumentException("sample source commit or source-lock SHA is malformed");
        }
        return new SourceTuple(
                sourceCommit,
                KAFKA_COMMIT,
                PULSAR_COMMIT,
                OXIA_CLIENT_COMMIT,
                OXIA_SERVER_COMMIT,
                OXIA_CLIENT_JAR_SHA,
                OXIA_CLIENT_POM_SHA,
                DOMAIN_JAR_SHA,
                DOMAIN_POM_SHA,
                OXIA_SERVER_IMAGE_DIGEST,
                sourceLocksSha256);
    }

    private static String evidenceSourceCommit() {
        if (Files.isRegularFile(SOURCE_COMMIT_PATH)) {
            return readAsciiToken(SOURCE_COMMIT_PATH, "[0-9a-f]{40}");
        }
        return REQUIRED_BASELINE_COMMIT;
    }

    private static String sourceLocksInputSha256() {
        if (Files.isRegularFile(SOURCE_LOCKS_INPUT_SHA_PATH)) {
            return readAsciiToken(SOURCE_LOCKS_INPUT_SHA_PATH, "[0-9a-f]{64}");
        }
        try {
            return VirtualLedgerReceiptV1.sha256(Files.readAllBytes(CURRENT_SOURCE_LOCKS));
        } catch (IOException error) {
            throw new IllegalStateException("cannot hash current source locks", error);
        }
    }

    private static String readAsciiToken(Path path, String pattern) {
        try {
            String value = Files.readString(path, StandardCharsets.US_ASCII).trim();
            if (!Pattern.matches(pattern, value)) {
                throw new IllegalStateException("malformed evidence identity file " + path);
            }
            return value;
        } catch (IOException error) {
            throw new IllegalStateException("cannot read evidence identity file " + path, error);
        }
    }

    private static byte[] digest(String value) {
        return java.util.HexFormat.of().parseHex(VirtualLedgerReceiptV1.sha256(value.getBytes(StandardCharsets.UTF_8)));
    }

    private static byte[] digestPrefix(String value, int bytes) {
        byte[] digest = digest(value);
        return java.util.Arrays.copyOf(digest, bytes);
    }
}
