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

package com.nereusstream.metadata.oxia.v2.allocator.evidence;

import com.nereusstream.domain.bytes.CanonicalBytes;
import com.nereusstream.domain.bytes.Sha256Digest;
import com.nereusstream.domain.registry.allocator.AllocatorCampaignCheckpointV3;
import com.nereusstream.domain.registry.allocator.AllocatorCampaignCheckpointV3.SourceBinding;
import com.nereusstream.domain.registry.allocator.AllocatorCampaignEvaluationSealV3;
import com.nereusstream.domain.registry.allocator.AllocatorCampaignPromotionGateV3;
import com.nereusstream.domain.registry.allocator.AllocatorCampaignPromotionGateV3.Decision;
import com.nereusstream.domain.registry.allocator.AllocatorCampaignPromotionGateV3.DecisionStatus;
import com.nereusstream.domain.registry.allocator.AllocatorCampaignPromotionGateV3.DiagnosticAttestation;
import com.nereusstream.domain.registry.allocator.AllocatorCampaignPromotionGateV3.DiagnosticScenario;
import com.nereusstream.domain.registry.allocator.AllocatorCampaignPromotionGateV3.JUnitSummary;
import com.nereusstream.domain.registry.allocator.AllocatorCampaignSelectionV3;
import java.io.IOException;
import java.nio.charset.StandardCharsets;
import java.nio.file.Files;
import java.nio.file.LinkOption;
import java.nio.file.Path;
import java.nio.file.StandardOpenOption;
import java.util.Comparator;
import java.util.EnumSet;
import java.util.HashSet;
import java.util.HexFormat;
import java.util.List;
import java.util.Objects;
import java.util.Set;
import java.util.regex.Pattern;
import javax.xml.XMLConstants;
import javax.xml.parsers.DocumentBuilderFactory;
import org.w3c.dom.Document;
import org.w3c.dom.Element;
import org.w3c.dom.Node;
import org.w3c.dom.NodeList;

/** Offline NACP3/NAEV3/NADV3 validation, sealing, and promotion-gate CLI. It never accesses Oxia. */
public final class M3V3AllocatorProtocolMain {
    private static final Pattern SHA256_HEX = Pattern.compile("[0-9a-f]{64}");
    private static final String PACKAGE = "com.nereusstream.metadata.oxia.v2.allocator.evidence.";
    private static final Set<String> DIAGNOSTIC_SUITES = Set.of(
            PACKAGE + "M3V3AsyncActorLaneRunnerTest",
            PACKAGE + "M3V3RealOxiaOperationDiagnosticTest",
            PACKAGE + "M3V3AllocatorWorkflowDiagnosticTest",
            PACKAGE + "M3V3NativePathDiagnosticTest",
            PACKAGE + "M3V3NativeBaselineCanaryTest");
    private static final Set<String> DIAGNOSTIC_TESTS = Set.of(
            identity(
                    "M3V3AsyncActorLaneRunnerTest",
                    "evidenceAdmissionCapIsDerivedFromFrozenRateLatencyAndActorCount()"),
            identity(
                    "M3V3AsyncActorLaneRunnerTest",
                    "dispatcherReachesEveryFrozenOutstandingLevelWithoutBlockingOnCompletion()"),
            identity(
                    "M3V3AsyncActorLaneRunnerTest",
                    "controlledLatencyFuturesCoverFrozenAndDerivedRatesIncludingTwoHundredFiftyMillis()"),
            identity(
                    "M3V3AsyncActorLaneRunnerTest",
                    "twoHundredFiftyMillisAtOneThousandRpsReachesTheDerivedAsyncCap()"),
            identity(
                    "M3V3AsyncActorLaneRunnerTest",
                    "callbackReorderingStillProducesOneCanonicalTerminalPerOrdinal()"),
            identity(
                    "M3V3AsyncActorLaneRunnerTest",
                    "cutoffKeepsUndispatchedRequestsInThePreAdmissionDropPartition()"),
            identity(
                    "M3V3AsyncActorLaneRunnerTest",
                    "cleanupTimeoutClosesTheWorkflowGuardAndLateCompletionCannotDispatchNextOperation()"),
            identity(
                    "M3V3AsyncActorLaneRunnerTest",
                    "normalIntervalsSingleFlightBindingsWhileConflictProofRetainsSameKeyConcurrency()"),
            identity(
                    "M3V3AsyncActorLaneRunnerTest",
                    "everyFrozenRateRetainsOneOrdinalAuthoritativeMeasurementTransition()"),
            identity(
                    "M3V3AsyncActorLaneRunnerTest",
                    "scheduleRejectsWarmupAfterMeasurementAndRunnerContainsNoCorrectnessLockOrWorkerPool()"),
            identity(
                    "M3V3RealOxiaOperationDiagnosticTest",
                    "realOxiaOperationsRemainNonzeroAcrossEveryFrozenLatency()"),
            identity(
                    "M3V3AllocatorWorkflowDiagnosticTest",
                    "strictAndRangeRowsUseAsyncAdmissionAtTwoHundredAndFiveHundred()"),
            identity("M3V3AllocatorWorkflowDiagnosticTest", "fourActorSameCellConflictStormPreservesUniqueLedgerIds()"),
            identity("M3V3NativePathDiagnosticTest", "formalAndDiagnosticUseOneNonBlockingRuntimeAndFrozenSchedule()"),
            identity(
                    "M3V3NativeBaselineCanaryTest",
                    "exactFormalScheduleClearsAllNativeBaselinesAndRepresentativeRows()"));

    private M3V3AllocatorProtocolMain() {}

    public static void main(String[] args) throws Exception {
        if (args.length == 0) {
            throw new IllegalArgumentException("allocator V3 protocol command is required");
        }
        switch (args[0]) {
            case "validate-checkpoint" -> validateCheckpoint(args);
            case "seal-evaluation" -> sealEvaluation(args);
            case "seal-diagnostic" -> sealDiagnostic(args);
            case "validate-diagnostic" -> validateDiagnostic(args);
            case "promotion-check" -> promotionCheck(args);
            case "seal-selection" -> sealSelection(args);
            default -> throw new IllegalArgumentException("unknown allocator V3 protocol command: " + args[0]);
        }
    }

    private static void validateCheckpoint(String[] args) throws IOException {
        requireLength(args, 7);
        CanonicalBytes encoded = readBounded(Path.of(args[1]), AllocatorCampaignCheckpointV3.MAX_ENCODED_BYTES);
        AllocatorCampaignCheckpointV3 checkpoint = AllocatorCampaignCheckpointV3.decode(encoded);
        requireSource(checkpoint.source(), source(args, 2));
        System.out.printf(
                "allocator V3 checkpoint valid: status=%s sequence=%d executed=%d dispositions=%d campaign=%s%n",
                checkpoint.status(),
                checkpoint.checkpointSequence(),
                checkpoint.executionRecords().size(),
                checkpoint.dispositions().size(),
                checkpoint.campaignId().toHex());
    }

    private static void sealEvaluation(String[] args) throws IOException {
        requireLength(args, 8);
        Path checkpointPath = Path.of(args[1]);
        Path output = Path.of(args[2]);
        CanonicalBytes checkpointBytes = readBounded(
                checkpointPath, AllocatorCampaignCheckpointV3.MAX_ENCODED_BYTES);
        AllocatorCampaignCheckpointV3 checkpoint = AllocatorCampaignCheckpointV3.decode(checkpointBytes);
        requireSource(checkpoint.source(), source(args, 3));
        CanonicalBytes evaluation = AllocatorCampaignEvaluationSealV3.seal(checkpointBytes);
        writeCreateNew(output, evaluation.toByteArray());
        var decoded = AllocatorCampaignEvaluationSealV3.decode(evaluation);
        System.out.printf(
                "allocator V3 evaluation sealed: status=%s selectionEligible=%s checkpoint=%s%n",
                decoded.status(), decoded.selectionEligible(), decoded.checkpointDigest().toHex());
    }

    private static void sealDiagnostic(String[] args) throws Exception {
        requireLength(args, 8);
        Path junitPath = Path.of(args[1]);
        Path output = Path.of(args[2]);
        SourceBinding source = source(args, 3);
        DiagnosticSuite junit = readDiagnosticSuite(junitPath);
        requireExactDiagnosticJUnit(junit);
        DiagnosticAttestation diagnostic = new DiagnosticAttestation(
                source,
                EnumSet.allOf(DiagnosticScenario.class),
                junit.manifestDigest());
        writeCreateNew(
                output,
                AllocatorCampaignPromotionGateV3.encodeDiagnostic(diagnostic).toByteArray());
        System.out.printf(
                "allocator V3 diagnostic sealed: tests=%d failures=0 errors=0 skips=0 junitSha256=%s%n",
                junit.summary().tests(), diagnostic.receiptDigest().toHex());
    }

    private static void validateDiagnostic(String[] args) throws Exception {
        requireLength(args, 8);
        CanonicalBytes encoded = readBounded(Path.of(args[1]), 4_096);
        DiagnosticAttestation diagnostic = AllocatorCampaignPromotionGateV3.decodeDiagnostic(encoded);
        DiagnosticSuite junit = readDiagnosticSuite(Path.of(args[2]));
        requireExactDiagnosticJUnit(junit);
        requireSource(diagnostic.source(), source(args, 3));
        if (!diagnostic.scenarios().equals(EnumSet.allOf(DiagnosticScenario.class))
                || !diagnostic.receiptDigest().equals(junit.manifestDigest())) {
            throw new IllegalArgumentException("allocator V3 diagnostic attestation differs from its JUnit suite");
        }
        System.out.printf(
                "allocator V3 diagnostic canonical: tests=%d failures=0 errors=0 skips=0 junitSha256=%s%n",
                junit.summary().tests(), diagnostic.receiptDigest().toHex());
    }

    private static void promotionCheck(String[] args) throws Exception {
        requireLength(args, 13);
        Path evaluationPath = Path.of(args[1]);
        Path checkpointPath = Path.of(args[2]);
        Path diagnosticPath = Path.of(args[3]);
        Path diagnosticJUnitPath = Path.of(args[4]);
        Path formalJUnitPath = Path.of(args[5]);
        Path attachmentDirectory = Path.of(args[6]);
        Path output = Path.of(args[7]);
        SourceBinding currentSource = source(args, 8);
        CanonicalBytes evaluation = readBounded(evaluationPath, 4_096);
        CanonicalBytes checkpoint = readBounded(
                checkpointPath, AllocatorCampaignCheckpointV3.MAX_ENCODED_BYTES);
        CanonicalBytes diagnosticBytes = readBounded(diagnosticPath, 4_096);
        DiagnosticAttestation diagnostic = AllocatorCampaignPromotionGateV3.decodeDiagnostic(diagnosticBytes);
        DiagnosticSuite diagnosticJUnit = readDiagnosticSuite(diagnosticJUnitPath);
        requireExactDiagnosticJUnit(diagnosticJUnit);
        byte[] formalJUnitBytes = readRegular(formalJUnitPath, 16 * 1024 * 1024);
        ParsedJUnit formalJUnit = parseJUnit(formalJUnitBytes);
        Set<Sha256Digest> attachments = attachmentDigests(attachmentDirectory);
        Decision decision = AllocatorCampaignPromotionGateV3.evaluate(
                evaluation,
                checkpoint,
                currentSource,
                attachments,
                diagnostic,
                diagnosticJUnit.manifestDigest(),
                formalJUnit.summary());
        if (decision.status() != DecisionStatus.PROMOTABLE
                && decision.status() != DecisionStatus.NON_PROMOTABLE_EVALUATION) {
            throw new IllegalStateException("allocator V3 promotion integrity gate rejected: " + decision.status());
        }
        String selected = decision.selectedCandidate().map(Enum::name).orElse("NONE");
        String json = "{\"schema\":\"NEREUS_V2_M3_ALLOCATOR_PROMOTION_DECISION_V3\","
                + "\"status\":\""
                + decision.status()
                + "\",\"selectedCandidate\":\""
                + selected
                + "\",\"checkpointSha256\":\""
                + AllocatorCampaignCheckpointV3.digest(checkpoint).toHex()
                + "\",\"evaluationSha256\":\""
                + AllocatorCampaignCheckpointV3.digest(evaluation).toHex()
                + "\",\"diagnosticSha256\":\""
                + AllocatorCampaignCheckpointV3.digest(diagnosticBytes).toHex()
                + "\",\"diagnosticJUnitSha256\":\""
                + diagnosticJUnit.manifestDigest().toHex()
                + "\",\"formalJUnitSha256\":\""
                + Sha256Digest.hash(CanonicalBytes.copyOf(formalJUnitBytes)).toHex()
                + "\"}\n";
        writeCreateNew(output, json.getBytes(StandardCharsets.UTF_8));
        System.out.printf(
                "allocator V3 promotion checked: status=%s selectedCandidate=%s%n",
                decision.status(), selected);
    }

    private static void sealSelection(String[] args) throws Exception {
        requireLength(args, 13);
        CanonicalBytes evaluation = readBounded(Path.of(args[1]), 4_096);
        CanonicalBytes checkpoint = readBounded(
                Path.of(args[2]), AllocatorCampaignCheckpointV3.MAX_ENCODED_BYTES);
        CanonicalBytes diagnosticBytes = readBounded(Path.of(args[3]), 4_096);
        DiagnosticAttestation diagnostic = AllocatorCampaignPromotionGateV3.decodeDiagnostic(diagnosticBytes);
        DiagnosticSuite diagnosticJUnit = readDiagnosticSuite(Path.of(args[4]));
        requireExactDiagnosticJUnit(diagnosticJUnit);
        ParsedJUnit formalJUnit = parseJUnit(readRegular(Path.of(args[5]), 16 * 1024 * 1024));
        SourceBinding currentSource = source(args, 8);
        CanonicalBytes selection = AllocatorCampaignSelectionV3.seal(
                evaluation,
                checkpoint,
                currentSource,
                attachmentDigests(Path.of(args[6])),
                diagnostic,
                diagnosticJUnit.manifestDigest(),
                formalJUnit.summary());
        AllocatorCampaignSelectionV3.decode(selection);
        writeCreateNew(Path.of(args[7]), selection.toByteArray());
        System.out.printf(
                "allocator V3 selection sealed: candidate=%s selectionSha256=%s%n",
                AllocatorCampaignSelectionV3.decode(selection).selectedCandidate(),
                Sha256Digest.hash(selection).toHex());
    }

    private static void requireExactDiagnosticJUnit(DiagnosticSuite junit) {
        if (junit.summary().failures() != 0
                || junit.summary().errors() != 0
                || junit.summary().skips() != 0
                || junit.summary().tests() != DIAGNOSTIC_TESTS.size()
                || !junit.suiteNames().equals(DIAGNOSTIC_SUITES)
                || !junit.testcaseIdentities().equals(DIAGNOSTIC_TESTS)) {
            throw new IllegalArgumentException("allocator V3 diagnostic JUnit inventory or result differs");
        }
    }

    private static DiagnosticSuite readDiagnosticSuite(Path directory) throws Exception {
        if (!Files.isDirectory(directory, LinkOption.NOFOLLOW_LINKS) || Files.isSymbolicLink(directory)) {
            throw new IllegalArgumentException("allocator V3 diagnostic JUnit directory is absent or a link");
        }
        List<Path> files;
        try (var stream = Files.list(directory)) {
            files = stream.filter(path -> path.getFileName().toString().matches("TEST-[A-Za-z0-9_.]+\\.xml"))
                    .sorted(Comparator.comparing(path -> path.getFileName().toString()))
                    .toList();
        }
        if (files.size() != DIAGNOSTIC_SUITES.size()) {
            throw new IllegalArgumentException("allocator V3 diagnostic JUnit file inventory differs");
        }
        Set<String> fileNames = files.stream()
                .map(path -> path.getFileName().toString())
                .collect(java.util.stream.Collectors.toUnmodifiableSet());
        Set<String> expectedFileNames = DIAGNOSTIC_SUITES.stream()
                .map(suite -> "TEST-" + suite + ".xml")
                .collect(java.util.stream.Collectors.toUnmodifiableSet());
        if (!fileNames.equals(expectedFileNames)) {
            throw new IllegalArgumentException("allocator V3 diagnostic JUnit file identity differs");
        }
        long tests = 0;
        long failures = 0;
        long errors = 0;
        long skips = 0;
        Set<String> suites = new HashSet<>();
        Set<String> testsSeen = new HashSet<>();
        StringBuilder manifest = new StringBuilder("NEREUS_V2_M3_ALLOCATOR_DIAGNOSTIC_JUNIT_MANIFEST_V3\n");
        for (Path file : files) {
            byte[] bytes = readRegular(file, 16 * 1024 * 1024);
            ParsedJUnit parsed = parseJUnit(bytes);
            tests = Math.addExact(tests, parsed.summary().tests());
            failures = Math.addExact(failures, parsed.summary().failures());
            errors = Math.addExact(errors, parsed.summary().errors());
            skips = Math.addExact(skips, parsed.summary().skips());
            if (!suites.addAll(parsed.suiteNames()) || !testsSeen.addAll(parsed.testcaseIdentities())) {
                throw new IllegalArgumentException("allocator V3 diagnostic JUnit identities alias");
            }
            manifest.append(file.getFileName())
                    .append('\0')
                    .append(bytes.length)
                    .append('\0')
                    .append(Sha256Digest.hash(CanonicalBytes.copyOf(bytes)).toHex())
                    .append('\n');
        }
        Sha256Digest manifestDigest = Sha256Digest.hash(
                CanonicalBytes.copyOf(manifest.toString().getBytes(StandardCharsets.UTF_8)));
        return new DiagnosticSuite(
                new JUnitSummary(tests, failures, errors, skips), suites, testsSeen, manifestDigest);
    }

    private static Set<Sha256Digest> attachmentDigests(Path directory) throws IOException {
        if (!Files.isDirectory(directory, LinkOption.NOFOLLOW_LINKS)) {
            throw new IllegalArgumentException("allocator V3 attachment directory is absent or a link");
        }
        List<Path> files;
        try (var stream = Files.list(directory)) {
            files = stream.sorted().toList();
        }
        if (files.isEmpty() || files.size() > 328) {
            throw new IllegalArgumentException("allocator V3 attachment directory count differs");
        }
        Set<Sha256Digest> digests = new HashSet<>();
        for (Path file : files) {
            byte[] bytes = readRegular(file, 16 * 1024 * 1024);
            if (!digests.add(Sha256Digest.hash(CanonicalBytes.copyOf(bytes)))) {
                throw new IllegalArgumentException("allocator V3 attachment digest aliases another file");
            }
        }
        return Set.copyOf(digests);
    }

    private static ParsedJUnit parseJUnit(byte[] bytes) throws Exception {
        DocumentBuilderFactory factory = DocumentBuilderFactory.newInstance();
        factory.setFeature("http://apache.org/xml/features/disallow-doctype-decl", true);
        factory.setFeature("http://xml.org/sax/features/external-general-entities", false);
        factory.setFeature("http://xml.org/sax/features/external-parameter-entities", false);
        factory.setFeature(XMLConstants.FEATURE_SECURE_PROCESSING, true);
        factory.setAttribute(XMLConstants.ACCESS_EXTERNAL_DTD, "");
        factory.setAttribute(XMLConstants.ACCESS_EXTERNAL_SCHEMA, "");
        factory.setExpandEntityReferences(false);
        factory.setXIncludeAware(false);
        Document document = factory.newDocumentBuilder()
                .parse(new java.io.ByteArrayInputStream(bytes));
        Element root = document.getDocumentElement();
        if (!root.getTagName().equals("testsuite") && !root.getTagName().equals("testsuites")) {
            throw new IllegalArgumentException("allocator V3 JUnit root differs");
        }
        long tests = 0;
        long failures = 0;
        long errors = 0;
        long skips = 0;
        long observedTests = 0;
        long observedFailures = 0;
        long observedErrors = 0;
        long observedSkips = 0;
        NodeList suites = root.getTagName().equals("testsuite")
                ? new SingletonNodeList(root)
                : root.getElementsByTagName("testsuite");
        Set<String> suiteNames = new HashSet<>();
        Set<String> testcaseIdentities = new HashSet<>();
        for (int index = 0; index < suites.getLength(); index++) {
            Element suite = (Element) suites.item(index);
            if (suite.getElementsByTagName("testsuite").getLength() != 0) {
                throw new IllegalArgumentException("allocator V3 JUnit suite nesting differs");
            }
            String suiteName = suite.getAttribute("name");
            if (suiteName.isBlank() || !suiteNames.add(suiteName)) {
                throw new IllegalArgumentException("allocator V3 JUnit suite identity differs");
            }
            tests += attribute(suite, "tests");
            failures += attribute(suite, "failures");
            errors += attribute(suite, "errors");
            skips += suite.hasAttribute("skipped") ? attribute(suite, "skipped") : 0;
            NodeList testcases = suite.getElementsByTagName("testcase");
            for (int testcase = 0; testcase < testcases.getLength(); testcase++) {
                Element testcaseElement = (Element) testcases.item(testcase);
                String testcaseName = testcaseElement.getAttribute("name");
                String testcaseClass = testcaseElement.getAttribute("classname");
                if (testcaseName.isBlank()
                        || testcaseClass.isBlank()
                        || !testcaseIdentities.add(testcaseClass + '#' + testcaseName)) {
                    throw new IllegalArgumentException("allocator V3 JUnit testcase name differs");
                }
                observedTests++;
                observedFailures += testcaseElement.getElementsByTagName("failure").getLength();
                observedErrors += testcaseElement.getElementsByTagName("error").getLength();
                observedSkips += testcaseElement.getElementsByTagName("skipped").getLength();
            }
        }
        if (tests != observedTests
                || failures != observedFailures
                || errors != observedErrors
                || skips != observedSkips) {
            throw new IllegalArgumentException("allocator V3 JUnit summary differs from testcase outcomes");
        }
        return new ParsedJUnit(
                new JUnitSummary(tests, failures, errors, skips),
                Set.copyOf(suiteNames),
                Set.copyOf(testcaseIdentities));
    }

    private static long attribute(Element element, String name) {
        try {
            long value = Long.parseLong(element.getAttribute(name));
            if (value < 0) {
                throw new NumberFormatException("negative");
            }
            return value;
        } catch (NumberFormatException failure) {
            throw new IllegalArgumentException("allocator V3 JUnit " + name + " attribute differs", failure);
        }
    }

    private static SourceBinding source(String[] args, int offset) {
        return new SourceBinding(
                args[offset],
                digest(args[offset + 1]),
                digest(args[offset + 2]),
                digest(args[offset + 3]),
                digest(args[offset + 4]));
    }

    private static Sha256Digest digest(String hex) {
        if (!SHA256_HEX.matcher(hex).matches()) {
            throw new IllegalArgumentException("allocator V3 source digest is not lowercase SHA-256");
        }
        try {
            return Sha256Digest.copyOf(HexFormat.of().parseHex(hex));
        } catch (IllegalArgumentException failure) {
            throw new IllegalArgumentException("allocator V3 source digest is not lowercase SHA-256", failure);
        }
    }

    private static void requireSource(SourceBinding actual, SourceBinding expected) {
        if (!actual.equals(expected)) {
            throw new IllegalArgumentException("allocator V3 exact source/executor binding differs");
        }
    }

    private static CanonicalBytes readBounded(Path path, int maximum) throws IOException {
        return CanonicalBytes.copyOf(readRegular(path, maximum));
    }

    private static byte[] readRegular(Path path, int maximum) throws IOException {
        Objects.requireNonNull(path, "path");
        if (!Files.isRegularFile(path, LinkOption.NOFOLLOW_LINKS)) {
            throw new IllegalArgumentException("allocator V3 input is absent, non-regular, or a link: " + path);
        }
        long size = Files.size(path);
        if (size <= 0 || size > maximum) {
            throw new IllegalArgumentException("allocator V3 input length is outside its cap: " + path);
        }
        byte[] bytes = Files.readAllBytes(path);
        if (bytes.length == 0 || bytes.length > maximum) {
            throw new IllegalArgumentException("allocator V3 input changed outside its cap while reading: " + path);
        }
        return bytes;
    }

    private static void writeCreateNew(Path path, byte[] bytes) throws IOException {
        Path parent = path.toAbsolutePath().normalize().getParent();
        if (parent == null || !Files.isDirectory(parent, LinkOption.NOFOLLOW_LINKS)) {
            throw new IllegalArgumentException("allocator V3 output parent is absent or a link");
        }
        Files.write(path, bytes, StandardOpenOption.CREATE_NEW, StandardOpenOption.WRITE);
    }

    private static void requireLength(String[] args, int expected) {
        if (args.length != expected) {
            throw new IllegalArgumentException(
                    "allocator V3 protocol argument count differs for " + args[0]);
        }
    }

    private static String identity(String className, String testName) {
        return PACKAGE + className + '#' + testName;
    }

    private record ParsedJUnit(
            JUnitSummary summary, Set<String> suiteNames, Set<String> testcaseIdentities) {}

    private record DiagnosticSuite(
            JUnitSummary summary,
            Set<String> suiteNames,
            Set<String> testcaseIdentities,
            Sha256Digest manifestDigest) {}

    private static final class SingletonNodeList implements NodeList {
        private final Node value;

        private SingletonNodeList(Node value) {
            this.value = value;
        }

        @Override
        public Node item(int index) {
            return index == 0 ? value : null;
        }

        @Override
        public int getLength() {
            return 1;
        }
    }
}
