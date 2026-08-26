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
import com.nereusstream.domain.registry.allocator.AllocatorCampaignCheckpointV2;
import com.nereusstream.domain.registry.allocator.AllocatorCampaignCheckpointV2.SourceBinding;
import com.nereusstream.domain.registry.allocator.AllocatorCampaignEvaluationSealV2;
import com.nereusstream.domain.registry.allocator.AllocatorCampaignPromotionGateV2;
import com.nereusstream.domain.registry.allocator.AllocatorCampaignPromotionGateV2.Decision;
import com.nereusstream.domain.registry.allocator.AllocatorCampaignPromotionGateV2.DecisionStatus;
import com.nereusstream.domain.registry.allocator.AllocatorCampaignPromotionGateV2.DiagnosticAttestation;
import com.nereusstream.domain.registry.allocator.AllocatorCampaignPromotionGateV2.DiagnosticScenario;
import com.nereusstream.domain.registry.allocator.AllocatorCampaignPromotionGateV2.JUnitSummary;
import java.io.IOException;
import java.nio.charset.StandardCharsets;
import java.nio.file.Files;
import java.nio.file.LinkOption;
import java.nio.file.Path;
import java.nio.file.StandardOpenOption;
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

/** Offline NACP2/NAEV2/NADV2 validation, sealing, and promotion-gate CLI. It never accesses Oxia. */
public final class M3V2AllocatorProtocolMain {
    private static final Pattern SHA256_HEX = Pattern.compile("[0-9a-f]{64}");
    private static final Set<String> DIAGNOSTIC_TESTS = Set.of(
            "strictWorkflowUsesRealOxia",
            "installedRangeReusesGrant",
            "rangeRenewalUsesCellCas",
            "conflictStormUsesFourIndependentCoordinators");

    private M3V2AllocatorProtocolMain() {}

    public static void main(String[] args) throws Exception {
        if (args.length == 0) {
            throw new IllegalArgumentException("allocator V2 protocol command is required");
        }
        switch (args[0]) {
            case "validate-checkpoint" -> validateCheckpoint(args);
            case "seal-evaluation" -> sealEvaluation(args);
            case "seal-diagnostic" -> sealDiagnostic(args);
            case "promotion-check" -> promotionCheck(args);
            default -> throw new IllegalArgumentException("unknown allocator V2 protocol command: " + args[0]);
        }
    }

    private static void validateCheckpoint(String[] args) throws IOException {
        requireLength(args, 7);
        CanonicalBytes encoded = readBounded(Path.of(args[1]), AllocatorCampaignCheckpointV2.MAX_ENCODED_BYTES);
        AllocatorCampaignCheckpointV2 checkpoint = AllocatorCampaignCheckpointV2.decode(encoded);
        requireSource(checkpoint.source(), source(args, 2));
        System.out.printf(
                "allocator V2 checkpoint valid: status=%s sequence=%d executed=%d dispositions=%d campaign=%s%n",
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
                checkpointPath, AllocatorCampaignCheckpointV2.MAX_ENCODED_BYTES);
        AllocatorCampaignCheckpointV2 checkpoint = AllocatorCampaignCheckpointV2.decode(checkpointBytes);
        requireSource(checkpoint.source(), source(args, 3));
        CanonicalBytes evaluation = AllocatorCampaignEvaluationSealV2.seal(checkpointBytes);
        writeCreateNew(output, evaluation.toByteArray());
        var decoded = AllocatorCampaignEvaluationSealV2.decode(evaluation);
        System.out.printf(
                "allocator V2 evaluation sealed: status=%s selectionEligible=%s checkpoint=%s%n",
                decoded.status(), decoded.selectionEligible(), decoded.checkpointDigest().toHex());
    }

    private static void sealDiagnostic(String[] args) throws Exception {
        requireLength(args, 8);
        Path junitPath = Path.of(args[1]);
        Path output = Path.of(args[2]);
        SourceBinding source = source(args, 3);
        byte[] junitBytes = readRegular(junitPath, 16 * 1024 * 1024);
        ParsedJUnit junit = parseJUnit(junitBytes);
        requireExactDiagnosticJUnit(junit);
        DiagnosticAttestation diagnostic = new DiagnosticAttestation(
                source,
                EnumSet.allOf(DiagnosticScenario.class),
                Sha256Digest.hash(CanonicalBytes.copyOf(junitBytes)));
        writeCreateNew(
                output,
                AllocatorCampaignPromotionGateV2.encodeDiagnostic(diagnostic).toByteArray());
        System.out.printf(
                "allocator V2 diagnostic sealed: tests=%d failures=0 errors=0 skips=0 junitSha256=%s%n",
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
                checkpointPath, AllocatorCampaignCheckpointV2.MAX_ENCODED_BYTES);
        CanonicalBytes diagnosticBytes = readBounded(diagnosticPath, 4_096);
        DiagnosticAttestation diagnostic = AllocatorCampaignPromotionGateV2.decodeDiagnostic(diagnosticBytes);
        byte[] diagnosticJUnitBytes = readRegular(diagnosticJUnitPath, 16 * 1024 * 1024);
        requireExactDiagnosticJUnit(parseJUnit(diagnosticJUnitBytes));
        byte[] formalJUnitBytes = readRegular(formalJUnitPath, 16 * 1024 * 1024);
        ParsedJUnit formalJUnit = parseJUnit(formalJUnitBytes);
        Set<Sha256Digest> attachments = attachmentDigests(attachmentDirectory);
        Decision decision = AllocatorCampaignPromotionGateV2.evaluate(
                evaluation,
                checkpoint,
                currentSource,
                attachments,
                diagnostic,
                Sha256Digest.hash(CanonicalBytes.copyOf(diagnosticJUnitBytes)),
                formalJUnit.summary());
        if (decision.status() != DecisionStatus.PROMOTABLE
                && decision.status() != DecisionStatus.NON_PROMOTABLE_EVALUATION) {
            throw new IllegalStateException("allocator V2 promotion integrity gate rejected: " + decision.status());
        }
        String selected = decision.selectedCandidate().map(Enum::name).orElse("NONE");
        String json = "{\"schema\":\"NEREUS_V2_M3_ALLOCATOR_PROMOTION_DECISION_V2\","
                + "\"status\":\""
                + decision.status()
                + "\",\"selectedCandidate\":\""
                + selected
                + "\",\"checkpointSha256\":\""
                + AllocatorCampaignCheckpointV2.digest(checkpoint).toHex()
                + "\",\"evaluationSha256\":\""
                + AllocatorCampaignCheckpointV2.digest(evaluation).toHex()
                + "\",\"diagnosticSha256\":\""
                + AllocatorCampaignCheckpointV2.digest(diagnosticBytes).toHex()
                + "\",\"diagnosticJUnitSha256\":\""
                + Sha256Digest.hash(CanonicalBytes.copyOf(diagnosticJUnitBytes)).toHex()
                + "\",\"formalJUnitSha256\":\""
                + Sha256Digest.hash(CanonicalBytes.copyOf(formalJUnitBytes)).toHex()
                + "\"}\n";
        writeCreateNew(output, json.getBytes(StandardCharsets.UTF_8));
        System.out.printf(
                "allocator V2 promotion checked: status=%s selectedCandidate=%s%n",
                decision.status(), selected);
    }

    private static void requireExactDiagnosticJUnit(ParsedJUnit junit) {
        if (junit.summary().failures() != 0
                || junit.summary().errors() != 0
                || junit.summary().skips() != 0
                || junit.summary().tests() != DIAGNOSTIC_TESTS.size()
                || !junit.testcaseNames().equals(DIAGNOSTIC_TESTS)) {
            throw new IllegalArgumentException("allocator V2 diagnostic JUnit inventory or result differs");
        }
    }

    private static Set<Sha256Digest> attachmentDigests(Path directory) throws IOException {
        if (!Files.isDirectory(directory, LinkOption.NOFOLLOW_LINKS)) {
            throw new IllegalArgumentException("allocator V2 attachment directory is absent or a link");
        }
        List<Path> files;
        try (var stream = Files.list(directory)) {
            files = stream.sorted().toList();
        }
        if (files.isEmpty() || files.size() > 328) {
            throw new IllegalArgumentException("allocator V2 attachment directory count differs");
        }
        Set<Sha256Digest> digests = new HashSet<>();
        for (Path file : files) {
            byte[] bytes = readRegular(file, 16 * 1024 * 1024);
            if (!digests.add(Sha256Digest.hash(CanonicalBytes.copyOf(bytes)))) {
                throw new IllegalArgumentException("allocator V2 attachment digest aliases another file");
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
            throw new IllegalArgumentException("allocator V2 JUnit root differs");
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
        Set<String> testcaseNames = new HashSet<>();
        for (int index = 0; index < suites.getLength(); index++) {
            Element suite = (Element) suites.item(index);
            if (suite.getElementsByTagName("testsuite").getLength() != 0) {
                throw new IllegalArgumentException("allocator V2 JUnit suite nesting differs");
            }
            tests += attribute(suite, "tests");
            failures += attribute(suite, "failures");
            errors += attribute(suite, "errors");
            skips += suite.hasAttribute("skipped") ? attribute(suite, "skipped") : 0;
            NodeList testcases = suite.getElementsByTagName("testcase");
            for (int testcase = 0; testcase < testcases.getLength(); testcase++) {
                Element testcaseElement = (Element) testcases.item(testcase);
                String testcaseName = testcaseElement.getAttribute("name");
                if (testcaseName.isBlank()) {
                    throw new IllegalArgumentException("allocator V2 JUnit testcase name differs");
                }
                testcaseNames.add(testcaseName);
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
            throw new IllegalArgumentException("allocator V2 JUnit summary differs from testcase outcomes");
        }
        return new ParsedJUnit(new JUnitSummary(tests, failures, errors, skips), Set.copyOf(testcaseNames));
    }

    private static long attribute(Element element, String name) {
        try {
            long value = Long.parseLong(element.getAttribute(name));
            if (value < 0) {
                throw new NumberFormatException("negative");
            }
            return value;
        } catch (NumberFormatException failure) {
            throw new IllegalArgumentException("allocator V2 JUnit " + name + " attribute differs", failure);
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
            throw new IllegalArgumentException("allocator V2 source digest is not lowercase SHA-256");
        }
        try {
            return Sha256Digest.copyOf(HexFormat.of().parseHex(hex));
        } catch (IllegalArgumentException failure) {
            throw new IllegalArgumentException("allocator V2 source digest is not lowercase SHA-256", failure);
        }
    }

    private static void requireSource(SourceBinding actual, SourceBinding expected) {
        if (!actual.equals(expected)) {
            throw new IllegalArgumentException("allocator V2 exact source/executor binding differs");
        }
    }

    private static CanonicalBytes readBounded(Path path, int maximum) throws IOException {
        return CanonicalBytes.copyOf(readRegular(path, maximum));
    }

    private static byte[] readRegular(Path path, int maximum) throws IOException {
        Objects.requireNonNull(path, "path");
        if (!Files.isRegularFile(path, LinkOption.NOFOLLOW_LINKS)) {
            throw new IllegalArgumentException("allocator V2 input is absent, non-regular, or a link: " + path);
        }
        long size = Files.size(path);
        if (size <= 0 || size > maximum) {
            throw new IllegalArgumentException("allocator V2 input length is outside its cap: " + path);
        }
        byte[] bytes = Files.readAllBytes(path);
        if (bytes.length == 0 || bytes.length > maximum) {
            throw new IllegalArgumentException("allocator V2 input changed outside its cap while reading: " + path);
        }
        return bytes;
    }

    private static void writeCreateNew(Path path, byte[] bytes) throws IOException {
        Path parent = path.toAbsolutePath().normalize().getParent();
        if (parent == null || !Files.isDirectory(parent, LinkOption.NOFOLLOW_LINKS)) {
            throw new IllegalArgumentException("allocator V2 output parent is absent or a link");
        }
        Files.write(path, bytes, StandardOpenOption.CREATE_NEW, StandardOpenOption.WRITE);
    }

    private static void requireLength(String[] args, int expected) {
        if (args.length != expected) {
            throw new IllegalArgumentException(
                    "allocator V2 protocol argument count differs for " + args[0]);
        }
    }

    private record ParsedJUnit(JUnitSummary summary, Set<String> testcaseNames) {}

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
