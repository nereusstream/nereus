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

package com.nereusstream.kafka.bookkeeper.object.evidence;

import java.io.IOException;
import java.io.StringReader;
import java.nio.ByteBuffer;
import java.nio.charset.StandardCharsets;
import java.nio.file.Files;
import java.nio.file.LinkOption;
import java.nio.file.Path;
import java.nio.file.StandardOpenOption;
import java.security.MessageDigest;
import java.security.NoSuchAlgorithmException;
import java.time.Instant;
import java.util.ArrayList;
import java.util.Comparator;
import java.util.HashSet;
import java.util.HexFormat;
import java.util.List;
import java.util.Objects;
import java.util.Set;
import javax.xml.XMLConstants;
import javax.xml.parsers.DocumentBuilderFactory;
import org.w3c.dom.Element;
import org.xml.sax.InputSource;

/** Closed source/JUnit-derived raw result for the M3 Kafka Object-WAL component gate. */
public final class KafkaObjectWalNativeResultV1 {
    public static final String SCHEMA = "nereus-v2-m3-native-result-v1";
    public static final String COMPONENT_KIND = "U_KAFKA_OBJECT_WAL";
    public static final String STATUS = "PASS";
    public static final String M6_EXCLUSION = "M6_NATIVE_BROKER_CONTROLLER_ACTIVATION";
    public static final int MAX_CANONICAL_BYTES = 256 * 1024;
    public static final String DEFAULT_COMMAND =
            "./gradlew :nereus-kafka-bookkeeper:test :nereus-kafka-bookkeeper:spotlessCheck "
                    + ":nereus-kafka-bookkeeper:checkstyleMain :nereus-kafka-bookkeeper:checkstyleTest";

    private static final String ZERO_SHA256 = "0".repeat(64);
    private static final Path MAIN_ROOT =
            Path.of("nereus-kafka-bookkeeper/src/main/java/com/nereusstream/kafka/bookkeeper/object");
    private static final Path TEST_ROOT =
            Path.of("nereus-kafka-bookkeeper/src/test/java/com/nereusstream/kafka/bookkeeper/object");
    private static final Path XML_ROOT = Path.of("nereus-kafka-bookkeeper/build/test-results/test");
    private static final Path BUILD_FILE = Path.of("nereus-kafka-bookkeeper/build.gradle.kts");
    private static final int SOURCE_ARTIFACT_COUNT = 51;
    private static final List<Path> M2_BRIDGE_SOURCES = List.of(
            Path.of("nereus-kafka-bookkeeper/src/main/java/com/nereusstream/kafka/bookkeeper/commit/"
                    + "KafkaCoherentCommitCoordinatorV1.java"),
            Path.of("nereus-kafka-bookkeeper/src/main/java/com/nereusstream/kafka/bookkeeper/protocol/"
                    + "KafkaPartitionPublicationCellV1.java"),
            Path.of("nereus-kafka-bookkeeper/src/main/java/com/nereusstream/kafka/bookkeeper/protocol/"
                    + "KafkaPartitionPublicationKindV1.java"),
            Path.of("nereus-kafka-bookkeeper/src/main/java/com/nereusstream/kafka/bookkeeper/protocol/"
                    + "KafkaPartitionPublicationOutcomeV1.java"),
            Path.of("nereus-kafka-bookkeeper/src/main/java/com/nereusstream/kafka/bookkeeper/protocol/"
                    + "KafkaPartitionObjectTailRetirementSlotV1.java"),
            Path.of("nereus-kafka-bookkeeper/src/main/java/com/nereusstream/kafka/bookkeeper/protocol/"
                    + "KafkaPartitionSpeculativeRollbackSlotV1.java"));
    private static final List<String> SUITES = List.of(
            "com.nereusstream.kafka.bookkeeper.object.nwkcp1.Nwkcp1CodecV1Test",
            "com.nereusstream.kafka.bookkeeper.object.nwkcp1.ObjectKafkaProtocolCheckpointStoreV1Test",
            "com.nereusstream.kafka.bookkeeper.object.nwkcp1.StorageObjectNwkcp1BackendV1Test",
            "com.nereusstream.kafka.bookkeeper.object.publication.KafkaObjectPublicationBridgeV1Test",
            "com.nereusstream.kafka.bookkeeper.object.evidence.KafkaObjectWalNativeResultV1Test");
    private static final List<Integer> SUITE_TEST_COUNTS = List.of(4, 3, 17, 14, 3);
    private static final List<String> WIRE_TESTS = List.of(
            id(SUITES.get(0), "roundTripsStrictWireKeyAndHead"),
            id(SUITES.get(0), "rejectsHeadOutsideExpectedRootBoundKeyContext"),
            id(SUITES.get(0), "rejectsHeaderCorruptionTrailingBytesAndNonCanonicalKeys"),
            id(SUITES.get(0), "rejectsNullHeadVectorElementAtConstruction"),
            id(SUITES.get(2), "rejectsSyntacticallyValidUnissuedCreatedAndHeadSelectedTokensBeforeProviderIo"));
    private static final List<String> RECOVERY_TESTS = List.of(
            id(SUITES.get(1), "convergesResponseLossAdvancesOrdinalAndTakesOver"),
            id(SUITES.get(1), "missingHeadWithoutAuthenticatedReplayFailsClosed"),
            id(SUITES.get(1), "definitiveProviderConflictFailsWithoutUnknownOutcomeReread"),
            id(SUITES.get(2), "mapsC1ProviderAndExactControlCasIncludingResponseLoss"),
            id(SUITES.get(2), "authenticatedEmptyPhysicalChainProducesBoundedRecoveryStateAndTail"),
            id(SUITES.get(2), "authenticatedPhysicalSuffixMergesInterleavedLanesAndCleansExactTempSpools"),
            id(SUITES.get(2), "sameLaneNonMonotonicKafkaOffsetsFailAndCleanTempSpools"),
            id(SUITES.get(2), "exactRecoveryDiskCapRejectsOverflowAndOneByteOverbound"),
            id(SUITES.get(2), "nativeDurableOwnerAuthorityRejectsRegressionEscapeSubstitutionAndReleasesFailure"),
            id(SUITES.get(2), "truncatedOrTamperedRecoveryLaneSpoolFailsAndAlwaysCleansFiles"),
            id(SUITES.get(2), "terminalHeadRequiresExactPhysicalClosureAndFencesFurtherPublicationAndTakeover"),
            id(SUITES.get(2), "unresolvedLaneZeroCandidateDoesNotStopLaneOneAndExactRetryDoesNotRepeatPut"),
            id(SUITES.get(2), "unresolvedProviderCandidateForbidsSealAndCreatesNoSealMetadata"),
            id(SUITES.get(2), "underboundStreamingCheckpointBudgetFailsBeforeAnyPageMetadataRead"),
            id(SUITES.get(2), "publicationReadFailureRetriesFromProviderExactWithoutSecondPut"),
            id(SUITES.get(2), "freshSessionCannotReplayAndPerformsNoMetadataOrProviderIo"),
            id(SUITES.get(2), "emptyTerminalClosureUsesRootBoundPublisherAndObjectSession"));
    private static final List<String> ACK_TESTS = List.of(
            id(SUITES.get(3), "repositoryRootSelectsLocatorNativeStateAndQueueBeforeAckThenRetainsLocatorBudget"),
            id(SUITES.get(3), "ackFailureRetainsRootPublishedTicketAndRetryDoesNotRepublish"),
            id(SUITES.get(2), "checkpointIoFailureRetainsDebtButCurrentVerifiedMemberReachesM2Tracker"));
    private static final List<String> ROLLBACK_TESTS = List.of(
            id(SUITES.get(3), "wholeSuffixRollbackUsesRepositoryRootCasThenReleasesExactSuffixTicket"),
            id(SUITES.get(3), "issuedWholeSuffixRollbackFencesSequenceUntilExactRootCasCompletes"),
            id(SUITES.get(3), "noEffectSequenceClaimFreezesRollbackAndAbortRestoresExactEligibility"),
            id(SUITES.get(3), "rollbackAfterSequenceEffectLeavesRootAndExactTicketIntact"),
            id(SUITES.get(3), "rollbackValidatesExactTicketCommitPairBeforeRootCas"));
    private static final List<String> NATIVE_STATE_TESTS = List.of(
            id(SUITES.get(3), "invalidNativeCutFailsBeforeRootCasAndLeavesTicketAndLocatorUnpublished"),
            id(SUITES.get(3), "forgedLastStableOffsetCannotExposeAnOpenTransaction"));
    private static final List<String> LOCATOR_TESTS = List.of(
            id(SUITES.get(3), "locatorReservationUsesTheActualCanonicalLocatorWireCharge"),
            id(SUITES.get(3), "retainedLocatorBudgetRequiresManifestBoundRootRetirementAndExactPinDrain"),
            id(SUITES.get(3), "takeoverKeepsRootSelectedLocatorBudgetUntilTypedRetirement"));
    private static final List<String> ISOLATION_TESTS = List.of(
            id(SUITES.get(3), "sharedPhysicalAndBindingFailureDomainsRemainIndependent"),
            id(SUITES.get(2), "sharedPhysicalObjectUsesOneFullGetAndIsolatesRealMemberFailureFromSibling"),
            id(SUITES.get(3), "takeoverRejectsAssignedPositionsAndInvalidatesOnlyUnassignedReservationValues"));
    private static final List<String> RECEIPT_TESTS = List.of(
            id(SUITES.get(4), "roundTripsClosedRawResultWithExactCountersAndM6Exclusion"),
            id(SUITES.get(4), "rejectsCallerStatusTrailingBytesAndSelfHashTampering"),
            id(SUITES.get(4), "rejectsFailureErrorSkipAndNonCanonicalInventory"));
    private static final List<String> REQUIRED_TESTS = computeRequiredTests();

    private KafkaObjectWalNativeResultV1() {}

    public record TestedSource(String repository, String commit, String treeSha256) {
        public TestedSource {
            repository = text(repository, "tested repository");
            requireCommit(commit);
            requireSha256(treeSha256);
        }
    }

    public record ExternalSource(String repository, String commit) {
        public ExternalSource {
            repository = text(repository, "external repository");
            requireCommit(commit);
        }
    }

    public record Execution(String command, String startedAtUtc, String finishedAtUtc, String jvm, String os) {
        public Execution {
            command = text(command, "execution command");
            jvm = text(jvm, "JVM identity");
            os = text(os, "OS identity");
            if (Instant.parse(finishedAtUtc).isBefore(Instant.parse(startedAtUtc))) {
                throw new IllegalArgumentException("execution finish precedes start");
            }
        }
    }

    public record JunitFile(String path, String sha256, int tests, int failures, int errors, int skipped) {
        public JunitFile {
            path = relativePath(path);
            requireSha256(sha256);
            if (tests <= 0 || failures < 0 || errors < 0 || skipped < 0) {
                throw new IllegalArgumentException("JUnit counters are invalid");
            }
        }
    }

    public record JunitTotals(int suites, int tests, int failures, int errors, int skipped) {
        public JunitTotals {
            if (suites <= 0 || tests <= 0 || failures < 0 || errors < 0 || skipped < 0) {
                throw new IllegalArgumentException("JUnit totals are invalid");
            }
        }
    }

    public record JunitEvidence(String xmlRoot, List<JunitFile> xmlFiles, JunitTotals totals) {
        public JunitEvidence {
            xmlRoot = relativePath(xmlRoot);
            xmlFiles = List.copyOf(Objects.requireNonNull(xmlFiles, "JUnit XML files"));
            Objects.requireNonNull(totals, "JUnit totals");
        }
    }

    public record Counters(
            int checkpointWireTests,
            int rootBoundRecoveryTests,
            int ackOrderingTests,
            int wholeSuffixRollbackTests,
            int nativeStateIntegrityTests,
            int locatorRetirementTests,
            int sharedIsolationTests,
            int rawReceiptTests,
            int completionTicketBits,
            int m6ActivationClaims) {
        public Counters {
            if (checkpointWireTests != WIRE_TESTS.size()
                    || rootBoundRecoveryTests != RECOVERY_TESTS.size()
                    || ackOrderingTests != ACK_TESTS.size()
                    || wholeSuffixRollbackTests != ROLLBACK_TESTS.size()
                    || nativeStateIntegrityTests != NATIVE_STATE_TESTS.size()
                    || locatorRetirementTests != LOCATOR_TESTS.size()
                    || sharedIsolationTests != ISOLATION_TESTS.size()
                    || rawReceiptTests != RECEIPT_TESTS.size()
                    || completionTicketBits != Long.SIZE
                    || m6ActivationClaims != 0) {
                throw new IllegalArgumentException("Kafka Object-WAL counters differ from the closed contract");
            }
        }
    }

    public record Artifact(String path, String sha256, long bytes) {
        public Artifact {
            path = relativePath(path);
            requireSha256(sha256);
            if (bytes <= 0) {
                throw new IllegalArgumentException("artifact must be non-empty");
            }
        }
    }

    public record Receipt(
            String schema,
            String componentKind,
            String status,
            TestedSource testedSource,
            List<ExternalSource> externalSources,
            Execution execution,
            JunitEvidence junit,
            List<String> requiredTests,
            Counters counters,
            List<Artifact> artifacts,
            List<String> exclusions,
            String receiptSha256) {
        public Receipt {
            Objects.requireNonNull(testedSource, "testedSource");
            externalSources = List.copyOf(Objects.requireNonNull(externalSources, "externalSources"));
            Objects.requireNonNull(execution, "execution");
            Objects.requireNonNull(junit, "junit");
            requiredTests = List.copyOf(Objects.requireNonNull(requiredTests, "requiredTests"));
            Objects.requireNonNull(counters, "counters");
            artifacts = List.copyOf(Objects.requireNonNull(artifacts, "artifacts"));
            exclusions = List.copyOf(Objects.requireNonNull(exclusions, "exclusions"));
            requireSha256(receiptSha256);
        }
    }

    public static Receipt generate(
            Path repositoryRoot,
            String nereusCommit,
            String kafkaRepository,
            String kafkaCommit,
            String startedAtUtc,
            String finishedAtUtc)
            throws IOException {
        Path root = exactDirectory(repositoryRoot);
        List<Path> sources = sourceFiles(root);
        TestedSource tested = new TestedSource("nereus", nereusCommit, treeSha256(root, sources));
        Receipt unsigned = new Receipt(
                SCHEMA,
                COMPONENT_KIND,
                STATUS,
                tested,
                List.of(new ExternalSource(kafkaRepository, kafkaCommit)),
                new Execution(
                        DEFAULT_COMMAND,
                        startedAtUtc,
                        finishedAtUtc,
                        System.getProperty("java.vm.name") + " " + System.getProperty("java.version"),
                        System.getProperty("os.name") + " " + System.getProperty("os.version") + " "
                                + System.getProperty("os.arch")),
                readJunit(root),
                REQUIRED_TESTS,
                counters(),
                artifacts(root, sources),
                List.of(M6_EXCLUSION),
                ZERO_SHA256);
        return withReceiptSha(unsigned, sha256(canonicalBytesUnchecked(unsigned)));
    }

    public static byte[] canonicalBytes(Receipt receipt) {
        validate(receipt);
        byte[] result = canonicalBytesUnchecked(receipt);
        if (result.length > MAX_CANONICAL_BYTES) {
            throw new IllegalArgumentException("native receipt exceeds its canonical byte cap");
        }
        return result;
    }

    public static Receipt parseCanonical(byte[] bytes) {
        Objects.requireNonNull(bytes, "canonical receipt bytes");
        if (bytes.length == 0 || bytes.length > MAX_CANONICAL_BYTES) {
            throw new IllegalArgumentException("native receipt byte length is invalid");
        }
        Cursor cursor = new Cursor(new String(bytes, StandardCharsets.UTF_8));
        cursor.expect("{\"schema\":");
        String schema = cursor.string();
        cursor.expect(",\"componentKind\":");
        String component = cursor.string();
        cursor.expect(",\"status\":");
        String status = cursor.string();
        cursor.expect(",\"testedSource\":");
        TestedSource tested = parseTestedSource(cursor);
        cursor.expect(",\"externalSources\":");
        List<ExternalSource> external = parseExternalSources(cursor);
        cursor.expect(",\"execution\":");
        Execution execution = parseExecution(cursor);
        cursor.expect(",\"junit\":");
        JunitEvidence junit = parseJunit(cursor);
        cursor.expect(",\"requiredTests\":");
        List<String> required = cursor.stringArray();
        cursor.expect(",\"counters\":");
        Counters counters = parseCounters(cursor);
        cursor.expect(",\"artifacts\":");
        List<Artifact> artifacts = parseArtifacts(cursor);
        cursor.expect(",\"exclusions\":");
        List<String> exclusions = cursor.stringArray();
        cursor.expect(",\"receiptSha256\":");
        String receiptSha = cursor.string();
        cursor.expect("}");
        cursor.eof();
        Receipt receipt = new Receipt(
                schema,
                component,
                status,
                tested,
                external,
                execution,
                junit,
                required,
                counters,
                artifacts,
                exclusions,
                receiptSha);
        validate(receipt);
        if (!MessageDigest.isEqual(bytes, canonicalBytesUnchecked(receipt))) {
            throw new IllegalArgumentException("native receipt is not exact canonical JSON");
        }
        return receipt;
    }

    public static void writeCanonical(Path output, Receipt receipt) throws IOException {
        Objects.requireNonNull(output, "output");
        if (Files.exists(output, LinkOption.NOFOLLOW_LINKS) && Files.isSymbolicLink(output)) {
            throw new IllegalArgumentException("native receipt output cannot be a symbolic link");
        }
        Files.createDirectories(Objects.requireNonNull(output.toAbsolutePath().getParent(), "receipt parent"));
        Files.write(output, canonicalBytes(receipt), StandardOpenOption.CREATE_NEW, StandardOpenOption.WRITE);
    }

    /** CLI: generate the raw artifact, or parse-check one existing canonical artifact. */
    public static void main(String[] arguments) throws Exception {
        if (arguments.length == 2 && "parse".equals(arguments[0])) {
            Receipt parsed = parseCanonical(Files.readAllBytes(Path.of(arguments[1])));
            System.out.println(parsed.componentKind() + " " + parsed.receiptSha256());
            return;
        }
        if (arguments.length != 8 || !"generate".equals(arguments[0])) {
            throw new IllegalArgumentException(
                    "usage: generate <repo-root> <nereus-commit> <kafka-repository> <kafka-commit> "
                            + "<started-at-utc> <finished-at-utc> <output> | parse <receipt>");
        }
        writeCanonical(
                Path.of(arguments[7]),
                generate(Path.of(arguments[1]), arguments[2], arguments[3], arguments[4], arguments[5], arguments[6]));
    }

    static Receipt sealForTest(Receipt unsigned) {
        if (!ZERO_SHA256.equals(unsigned.receiptSha256())) {
            throw new IllegalArgumentException("test receipt projection must be unsigned");
        }
        return withReceiptSha(unsigned, sha256(canonicalBytesUnchecked(unsigned)));
    }

    static List<String> requiredTestsForTest() {
        return REQUIRED_TESTS;
    }

    static Counters countersForTest() {
        return counters();
    }

    private static Receipt withReceiptSha(Receipt source, String sha) {
        return new Receipt(
                source.schema(),
                source.componentKind(),
                source.status(),
                source.testedSource(),
                source.externalSources(),
                source.execution(),
                source.junit(),
                source.requiredTests(),
                source.counters(),
                source.artifacts(),
                source.exclusions(),
                sha);
    }

    private static void validate(Receipt receipt) {
        Objects.requireNonNull(receipt, "receipt");
        if (!SCHEMA.equals(receipt.schema())
                || !COMPONENT_KIND.equals(receipt.componentKind())
                || !STATUS.equals(receipt.status())
                || !"nereus".equals(receipt.testedSource().repository())
                || !receipt.requiredTests().equals(REQUIRED_TESTS)
                || !receipt.exclusions().equals(List.of(M6_EXCLUSION))
                || receipt.externalSources().size() != 1
                || !"apache/kafka".equals(receipt.externalSources().get(0).repository())
                || !DEFAULT_COMMAND.equals(receipt.execution().command())
                || !slash(XML_ROOT).equals(receipt.junit().xmlRoot())
                || !validArtifactInventory(receipt.artifacts())) {
            throw new IllegalArgumentException("native receipt closed identity/inventory differs");
        }
        JunitTotals totals = receipt.junit().totals();
        if (totals.suites() != SUITES.size()
                || receipt.junit().xmlFiles().size() != SUITES.size()
                || totals.failures() != 0
                || totals.errors() != 0
                || totals.skipped() != 0) {
            throw new IllegalArgumentException("native receipt requires exact non-empty zero-failure/skip suites");
        }
        for (int index = 0; index < SUITES.size(); index++) {
            JunitFile file = receipt.junit().xmlFiles().get(index);
            String exactPath = slash(XML_ROOT.resolve("TEST-" + SUITES.get(index) + ".xml"));
            if (!exactPath.equals(file.path())
                    || file.tests() != SUITE_TEST_COUNTS.get(index)
                    || file.failures() != 0
                    || file.errors() != 0
                    || file.skipped() != 0) {
                throw new IllegalArgumentException("native receipt JUnit suite identity/counters differ");
            }
        }
        if (receipt.junit().xmlFiles().stream().mapToInt(JunitFile::tests).sum() != totals.tests()
                || receipt.junit().xmlFiles().stream()
                                .mapToInt(JunitFile::failures)
                                .sum()
                        != totals.failures()
                || receipt.junit().xmlFiles().stream()
                                .mapToInt(JunitFile::errors)
                                .sum()
                        != totals.errors()
                || receipt.junit().xmlFiles().stream()
                                .mapToInt(JunitFile::skipped)
                                .sum()
                        != totals.skipped()) {
            throw new IllegalArgumentException("native receipt JUnit totals do not add up");
        }
        Receipt unsigned = withReceiptSha(receipt, ZERO_SHA256);
        if (!sha256(canonicalBytesUnchecked(unsigned)).equals(receipt.receiptSha256())) {
            throw new IllegalArgumentException("native receipt SHA-256 does not bind its exact unsigned projection");
        }
    }

    private static JunitEvidence readJunit(Path root) throws IOException {
        List<JunitFile> files = new ArrayList<>();
        Set<String> observedTests = new HashSet<>();
        for (String suite : SUITES) {
            Path relative = XML_ROOT.resolve("TEST-" + suite + ".xml");
            Path xml = exactRegularFile(root, relative);
            Element document = parseXml(Files.readString(xml, StandardCharsets.UTF_8));
            if (!"testsuite".equals(document.getTagName()) || !suite.equals(document.getAttribute("name"))) {
                throw new IllegalArgumentException("JUnit XML suite identity differs: " + relative);
            }
            int tests = integerAttribute(document, "tests");
            int failures = integerAttribute(document, "failures");
            int errors = integerAttribute(document, "errors");
            int skipped = integerAttribute(document, "skipped");
            var cases = document.getElementsByTagName("testcase");
            if (cases.getLength() != tests) {
                throw new IllegalArgumentException("JUnit XML declared testcase count differs: " + relative);
            }
            for (int index = 0; index < cases.getLength(); index++) {
                Element test = (Element) cases.item(index);
                String className = test.getAttribute("classname");
                if (className.isEmpty()) {
                    className = suite;
                }
                String testName = test.getAttribute("name");
                if (testName.endsWith("()")) {
                    testName = testName.substring(0, testName.length() - 2);
                }
                observedTests.add(id(className, testName));
            }
            files.add(
                    new JunitFile(slash(relative), sha256(Files.readAllBytes(xml)), tests, failures, errors, skipped));
        }
        if (!observedTests.containsAll(REQUIRED_TESTS)) {
            Set<String> missing = new HashSet<>(REQUIRED_TESTS);
            missing.removeAll(observedTests);
            throw new IllegalArgumentException("JUnit XML lacks required Kafka Object-WAL contracts: " + missing);
        }
        return new JunitEvidence(
                slash(XML_ROOT),
                files,
                new JunitTotals(
                        files.size(),
                        files.stream().mapToInt(JunitFile::tests).sum(),
                        files.stream().mapToInt(JunitFile::failures).sum(),
                        files.stream().mapToInt(JunitFile::errors).sum(),
                        files.stream().mapToInt(JunitFile::skipped).sum()));
    }

    private static Element parseXml(String value) {
        try {
            DocumentBuilderFactory factory = DocumentBuilderFactory.newInstance();
            factory.setFeature("http://apache.org/xml/features/disallow-doctype-decl", true);
            factory.setFeature("http://xml.org/sax/features/external-general-entities", false);
            factory.setFeature("http://xml.org/sax/features/external-parameter-entities", false);
            factory.setAttribute(XMLConstants.ACCESS_EXTERNAL_DTD, "");
            factory.setAttribute(XMLConstants.ACCESS_EXTERNAL_SCHEMA, "");
            factory.setXIncludeAware(false);
            factory.setExpandEntityReferences(false);
            return factory.newDocumentBuilder()
                    .parse(new InputSource(new StringReader(value)))
                    .getDocumentElement();
        } catch (Exception failure) {
            throw new IllegalArgumentException("cannot parse closed JUnit XML", failure);
        }
    }

    private static List<Path> sourceFiles(Path root) throws IOException {
        List<Path> result = new ArrayList<>();
        result.add(exactRegularFile(root, BUILD_FILE));
        for (Path relativeRoot : List.of(MAIN_ROOT, TEST_ROOT)) {
            Path directory = exactDirectory(root.resolve(relativeRoot));
            try (var stream = Files.walk(directory)) {
                stream.filter(path -> path.getFileName().toString().endsWith(".java"))
                        .forEach(result::add);
            }
        }
        for (Path source : M2_BRIDGE_SOURCES) {
            result.add(exactRegularFile(root, source));
        }
        result.sort(Comparator.comparing(path -> slash(root.relativize(path))));
        if (result.size() != SOURCE_ARTIFACT_COUNT) {
            throw new IllegalArgumentException("Kafka Object-WAL source inventory differs from its closed count");
        }
        return List.copyOf(result);
    }

    private static boolean validArtifactInventory(List<Artifact> artifacts) {
        if (artifacts.size() != SOURCE_ARTIFACT_COUNT) {
            return false;
        }
        List<String> paths = artifacts.stream().map(Artifact::path).toList();
        if (!paths.equals(paths.stream().distinct().sorted().toList()) || !paths.contains(slash(BUILD_FILE))) {
            return false;
        }
        Set<String> exactBridgeSources = M2_BRIDGE_SOURCES.stream()
                .map(KafkaObjectWalNativeResultV1::slash)
                .collect(java.util.stream.Collectors.toUnmodifiableSet());
        if (!paths.containsAll(exactBridgeSources)) {
            return false;
        }
        String mainPrefix = slash(MAIN_ROOT) + "/";
        String testPrefix = slash(TEST_ROOT) + "/";
        return paths.stream()
                .allMatch(path -> path.equals(slash(BUILD_FILE))
                        || exactBridgeSources.contains(path)
                        || path.startsWith(mainPrefix) && path.endsWith(".java")
                        || path.startsWith(testPrefix) && path.endsWith(".java"));
    }

    private static List<Artifact> artifacts(Path root, List<Path> sources) throws IOException {
        List<Artifact> result = new ArrayList<>();
        for (Path source : sources) {
            byte[] bytes = Files.readAllBytes(source);
            result.add(new Artifact(slash(root.relativize(source)), sha256(bytes), bytes.length));
        }
        return List.copyOf(result);
    }

    private static String treeSha256(Path root, List<Path> sources) throws IOException {
        MessageDigest digest = digest();
        for (Path source : sources) {
            byte[] path = slash(root.relativize(source)).getBytes(StandardCharsets.UTF_8);
            byte[] bytes = Files.readAllBytes(source);
            digest.update(ByteBuffer.allocate(Integer.BYTES).putInt(path.length).array());
            digest.update(path);
            digest.update(ByteBuffer.allocate(Long.BYTES).putLong(bytes.length).array());
            digest.update(bytes);
        }
        return HexFormat.of().formatHex(digest.digest());
    }

    private static Counters counters() {
        return new Counters(
                WIRE_TESTS.size(),
                RECOVERY_TESTS.size(),
                ACK_TESTS.size(),
                ROLLBACK_TESTS.size(),
                NATIVE_STATE_TESTS.size(),
                LOCATOR_TESTS.size(),
                ISOLATION_TESTS.size(),
                RECEIPT_TESTS.size(),
                Long.SIZE,
                0);
    }

    private static List<String> computeRequiredTests() {
        return java.util.stream.Stream.of(
                        WIRE_TESTS,
                        RECOVERY_TESTS,
                        ACK_TESTS,
                        ROLLBACK_TESTS,
                        NATIVE_STATE_TESTS,
                        LOCATOR_TESTS,
                        ISOLATION_TESTS,
                        RECEIPT_TESTS)
                .flatMap(List::stream)
                .distinct()
                .sorted()
                .toList();
    }

    private static byte[] canonicalBytesUnchecked(Receipt receipt) {
        StringBuilder out = new StringBuilder(8192);
        out.append("{\"schema\":");
        string(out, receipt.schema());
        out.append(",\"componentKind\":");
        string(out, receipt.componentKind());
        out.append(",\"status\":");
        string(out, receipt.status());
        out.append(",\"testedSource\":{\"repository\":");
        string(out, receipt.testedSource().repository());
        out.append(",\"commit\":");
        string(out, receipt.testedSource().commit());
        out.append(",\"treeSha256\":");
        string(out, receipt.testedSource().treeSha256());
        out.append("},\"externalSources\":[");
        for (int index = 0; index < receipt.externalSources().size(); index++) {
            comma(out, index);
            ExternalSource source = receipt.externalSources().get(index);
            out.append("{\"repository\":");
            string(out, source.repository());
            out.append(",\"commit\":");
            string(out, source.commit());
            out.append('}');
        }
        out.append("],\"execution\":{\"command\":");
        string(out, receipt.execution().command());
        out.append(",\"startedAtUtc\":");
        string(out, receipt.execution().startedAtUtc());
        out.append(",\"finishedAtUtc\":");
        string(out, receipt.execution().finishedAtUtc());
        out.append(",\"jvm\":");
        string(out, receipt.execution().jvm());
        out.append(",\"os\":");
        string(out, receipt.execution().os());
        out.append("},\"junit\":{\"xmlRoot\":");
        string(out, receipt.junit().xmlRoot());
        out.append(",\"xmlFiles\":[");
        for (int index = 0; index < receipt.junit().xmlFiles().size(); index++) {
            comma(out, index);
            JunitFile file = receipt.junit().xmlFiles().get(index);
            out.append("{\"path\":");
            string(out, file.path());
            out.append(",\"sha256\":");
            string(out, file.sha256());
            out.append(",\"tests\":").append(file.tests());
            out.append(",\"failures\":").append(file.failures());
            out.append(",\"errors\":").append(file.errors());
            out.append(",\"skipped\":").append(file.skipped()).append('}');
        }
        JunitTotals totals = receipt.junit().totals();
        out.append("],\"totals\":{\"suites\":").append(totals.suites());
        out.append(",\"tests\":").append(totals.tests());
        out.append(",\"failures\":").append(totals.failures());
        out.append(",\"errors\":").append(totals.errors());
        out.append(",\"skipped\":").append(totals.skipped()).append("}}");
        out.append(",\"requiredTests\":");
        strings(out, receipt.requiredTests());
        out.append(",\"counters\":");
        counters(out, receipt.counters());
        out.append(",\"artifacts\":[");
        for (int index = 0; index < receipt.artifacts().size(); index++) {
            comma(out, index);
            Artifact artifact = receipt.artifacts().get(index);
            out.append("{\"path\":");
            string(out, artifact.path());
            out.append(",\"sha256\":");
            string(out, artifact.sha256());
            out.append(",\"bytes\":").append(artifact.bytes()).append('}');
        }
        out.append("],\"exclusions\":");
        strings(out, receipt.exclusions());
        out.append(",\"receiptSha256\":");
        string(out, receipt.receiptSha256());
        return out.append('}').toString().getBytes(StandardCharsets.UTF_8);
    }

    private static void counters(StringBuilder out, Counters value) {
        out.append("{\"checkpointWireTests\":").append(value.checkpointWireTests());
        out.append(",\"rootBoundRecoveryTests\":").append(value.rootBoundRecoveryTests());
        out.append(",\"ackOrderingTests\":").append(value.ackOrderingTests());
        out.append(",\"wholeSuffixRollbackTests\":").append(value.wholeSuffixRollbackTests());
        out.append(",\"nativeStateIntegrityTests\":").append(value.nativeStateIntegrityTests());
        out.append(",\"locatorRetirementTests\":").append(value.locatorRetirementTests());
        out.append(",\"sharedIsolationTests\":").append(value.sharedIsolationTests());
        out.append(",\"rawReceiptTests\":").append(value.rawReceiptTests());
        out.append(",\"completionTicketBits\":").append(value.completionTicketBits());
        out.append(",\"m6ActivationClaims\":")
                .append(value.m6ActivationClaims())
                .append('}');
    }

    private static TestedSource parseTestedSource(Cursor cursor) {
        cursor.expect("{\"repository\":");
        String repository = cursor.string();
        cursor.expect(",\"commit\":");
        String commit = cursor.string();
        cursor.expect(",\"treeSha256\":");
        String tree = cursor.string();
        cursor.expect("}");
        return new TestedSource(repository, commit, tree);
    }

    private static List<ExternalSource> parseExternalSources(Cursor cursor) {
        List<ExternalSource> result = new ArrayList<>();
        cursor.array(() -> {
            cursor.expect("{\"repository\":");
            String repository = cursor.string();
            cursor.expect(",\"commit\":");
            String commit = cursor.string();
            cursor.expect("}");
            result.add(new ExternalSource(repository, commit));
        });
        return List.copyOf(result);
    }

    private static Execution parseExecution(Cursor cursor) {
        cursor.expect("{\"command\":");
        String command = cursor.string();
        cursor.expect(",\"startedAtUtc\":");
        String start = cursor.string();
        cursor.expect(",\"finishedAtUtc\":");
        String finish = cursor.string();
        cursor.expect(",\"jvm\":");
        String jvm = cursor.string();
        cursor.expect(",\"os\":");
        String os = cursor.string();
        cursor.expect("}");
        return new Execution(command, start, finish, jvm, os);
    }

    private static JunitEvidence parseJunit(Cursor cursor) {
        cursor.expect("{\"xmlRoot\":");
        String root = cursor.string();
        cursor.expect(",\"xmlFiles\":");
        List<JunitFile> files = new ArrayList<>();
        cursor.array(() -> {
            cursor.expect("{\"path\":");
            String path = cursor.string();
            cursor.expect(",\"sha256\":");
            String sha = cursor.string();
            cursor.expect(",\"tests\":");
            int tests = cursor.integer();
            cursor.expect(",\"failures\":");
            int failures = cursor.integer();
            cursor.expect(",\"errors\":");
            int errors = cursor.integer();
            cursor.expect(",\"skipped\":");
            int skipped = cursor.integer();
            cursor.expect("}");
            files.add(new JunitFile(path, sha, tests, failures, errors, skipped));
        });
        cursor.expect(",\"totals\":{\"suites\":");
        int suites = cursor.integer();
        cursor.expect(",\"tests\":");
        int tests = cursor.integer();
        cursor.expect(",\"failures\":");
        int failures = cursor.integer();
        cursor.expect(",\"errors\":");
        int errors = cursor.integer();
        cursor.expect(",\"skipped\":");
        int skipped = cursor.integer();
        cursor.expect("}}");
        return new JunitEvidence(root, files, new JunitTotals(suites, tests, failures, errors, skipped));
    }

    private static Counters parseCounters(Cursor cursor) {
        cursor.expect("{\"checkpointWireTests\":");
        int wire = cursor.integer();
        cursor.expect(",\"rootBoundRecoveryTests\":");
        int recovery = cursor.integer();
        cursor.expect(",\"ackOrderingTests\":");
        int ack = cursor.integer();
        cursor.expect(",\"wholeSuffixRollbackTests\":");
        int rollback = cursor.integer();
        cursor.expect(",\"nativeStateIntegrityTests\":");
        int nativeState = cursor.integer();
        cursor.expect(",\"locatorRetirementTests\":");
        int locator = cursor.integer();
        cursor.expect(",\"sharedIsolationTests\":");
        int isolation = cursor.integer();
        cursor.expect(",\"rawReceiptTests\":");
        int receipt = cursor.integer();
        cursor.expect(",\"completionTicketBits\":");
        int bits = cursor.integer();
        cursor.expect(",\"m6ActivationClaims\":");
        int m6 = cursor.integer();
        cursor.expect("}");
        return new Counters(wire, recovery, ack, rollback, nativeState, locator, isolation, receipt, bits, m6);
    }

    private static List<Artifact> parseArtifacts(Cursor cursor) {
        List<Artifact> result = new ArrayList<>();
        cursor.array(() -> {
            cursor.expect("{\"path\":");
            String path = cursor.string();
            cursor.expect(",\"sha256\":");
            String sha = cursor.string();
            cursor.expect(",\"bytes\":");
            long bytes = cursor.longValue();
            cursor.expect("}");
            result.add(new Artifact(path, sha, bytes));
        });
        return List.copyOf(result);
    }

    private static void strings(StringBuilder out, List<String> values) {
        out.append('[');
        for (int index = 0; index < values.size(); index++) {
            comma(out, index);
            string(out, values.get(index));
        }
        out.append(']');
    }

    private static void string(StringBuilder out, String value) {
        Objects.requireNonNull(value, "JSON string");
        out.append('"');
        for (int index = 0; index < value.length(); index++) {
            char item = value.charAt(index);
            switch (item) {
                case '"' -> out.append("\\\"");
                case '\\' -> out.append("\\\\");
                case '\b' -> out.append("\\b");
                case '\f' -> out.append("\\f");
                case '\n' -> out.append("\\n");
                case '\r' -> out.append("\\r");
                case '\t' -> out.append("\\t");
                default -> {
                    if (item < 0x20) {
                        out.append(String.format("\\u%04x", (int) item));
                    } else {
                        out.append(item);
                    }
                }
            }
        }
        out.append('"');
    }

    private static void comma(StringBuilder out, int index) {
        if (index > 0) {
            out.append(',');
        }
    }

    private static int integerAttribute(Element element, String name) {
        try {
            return Integer.parseInt(element.getAttribute(name));
        } catch (NumberFormatException failure) {
            throw new IllegalArgumentException("JUnit XML has invalid " + name, failure);
        }
    }

    private static Path exactDirectory(Path path) throws IOException {
        Path exact = path.toRealPath(LinkOption.NOFOLLOW_LINKS);
        if (!Files.isDirectory(exact, LinkOption.NOFOLLOW_LINKS) || Files.isSymbolicLink(exact)) {
            throw new IllegalArgumentException("path is not an exact non-symbolic directory: " + path);
        }
        return exact;
    }

    private static Path exactRegularFile(Path root, Path relative) throws IOException {
        if (relative.isAbsolute() || relative.normalize().startsWith("..")) {
            throw new IllegalArgumentException("artifact path escapes the repository");
        }
        Path path = root.resolve(relative).normalize();
        if (!path.startsWith(root)
                || Files.isSymbolicLink(path)
                || !Files.isRegularFile(path, LinkOption.NOFOLLOW_LINKS)) {
            throw new IllegalArgumentException("artifact is not an exact regular file: " + relative);
        }
        return path;
    }

    private static String relativePath(String value) {
        String result = text(value, "relative path");
        Path path = Path.of(result);
        if (path.isAbsolute()
                || !path.normalize().toString().replace('\\', '/').equals(result)
                || result.startsWith("../")) {
            throw new IllegalArgumentException("path is not exact normalized relative UTF-8");
        }
        return result;
    }

    private static String text(String value, String name) {
        if (value == null || value.isEmpty() || value.indexOf('\0') >= 0) {
            throw new IllegalArgumentException(name + " is empty or contains NUL");
        }
        return value;
    }

    private static void requireCommit(String value) {
        if (value == null || !value.matches("[0-9a-f]{40}")) {
            throw new IllegalArgumentException("source commit must be exact lower-hex SHA-1");
        }
    }

    private static void requireSha256(String value) {
        if (value == null || !value.matches("[0-9a-f]{64}")) {
            throw new IllegalArgumentException("SHA-256 must be exact lower hex");
        }
    }

    private static String id(String suite, String test) {
        return suite + "#" + test;
    }

    private static String slash(Path value) {
        return value.toString().replace('\\', '/');
    }

    private static String sha256(byte[] value) {
        return HexFormat.of().formatHex(digest().digest(value));
    }

    private static MessageDigest digest() {
        try {
            return MessageDigest.getInstance("SHA-256");
        } catch (NoSuchAlgorithmException failure) {
            throw new IllegalStateException("JVM lacks SHA-256", failure);
        }
    }

    private static final class Cursor {
        private final String value;
        private int offset;

        private Cursor(String value) {
            this.value = value;
        }

        private void expect(String exact) {
            if (!value.startsWith(exact, offset)) {
                throw new IllegalArgumentException("native receipt has non-canonical member order at byte " + offset);
            }
            offset += exact.length();
        }

        private String string() {
            expect("\"");
            StringBuilder result = new StringBuilder();
            while (offset < value.length()) {
                char item = value.charAt(offset++);
                if (item == '"') {
                    return result.toString();
                }
                if (item == '\\') {
                    if (offset >= value.length()) {
                        throw new IllegalArgumentException("truncated JSON escape");
                    }
                    char escaped = value.charAt(offset++);
                    switch (escaped) {
                        case '"', '\\', '/' -> result.append(escaped);
                        case 'b' -> result.append('\b');
                        case 'f' -> result.append('\f');
                        case 'n' -> result.append('\n');
                        case 'r' -> result.append('\r');
                        case 't' -> result.append('\t');
                        case 'u' -> {
                            if (offset + 4 > value.length()) {
                                throw new IllegalArgumentException("truncated Unicode escape");
                            }
                            result.append((char) Integer.parseInt(value.substring(offset, offset + 4), 16));
                            offset += 4;
                        }
                        default -> throw new IllegalArgumentException("invalid JSON escape");
                    }
                } else if (item < 0x20) {
                    throw new IllegalArgumentException("raw control byte in JSON string");
                } else {
                    result.append(item);
                }
            }
            throw new IllegalArgumentException("unterminated JSON string");
        }

        private int integer() {
            long result = longValue();
            if (result > Integer.MAX_VALUE) {
                throw new IllegalArgumentException("JSON integer exceeds int");
            }
            return (int) result;
        }

        private long longValue() {
            int start = offset;
            while (offset < value.length() && Character.isDigit(value.charAt(offset))) {
                offset++;
            }
            if (start == offset || value.charAt(start) == '0' && offset - start > 1) {
                throw new IllegalArgumentException("invalid canonical non-negative JSON integer");
            }
            try {
                return Long.parseLong(value.substring(start, offset));
            } catch (NumberFormatException failure) {
                throw new IllegalArgumentException("JSON integer exceeds long", failure);
            }
        }

        private List<String> stringArray() {
            List<String> result = new ArrayList<>();
            array(() -> result.add(string()));
            return List.copyOf(result);
        }

        private void array(Runnable element) {
            expect("[");
            if (value.startsWith("]", offset)) {
                offset++;
                return;
            }
            int index = 0;
            while (true) {
                if (index++ > 0) {
                    expect(",");
                }
                element.run();
                if (value.startsWith("]", offset)) {
                    offset++;
                    return;
                }
            }
        }

        private void eof() {
            if (offset != value.length()) {
                throw new IllegalArgumentException("native receipt has trailing bytes");
            }
        }
    }
}
