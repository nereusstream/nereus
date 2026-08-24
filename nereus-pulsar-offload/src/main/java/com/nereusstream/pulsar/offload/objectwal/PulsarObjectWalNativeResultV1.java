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

package com.nereusstream.pulsar.offload.objectwal;

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

/** Closed, source-bound raw JUnit result for the M3 Pulsar Object-WAL component gate. */
public final class PulsarObjectWalNativeResultV1 {
    public static final String SCHEMA = "nereus-v2-m3-native-result-v1";
    public static final String COMPONENT_KIND = "P_PULSAR_OBJECT_WAL";
    public static final String STATUS = "PASS";
    public static final String M6_EXCLUSION = "M6_NATIVE_BROKER_CONTROLLER_ACTIVATION";
    public static final int MAX_CANONICAL_BYTES = 256 * 1024;
    public static final String DEFAULT_COMMAND =
            "./gradlew :nereus-pulsar-offload:test :nereus-pulsar-offload:spotlessCheck "
                    + ":nereus-pulsar-offload:checkstyleMain :nereus-pulsar-offload:checkstyleTest";

    private static final String ZERO_SHA256 = "0".repeat(64);
    private static final Path MODULE_BUILD = Path.of("nereus-pulsar-offload/build.gradle.kts");
    private static final Path MAIN_ROOT =
            Path.of("nereus-pulsar-offload/src/main/java/com/nereusstream/pulsar/offload/objectwal");
    private static final Path TEST_ROOT =
            Path.of("nereus-pulsar-offload/src/test/java/com/nereusstream/pulsar/offload/objectwal");
    private static final Path XML_ROOT = Path.of("nereus-pulsar-offload/build/test-results/test");
    private static final List<String> SUITES = List.of(
            "com.nereusstream.pulsar.offload.objectwal.PulsarObjectWalBridgeV1Test",
            "com.nereusstream.pulsar.offload.objectwal.PulsarVirtualLedgerChainControllerV1Test");
    private static final List<Integer> SUITE_TEST_COUNTS = List.of(39, 7);
    private static final List<Path> SOURCE_PATHS = List.of(
            MODULE_BUILD,
            MAIN_ROOT.resolve("PulsarObjectWalBridgeV1.java"),
            MAIN_ROOT.resolve("PulsarObjectWalNativeResultV1.java"),
            MAIN_ROOT.resolve("PulsarVirtualLedgerChainControllerV1.java"),
            TEST_ROOT.resolve("PulsarObjectWalBridgeV1Test.java"),
            TEST_ROOT.resolve("PulsarVirtualLedgerChainControllerV1Test.java"));

    private static final List<String> FIXED_SLICE_TESTS = List.of(
            id(SUITES.get(1), "opensFirstLedgerAndPublishesExplicitSuccessorLink"),
            id(SUITES.get(1), "rejectsAllocatorValueOutsideTheImmutableSlice"),
            id(SUITES.get(1), "failsClosedWhenAllocatorExhaustsInsteadOfSearchingAnotherSlice"),
            id(SUITES.get(0), "successorAllocationExhaustionLeavesTheCurrentHeadUnchanged"));
    private static final List<String> NO_GAP_TESTS = List.of(
            id(SUITES.get(0), "definitiveAbsenceStopsAdmissionThenResumeUsesTheSameEntry"),
            id(SUITES.get(0), "exceptionalPublishRetainsTheExactPositionAndTicketForSameEntryRecovery"),
            id(SUITES.get(0), "exceptionalReconcileRetainsUnknownPositionAndTicketForSameEntryRecovery"),
            id(SUITES.get(0), "definitivelyAbsentSingleEntryCanSealAndMoveToSuccessorEntryZero"),
            id(SUITES.get(0), "successorLocalSealFailureRetainsExactEntryZeroForOldPlanRetry"),
            id(SUITES.get(0), "sharedDefinitiveAbsenceCannotPartiallyRolloverHeads"),
            id(SUITES.get(0), "bindingLocalAppendFailureRetainsOnlyItsTypedGapWhileSiblingPublishesAndContinues"));
    private static final List<String> TICKET_TESTS = List.of(
            id(SUITES.get(0), "combinedTrackerAndLocatorReservationFailsBeforeAnySharedPositionOrTicket"),
            id(
                    SUITES.get(0),
                    "checkedTicketCounterCrossesSignedBoundaryAndUsesUnsignedMaxOnceThenFailsBeforeAnotherPosition"),
            id(SUITES.get(0), "durableTakeoverFenceDiscardsOldOwnerReservationsAndTickets"),
            id(SUITES.get(0), "localSealFailureReleasesTheExactPostPositionTicketBeforeProviderDispatch"),
            id(SUITES.get(0), "everyCancellationUnknownRetryAndCompletionReleaseRequiresExactTicketOwnership"),
            id(SUITES.get(0), "staleTicketCannotReleaseAReusedRingSlot"),
            id(SUITES.get(0), "boundedRecoveryValidatesAdjacencyThenReconstructsFreshTickets"),
            id(SUITES.get(0), "sharedFailedPlanTakeoverMustFenceAndDiscardEverySiblingAtomically"));
    private static final List<String> ISOLATION_TESTS = List.of(
            id(SUITES.get(0), "publishesSharedExtentWithExactPositionsAndIndependentBindingFrontiers"),
            id(SUITES.get(0), "installsActiveTailBeforeAckAndReadsEachSharedMemberWithoutCrossBindingPoisoning"),
            id(SUITES.get(0), "bindingLocalAppendFailureRetainsOnlyItsTypedGapWhileSiblingPublishesAndContinues"),
            id(SUITES.get(0), "perMemberFailureCannotMasqueradeAsSharedOrPreBindingValidation"));
    private static final List<String> MANIFEST_TESTS = List.of(
            id(SUITES.get(0), "manifestHandoffWaitsForActiveReadPinsBeforeReleasingCoveredLocator"),
            id(SUITES.get(0), "manifestAuthorityMismatchRetainsEveryActiveLocator"),
            id(SUITES.get(0), "recoveredManifestCoverageIsAuthorityVerifiedBeforeActivationAndRead"),
            id(SUITES.get(0), "exceptionalManifestVerificationCannotPublishCoverageOrReleaseLocator"));
    private static final List<String> RESPONSE_LOSS_TESTS = List.of(
            id(SUITES.get(1), "reconcilesUnknownMutationOnlyByExactReread"),
            id(SUITES.get(1), "rejectsUnknownMutationWhoseRereadIsAnotherWinner"),
            id(SUITES.get(1), "redrivesTheSameCandidateWhenUnknownRereadStillShowsExactPredecessor"));
    private static final List<String> PRODUCTION_ADAPTER_TESTS = List.of(
            id(SUITES.get(1), "productionNvAdapterRejectsEvidenceOnlyCoordinatorAsRuntimeAuthority"),
            id(SUITES.get(0), "productionObjectWalAdapterOwnsCommonSessionAndExposesNoPlaintextKeyOrListBudget"),
            id(SUITES.get(0), "pulsarOwnerFenceCoversRecoveryAndRejectsOldOwnerLatePut"),
            id(SUITES.get(0), "pulsarOwnerFenceRejectsMonotonicRollbackBeforeRecoveryCallback"),
            id(SUITES.get(0), "pulsarOwnerFenceReleasesAuthorityAfterRecoveryCallbackFailure"),
            id(SUITES.get(0), "unknownDrainRetainsCommonRecoveryAndKmsUntilReconcileThenTerminalClose"),
            id(SUITES.get(0), "productionAtomicProjectionIgnoresExistingVersionTokenUnderRootNoneAndPublishesClosure"));
    private static final List<String> RECOVERY_AUTHORITY_TESTS = List.of(
            id(SUITES.get(0), "boundedRecoveryValidatesAdjacencyThenReconstructsFreshTickets"),
            id(SUITES.get(0), "failedRecoveryAuthenticationNeverActivatesBindingOrPublishesFrontier"));
    private static final List<String> RAW_RECEIPT_TESTS = List.of(
            id(SUITES.get(0), "rawNativeResultRoundTripsWithExactSourceJunitCountersAndM6Exclusion"),
            id(SUITES.get(0), "rawNativeResultRejectsCallerStatusTrailingBytesAndReceiptHashTampering"),
            id(SUITES.get(0), "rawNativeResultRejectsAnyFailureErrorOrSkipCounter"));
    private static final List<String> REQUIRED_TESTS = computeRequiredTests();

    private PulsarObjectWalNativeResultV1() {}

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
            Instant start = Instant.parse(startedAtUtc);
            Instant finish = Instant.parse(finishedAtUtc);
            if (finish.isBefore(start)) {
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
            long fixedSliceLedgerIds,
            int completionTicketBits,
            int normalAppendRemoteMetadataOperations,
            int sharedExtentTicketsPerMember,
            int fixedSliceTests,
            int noGapTests,
            int completionTicketTests,
            int perMemberIsolationTests,
            int manifestAuthorityTests,
            int responseLossTests,
            int productionAdapterTests,
            int recoveryAuthorityTests,
            int rawReceiptTests) {
        public Counters {
            if (fixedSliceLedgerIds != PulsarVirtualLedgerChainControllerV1.SLICE_SIZE
                    || completionTicketBits != Long.SIZE
                    || normalAppendRemoteMetadataOperations != 0
                    || sharedExtentTicketsPerMember != 1
                    || fixedSliceTests != FIXED_SLICE_TESTS.size()
                    || noGapTests != NO_GAP_TESTS.size()
                    || completionTicketTests != TICKET_TESTS.size()
                    || perMemberIsolationTests != ISOLATION_TESTS.size()
                    || manifestAuthorityTests != MANIFEST_TESTS.size()
                    || responseLossTests != RESPONSE_LOSS_TESTS.size()
                    || productionAdapterTests != PRODUCTION_ADAPTER_TESTS.size()
                    || recoveryAuthorityTests != RECOVERY_AUTHORITY_TESTS.size()
                    || rawReceiptTests != RAW_RECEIPT_TESTS.size()) {
                throw new IllegalArgumentException("Pulsar Object-WAL counters differ from the closed contract");
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
            String pulsarRepository,
            String pulsarCommit,
            String startedAtUtc,
            String finishedAtUtc)
            throws IOException {
        Path root = exactDirectory(repositoryRoot);
        List<Path> sources = sourceFiles(root);
        List<Artifact> artifacts = artifacts(root, sources);
        TestedSource tested = new TestedSource("nereus", nereusCommit, treeSha256(root, sources));
        List<ExternalSource> external = List.of(new ExternalSource(pulsarRepository, pulsarCommit));
        Execution execution = new Execution(
                DEFAULT_COMMAND,
                startedAtUtc,
                finishedAtUtc,
                System.getProperty("java.vm.name") + " " + System.getProperty("java.version"),
                System.getProperty("os.name") + " " + System.getProperty("os.version") + " "
                        + System.getProperty("os.arch"));
        JunitEvidence junit = readJunit(root);
        Receipt unsigned = new Receipt(
                SCHEMA,
                COMPONENT_KIND,
                STATUS,
                tested,
                external,
                execution,
                junit,
                REQUIRED_TESTS,
                counters(),
                artifacts,
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
        String componentKind = cursor.string();
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
        List<String> requiredTests = cursor.stringArray();
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
                componentKind,
                status,
                tested,
                external,
                execution,
                junit,
                requiredTests,
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
        Path parent = Objects.requireNonNull(output.toAbsolutePath().getParent(), "receipt parent");
        Files.createDirectories(parent);
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
                    "usage: generate <repo-root> <nereus-commit> <pulsar-repository> <pulsar-commit> "
                            + "<started-at-utc> <finished-at-utc> <output> | parse <receipt>");
        }
        Receipt receipt =
                generate(Path.of(arguments[1]), arguments[2], arguments[3], arguments[4], arguments[5], arguments[6]);
        writeCanonical(Path.of(arguments[7]), receipt);
    }

    static Receipt withReceiptSha(Receipt source, String sha) {
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

    private static void validate(Receipt receipt) {
        Objects.requireNonNull(receipt, "receipt");
        if (!SCHEMA.equals(receipt.schema())
                || !COMPONENT_KIND.equals(receipt.componentKind())
                || !STATUS.equals(receipt.status())
                || !"nereus".equals(receipt.testedSource().repository())
                || !receipt.requiredTests().equals(REQUIRED_TESTS)
                || !receipt.exclusions().equals(List.of(M6_EXCLUSION))
                || receipt.externalSources().size() != 1
                || !"apache/pulsar".equals(receipt.externalSources().get(0).repository())
                || !DEFAULT_COMMAND.equals(receipt.execution().command())
                || !slash(XML_ROOT).equals(receipt.junit().xmlRoot())
                || !receipt.artifacts().stream()
                        .map(Artifact::path)
                        .toList()
                        .equals(SOURCE_PATHS.stream()
                                .map(PulsarObjectWalNativeResultV1::slash)
                                .toList())) {
            throw new IllegalArgumentException("native receipt closed identity/inventory differs");
        }
        JunitTotals totals = receipt.junit().totals();
        if (totals.suites() != SUITES.size()
                || receipt.junit().xmlFiles().size() != SUITES.size()
                || totals.failures() != 0
                || totals.errors() != 0
                || totals.skipped() != 0) {
            throw new IllegalArgumentException("native receipt requires two non-empty zero-failure/skip suites");
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
        int tests =
                receipt.junit().xmlFiles().stream().mapToInt(JunitFile::tests).sum();
        int failures = receipt.junit().xmlFiles().stream()
                .mapToInt(JunitFile::failures)
                .sum();
        int errors =
                receipt.junit().xmlFiles().stream().mapToInt(JunitFile::errors).sum();
        int skipped =
                receipt.junit().xmlFiles().stream().mapToInt(JunitFile::skipped).sum();
        if (tests != totals.tests()
                || failures != totals.failures()
                || errors != totals.errors()
                || skipped != totals.skipped()) {
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
            if (failures != 0 || errors != 0 || skipped != 0) {
                throw new IllegalArgumentException("JUnit XML suite is not zero-failure/skip: " + relative);
            }
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
                if (!suite.equals(className)
                        || test.getElementsByTagName("failure").getLength() != 0
                        || test.getElementsByTagName("error").getLength() != 0
                        || test.getElementsByTagName("skipped").getLength() != 0
                        || !observedTests.add(id(className, testName))) {
                    throw new IllegalArgumentException(
                            "JUnit XML contains a foreign, duplicate, failed, errored, or skipped testcase: "
                                    + relative);
                }
            }
            files.add(
                    new JunitFile(slash(relative), sha256(Files.readAllBytes(xml)), tests, failures, errors, skipped));
        }
        if (!observedTests.containsAll(REQUIRED_TESTS)) {
            Set<String> missing = new HashSet<>(REQUIRED_TESTS);
            missing.removeAll(observedTests);
            throw new IllegalArgumentException("JUnit XML lacks required Pulsar contracts: " + missing);
        }
        JunitTotals totals = new JunitTotals(
                files.size(),
                files.stream().mapToInt(JunitFile::tests).sum(),
                files.stream().mapToInt(JunitFile::failures).sum(),
                files.stream().mapToInt(JunitFile::errors).sum(),
                files.stream().mapToInt(JunitFile::skipped).sum());
        return new JunitEvidence(slash(XML_ROOT), files, totals);
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
        } catch (Exception error) {
            throw new IllegalArgumentException("cannot parse closed JUnit XML", error);
        }
    }

    private static int integerAttribute(Element element, String name) {
        try {
            return Integer.parseInt(element.getAttribute(name));
        } catch (NumberFormatException error) {
            throw new IllegalArgumentException("JUnit XML has invalid " + name, error);
        }
    }

    private static List<Path> sourceFiles(Path root) throws IOException {
        List<Path> observed = new ArrayList<>(List.of(MODULE_BUILD));
        for (Path sourceRoot : List.of(MAIN_ROOT, TEST_ROOT)) {
            try (var stream = Files.walk(root.resolve(sourceRoot))) {
                stream.filter(path -> path.getFileName().toString().endsWith(".java"))
                        .map(root::relativize)
                        .forEach(observed::add);
            }
        }
        observed.sort(Comparator.comparing(PulsarObjectWalNativeResultV1::slash));
        if (!observed.equals(SOURCE_PATHS)) {
            throw new IllegalArgumentException("Pulsar Object-WAL source inventory differs from the closed list");
        }
        List<Path> result = SOURCE_PATHS.stream().map(root::resolve).toList();
        for (Path relative : SOURCE_PATHS) {
            exactRegularFile(root, relative);
        }
        return List.copyOf(result);
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
                PulsarVirtualLedgerChainControllerV1.SLICE_SIZE,
                Long.SIZE,
                0,
                1,
                FIXED_SLICE_TESTS.size(),
                NO_GAP_TESTS.size(),
                TICKET_TESTS.size(),
                ISOLATION_TESTS.size(),
                MANIFEST_TESTS.size(),
                RESPONSE_LOSS_TESTS.size(),
                PRODUCTION_ADAPTER_TESTS.size(),
                RECOVERY_AUTHORITY_TESTS.size(),
                RAW_RECEIPT_TESTS.size());
    }

    private static List<String> computeRequiredTests() {
        return java.util.stream.Stream.of(
                        FIXED_SLICE_TESTS,
                        NO_GAP_TESTS,
                        TICKET_TESTS,
                        ISOLATION_TESTS,
                        MANIFEST_TESTS,
                        RESPONSE_LOSS_TESTS,
                        PRODUCTION_ADAPTER_TESTS,
                        RECOVERY_AUTHORITY_TESTS,
                        RAW_RECEIPT_TESTS)
                .flatMap(List::stream)
                .distinct()
                .sorted()
                .toList();
    }

    private static byte[] canonicalBytesUnchecked(Receipt receipt) {
        StringBuilder output = new StringBuilder(8192);
        output.append("{\"schema\":");
        string(output, receipt.schema());
        output.append(",\"componentKind\":");
        string(output, receipt.componentKind());
        output.append(",\"status\":");
        string(output, receipt.status());
        output.append(",\"testedSource\":");
        testedSource(output, receipt.testedSource());
        output.append(",\"externalSources\":[");
        for (int index = 0; index < receipt.externalSources().size(); index++) {
            comma(output, index);
            ExternalSource source = receipt.externalSources().get(index);
            output.append("{\"repository\":");
            string(output, source.repository());
            output.append(",\"commit\":");
            string(output, source.commit());
            output.append('}');
        }
        output.append("],\"execution\":");
        execution(output, receipt.execution());
        output.append(",\"junit\":");
        junit(output, receipt.junit());
        output.append(",\"requiredTests\":");
        strings(output, receipt.requiredTests());
        output.append(",\"counters\":");
        counters(output, receipt.counters());
        output.append(",\"artifacts\":[");
        for (int index = 0; index < receipt.artifacts().size(); index++) {
            comma(output, index);
            Artifact artifact = receipt.artifacts().get(index);
            output.append("{\"path\":");
            string(output, artifact.path());
            output.append(",\"sha256\":");
            string(output, artifact.sha256());
            output.append(",\"bytes\":").append(artifact.bytes()).append('}');
        }
        output.append("],\"exclusions\":");
        strings(output, receipt.exclusions());
        output.append(",\"receiptSha256\":");
        string(output, receipt.receiptSha256());
        return output.append('}').toString().getBytes(StandardCharsets.UTF_8);
    }

    private static void testedSource(StringBuilder output, TestedSource source) {
        output.append("{\"repository\":");
        string(output, source.repository());
        output.append(",\"commit\":");
        string(output, source.commit());
        output.append(",\"treeSha256\":");
        string(output, source.treeSha256());
        output.append('}');
    }

    private static void execution(StringBuilder output, Execution execution) {
        output.append("{\"command\":");
        string(output, execution.command());
        output.append(",\"startedAtUtc\":");
        string(output, execution.startedAtUtc());
        output.append(",\"finishedAtUtc\":");
        string(output, execution.finishedAtUtc());
        output.append(",\"jvm\":");
        string(output, execution.jvm());
        output.append(",\"os\":");
        string(output, execution.os());
        output.append('}');
    }

    private static void junit(StringBuilder output, JunitEvidence junit) {
        output.append("{\"xmlRoot\":");
        string(output, junit.xmlRoot());
        output.append(",\"xmlFiles\":[");
        for (int index = 0; index < junit.xmlFiles().size(); index++) {
            comma(output, index);
            JunitFile file = junit.xmlFiles().get(index);
            output.append("{\"path\":");
            string(output, file.path());
            output.append(",\"sha256\":");
            string(output, file.sha256());
            output.append(",\"tests\":").append(file.tests());
            output.append(",\"failures\":").append(file.failures());
            output.append(",\"errors\":").append(file.errors());
            output.append(",\"skipped\":").append(file.skipped()).append('}');
        }
        JunitTotals totals = junit.totals();
        output.append("],\"totals\":{\"suites\":").append(totals.suites());
        output.append(",\"tests\":").append(totals.tests());
        output.append(",\"failures\":").append(totals.failures());
        output.append(",\"errors\":").append(totals.errors());
        output.append(",\"skipped\":").append(totals.skipped()).append("}}");
    }

    private static void counters(StringBuilder output, Counters counters) {
        output.append("{\"fixedSliceLedgerIds\":").append(counters.fixedSliceLedgerIds());
        output.append(",\"completionTicketBits\":").append(counters.completionTicketBits());
        output.append(",\"normalAppendRemoteMetadataOperations\":")
                .append(counters.normalAppendRemoteMetadataOperations());
        output.append(",\"sharedExtentTicketsPerMember\":").append(counters.sharedExtentTicketsPerMember());
        output.append(",\"fixedSliceTests\":").append(counters.fixedSliceTests());
        output.append(",\"noGapTests\":").append(counters.noGapTests());
        output.append(",\"completionTicketTests\":").append(counters.completionTicketTests());
        output.append(",\"perMemberIsolationTests\":").append(counters.perMemberIsolationTests());
        output.append(",\"manifestAuthorityTests\":").append(counters.manifestAuthorityTests());
        output.append(",\"responseLossTests\":").append(counters.responseLossTests());
        output.append(",\"productionAdapterTests\":")
                .append(counters.productionAdapterTests())
                .append(",\"recoveryAuthorityTests\":")
                .append(counters.recoveryAuthorityTests())
                .append(",\"rawReceiptTests\":")
                .append(counters.rawReceiptTests())
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
        String started = cursor.string();
        cursor.expect(",\"finishedAtUtc\":");
        String finished = cursor.string();
        cursor.expect(",\"jvm\":");
        String jvm = cursor.string();
        cursor.expect(",\"os\":");
        String os = cursor.string();
        cursor.expect("}");
        return new Execution(command, started, finished, jvm, os);
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
        cursor.expect("{\"fixedSliceLedgerIds\":");
        long fixed = cursor.longValue();
        cursor.expect(",\"completionTicketBits\":");
        int bits = cursor.integer();
        cursor.expect(",\"normalAppendRemoteMetadataOperations\":");
        int metadata = cursor.integer();
        cursor.expect(",\"sharedExtentTicketsPerMember\":");
        int memberTickets = cursor.integer();
        cursor.expect(",\"fixedSliceTests\":");
        int fixedTests = cursor.integer();
        cursor.expect(",\"noGapTests\":");
        int noGap = cursor.integer();
        cursor.expect(",\"completionTicketTests\":");
        int tickets = cursor.integer();
        cursor.expect(",\"perMemberIsolationTests\":");
        int isolation = cursor.integer();
        cursor.expect(",\"manifestAuthorityTests\":");
        int manifest = cursor.integer();
        cursor.expect(",\"responseLossTests\":");
        int response = cursor.integer();
        cursor.expect(",\"productionAdapterTests\":");
        int adapters = cursor.integer();
        cursor.expect(",\"recoveryAuthorityTests\":");
        int recoveryAuthority = cursor.integer();
        cursor.expect(",\"rawReceiptTests\":");
        int receipts = cursor.integer();
        cursor.expect("}");
        return new Counters(
                fixed,
                bits,
                metadata,
                memberTickets,
                fixedTests,
                noGap,
                tickets,
                isolation,
                manifest,
                response,
                adapters,
                recoveryAuthority,
                receipts);
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

    private static void strings(StringBuilder output, List<String> values) {
        output.append('[');
        for (int index = 0; index < values.size(); index++) {
            comma(output, index);
            string(output, values.get(index));
        }
        output.append(']');
    }

    private static void string(StringBuilder output, String value) {
        Objects.requireNonNull(value, "JSON string");
        output.append('"');
        for (int index = 0; index < value.length(); index++) {
            char character = value.charAt(index);
            switch (character) {
                case '"' -> output.append("\\\"");
                case '\\' -> output.append("\\\\");
                case '\b' -> output.append("\\b");
                case '\f' -> output.append("\\f");
                case '\n' -> output.append("\\n");
                case '\r' -> output.append("\\r");
                case '\t' -> output.append("\\t");
                default -> {
                    if (character < 0x20) {
                        output.append(String.format(java.util.Locale.ROOT, "\\u%04x", (int) character));
                    } else {
                        output.append(character);
                    }
                }
            }
        }
        output.append('"');
    }

    private static void comma(StringBuilder output, int index) {
        if (index > 0) {
            output.append(',');
        }
    }

    private static Path exactDirectory(Path value) throws IOException {
        Objects.requireNonNull(value, "repository root");
        Path result = value.toRealPath(LinkOption.NOFOLLOW_LINKS);
        if (!Files.isDirectory(result, LinkOption.NOFOLLOW_LINKS) || Files.isSymbolicLink(result)) {
            throw new IllegalArgumentException("repository root must be a real directory");
        }
        return result;
    }

    private static Path exactRegularFile(Path root, Path relative) throws IOException {
        Path candidate = root.resolve(relative).normalize();
        if (!candidate.startsWith(root)
                || !Files.isRegularFile(candidate, LinkOption.NOFOLLOW_LINKS)
                || Files.isSymbolicLink(candidate)) {
            throw new IllegalArgumentException("evidence input is not an exact regular file: " + relative);
        }
        return candidate;
    }

    private static String relativePath(String value) {
        String result = text(value, "relative path");
        Path path = Path.of(result).normalize();
        if (path.isAbsolute() || path.startsWith("..") || !slash(path).equals(result)) {
            throw new IllegalArgumentException("evidence path must be normalized repository-relative text");
        }
        return result;
    }

    private static String slash(Path path) {
        return path.toString().replace(path.getFileSystem().getSeparator(), "/");
    }

    private static String text(String value, String name) {
        Objects.requireNonNull(value, name);
        if (value.isBlank() || value.length() > 4096 || value.indexOf('\0') >= 0) {
            throw new IllegalArgumentException(name + " is invalid");
        }
        return value;
    }

    private static void requireCommit(String value) {
        if (value == null || !value.matches("[0-9a-f]{40}")) {
            throw new IllegalArgumentException("source commit must be one lowercase 40-hex object ID");
        }
    }

    private static void requireSha256(String value) {
        if (value == null || !value.matches("[0-9a-f]{64}")) {
            throw new IllegalArgumentException("SHA-256 must be one lowercase 64-hex digest");
        }
    }

    private static String sha256(byte[] bytes) {
        return HexFormat.of().formatHex(digest().digest(bytes));
    }

    private static MessageDigest digest() {
        try {
            return MessageDigest.getInstance("SHA-256");
        } catch (NoSuchAlgorithmException error) {
            throw new IllegalStateException("SHA-256 is unavailable", error);
        }
    }

    private static String id(String suite, String test) {
        return suite + "#" + test;
    }

    private static final class Cursor {
        private final String value;
        private int index;

        private Cursor(String value) {
            this.value = value;
        }

        private void expect(String expected) {
            if (!value.startsWith(expected, index)) {
                throw new IllegalArgumentException("native receipt grammar mismatch at byte " + index);
            }
            index += expected.length();
        }

        private String string() {
            expect("\"");
            StringBuilder output = new StringBuilder();
            while (index < value.length()) {
                char character = value.charAt(index++);
                if (character == '"') {
                    return output.toString();
                }
                if (character == '\\') {
                    if (index >= value.length()) {
                        break;
                    }
                    char escaped = value.charAt(index++);
                    switch (escaped) {
                        case '"', '\\', '/' -> output.append(escaped);
                        case 'b' -> output.append('\b');
                        case 'f' -> output.append('\f');
                        case 'n' -> output.append('\n');
                        case 'r' -> output.append('\r');
                        case 't' -> output.append('\t');
                        case 'u' -> output.append(unicode());
                        default -> throw new IllegalArgumentException("invalid JSON escape");
                    }
                } else if (character < 0x20) {
                    throw new IllegalArgumentException("unescaped JSON control character");
                } else {
                    output.append(character);
                }
            }
            throw new IllegalArgumentException("unterminated JSON string");
        }

        private char unicode() {
            if (index + 4 > value.length()) {
                throw new IllegalArgumentException("short JSON unicode escape");
            }
            try {
                char result = (char) Integer.parseInt(value.substring(index, index + 4), 16);
                index += 4;
                return result;
            } catch (NumberFormatException error) {
                throw new IllegalArgumentException("invalid JSON unicode escape", error);
            }
        }

        private int integer() {
            return Math.toIntExact(longValue());
        }

        private long longValue() {
            int start = index;
            while (index < value.length() && value.charAt(index) >= '0' && value.charAt(index) <= '9') {
                index++;
            }
            if (start == index || (index - start > 1 && value.charAt(start) == '0')) {
                throw new IllegalArgumentException("invalid canonical non-negative integer");
            }
            try {
                return Long.parseLong(value.substring(start, index));
            } catch (NumberFormatException error) {
                throw new IllegalArgumentException("integer lies outside the 64-bit domain", error);
            }
        }

        private List<String> stringArray() {
            List<String> result = new ArrayList<>();
            array(() -> result.add(string()));
            return List.copyOf(result);
        }

        private void array(Runnable element) {
            expect("[");
            if (value.startsWith("]", index)) {
                index++;
                return;
            }
            while (true) {
                element.run();
                if (value.startsWith("]", index)) {
                    index++;
                    return;
                }
                expect(",");
            }
        }

        private void eof() {
            if (index != value.length()) {
                throw new IllegalArgumentException("trailing bytes after native receipt");
            }
        }
    }
}
