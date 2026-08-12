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

import java.io.ByteArrayOutputStream;
import java.io.IOException;
import java.io.InputStream;
import java.nio.charset.CharacterCodingException;
import java.nio.charset.CodingErrorAction;
import java.nio.charset.StandardCharsets;
import java.nio.file.Files;
import java.nio.file.LinkOption;
import java.nio.file.OpenOption;
import java.nio.file.Path;
import java.nio.file.StandardOpenOption;
import java.nio.file.attribute.BasicFileAttributes;
import java.security.MessageDigest;
import java.security.NoSuchAlgorithmException;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.EnumMap;
import java.util.HashSet;
import java.util.LinkedHashMap;
import java.util.List;
import java.util.Map;
import java.util.Objects;
import java.util.Set;
import java.util.regex.Pattern;

/** Test/evidence-only strict M1-2 receipt v1 capacity model. */
final class ReceiptV1CapacityModel {
    static final String SCHEMA = "NEREUS_VIRTUAL_LEDGER_RECEIPT_V1";
    static final long MAX_EXACT_JSON_INTEGER = 9_007_199_254_740_991L;
    static final int MAX_CANONICAL_ROOT_BYTES = 65_536;
    static final int MAX_SCENARIOS = 16;
    static final int MAX_SUITES_PER_SCENARIO = 128;
    static final int MAX_ATTACHMENTS = 32;
    static final int MAX_SINGLE_ATTACHMENT_BYTES = 262_144;
    static final int MAX_TOTAL_ATTACHMENT_BYTES = 524_288;
    static final int MAX_PATH_BYTES = 256;
    static final int MAX_PATH_SEGMENTS = 16;
    static final int MAX_SANITIZED_LOG_BYTES = 65_536;
    static final int MAX_SCENARIO_ID_BYTES = 64;
    static final int MAX_SUITE_ID_BYTES = 256;

    private static final int MAX_JSON_ARRAY_ELEMENTS = 256;
    private static final int MAX_JSON_OBJECT_FIELDS = 16;
    private static final int MAX_JSON_STRING_CHARS = 512;
    private static final Pattern SHA256 = Pattern.compile("[0-9a-f]{64}");
    private static final Pattern COMMIT = Pattern.compile("[0-9a-f]{40}");
    private static final Pattern IMAGE_DIGEST = Pattern.compile("sha256:[0-9a-f]{64}");
    private static final Pattern SCENARIO_ID = Pattern.compile("V2-(?:[A-Z]+-)+[0-9]{3}");
    private static final Pattern SUITE_ID = Pattern.compile("[A-Za-z0-9][A-Za-z0-9._:/-]*");

    private static final Set<String> ROOT_FIELDS = Set.of("attachments", "kind", "scenarios", "schema", "sourceTuple");
    private static final Set<String> SOURCE_FIELDS = Set.of(
            "domainJarSha256",
            "domainPomSha256",
            "kafkaCommit",
            "nereusCommit",
            "oxiaClientCommit",
            "oxiaClientJarSha256",
            "oxiaClientPomSha256",
            "oxiaServerCommit",
            "oxiaServerImageDigest",
            "pulsarCommit",
            "sourceLocksSha256");
    private static final Set<String> SCENARIO_FIELDS = Set.of("scenarioId", "suites");
    private static final Set<String> SUITE_FIELDS =
            Set.of("aborted", "discovered", "executed", "failed", "passed", "skipped", "suiteId");
    private static final Set<String> ATTACHMENT_FIELDS = Set.of("attachmentKind", "length", "path", "sha256");

    private ReceiptV1CapacityModel() {}

    enum ReceiptKind {
        REGISTRY_CONFORMANCE,
        HARNESS_CONFORMANCE_ONLY
    }

    enum AttachmentKind {
        TEST_REPORT,
        REGISTRY_BYTES,
        REGISTRY_ADMISSION_EVIDENCE,
        WRITER_INTERLOCK_SNAPSHOT,
        SANITIZED_LOG_EXCERPT
    }

    enum RejectionCode {
        RECEIPT_ROOT_NOT_REGULAR,
        RECEIPT_ROOT_BYTES_EXCEEDED,
        RECEIPT_MALFORMED_JSON,
        RECEIPT_NON_CANONICAL_JSON,
        RECEIPT_DUPLICATE_FIELD,
        RECEIPT_UNKNOWN_OR_MISSING_FIELD,
        RECEIPT_WRONG_TYPE_OR_NUMBER,
        RECEIPT_SCHEMA_OR_KIND_INVALID,
        RECEIPT_SOURCE_TUPLE_INVALID,
        RECEIPT_SCENARIO_COUNT_EXCEEDED,
        RECEIPT_SUITE_COUNT_EXCEEDED,
        RECEIPT_ATTACHMENT_COUNT_EXCEEDED,
        RECEIPT_DUPLICATE_OR_UNSORTED_ID,
        RECEIPT_ACCOUNTING_INVALID,
        RECEIPT_PATH_INVALID,
        RECEIPT_PATH_BYTES_EXCEEDED,
        RECEIPT_PATH_SEGMENTS_EXCEEDED,
        RECEIPT_ATTACHMENT_BYTES_EXCEEDED,
        RECEIPT_ATTACHMENT_TOTAL_BYTES_EXCEEDED,
        RECEIPT_SANITIZED_LOG_BYTES_EXCEEDED,
        RECEIPT_ATTACHMENT_NOT_REGULAR,
        RECEIPT_ATTACHMENT_SYMLINK,
        RECEIPT_ATTACHMENT_LENGTH_MISMATCH,
        RECEIPT_ATTACHMENT_DIGEST_MISMATCH,
        RECEIPT_CHECKED_ARITHMETIC_OVERFLOW,
        RECEIPT_REQUIRED_SUITE_MISSING,
        RECEIPT_MANDATORY_RESULT_NOT_PASS
    }

    static final class ReceiptRejectedException extends IllegalArgumentException {
        private final RejectionCode code;

        ReceiptRejectedException(RejectionCode code, String detail) {
            super(code + ": " + detail);
            this.code = Objects.requireNonNull(code, "code");
        }

        ReceiptRejectedException(RejectionCode code, String detail, Throwable cause) {
            super(code + ": " + detail, cause);
            this.code = Objects.requireNonNull(code, "code");
        }

        RejectionCode code() {
            return code;
        }
    }

    record SourceTuple(
            String nereusCommit,
            String kafkaCommit,
            String pulsarCommit,
            String oxiaClientCommit,
            String oxiaServerCommit,
            String oxiaClientJarSha256,
            String oxiaClientPomSha256,
            String domainJarSha256,
            String domainPomSha256,
            String oxiaServerImageDigest,
            String sourceLocksSha256) {}

    record SuiteResult(
            String suiteId, long discovered, long executed, long passed, long failed, long skipped, long aborted) {}

    record ScenarioResult(String scenarioId, List<SuiteResult> suites) {
        ScenarioResult {
            suites = List.copyOf(suites);
        }
    }

    record AttachmentRef(AttachmentKind attachmentKind, String path, long length, String sha256) {}

    record ReceiptRoot(
            String schema,
            ReceiptKind kind,
            SourceTuple sourceTuple,
            List<ScenarioResult> scenarios,
            List<AttachmentRef> attachments) {
        ReceiptRoot {
            scenarios = List.copyOf(scenarios);
            attachments = List.copyOf(attachments);
        }
    }

    static ReceiptRoot parseCanonical(byte[] bytes) {
        Objects.requireNonNull(bytes, "bytes");
        requireRootBytes(bytes.length);
        if (bytes.length >= 3
                && Byte.toUnsignedInt(bytes[0]) == 0xef
                && Byte.toUnsignedInt(bytes[1]) == 0xbb
                && Byte.toUnsignedInt(bytes[2]) == 0xbf) {
            throw reject(RejectionCode.RECEIPT_MALFORMED_JSON, "UTF-8 BOM is forbidden");
        }

        String json;
        try {
            json = StandardCharsets.UTF_8
                    .newDecoder()
                    .onMalformedInput(CodingErrorAction.REPORT)
                    .onUnmappableCharacter(CodingErrorAction.REPORT)
                    .decode(java.nio.ByteBuffer.wrap(bytes))
                    .toString();
        } catch (CharacterCodingException error) {
            throw new ReceiptRejectedException(RejectionCode.RECEIPT_MALFORMED_JSON, "root is not strict UTF-8", error);
        }

        Object parsed = new JsonReader(json).parseDocument();
        ReceiptRoot receipt = toReceipt(parsed);
        validate(receipt);
        byte[] canonical = canonicalBytes(receipt);
        if (!Arrays.equals(bytes, canonical)) {
            throw reject(RejectionCode.RECEIPT_NON_CANONICAL_JSON, "root differs from closed-model JCS bytes");
        }
        return receipt;
    }

    static ReceiptRoot parseCanonicalFile(Path rootFile) {
        Objects.requireNonNull(rootFile, "rootFile");
        BasicFileAttributes before = readAttributes(rootFile, RejectionCode.RECEIPT_ROOT_NOT_REGULAR);
        if (before.isSymbolicLink() || Files.isSymbolicLink(rootFile)) {
            throw reject(RejectionCode.RECEIPT_ROOT_NOT_REGULAR, "receipt root is a symlink");
        }
        if (!before.isRegularFile()) {
            throw reject(RejectionCode.RECEIPT_ROOT_NOT_REGULAR, "receipt root is not a regular file");
        }
        requireRootBytes(before.size());
        byte[] bytes = readBounded(rootFile, MAX_CANONICAL_ROOT_BYTES, RejectionCode.RECEIPT_ROOT_BYTES_EXCEEDED);
        BasicFileAttributes after = readAttributes(rootFile, RejectionCode.RECEIPT_ROOT_NOT_REGULAR);
        requireSameOpenFile(before, after, RejectionCode.RECEIPT_ROOT_NOT_REGULAR);
        return parseCanonical(bytes);
    }

    static byte[] canonicalBytes(ReceiptRoot receipt) {
        validate(receipt);
        StringBuilder output = new StringBuilder();
        output.append('{');
        appendQuoted(output, "attachments");
        output.append(':');
        appendAttachments(output, receipt.attachments());
        output.append(',');
        appendQuoted(output, "kind");
        output.append(':');
        appendQuoted(output, receipt.kind().name());
        output.append(',');
        appendQuoted(output, "scenarios");
        output.append(':');
        appendScenarios(output, receipt.scenarios());
        output.append(',');
        appendQuoted(output, "schema");
        output.append(':');
        appendQuoted(output, receipt.schema());
        output.append(',');
        appendQuoted(output, "sourceTuple");
        output.append(':');
        appendSourceTuple(output, receipt.sourceTuple());
        output.append('}');
        byte[] bytes = output.toString().getBytes(StandardCharsets.UTF_8);
        requireRootBytes(bytes.length);
        return bytes;
    }

    static void validate(ReceiptRoot receipt) {
        Objects.requireNonNull(receipt, "receipt");
        if (!SCHEMA.equals(receipt.schema()) || receipt.kind() == null) {
            throw reject(RejectionCode.RECEIPT_SCHEMA_OR_KIND_INVALID, "unknown schema or receipt kind");
        }
        validateSourceTuple(receipt.sourceTuple());
        requireScenarioCount(receipt.scenarios().size());

        String previousScenario = null;
        for (ScenarioResult scenario : receipt.scenarios()) {
            Objects.requireNonNull(scenario, "scenario");
            requireAscii(scenario.scenarioId(), MAX_SCENARIO_ID_BYTES, RejectionCode.RECEIPT_DUPLICATE_OR_UNSORTED_ID);
            if (!SCENARIO_ID.matcher(scenario.scenarioId()).matches()) {
                throw reject(RejectionCode.RECEIPT_DUPLICATE_OR_UNSORTED_ID, "invalid scenario ID");
            }
            if (previousScenario != null && previousScenario.compareTo(scenario.scenarioId()) >= 0) {
                throw reject(RejectionCode.RECEIPT_DUPLICATE_OR_UNSORTED_ID, "scenario IDs must be sorted and unique");
            }
            previousScenario = scenario.scenarioId();
            requireSuiteCount(scenario.suites().size());

            String previousSuite = null;
            for (SuiteResult suite : scenario.suites()) {
                validateSuite(suite);
                if (previousSuite != null && previousSuite.compareTo(suite.suiteId()) >= 0) {
                    throw reject(
                            RejectionCode.RECEIPT_DUPLICATE_OR_UNSORTED_ID,
                            "suite IDs must be sorted and unique within a scenario");
                }
                previousSuite = suite.suiteId();
            }
        }

        requireAttachmentCount(receipt.attachments().size());
        long totalBytes = 0;
        String previousPath = null;
        for (AttachmentRef attachment : receipt.attachments()) {
            Objects.requireNonNull(attachment, "attachment");
            Objects.requireNonNull(attachment.attachmentKind(), "attachmentKind");
            validatePath(attachment.path());
            if (previousPath != null && previousPath.compareTo(attachment.path()) >= 0) {
                throw reject(
                        RejectionCode.RECEIPT_DUPLICATE_OR_UNSORTED_ID, "attachment paths must be sorted and unique");
            }
            previousPath = attachment.path();
            requireSingleAttachmentBytes(attachment.length());
            if (attachment.attachmentKind() == AttachmentKind.SANITIZED_LOG_EXCERPT) {
                requireSanitizedLogBytes(attachment.length());
            }
            if (!SHA256.matcher(Objects.requireNonNull(attachment.sha256(), "sha256"))
                    .matches()) {
                throw reject(RejectionCode.RECEIPT_ATTACHMENT_DIGEST_MISMATCH, "invalid SHA-256 grammar");
            }
            totalBytes = checkedAdd(totalBytes, attachment.length());
            requireTotalAttachmentBytes(totalBytes);
        }
    }

    static SuiteResult normalizeJUnit(
            String suiteId, long tests, long failures, long errors, long skipped, long aborted) {
        requireCount(tests);
        requireCount(failures);
        requireCount(errors);
        requireCount(skipped);
        requireCount(aborted);
        long failed = checkedAdd(failures, errors);
        long executed = checkedSubtract(tests, skipped);
        long passed = checkedSubtract(executed, checkedAdd(failed, aborted));
        SuiteResult suite = new SuiteResult(suiteId, tests, executed, passed, failed, skipped, aborted);
        validateSuite(suite);
        return suite;
    }

    static void requireMandatoryPass(ReceiptRoot receipt, Set<String> requiredSuiteIds) {
        validate(receipt);
        Objects.requireNonNull(requiredSuiteIds, "requiredSuiteIds");
        Set<String> seen = new HashSet<>();
        for (ScenarioResult scenario : receipt.scenarios()) {
            for (SuiteResult suite : scenario.suites()) {
                seen.add(suite.suiteId());
                if (suite.discovered() == 0
                        || suite.executed() == 0
                        || suite.failed() != 0
                        || suite.skipped() != 0
                        || suite.aborted() != 0) {
                    throw reject(
                            RejectionCode.RECEIPT_MANDATORY_RESULT_NOT_PASS,
                            "mandatory suite is zero, failed, skipped, or aborted: " + suite.suiteId());
                }
            }
        }
        if (!seen.containsAll(requiredSuiteIds)) {
            throw reject(RejectionCode.RECEIPT_REQUIRED_SUITE_MISSING, "required suite is absent");
        }
    }

    static Map<AttachmentKind, Long> verifyAttachments(Path receiptDirectory, ReceiptRoot receipt) {
        validate(receipt);
        Objects.requireNonNull(receiptDirectory, "receiptDirectory");
        BasicFileAttributes rootAttributes =
                readAttributes(receiptDirectory, RejectionCode.RECEIPT_ATTACHMENT_NOT_REGULAR);
        if (rootAttributes.isSymbolicLink() || Files.isSymbolicLink(receiptDirectory)) {
            throw reject(RejectionCode.RECEIPT_ATTACHMENT_SYMLINK, "receipt directory is a symlink");
        }
        if (!rootAttributes.isDirectory()) {
            throw reject(RejectionCode.RECEIPT_ATTACHMENT_NOT_REGULAR, "receipt root is not a directory");
        }

        Path realRoot;
        try {
            realRoot = receiptDirectory.toRealPath(LinkOption.NOFOLLOW_LINKS);
        } catch (IOException error) {
            throw new ReceiptRejectedException(
                    RejectionCode.RECEIPT_ATTACHMENT_NOT_REGULAR, "cannot resolve receipt directory", error);
        }

        Map<AttachmentKind, Long> verifiedByKind = new EnumMap<>(AttachmentKind.class);
        for (AttachmentRef attachment : receipt.attachments()) {
            Path target = resolveWithoutLinks(realRoot, attachment.path());
            BasicFileAttributes before = readAttributes(target, RejectionCode.RECEIPT_ATTACHMENT_NOT_REGULAR);
            if (before.isSymbolicLink() || Files.isSymbolicLink(target)) {
                throw reject(RejectionCode.RECEIPT_ATTACHMENT_SYMLINK, "attachment is a symlink");
            }
            if (!before.isRegularFile()) {
                throw reject(RejectionCode.RECEIPT_ATTACHMENT_NOT_REGULAR, "attachment is not a regular file");
            }
            if (before.size() != attachment.length()) {
                throw reject(
                        RejectionCode.RECEIPT_ATTACHMENT_LENGTH_MISMATCH,
                        "declared and stat lengths differ for " + attachment.path());
            }

            MessageDigest digest = sha256Digest();
            long actualLength = 0;
            try (InputStream input = java.nio.channels.Channels.newInputStream(Files.newByteChannel(
                    target, Set.<OpenOption>of(StandardOpenOption.READ, LinkOption.NOFOLLOW_LINKS)))) {
                byte[] buffer = new byte[8192];
                int read;
                while ((read = input.read(buffer)) != -1) {
                    actualLength = checkedAdd(actualLength, read);
                    if (actualLength > attachment.length()) {
                        throw reject(
                                RejectionCode.RECEIPT_ATTACHMENT_LENGTH_MISMATCH,
                                "attachment contains bytes after its declared length");
                    }
                    digest.update(buffer, 0, read);
                }
            } catch (ReceiptRejectedException error) {
                throw error;
            } catch (IOException error) {
                throw new ReceiptRejectedException(
                        RejectionCode.RECEIPT_ATTACHMENT_NOT_REGULAR, "attachment read failed", error);
            }
            if (actualLength != attachment.length()) {
                throw reject(
                        RejectionCode.RECEIPT_ATTACHMENT_LENGTH_MISMATCH,
                        "attachment ended before its declared length");
            }
            String actualSha = toHex(digest.digest());
            if (!actualSha.equals(attachment.sha256())) {
                throw reject(
                        RejectionCode.RECEIPT_ATTACHMENT_DIGEST_MISMATCH,
                        "attachment SHA-256 differs for " + attachment.path());
            }
            BasicFileAttributes after = readAttributes(target, RejectionCode.RECEIPT_ATTACHMENT_NOT_REGULAR);
            requireSameOpenFile(before, after, RejectionCode.RECEIPT_ATTACHMENT_NOT_REGULAR);
            verifiedByKind.merge(attachment.attachmentKind(), actualLength, ReceiptV1CapacityModel::checkedAdd);
        }
        return Map.copyOf(verifiedByKind);
    }

    static void requireRootBytes(long bytes) {
        requireBound(bytes, MAX_CANONICAL_ROOT_BYTES, RejectionCode.RECEIPT_ROOT_BYTES_EXCEEDED);
    }

    static void requireScenarioCount(int count) {
        if (count <= 0 || count > MAX_SCENARIOS) {
            throw reject(RejectionCode.RECEIPT_SCENARIO_COUNT_EXCEEDED, "scenario count=" + count);
        }
    }

    static void requireSuiteCount(int count) {
        if (count <= 0 || count > MAX_SUITES_PER_SCENARIO) {
            throw reject(RejectionCode.RECEIPT_SUITE_COUNT_EXCEEDED, "suite count=" + count);
        }
    }

    static void requireAttachmentCount(int count) {
        if (count < 0 || count > MAX_ATTACHMENTS) {
            throw reject(RejectionCode.RECEIPT_ATTACHMENT_COUNT_EXCEEDED, "attachment count=" + count);
        }
    }

    static void requireSingleAttachmentBytes(long bytes) {
        requireBound(bytes, MAX_SINGLE_ATTACHMENT_BYTES, RejectionCode.RECEIPT_ATTACHMENT_BYTES_EXCEEDED);
    }

    static void requireTotalAttachmentBytes(long bytes) {
        requireBound(bytes, MAX_TOTAL_ATTACHMENT_BYTES, RejectionCode.RECEIPT_ATTACHMENT_TOTAL_BYTES_EXCEEDED);
    }

    static void requireSanitizedLogBytes(long bytes) {
        requireBound(bytes, MAX_SANITIZED_LOG_BYTES, RejectionCode.RECEIPT_SANITIZED_LOG_BYTES_EXCEEDED);
    }

    static List<String> validatePath(String path) {
        Objects.requireNonNull(path, "path");
        int pathBytes = asciiLength(path, RejectionCode.RECEIPT_PATH_INVALID);
        if (pathBytes == 0 || pathBytes > MAX_PATH_BYTES) {
            throw reject(RejectionCode.RECEIPT_PATH_BYTES_EXCEEDED, "path bytes=" + pathBytes);
        }
        if (path.startsWith("/") || path.endsWith("/") || path.indexOf('\\') >= 0 || path.indexOf(':') >= 0) {
            throw reject(RejectionCode.RECEIPT_PATH_INVALID, "path is not canonical POSIX-relative ASCII");
        }
        String[] segments = path.split("/", -1);
        if (segments.length > MAX_PATH_SEGMENTS) {
            throw reject(RejectionCode.RECEIPT_PATH_SEGMENTS_EXCEEDED, "path segments=" + segments.length);
        }
        for (String segment : segments) {
            if (segment.isEmpty() || segment.equals(".") || segment.equals("..")) {
                throw reject(RejectionCode.RECEIPT_PATH_INVALID, "empty or traversal path segment");
            }
            if (!isAsciiAlphaNumeric(segment.charAt(0))) {
                throw reject(RejectionCode.RECEIPT_PATH_INVALID, "path segment must begin alphanumeric");
            }
            for (int index = 0; index < segment.length(); index++) {
                char value = segment.charAt(index);
                if (!isAsciiAlphaNumeric(value) && value != '.' && value != '_' && value != '-') {
                    throw reject(RejectionCode.RECEIPT_PATH_INVALID, "forbidden path byte");
                }
            }
        }
        return List.of(segments);
    }

    static long checkedAdd(long... values) {
        long result = 0;
        try {
            for (long value : values) {
                result = Math.addExact(result, value);
            }
            return result;
        } catch (ArithmeticException error) {
            throw new ReceiptRejectedException(
                    RejectionCode.RECEIPT_CHECKED_ARITHMETIC_OVERFLOW, "checked addition overflow", error);
        }
    }

    static long checkedMultiply(long left, long right) {
        try {
            return Math.multiplyExact(left, right);
        } catch (ArithmeticException error) {
            throw new ReceiptRejectedException(
                    RejectionCode.RECEIPT_CHECKED_ARITHMETIC_OVERFLOW, "checked multiplication overflow", error);
        }
    }

    static String sha256(byte[] bytes) {
        return toHex(sha256Digest().digest(bytes));
    }

    private static ReceiptRoot toReceipt(Object value) {
        Map<String, Object> root = requireObject(value, ROOT_FIELDS);
        String schema = requireString(root, "schema");
        String kindValue = requireString(root, "kind");
        ReceiptKind kind;
        try {
            kind = ReceiptKind.valueOf(kindValue);
        } catch (IllegalArgumentException error) {
            throw reject(RejectionCode.RECEIPT_SCHEMA_OR_KIND_INVALID, "unknown receipt kind");
        }

        Map<String, Object> source = requireObject(root.get("sourceTuple"), SOURCE_FIELDS);
        SourceTuple sourceTuple = new SourceTuple(
                requireString(source, "nereusCommit"),
                requireString(source, "kafkaCommit"),
                requireString(source, "pulsarCommit"),
                requireString(source, "oxiaClientCommit"),
                requireString(source, "oxiaServerCommit"),
                requireString(source, "oxiaClientJarSha256"),
                requireString(source, "oxiaClientPomSha256"),
                requireString(source, "domainJarSha256"),
                requireString(source, "domainPomSha256"),
                requireString(source, "oxiaServerImageDigest"),
                requireString(source, "sourceLocksSha256"));

        List<Object> scenarioValues = requireArray(root, "scenarios");
        if (scenarioValues.size() > MAX_SCENARIOS) {
            throw reject(RejectionCode.RECEIPT_SCENARIO_COUNT_EXCEEDED, "scenario array exceeds cap");
        }
        List<ScenarioResult> scenarios = new ArrayList<>(scenarioValues.size());
        for (Object scenarioValue : scenarioValues) {
            Map<String, Object> scenario = requireObject(scenarioValue, SCENARIO_FIELDS);
            List<Object> suiteValues = requireArray(scenario, "suites");
            if (suiteValues.size() > MAX_SUITES_PER_SCENARIO) {
                throw reject(RejectionCode.RECEIPT_SUITE_COUNT_EXCEEDED, "suite array exceeds cap");
            }
            List<SuiteResult> suites = new ArrayList<>(suiteValues.size());
            for (Object suiteValue : suiteValues) {
                Map<String, Object> suite = requireObject(suiteValue, SUITE_FIELDS);
                suites.add(new SuiteResult(
                        requireString(suite, "suiteId"),
                        requireLong(suite, "discovered"),
                        requireLong(suite, "executed"),
                        requireLong(suite, "passed"),
                        requireLong(suite, "failed"),
                        requireLong(suite, "skipped"),
                        requireLong(suite, "aborted")));
            }
            scenarios.add(new ScenarioResult(requireString(scenario, "scenarioId"), suites));
        }

        List<Object> attachmentValues = requireArray(root, "attachments");
        if (attachmentValues.size() > MAX_ATTACHMENTS) {
            throw reject(RejectionCode.RECEIPT_ATTACHMENT_COUNT_EXCEEDED, "attachment array exceeds cap");
        }
        List<AttachmentRef> attachments = new ArrayList<>(attachmentValues.size());
        for (Object attachmentValue : attachmentValues) {
            Map<String, Object> attachment = requireObject(attachmentValue, ATTACHMENT_FIELDS);
            String attachmentKindValue = requireString(attachment, "attachmentKind");
            AttachmentKind attachmentKind;
            try {
                attachmentKind = AttachmentKind.valueOf(attachmentKindValue);
            } catch (IllegalArgumentException error) {
                throw reject(RejectionCode.RECEIPT_SCHEMA_OR_KIND_INVALID, "unknown attachment kind");
            }
            attachments.add(new AttachmentRef(
                    attachmentKind,
                    requireString(attachment, "path"),
                    requireLong(attachment, "length"),
                    requireString(attachment, "sha256")));
        }
        return new ReceiptRoot(schema, kind, sourceTuple, scenarios, attachments);
    }

    private static void validateSourceTuple(SourceTuple source) {
        Objects.requireNonNull(source, "sourceTuple");
        for (String commit : List.of(
                source.nereusCommit(),
                source.kafkaCommit(),
                source.pulsarCommit(),
                source.oxiaClientCommit(),
                source.oxiaServerCommit())) {
            if (!COMMIT.matcher(Objects.requireNonNull(commit, "commit")).matches()) {
                throw reject(RejectionCode.RECEIPT_SOURCE_TUPLE_INVALID, "invalid source commit");
            }
        }
        for (String digest : List.of(
                source.oxiaClientJarSha256(),
                source.oxiaClientPomSha256(),
                source.domainJarSha256(),
                source.domainPomSha256(),
                source.sourceLocksSha256())) {
            if (!SHA256.matcher(Objects.requireNonNull(digest, "digest")).matches()) {
                throw reject(RejectionCode.RECEIPT_SOURCE_TUPLE_INVALID, "invalid source tuple SHA-256");
            }
        }
        if (!IMAGE_DIGEST
                .matcher(Objects.requireNonNull(source.oxiaServerImageDigest(), "imageDigest"))
                .matches()) {
            throw reject(RejectionCode.RECEIPT_SOURCE_TUPLE_INVALID, "invalid server image digest");
        }
    }

    private static void validateSuite(SuiteResult suite) {
        Objects.requireNonNull(suite, "suite");
        requireAscii(suite.suiteId(), MAX_SUITE_ID_BYTES, RejectionCode.RECEIPT_DUPLICATE_OR_UNSORTED_ID);
        if (!SUITE_ID.matcher(suite.suiteId()).matches()) {
            throw reject(RejectionCode.RECEIPT_DUPLICATE_OR_UNSORTED_ID, "invalid suite ID");
        }
        requireCount(suite.discovered());
        requireCount(suite.executed());
        requireCount(suite.passed());
        requireCount(suite.failed());
        requireCount(suite.skipped());
        requireCount(suite.aborted());
        if (suite.discovered() != checkedAdd(suite.executed(), suite.skipped())
                || suite.executed() != checkedAdd(suite.passed(), suite.failed(), suite.aborted())) {
            throw reject(RejectionCode.RECEIPT_ACCOUNTING_INVALID, "suite accounting equations differ");
        }
    }

    private static void requireCount(long value) {
        if (value < 0 || value > MAX_EXACT_JSON_INTEGER) {
            throw reject(RejectionCode.RECEIPT_WRONG_TYPE_OR_NUMBER, "count is outside exact JSON integer range");
        }
    }

    private static void requireBound(long value, long maximum, RejectionCode code) {
        if (value < 0 || value > maximum) {
            throw reject(code, "value=" + value + " maximum=" + maximum);
        }
    }

    private static long checkedSubtract(long left, long right) {
        try {
            long result = Math.subtractExact(left, right);
            if (result < 0) {
                throw new ArithmeticException("negative result");
            }
            return result;
        } catch (ArithmeticException error) {
            throw new ReceiptRejectedException(
                    RejectionCode.RECEIPT_ACCOUNTING_INVALID, "checked accounting subtraction failed", error);
        }
    }

    private static Path resolveWithoutLinks(Path realRoot, String canonicalPath) {
        Path current = realRoot;
        List<String> segments = validatePath(canonicalPath);
        for (int index = 0; index < segments.size(); index++) {
            current = current.resolve(segments.get(index));
            BasicFileAttributes attributes = readAttributes(current, RejectionCode.RECEIPT_ATTACHMENT_NOT_REGULAR);
            if (attributes.isSymbolicLink() || Files.isSymbolicLink(current)) {
                throw reject(RejectionCode.RECEIPT_ATTACHMENT_SYMLINK, "path contains a symlink");
            }
            if (index + 1 < segments.size() && !attributes.isDirectory()) {
                throw reject(RejectionCode.RECEIPT_ATTACHMENT_NOT_REGULAR, "path ancestor is not a directory");
            }
        }
        Path normalized = current.normalize();
        if (!normalized.startsWith(realRoot)) {
            throw reject(RejectionCode.RECEIPT_PATH_INVALID, "normalized path leaves receipt root");
        }
        return normalized;
    }

    private static BasicFileAttributes readAttributes(Path path, RejectionCode code) {
        try {
            return Files.readAttributes(path, BasicFileAttributes.class, LinkOption.NOFOLLOW_LINKS);
        } catch (IOException error) {
            throw new ReceiptRejectedException(code, "cannot read file attributes", error);
        }
    }

    private static void requireSameOpenFile(BasicFileAttributes before, BasicFileAttributes after, RejectionCode code) {
        if (after.isSymbolicLink() || !after.isRegularFile() || before.size() != after.size()) {
            throw reject(code, "file identity or attributes changed while reading");
        }
        if (before.fileKey() != null
                && after.fileKey() != null
                && !before.fileKey().equals(after.fileKey())) {
            throw reject(code, "file identity changed while reading");
        }
    }

    private static byte[] readBounded(Path path, int maximum, RejectionCode overflowCode) {
        try (InputStream input = java.nio.channels.Channels.newInputStream(Files.newByteChannel(
                        path, Set.<OpenOption>of(StandardOpenOption.READ, LinkOption.NOFOLLOW_LINKS)));
                ByteArrayOutputStream output = new ByteArrayOutputStream(Math.min(maximum, 8192))) {
            byte[] buffer = new byte[8192];
            int total = 0;
            int read;
            while ((read = input.read(buffer)) != -1) {
                total = Math.addExact(total, read);
                if (total > maximum) {
                    throw reject(overflowCode, "streamed bytes exceed cap");
                }
                output.write(buffer, 0, read);
            }
            return output.toByteArray();
        } catch (ReceiptRejectedException error) {
            throw error;
        } catch (ArithmeticException error) {
            throw new ReceiptRejectedException(
                    RejectionCode.RECEIPT_CHECKED_ARITHMETIC_OVERFLOW, "stream length overflow", error);
        } catch (IOException error) {
            throw new ReceiptRejectedException(
                    RejectionCode.RECEIPT_ROOT_NOT_REGULAR, "cannot read bounded root", error);
        }
    }

    private static void appendAttachments(StringBuilder output, List<AttachmentRef> attachments) {
        output.append('[');
        for (int index = 0; index < attachments.size(); index++) {
            if (index != 0) {
                output.append(',');
            }
            AttachmentRef attachment = attachments.get(index);
            output.append('{');
            appendQuoted(output, "attachmentKind");
            output.append(':');
            appendQuoted(output, attachment.attachmentKind().name());
            output.append(',');
            appendQuoted(output, "length");
            output.append(':').append(attachment.length());
            output.append(',');
            appendQuoted(output, "path");
            output.append(':');
            appendQuoted(output, attachment.path());
            output.append(',');
            appendQuoted(output, "sha256");
            output.append(':');
            appendQuoted(output, attachment.sha256());
            output.append('}');
        }
        output.append(']');
    }

    private static void appendScenarios(StringBuilder output, List<ScenarioResult> scenarios) {
        output.append('[');
        for (int scenarioIndex = 0; scenarioIndex < scenarios.size(); scenarioIndex++) {
            if (scenarioIndex != 0) {
                output.append(',');
            }
            ScenarioResult scenario = scenarios.get(scenarioIndex);
            output.append('{');
            appendQuoted(output, "scenarioId");
            output.append(':');
            appendQuoted(output, scenario.scenarioId());
            output.append(',');
            appendQuoted(output, "suites");
            output.append(':').append('[');
            for (int suiteIndex = 0; suiteIndex < scenario.suites().size(); suiteIndex++) {
                if (suiteIndex != 0) {
                    output.append(',');
                }
                SuiteResult suite = scenario.suites().get(suiteIndex);
                output.append('{');
                appendQuoted(output, "aborted");
                output.append(':').append(suite.aborted());
                output.append(',');
                appendQuoted(output, "discovered");
                output.append(':').append(suite.discovered());
                output.append(',');
                appendQuoted(output, "executed");
                output.append(':').append(suite.executed());
                output.append(',');
                appendQuoted(output, "failed");
                output.append(':').append(suite.failed());
                output.append(',');
                appendQuoted(output, "passed");
                output.append(':').append(suite.passed());
                output.append(',');
                appendQuoted(output, "skipped");
                output.append(':').append(suite.skipped());
                output.append(',');
                appendQuoted(output, "suiteId");
                output.append(':');
                appendQuoted(output, suite.suiteId());
                output.append('}');
            }
            output.append(']').append('}');
        }
        output.append(']');
    }

    private static void appendSourceTuple(StringBuilder output, SourceTuple source) {
        output.append('{');
        appendStringMember(output, "domainJarSha256", source.domainJarSha256(), false);
        appendStringMember(output, "domainPomSha256", source.domainPomSha256(), true);
        appendStringMember(output, "kafkaCommit", source.kafkaCommit(), true);
        appendStringMember(output, "nereusCommit", source.nereusCommit(), true);
        appendStringMember(output, "oxiaClientCommit", source.oxiaClientCommit(), true);
        appendStringMember(output, "oxiaClientJarSha256", source.oxiaClientJarSha256(), true);
        appendStringMember(output, "oxiaClientPomSha256", source.oxiaClientPomSha256(), true);
        appendStringMember(output, "oxiaServerCommit", source.oxiaServerCommit(), true);
        appendStringMember(output, "oxiaServerImageDigest", source.oxiaServerImageDigest(), true);
        appendStringMember(output, "pulsarCommit", source.pulsarCommit(), true);
        appendStringMember(output, "sourceLocksSha256", source.sourceLocksSha256(), true);
        output.append('}');
    }

    private static void appendStringMember(StringBuilder output, String name, String value, boolean prependComma) {
        if (prependComma) {
            output.append(',');
        }
        appendQuoted(output, name);
        output.append(':');
        appendQuoted(output, value);
    }

    private static void appendQuoted(StringBuilder output, String value) {
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
                        output.append("\\u").append(String.format(java.util.Locale.ROOT, "%04x", (int) character));
                    } else {
                        output.append(character);
                    }
                }
            }
        }
        output.append('"');
    }

    private static Map<String, Object> requireObject(Object value, Set<String> expectedFields) {
        if (!(value instanceof Map<?, ?> input)) {
            throw reject(RejectionCode.RECEIPT_WRONG_TYPE_OR_NUMBER, "expected JSON object");
        }
        Map<String, Object> result = new LinkedHashMap<>();
        for (Map.Entry<?, ?> entry : input.entrySet()) {
            if (!(entry.getKey() instanceof String key)) {
                throw reject(RejectionCode.RECEIPT_WRONG_TYPE_OR_NUMBER, "object key is not a string");
            }
            result.put(key, entry.getValue());
        }
        if (!result.keySet().equals(expectedFields)) {
            throw reject(
                    RejectionCode.RECEIPT_UNKNOWN_OR_MISSING_FIELD,
                    "expected fields=" + expectedFields + " actual=" + result.keySet());
        }
        return result;
    }

    private static String requireString(Map<String, Object> object, String field) {
        Object value = object.get(field);
        if (!(value instanceof String string)) {
            throw reject(RejectionCode.RECEIPT_WRONG_TYPE_OR_NUMBER, field + " must be a string");
        }
        return string;
    }

    private static long requireLong(Map<String, Object> object, String field) {
        Object value = object.get(field);
        if (!(value instanceof Long number)) {
            throw reject(RejectionCode.RECEIPT_WRONG_TYPE_OR_NUMBER, field + " must be an exact integer");
        }
        requireCount(number);
        return number;
    }

    @SuppressWarnings("unchecked")
    private static List<Object> requireArray(Map<String, Object> object, String field) {
        Object value = object.get(field);
        if (!(value instanceof List<?> list)) {
            throw reject(RejectionCode.RECEIPT_WRONG_TYPE_OR_NUMBER, field + " must be an array");
        }
        return (List<Object>) list;
    }

    private static void requireAscii(String value, int maximumBytes, RejectionCode code) {
        int bytes = asciiLength(Objects.requireNonNull(value, "value"), code);
        if (bytes == 0 || bytes > maximumBytes) {
            throw reject(code, "ASCII field bytes=" + bytes + " maximum=" + maximumBytes);
        }
    }

    private static int asciiLength(String value, RejectionCode code) {
        for (int index = 0; index < value.length(); index++) {
            char character = value.charAt(index);
            if (character < 0x20 || character > 0x7e) {
                throw reject(code, "non-safe-ASCII byte");
            }
        }
        return value.length();
    }

    private static boolean isAsciiAlphaNumeric(char value) {
        return (value >= 'a' && value <= 'z') || (value >= 'A' && value <= 'Z') || (value >= '0' && value <= '9');
    }

    private static MessageDigest sha256Digest() {
        try {
            return MessageDigest.getInstance("SHA-256");
        } catch (NoSuchAlgorithmException error) {
            throw new IllegalStateException("JDK SHA-256 is unavailable", error);
        }
    }

    private static String toHex(byte[] bytes) {
        return java.util.HexFormat.of().formatHex(bytes);
    }

    private static ReceiptRejectedException reject(RejectionCode code, String detail) {
        return new ReceiptRejectedException(code, detail);
    }

    private static final class JsonReader {
        private final String input;
        private int offset;

        JsonReader(String input) {
            this.input = Objects.requireNonNull(input, "input");
        }

        Object parseDocument() {
            skipWhitespace();
            Object value = parseValue(0, null);
            skipWhitespace();
            if (offset != input.length()) {
                throw reject(RejectionCode.RECEIPT_MALFORMED_JSON, "trailing JSON bytes");
            }
            return value;
        }

        private Object parseValue(int depth, String fieldName) {
            if (depth > 8 || offset >= input.length()) {
                throw reject(RejectionCode.RECEIPT_MALFORMED_JSON, "unexpected JSON depth or EOF");
            }
            return switch (input.charAt(offset)) {
                case '{' -> parseObject(depth + 1);
                case '[' -> parseArray(depth + 1, arrayBound(fieldName));
                case '"' -> parseString();
                case 't' -> parseLiteral("true", Boolean.TRUE);
                case 'f' -> parseLiteral("false", Boolean.FALSE);
                case 'n' -> parseLiteral("null", null);
                default -> parseNumber();
            };
        }

        private Map<String, Object> parseObject(int depth) {
            expect('{');
            skipWhitespace();
            Map<String, Object> values = new LinkedHashMap<>();
            if (consume('}')) {
                return values;
            }
            while (true) {
                if (offset >= input.length() || input.charAt(offset) != '"') {
                    throw reject(RejectionCode.RECEIPT_MALFORMED_JSON, "object key must be a string");
                }
                String key = parseString();
                if (values.containsKey(key)) {
                    throw reject(RejectionCode.RECEIPT_DUPLICATE_FIELD, "duplicate field " + key);
                }
                skipWhitespace();
                expect(':');
                skipWhitespace();
                values.put(key, parseValue(depth, key));
                if (values.size() > MAX_JSON_OBJECT_FIELDS) {
                    throw reject(RejectionCode.RECEIPT_UNKNOWN_OR_MISSING_FIELD, "too many object fields");
                }
                skipWhitespace();
                if (consume('}')) {
                    return values;
                }
                expect(',');
                skipWhitespace();
            }
        }

        private List<Object> parseArray(int depth, ArrayBound bound) {
            expect('[');
            skipWhitespace();
            List<Object> values = new ArrayList<>(bound.maximum());
            if (consume(']')) {
                return values;
            }
            while (true) {
                if (values.size() == bound.maximum()) {
                    throw reject(bound.rejectionCode(), "JSON array exceeds pre-allocation guard");
                }
                values.add(parseValue(depth, null));
                skipWhitespace();
                if (consume(']')) {
                    return values;
                }
                expect(',');
                skipWhitespace();
            }
        }

        private static ArrayBound arrayBound(String fieldName) {
            if ("scenarios".equals(fieldName)) {
                return new ArrayBound(MAX_SCENARIOS, RejectionCode.RECEIPT_SCENARIO_COUNT_EXCEEDED);
            }
            if ("suites".equals(fieldName)) {
                return new ArrayBound(MAX_SUITES_PER_SCENARIO, RejectionCode.RECEIPT_SUITE_COUNT_EXCEEDED);
            }
            if ("attachments".equals(fieldName)) {
                return new ArrayBound(MAX_ATTACHMENTS, RejectionCode.RECEIPT_ATTACHMENT_COUNT_EXCEEDED);
            }
            return new ArrayBound(MAX_JSON_ARRAY_ELEMENTS, RejectionCode.RECEIPT_WRONG_TYPE_OR_NUMBER);
        }

        private record ArrayBound(int maximum, RejectionCode rejectionCode) {}

        private String parseString() {
            expect('"');
            StringBuilder result = new StringBuilder();
            while (offset < input.length()) {
                char character = input.charAt(offset++);
                if (character == '"') {
                    return result.toString();
                }
                if (character < 0x20) {
                    throw reject(RejectionCode.RECEIPT_MALFORMED_JSON, "unescaped control character");
                }
                if (character == '\\') {
                    if (offset >= input.length()) {
                        throw reject(RejectionCode.RECEIPT_MALFORMED_JSON, "truncated escape");
                    }
                    char escape = input.charAt(offset++);
                    switch (escape) {
                        case '"', '\\', '/' -> result.append(escape);
                        case 'b' -> result.append('\b');
                        case 'f' -> result.append('\f');
                        case 'n' -> result.append('\n');
                        case 'r' -> result.append('\r');
                        case 't' -> result.append('\t');
                        case 'u' -> appendUnicodeEscape(result);
                        default -> throw reject(RejectionCode.RECEIPT_MALFORMED_JSON, "unknown string escape");
                    }
                } else {
                    result.append(character);
                }
                if (result.length() > MAX_JSON_STRING_CHARS) {
                    throw reject(RejectionCode.RECEIPT_WRONG_TYPE_OR_NUMBER, "JSON string exceeds parser guard");
                }
            }
            throw reject(RejectionCode.RECEIPT_MALFORMED_JSON, "unterminated string");
        }

        private void appendUnicodeEscape(StringBuilder result) {
            char first = parseHexCodeUnit();
            if (Character.isHighSurrogate(first)) {
                if (offset + 2 > input.length() || input.charAt(offset) != '\\' || input.charAt(offset + 1) != 'u') {
                    throw reject(RejectionCode.RECEIPT_MALFORMED_JSON, "unpaired high surrogate");
                }
                offset += 2;
                char second = parseHexCodeUnit();
                if (!Character.isLowSurrogate(second)) {
                    throw reject(RejectionCode.RECEIPT_MALFORMED_JSON, "unpaired high surrogate");
                }
                result.append(first).append(second);
            } else if (Character.isLowSurrogate(first)) {
                throw reject(RejectionCode.RECEIPT_MALFORMED_JSON, "unpaired low surrogate");
            } else {
                result.append(first);
            }
        }

        private char parseHexCodeUnit() {
            if (offset + 4 > input.length()) {
                throw reject(RejectionCode.RECEIPT_MALFORMED_JSON, "truncated Unicode escape");
            }
            int value = 0;
            for (int index = 0; index < 4; index++) {
                int digit = Character.digit(input.charAt(offset++), 16);
                if (digit < 0) {
                    throw reject(RejectionCode.RECEIPT_MALFORMED_JSON, "invalid Unicode escape");
                }
                value = value * 16 + digit;
            }
            return (char) value;
        }

        private Object parseNumber() {
            int start = offset;
            if (consume('-')) {
                // Parsed below and rejected as a non-canonical non-negative receipt count.
            }
            int digitStart = offset;
            while (offset < input.length() && Character.isDigit(input.charAt(offset))) {
                offset++;
            }
            if (digitStart == offset) {
                throw reject(RejectionCode.RECEIPT_MALFORMED_JSON, "expected JSON value");
            }
            boolean fractional = false;
            if (offset < input.length() && input.charAt(offset) == '.') {
                fractional = true;
                offset++;
                while (offset < input.length() && Character.isDigit(input.charAt(offset))) {
                    offset++;
                }
            }
            if (offset < input.length() && (input.charAt(offset) == 'e' || input.charAt(offset) == 'E')) {
                fractional = true;
                offset++;
                if (offset < input.length() && (input.charAt(offset) == '+' || input.charAt(offset) == '-')) {
                    offset++;
                }
                while (offset < input.length() && Character.isDigit(input.charAt(offset))) {
                    offset++;
                }
            }
            String token = input.substring(start, offset);
            if (fractional || token.startsWith("-") || (token.length() > 1 && token.charAt(0) == '0')) {
                throw reject(
                        RejectionCode.RECEIPT_WRONG_TYPE_OR_NUMBER, "number is not a canonical non-negative integer");
            }
            try {
                long value = Long.parseLong(token);
                requireCount(value);
                return value;
            } catch (NumberFormatException error) {
                throw new ReceiptRejectedException(
                        RejectionCode.RECEIPT_WRONG_TYPE_OR_NUMBER, "integer is outside long range", error);
            }
        }

        private Object parseLiteral(String literal, Object value) {
            if (!input.startsWith(literal, offset)) {
                throw reject(RejectionCode.RECEIPT_MALFORMED_JSON, "invalid JSON literal");
            }
            offset += literal.length();
            return value;
        }

        private void skipWhitespace() {
            while (offset < input.length()) {
                char character = input.charAt(offset);
                if (character == ' ' || character == '\n' || character == '\r' || character == '\t') {
                    offset++;
                } else {
                    return;
                }
            }
        }

        private void expect(char expected) {
            if (offset >= input.length() || input.charAt(offset) != expected) {
                throw reject(RejectionCode.RECEIPT_MALFORMED_JSON, "expected '" + expected + "'");
            }
            offset++;
        }

        private boolean consume(char expected) {
            if (offset < input.length() && input.charAt(offset) == expected) {
                offset++;
                return true;
            }
            return false;
        }
    }
}
