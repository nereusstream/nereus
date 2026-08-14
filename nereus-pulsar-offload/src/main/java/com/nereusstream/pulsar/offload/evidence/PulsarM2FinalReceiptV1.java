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
import java.util.HashMap;
import java.util.HashSet;
import java.util.List;
import java.util.Map;
import java.util.Objects;
import java.util.Set;
import java.util.regex.Pattern;

/** Closed canonical Pulsar M2 Final receipt with bounded, symlink-safe attachment verification. */
public final class PulsarM2FinalReceiptV1 {
    public static final String SCHEMA = "NEREUS_V2_M2_PULSAR_FINAL_RECEIPT_V1";
    public static final String KAFKA_FINAL_PATH = "docs/v2/evidence/v2-m2/kafka/k10/kafka-final.json";
    public static final String P6_EXECUTION_PATH = "docs/v2/evidence/v2-m2/pulsar/p6/execution.json";
    public static final int MAX_CANONICAL_BYTES = 65_536;
    public static final int MAX_ATTACHMENTS = 16;
    public static final int MAX_SCENARIOS = 11;
    public static final int MAX_SUITES_PER_SCENARIO = 8;
    public static final int MAX_PATH_BYTES = 384;
    public static final int MAX_PATH_SEGMENTS = 20;
    public static final int MAX_SINGLE_ATTACHMENT_BYTES = 2_097_152;
    public static final int MAX_TOTAL_ATTACHMENT_BYTES = 16_777_216;

    private static final Pattern SHA256 = Pattern.compile("[0-9a-f]{64}");
    private static final Pattern COMMIT = Pattern.compile("[0-9a-f]{40}");
    private static final Pattern SCENARIO_ID = Pattern.compile("V2-(?:[A-Z]+-)+[0-9]{3}");
    private static final Pattern SUITE_ID = Pattern.compile("[A-Za-z0-9][A-Za-z0-9._:/-]*");
    private static final Pattern PATH_SEGMENT = Pattern.compile("[A-Za-z0-9][A-Za-z0-9._-]*");

    private PulsarM2FinalReceiptV1() {}

    public enum ReceiptKind {
        PULSAR_M2_FINAL
    }

    public enum ReceiptResult {
        PASS_PULSAR_M2_FINAL
    }

    public enum AttachmentKind {
        KAFKA_FINAL_RECEIPT,
        LOCAL_JUNIT_SUMMARY,
        NATIVE_JUNIT_SUMMARY,
        P6_CANDIDATE_MATRIX,
        P6_EXECUTION_RECEIPT,
        P6_NATIVE_BASELINE,
        P6_PROVIDER_JUNIT_SUMMARY,
        P6_REAL_PROVIDER
    }

    public enum RejectionCode {
        ROOT_NOT_REGULAR,
        ROOT_BYTES_EXCEEDED,
        MALFORMED_OR_NON_CANONICAL,
        SCHEMA_KIND_RESULT_INVALID,
        SOURCE_TUPLE_INVALID,
        SCENARIO_SET_INVALID,
        SUITE_SET_INVALID,
        MANDATORY_RESULT_NOT_PASS,
        ATTACHMENT_SET_INVALID,
        PATH_INVALID,
        ATTACHMENT_BYTES_EXCEEDED,
        ATTACHMENT_TOTAL_BYTES_EXCEEDED,
        ATTACHMENT_NOT_REGULAR,
        ATTACHMENT_LENGTH_MISMATCH,
        ATTACHMENT_DIGEST_MISMATCH,
        PREREQUISITE_BINDING_INVALID,
        CHECKED_ARITHMETIC_OVERFLOW
    }

    public static final class ReceiptRejectedException extends IllegalArgumentException {
        private final RejectionCode code;

        public ReceiptRejectedException(RejectionCode code, String detail) {
            super(code + ": " + detail);
            this.code = Objects.requireNonNull(code, "code");
        }

        public ReceiptRejectedException(RejectionCode code, String detail, Throwable cause) {
            super(code + ": " + detail, cause);
            this.code = Objects.requireNonNull(code, "code");
        }

        public RejectionCode code() {
            return code;
        }
    }

    public record SourceTuple(
            String kafkaFinalReceiptSha256,
            String nereusCommit,
            String p6ExecutionReceiptSha256,
            String pulsarForkCommit,
            String sourceLocksSha256) {}

    public record SuiteResult(
            String attachmentPath, long errors, long failed, long skipped, String suiteId, long tests) {}

    public record ScenarioResult(String scenarioId, List<SuiteResult> suites) {
        public ScenarioResult {
            suites = List.copyOf(suites);
        }
    }

    public record AttachmentRef(AttachmentKind kind, String path, long bytes, String sha256) {}

    public record Receipt(
            List<AttachmentRef> attachments,
            ReceiptKind kind,
            boolean promotionEligible,
            ReceiptResult result,
            List<ScenarioResult> scenarios,
            String schema,
            SourceTuple sourceTuple) {
        public Receipt {
            attachments = List.copyOf(attachments);
            scenarios = List.copyOf(scenarios);
        }
    }

    public static byte[] canonicalBytes(Receipt receipt) {
        validate(receipt);
        StringBuilder output = new StringBuilder(32_768);
        output.append("{\"attachments\":[");
        for (int index = 0; index < receipt.attachments().size(); index++) {
            if (index != 0) {
                output.append(',');
            }
            AttachmentRef row = receipt.attachments().get(index);
            output.append("{\"bytes\":").append(row.bytes()).append(",\"kind\":");
            quote(output, row.kind().name());
            output.append(",\"path\":");
            quote(output, row.path());
            output.append(",\"sha256\":");
            quote(output, row.sha256());
            output.append('}');
        }
        output.append("],\"kind\":");
        quote(output, receipt.kind().name());
        output.append(",\"promotionEligible\":true,\"result\":");
        quote(output, receipt.result().name());
        output.append(",\"scenarios\":[");
        for (int scenarioIndex = 0; scenarioIndex < receipt.scenarios().size(); scenarioIndex++) {
            if (scenarioIndex != 0) {
                output.append(',');
            }
            ScenarioResult scenario = receipt.scenarios().get(scenarioIndex);
            output.append("{\"scenarioId\":");
            quote(output, scenario.scenarioId());
            output.append(",\"suites\":[");
            for (int suiteIndex = 0; suiteIndex < scenario.suites().size(); suiteIndex++) {
                if (suiteIndex != 0) {
                    output.append(',');
                }
                SuiteResult suite = scenario.suites().get(suiteIndex);
                output.append("{\"attachmentPath\":");
                quote(output, suite.attachmentPath());
                output.append(",\"errors\":")
                        .append(suite.errors())
                        .append(",\"failed\":")
                        .append(suite.failed())
                        .append(",\"skipped\":")
                        .append(suite.skipped())
                        .append(",\"suiteId\":");
                quote(output, suite.suiteId());
                output.append(",\"tests\":").append(suite.tests()).append('}');
            }
            output.append("]}");
        }
        output.append("],\"schema\":");
        quote(output, receipt.schema());
        output.append(",\"sourceTuple\":{");
        SourceTuple source = receipt.sourceTuple();
        field(output, "kafkaFinalReceiptSha256", source.kafkaFinalReceiptSha256(), false);
        field(output, "nereusCommit", source.nereusCommit(), true);
        field(output, "p6ExecutionReceiptSha256", source.p6ExecutionReceiptSha256(), true);
        field(output, "pulsarForkCommit", source.pulsarForkCommit(), true);
        field(output, "sourceLocksSha256", source.sourceLocksSha256(), true);
        output.append("}}");
        byte[] bytes = output.toString().getBytes(StandardCharsets.UTF_8);
        if (bytes.length == 0 || bytes.length > MAX_CANONICAL_BYTES) {
            throw reject(RejectionCode.ROOT_BYTES_EXCEEDED, "canonical receipt exceeds cap");
        }
        return bytes;
    }

    public static Receipt parseCanonical(byte[] bytes) {
        Cursor cursor = new Cursor(decode(bytes));
        cursor.expect("{\"attachments\":[");
        List<AttachmentRef> attachments = new ArrayList<>();
        if (!cursor.peek(']')) {
            do {
                if (attachments.size() == MAX_ATTACHMENTS) {
                    throw reject(RejectionCode.ATTACHMENT_SET_INVALID, "attachment count exceeds cap");
                }
                cursor.expect("{\"bytes\":");
                long length = cursor.number();
                cursor.expect(",\"kind\":");
                AttachmentKind kind = enumValue(AttachmentKind.class, cursor.string());
                cursor.expect(",\"path\":");
                String path = cursor.string();
                cursor.expect(",\"sha256\":");
                String sha256 = cursor.string();
                cursor.expect("}");
                attachments.add(new AttachmentRef(kind, path, length, sha256));
            } while (cursor.comma());
        }
        cursor.expect("],\"kind\":");
        ReceiptKind kind = enumValue(ReceiptKind.class, cursor.string());
        cursor.expect(",\"promotionEligible\":true,\"result\":");
        ReceiptResult result = enumValue(ReceiptResult.class, cursor.string());
        cursor.expect(",\"scenarios\":[");
        List<ScenarioResult> scenarios = new ArrayList<>();
        if (!cursor.peek(']')) {
            do {
                if (scenarios.size() == MAX_SCENARIOS) {
                    throw reject(RejectionCode.SCENARIO_SET_INVALID, "scenario count exceeds cap");
                }
                cursor.expect("{\"scenarioId\":");
                String scenarioId = cursor.string();
                cursor.expect(",\"suites\":[");
                List<SuiteResult> suites = new ArrayList<>();
                if (!cursor.peek(']')) {
                    do {
                        if (suites.size() == MAX_SUITES_PER_SCENARIO) {
                            throw reject(RejectionCode.SUITE_SET_INVALID, "suite count exceeds cap");
                        }
                        cursor.expect("{\"attachmentPath\":");
                        String attachmentPath = cursor.string();
                        cursor.expect(",\"errors\":");
                        long errors = cursor.number();
                        cursor.expect(",\"failed\":");
                        long failed = cursor.number();
                        cursor.expect(",\"skipped\":");
                        long skipped = cursor.number();
                        cursor.expect(",\"suiteId\":");
                        String suiteId = cursor.string();
                        cursor.expect(",\"tests\":");
                        long tests = cursor.number();
                        cursor.expect("}");
                        suites.add(new SuiteResult(attachmentPath, errors, failed, skipped, suiteId, tests));
                    } while (cursor.comma());
                }
                cursor.expect("]}");
                scenarios.add(new ScenarioResult(scenarioId, suites));
            } while (cursor.comma());
        }
        cursor.expect("],\"schema\":");
        String schema = cursor.string();
        cursor.expect(",\"sourceTuple\":{\"kafkaFinalReceiptSha256\":");
        String kafkaFinalSha = cursor.string();
        cursor.expect(",\"nereusCommit\":");
        String nereusCommit = cursor.string();
        cursor.expect(",\"p6ExecutionReceiptSha256\":");
        String p6ExecutionSha = cursor.string();
        cursor.expect(",\"pulsarForkCommit\":");
        String pulsarCommit = cursor.string();
        cursor.expect(",\"sourceLocksSha256\":");
        String locksSha = cursor.string();
        cursor.expect("}}");
        cursor.end();
        Receipt receipt = new Receipt(
                attachments,
                kind,
                true,
                result,
                scenarios,
                schema,
                new SourceTuple(kafkaFinalSha, nereusCommit, p6ExecutionSha, pulsarCommit, locksSha));
        validate(receipt);
        if (!Arrays.equals(bytes, canonicalBytes(receipt))) {
            throw reject(RejectionCode.MALFORMED_OR_NON_CANONICAL, "receipt differs from canonical bytes");
        }
        return receipt;
    }

    public static Receipt parseCanonicalFile(Path path) {
        return parseCanonical(readRoot(path));
    }

    public static void validate(Receipt receipt) {
        Objects.requireNonNull(receipt, "receipt");
        if (!SCHEMA.equals(receipt.schema())
                || receipt.kind() != ReceiptKind.PULSAR_M2_FINAL
                || receipt.result() != ReceiptResult.PASS_PULSAR_M2_FINAL
                || !receipt.promotionEligible()) {
            throw reject(RejectionCode.SCHEMA_KIND_RESULT_INVALID, "invalid Pulsar Final identity");
        }
        validateSource(receipt.sourceTuple());
        if (receipt.scenarios().size() != MAX_SCENARIOS) {
            throw reject(RejectionCode.SCENARIO_SET_INVALID, "Pulsar Final must contain eleven scenarios");
        }

        Map<String, AttachmentRef> attachmentByPath = validateAttachments(receipt.attachments());
        String previousScenario = null;
        Set<String> referencedPaths = new HashSet<>();
        for (ScenarioResult scenario : receipt.scenarios()) {
            if (scenario.scenarioId() == null
                    || !SCENARIO_ID.matcher(scenario.scenarioId()).matches()) {
                throw reject(RejectionCode.SCENARIO_SET_INVALID, "invalid scenario ID");
            }
            if (previousScenario != null && previousScenario.compareTo(scenario.scenarioId()) >= 0) {
                throw reject(RejectionCode.SCENARIO_SET_INVALID, "scenario IDs must be sorted and unique");
            }
            previousScenario = scenario.scenarioId();
            if (scenario.suites().isEmpty() || scenario.suites().size() > MAX_SUITES_PER_SCENARIO) {
                throw reject(RejectionCode.SUITE_SET_INVALID, "scenario suite count outside cap");
            }
            String previousSuite = null;
            for (SuiteResult suite : scenario.suites()) {
                validatePath(suite.attachmentPath());
                if (suite.suiteId() == null
                        || !SUITE_ID.matcher(suite.suiteId()).matches()) {
                    throw reject(RejectionCode.SUITE_SET_INVALID, "invalid suite ID");
                }
                if (previousSuite != null && previousSuite.compareTo(suite.suiteId()) >= 0) {
                    throw reject(RejectionCode.SUITE_SET_INVALID, "suite IDs must be sorted and unique");
                }
                previousSuite = suite.suiteId();
                if (suite.tests() <= 0 || suite.failed() != 0 || suite.errors() != 0 || suite.skipped() != 0) {
                    throw reject(
                            RejectionCode.MANDATORY_RESULT_NOT_PASS, "suite is empty, failed, errored, or skipped");
                }
                if (!attachmentByPath.containsKey(suite.attachmentPath())) {
                    throw reject(RejectionCode.ATTACHMENT_SET_INVALID, "suite attachment is absent");
                }
                referencedPaths.add(suite.attachmentPath());
            }
        }

        AttachmentRef kafka =
                requireTypedAttachment(attachmentByPath, KAFKA_FINAL_PATH, AttachmentKind.KAFKA_FINAL_RECEIPT);
        AttachmentRef p6 =
                requireTypedAttachment(attachmentByPath, P6_EXECUTION_PATH, AttachmentKind.P6_EXECUTION_RECEIPT);
        if (!kafka.sha256().equals(receipt.sourceTuple().kafkaFinalReceiptSha256())
                || !p6.sha256().equals(receipt.sourceTuple().p6ExecutionReceiptSha256())) {
            throw reject(RejectionCode.PREREQUISITE_BINDING_INVALID, "Kafka Final or P6 digest differs");
        }
        for (AttachmentRef attachment : receipt.attachments()) {
            if (!attachment.path().equals(KAFKA_FINAL_PATH)
                    && !attachment.path().equals(P6_EXECUTION_PATH)
                    && !referencedPaths.contains(attachment.path())) {
                throw reject(RejectionCode.ATTACHMENT_SET_INVALID, "unreferenced evidence attachment");
            }
        }
    }

    public static void verifyAttachments(Path repositoryRoot, Receipt receipt) {
        validate(receipt);
        Path root = verifiedRoot(repositoryRoot);
        for (AttachmentRef attachment : receipt.attachments()) {
            readVerified(root, attachment);
        }
    }

    private static Map<String, AttachmentRef> validateAttachments(List<AttachmentRef> attachments) {
        if (attachments.size() < 2 || attachments.size() > MAX_ATTACHMENTS) {
            throw reject(RejectionCode.ATTACHMENT_SET_INVALID, "attachment count outside cap");
        }
        Map<String, AttachmentRef> result = new HashMap<>();
        String previous = null;
        long total = 0;
        for (AttachmentRef attachment : attachments) {
            Objects.requireNonNull(attachment.kind(), "attachment kind");
            validatePath(attachment.path());
            if (previous != null && previous.compareTo(attachment.path()) >= 0) {
                throw reject(RejectionCode.ATTACHMENT_SET_INVALID, "attachments must be sorted and unique");
            }
            previous = attachment.path();
            if (attachment.bytes() <= 0 || attachment.bytes() > MAX_SINGLE_ATTACHMENT_BYTES) {
                throw reject(RejectionCode.ATTACHMENT_BYTES_EXCEEDED, "attachment bytes outside cap");
            }
            requireSha(attachment.sha256());
            total = checkedAdd(total, attachment.bytes());
            result.put(attachment.path(), attachment);
        }
        if (total > MAX_TOTAL_ATTACHMENT_BYTES) {
            throw reject(RejectionCode.ATTACHMENT_TOTAL_BYTES_EXCEEDED, "attachment total exceeds cap");
        }
        return result;
    }

    private static AttachmentRef requireTypedAttachment(
            Map<String, AttachmentRef> attachments, String path, AttachmentKind kind) {
        AttachmentRef attachment = attachments.get(path);
        if (attachment == null || attachment.kind() != kind) {
            throw reject(RejectionCode.PREREQUISITE_BINDING_INVALID, "required typed prerequisite is absent");
        }
        return attachment;
    }

    private static void validateSource(SourceTuple source) {
        Objects.requireNonNull(source, "sourceTuple");
        requireSha(source.kafkaFinalReceiptSha256());
        requireCommit(source.nereusCommit());
        requireSha(source.p6ExecutionReceiptSha256());
        requireCommit(source.pulsarForkCommit());
        requireSha(source.sourceLocksSha256());
    }

    private static List<String> validatePath(String path) {
        if (path == null || path.isEmpty() || path.getBytes(StandardCharsets.UTF_8).length > MAX_PATH_BYTES) {
            throw reject(RejectionCode.PATH_INVALID, "path bytes outside cap");
        }
        if (path.startsWith("/") || path.endsWith("/") || path.contains("\\") || path.contains("//")) {
            throw reject(RejectionCode.PATH_INVALID, "path is not repository relative");
        }
        String[] raw = path.split("/", -1);
        if (raw.length == 0 || raw.length > MAX_PATH_SEGMENTS) {
            throw reject(RejectionCode.PATH_INVALID, "path segment count outside cap");
        }
        List<String> segments = new ArrayList<>(raw.length);
        for (String segment : raw) {
            if (segment.equals(".")
                    || segment.equals("..")
                    || !PATH_SEGMENT.matcher(segment).matches()) {
                throw reject(RejectionCode.PATH_INVALID, "invalid path segment");
            }
            segments.add(segment);
        }
        return segments;
    }

    private static Path verifiedRoot(Path repositoryRoot) {
        Objects.requireNonNull(repositoryRoot, "repositoryRoot");
        try {
            if (Files.isSymbolicLink(repositoryRoot)) {
                throw reject(RejectionCode.ATTACHMENT_NOT_REGULAR, "repository root is a symlink");
            }
            Path root = repositoryRoot.toRealPath(LinkOption.NOFOLLOW_LINKS);
            BasicFileAttributes attributes =
                    Files.readAttributes(root, BasicFileAttributes.class, LinkOption.NOFOLLOW_LINKS);
            if (!attributes.isDirectory() || attributes.isSymbolicLink()) {
                throw reject(RejectionCode.ATTACHMENT_NOT_REGULAR, "repository root is not a directory");
            }
            return root;
        } catch (ReceiptRejectedException failure) {
            throw failure;
        } catch (IOException failure) {
            throw new ReceiptRejectedException(
                    RejectionCode.ATTACHMENT_NOT_REGULAR, "cannot resolve repository root", failure);
        }
    }

    private static byte[] readVerified(Path root, AttachmentRef reference) {
        Path current = root;
        for (String segment : validatePath(reference.path())) {
            current = current.resolve(segment);
            BasicFileAttributes attributes = attributes(current);
            if (attributes.isSymbolicLink() || Files.isSymbolicLink(current)) {
                throw reject(RejectionCode.ATTACHMENT_NOT_REGULAR, "attachment path contains a symlink");
            }
        }
        Path normalized = current.normalize();
        if (!normalized.startsWith(root)) {
            throw reject(RejectionCode.PATH_INVALID, "attachment leaves repository root");
        }
        BasicFileAttributes before = attributes(normalized);
        if (!before.isRegularFile() || before.size() != reference.bytes()) {
            throw reject(RejectionCode.ATTACHMENT_LENGTH_MISMATCH, "attachment length differs");
        }
        byte[] bytes = readBounded(normalized, reference.bytes());
        BasicFileAttributes after = attributes(normalized);
        if (!after.isRegularFile()
                || before.size() != after.size()
                || (before.fileKey() != null
                        && after.fileKey() != null
                        && !before.fileKey().equals(after.fileKey()))) {
            throw reject(RejectionCode.ATTACHMENT_NOT_REGULAR, "attachment changed while reading");
        }
        if (!sha256(bytes).equals(reference.sha256())) {
            throw reject(RejectionCode.ATTACHMENT_DIGEST_MISMATCH, "attachment SHA-256 differs");
        }
        return bytes;
    }

    private static BasicFileAttributes attributes(Path path) {
        try {
            return Files.readAttributes(path, BasicFileAttributes.class, LinkOption.NOFOLLOW_LINKS);
        } catch (IOException failure) {
            throw new ReceiptRejectedException(
                    RejectionCode.ATTACHMENT_NOT_REGULAR, "cannot read attachment attributes", failure);
        }
    }

    private static byte[] readBounded(Path path, long expectedBytes) {
        try (InputStream input = java.nio.channels.Channels.newInputStream(Files.newByteChannel(
                        path, Set.<OpenOption>of(StandardOpenOption.READ, LinkOption.NOFOLLOW_LINKS)));
                ByteArrayOutputStream output = new ByteArrayOutputStream((int) expectedBytes)) {
            byte[] buffer = new byte[8192];
            int count;
            while ((count = input.read(buffer)) != -1) {
                if (checkedAdd(output.size(), count) > expectedBytes) {
                    throw reject(RejectionCode.ATTACHMENT_LENGTH_MISMATCH, "streamed attachment grew");
                }
                output.write(buffer, 0, count);
            }
            if (output.size() != expectedBytes) {
                throw reject(RejectionCode.ATTACHMENT_LENGTH_MISMATCH, "streamed attachment shortened");
            }
            return output.toByteArray();
        } catch (ReceiptRejectedException failure) {
            throw failure;
        } catch (IOException failure) {
            throw new ReceiptRejectedException(RejectionCode.ATTACHMENT_NOT_REGULAR, "cannot read attachment", failure);
        }
    }

    private static byte[] readRoot(Path path) {
        Objects.requireNonNull(path, "path");
        BasicFileAttributes before = attributes(path);
        if (before.isSymbolicLink() || Files.isSymbolicLink(path) || !before.isRegularFile()) {
            throw reject(RejectionCode.ROOT_NOT_REGULAR, "receipt root is not a regular file");
        }
        if (before.size() <= 0 || before.size() > MAX_CANONICAL_BYTES) {
            throw reject(RejectionCode.ROOT_BYTES_EXCEEDED, "receipt root bytes outside cap");
        }
        byte[] bytes = readRootBytes(path, before.size());
        BasicFileAttributes after = attributes(path);
        if (!after.isRegularFile()
                || before.size() != after.size()
                || (before.fileKey() != null
                        && after.fileKey() != null
                        && !before.fileKey().equals(after.fileKey()))) {
            throw reject(RejectionCode.ROOT_NOT_REGULAR, "receipt root changed while reading");
        }
        return bytes;
    }

    private static byte[] readRootBytes(Path path, long expectedBytes) {
        try {
            byte[] bytes = Files.readAllBytes(path);
            if (bytes.length != expectedBytes || bytes.length > MAX_CANONICAL_BYTES) {
                throw reject(RejectionCode.ROOT_BYTES_EXCEEDED, "receipt root bytes changed");
            }
            return bytes;
        } catch (ReceiptRejectedException failure) {
            throw failure;
        } catch (IOException failure) {
            throw new ReceiptRejectedException(RejectionCode.ROOT_NOT_REGULAR, "cannot read receipt root", failure);
        }
    }

    private static String decode(byte[] bytes) {
        Objects.requireNonNull(bytes, "bytes");
        if (bytes.length == 0 || bytes.length > MAX_CANONICAL_BYTES) {
            throw reject(RejectionCode.ROOT_BYTES_EXCEEDED, "receipt bytes outside cap");
        }
        if (bytes.length >= 3
                && Byte.toUnsignedInt(bytes[0]) == 0xef
                && Byte.toUnsignedInt(bytes[1]) == 0xbb
                && Byte.toUnsignedInt(bytes[2]) == 0xbf) {
            throw reject(RejectionCode.MALFORMED_OR_NON_CANONICAL, "UTF-8 BOM is forbidden");
        }
        try {
            return StandardCharsets.UTF_8
                    .newDecoder()
                    .onMalformedInput(CodingErrorAction.REPORT)
                    .onUnmappableCharacter(CodingErrorAction.REPORT)
                    .decode(java.nio.ByteBuffer.wrap(bytes))
                    .toString();
        } catch (CharacterCodingException failure) {
            throw new ReceiptRejectedException(
                    RejectionCode.MALFORMED_OR_NON_CANONICAL, "receipt is not strict UTF-8", failure);
        }
    }

    private static long checkedAdd(long left, long right) {
        try {
            return Math.addExact(left, right);
        } catch (ArithmeticException failure) {
            throw new ReceiptRejectedException(
                    RejectionCode.CHECKED_ARITHMETIC_OVERFLOW, "checked length overflow", failure);
        }
    }

    private static String sha256(byte[] bytes) {
        try {
            return java.util.HexFormat.of()
                    .formatHex(MessageDigest.getInstance("SHA-256").digest(bytes));
        } catch (NoSuchAlgorithmException failure) {
            throw new IllegalStateException("JDK lacks SHA-256", failure);
        }
    }

    private static void requireSha(String value) {
        if (value == null || !SHA256.matcher(value).matches()) {
            throw reject(RejectionCode.SOURCE_TUPLE_INVALID, "invalid lowercase SHA-256");
        }
    }

    private static void requireCommit(String value) {
        if (value == null || !COMMIT.matcher(value).matches()) {
            throw reject(RejectionCode.SOURCE_TUPLE_INVALID, "invalid commit");
        }
    }

    private static void field(StringBuilder output, String name, String value, boolean comma) {
        if (comma) {
            output.append(',');
        }
        quote(output, name);
        output.append(':');
        quote(output, value);
    }

    private static void quote(StringBuilder output, String value) {
        output.append('"').append(value).append('"');
    }

    private static <T extends Enum<T>> T enumValue(Class<T> type, String value) {
        try {
            return Enum.valueOf(type, value);
        } catch (IllegalArgumentException failure) {
            throw new ReceiptRejectedException(RejectionCode.MALFORMED_OR_NON_CANONICAL, "unknown enum value", failure);
        }
    }

    private static ReceiptRejectedException reject(RejectionCode code, String detail) {
        return new ReceiptRejectedException(code, detail);
    }

    private static final class Cursor {
        private final String input;
        private int position;

        private Cursor(String input) {
            this.input = input;
        }

        private void expect(String expected) {
            if (!input.startsWith(expected, position)) {
                throw reject(RejectionCode.MALFORMED_OR_NON_CANONICAL, "unexpected canonical token");
            }
            position += expected.length();
        }

        private boolean peek(char expected) {
            return position < input.length() && input.charAt(position) == expected;
        }

        private boolean comma() {
            if (peek(',')) {
                position++;
                return true;
            }
            return false;
        }

        private String string() {
            if (!peek('"')) {
                throw reject(RejectionCode.MALFORMED_OR_NON_CANONICAL, "expected canonical string");
            }
            int start = ++position;
            while (position < input.length() && input.charAt(position) != '"') {
                char value = input.charAt(position++);
                if (value < 0x20 || value > 0x7e || value == '\\') {
                    throw reject(RejectionCode.MALFORMED_OR_NON_CANONICAL, "escaped or non-ASCII string forbidden");
                }
            }
            if (position == input.length()) {
                throw reject(RejectionCode.MALFORMED_OR_NON_CANONICAL, "unterminated string");
            }
            String value = input.substring(start, position);
            position++;
            return value;
        }

        private long number() {
            int start = position;
            if (position >= input.length() || !Character.isDigit(input.charAt(position))) {
                throw reject(RejectionCode.MALFORMED_OR_NON_CANONICAL, "expected canonical integer");
            }
            if (input.charAt(position) == '0') {
                position++;
            } else {
                while (position < input.length() && Character.isDigit(input.charAt(position))) {
                    position++;
                }
            }
            if (position < input.length() && Character.isDigit(input.charAt(position))) {
                throw reject(RejectionCode.MALFORMED_OR_NON_CANONICAL, "integer has a leading zero");
            }
            try {
                return Long.parseLong(input.substring(start, position));
            } catch (NumberFormatException failure) {
                throw new ReceiptRejectedException(
                        RejectionCode.MALFORMED_OR_NON_CANONICAL, "integer outside long domain", failure);
            }
        }

        private void end() {
            if (position != input.length()) {
                throw reject(RejectionCode.MALFORMED_OR_NON_CANONICAL, "trailing receipt bytes");
            }
        }
    }
}
