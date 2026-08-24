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

package com.nereusstream.storage.object.evidence;

import java.io.IOException;
import java.nio.charset.CharacterCodingException;
import java.nio.charset.CodingErrorAction;
import java.nio.charset.StandardCharsets;
import java.nio.file.Files;
import java.nio.file.LinkOption;
import java.nio.file.Path;
import java.nio.file.attribute.BasicFileAttributes;
import java.security.MessageDigest;
import java.security.NoSuchAlgorithmException;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.EnumSet;
import java.util.HashSet;
import java.util.List;
import java.util.Objects;
import java.util.Set;
import java.util.regex.Pattern;

/** Closed-domain RFC-8785/JCS receipt for one complete exact-source V2 M3 Final. */
public final class M3FinalReceiptV1 {
    public static final String SCHEMA = "NEREUS_V2_M3_FINAL_V1";
    public static final int MAX_CANONICAL_BYTES = 131_072;
    public static final int MAX_PATH_BYTES = 384;
    public static final int MAX_PATH_SEGMENTS = 20;
    public static final int MAX_ATTACHMENTS_PER_CHILD = 32;
    public static final long MAX_SAFE_INTEGER = 9_007_199_254_740_991L;
    public static final String EVIDENCE_PREFIX = "docs/v2/evidence/v2-m3/";

    private static final Pattern SHA256 = Pattern.compile("[0-9a-f]{64}");
    private static final Pattern COMMIT = Pattern.compile("[0-9a-f]{40}");
    private static final Pattern SOURCE_ID = Pattern.compile("[A-Z0-9][A-Z0-9_.:-]{0,127}");
    private static final Pattern PATH_SEGMENT = Pattern.compile("[A-Za-z0-9][A-Za-z0-9._-]*");

    private M3FinalReceiptV1() {}

    public enum ReceiptKind {
        V2_M3_FINAL
    }

    public enum ReceiptResult {
        PASS_V2_M3_FINAL
    }

    /** The complete M3 Final child inventory. Declaration order is the canonical receipt order. */
    public enum ChildKind {
        W1_CURRENT_SOURCE_M2_REGRESSION("PASS_CURRENT_SOURCE_M2_REGRESSION_ONLY"),
        AB_NWG1_WIRE("PASS_NWG1_WIRE_ONLY"),
        C_OBJECT_WAL_STATE_TRACE("PASS_OBJECT_WAL_STATE_TRACE_ONLY"),
        D_LOCAL_CAP("PASS_LOCAL_CAP_ONLY"),
        C1_REAL_PROVIDER_KMS("PASS_REAL_PROVIDER_KMS_ONLY"),
        C2_SEGMENTED_PREFIX("PASS_C2_SEGMENTED_PREFIX_EVIDENCE_ONLY"),
        R_CONTROL_RECOVERY("PASS_CONTROL_RECOVERY_ONLY"),
        K_NWKCP1("PASS_NWKCP1_ONLY"),
        U_KAFKA_OBJECT_WAL("PASS_KAFKA_OBJECT_WAL_ONLY"),
        P_PULSAR_OBJECT_WAL("PASS_PULSAR_OBJECT_WAL_ONLY"),
        ALLOCATOR_SELECTION("PASS_ALLOCATOR_SELECTION_ONLY");

        private final String requiredResult;

        ChildKind(String requiredResult) {
            this.requiredResult = requiredResult;
        }

        public String requiredResult() {
            return requiredResult;
        }
    }

    public enum AttachmentKind {
        ALLOCATOR_FAULT_SUMMARY,
        ALLOCATOR_NATIVE_RELATIVE_SUMMARY,
        ALLOCATOR_RAW_VERIFICATION,
        ALLOCATOR_SCALE_100000_SUMMARY,
        ALLOCATOR_SCALE_10000_SUMMARY,
        CURRENT_SOURCE_M2_GATE_RESULT,
        JUNIT_SUMMARY,
        KMS_REAL_RECEIPT,
        LOCAL_CAP_RESULT,
        MUTATION_MANIFEST,
        NATIVE_RESULT,
        NWG1_VECTOR_MANIFEST,
        PROTOCOL_FIXTURE,
        PROVIDER_REAL_RECEIPT,
        RECOVERY_MANIFEST,
        SOURCE_LOCK_SNAPSHOT,
        TRACE_MANIFEST,
        WIRE_ARTIFACT,
        ZSTD_INTEROPERABILITY_FIXTURE
    }

    public enum Exclusion {
        C1_EVIDENCE_SUBSTITUTE,
        M3_FINAL_AGGREGATE,
        M6_PROCESS_ACTIVATION,
        M8_NATIVE_PARITY,
        PRODUCTION_ALLOWLIST,
        REAL_KMS,
        REAL_PROVIDER,
        SCENARIO_PROMOTION
    }

    public enum AllocatorMode {
        RANGE,
        STRICT
    }

    public enum RejectionCode {
        ROOT_NOT_REGULAR,
        ROOT_BYTES_EXCEEDED,
        MALFORMED_OR_NON_CANONICAL,
        SCHEMA_KIND_RESULT_INVALID,
        SOURCE_TUPLE_INVALID,
        CHILD_SET_INVALID,
        CHILD_RESULT_NOT_PASS,
        ATTACHMENT_SET_INVALID,
        EXCLUSION_SET_INVALID,
        PATH_INVALID,
        SCENARIO_SET_INVALID,
        PROVIDER_EVIDENCE_INVALID,
        ALLOCATOR_EVIDENCE_INVALID
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

    public record RootSourceTuple(String nereusCommit, String sourceLocksSha256) {}

    public record ChildSourceTuple(String nereusCommit, String sourceTupleId, String sourceTupleSha256) {}

    public record AttachmentRef(long bytes, AttachmentKind kind, String path, String sha256) {}

    public record ChildReceiptRef(
            List<AttachmentRef> attachments,
            long bytes,
            long errors,
            List<Exclusion> exclusions,
            long failures,
            ChildKind kind,
            String path,
            boolean promotionEligible,
            String result,
            String sha256,
            long skipped,
            ChildSourceTuple sourceTuple,
            long tests) {
        public ChildReceiptRef {
            attachments = List.copyOf(attachments);
            exclusions = List.copyOf(exclusions);
        }
    }

    public record ProviderEvidence(boolean c2PromotionEligible, boolean realKms, boolean realProvider) {}

    public record AllocatorSelection(
            boolean faultEvidence,
            AllocatorMode mode,
            boolean nativeRelativeEvidence,
            boolean scale10000,
            boolean scale100000) {}

    public record Receipt(
            AllocatorSelection allocatorSelection,
            List<ChildReceiptRef> childReceipts,
            List<Exclusion> exclusions,
            ReceiptKind kind,
            boolean promotionEligible,
            ProviderEvidence providerEvidence,
            ReceiptResult result,
            List<String> scenarios,
            String schema,
            RootSourceTuple sourceTuple) {
        public Receipt {
            childReceipts = List.copyOf(childReceipts);
            exclusions = List.copyOf(exclusions);
            scenarios = List.copyOf(scenarios);
        }
    }

    public static List<String> requiredScenarios() {
        return List.of(
                "V2-FABRIC-002",
                "V2-OBJ-001",
                "V2-OBJ-002",
                "V2-OBJ-003",
                "V2-OBJ-004",
                "V2-OBJ-005",
                "V2-OBJ-006",
                "V2-OBJ-007",
                "V2-OBJ-008",
                "V2-OBJ-009",
                "V2-OBJ-010",
                "V2-OBJ-012",
                "V2-OBJ-013",
                "V2-OBJ-016",
                "V2-OBJ-017",
                "V2-OBJ-019",
                "V2-OBJ-021",
                "V2-OBJ-023",
                "V2-OBJ-024",
                "V2-POSITION-012",
                "V2-POSITION-013",
                "V2-POSITION-014",
                "V2-POSITION-015",
                "V2-POSITION-016",
                "V2-POSITION-017",
                "V2-POSITION-018");
    }

    public static byte[] canonicalBytes(Receipt receipt) {
        validate(receipt);
        StringBuilder output = new StringBuilder(65_536);
        AllocatorSelection allocator = receipt.allocatorSelection();
        output.append("{\"allocatorSelection\":{\"faultEvidence\":")
                .append(allocator.faultEvidence())
                .append(",\"mode\":");
        quote(output, allocator.mode().name());
        output.append(",\"nativeRelativeEvidence\":")
                .append(allocator.nativeRelativeEvidence())
                .append(",\"scale10000\":")
                .append(allocator.scale10000())
                .append(",\"scale100000\":")
                .append(allocator.scale100000())
                .append("},\"childReceipts\":[");
        for (int index = 0; index < receipt.childReceipts().size(); index++) {
            if (index != 0) {
                output.append(',');
            }
            writeChild(output, receipt.childReceipts().get(index));
        }
        output.append("],\"exclusions\":[");
        writeExclusions(output, receipt.exclusions());
        output.append("],\"kind\":");
        quote(output, receipt.kind().name());
        output.append(",\"promotionEligible\":")
                .append(receipt.promotionEligible())
                .append(",\"providerEvidence\":{\"c2PromotionEligible\":")
                .append(receipt.providerEvidence().c2PromotionEligible())
                .append(",\"realKms\":")
                .append(receipt.providerEvidence().realKms())
                .append(",\"realProvider\":")
                .append(receipt.providerEvidence().realProvider())
                .append("},\"result\":");
        quote(output, receipt.result().name());
        output.append(",\"scenarios\":[");
        for (int index = 0; index < receipt.scenarios().size(); index++) {
            if (index != 0) {
                output.append(',');
            }
            quote(output, receipt.scenarios().get(index));
        }
        output.append("],\"schema\":");
        quote(output, receipt.schema());
        output.append(",\"sourceTuple\":{\"nereusCommit\":");
        quote(output, receipt.sourceTuple().nereusCommit());
        output.append(",\"sourceLocksSha256\":");
        quote(output, receipt.sourceTuple().sourceLocksSha256());
        output.append("}}");
        byte[] bytes = output.toString().getBytes(StandardCharsets.UTF_8);
        if (bytes.length == 0 || bytes.length > MAX_CANONICAL_BYTES) {
            throw reject(RejectionCode.ROOT_BYTES_EXCEEDED, "canonical receipt exceeds cap");
        }
        return bytes;
    }

    private static void writeChild(StringBuilder output, ChildReceiptRef child) {
        output.append("{\"attachments\":[");
        for (int index = 0; index < child.attachments().size(); index++) {
            if (index != 0) {
                output.append(',');
            }
            AttachmentRef attachment = child.attachments().get(index);
            output.append("{\"bytes\":").append(attachment.bytes()).append(",\"kind\":");
            quote(output, attachment.kind().name());
            output.append(",\"path\":");
            quote(output, attachment.path());
            output.append(",\"sha256\":");
            quote(output, attachment.sha256());
            output.append('}');
        }
        output.append("],\"bytes\":")
                .append(child.bytes())
                .append(",\"errors\":")
                .append(child.errors())
                .append(",\"exclusions\":[");
        writeExclusions(output, child.exclusions());
        output.append("],\"failures\":").append(child.failures()).append(",\"kind\":");
        quote(output, child.kind().name());
        output.append(",\"path\":");
        quote(output, child.path());
        output.append(",\"promotionEligible\":")
                .append(child.promotionEligible())
                .append(",\"result\":");
        quote(output, child.result());
        output.append(",\"sha256\":");
        quote(output, child.sha256());
        output.append(",\"skipped\":").append(child.skipped()).append(",\"sourceTuple\":{\"nereusCommit\":");
        quote(output, child.sourceTuple().nereusCommit());
        output.append(",\"sourceTupleId\":");
        quote(output, child.sourceTuple().sourceTupleId());
        output.append(",\"sourceTupleSha256\":");
        quote(output, child.sourceTuple().sourceTupleSha256());
        output.append("},\"tests\":").append(child.tests()).append('}');
    }

    private static void writeExclusions(StringBuilder output, List<Exclusion> exclusions) {
        for (int index = 0; index < exclusions.size(); index++) {
            if (index != 0) {
                output.append(',');
            }
            quote(output, exclusions.get(index).name());
        }
    }

    public static Receipt parseCanonical(byte[] bytes) {
        Cursor cursor = new Cursor(decode(bytes));
        cursor.expect("{\"allocatorSelection\":{\"faultEvidence\":");
        boolean fault = cursor.bool();
        cursor.expect(",\"mode\":");
        AllocatorMode mode = enumValue(AllocatorMode.class, cursor.string());
        cursor.expect(",\"nativeRelativeEvidence\":");
        boolean nativeRelative = cursor.bool();
        cursor.expect(",\"scale10000\":");
        boolean scale10000 = cursor.bool();
        cursor.expect(",\"scale100000\":");
        boolean scale100000 = cursor.bool();
        cursor.expect("},\"childReceipts\":[");
        List<ChildReceiptRef> children = new ArrayList<>();
        if (!cursor.peek(']')) {
            do {
                if (children.size() == ChildKind.values().length) {
                    throw reject(RejectionCode.CHILD_SET_INVALID, "child count exceeds closed inventory");
                }
                children.add(parseChild(cursor));
            } while (cursor.comma());
        }
        cursor.expect("],\"exclusions\":[");
        List<Exclusion> exclusions = parseExclusions(cursor);
        cursor.expect("],\"kind\":");
        ReceiptKind kind = enumValue(ReceiptKind.class, cursor.string());
        cursor.expect(",\"promotionEligible\":");
        boolean promotion = cursor.bool();
        cursor.expect(",\"providerEvidence\":{\"c2PromotionEligible\":");
        boolean c2 = cursor.bool();
        cursor.expect(",\"realKms\":");
        boolean realKms = cursor.bool();
        cursor.expect(",\"realProvider\":");
        boolean realProvider = cursor.bool();
        cursor.expect("},\"result\":");
        ReceiptResult result = enumValue(ReceiptResult.class, cursor.string());
        cursor.expect(",\"scenarios\":[");
        List<String> scenarios = new ArrayList<>();
        if (!cursor.peek(']')) {
            do {
                if (scenarios.size() == requiredScenarios().size()) {
                    throw reject(RejectionCode.SCENARIO_SET_INVALID, "scenario count exceeds M3 allowlist");
                }
                scenarios.add(cursor.string());
            } while (cursor.comma());
        }
        cursor.expect("],\"schema\":");
        String schema = cursor.string();
        cursor.expect(",\"sourceTuple\":{\"nereusCommit\":");
        String commit = cursor.string();
        cursor.expect(",\"sourceLocksSha256\":");
        String locks = cursor.string();
        cursor.expect("}}");
        cursor.end();
        Receipt receipt = new Receipt(
                new AllocatorSelection(fault, mode, nativeRelative, scale10000, scale100000),
                children,
                exclusions,
                kind,
                promotion,
                new ProviderEvidence(c2, realKms, realProvider),
                result,
                scenarios,
                schema,
                new RootSourceTuple(commit, locks));
        validate(receipt);
        if (!Arrays.equals(bytes, canonicalBytes(receipt))) {
            throw reject(RejectionCode.MALFORMED_OR_NON_CANONICAL, "receipt differs from canonical bytes");
        }
        return receipt;
    }

    private static ChildReceiptRef parseChild(Cursor cursor) {
        cursor.expect("{\"attachments\":[");
        List<AttachmentRef> attachments = new ArrayList<>();
        if (!cursor.peek(']')) {
            do {
                if (attachments.size() == MAX_ATTACHMENTS_PER_CHILD) {
                    throw reject(RejectionCode.ATTACHMENT_SET_INVALID, "child attachment count exceeds cap");
                }
                cursor.expect("{\"bytes\":");
                long bytes = cursor.number();
                cursor.expect(",\"kind\":");
                AttachmentKind kind = enumValue(AttachmentKind.class, cursor.string());
                cursor.expect(",\"path\":");
                String path = cursor.string();
                cursor.expect(",\"sha256\":");
                String sha = cursor.string();
                cursor.expect("}");
                attachments.add(new AttachmentRef(bytes, kind, path, sha));
            } while (cursor.comma());
        }
        cursor.expect("],\"bytes\":");
        long bytes = cursor.number();
        cursor.expect(",\"errors\":");
        long errors = cursor.number();
        cursor.expect(",\"exclusions\":[");
        List<Exclusion> exclusions = parseExclusions(cursor);
        cursor.expect("],\"failures\":");
        long failures = cursor.number();
        cursor.expect(",\"kind\":");
        ChildKind kind = enumValue(ChildKind.class, cursor.string());
        cursor.expect(",\"path\":");
        String path = cursor.string();
        cursor.expect(",\"promotionEligible\":");
        boolean promotion = cursor.bool();
        cursor.expect(",\"result\":");
        String result = cursor.string();
        cursor.expect(",\"sha256\":");
        String sha = cursor.string();
        cursor.expect(",\"skipped\":");
        long skipped = cursor.number();
        cursor.expect(",\"sourceTuple\":{\"nereusCommit\":");
        String commit = cursor.string();
        cursor.expect(",\"sourceTupleId\":");
        String sourceId = cursor.string();
        cursor.expect(",\"sourceTupleSha256\":");
        String sourceSha = cursor.string();
        cursor.expect("},\"tests\":");
        long tests = cursor.number();
        cursor.expect("}");
        return new ChildReceiptRef(
                attachments,
                bytes,
                errors,
                exclusions,
                failures,
                kind,
                path,
                promotion,
                result,
                sha,
                skipped,
                new ChildSourceTuple(commit, sourceId, sourceSha),
                tests);
    }

    private static List<Exclusion> parseExclusions(Cursor cursor) {
        List<Exclusion> result = new ArrayList<>();
        if (!cursor.peek(']')) {
            do {
                if (result.size() == Exclusion.values().length) {
                    throw reject(RejectionCode.EXCLUSION_SET_INVALID, "exclusion count exceeds closed inventory");
                }
                result.add(enumValue(Exclusion.class, cursor.string()));
            } while (cursor.comma());
        }
        return result;
    }

    public static Receipt parseCanonicalFile(Path path) {
        Objects.requireNonNull(path, "path");
        BasicFileAttributes before = attributes(path);
        if (before.isSymbolicLink() || Files.isSymbolicLink(path) || !before.isRegularFile()) {
            throw reject(RejectionCode.ROOT_NOT_REGULAR, "receipt root is not a regular file");
        }
        if (before.size() <= 0 || before.size() > MAX_CANONICAL_BYTES) {
            throw reject(RejectionCode.ROOT_BYTES_EXCEEDED, "receipt root bytes outside cap");
        }
        byte[] bytes;
        try {
            bytes = Files.readAllBytes(path);
        } catch (IOException failure) {
            throw new ReceiptRejectedException(RejectionCode.ROOT_NOT_REGULAR, "cannot read receipt root", failure);
        }
        BasicFileAttributes after = attributes(path);
        if (!after.isRegularFile()
                || bytes.length != before.size()
                || before.size() != after.size()
                || (before.fileKey() != null
                        && after.fileKey() != null
                        && !before.fileKey().equals(after.fileKey()))) {
            throw reject(RejectionCode.ROOT_NOT_REGULAR, "receipt root changed while reading");
        }
        return parseCanonical(bytes);
    }

    /** Validates the closed child schema and its exact projection into the Final row. */
    public static void validateChildReceiptCanonical(
            byte[] bytes, ChildReceiptRef expected, RootSourceTuple finalSource) {
        Objects.requireNonNull(expected, "expected");
        Objects.requireNonNull(finalSource, "finalSource");
        Cursor cursor = new Cursor(decode(bytes));
        if (expected.kind() == ChildKind.W1_CURRENT_SOURCE_M2_REGRESSION) {
            validateW1Child(cursor, expected, finalSource);
        } else {
            validateGenericChild(cursor, expected, finalSource);
        }
        cursor.end();
    }

    private static void validateGenericChild(Cursor cursor, ChildReceiptRef expected, RootSourceTuple finalSource) {
        cursor.expect("{\"attachments\":[");
        List<AttachmentRef> attachments = new ArrayList<>();
        if (!cursor.peek(']')) {
            do {
                if (attachments.size() == MAX_ATTACHMENTS_PER_CHILD) {
                    throw reject(RejectionCode.ATTACHMENT_SET_INVALID, "generic child attachment count exceeds cap");
                }
                cursor.expect("{\"bytes\":");
                long bytes = cursor.number();
                cursor.expect(",\"kind\":");
                AttachmentKind kind = enumValue(AttachmentKind.class, cursor.string());
                cursor.expect(",\"path\":");
                String path = cursor.string();
                cursor.expect(",\"sha256\":");
                String sha256 = cursor.string();
                cursor.expect("}");
                attachments.add(new AttachmentRef(bytes, kind, path, sha256));
            } while (cursor.comma());
        }
        cursor.expect("],\"exclusions\":[");
        List<Exclusion> exclusions = parseExclusions(cursor);
        cursor.expect("],\"kind\":");
        ChildKind kind = enumValue(ChildKind.class, cursor.string());
        cursor.expect(",\"promotionEligible\":");
        boolean promotionEligible = cursor.bool();
        cursor.expect(",\"result\":");
        String result = cursor.string();
        cursor.expect(",\"schema\":");
        String schema = cursor.string();
        cursor.expect(",\"sourceTuple\":{\"nereusCommit\":");
        String commit = cursor.string();
        cursor.expect(",\"sourceLocksSha256\":");
        String sourceLocksSha256 = cursor.string();
        cursor.expect("},\"testSummary\":{\"errors\":");
        long errors = cursor.number();
        cursor.expect(",\"failures\":");
        long failures = cursor.number();
        cursor.expect(",\"skipped\":");
        long skipped = cursor.number();
        cursor.expect(",\"tests\":");
        long tests = cursor.number();
        cursor.expect("}}");
        String sourceTuple =
                "{\"nereusCommit\":\"" + commit + "\",\"sourceLocksSha256\":\"" + sourceLocksSha256 + "\"}";
        if (!attachments.equals(expected.attachments())
                || !exclusions.equals(expected.exclusions())
                || kind != expected.kind()
                || promotionEligible
                || !result.equals(expected.result())
                || !"NEREUS_V2_M3_CHILD_EVIDENCE_V1".equals(schema)
                || !commit.equals(finalSource.nereusCommit())
                || !sourceLocksSha256.equals(finalSource.sourceLocksSha256())
                || !sha256(sourceTuple.getBytes(StandardCharsets.UTF_8))
                        .equals(expected.sourceTuple().sourceTupleSha256())
                || !"SOURCE_TUPLE".equals(expected.sourceTuple().sourceTupleId())
                || tests != expected.tests()
                || failures != expected.failures()
                || errors != expected.errors()
                || skipped != expected.skipped()) {
            throw reject(RejectionCode.CHILD_SET_INVALID, "generic child receipt is not the exact Final projection");
        }
    }

    private static void validateW1Child(Cursor cursor, ChildReceiptRef expected, RootSourceTuple finalSource) {
        cursor.expect("{\"childGates\":[");
        Set<String> expectedAttachments = new HashSet<>();
        for (AttachmentRef attachment : expected.attachments()) {
            expectedAttachments.add(attachment.path() + ":" + attachment.bytes() + ":" + attachment.sha256());
        }
        Set<String> observedAttachments = new HashSet<>();
        long tests = 0;
        int gates = 0;
        if (!cursor.peek(']')) {
            do {
                if (gates == requiredM2Gates().size()) {
                    throw reject(RejectionCode.CHILD_SET_INVALID, "W1 child gate count exceeds exact inventory");
                }
                cursor.expect("{\"attachment\":{\"bytes\":");
                long bytes = cursor.number();
                cursor.expect(",\"path\":");
                String path = cursor.string();
                cursor.expect(",\"sha256\":");
                String sha256 = cursor.string();
                cursor.expect("},\"errors\":0,\"failures\":0,\"gateId\":");
                String gate = cursor.string();
                cursor.expect(",\"result\":\"PASS\",\"skipped\":0,\"tests\":");
                long gateTests = cursor.number();
                cursor.expect("}");
                String expectedPath = "docs/v2/evidence/v2-m3/w1/m2-regression/attachments/" + gate + ".json";
                if (!gate.equals(requiredM2Gates().get(gates)) || !path.equals(expectedPath) || gateTests <= 0) {
                    throw reject(RejectionCode.CHILD_SET_INVALID, "W1 child gate path/tests differ");
                }
                observedAttachments.add(path + ":" + bytes + ":" + sha256);
                tests = Math.addExact(tests, gateTests);
                gates++;
            } while (cursor.comma());
        }
        cursor.expect("],\"evidenceClass\":\"TRUSTED_FULL_CURRENT_SOURCE_M2\",\"exclusions\":["
                + "\"M3_IMPLEMENTATION_AND_FINAL\",\"M6_PROCESS_ACTIVATION\","
                + "\"M8_NATIVE_PARITY\",\"SCENARIO_PROMOTION\"],\"historicalFinal\":{"
                + "\"bytes\":1927,\"path\":\"docs/v2/evidence/v2-m2/final/m2-final.json\","
                + "\"publishedNereusCommit\":\"0349fd68e04d94085d9c722c7ebc448cbb810d72\","
                + "\"sha256\":\"2ba2d1cab0547c456ec7e492edaf9b953e9e0d71707770d3c4b4fe8a4d6217dd\","
                + "\"testedNereusCommit\":\"4af3278234d84df7a2fdce4fc6b3e4e227916d56\"},"
                + "\"kind\":\"CURRENT_SOURCE_M2_REGRESSION\",\"m2AmendmentLineage\":[],"
                + "\"promotionEligible\":false,\"result\":\"PASS_CURRENT_SOURCE_M2_REGRESSION_ONLY\","
                + "\"scenarioPromotion\":false,\"schema\":\"NEREUS_V2_M3_CURRENT_SOURCE_M2_REGRESSION_V1\","
                + "\"sources\":");
        String sources = cursor.canonicalValue();
        cursor.expect(",\"testedNereusCommit\":");
        String tested = cursor.string();
        cursor.expect("}");
        if (gates != requiredM2Gates().size()
                || tests != expected.tests()
                || !expectedAttachments.equals(observedAttachments)
                || !tested.equals(finalSource.nereusCommit())
                || !"SOURCES".equals(expected.sourceTuple().sourceTupleId())
                || !sha256(sources.getBytes(StandardCharsets.UTF_8))
                        .equals(expected.sourceTuple().sourceTupleSha256())) {
            throw reject(RejectionCode.CHILD_SET_INVALID, "W1 receipt is not the exact Final projection");
        }
    }

    private static List<String> requiredM2Gates() {
        return List.of(
                "KAFKA_K0",
                "KAFKA_K1",
                "KAFKA_K2",
                "KAFKA_K3",
                "KAFKA_K4",
                "KAFKA_K5",
                "KAFKA_K6",
                "KAFKA_K7",
                "KAFKA_K8",
                "KAFKA_K9",
                "KAFKA_K10",
                "KAFKA_EXACT",
                "KAFKA_REAL_BOOKKEEPER",
                "KAFKA_SCALE_10000",
                "KAFKA_SCALE_100000",
                "PULSAR_P0",
                "PULSAR_P1",
                "PULSAR_P2",
                "PULSAR_P3",
                "PULSAR_P4",
                "PULSAR_P5",
                "PULSAR_P6",
                "PULSAR_NATIVE",
                "PULSAR_P6_PROVIDER",
                "PULSAR_FINAL_PARSER_POLICY");
    }

    private static String sha256(byte[] bytes) {
        try {
            return java.util.HexFormat.of()
                    .formatHex(MessageDigest.getInstance("SHA-256").digest(bytes));
        } catch (NoSuchAlgorithmException failure) {
            throw new IllegalStateException("JDK lacks SHA-256", failure);
        }
    }

    public static void validate(Receipt receipt) {
        Objects.requireNonNull(receipt, "receipt");
        if (!SCHEMA.equals(receipt.schema())
                || receipt.kind() != ReceiptKind.V2_M3_FINAL
                || receipt.result() != ReceiptResult.PASS_V2_M3_FINAL
                || !receipt.promotionEligible()) {
            throw reject(RejectionCode.SCHEMA_KIND_RESULT_INVALID, "invalid M3 Final identity");
        }
        validateRootSource(receipt.sourceTuple());
        if (!receipt.scenarios().equals(requiredScenarios())) {
            throw reject(RejectionCode.SCENARIO_SET_INVALID, "scenario set differs from exact M3 allowlist");
        }
        if (!receipt.exclusions().equals(List.of(Exclusion.M6_PROCESS_ACTIVATION, Exclusion.M8_NATIVE_PARITY))) {
            throw reject(RejectionCode.EXCLUSION_SET_INVALID, "Final must retain exact M6 and M8 exclusions");
        }
        ProviderEvidence provider = Objects.requireNonNull(receipt.providerEvidence(), "providerEvidence");
        if (provider.c2PromotionEligible() || !provider.realKms() || !provider.realProvider()) {
            throw reject(
                    RejectionCode.PROVIDER_EVIDENCE_INVALID,
                    "Final requires real Provider/KMS evidence and keeps C2 non-promotable");
        }
        AllocatorSelection allocator = Objects.requireNonNull(receipt.allocatorSelection(), "allocatorSelection");
        if (allocator.mode() == null
                || !allocator.faultEvidence()
                || !allocator.nativeRelativeEvidence()
                || !allocator.scale10000()
                || !allocator.scale100000()) {
            throw reject(
                    RejectionCode.ALLOCATOR_EVIDENCE_INVALID,
                    "one allocator mode requires fault, native-relative, 10k, and 100k evidence");
        }
        validateChildren(receipt.childReceipts(), receipt.sourceTuple().nereusCommit());
    }

    private static void validateChildren(List<ChildReceiptRef> children, String finalCommit) {
        if (children.size() != ChildKind.values().length) {
            throw reject(RejectionCode.CHILD_SET_INVALID, "Final requires the complete child inventory");
        }
        Set<String> allPaths = new HashSet<>();
        for (int index = 0; index < children.size(); index++) {
            ChildReceiptRef child = Objects.requireNonNull(children.get(index), "child");
            ChildKind expectedKind = ChildKind.values()[index];
            if (child.kind() != expectedKind || !expectedKind.requiredResult().equals(child.result())) {
                throw reject(RejectionCode.CHILD_SET_INVALID, "child order, kind, or result differs");
            }
            if (child.promotionEligible()
                    || child.tests() <= 0
                    || child.tests() > MAX_SAFE_INTEGER
                    || child.failures() != 0
                    || child.errors() != 0
                    || child.skipped() != 0) {
                throw reject(
                        RejectionCode.CHILD_RESULT_NOT_PASS, "child is promotable, empty, failed, errored, or skipped");
            }
            if (child.bytes() <= 0 || child.bytes() > MAX_SAFE_INTEGER) {
                throw reject(RejectionCode.CHILD_SET_INVALID, "child receipt length is outside safe domain");
            }
            validatePath(child.path());
            requireSha(child.sha256(), RejectionCode.CHILD_SET_INVALID);
            if (!allPaths.add(child.path())) {
                throw reject(RejectionCode.CHILD_SET_INVALID, "child receipt path is duplicated");
            }
            validateChildSource(child.sourceTuple(), finalCommit);
            validateChildExclusions(child);
            validateAttachments(child, allPaths);
        }
        requireAttachmentKinds(children.get(0), EnumSet.of(AttachmentKind.CURRENT_SOURCE_M2_GATE_RESULT));
        requireAttachmentKinds(
                children.get(1),
                EnumSet.of(
                        AttachmentKind.MUTATION_MANIFEST,
                        AttachmentKind.NWG1_VECTOR_MANIFEST,
                        AttachmentKind.WIRE_ARTIFACT,
                        AttachmentKind.ZSTD_INTEROPERABILITY_FIXTURE));
        requireAttachmentKinds(children.get(2), EnumSet.of(AttachmentKind.TRACE_MANIFEST));
        requireAttachmentKinds(children.get(3), EnumSet.of(AttachmentKind.LOCAL_CAP_RESULT));
        requireAttachmentKinds(
                children.get(4), EnumSet.of(AttachmentKind.KMS_REAL_RECEIPT, AttachmentKind.PROVIDER_REAL_RECEIPT));
        requireAttachmentKinds(children.get(6), EnumSet.of(AttachmentKind.RECOVERY_MANIFEST));
        requireAttachmentKinds(children.get(7), EnumSet.of(AttachmentKind.PROTOCOL_FIXTURE));
        requireAttachmentKinds(children.get(8), EnumSet.of(AttachmentKind.NATIVE_RESULT));
        requireAttachmentKinds(children.get(9), EnumSet.of(AttachmentKind.NATIVE_RESULT));
        requireAttachmentKinds(
                children.get(10),
                EnumSet.of(
                        AttachmentKind.ALLOCATOR_FAULT_SUMMARY,
                        AttachmentKind.ALLOCATOR_NATIVE_RELATIVE_SUMMARY,
                        AttachmentKind.ALLOCATOR_RAW_VERIFICATION,
                        AttachmentKind.ALLOCATOR_SCALE_10000_SUMMARY,
                        AttachmentKind.ALLOCATOR_SCALE_100000_SUMMARY));
    }

    private static void validateChildExclusions(ChildReceiptRef child) {
        validateSortedUniqueExclusions(child.exclusions());
        if (!child.exclusions().contains(Exclusion.M3_FINAL_AGGREGATE)
                || !child.exclusions().contains(Exclusion.SCENARIO_PROMOTION)) {
            throw reject(
                    RejectionCode.EXCLUSION_SET_INVALID,
                    "every focused child must exclude aggregate and scenario promotion");
        }
        if (child.kind() == ChildKind.D_LOCAL_CAP
                && (!child.exclusions().contains(Exclusion.REAL_PROVIDER)
                        || !child.exclusions().contains(Exclusion.REAL_KMS))) {
            throw reject(RejectionCode.EXCLUSION_SET_INVALID, "D local must exclude real Provider and KMS claims");
        }
        if (child.kind() == ChildKind.C2_SEGMENTED_PREFIX
                && (!child.exclusions().contains(Exclusion.C1_EVIDENCE_SUBSTITUTE)
                        || !child.exclusions().contains(Exclusion.PRODUCTION_ALLOWLIST))) {
            throw reject(
                    RejectionCode.EXCLUSION_SET_INVALID,
                    "C2 must exclude C1 substitution and production allowlist admission");
        }
    }

    private static void validateAttachments(ChildReceiptRef child, Set<String> allPaths) {
        if (child.attachments().isEmpty() || child.attachments().size() > MAX_ATTACHMENTS_PER_CHILD) {
            throw reject(RejectionCode.ATTACHMENT_SET_INVALID, "child attachment count outside cap");
        }
        String previous = null;
        for (AttachmentRef attachment : child.attachments()) {
            Objects.requireNonNull(attachment, "attachment");
            Objects.requireNonNull(attachment.kind(), "attachment.kind");
            validatePath(attachment.path());
            if (previous != null && previous.compareTo(attachment.path()) >= 0) {
                throw reject(RejectionCode.ATTACHMENT_SET_INVALID, "attachments must be sorted and unique");
            }
            previous = attachment.path();
            if (!allPaths.add(attachment.path())) {
                throw reject(RejectionCode.ATTACHMENT_SET_INVALID, "evidence path is duplicated");
            }
            if (attachment.bytes() <= 0 || attachment.bytes() > MAX_SAFE_INTEGER) {
                throw reject(RejectionCode.ATTACHMENT_SET_INVALID, "attachment length outside safe domain");
            }
            requireSha(attachment.sha256(), RejectionCode.ATTACHMENT_SET_INVALID);
        }
    }

    private static void requireAttachmentKinds(ChildReceiptRef child, Set<AttachmentKind> required) {
        Set<AttachmentKind> actual = EnumSet.noneOf(AttachmentKind.class);
        for (AttachmentRef attachment : child.attachments()) {
            actual.add(attachment.kind());
        }
        if (!actual.containsAll(required)) {
            throw reject(RejectionCode.ATTACHMENT_SET_INVALID, "mandatory typed child attachment is absent");
        }
    }

    private static void validateRootSource(RootSourceTuple source) {
        Objects.requireNonNull(source, "sourceTuple");
        requireCommit(source.nereusCommit());
        requireSha(source.sourceLocksSha256(), RejectionCode.SOURCE_TUPLE_INVALID);
    }

    private static void validateChildSource(ChildSourceTuple source, String finalCommit) {
        Objects.requireNonNull(source, "child.sourceTuple");
        requireCommit(source.nereusCommit());
        if (!finalCommit.equals(source.nereusCommit())) {
            throw reject(RejectionCode.SOURCE_TUPLE_INVALID, "child tested Nereus source differs from Final source");
        }
        if (source.sourceTupleId() == null
                || !SOURCE_ID.matcher(source.sourceTupleId()).matches()) {
            throw reject(RejectionCode.SOURCE_TUPLE_INVALID, "child source tuple ID is invalid");
        }
        requireSha(source.sourceTupleSha256(), RejectionCode.SOURCE_TUPLE_INVALID);
    }

    private static void validateSortedUniqueExclusions(List<Exclusion> exclusions) {
        if (exclusions.isEmpty()) {
            throw reject(RejectionCode.EXCLUSION_SET_INVALID, "child exclusions are empty");
        }
        String previous = null;
        for (Exclusion exclusion : exclusions) {
            Objects.requireNonNull(exclusion, "exclusion");
            if (previous != null && previous.compareTo(exclusion.name()) >= 0) {
                throw reject(RejectionCode.EXCLUSION_SET_INVALID, "exclusions must be sorted and unique");
            }
            previous = exclusion.name();
        }
    }

    public static List<String> validatePath(String path) {
        if (path == null
                || path.isEmpty()
                || path.getBytes(StandardCharsets.UTF_8).length > MAX_PATH_BYTES
                || !path.startsWith(EVIDENCE_PREFIX)
                || path.endsWith("/")
                || path.contains("\\")
                || path.contains("//")) {
            throw reject(RejectionCode.PATH_INVALID, "path is outside the M3 evidence prefix or malformed");
        }
        String[] raw = path.split("/", -1);
        if (raw.length == 0 || raw.length > MAX_PATH_SEGMENTS) {
            throw reject(RejectionCode.PATH_INVALID, "path segment count outside cap");
        }
        List<String> result = new ArrayList<>(raw.length);
        for (String segment : raw) {
            if (segment.equals(".")
                    || segment.equals("..")
                    || !PATH_SEGMENT.matcher(segment).matches()) {
                throw reject(RejectionCode.PATH_INVALID, "invalid path segment");
            }
            result.add(segment);
        }
        return result;
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

    private static BasicFileAttributes attributes(Path path) {
        try {
            return Files.readAttributes(path, BasicFileAttributes.class, LinkOption.NOFOLLOW_LINKS);
        } catch (IOException failure) {
            throw new ReceiptRejectedException(RejectionCode.ROOT_NOT_REGULAR, "cannot read file attributes", failure);
        }
    }

    private static void requireCommit(String value) {
        if (value == null || !COMMIT.matcher(value).matches()) {
            throw reject(RejectionCode.SOURCE_TUPLE_INVALID, "invalid lowercase commit");
        }
    }

    private static void requireSha(String value, RejectionCode code) {
        if (value == null || !SHA256.matcher(value).matches()) {
            throw reject(code, "invalid lowercase SHA-256");
        }
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
                throw reject(RejectionCode.MALFORMED_OR_NON_CANONICAL, "unexpected JSON token");
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

        private boolean bool() {
            if (input.startsWith("true", position)) {
                position += 4;
                return true;
            }
            if (input.startsWith("false", position)) {
                position += 5;
                return false;
            }
            throw reject(RejectionCode.MALFORMED_OR_NON_CANONICAL, "boolean expected");
        }

        private long number() {
            int start = position;
            if (peek('0')) {
                position++;
                if (position < input.length() && Character.isDigit(input.charAt(position))) {
                    throw reject(RejectionCode.MALFORMED_OR_NON_CANONICAL, "leading zero is forbidden");
                }
            } else {
                while (position < input.length() && Character.isDigit(input.charAt(position))) {
                    position++;
                }
            }
            if (start == position) {
                throw reject(RejectionCode.MALFORMED_OR_NON_CANONICAL, "non-negative integer expected");
            }
            try {
                long value = Long.parseLong(input.substring(start, position));
                if (value > MAX_SAFE_INTEGER) {
                    throw reject(RejectionCode.MALFORMED_OR_NON_CANONICAL, "integer exceeds JCS safe domain");
                }
                return value;
            } catch (NumberFormatException failure) {
                throw new ReceiptRejectedException(
                        RejectionCode.MALFORMED_OR_NON_CANONICAL, "integer is invalid", failure);
            }
        }

        private String string() {
            expect("\"");
            int start = position;
            while (position < input.length() && input.charAt(position) != '"') {
                char value = input.charAt(position);
                if (value < 0x20 || value > 0x7e || value == '\\') {
                    throw reject(
                            RejectionCode.MALFORMED_OR_NON_CANONICAL,
                            "string is outside the closed canonical ASCII domain");
                }
                position++;
            }
            if (position == input.length()) {
                throw reject(RejectionCode.MALFORMED_OR_NON_CANONICAL, "unterminated string");
            }
            String value = input.substring(start, position);
            position++;
            return value;
        }

        private String canonicalValue() {
            if (position >= input.length() || (input.charAt(position) != '{' && input.charAt(position) != '[')) {
                throw reject(RejectionCode.MALFORMED_OR_NON_CANONICAL, "canonical object/array value expected");
            }
            int start = position;
            int objectDepth = 0;
            int arrayDepth = 0;
            boolean quoted = false;
            boolean escaped = false;
            while (position < input.length()) {
                char value = input.charAt(position++);
                if (quoted) {
                    if (escaped) {
                        escaped = false;
                    } else if (value == '\\') {
                        escaped = true;
                    } else if (value == '"') {
                        quoted = false;
                    }
                    continue;
                }
                if (value == '"') {
                    quoted = true;
                } else if (value == '{') {
                    objectDepth++;
                } else if (value == '}') {
                    objectDepth--;
                } else if (value == '[') {
                    arrayDepth++;
                } else if (value == ']') {
                    arrayDepth--;
                }
                if (objectDepth < 0 || arrayDepth < 0) {
                    throw reject(RejectionCode.MALFORMED_OR_NON_CANONICAL, "unbalanced canonical value");
                }
                if (objectDepth == 0 && arrayDepth == 0) {
                    return input.substring(start, position);
                }
            }
            throw reject(RejectionCode.MALFORMED_OR_NON_CANONICAL, "unterminated canonical value");
        }

        private void end() {
            if (position != input.length()) {
                throw reject(RejectionCode.MALFORMED_OR_NON_CANONICAL, "trailing JSON bytes");
            }
        }
    }
}
