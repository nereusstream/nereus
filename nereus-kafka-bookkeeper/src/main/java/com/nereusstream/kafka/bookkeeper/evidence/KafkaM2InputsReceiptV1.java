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

package com.nereusstream.kafka.bookkeeper.evidence;

import java.io.IOException;
import java.nio.charset.CharacterCodingException;
import java.nio.charset.CodingErrorAction;
import java.nio.charset.StandardCharsets;
import java.nio.file.Files;
import java.nio.file.LinkOption;
import java.nio.file.Path;
import java.nio.file.attribute.BasicFileAttributes;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.List;
import java.util.Objects;
import java.util.regex.Pattern;

/** Closed canonical non-promotable receipt for the five Kafka M2 K0 input gates. */
public final class KafkaM2InputsReceiptV1 {
    public static final String SCHEMA = "NEREUS_V2_M2_KAFKA_INPUTS_RECEIPT_V1";
    public static final int MAX_CANONICAL_BYTES = 16_384;

    private static final Pattern SHA256 = Pattern.compile("[0-9a-f]{64}");
    private static final Pattern IMAGE_DIGEST = Pattern.compile("sha256:[0-9a-f]{64}");
    private static final Pattern COMMIT = Pattern.compile("[0-9a-f]{40}");

    private KafkaM2InputsReceiptV1() {}

    public enum ReceiptKind {
        KAFKA_M2_INPUTS_ONLY
    }

    public enum ReceiptResult {
        PASS_KAFKA_M2_INPUTS_ONLY
    }

    public enum GateId {
        K0_E,
        K0_M,
        K0_N,
        K0_P,
        K0_W
    }

    public enum RejectionCode {
        MALFORMED_OR_NON_CANONICAL,
        ROOT_NOT_REGULAR,
        ROOT_BYTES_EXCEEDED,
        SCHEMA_KIND_RESULT_INVALID,
        PROMOTION_FORBIDDEN,
        SOURCE_TUPLE_INVALID,
        GATE_SET_INVALID,
        GATE_RESULT_NOT_PASS
    }

    public static final class ReceiptRejectedException extends IllegalArgumentException {
        private final RejectionCode code;

        public ReceiptRejectedException(RejectionCode code, String message) {
            super(code + ": " + message);
            this.code = Objects.requireNonNull(code, "code");
        }

        public ReceiptRejectedException(RejectionCode code, String message, Throwable cause) {
            super(code + ": " + message, cause);
            this.code = Objects.requireNonNull(code, "code");
        }

        public RejectionCode code() {
            return code;
        }
    }

    public record GateResult(GateId gateId, long suites, long tests, long failed, long errors, long skipped) {}

    public record SourceTuple(
            String bookKeeperCapabilitySha256,
            String bookKeeperClientJarSha256,
            String bookKeeperClientPomSha256,
            String bookKeeperImageConfigDigest,
            String bookKeeperImageManifestDigest,
            String bookKeeperSourceCommit,
            String bookKeeperTagObject,
            String k0ModuleManifestSha256,
            String k0ModuleReceiptSha256,
            String kafkaBaseCommit,
            String kafkaForkCommit,
            String m1FinalIndexSha256,
            String m1SourceTupleSha256,
            String n1ManifestSha256,
            String n1SourceCommit,
            String nbke2GoldensSha256,
            String nbke2ProjectionSha256,
            String nereusCommit,
            String numericProjectionSha256,
            String sourceLocksSha256) {}

    public record Receipt(
            String schema,
            ReceiptKind kind,
            boolean promotionEligible,
            ReceiptResult result,
            SourceTuple sourceTuple,
            List<GateResult> childGates) {
        public Receipt {
            childGates = List.copyOf(childGates);
        }
    }

    public static Receipt parseCanonical(byte[] bytes) {
        Objects.requireNonNull(bytes, "bytes");
        if (bytes.length == 0 || bytes.length > MAX_CANONICAL_BYTES) {
            throw reject(RejectionCode.ROOT_BYTES_EXCEEDED, "receipt bytes are outside the cap");
        }
        String json;
        try {
            json = StandardCharsets.UTF_8
                    .newDecoder()
                    .onMalformedInput(CodingErrorAction.REPORT)
                    .onUnmappableCharacter(CodingErrorAction.REPORT)
                    .decode(java.nio.ByteBuffer.wrap(bytes))
                    .toString();
        } catch (CharacterCodingException failure) {
            throw new ReceiptRejectedException(
                    RejectionCode.MALFORMED_OR_NON_CANONICAL, "receipt is not strict UTF-8", failure);
        }

        Cursor cursor = new Cursor(json);
        cursor.expect("{\"childGates\":[");
        List<GateResult> gates = new ArrayList<>();
        if (!cursor.peek(']')) {
            do {
                if (gates.size() == GateId.values().length) {
                    throw reject(RejectionCode.GATE_SET_INVALID, "too many child gates");
                }
                cursor.expect("{\"errors\":");
                long errors = cursor.number();
                cursor.expect(",\"failed\":");
                long failed = cursor.number();
                cursor.expect(",\"gateId\":");
                GateId gateId = enumValue(GateId.class, cursor.string());
                cursor.expect(",\"skipped\":");
                long skipped = cursor.number();
                cursor.expect(",\"suites\":");
                long suites = cursor.number();
                cursor.expect(",\"tests\":");
                long tests = cursor.number();
                cursor.expect("}");
                gates.add(new GateResult(gateId, suites, tests, failed, errors, skipped));
            } while (cursor.comma());
        }
        cursor.expect("],\"kind\":");
        ReceiptKind kind = enumValue(ReceiptKind.class, cursor.string());
        cursor.expect(",\"promotionEligible\":false,\"result\":");
        ReceiptResult result = enumValue(ReceiptResult.class, cursor.string());
        cursor.expect(",\"schema\":");
        String schema = cursor.string();
        cursor.expect(",\"sourceTuple\":{");
        cursor.expect("\"bookKeeperCapabilitySha256\":");
        String capabilitySha = cursor.string();
        cursor.expect(",\"bookKeeperClientJarSha256\":");
        String clientJarSha = cursor.string();
        cursor.expect(",\"bookKeeperClientPomSha256\":");
        String clientPomSha = cursor.string();
        cursor.expect(",\"bookKeeperImageConfigDigest\":");
        String imageConfigDigest = cursor.string();
        cursor.expect(",\"bookKeeperImageManifestDigest\":");
        String imageManifestDigest = cursor.string();
        cursor.expect(",\"bookKeeperSourceCommit\":");
        String bookKeeperCommit = cursor.string();
        cursor.expect(",\"bookKeeperTagObject\":");
        String bookKeeperTagObject = cursor.string();
        cursor.expect(",\"k0ModuleManifestSha256\":");
        String moduleManifestSha = cursor.string();
        cursor.expect(",\"k0ModuleReceiptSha256\":");
        String moduleReceiptSha = cursor.string();
        cursor.expect(",\"kafkaBaseCommit\":");
        String kafkaBaseCommit = cursor.string();
        cursor.expect(",\"kafkaForkCommit\":");
        String kafkaForkCommit = cursor.string();
        cursor.expect(",\"m1FinalIndexSha256\":");
        String m1FinalSha = cursor.string();
        cursor.expect(",\"m1SourceTupleSha256\":");
        String m1TupleSha = cursor.string();
        cursor.expect(",\"n1ManifestSha256\":");
        String n1ManifestSha = cursor.string();
        cursor.expect(",\"n1SourceCommit\":");
        String n1Commit = cursor.string();
        cursor.expect(",\"nbke2GoldensSha256\":");
        String goldensSha = cursor.string();
        cursor.expect(",\"nbke2ProjectionSha256\":");
        String nbke2ProjectionSha = cursor.string();
        cursor.expect(",\"nereusCommit\":");
        String nereusCommit = cursor.string();
        cursor.expect(",\"numericProjectionSha256\":");
        String numericProjectionSha = cursor.string();
        cursor.expect(",\"sourceLocksSha256\":");
        String sourceLocksSha = cursor.string();
        cursor.expect("}}");
        cursor.end();

        Receipt receipt = new Receipt(
                schema,
                kind,
                false,
                result,
                new SourceTuple(
                        capabilitySha,
                        clientJarSha,
                        clientPomSha,
                        imageConfigDigest,
                        imageManifestDigest,
                        bookKeeperCommit,
                        bookKeeperTagObject,
                        moduleManifestSha,
                        moduleReceiptSha,
                        kafkaBaseCommit,
                        kafkaForkCommit,
                        m1FinalSha,
                        m1TupleSha,
                        n1ManifestSha,
                        n1Commit,
                        goldensSha,
                        nbke2ProjectionSha,
                        nereusCommit,
                        numericProjectionSha,
                        sourceLocksSha),
                gates);
        validate(receipt);
        if (!Arrays.equals(bytes, canonicalBytes(receipt))) {
            throw reject(RejectionCode.MALFORMED_OR_NON_CANONICAL, "receipt differs from canonical bytes");
        }
        return receipt;
    }

    public static Receipt parseCanonicalFile(Path receiptFile) {
        Objects.requireNonNull(receiptFile, "receiptFile");
        try {
            BasicFileAttributes before =
                    Files.readAttributes(receiptFile, BasicFileAttributes.class, LinkOption.NOFOLLOW_LINKS);
            if (before.isSymbolicLink() || !before.isRegularFile() || Files.isSymbolicLink(receiptFile)) {
                throw reject(RejectionCode.ROOT_NOT_REGULAR, "receipt is not one regular non-symlink file");
            }
            if (before.size() <= 0 || before.size() > MAX_CANONICAL_BYTES) {
                throw reject(RejectionCode.ROOT_BYTES_EXCEEDED, "receipt bytes are outside the cap");
            }
            byte[] bytes = Files.readAllBytes(receiptFile);
            BasicFileAttributes after =
                    Files.readAttributes(receiptFile, BasicFileAttributes.class, LinkOption.NOFOLLOW_LINKS);
            if (!Objects.equals(before.fileKey(), after.fileKey()) || before.size() != after.size()) {
                throw reject(RejectionCode.ROOT_NOT_REGULAR, "receipt changed while it was read");
            }
            return parseCanonical(bytes);
        } catch (ReceiptRejectedException failure) {
            throw failure;
        } catch (IOException failure) {
            throw new ReceiptRejectedException(RejectionCode.ROOT_NOT_REGULAR, "cannot read receipt", failure);
        }
    }

    public static byte[] canonicalBytes(Receipt receipt) {
        validate(receipt);
        StringBuilder output = new StringBuilder(4_096);
        output.append("{\"childGates\":[");
        for (int index = 0; index < receipt.childGates().size(); index++) {
            if (index != 0) {
                output.append(',');
            }
            GateResult gate = receipt.childGates().get(index);
            output.append("{\"errors\":")
                    .append(gate.errors())
                    .append(",\"failed\":")
                    .append(gate.failed())
                    .append(",\"gateId\":");
            quote(output, gate.gateId().name());
            output.append(",\"skipped\":")
                    .append(gate.skipped())
                    .append(",\"suites\":")
                    .append(gate.suites())
                    .append(",\"tests\":")
                    .append(gate.tests())
                    .append('}');
        }
        output.append("],\"kind\":");
        quote(output, receipt.kind().name());
        output.append(",\"promotionEligible\":false,\"result\":");
        quote(output, receipt.result().name());
        output.append(",\"schema\":");
        quote(output, receipt.schema());
        output.append(",\"sourceTuple\":{");
        SourceTuple source = receipt.sourceTuple();
        field(output, "bookKeeperCapabilitySha256", source.bookKeeperCapabilitySha256(), false);
        field(output, "bookKeeperClientJarSha256", source.bookKeeperClientJarSha256(), true);
        field(output, "bookKeeperClientPomSha256", source.bookKeeperClientPomSha256(), true);
        field(output, "bookKeeperImageConfigDigest", source.bookKeeperImageConfigDigest(), true);
        field(output, "bookKeeperImageManifestDigest", source.bookKeeperImageManifestDigest(), true);
        field(output, "bookKeeperSourceCommit", source.bookKeeperSourceCommit(), true);
        field(output, "bookKeeperTagObject", source.bookKeeperTagObject(), true);
        field(output, "k0ModuleManifestSha256", source.k0ModuleManifestSha256(), true);
        field(output, "k0ModuleReceiptSha256", source.k0ModuleReceiptSha256(), true);
        field(output, "kafkaBaseCommit", source.kafkaBaseCommit(), true);
        field(output, "kafkaForkCommit", source.kafkaForkCommit(), true);
        field(output, "m1FinalIndexSha256", source.m1FinalIndexSha256(), true);
        field(output, "m1SourceTupleSha256", source.m1SourceTupleSha256(), true);
        field(output, "n1ManifestSha256", source.n1ManifestSha256(), true);
        field(output, "n1SourceCommit", source.n1SourceCommit(), true);
        field(output, "nbke2GoldensSha256", source.nbke2GoldensSha256(), true);
        field(output, "nbke2ProjectionSha256", source.nbke2ProjectionSha256(), true);
        field(output, "nereusCommit", source.nereusCommit(), true);
        field(output, "numericProjectionSha256", source.numericProjectionSha256(), true);
        field(output, "sourceLocksSha256", source.sourceLocksSha256(), true);
        output.append("}}");
        byte[] bytes = output.toString().getBytes(StandardCharsets.UTF_8);
        if (bytes.length > MAX_CANONICAL_BYTES) {
            throw reject(RejectionCode.ROOT_BYTES_EXCEEDED, "canonical receipt exceeds the cap");
        }
        return bytes;
    }

    public static void validate(Receipt receipt) {
        Objects.requireNonNull(receipt, "receipt");
        if (!SCHEMA.equals(receipt.schema())
                || receipt.kind() != ReceiptKind.KAFKA_M2_INPUTS_ONLY
                || receipt.result() != ReceiptResult.PASS_KAFKA_M2_INPUTS_ONLY) {
            throw reject(RejectionCode.SCHEMA_KIND_RESULT_INVALID, "unknown receipt schema, kind, or result");
        }
        if (receipt.promotionEligible()) {
            throw reject(RejectionCode.PROMOTION_FORBIDDEN, "Kafka Inputs receipt can never promote a scenario");
        }
        validateSourceTuple(receipt.sourceTuple());
        if (receipt.childGates().size() != GateId.values().length) {
            throw reject(RejectionCode.GATE_SET_INVALID, "receipt does not contain the five exact K0 gates");
        }
        GateId[] expected = GateId.values();
        for (int index = 0; index < expected.length; index++) {
            GateResult gate = receipt.childGates().get(index);
            if (gate.gateId() != expected[index]) {
                throw reject(RejectionCode.GATE_SET_INVALID, "K0 gates are missing, duplicated, or unsorted");
            }
            if (gate.suites() <= 0
                    || gate.tests() <= 0
                    || gate.failed() != 0
                    || gate.errors() != 0
                    || gate.skipped() != 0) {
                throw reject(RejectionCode.GATE_RESULT_NOT_PASS, "a K0 gate is empty, failed, errored, or skipped");
            }
        }
    }

    private static void validateSourceTuple(SourceTuple source) {
        Objects.requireNonNull(source, "sourceTuple");
        requireSha(source.bookKeeperCapabilitySha256());
        requireSha(source.bookKeeperClientJarSha256());
        requireSha(source.bookKeeperClientPomSha256());
        requireImageDigest(source.bookKeeperImageConfigDigest());
        requireImageDigest(source.bookKeeperImageManifestDigest());
        requireCommit(source.bookKeeperSourceCommit());
        requireCommit(source.bookKeeperTagObject());
        requireSha(source.k0ModuleManifestSha256());
        requireSha(source.k0ModuleReceiptSha256());
        requireCommit(source.kafkaBaseCommit());
        requireCommit(source.kafkaForkCommit());
        requireSha(source.m1FinalIndexSha256());
        requireSha(source.m1SourceTupleSha256());
        requireSha(source.n1ManifestSha256());
        requireCommit(source.n1SourceCommit());
        requireSha(source.nbke2GoldensSha256());
        requireSha(source.nbke2ProjectionSha256());
        requireCommit(source.nereusCommit());
        requireSha(source.numericProjectionSha256());
        requireSha(source.sourceLocksSha256());
    }

    private static void requireSha(String value) {
        if (value == null || !SHA256.matcher(value).matches()) {
            throw reject(RejectionCode.SOURCE_TUPLE_INVALID, "source tuple SHA-256 is invalid");
        }
    }

    private static void requireImageDigest(String value) {
        if (value == null || !IMAGE_DIGEST.matcher(value).matches()) {
            throw reject(RejectionCode.SOURCE_TUPLE_INVALID, "source tuple image digest is invalid");
        }
    }

    private static void requireCommit(String value) {
        if (value == null || !COMMIT.matcher(value).matches()) {
            throw reject(RejectionCode.SOURCE_TUPLE_INVALID, "source tuple commit is invalid");
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

    private static <T extends Enum<T>> T enumValue(Class<T> type, String name) {
        try {
            return Enum.valueOf(type, name);
        } catch (IllegalArgumentException failure) {
            throw new ReceiptRejectedException(RejectionCode.MALFORMED_OR_NON_CANONICAL, "unknown enum value", failure);
        }
    }

    private static ReceiptRejectedException reject(RejectionCode code, String message) {
        return new ReceiptRejectedException(code, message);
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

        private boolean peek(char value) {
            return position < input.length() && input.charAt(position) == value;
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
                if (value < 0x20 || value == '\\') {
                    throw reject(RejectionCode.MALFORMED_OR_NON_CANONICAL, "escaped/control strings are forbidden");
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
            while (position < input.length() && Character.isDigit(input.charAt(position))) {
                position++;
            }
            if (start == position || position - start > 1 && input.charAt(start) == '0') {
                throw reject(RejectionCode.MALFORMED_OR_NON_CANONICAL, "invalid canonical integer");
            }
            try {
                return Long.parseLong(input.substring(start, position));
            } catch (NumberFormatException failure) {
                throw new ReceiptRejectedException(
                        RejectionCode.MALFORMED_OR_NON_CANONICAL, "integer is outside the exact domain", failure);
            }
        }

        private void end() {
            if (position != input.length()) {
                throw reject(RejectionCode.MALFORMED_OR_NON_CANONICAL, "trailing receipt bytes");
            }
        }
    }
}
