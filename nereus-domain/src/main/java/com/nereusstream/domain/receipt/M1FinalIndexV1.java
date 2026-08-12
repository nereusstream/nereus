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
import java.util.ArrayList;
import java.util.Arrays;
import java.util.Comparator;
import java.util.HashSet;
import java.util.List;
import java.util.Objects;
import java.util.Set;
import java.util.regex.Pattern;

/** Closed canonical Final-index and gate-result wire used only to aggregate already executed M1 evidence. */
public final class M1FinalIndexV1 {
    public static final String INDEX_SCHEMA = "NEREUS_V2_M1_FINAL_INDEX_V1";
    public static final String GATE_RESULT_SCHEMA = "NEREUS_V2_M1_GATE_RESULT_V1";
    public static final int MAX_CANONICAL_BYTES = VirtualLedgerReceiptV1.MAX_CANONICAL_ROOT_BYTES;
    public static final int MAX_GATE_REFS = 8;
    public static final int MAX_RECEIPT_REFS = VirtualLedgerReceiptV1.MAX_ATTACHMENTS;
    public static final int MAX_REFERENCE_BYTES = VirtualLedgerReceiptV1.MAX_CANONICAL_ROOT_BYTES;
    public static final int MAX_TOTAL_REFERENCE_BYTES = VirtualLedgerReceiptV1.MAX_TOTAL_ATTACHMENT_BYTES;

    private static final Pattern SHA256 = Pattern.compile("[0-9a-f]{64}");
    private static final Comparator<GateRef> GATE_ORDER =
            Comparator.comparing(row -> row.gateId().name());
    private static final Comparator<ReceiptRef> RECEIPT_ORDER = Comparator.comparing(ReceiptRef::path);

    private M1FinalIndexV1() {}

    public enum GateId {
        V2_M1_FAST,
        V2_M1_EXACT_SOURCE
    }

    public enum GateOutcome {
        PASS,
        FAIL
    }

    public enum RejectionCode {
        FINAL_ROOT_NOT_REGULAR,
        FINAL_ROOT_BYTES_EXCEEDED,
        FINAL_MALFORMED_OR_NON_CANONICAL,
        FINAL_SCHEMA_INVALID,
        FINAL_SOURCE_TUPLE_INVALID,
        FINAL_REFERENCE_COUNT_EXCEEDED,
        FINAL_REFERENCE_INVALID,
        FINAL_REFERENCE_UNSORTED_OR_DUPLICATE,
        FINAL_REFERENCE_BYTES_EXCEEDED,
        FINAL_REFERENCE_TOTAL_BYTES_EXCEEDED,
        FINAL_REFERENCE_NOT_REGULAR,
        FINAL_REFERENCE_LENGTH_MISMATCH,
        FINAL_REFERENCE_DIGEST_MISMATCH,
        FINAL_REQUIRED_GATE_MISSING,
        FINAL_GATE_NOT_PASS,
        FINAL_RECEIPT_KIND_MISMATCH,
        FINAL_RECEIPT_SET_INCOMPLETE,
        FINAL_RECEIPT_SCENARIO_DUPLICATE,
        FINAL_RECEIPT_SOURCE_TUPLE_MISMATCH,
        FINAL_CHECKED_ARITHMETIC_OVERFLOW
    }

    public static final class FinalRejectedException extends IllegalArgumentException {
        private final RejectionCode code;

        public FinalRejectedException(RejectionCode code, String detail) {
            super(code + ": " + detail);
            this.code = Objects.requireNonNull(code, "code");
        }

        public FinalRejectedException(RejectionCode code, String detail, Throwable cause) {
            super(code + ": " + detail, cause);
            this.code = Objects.requireNonNull(code, "code");
        }

        public RejectionCode code() {
            return code;
        }
    }

    public record GateRef(GateId gateId, String path, long length, String sha256) {}

    public record ReceiptRef(VirtualLedgerReceiptV1.ReceiptKind kind, String path, long length, String sha256) {}

    public record Index(
            String schema, String sourceTupleSha, List<GateRef> requiredGateRefs, List<ReceiptRef> receiptRefs) {
        public Index {
            requiredGateRefs = List.copyOf(requiredGateRefs);
            receiptRefs = List.copyOf(receiptRefs);
        }
    }

    public record GateResult(String schema, GateId gateId, GateOutcome outcome, String sourceTupleSha) {}

    public static byte[] canonicalBytes(Index index) {
        validate(index);
        StringBuilder output = new StringBuilder(2048);
        output.append("{\"receiptRefs\":[");
        for (int position = 0; position < index.receiptRefs().size(); position++) {
            if (position != 0) {
                output.append(',');
            }
            ReceiptRef row = index.receiptRefs().get(position);
            output.append("{\"kind\":");
            quote(output, row.kind().name());
            output.append(",\"length\":").append(row.length()).append(",\"path\":");
            quote(output, row.path());
            output.append(",\"sha256\":");
            quote(output, row.sha256());
            output.append('}');
        }
        output.append("],\"requiredGateRefs\":[");
        for (int position = 0; position < index.requiredGateRefs().size(); position++) {
            if (position != 0) {
                output.append(',');
            }
            GateRef row = index.requiredGateRefs().get(position);
            output.append("{\"gateId\":");
            quote(output, row.gateId().name());
            output.append(",\"length\":").append(row.length()).append(",\"path\":");
            quote(output, row.path());
            output.append(",\"sha256\":");
            quote(output, row.sha256());
            output.append('}');
        }
        output.append("],\"schema\":");
        quote(output, index.schema());
        output.append(",\"sourceTupleSha\":");
        quote(output, index.sourceTupleSha());
        output.append('}');
        return bounded(output.toString().getBytes(StandardCharsets.UTF_8));
    }

    public static byte[] canonicalBytes(GateResult result) {
        validate(result);
        StringBuilder output = new StringBuilder(256);
        output.append("{\"gateId\":");
        quote(output, result.gateId().name());
        output.append(",\"outcome\":");
        quote(output, result.outcome().name());
        output.append(",\"schema\":");
        quote(output, result.schema());
        output.append(",\"sourceTupleSha\":");
        quote(output, result.sourceTupleSha());
        output.append('}');
        return bounded(output.toString().getBytes(StandardCharsets.UTF_8));
    }

    public static Index parseCanonical(byte[] bytes) {
        String json = decode(bytes);
        Cursor cursor = new Cursor(json);
        cursor.expect("{\"receiptRefs\":[");
        List<ReceiptRef> receipts = new ArrayList<>();
        if (!cursor.peek(']')) {
            do {
                if (receipts.size() == MAX_RECEIPT_REFS) {
                    throw reject(RejectionCode.FINAL_REFERENCE_COUNT_EXCEEDED, "receipt reference count exceeds cap");
                }
                cursor.expect("{\"kind\":");
                VirtualLedgerReceiptV1.ReceiptKind kind =
                        enumValue(VirtualLedgerReceiptV1.ReceiptKind.class, cursor.string(), "receipt kind");
                cursor.expect(",\"length\":");
                long length = cursor.number();
                cursor.expect(",\"path\":");
                String path = cursor.string();
                cursor.expect(",\"sha256\":");
                String sha256 = cursor.string();
                cursor.expect("}");
                receipts.add(new ReceiptRef(kind, path, length, sha256));
            } while (cursor.comma());
        }
        cursor.expect("],\"requiredGateRefs\":[");
        List<GateRef> gates = new ArrayList<>();
        if (!cursor.peek(']')) {
            do {
                if (gates.size() == MAX_GATE_REFS) {
                    throw reject(RejectionCode.FINAL_REFERENCE_COUNT_EXCEEDED, "gate reference count exceeds cap");
                }
                cursor.expect("{\"gateId\":");
                GateId gateId = enumValue(GateId.class, cursor.string(), "gate ID");
                cursor.expect(",\"length\":");
                long length = cursor.number();
                cursor.expect(",\"path\":");
                String path = cursor.string();
                cursor.expect(",\"sha256\":");
                String sha256 = cursor.string();
                cursor.expect("}");
                gates.add(new GateRef(gateId, path, length, sha256));
            } while (cursor.comma());
        }
        cursor.expect("],\"schema\":");
        String schema = cursor.string();
        cursor.expect(",\"sourceTupleSha\":");
        String sourceTupleSha = cursor.string();
        cursor.expect("}");
        cursor.end();
        Index result = new Index(schema, sourceTupleSha, gates, receipts);
        validate(result);
        if (!Arrays.equals(bytes, canonicalBytes(result))) {
            throw reject(RejectionCode.FINAL_MALFORMED_OR_NON_CANONICAL, "Final index is not canonical");
        }
        return result;
    }

    public static GateResult parseCanonicalGateResult(byte[] bytes) {
        String json = decode(bytes);
        Cursor cursor = new Cursor(json);
        cursor.expect("{\"gateId\":");
        GateId gateId = enumValue(GateId.class, cursor.string(), "gate ID");
        cursor.expect(",\"outcome\":");
        GateOutcome outcome = enumValue(GateOutcome.class, cursor.string(), "gate outcome");
        cursor.expect(",\"schema\":");
        String schema = cursor.string();
        cursor.expect(",\"sourceTupleSha\":");
        String sourceTupleSha = cursor.string();
        cursor.expect("}");
        cursor.end();
        GateResult result = new GateResult(schema, gateId, outcome, sourceTupleSha);
        validate(result);
        if (!Arrays.equals(bytes, canonicalBytes(result))) {
            throw reject(RejectionCode.FINAL_MALFORMED_OR_NON_CANONICAL, "gate result is not canonical");
        }
        return result;
    }

    public static Index parseCanonicalFile(Path path) {
        return parseCanonical(readCanonicalFile(path));
    }

    public static GateResult parseCanonicalGateResultFile(Path path) {
        return parseCanonicalGateResult(readCanonicalFile(path));
    }

    public static void validate(Index index) {
        Objects.requireNonNull(index, "index");
        if (!INDEX_SCHEMA.equals(index.schema())) {
            throw reject(RejectionCode.FINAL_SCHEMA_INVALID, "unknown Final-index schema");
        }
        requireSha(index.sourceTupleSha(), RejectionCode.FINAL_SOURCE_TUPLE_INVALID);
        if (index.requiredGateRefs().size() > MAX_GATE_REFS
                || index.receiptRefs().size() > MAX_RECEIPT_REFS) {
            throw reject(RejectionCode.FINAL_REFERENCE_COUNT_EXCEEDED, "reference count exceeds cap");
        }
        validateOrdered(index.requiredGateRefs(), GATE_ORDER, "gate references");
        validateOrdered(index.receiptRefs(), RECEIPT_ORDER, "receipt references");
        long total = 0;
        Set<String> paths = new HashSet<>();
        for (GateRef row : index.requiredGateRefs()) {
            Objects.requireNonNull(row.gateId(), "gateId");
            validateReference(row.path(), row.length(), row.sha256());
            if (!paths.add(row.path())) {
                throw reject(RejectionCode.FINAL_REFERENCE_UNSORTED_OR_DUPLICATE, "duplicate reference path");
            }
            total = checkedAdd(total, row.length());
        }
        for (ReceiptRef row : index.receiptRefs()) {
            Objects.requireNonNull(row.kind(), "kind");
            validateReference(row.path(), row.length(), row.sha256());
            if (!paths.add(row.path())) {
                throw reject(RejectionCode.FINAL_REFERENCE_UNSORTED_OR_DUPLICATE, "duplicate reference path");
            }
            total = checkedAdd(total, row.length());
        }
        if (total > MAX_TOTAL_REFERENCE_BYTES) {
            throw reject(RejectionCode.FINAL_REFERENCE_TOTAL_BYTES_EXCEEDED, "referenced canonical roots exceed cap");
        }
    }

    public static void validate(GateResult result) {
        Objects.requireNonNull(result, "result");
        if (!GATE_RESULT_SCHEMA.equals(result.schema()) || result.gateId() == null || result.outcome() == null) {
            throw reject(RejectionCode.FINAL_SCHEMA_INVALID, "invalid gate-result identity");
        }
        requireSha(result.sourceTupleSha(), RejectionCode.FINAL_SOURCE_TUPLE_INVALID);
    }

    private static void validateReference(String path, long length, String sha256) {
        try {
            VirtualLedgerReceiptV1.validatePath(path);
        } catch (VirtualLedgerReceiptV1.ReceiptRejectedException error) {
            throw new FinalRejectedException(RejectionCode.FINAL_REFERENCE_INVALID, "invalid reference path", error);
        }
        if (length <= 0 || length > MAX_REFERENCE_BYTES) {
            throw reject(RejectionCode.FINAL_REFERENCE_BYTES_EXCEEDED, "reference length is outside cap");
        }
        requireSha(sha256, RejectionCode.FINAL_REFERENCE_INVALID);
    }

    private static <T> void validateOrdered(List<T> rows, Comparator<T> order, String name) {
        for (int index = 1; index < rows.size(); index++) {
            if (order.compare(rows.get(index - 1), rows.get(index)) >= 0) {
                throw reject(RejectionCode.FINAL_REFERENCE_UNSORTED_OR_DUPLICATE, name + " must be sorted and unique");
            }
        }
    }

    private static byte[] readCanonicalFile(Path path) {
        Objects.requireNonNull(path, "path");
        BasicFileAttributes before = attributes(path);
        if (before.isSymbolicLink() || Files.isSymbolicLink(path) || !before.isRegularFile()) {
            throw reject(RejectionCode.FINAL_ROOT_NOT_REGULAR, "canonical root is not a regular file");
        }
        if (before.size() > MAX_CANONICAL_BYTES) {
            throw reject(RejectionCode.FINAL_ROOT_BYTES_EXCEEDED, "canonical root exceeds cap");
        }
        byte[] bytes;
        try (InputStream input = java.nio.channels.Channels.newInputStream(Files.newByteChannel(
                        path, Set.<OpenOption>of(StandardOpenOption.READ, LinkOption.NOFOLLOW_LINKS)));
                ByteArrayOutputStream output = new ByteArrayOutputStream((int) Math.min(8192, before.size()))) {
            byte[] buffer = new byte[8192];
            int read;
            while ((read = input.read(buffer)) != -1) {
                if (checkedAdd(output.size(), read) > MAX_CANONICAL_BYTES) {
                    throw reject(RejectionCode.FINAL_ROOT_BYTES_EXCEEDED, "streamed canonical root exceeds cap");
                }
                output.write(buffer, 0, read);
            }
            bytes = output.toByteArray();
        } catch (FinalRejectedException error) {
            throw error;
        } catch (IOException error) {
            throw new FinalRejectedException(RejectionCode.FINAL_ROOT_NOT_REGULAR, "canonical root read failed", error);
        }
        BasicFileAttributes after = attributes(path);
        if (!after.isRegularFile()
                || after.isSymbolicLink()
                || before.size() != after.size()
                || (before.fileKey() != null
                        && after.fileKey() != null
                        && !before.fileKey().equals(after.fileKey()))) {
            throw reject(RejectionCode.FINAL_ROOT_NOT_REGULAR, "canonical root changed while reading");
        }
        return bytes;
    }

    private static BasicFileAttributes attributes(Path path) {
        try {
            return Files.readAttributes(path, BasicFileAttributes.class, LinkOption.NOFOLLOW_LINKS);
        } catch (IOException error) {
            throw new FinalRejectedException(
                    RejectionCode.FINAL_ROOT_NOT_REGULAR, "cannot read file attributes", error);
        }
    }

    private static String decode(byte[] bytes) {
        Objects.requireNonNull(bytes, "bytes");
        bounded(bytes);
        if (bytes.length >= 3
                && Byte.toUnsignedInt(bytes[0]) == 0xef
                && Byte.toUnsignedInt(bytes[1]) == 0xbb
                && Byte.toUnsignedInt(bytes[2]) == 0xbf) {
            throw reject(RejectionCode.FINAL_MALFORMED_OR_NON_CANONICAL, "UTF-8 BOM is forbidden");
        }
        try {
            return StandardCharsets.UTF_8
                    .newDecoder()
                    .onMalformedInput(CodingErrorAction.REPORT)
                    .onUnmappableCharacter(CodingErrorAction.REPORT)
                    .decode(java.nio.ByteBuffer.wrap(bytes))
                    .toString();
        } catch (CharacterCodingException error) {
            throw new FinalRejectedException(
                    RejectionCode.FINAL_MALFORMED_OR_NON_CANONICAL, "canonical root is not strict UTF-8", error);
        }
    }

    private static byte[] bounded(byte[] bytes) {
        if (bytes.length > MAX_CANONICAL_BYTES) {
            throw reject(RejectionCode.FINAL_ROOT_BYTES_EXCEEDED, "canonical root exceeds cap");
        }
        return bytes;
    }

    private static void requireSha(String value, RejectionCode code) {
        if (value == null || !SHA256.matcher(value).matches()) {
            throw reject(code, "invalid lowercase SHA-256");
        }
    }

    private static long checkedAdd(long left, long right) {
        try {
            return Math.addExact(left, right);
        } catch (ArithmeticException error) {
            throw new FinalRejectedException(RejectionCode.FINAL_CHECKED_ARITHMETIC_OVERFLOW, "length overflow", error);
        }
    }

    private static <T extends Enum<T>> T enumValue(Class<T> type, String value, String field) {
        try {
            return Enum.valueOf(type, value);
        } catch (IllegalArgumentException error) {
            throw new FinalRejectedException(RejectionCode.FINAL_SCHEMA_INVALID, "unknown " + field, error);
        }
    }

    private static void quote(StringBuilder output, String value) {
        output.append('"').append(value).append('"');
    }

    private static FinalRejectedException reject(RejectionCode code, String detail) {
        return new FinalRejectedException(code, detail);
    }

    private static final class Cursor {
        private final String input;
        private int offset;

        private Cursor(String input) {
            this.input = input;
        }

        private void expect(String value) {
            if (!input.startsWith(value, offset)) {
                throw reject(RejectionCode.FINAL_MALFORMED_OR_NON_CANONICAL, "unexpected JSON token");
            }
            offset += value.length();
        }

        private boolean peek(char value) {
            return offset < input.length() && input.charAt(offset) == value;
        }

        private boolean comma() {
            if (peek(',')) {
                offset++;
                return true;
            }
            return false;
        }

        private String string() {
            if (!peek('"')) {
                throw reject(RejectionCode.FINAL_MALFORMED_OR_NON_CANONICAL, "expected string");
            }
            offset++;
            int start = offset;
            while (offset < input.length() && input.charAt(offset) != '"') {
                char value = input.charAt(offset);
                if (value < 0x20 || value > 0x7e || value == '\\') {
                    throw reject(RejectionCode.FINAL_MALFORMED_OR_NON_CANONICAL, "escaped/non-ASCII string forbidden");
                }
                offset++;
            }
            if (!peek('"')) {
                throw reject(RejectionCode.FINAL_MALFORMED_OR_NON_CANONICAL, "unterminated string");
            }
            String result = input.substring(start, offset);
            offset++;
            return result;
        }

        private long number() {
            int start = offset;
            if (offset >= input.length() || input.charAt(offset) < '0' || input.charAt(offset) > '9') {
                throw reject(RejectionCode.FINAL_MALFORMED_OR_NON_CANONICAL, "expected canonical integer");
            }
            if (input.charAt(offset) == '0') {
                offset++;
            } else {
                while (offset < input.length() && input.charAt(offset) >= '0' && input.charAt(offset) <= '9') {
                    offset++;
                }
            }
            if (offset < input.length() && input.charAt(offset) >= '0' && input.charAt(offset) <= '9') {
                throw reject(RejectionCode.FINAL_MALFORMED_OR_NON_CANONICAL, "integer has a leading zero");
            }
            try {
                long value = Long.parseLong(input.substring(start, offset));
                if (value > VirtualLedgerReceiptV1.MAX_EXACT_JSON_INTEGER) {
                    throw new NumberFormatException("outside exact JSON integer range");
                }
                return value;
            } catch (NumberFormatException error) {
                throw new FinalRejectedException(
                        RejectionCode.FINAL_MALFORMED_OR_NON_CANONICAL, "invalid canonical integer", error);
            }
        }

        private void end() {
            if (offset != input.length()) {
                throw reject(RejectionCode.FINAL_MALFORMED_OR_NON_CANONICAL, "trailing bytes");
            }
        }
    }
}
