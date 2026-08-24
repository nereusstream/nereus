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

package com.nereusstream.storage.object.extent;

import com.nereusstream.storage.object.nwg1.Nwg1EnvelopeV1;
import com.nereusstream.storage.object.nwg1.Nwg1ObjectReaderV1;
import com.nereusstream.storage.object.nwg1.Nwg1RejectionV1;
import com.nereusstream.storage.object.nwg1.Nwg1ValidationException;
import com.nereusstream.storage.object.nwg1.Nwg1ValidationStageV1;
import com.nereusstream.storage.object.nwg1.Nwg1ZstdV1;
import java.nio.charset.StandardCharsets;
import java.nio.file.Files;
import java.nio.file.Path;
import java.nio.file.StandardOpenOption;
import java.security.MessageDigest;
import java.util.Arrays;
import java.util.HexFormat;
import java.util.LinkedHashMap;
import java.util.List;
import java.util.Map;
import java.util.regex.Pattern;

/**
 * Allocation-free D1 capacity harness at the admitted NWG1 v1 format-hard bounds. It exercises checked counters and
 * closed analytical charges; it never materializes a maximum-size Object or decoded body.
 */
public final class ObjectWalLocalCapacityHarnessV1 {
    public static final String RECEIPT_SCHEMA = "NEREUS_V2_M3_D1_LOCAL_CAP_RESULT_V1";
    public static final String RECEIPT_RESULT = "PASS_LOCAL_FORMAT_CAP_CONFORMANCE_ONLY";
    private static final String ZERO_SHA256 = "0".repeat(64);
    private static final Pattern COMMIT = Pattern.compile("[0-9a-f]{40}");
    private static final Pattern SHA256 = Pattern.compile("[0-9a-f]{64}");
    private static final List<RecordContract> RECORD_CONTRACTS = List.of(
            new RecordContract(
                    "NWG1_CAP_LOCAL_FORMULA_V1",
                    "NWG1 v1 format-hard cap formula and Cartesian non-closure",
                    "nereus-storage-object/src/main/java/com/nereusstream/storage/object/extent/"
                            + "ObjectWalFormatCaps.java"),
            new RecordContract(
                    "NWG1_CAP_LOCAL_PARSER_V1",
                    "lengths-first bounded NWG1 local parser inputs",
                    "nereus-storage-object/src/main/java/com/nereusstream/storage/object/nwg1/"
                            + "Nwg1ObjectReaderV1.java"),
            new RecordContract(
                    "NWG1_CAP_LOCAL_CHECKED_ARITHMETIC_V1",
                    "checked count offset range and narrowing arithmetic",
                    "nereus-storage-object/src/main/java/com/nereusstream/storage/object/extent/"
                            + "CheckedExtentAccounting.java"),
            new RecordContract(
                    "NWG1_CAP_LOCAL_KMS_ENVELOPE_V1",
                    "bounded canonical KMS envelope round trip and oversize rejection",
                    "nereus-storage-object/src/main/java/com/nereusstream/storage/object/nwg1/Nwg1EnvelopeV1.java"),
            new RecordContract(
                    "NWG1_CAP_LOCAL_ZSTD_V1",
                    "bounded ZSTD semantic round trip without exact compressor-output authority",
                    "nereus-storage-object/src/main/java/com/nereusstream/storage/object/nwg1/Nwg1ZstdV1.java"),
            new RecordContract(
                    "NWG1_CAP_LOCAL_STREAMING_COUNTER_V1",
                    "allocation-free analytical counters only; no real Provider transfer",
                    "nereus-storage-object/src/main/java/com/nereusstream/storage/object/extent/"
                            + "CheckedStreamingCounter.java"));

    private ObjectWalLocalCapacityHarnessV1() {}

    public static Evidence verifyFormatHardCaps() {
        ObjectWalAdmissionCaps caps = formatHardCaps();
        Counts counts = new Counts();

        CheckedExtentAccounting minimum = new CheckedExtentAccounting(caps);
        require(
                minimum.snapshot().equals(new CheckedExtentAccounting.Snapshot(0, 0, 0, 0, 0, 0)),
                "minimum empty counters");
        minimum.chargeFrame(0, 16, 0);
        require(minimum.snapshot().storedBodyBytes() == 16, "minimum zero-payload GCM frame");
        counts.exact();
        counts.minimumChecks++;

        CheckedExtentAccounting contexts = new CheckedExtentAccounting(caps);
        for (int index = 0; index < ObjectWalFormatCaps.MAX_CONTEXTS; index++) {
            contexts.chargeContext(0);
        }
        require(contexts.snapshot().contexts() == ObjectWalFormatCaps.MAX_CONTEXTS, "context exact cap");
        counts.exact();
        counts.reject(() -> contexts.chargeContext(0));

        CheckedExtentAccounting units = new CheckedExtentAccounting(caps);
        for (int index = 0; index < ObjectWalFormatCaps.MAX_APPEND_UNITS; index++) {
            units.chargeAppendUnit(0);
        }
        require(units.snapshot().appendUnits() == ObjectWalFormatCaps.MAX_APPEND_UNITS, "append-unit exact cap");
        counts.exact();
        counts.reject(() -> units.chargeAppendUnit(0));

        CheckedExtentAccounting frames = new CheckedExtentAccounting(caps);
        for (int index = 0; index < ObjectWalFormatCaps.MAX_FRAMES; index++) {
            frames.chargeFrame(0, 16, 0);
        }
        require(frames.snapshot().frames() == ObjectWalFormatCaps.MAX_FRAMES, "frame exact cap");
        require(frames.snapshot().totalDecodedBytes() == 0, "zero decoded frame");
        require(frames.snapshot().storedBodyBytes() == 16L * ObjectWalFormatCaps.MAX_FRAMES, "GCM-only body");
        counts.exact();
        counts.reject(() -> frames.chargeFrame(0, 16, 0));

        CheckedExtentAccounting directory = new CheckedExtentAccounting(caps);
        directory.chargeContext(ObjectWalFormatCaps.MAX_DIRECTORY_PLAINTEXT_BYTES);
        require(
                directory.snapshot().directoryPlaintextBytes() == ObjectWalFormatCaps.MAX_DIRECTORY_PLAINTEXT_BYTES,
                "directory plaintext exact cap");
        counts.exact();
        counts.reject(() -> directory.chargeAppendUnit(1));
        require(directory.snapshot().appendUnits() == 0, "rejected directory charge must be atomic");

        CheckedExtentAccounting maximumFrame = new CheckedExtentAccounting(caps);
        maximumFrame.chargeFrame(
                0, ObjectWalFormatCaps.MAX_FRAME_STORED_BYTES, ObjectWalFormatCaps.MAX_FRAME_DECODED_BYTES);
        require(
                maximumFrame.snapshot().storedBodyBytes() == ObjectWalFormatCaps.MAX_FRAME_STORED_BYTES,
                "stored frame exact cap");
        require(
                maximumFrame.snapshot().totalDecodedBytes() == ObjectWalFormatCaps.MAX_FRAME_DECODED_BYTES,
                "decoded frame exact cap");
        counts.exact();
        counts.reject(() ->
                new CheckedExtentAccounting(caps).chargeFrame(0, ObjectWalFormatCaps.MAX_FRAME_STORED_BYTES + 1, 0));
        counts.reject(() ->
                new CheckedExtentAccounting(caps).chargeFrame(0, 16, ObjectWalFormatCaps.MAX_FRAME_DECODED_BYTES + 1));

        CheckedExtentAccounting decodedTotal = new CheckedExtentAccounting(caps);
        int maximumFramesAtDecodedCap = Math.toIntExact(
                ObjectWalFormatCaps.MAX_TOTAL_DECODED_BYTES / ObjectWalFormatCaps.MAX_FRAME_DECODED_BYTES);
        for (int index = 0; index < maximumFramesAtDecodedCap; index++) {
            decodedTotal.chargeFrame(0, 16, ObjectWalFormatCaps.MAX_FRAME_DECODED_BYTES);
        }
        require(
                decodedTotal.snapshot().totalDecodedBytes() == ObjectWalFormatCaps.MAX_TOTAL_DECODED_BYTES,
                "decoded total exact cap");
        counts.exact();
        counts.reject(() -> decodedTotal.chargeFrame(0, 16, 1));

        CheckedExtentAccounting body = new CheckedExtentAccounting(caps);
        body.chargeFixedBodyBytes(ObjectWalFormatCaps.MAX_BODY_BYTES - 1);
        require(body.snapshot().storedBodyBytes() == ObjectWalFormatCaps.MAX_BODY_BYTES - 1, "body cap minus one");
        counts.exact();
        counts.capMinusOneChecks++;
        body.chargeFixedBodyBytes(1);
        require(body.snapshot().storedBodyBytes() == ObjectWalFormatCaps.MAX_BODY_BYTES, "body exact cap");
        counts.exact();
        counts.reject(() -> body.chargeFixedBodyBytes(1));

        CheckedExtentAccounting prefix = new CheckedExtentAccounting(caps);
        prefix.chargeFixedBodyBytes(ObjectWalFormatCaps.MAX_DIRECTORY_PREFIX_BYTES);
        int prefixEnd = prefix.checkedDirectoryPrefixEnd(256, ObjectWalFormatCaps.MAX_DIRECTORY_PREFIX_BYTES - 256L);
        require(prefixEnd == ObjectWalFormatCaps.MAX_DIRECTORY_PREFIX_BYTES, "directory prefix exact endpoint");
        counts.exact();
        counts.reject(
                () -> prefix.checkedDirectoryPrefixEnd(256, ObjectWalFormatCaps.MAX_DIRECTORY_PREFIX_BYTES - 255L));

        long bodyEnd = CheckedExtentAccounting.checkedEnd(
                ObjectWalFormatCaps.MAX_BODY_BYTES - 1, 1, ObjectWalFormatCaps.MAX_BODY_BYTES);
        require(bodyEnd == ObjectWalFormatCaps.MAX_BODY_BYTES, "body exact endpoint");
        counts.exact();
        counts.reject(() -> CheckedExtentAccounting.checkedEnd(
                ObjectWalFormatCaps.MAX_BODY_BYTES - 1, 2, ObjectWalFormatCaps.MAX_BODY_BYTES));
        counts.reject(() -> CheckedExtentAccounting.checkedEnd(Long.MAX_VALUE, 1, Long.MAX_VALUE));
        counts.reject(() -> Math.toIntExact(ObjectWalFormatCaps.MAX_BODY_BYTES));
        counts.checkedToIntChecks++;

        CheckedExtentAccounting cartesian = new CheckedExtentAccounting(caps);
        for (int index = 0; index < 63; index++) {
            cartesian.chargeFrame(
                    0, ObjectWalFormatCaps.MAX_FRAME_STORED_BYTES, ObjectWalFormatCaps.MAX_FRAME_DECODED_BYTES);
        }
        counts.reject(() -> cartesian.chargeFrame(
                0, ObjectWalFormatCaps.MAX_FRAME_STORED_BYTES, ObjectWalFormatCaps.MAX_FRAME_DECODED_BYTES));
        counts.cartesianNonClosureChecks++;

        verifyStreamingCounter(ObjectWalFormatCaps.MAX_BODY_BYTES, 64 * 1024, counts, "4-GiB body");
        verifyStreamingCounter(ObjectWalFormatCaps.MAX_TOTAL_DECODED_BYTES, 64 * 1024, counts, "4-GiB decoded");
        verifyStreamingCounter(ObjectWalFormatCaps.MAX_DIRECTORY_PREFIX_BYTES, 64 * 1024, counts, "4-MiB prefix");
        verifyStreamingCounter(ObjectWalFormatCaps.MAX_FRAME_DECODED_BYTES, 64 * 1024, counts, "64-MiB frame");

        verifyKmsEnvelope(counts);
        verifyLengthsFirstParser(counts);
        verifyZstdSemanticRoundTrip(counts);

        List<LocalRecord> records = List.of(
                record(0, "exactBoundaryChecks=15;cartesianNonClosureChecks=1"),
                record(1, "truncatedInputRejects=2;validationStage=HEADER_GRAMMAR"),
                record(2, "rejectionChecks=17;checkedToIntChecks=1"),
                record(3, "canonicalRoundTrips=1;oversizeRejections=1"),
                record(4, "semanticRoundTrips=1;productionExactOutputClaims=0"),
                record(5, "analytical4GiBCounters=2;realProviderTransfers=0;streamingCounterChecks=4"));

        return new Evidence(
                counts.exactBoundaryChecks,
                counts.rejectionChecks,
                counts.minimumChecks,
                counts.capMinusOneChecks,
                counts.cartesianNonClosureChecks,
                counts.checkedToIntChecks,
                counts.streamingCounterChecks,
                counts.parserChecks,
                counts.kmsEnvelopeChecks,
                counts.zstdSemanticChecks,
                ObjectWalFormatCaps.MAX_DIRECTORY_PREFIX_BYTES,
                ObjectWalFormatCaps.MAX_DIRECTORY_PLAINTEXT_BYTES,
                ObjectWalFormatCaps.MAX_CONTEXTS,
                ObjectWalFormatCaps.MAX_APPEND_UNITS,
                ObjectWalFormatCaps.MAX_FRAMES,
                ObjectWalFormatCaps.MAX_FRAME_DECODED_BYTES,
                ObjectWalFormatCaps.MAX_FRAME_STORED_BYTES,
                ObjectWalFormatCaps.MAX_TOTAL_DECODED_BYTES,
                ObjectWalFormatCaps.MAX_BODY_BYTES,
                true,
                true,
                false,
                records);
    }

    private static void verifyKmsEnvelope(Counts counts) {
        Nwg1EnvelopeV1 envelope = new Nwg1EnvelopeV1(
                ascii("VAULT_TRANSIT"), ascii("AES256_GCM96"), ascii("transit/keys/nereus"), ascii("2"), new byte[32]);
        byte[] framed = envelope.framedBytes();
        require(Arrays.equals(framed, Nwg1EnvelopeV1.decode(framed).framedBytes()), "KMS envelope round trip");
        expectRejected(() -> new Nwg1EnvelopeV1(
                ascii("VAULT_TRANSIT"),
                ascii("AES256_GCM96"),
                ascii("transit/keys/nereus"),
                ascii("2"),
                new byte[16_385]));
        counts.kmsEnvelopeChecks = 2;
    }

    private static void verifyLengthsFirstParser(Counts counts) {
        expectParserRejection(null);
        expectParserRejection(new byte[255]);
        counts.parserChecks = 2;
    }

    private static void expectParserRejection(byte[] body) {
        try {
            Nwg1ObjectReaderV1.read(body, new byte[32], null, new byte[32]);
        } catch (Nwg1ValidationException expected) {
            require(expected.rejection() == Nwg1RejectionV1.TRUNCATED_INPUT, "parser truncated-input rejection");
            require(expected.stage() == Nwg1ValidationStageV1.HEADER_GRAMMAR, "parser lengths-first stage");
            return;
        }
        throw new IllegalStateException("D1 truncated parser input was admitted");
    }

    private static void verifyZstdSemanticRoundTrip(Counts counts) {
        byte[] decoded = new byte[4_096];
        Arrays.fill(decoded, (byte) 'z');
        Nwg1ZstdV1.EncodingResult encoded = Nwg1ZstdV1.encodeIfSmaller(decoded);
        require(encoded.codecKind() == 1, "compressible ZSTD semantic selection");
        require(
                Arrays.equals(decoded, Nwg1ZstdV1.decompress(encoded.preAeadBytes(), decoded.length)),
                "ZSTD semantic round trip");
        expectRejected(() -> Nwg1ZstdV1.validateStandardFrame(
                encoded.preAeadBytes(), ObjectWalFormatCaps.MAX_FRAME_DECODED_BYTES + 1));
        counts.zstdSemanticChecks = 2;
    }

    private static byte[] ascii(String value) {
        return value.getBytes(StandardCharsets.US_ASCII);
    }

    private static void expectRejected(Runnable operation) {
        try {
            operation.run();
        } catch (IllegalArgumentException expected) {
            return;
        }
        throw new IllegalStateException("D1 component cap+1 operation was admitted");
    }

    private static LocalRecord record(int ordinal, String counter) {
        RecordContract contract = RECORD_CONTRACTS.get(ordinal);
        return new LocalRecord(contract.name(), contract.subject(), counter, contract.sourcePath());
    }

    private static ObjectWalAdmissionCaps formatHardCaps() {
        return new ObjectWalAdmissionCaps(
                ObjectWalFormatCaps.MAX_BODY_BYTES,
                ObjectWalFormatCaps.MAX_DIRECTORY_PREFIX_BYTES,
                ObjectWalFormatCaps.MAX_DIRECTORY_PLAINTEXT_BYTES,
                ObjectWalFormatCaps.MAX_TOTAL_DECODED_BYTES);
    }

    private static void verifyStreamingCounter(long maximumBytes, int chunkBytes, Counts counts, String label) {
        CheckedStreamingCounter counter = new CheckedStreamingCounter(maximumBytes);
        long chunks = Math.floorDiv(maximumBytes, chunkBytes);
        long remainder = Math.floorMod(maximumBytes, chunkBytes);
        for (long index = 0; index < chunks; index++) {
            counter.charge(chunkBytes);
        }
        if (remainder != 0) {
            counter.charge(remainder);
            chunks = Math.incrementExact(chunks);
        }
        require(counter.snapshot().bytes() == maximumBytes, label + " streaming exact bytes");
        require(counter.snapshot().chunks() == chunks, label + " streaming exact chunks");
        counts.exact();
        counts.streamingCounterChecks = Math.incrementExact(counts.streamingCounterChecks);
        counts.reject(() -> counter.charge(1));
    }

    private static void require(boolean condition, String label) {
        if (!condition) {
            throw new IllegalStateException("D1 exact-cap invariant failed: " + label);
        }
    }

    private static final class Counts {
        private int exactBoundaryChecks;
        private int rejectionChecks;
        private int minimumChecks;
        private int capMinusOneChecks;
        private int cartesianNonClosureChecks;
        private int checkedToIntChecks;
        private int streamingCounterChecks;
        private int parserChecks;
        private int kmsEnvelopeChecks;
        private int zstdSemanticChecks;

        private void exact() {
            exactBoundaryChecks = Math.incrementExact(exactBoundaryChecks);
        }

        private void reject(Runnable operation) {
            try {
                operation.run();
            } catch (IllegalArgumentException | ArithmeticException expected) {
                rejectionChecks = Math.incrementExact(rejectionChecks);
                return;
            }
            throw new IllegalStateException("D1 cap+1/overflow operation was admitted");
        }
    }

    public record Evidence(
            int exactBoundaryChecks,
            int rejectionChecks,
            int minimumChecks,
            int capMinusOneChecks,
            int cartesianNonClosureChecks,
            int checkedToIntChecks,
            int streamingCounterChecks,
            int parserChecks,
            int kmsEnvelopeChecks,
            int zstdSemanticChecks,
            int maximumDirectoryPrefixBytes,
            int maximumDirectoryPlaintextBytes,
            int maximumContexts,
            int maximumAppendUnits,
            int maximumFrames,
            int maximumFrameDecodedBytes,
            int maximumFrameStoredBytes,
            long maximumTotalDecodedBytes,
            long maximumBodyBytes,
            boolean zeroDecodedFrameAdmitted,
            boolean allocationFreeAnalyticalOnly,
            boolean providerTransferClaimed,
            List<LocalRecord> records) {
        public Evidence {
            records = List.copyOf(records);
            if (exactBoundaryChecks <= 0
                    || rejectionChecks <= 0
                    || minimumChecks <= 0
                    || capMinusOneChecks <= 0
                    || cartesianNonClosureChecks <= 0
                    || checkedToIntChecks <= 0
                    || streamingCounterChecks <= 0
                    || parserChecks != 2
                    || kmsEnvelopeChecks != 2
                    || zstdSemanticChecks != 2
                    || !zeroDecodedFrameAdmitted
                    || !allocationFreeAnalyticalOnly
                    || providerTransferClaimed
                    || records.size() != RECORD_CONTRACTS.size()
                    || !records.stream()
                            .map(LocalRecord::name)
                            .toList()
                            .equals(RECORD_CONTRACTS.stream()
                                    .map(RecordContract::name)
                                    .toList())) {
                throw new IllegalArgumentException("D1 capacity evidence is empty or incomplete");
            }
        }
    }

    public record LocalRecord(String name, String subject, String counter, String sourcePath) {
        public LocalRecord {
            if (name == null
                    || name.isBlank()
                    || subject == null
                    || subject.isBlank()
                    || counter == null
                    || counter.isBlank()
                    || sourcePath == null
                    || sourcePath.isBlank()) {
                throw new IllegalArgumentException("D1 local record is empty");
            }
        }
    }

    private record RecordContract(String name, String subject, String sourcePath) {}

    /** Emits the exact six-record execution payload. Source hashes are supplied by the clean exact-source gate. */
    public static String renderEvidenceJson(
            String testedCommit,
            String harnessSourceSha256,
            String harnessTestSourceSha256,
            Map<String, String> sourceSha256) {
        if (testedCommit == null || !COMMIT.matcher(testedCommit).matches()) {
            throw new IllegalArgumentException("testedCommit");
        }
        if (harnessSourceSha256 == null
                || !SHA256.matcher(harnessSourceSha256).matches()
                || harnessTestSourceSha256 == null
                || !SHA256.matcher(harnessTestSourceSha256).matches()) {
            throw new IllegalArgumentException("D1 harness source SHA inventory");
        }
        if (sourceSha256 == null
                || sourceSha256.size() != RECORD_CONTRACTS.size()
                || !sourceSha256
                        .keySet()
                        .equals(RECORD_CONTRACTS.stream()
                                .map(RecordContract::name)
                                .collect(java.util.stream.Collectors.toUnmodifiableSet()))) {
            throw new IllegalArgumentException("D1 source SHA inventory");
        }
        Evidence evidence = verifyFormatHardCaps();
        StringBuilder records = new StringBuilder(2_048).append('[');
        for (int index = 0; index < evidence.records().size(); index++) {
            if (index > 0) {
                records.append(',');
            }
            LocalRecord record = evidence.records().get(index);
            String sourceSha = sourceSha256.get(record.name());
            if (sourceSha == null || !SHA256.matcher(sourceSha).matches()) {
                throw new IllegalArgumentException("D1 source SHA: " + record.name());
            }
            records.append("{\"counter\":\"")
                    .append(record.counter())
                    .append("\",\"name\":\"")
                    .append(record.name())
                    .append("\",\"sourcePath\":\"")
                    .append(record.sourcePath())
                    .append("\",\"sourceSha256\":\"")
                    .append(sourceSha)
                    .append("\",\"subject\":\"")
                    .append(record.subject())
                    .append("\"}");
        }
        records.append(']');
        String zeroed = "{\"allocationFreeAnalyticalOnly\":true,\"childKind\":\"D_LOCAL_CAP\","
                + "\"errors\":0,\"failures\":0,\"harnessSourceSha256\":\""
                + harnessSourceSha256
                + "\",\"harnessTestSourceSha256\":\""
                + harnessTestSourceSha256
                + "\",\"nereusCommit\":\""
                + testedCommit
                + "\",\"promotionEligible\":false,\"providerTransferClaimed\":false,"
                + "\"receiptSha256\":\""
                + ZERO_SHA256
                + "\",\"records\":"
                + records
                + ",\"result\":\""
                + RECEIPT_RESULT
                + "\",\"schema\":\""
                + RECEIPT_SCHEMA
                + "\",\"skipped\":0,\"tests\":6}";
        String selfSha = sha256(zeroed.getBytes(StandardCharsets.UTF_8));
        int offset = zeroed.indexOf(ZERO_SHA256);
        return zeroed.substring(0, offset) + selfSha + zeroed.substring(offset + ZERO_SHA256.length());
    }

    /** Standalone CREATE_NEW emitter used by the formal post-test evidence task. */
    public static void main(String[] arguments) throws Exception {
        if (arguments.length != 10) {
            throw new IllegalArgumentException(
                    "expected output, tested commit, harness/test SHA, and six ordered component source SHA values");
        }
        Map<String, String> sourceSha256 = new LinkedHashMap<>();
        for (int index = 0; index < RECORD_CONTRACTS.size(); index++) {
            sourceSha256.put(RECORD_CONTRACTS.get(index).name(), arguments[index + 4]);
        }
        Path output = Path.of(arguments[0]).toAbsolutePath().normalize();
        Files.writeString(
                output,
                renderEvidenceJson(arguments[1], arguments[2], arguments[3], sourceSha256),
                StandardCharsets.UTF_8,
                StandardOpenOption.CREATE_NEW,
                StandardOpenOption.WRITE);
    }

    private static String sha256(byte[] value) {
        try {
            return HexFormat.of().formatHex(MessageDigest.getInstance("SHA-256").digest(value));
        } catch (java.security.NoSuchAlgorithmException impossible) {
            throw new AssertionError(impossible);
        }
    }
}
