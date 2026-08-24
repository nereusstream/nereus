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

package com.nereusstream.domain.registry.allocator;

import com.nereusstream.domain.bytes.CanonicalBytes;
import com.nereusstream.domain.bytes.Sha256Digest;
import java.io.IOException;
import java.io.InputStream;
import java.nio.ByteBuffer;
import java.nio.channels.FileChannel;
import java.nio.file.Files;
import java.nio.file.LinkOption;
import java.nio.file.Path;
import java.nio.file.StandardOpenOption;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.EnumMap;
import java.util.List;
import java.util.Map;
import java.util.Objects;

/**
 * Parsed NARS1 selection authority. Production construction and parsing always stream and recompute the exact five
 * source-bound NAEA1 attachments; caller aggregate metrics, pass flags, and selected-mode booleans are never inputs.
 */
public final class AllocatorSelectionReceiptV1 {
    public static final int REQUIRED_FAULT_CUTS = 9;
    public static final int RECEIPT_BYTES = 2328;
    public static final String RECEIPT_FILE_NAME = "selection.nars";

    private static final byte[] MAGIC = "NARS".getBytes(java.nio.charset.StandardCharsets.US_ASCII);
    private static final int SCHEMA_VERSION = 1;
    private static final int COMPLETE_EVIDENCE_FLAGS = 0x7;
    private static final int BROKER_COUNT = 4;
    private static final int METRIC_COUNT = 8;
    private static final int RESERVED_BYTES = 36;

    private final AllocatorModeV1 selectedMode;
    private final int allocatorProtocolVersion;
    private final long selectedRangeSize;
    private final AllocatorEvidenceSourceTupleV1 sourceTuple;
    private final Sha256Digest evidenceReceiptSha256;
    private final List<AllocatorNativeRelativeMetricsV1> completeScaleMetrics;
    private final Map<AllocatorEvidenceAttachmentKindV1, Sha256Digest> attachmentSha256;

    private AllocatorSelectionReceiptV1(
            AllocatorModeV1 selectedMode,
            int allocatorProtocolVersion,
            long selectedRangeSize,
            AllocatorEvidenceSourceTupleV1 sourceTuple,
            Sha256Digest evidenceReceiptSha256,
            List<AllocatorNativeRelativeMetricsV1> completeScaleMetrics,
            Map<AllocatorEvidenceAttachmentKindV1, Sha256Digest> attachmentSha256) {
        this.selectedMode = Objects.requireNonNull(selectedMode, "selectedMode");
        this.allocatorProtocolVersion = allocatorProtocolVersion;
        this.selectedRangeSize = selectedRangeSize;
        this.sourceTuple = Objects.requireNonNull(sourceTuple, "sourceTuple");
        this.evidenceReceiptSha256 = Objects.requireNonNull(evidenceReceiptSha256, "evidenceReceiptSha256");
        this.completeScaleMetrics = List.copyOf(completeScaleMetrics);
        this.attachmentSha256 = Map.copyOf(attachmentSha256);
    }

    /** Parses all raw files and returns an honest selected, neither-qualified, or both-qualified evaluation. */
    public static AllocatorEvidenceEvaluationV1 evaluateCanonicalAttachments(
            List<Path> canonicalAttachmentFiles, AllocatorEvidenceSourceArtifactsV1 sourceArtifacts) {
        EvidenceInputs inputs = loadEvidence(canonicalAttachmentFiles, sourceArtifacts);
        return evaluation(inputs);
    }

    /** Builds NARS1 only from an eligible production-parser evaluation; there is no aggregate/Boolean builder. */
    public static CanonicalBytes canonicalFromEvidence(
            List<Path> canonicalAttachmentFiles, AllocatorEvidenceSourceArtifactsV1 sourceArtifacts) {
        EvidenceInputs inputs = loadEvidence(canonicalAttachmentFiles, sourceArtifacts);
        AllocatorEvidenceEvaluationV1 evaluation = evaluation(inputs);
        if (!evaluation.selectionEligible()) {
            throw invalid("ADR-0094 raw evidence selects neither mode or qualifies both modes");
        }
        return encode(evaluation);
    }

    /** Writes one CREATE_NEW NARS1 receipt derived exclusively from the exact attachment files. */
    public static void writeCanonicalFromEvidence(
            Path target, List<Path> canonicalAttachmentFiles, AllocatorEvidenceSourceArtifactsV1 sourceArtifacts) {
        Objects.requireNonNull(target, "target");
        Path exactTarget = requireReceiptPath(target, canonicalAttachmentFiles);
        CanonicalBytes canonical = canonicalFromEvidence(canonicalAttachmentFiles, sourceArtifacts);
        try (FileChannel output =
                FileChannel.open(exactTarget, StandardOpenOption.CREATE_NEW, StandardOpenOption.WRITE)) {
            ByteBuffer bytes = ByteBuffer.wrap(canonical.toByteArray());
            while (bytes.hasRemaining()) {
                output.write(bytes);
            }
            output.force(true);
        } catch (IOException error) {
            throw new AllocatorProtocolException(
                    AllocatorProtocolException.Code.SELECTION_NOT_ELIGIBLE,
                    "allocator selection receipt could not be written",
                    error);
        }
    }

    /** Parses NARS1 and independently recomputes every row and the closed selection from raw attachment files. */
    public static AllocatorSelectionReceiptV1 parseCanonical(
            CanonicalBytes canonicalReceipt,
            List<Path> canonicalAttachmentFiles,
            AllocatorEvidenceSourceArtifactsV1 sourceArtifacts) {
        Objects.requireNonNull(canonicalReceipt, "canonicalReceipt");
        EvidenceInputs inputs = loadEvidence(canonicalAttachmentFiles, sourceArtifacts);
        AllocatorEvidenceEvaluationV1 evaluation = evaluation(inputs);
        ParsedReceipt parsed = parseReceipt(canonicalReceipt);
        if (!evaluation.selectionEligible()
                || evaluation.selectedCandidate().isEmpty()
                || parsed.mode != evaluation.selectedCandidate().orElseThrow().mode()
                || parsed.range != evaluation.selectedCandidate().orElseThrow().rangeSize()
                || !parsed.sourceTuple.equals(evaluation.sourceTuple())
                || !parsed.attachments.equals(evaluation.attachmentSha256())
                || !parsed.metrics.equals(evaluation.selectedRows())) {
            throw invalid("allocator selection receipt differs from production raw recomputation");
        }
        return new AllocatorSelectionReceiptV1(
                parsed.mode,
                VirtualLedgerCellAllocatorStateV1.PROTOCOL_VERSION,
                parsed.range,
                parsed.sourceTuple,
                Sha256Digest.hash(canonicalReceipt),
                parsed.metrics,
                parsed.attachments);
    }

    public static AllocatorSelectionReceiptV1 parseCanonical(
            Path canonicalReceiptFile,
            List<Path> canonicalAttachmentFiles,
            AllocatorEvidenceSourceArtifactsV1 sourceArtifacts) {
        Objects.requireNonNull(canonicalReceiptFile, "canonicalReceiptFile");
        Path exactReceiptFile = requireReceiptPath(canonicalReceiptFile, canonicalAttachmentFiles);
        try {
            if (!Files.isRegularFile(exactReceiptFile, LinkOption.NOFOLLOW_LINKS)) {
                throw invalid("allocator selection receipt must be an exact non-symlink regular file");
            }
            byte[] bytes;
            try (InputStream input = Files.newInputStream(exactReceiptFile, LinkOption.NOFOLLOW_LINKS)) {
                bytes = input.readNBytes(RECEIPT_BYTES + 1);
            }
            return parseCanonical(CanonicalBytes.copyOf(bytes), canonicalAttachmentFiles, sourceArtifacts);
        } catch (IOException error) {
            throw new AllocatorProtocolException(
                    AllocatorProtocolException.Code.SELECTION_NOT_ELIGIBLE,
                    "allocator selection receipt file could not be read",
                    error);
        }
    }

    private static AllocatorEvidenceEvaluationV1 evaluation(EvidenceInputs inputs) {
        AllocatorRawEvidenceValidatorV1.SelectionComputation computation =
                AllocatorRawEvidenceValidatorV1.validate(inputs.attachments);
        return AllocatorEvidenceEvaluationV1.from(inputs.sourceTuple, inputs.digests, computation);
    }

    private static EvidenceInputs loadEvidence(List<Path> files, AllocatorEvidenceSourceArtifactsV1 sourceArtifacts) {
        Objects.requireNonNull(files, "canonicalAttachmentFiles");
        Objects.requireNonNull(sourceArtifacts, "sourceArtifacts");
        if (files.size() != AllocatorEvidenceAttachmentKindV1.values().length) {
            throw invalid("allocator selection requires exactly five named evidence attachment files");
        }
        EnumMap<AllocatorEvidenceAttachmentKindV1, AllocatorEvidenceAttachmentV1> inventory =
                new EnumMap<>(AllocatorEvidenceAttachmentKindV1.class);
        EnumMap<AllocatorEvidenceAttachmentKindV1, Sha256Digest> digests =
                new EnumMap<>(AllocatorEvidenceAttachmentKindV1.class);
        AllocatorEvidenceSourceTupleV1 sourceTuple = null;
        Path exactDirectory = null;
        for (Path file : files) {
            Path exactFile = Objects.requireNonNull(file, "canonicalAttachmentFile")
                    .toAbsolutePath()
                    .normalize();
            Path parent = exactFile.getParent();
            if (parent == null || (exactDirectory != null && !exactDirectory.equals(parent))) {
                throw invalid("allocator selection attachments must share one exact directory");
            }
            exactDirectory = parent;
            AllocatorEvidenceAttachmentV1 attachment = AllocatorEvidenceAttachmentV1.parseCanonical(exactFile);
            if (!exactFile.getFileName().toString().equals(attachment.kind().fileName())) {
                throw invalid("allocator selection attachment path differs from its closed kind basename");
            }
            if (inventory.putIfAbsent(attachment.kind(), attachment) != null) {
                throw invalid("allocator selection attachment kind is duplicated");
            }
            if (sourceTuple == null) {
                sourceTuple = attachment.sourceTuple();
            } else if (!sourceTuple.equals(attachment.sourceTuple())) {
                throw invalid("allocator selection attachment exact source/executor tuples differ");
            }
            digests.put(attachment.kind(), attachment.envelopeSha256());
        }
        if (inventory.size() != AllocatorEvidenceAttachmentKindV1.values().length || sourceTuple == null) {
            throw invalid("allocator selection attachment inventory is incomplete");
        }
        sourceArtifacts.requireExact(sourceTuple);
        List<AllocatorEvidenceAttachmentV1> ordered = Arrays.stream(AllocatorEvidenceAttachmentKindV1.values())
                .map(inventory::get)
                .toList();
        return new EvidenceInputs(sourceTuple, ordered, Map.copyOf(digests));
    }

    /** Returns the exact five NAEA1 paths, in wire-kind order, for one formal evidence directory. */
    public static List<Path> canonicalAttachmentPaths(Path evidenceDirectory) {
        Objects.requireNonNull(evidenceDirectory, "evidenceDirectory");
        Path exactDirectory = evidenceDirectory.toAbsolutePath().normalize();
        return Arrays.stream(AllocatorEvidenceAttachmentKindV1.values())
                .map(kind -> exactDirectory.resolve(kind.fileName()))
                .toList();
    }

    private static Path requireReceiptPath(Path receipt, List<Path> attachmentFiles) {
        Objects.requireNonNull(receipt, "receipt");
        Objects.requireNonNull(attachmentFiles, "canonicalAttachmentFiles");
        if (attachmentFiles.isEmpty()) {
            throw invalid("allocator selection receipt has no evidence directory authority");
        }
        Path exactReceipt = receipt.toAbsolutePath().normalize();
        Path firstAttachment = Objects.requireNonNull(attachmentFiles.get(0), "canonicalAttachmentFile")
                .toAbsolutePath()
                .normalize();
        if (exactReceipt.getFileName() == null
                || !RECEIPT_FILE_NAME.equals(exactReceipt.getFileName().toString())
                || exactReceipt.getParent() == null
                || !exactReceipt.getParent().equals(firstAttachment.getParent())) {
            throw invalid("allocator selection receipt path must be selection.nars beside the five NAEA1 files");
        }
        return exactReceipt;
    }

    private static ParsedReceipt parseReceipt(CanonicalBytes canonical) {
        if (canonical.length() != RECEIPT_BYTES) {
            throw invalid("allocator selection receipt length differs from fixed NARS1");
        }
        ByteBuffer input = ByteBuffer.wrap(canonical.toByteArray());
        byte[] magic = new byte[MAGIC.length];
        input.get(magic);
        if (!Arrays.equals(magic, MAGIC) || Short.toUnsignedInt(input.getShort()) != SCHEMA_VERSION) {
            throw invalid("allocator selection receipt magic/schema differs");
        }
        AllocatorModeV1 mode = AllocatorModeV1.fromCode(Short.toUnsignedInt(input.getShort()));
        int protocol = input.getInt();
        int flags = input.getInt();
        long range = input.getLong();
        int brokers = input.getInt();
        int requiredCuts = Short.toUnsignedInt(input.getShort());
        int completedCuts = Short.toUnsignedInt(input.getShort());
        AllocatorEvidenceSourceTupleV1 sourceTuple = readSourceTuple(input);
        EnumMap<AllocatorEvidenceAttachmentKindV1, Sha256Digest> attachments =
                new EnumMap<>(AllocatorEvidenceAttachmentKindV1.class);
        for (AllocatorEvidenceAttachmentKindV1 kind : AllocatorEvidenceAttachmentKindV1.values()) {
            attachments.put(kind, AllocatorEvidenceAttachmentV1.readDigest(input));
        }
        int metricCount = input.getInt();
        if (protocol != VirtualLedgerCellAllocatorStateV1.PROTOCOL_VERSION
                || flags != COMPLETE_EVIDENCE_FLAGS
                || brokers != BROKER_COUNT
                || requiredCuts != REQUIRED_FAULT_CUTS
                || completedCuts != REQUIRED_FAULT_CUTS
                || metricCount != METRIC_COUNT
                || (mode == AllocatorModeV1.STRICT_SERIALIZED && range != 1)
                || (mode == AllocatorModeV1.RANGE_LEASED
                        && !AllocatorEvidenceCandidateV1.RANGE_SIZES.contains(range))) {
            throw invalid("allocator selection receipt eligibility/version/range matrix differs");
        }
        List<AllocatorEvidenceWorkloadV1> expected = AllocatorEvidenceWorkloadV1.completeMatrix(mode, range, brokers);
        List<AllocatorNativeRelativeMetricsV1> metrics = new ArrayList<>(METRIC_COUNT);
        for (int index = 0; index < METRIC_COUNT; index++) {
            AllocatorNativeRelativeMetricsV1 metric = readMetric(input, mode, range);
            if (!metric.workload().equals(expected.get(index))) {
                throw invalid("allocator selection receipt metric dimensions/order differ");
            }
            metrics.add(metric);
        }
        AllocatorEvidenceAttachmentV1.requireZero(input, RESERVED_BYTES);
        if (input.hasRemaining()) {
            throw invalid("allocator selection receipt has trailing bytes");
        }
        return new ParsedReceipt(mode, range, sourceTuple, Map.copyOf(attachments), List.copyOf(metrics));
    }

    private static CanonicalBytes encode(AllocatorEvidenceEvaluationV1 evaluation) {
        AllocatorEvidenceCandidateV1 selected = evaluation.selectedCandidate().orElseThrow();
        ByteBuffer output = ByteBuffer.allocate(RECEIPT_BYTES);
        output.put(MAGIC)
                .putShort((short) SCHEMA_VERSION)
                .putShort((short) selected.mode().code())
                .putInt(VirtualLedgerCellAllocatorStateV1.PROTOCOL_VERSION)
                .putInt(COMPLETE_EVIDENCE_FLAGS)
                .putLong(selected.rangeSize())
                .putInt(BROKER_COUNT)
                .putShort((short) REQUIRED_FAULT_CUTS)
                .putShort((short) REQUIRED_FAULT_CUTS);
        writeSourceTuple(output, evaluation.sourceTuple());
        for (AllocatorEvidenceAttachmentKindV1 kind : AllocatorEvidenceAttachmentKindV1.values()) {
            AllocatorEvidenceAttachmentV1.putDigest(
                    output, Objects.requireNonNull(evaluation.attachmentSha256().get(kind), "attachment digest"));
        }
        output.putInt(evaluation.selectedRows().size());
        evaluation.selectedRows().forEach(metric -> writeMetric(output, metric));
        output.put(new byte[RESERVED_BYTES]);
        if (output.hasRemaining()) {
            throw new IllegalStateException("allocator selection receipt encoder did not fill fixed NARS1");
        }
        return CanonicalBytes.copyOf(output.array());
    }

    private static void writeSourceTuple(ByteBuffer output, AllocatorEvidenceSourceTupleV1 tuple) {
        putCommit(output, tuple.nereusSourceCommit());
        putCommit(output, tuple.pulsarSourceCommit());
        putCommit(output, tuple.oxiaClientSourceCommit());
        putCommit(output, tuple.oxiaServerSourceCommit());
        AllocatorEvidenceAttachmentV1.putDigest(output, tuple.oxiaClientJarSha256());
        AllocatorEvidenceAttachmentV1.putDigest(output, tuple.testedEvidenceArtifactSha256());
        AllocatorEvidenceAttachmentV1.putDigest(output, tuple.runtimeDomainArtifactSha256());
        AllocatorEvidenceAttachmentV1.putDigest(output, tuple.runtimeMetadataSpiArtifactSha256());
        AllocatorEvidenceAttachmentV1.putDigest(output, tuple.runtimeMetadataOxiaArtifactSha256());
        AllocatorEvidenceAttachmentV1.putDigest(output, tuple.sourceLocksSha256());
        AllocatorEvidenceAttachmentV1.putDigest(output, tuple.executorManifestSha256());
    }

    private static AllocatorEvidenceSourceTupleV1 readSourceTuple(ByteBuffer input) {
        return new AllocatorEvidenceSourceTupleV1(
                AllocatorEvidenceAttachmentV1.readCommit(input),
                AllocatorEvidenceAttachmentV1.readCommit(input),
                AllocatorEvidenceAttachmentV1.readCommit(input),
                AllocatorEvidenceAttachmentV1.readCommit(input),
                AllocatorEvidenceAttachmentV1.readDigest(input),
                AllocatorEvidenceAttachmentV1.readDigest(input),
                AllocatorEvidenceAttachmentV1.readDigest(input),
                AllocatorEvidenceAttachmentV1.readDigest(input),
                AllocatorEvidenceAttachmentV1.readDigest(input),
                AllocatorEvidenceAttachmentV1.readDigest(input),
                AllocatorEvidenceAttachmentV1.readDigest(input));
    }

    private static void writeMetric(ByteBuffer output, AllocatorNativeRelativeMetricsV1 metric) {
        output.putInt(metric.workload().activeManagedLedgers())
                .putInt(metric.workload().brokerCount())
                .putInt(metric.workload().metadataLatencyP99Millis())
                .putInt(0)
                .putDouble(metric.sustainableRolloverRequestsPerSecond())
                .putDouble(metric.nativeRolloverRequestsPerSecond())
                .putLong(metric.rolloverEndToEndP99Micros())
                .putLong(metric.oxiaOperationP99Micros())
                .putLong(metric.queueDepthMaximum())
                .putLong(metric.queueAgeP99Micros())
                .putLong(metric.topicStarvationMaximumMicros())
                .putLong(metric.cellAppendStallP99Micros())
                .putLong(metric.nativeCellAppendStallP99Micros())
                .putLong(metric.takeoverRecoveryP99Micros())
                .putLong(metric.successfulOperations())
                .putLong(metric.fencedOperations())
                .putLong(metric.errorOperations())
                .putLong(metric.timedOutOperations())
                .putLong(metric.permanentOrphans())
                .putLong(metric.duplicateLedgerIds())
                .putLong(metric.reusedLedgerIds())
                .putLong(metric.failedAssertions())
                .putLong(metric.skippedAssertions())
                .putLong(metric.unexpectedErrors())
                .putLong(metric.metadataOperationCalls())
                .putLong(metric.metadataRequestBytes())
                .putLong(metric.metadataResponseBytes())
                .putLong(metric.grantUseOperations())
                .putLong(metric.grantWasteIds())
                .putLong(metric.staleCandidateBurns());
    }

    private static AllocatorNativeRelativeMetricsV1 readMetric(ByteBuffer input, AllocatorModeV1 mode, long range) {
        int population = input.getInt();
        int brokers = input.getInt();
        int latency = input.getInt();
        if (input.getInt() != 0) {
            throw invalid("allocator selection metric reserved field is non-zero");
        }
        return new AllocatorNativeRelativeMetricsV1(
                new AllocatorEvidenceWorkloadV1(mode, range, population, brokers, latency),
                input.getDouble(),
                input.getDouble(),
                input.getLong(),
                input.getLong(),
                input.getLong(),
                input.getLong(),
                input.getLong(),
                input.getLong(),
                input.getLong(),
                input.getLong(),
                input.getLong(),
                input.getLong(),
                input.getLong(),
                input.getLong(),
                input.getLong(),
                input.getLong(),
                input.getLong(),
                input.getLong(),
                input.getLong(),
                input.getLong(),
                input.getLong(),
                input.getLong(),
                input.getLong(),
                input.getLong(),
                input.getLong(),
                input.getLong());
    }

    private static void putCommit(ByteBuffer output, String value) {
        byte[] bytes;
        try {
            bytes = java.util.HexFormat.of().parseHex(value);
        } catch (IllegalArgumentException error) {
            throw invalid("allocator selection source commit is non-canonical");
        }
        if (bytes.length != 20) {
            throw invalid("allocator selection source commit length differs");
        }
        output.put(bytes);
    }

    public AllocatorModeV1 selectedMode() {
        return selectedMode;
    }

    public int allocatorProtocolVersion() {
        return allocatorProtocolVersion;
    }

    public long selectedRangeSize() {
        return selectedRangeSize;
    }

    public AllocatorEvidenceSourceTupleV1 sourceTuple() {
        return sourceTuple;
    }

    public String nereusSourceCommit() {
        return sourceTuple.nereusSourceCommit();
    }

    public String pulsarSourceCommit() {
        return sourceTuple.pulsarSourceCommit();
    }

    public String oxiaSourceCommit() {
        return sourceTuple.oxiaClientSourceCommit();
    }

    public Sha256Digest exactArtifactSha256() {
        return sourceTuple.runtimeDomainArtifactSha256();
    }

    public Sha256Digest sourceLocksSha256() {
        return sourceTuple.sourceLocksSha256();
    }

    public Sha256Digest evidenceReceiptSha256() {
        return evidenceReceiptSha256;
    }

    public List<AllocatorNativeRelativeMetricsV1> completeScaleMetrics() {
        return completeScaleMetrics;
    }

    public Map<AllocatorEvidenceAttachmentKindV1, Sha256Digest> attachmentSha256() {
        return attachmentSha256;
    }

    static AllocatorProtocolException invalid(String message) {
        return new AllocatorProtocolException(AllocatorProtocolException.Code.SELECTION_NOT_ELIGIBLE, message);
    }

    private record EvidenceInputs(
            AllocatorEvidenceSourceTupleV1 sourceTuple,
            List<AllocatorEvidenceAttachmentV1> attachments,
            Map<AllocatorEvidenceAttachmentKindV1, Sha256Digest> digests) {}

    private record ParsedReceipt(
            AllocatorModeV1 mode,
            long range,
            AllocatorEvidenceSourceTupleV1 sourceTuple,
            Map<AllocatorEvidenceAttachmentKindV1, Sha256Digest> attachments,
            List<AllocatorNativeRelativeMetricsV1> metrics) {}
}
