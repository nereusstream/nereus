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

package com.nereusstream.kafka.bookkeeper.compaction;

import com.nereusstream.domain.bytes.CanonicalBytes;
import com.nereusstream.domain.bytes.Sha256Digest;
import com.nereusstream.kafka.bookkeeper.compaction.KafkaCompactionRecordsV1.CompactionPlan;
import com.nereusstream.kafka.bookkeeper.compaction.KafkaCompactionRecordsV1.ControlKind;
import com.nereusstream.kafka.bookkeeper.compaction.KafkaCompactionRecordsV1.Disposition;
import com.nereusstream.kafka.bookkeeper.compaction.KafkaCompactionRecordsV1.DispositionRow;
import com.nereusstream.kafka.bookkeeper.compaction.KafkaCompactionRecordsV1.Gap;
import com.nereusstream.kafka.bookkeeper.compaction.KafkaCompactionRecordsV1.LatestKeyProof;
import com.nereusstream.kafka.bookkeeper.compaction.KafkaCompactionRecordsV1.ParsedBatch;
import com.nereusstream.kafka.bookkeeper.compaction.KafkaCompactionRecordsV1.RecordValue;
import com.nereusstream.kafka.bookkeeper.compaction.KafkaCompactionRecordsV1.TransactionOutcome;
import com.nereusstream.storage.object.materialization.M5MaterializationCodecV1;
import com.nereusstream.storage.object.materialization.M5MaterializationObjectSessionV1.Candidate;
import com.nereusstream.storage.object.materialization.M5MaterializationRecordsV1.GenerationObject;
import com.nereusstream.storage.object.materialization.M5MaterializationRecordsV1.IndexKind;
import com.nereusstream.storage.object.materialization.M5MaterializationRecordsV1.IndexPlan;
import com.nereusstream.storage.object.materialization.M5MaterializationRecordsV1.MaterializationPlan;
import com.nereusstream.storage.object.materialization.M5MaterializationRecordsV1.OutputPartPlan;
import com.nereusstream.storage.object.materialization.M5MaterializationRecordsV1.PayloadKind;
import com.nereusstream.storage.object.materialization.M5MaterializationRecordsV1.PositionDomain;
import com.nereusstream.storage.object.materialization.M5MaterializationRecordsV1.ProtocolCoverage;
import com.nereusstream.storage.object.materialization.M5MaterializationRecordsV1.RepresentationMode;
import com.nereusstream.storage.object.materialization.M5MaterializationValidatorV1.SemanticValidationProof;
import com.nereusstream.storage.object.materialization.Nms1CodecV1;
import com.nereusstream.storage.object.materialization.Nms1ObjectV1;
import com.nereusstream.storage.object.provider.ObjectIdentity;
import java.io.ByteArrayOutputStream;
import java.io.IOException;
import java.nio.charset.StandardCharsets;
import java.util.ArrayList;
import java.util.Comparator;
import java.util.EnumMap;
import java.util.LinkedHashMap;
import java.util.List;
import java.util.Map;
import java.util.Objects;
import java.util.Optional;
import java.util.Set;

/** Deterministic, bounded Kafka semantic compactor producing one complete M5-B candidate generation. */
public final class KafkaSemanticCompactorV1 {
    private static final List<IndexKind> REQUIRED_INDEXES = List.of(IndexKind.values());

    public record BatchOutput(
            ParsedBatch input,
            Optional<ParsedBatch> output,
            Optional<CanonicalBytes> outputBody,
            int outputBatchOrdinal,
            int payloadOffset) {
        public BatchOutput {
            Objects.requireNonNull(input, "input");
            Objects.requireNonNull(output, "output");
            Objects.requireNonNull(outputBody, "outputBody");
            if (output.isPresent() != outputBody.isPresent()
                    || output.isPresent() != (outputBatchOrdinal >= 0)
                    || payloadOffset < 0) {
                throw new IllegalArgumentException("M5-B output batch descriptor is inconsistent");
            }
        }
    }

    public record CandidateGeneration(
            MaterializationPlan materializationPlan,
            Sha256Digest compactionPlanRootSha256,
            Sha256Digest compactionTaskIdSha256,
            List<ParsedBatch> inputBatches,
            List<BatchOutput> batchOutputs,
            List<DispositionRow> dispositions,
            List<Gap> gaps,
            List<KafkaCompactionIndexV1> indexes,
            Nms1ObjectV1 nms1,
            GenerationObject payloadObject,
            List<GenerationObject> indexObjects,
            Candidate payloadCandidate,
            List<Candidate> indexCandidates) {
        public CandidateGeneration {
            Objects.requireNonNull(materializationPlan, "materializationPlan");
            KafkaCompactionRecordsV1.requireDigest(compactionPlanRootSha256, "compactionPlanRootSha256");
            KafkaCompactionRecordsV1.requireDigest(compactionTaskIdSha256, "compactionTaskIdSha256");
            inputBatches = List.copyOf(inputBatches);
            batchOutputs = List.copyOf(batchOutputs);
            dispositions = List.copyOf(dispositions);
            gaps = List.copyOf(gaps);
            indexes = List.copyOf(indexes);
            Objects.requireNonNull(nms1, "nms1");
            Objects.requireNonNull(payloadObject, "payloadObject");
            indexObjects = List.copyOf(indexObjects);
            Objects.requireNonNull(payloadCandidate, "payloadCandidate");
            indexCandidates = List.copyOf(indexCandidates);
        }

        public List<ParsedBatch> outputBatches() {
            return batchOutputs.stream()
                    .flatMap(value -> value.output().stream())
                    .toList();
        }

        public List<CanonicalBytes> indexBodies() {
            return indexCandidates.stream().map(Candidate::canonicalBody).toList();
        }
    }

    public record Result(CandidateGeneration candidate, SemanticValidationProof semanticProof) {
        public Result {
            Objects.requireNonNull(candidate, "candidate");
            Objects.requireNonNull(semanticProof, "semanticProof");
        }
    }

    public Result compact(CompactionPlan plan) {
        Objects.requireNonNull(plan, "plan");
        List<ParsedBatch> inputs = plan.inputBatches().stream()
                .map(value -> KafkaRecordBatchCodecV1.parse(value.canonicalBody()))
                .toList();
        requireInputCut(plan, inputs);
        MaterializationPlan materialization = materializationPlan(plan);
        Map<String, LatestKeyProof> proofs = new LinkedHashMap<>();
        long keyBytes = 0;
        for (LatestKeyProof proof : plan.keyProofs()) {
            proofs.put(proof.key().toHex(), proof);
            keyBytes = Math.addExact(keyBytes, proof.key().length());
        }
        if (keyBytes > plan.policy().caps().maximumKeyBytes()) {
            throw new IllegalArgumentException("M5-B key bytes exceed the Cell cap");
        }
        Set<Long> undecidable = Set.copyOf(plan.undecidableOffsets());
        Set<Long> recoveryRequired = Set.copyOf(plan.recoveryRequiredOffsets());
        List<DispositionRow> dispositions = new ArrayList<>();
        List<BatchOutput> outputs = new ArrayList<>();
        int outputOrdinal = 0;
        int payloadOffset = 0;
        int tombstones = 0;
        for (ParsedBatch input : inputs) {
            List<RecordValue> retained = new ArrayList<>();
            int dispositionStart = dispositions.size();
            for (RecordValue record : input.records()) {
                if (record.tombstone()) {
                    tombstones = Math.addExact(tombstones, 1);
                }
                Disposition disposition = select(plan, input, record, proofs, undecidable, recoveryRequired);
                DispositionRow row = new DispositionRow(
                        record.offset(),
                        input.bodySha256(),
                        disposition,
                        KafkaCompactionCanonicalV1.recordIdentity(input, record));
                dispositions.add(row);
                if (row.retained()) {
                    retained.add(record);
                }
            }
            if (!KafkaRecordBatchCodecV1.canRewriteSubset(input, retained)) {
                retained = input.records();
                for (int index = dispositionStart; index < dispositions.size(); index++) {
                    DispositionRow previous = dispositions.get(index);
                    dispositions.set(
                            index,
                            new DispositionRow(
                                    previous.offset(),
                                    previous.inputBatchSha256(),
                                    Disposition.RETAIN_UNKNOWN,
                                    previous.recordIdentitySha256()));
                }
            }
            Optional<CanonicalBytes> outputBody = KafkaRecordBatchCodecV1.rewrite(
                    input, retained, plan.policy().outputCompression());
            Optional<ParsedBatch> output = outputBody.map(KafkaRecordBatchCodecV1::parse);
            outputs.add(
                    new BatchOutput(input, output, outputBody, output.isPresent() ? outputOrdinal : -1, payloadOffset));
            if (outputBody.isPresent()) {
                outputOrdinal++;
                payloadOffset =
                        Math.addExact(payloadOffset, outputBody.orElseThrow().length());
            }
        }
        if (tombstones > plan.policy().caps().maximumTombstones()
                || payloadOffset > plan.policy().caps().maximumOutputBytes()) {
            throw new IllegalArgumentException("M5-B tombstone/output bytes exceed the Cell cap");
        }
        dispositions = dispositions.stream()
                .sorted(Comparator.comparingLong(DispositionRow::offset))
                .toList();
        requireDispositionDomain(inputs, dispositions);
        List<Gap> gaps = gaps(plan, outputs);
        List<KafkaCompactionIndexV1> indexes = indexes(plan, materialization, outputs, gaps);
        long indexBytes = indexes.stream()
                .map(KafkaCompactionIndexV1::encode)
                .mapToLong(CanonicalBytes::length)
                .sum();
        if (indexBytes > plan.policy().caps().maximumIndexBytes()) {
            throw new IllegalArgumentException("M5-B rebuilt index bytes exceed the Cell cap");
        }
        CandidateGeneration candidate = generation(
                plan,
                materialization,
                inputs,
                outputs,
                dispositions,
                gaps,
                indexes,
                KafkaCompactionCanonicalV1.planRoot(plan));
        SemanticValidationProof proof = new KafkaCompactionSemanticValidatorV1().validate(plan, candidate);
        return new Result(candidate, proof);
    }

    private static MaterializationPlan materializationPlan(CompactionPlan plan) {
        Sha256Digest planRoot = KafkaCompactionCanonicalV1.planRoot(plan);
        Sha256Digest taskId = M5MaterializationCodecV1.calculateTaskId(plan.sourceCut());
        Sha256Digest compactionTask = KafkaCompactionCanonicalV1.compactionTaskId(taskId, planRoot);
        String prefix = "m5/kafka-compaction/" + compactionTask.toHex();
        ProtocolCoverage coverage = plan.sourceCut().coverage();
        List<OutputPartPlan> parts = List.of(new OutputPartPlan(
                0,
                coverage,
                PayloadKind.KAFKA_SEMANTIC_COMPACTED_V1,
                hash("M5-B-PART-V1", compactionTask.toHex()),
                prefix + "/payload/000000.nms1"));
        List<IndexPlan> indexes = REQUIRED_INDEXES.stream()
                .map(kind -> new IndexPlan(
                        kind,
                        coverage,
                        1,
                        hash("M5-B-INDEX-PLAN-V1", compactionTask.toHex(), kind.name()),
                        String.format(java.util.Locale.ROOT, "%s/index/%02d.k5i1", prefix, kind.ordinal())))
                .toList();
        Sha256Digest outputIdentity = M5MaterializationCodecV1.calculateOutputIdentity(
                plan.sourceCut(),
                RepresentationMode.REWRITE_GENERATION,
                PayloadKind.KAFKA_SEMANTIC_COMPACTED_V1,
                taskId,
                plan.sourceCut().identity().providerScopeSha256(),
                hash(
                        "M5-B-COMPRESSION-POLICY-V1",
                        plan.policy().outputCompression().name()),
                hash("M5-B-CHECKSUM-POLICY-V1", "KAFKA_CRC32C_AND_FULL_SHA256"),
                parts,
                indexes);
        return new MaterializationPlan(
                plan.sourceCut(),
                RepresentationMode.REWRITE_GENERATION,
                PayloadKind.KAFKA_SEMANTIC_COMPACTED_V1,
                taskId,
                outputIdentity,
                plan.sourceCut().identity().providerScopeSha256(),
                hash(
                        "M5-B-COMPRESSION-POLICY-V1",
                        plan.policy().outputCompression().name()),
                hash("M5-B-CHECKSUM-POLICY-V1", "KAFKA_CRC32C_AND_FULL_SHA256"),
                parts,
                indexes);
    }

    private static Disposition select(
            CompactionPlan plan,
            ParsedBatch batch,
            RecordValue record,
            Map<String, LatestKeyProof> proofs,
            Set<Long> undecidable,
            Set<Long> recoveryRequired) {
        if (batch.transactional() || batch.controlKind() != ControlKind.NONE) {
            return Disposition.KEEP_TRANSACTION_OR_CONTROL;
        }
        if (undecidable.contains(record.offset()) || recoveryRequired.contains(record.offset())) {
            return Disposition.RETAIN_UNKNOWN;
        }
        if (record.key().isEmpty()) {
            return Disposition.KEEP_NULL_KEY;
        }
        LatestKeyProof proof = proofs.get(record.key().orElseThrow().toHex());
        if (proof == null || !proof.completeDomain() || !proof.latestCommitted()) {
            return Disposition.RETAIN_UNKNOWN;
        }
        if (proof.latestEligibleOffset() < record.offset()) {
            return Disposition.RETAIN_UNKNOWN;
        }
        if (proof.latestEligibleOffset() > record.offset()) {
            return Disposition.DROP_SUPERSEDED_VALUE;
        }
        if (!record.tombstone()) {
            return Disposition.KEEP_KEY_LATEST;
        }
        if (plan.policy().capturedNowMs() < proof.tombstoneDeadlineMs() || proof.olderValueMayReappear()) {
            return Disposition.KEEP_TOMBSTONE_WITHIN_RETENTION;
        }
        return Disposition.DROP_EXPIRED_TOMBSTONE;
    }

    private static void requireInputCut(CompactionPlan plan, List<ParsedBatch> inputs) {
        long bytes = 0;
        int records = 0;
        long previousEnd = plan.frontiers().candidateStartOffset();
        for (int index = 0; index < inputs.size(); index++) {
            ParsedBatch batch = inputs.get(index);
            if (batch.baseOffset() < previousEnd
                    || batch.baseOffset() < plan.frontiers().candidateStartOffset()
                    || batch.endOffsetExclusive() > plan.frontiers().candidateEndOffsetExclusive()) {
                throw new IllegalArgumentException("M5-B input batches overlap, regress, or escape the candidate cut");
            }
            previousEnd = batch.endOffsetExclusive();
            bytes = Math.addExact(bytes, batch.canonicalBody().length());
            records = Math.addExact(records, batch.records().size());
            boolean epochCovered = plan.leaderEpochs().stream()
                    .anyMatch(epoch -> epoch.leaderEpoch() == batch.partitionLeaderEpoch()
                            && epoch.startOffset() <= batch.baseOffset()
                            && epoch.endOffsetExclusive() >= batch.endOffsetExclusive());
            if (!epochCovered) {
                throw new IllegalArgumentException("M5-B input batch is not covered by the leader-epoch root");
            }
        }
        if (bytes > plan.policy().caps().maximumDirtyBytes()
                || records > plan.policy().caps().maximumRecords()) {
            throw new IllegalArgumentException("M5-B dirty bytes/record count exceed the Cell cap");
        }
    }

    private static void requireDispositionDomain(List<ParsedBatch> inputs, List<DispositionRow> rows) {
        List<Long> expected = inputs.stream()
                .flatMap(batch -> batch.records().stream())
                .map(RecordValue::offset)
                .sorted()
                .toList();
        List<Long> actual = rows.stream().map(DispositionRow::offset).toList();
        if (!actual.equals(expected) || actual.stream().distinct().count() != actual.size()) {
            throw new IllegalStateException("M5-B disposition root does not bind every input record exactly once");
        }
    }

    private static List<Gap> gaps(CompactionPlan plan, List<BatchOutput> outputs) {
        List<Long> retained = outputs.stream()
                .flatMap(value -> value.output().stream())
                .flatMap(value -> value.records().stream())
                .map(RecordValue::offset)
                .sorted()
                .toList();
        List<Gap> result = new ArrayList<>();
        long cursor = plan.frontiers().candidateStartOffset();
        for (long offset : retained) {
            if (cursor < offset) {
                result.add(new Gap(cursor, offset));
            }
            cursor = Math.addExact(offset, 1);
        }
        if (cursor < plan.frontiers().candidateEndOffsetExclusive()) {
            result.add(new Gap(cursor, plan.frontiers().candidateEndOffsetExclusive()));
        }
        return List.copyOf(result);
    }

    private static List<KafkaCompactionIndexV1> indexes(
            CompactionPlan plan, MaterializationPlan materialization, List<BatchOutput> outputs, List<Gap> gaps) {
        Map<IndexKind, List<KafkaCompactionIndexV1.Row>> rows = new EnumMap<>(IndexKind.class);
        for (IndexKind kind : REQUIRED_INDEXES) {
            rows.put(kind, new ArrayList<>());
        }
        for (BatchOutput output : outputs) {
            if (output.output().isEmpty()) {
                continue;
            }
            ParsedBatch batch = output.output().orElseThrow();
            boolean aborted = plan.transactions().stream()
                    .anyMatch(transaction -> transaction.outcome() == TransactionOutcome.ABORTED
                            && transaction.touches(batch.baseOffset(), batch.endOffsetExclusive()));
            for (RecordValue record : batch.records()) {
                int flags = KafkaCompactionIndexV1.FLAG_RETAINED;
                if (batch.transactional()) {
                    flags |= KafkaCompactionIndexV1.FLAG_TRANSACTIONAL;
                }
                if (batch.controlKind() != ControlKind.NONE) {
                    flags |= KafkaCompactionIndexV1.FLAG_CONTROL;
                }
                if (aborted) {
                    flags |= KafkaCompactionIndexV1.FLAG_ABORTED;
                }
                KafkaCompactionIndexV1.Row row = new KafkaCompactionIndexV1.Row(
                        new ProtocolCoverage(PositionDomain.KAFKA_OFFSET, record.offset(), record.offset() + 1),
                        output.outputBatchOrdinal(),
                        output.payloadOffset(),
                        output.outputBody().orElseThrow().length(),
                        record.timestamp(),
                        record.timestamp(),
                        batch.producerId(),
                        batch.producerEpoch(),
                        record.sequence(),
                        batch.partitionLeaderEpoch(),
                        flags,
                        KafkaCompactionCanonicalV1.recordIdentity(batch, record));
                rows.get(IndexKind.OFFSET_OR_POSITION).add(row);
                rows.get(IndexKind.PAYLOAD_LOCATOR).add(row);
                rows.get(IndexKind.TIMESTAMP).add(row);
                rows.get(IndexKind.LEADER_EPOCH).add(row);
                rows.get(IndexKind.CHECKSUM_COVERAGE).add(row);
                if (batch.producerId() != -1) {
                    rows.get(IndexKind.PRODUCER_RECOVERY).add(row);
                }
                if (batch.transactional()) {
                    rows.get(IndexKind.TRANSACTION).add(row);
                }
                if (aborted) {
                    rows.get(IndexKind.ABORTED_TRANSACTION).add(row);
                }
            }
        }
        List<KafkaCompactionIndexV1.Row> checksum = rows.get(IndexKind.CHECKSUM_COVERAGE);
        for (Gap gap : gaps) {
            checksum.add(new KafkaCompactionIndexV1.Row(
                    new ProtocolCoverage(PositionDomain.KAFKA_OFFSET, gap.inclusiveStart(), gap.exclusiveEnd()),
                    -1,
                    0,
                    0,
                    -1,
                    -1,
                    -1,
                    (short) -1,
                    -1,
                    -1,
                    KafkaCompactionIndexV1.FLAG_GAP,
                    hash("M5-B-GAP-V1", Long.toString(gap.inclusiveStart()), Long.toString(gap.exclusiveEnd()))));
        }
        for (List<KafkaCompactionIndexV1.Row> value : rows.values()) {
            value.sort(Comparator.comparingLong(row -> row.coverage().inclusiveStart()));
        }
        return REQUIRED_INDEXES.stream()
                .map(kind -> new KafkaCompactionIndexV1(
                        kind,
                        plan.sourceCut().coverage(),
                        materialization.taskIdSha256(),
                        materialization.outputIdentitySha256(),
                        rows.get(kind)))
                .toList();
    }

    private static CandidateGeneration generation(
            CompactionPlan plan,
            MaterializationPlan materialization,
            List<ParsedBatch> inputs,
            List<BatchOutput> outputs,
            List<DispositionRow> dispositions,
            List<Gap> gaps,
            List<KafkaCompactionIndexV1> indexes,
            Sha256Digest planRoot) {
        CanonicalBytes payload = concatenate(
                outputs.stream().flatMap(value -> value.outputBody().stream()).toList());
        List<Nms1ObjectV1.ExtentRow> extents = extentRows(plan, outputs);
        List<Nms1ObjectV1.SourceContribution> sources = plan.sourceCut().sources().stream()
                .sorted(Comparator.comparing(
                        value -> value.sourceIdentitySha256().toHex()))
                .map(source -> new Nms1ObjectV1.SourceContribution(
                        source.sourceIdentitySha256(), source.coverage(), source.bodySha256()))
                .toList();
        Nms1ObjectV1 nms1 = new Nms1ObjectV1(
                plan.sourceCut().identity(),
                PayloadKind.KAFKA_SEMANTIC_COMPACTED_V1,
                materialization.taskIdSha256(),
                materialization.outputIdentitySha256(),
                plan.sourceCut().coverage(),
                0,
                1,
                materialization.encryptionGenerationSha256(),
                materialization.compressionPolicySha256(),
                materialization.checksumPolicySha256(),
                sources,
                extents,
                payload,
                List.of());
        CanonicalBytes nms1Body = Nms1CodecV1.encode(nms1);
        GenerationObject payloadObject = object(
                0,
                null,
                plan.sourceCut().coverage(),
                materialization.outputParts().get(0).objectKey(),
                nms1Body);
        List<GenerationObject> indexObjects = new ArrayList<>();
        List<Candidate> indexCandidates = new ArrayList<>();
        for (int ordinal = 0; ordinal < indexes.size(); ordinal++) {
            KafkaCompactionIndexV1 index = indexes.get(ordinal);
            CanonicalBytes body = index.encode();
            GenerationObject descriptor = object(
                    ordinal,
                    index.kind(),
                    plan.sourceCut().coverage(),
                    materialization.indexes().get(ordinal).objectKey(),
                    body);
            indexObjects.add(descriptor);
            indexCandidates.add(new Candidate(descriptor, body));
        }
        return new CandidateGeneration(
                materialization,
                planRoot,
                KafkaCompactionCanonicalV1.compactionTaskId(materialization.taskIdSha256(), planRoot),
                inputs,
                outputs,
                dispositions,
                gaps,
                indexes,
                nms1,
                payloadObject,
                indexObjects,
                new Candidate(payloadObject, nms1Body),
                indexCandidates);
    }

    private static List<Nms1ObjectV1.ExtentRow> extentRows(CompactionPlan plan, List<BatchOutput> outputs) {
        List<Nms1ObjectV1.ExtentRow> result = new ArrayList<>();
        long cursor = plan.frontiers().candidateStartOffset();
        int payloadOffset = 0;
        for (BatchOutput output : outputs) {
            if (cursor < output.input().baseOffset()) {
                result.add(gapExtent(cursor, output.input().baseOffset(), payloadOffset));
            }
            int length = output.outputBody().map(CanonicalBytes::length).orElse(0);
            List<RecordValue> records =
                    output.output().map(ParsedBatch::records).orElse(List.of());
            long minimumTimestamp =
                    records.stream().mapToLong(RecordValue::timestamp).min().orElse(-1);
            long maximumTimestamp =
                    records.stream().mapToLong(RecordValue::timestamp).max().orElse(-1);
            int flags = 0;
            if (output.input().transactional()) {
                flags |= 1;
            }
            if (output.input().controlKind() != ControlKind.NONE) {
                flags |= 2;
            }
            if (output.input().compressionType() != org.apache.kafka.common.record.CompressionType.NONE) {
                flags |= 4;
            }
            result.add(new Nms1ObjectV1.ExtentRow(
                    new ProtocolCoverage(
                            PositionDomain.KAFKA_OFFSET,
                            output.input().baseOffset(),
                            output.input().endOffsetExclusive()),
                    payloadOffset,
                    length,
                    records.size(),
                    minimumTimestamp,
                    maximumTimestamp,
                    Sha256Digest.hash(output.outputBody().orElse(CanonicalBytes.empty())),
                    flags));
            payloadOffset = Math.addExact(payloadOffset, length);
            cursor = output.input().endOffsetExclusive();
        }
        if (cursor < plan.frontiers().candidateEndOffsetExclusive()) {
            result.add(gapExtent(cursor, plan.frontiers().candidateEndOffsetExclusive(), payloadOffset));
        }
        return List.copyOf(result);
    }

    private static Nms1ObjectV1.ExtentRow gapExtent(long start, long end, int payloadOffset) {
        return new Nms1ObjectV1.ExtentRow(
                new ProtocolCoverage(PositionDomain.KAFKA_OFFSET, start, end),
                payloadOffset,
                0,
                0,
                -1,
                -1,
                Sha256Digest.hash(CanonicalBytes.empty()),
                0);
    }

    private static GenerationObject object(
            int ordinal, IndexKind kind, ProtocolCoverage coverage, String key, CanonicalBytes body) {
        return new GenerationObject(
                ordinal,
                kind,
                coverage,
                new ObjectIdentity(key, body.length(), Sha256Digest.hash(body)),
                Optional.empty());
    }

    private static CanonicalBytes concatenate(List<CanonicalBytes> values) {
        try {
            ByteArrayOutputStream output = new ByteArrayOutputStream();
            for (CanonicalBytes value : values) {
                output.write(value.toByteArray());
            }
            return CanonicalBytes.copyOf(output.toByteArray());
        } catch (IOException impossible) {
            throw new IllegalStateException("in-memory M5-B payload concatenation failed", impossible);
        }
    }

    private static Sha256Digest hash(String domain, String... values) {
        String body = domain + "\0" + String.join("\0", values);
        return Sha256Digest.hash(CanonicalBytes.copyOf(body.getBytes(StandardCharsets.UTF_8)));
    }
}
