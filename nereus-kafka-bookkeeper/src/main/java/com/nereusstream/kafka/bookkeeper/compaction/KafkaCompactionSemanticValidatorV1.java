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
import com.nereusstream.kafka.bookkeeper.compaction.KafkaCompactionRecordsV1.LatestKeyProof;
import com.nereusstream.kafka.bookkeeper.compaction.KafkaCompactionRecordsV1.ParsedBatch;
import com.nereusstream.kafka.bookkeeper.compaction.KafkaCompactionRecordsV1.RecordValue;
import com.nereusstream.storage.object.materialization.M5MaterializationRecordsV1.IndexKind;
import com.nereusstream.storage.object.materialization.M5MaterializationRecordsV1.PayloadKind;
import com.nereusstream.storage.object.materialization.M5MaterializationRecordsV1.RepresentationMode;
import com.nereusstream.storage.object.materialization.M5MaterializationValidatorV1;
import com.nereusstream.storage.object.materialization.M5MaterializationValidatorV1.SemanticValidationProof;
import com.nereusstream.storage.object.materialization.Nms1CodecV1;
import java.util.ArrayList;
import java.util.Comparator;
import java.util.EnumMap;
import java.util.HashMap;
import java.util.LinkedHashMap;
import java.util.List;
import java.util.Map;
import java.util.Objects;
import java.util.Set;
import java.util.stream.Collectors;

/** Independent full-record semantic and rebuilt-index validator for one M5-B candidate. */
public final class KafkaCompactionSemanticValidatorV1 {
    public SemanticValidationProof validate(
            CompactionPlan plan, KafkaSemanticCompactorV1.CandidateGeneration candidate) {
        Objects.requireNonNull(plan, "plan");
        Objects.requireNonNull(candidate, "candidate");
        Sha256Digest planRoot = KafkaCompactionCanonicalV1.planRoot(plan);
        Sha256Digest taskId = KafkaCompactionCanonicalV1.compactionTaskId(
                candidate.materializationPlan().taskIdSha256(), planRoot);
        if (!candidate.compactionPlanRootSha256().equals(planRoot)
                || !candidate.compactionTaskIdSha256().equals(taskId)
                || candidate.materializationPlan().representationMode() != RepresentationMode.REWRITE_GENERATION
                || candidate.materializationPlan().payloadKind() != PayloadKind.KAFKA_SEMANTIC_COMPACTED_V1
                || !candidate.materializationPlan().sourceCut().equals(plan.sourceCut())) {
            throw new IllegalStateException("M5-B candidate identity differs from the frozen compaction plan");
        }
        List<ParsedBatch> inputs = plan.inputBatches().stream()
                .map(value -> KafkaRecordBatchCodecV1.parse(value.canonicalBody()))
                .toList();
        if (!candidate.inputBatches().equals(inputs)) {
            throw new IllegalStateException("M5-B candidate input parse differs on independent reread");
        }
        List<DispositionRow> expected = expectedDispositions(plan, inputs);
        if (!candidate.dispositions().equals(expected)) {
            throw new IllegalStateException("M5-B disposition root differs from deterministic selection");
        }
        requireOutputEquivalence(plan, candidate, expected);
        requireNms1(candidate);
        requireIndexes(plan, candidate);

        Sha256Digest dispositionRoot = KafkaCompactionCanonicalV1.dispositionRoot(expected);
        Sha256Digest suppressionRoot = KafkaCompactionCanonicalV1.suppressionRoot(expected);
        Sha256Digest protocolRoot = KafkaCompactionCanonicalV1.protocolStateRoot(plan);
        Sha256Digest outputRecordRoot = KafkaCompactionCanonicalV1.outputRecordRoot(candidate.outputBatches());
        Sha256Digest payloadBodiesRoot = M5MaterializationValidatorV1.semanticPayloadBodiesRoot(
                List.of(candidate.nms1().payload()));
        Sha256Digest indexBodiesRoot = M5MaterializationValidatorV1.semanticIndexBodiesRoot(candidate.indexBodies());
        Sha256Digest semanticRoot = KafkaCompactionCanonicalV1.semanticValidationRoot(
                taskId,
                planRoot,
                dispositionRoot,
                protocolRoot,
                suppressionRoot,
                outputRecordRoot,
                payloadBodiesRoot,
                indexBodiesRoot);
        return new SemanticValidationProof(
                candidate.materializationPlan().taskIdSha256(),
                candidate.materializationPlan().outputIdentitySha256(),
                plan.sourceCut().sourceSetSha256(),
                semanticRoot,
                protocolRoot,
                suppressionRoot,
                payloadBodiesRoot,
                indexBodiesRoot);
    }

    private static List<DispositionRow> expectedDispositions(CompactionPlan plan, List<ParsedBatch> inputs) {
        Map<String, LatestKeyProof> proofs = plan.keyProofs().stream()
                .collect(Collectors.toMap(
                        value -> value.key().toHex(), value -> value, (left, right) -> left, LinkedHashMap::new));
        Set<Long> undecidable = Set.copyOf(plan.undecidableOffsets());
        Set<Long> recoveryRequired = Set.copyOf(plan.recoveryRequiredOffsets());
        List<DispositionRow> result = new ArrayList<>();
        for (ParsedBatch batch : inputs) {
            List<Disposition> selections = batch.records().stream()
                    .map(record -> select(plan, batch, record, proofs, undecidable, recoveryRequired))
                    .collect(java.util.stream.Collectors.toCollection(ArrayList::new));
            List<RecordValue> retained = new ArrayList<>();
            for (int index = 0; index < batch.records().size(); index++) {
                if (retained(selections.get(index))) {
                    retained.add(batch.records().get(index));
                }
            }
            if (!KafkaRecordBatchCodecV1.canRewriteSubset(batch, retained)) {
                selections.replaceAll(ignored -> Disposition.RETAIN_UNKNOWN);
            }
            for (int index = 0; index < batch.records().size(); index++) {
                RecordValue record = batch.records().get(index);
                result.add(new DispositionRow(
                        record.offset(),
                        batch.bodySha256(),
                        selections.get(index),
                        KafkaCompactionCanonicalV1.recordIdentity(batch, record)));
            }
        }
        return result.stream()
                .sorted(Comparator.comparingLong(DispositionRow::offset))
                .toList();
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
        if (proof == null
                || !proof.completeDomain()
                || !proof.latestCommitted()
                || proof.latestEligibleOffset() < record.offset()) {
            return Disposition.RETAIN_UNKNOWN;
        }
        if (proof.latestEligibleOffset() > record.offset()) {
            return Disposition.DROP_SUPERSEDED_VALUE;
        }
        if (!record.tombstone()) {
            return Disposition.KEEP_KEY_LATEST;
        }
        return plan.policy().capturedNowMs() < proof.tombstoneDeadlineMs() || proof.olderValueMayReappear()
                ? Disposition.KEEP_TOMBSTONE_WITHIN_RETENTION
                : Disposition.DROP_EXPIRED_TOMBSTONE;
    }

    private static boolean retained(Disposition value) {
        return switch (value) {
            case KEEP_KEY_LATEST,
                    KEEP_NULL_KEY,
                    KEEP_TOMBSTONE_WITHIN_RETENTION,
                    KEEP_TRANSACTION_OR_CONTROL,
                    RETAIN_UNKNOWN -> true;
            case DROP_SUPERSEDED_VALUE, DROP_EXPIRED_TOMBSTONE -> false;
        };
    }

    private static void requireOutputEquivalence(
            CompactionPlan plan,
            KafkaSemanticCompactorV1.CandidateGeneration candidate,
            List<DispositionRow> dispositions) {
        Map<Long, DispositionRow> byOffset =
                dispositions.stream().collect(Collectors.toMap(DispositionRow::offset, value -> value));
        if (candidate.batchOutputs().size() != candidate.inputBatches().size()) {
            throw new IllegalStateException("M5-B output batch cardinality differs from input");
        }
        List<Long> expectedOffsets = new ArrayList<>();
        List<Long> actualOffsets = new ArrayList<>();
        for (int ordinal = 0; ordinal < candidate.batchOutputs().size(); ordinal++) {
            var output = candidate.batchOutputs().get(ordinal);
            ParsedBatch input = candidate.inputBatches().get(ordinal);
            if (!output.input().equals(input)) {
                throw new IllegalStateException("M5-B output names the wrong input batch");
            }
            List<RecordValue> expectedRecords = input.records().stream()
                    .filter(record -> byOffset.get(record.offset()).retained())
                    .toList();
            expectedOffsets.addAll(
                    expectedRecords.stream().map(RecordValue::offset).toList());
            if (expectedRecords.isEmpty()) {
                if (output.output().isPresent()) {
                    throw new IllegalStateException("M5-B fabricated a physical batch for empty coverage");
                }
                continue;
            }
            ParsedBatch actual =
                    output.output().orElseThrow(() -> new IllegalStateException("M5-B omitted retained Kafka records"));
            if (!actual.records().equals(expectedRecords)
                    || actual.producerId() != input.producerId()
                    || actual.producerEpoch() != input.producerEpoch()
                    || actual.baseSequence() != input.baseSequence()
                    || actual.transactional() != input.transactional()
                    || actual.controlKind() != input.controlKind()
                    || actual.partitionLeaderEpoch() != input.partitionLeaderEpoch()
                    || (input.controlKind() == ControlKind.NONE
                            && actual.compressionType() != plan.policy().outputCompression())) {
                throw new IllegalStateException("M5-B output changed retained Kafka record/batch semantics");
            }
            actualOffsets.addAll(
                    actual.records().stream().map(RecordValue::offset).toList());
        }
        if (!actualOffsets.equals(expectedOffsets)) {
            throw new IllegalStateException("M5-B output offsets differ from retained absolute offsets");
        }
    }

    private static void requireNms1(KafkaSemanticCompactorV1.CandidateGeneration candidate) {
        if (!Nms1CodecV1.decode(candidate.payloadCandidate().canonicalBody()).equals(candidate.nms1())
                || !candidate
                        .payloadObject()
                        .identity()
                        .bodySha256()
                        .equals(Sha256Digest.hash(candidate.payloadCandidate().canonicalBody()))) {
            throw new IllegalStateException("M5-B NMS1 body/identity differs on full reread");
        }
        CanonicalBytes expectedPayload = concatenate(candidate.batchOutputs().stream()
                .flatMap(value -> value.outputBody().stream())
                .toList());
        if (!candidate.nms1().payload().equals(expectedPayload)) {
            throw new IllegalStateException("M5-B NMS1 payload does not contain the exact output batches");
        }
    }

    private static void requireIndexes(CompactionPlan plan, KafkaSemanticCompactorV1.CandidateGeneration candidate) {
        if (candidate.indexes().size() != IndexKind.values().length
                || candidate.indexCandidates().size() != IndexKind.values().length) {
            throw new IllegalStateException("M5-B did not rebuild the complete eight-index set");
        }
        Map<IndexKind, KafkaCompactionIndexV1> indexes = new EnumMap<>(IndexKind.class);
        for (int ordinal = 0; ordinal < candidate.indexes().size(); ordinal++) {
            KafkaCompactionIndexV1 expected = candidate.indexes().get(ordinal);
            KafkaCompactionIndexV1 decoded = KafkaCompactionIndexV1.decode(
                    candidate.indexCandidates().get(ordinal).canonicalBody());
            if (!decoded.equals(expected)
                    || decoded.kind().ordinal() != ordinal
                    || !decoded.materializationTaskIdSha256()
                            .equals(candidate.materializationPlan().taskIdSha256())
                    || !decoded.outputIdentitySha256()
                            .equals(candidate.materializationPlan().outputIdentitySha256())) {
                throw new IllegalStateException("M5-B rebuilt index body/identity differs");
            }
            indexes.put(decoded.kind(), decoded);
        }
        List<Long> retainedOffsets = candidate.outputBatches().stream()
                .flatMap(batch -> batch.records().stream())
                .map(RecordValue::offset)
                .sorted()
                .toList();
        for (IndexKind kind : List.of(
                IndexKind.OFFSET_OR_POSITION, IndexKind.PAYLOAD_LOCATOR, IndexKind.TIMESTAMP, IndexKind.LEADER_EPOCH)) {
            List<Long> indexed = indexes.get(kind).rows().stream()
                    .map(row -> row.coverage().inclusiveStart())
                    .toList();
            if (!indexed.equals(retainedOffsets)) {
                throw new IllegalStateException("M5-B rebuilt index omits or invents retained offsets: " + kind);
            }
        }
        for (var gap : candidate.gaps()) {
            OptionalRowCheck.requireSuccessorOrEnd(indexes.get(IndexKind.OFFSET_OR_POSITION), gap.inclusiveStart());
        }
        requireExactCoverage(indexes.get(IndexKind.CHECKSUM_COVERAGE), plan);

        Map<Long, RecordValue> records = candidate.outputBatches().stream()
                .flatMap(batch -> batch.records().stream())
                .collect(Collectors.toMap(RecordValue::offset, value -> value));
        for (var row : indexes.get(IndexKind.TIMESTAMP).rows()) {
            RecordValue record = records.get(row.coverage().inclusiveStart());
            if (record == null
                    || row.minimumTimestamp() != record.timestamp()
                    || row.maximumTimestamp() != record.timestamp()) {
                throw new IllegalStateException("M5-B timestamp index differs from retained records");
            }
        }
        Map<Long, ParsedBatch> batchesByOffset = new HashMap<>();
        for (ParsedBatch batch : candidate.outputBatches()) {
            for (RecordValue record : batch.records()) {
                batchesByOffset.put(record.offset(), batch);
            }
        }
        for (var row : indexes.get(IndexKind.PRODUCER_RECOVERY).rows()) {
            ParsedBatch batch = batchesByOffset.get(row.coverage().inclusiveStart());
            if (batch == null
                    || row.producerId() != batch.producerId()
                    || row.producerEpoch() != batch.producerEpoch()) {
                throw new IllegalStateException("M5-B producer recovery index differs from retained batches");
            }
        }
    }

    private static void requireExactCoverage(KafkaCompactionIndexV1 index, CompactionPlan plan) {
        long cursor = plan.frontiers().candidateStartOffset();
        for (var row : index.rows()) {
            if (row.coverage().inclusiveStart() != cursor) {
                throw new IllegalStateException("M5-B checksum/gap index does not cover the complete cut");
            }
            cursor = row.coverage().exclusiveEnd();
        }
        if (cursor != plan.frontiers().candidateEndOffsetExclusive()) {
            throw new IllegalStateException("M5-B checksum/gap index ends before the captured cut");
        }
    }

    private static CanonicalBytes concatenate(List<CanonicalBytes> values) {
        int length = values.stream().mapToInt(CanonicalBytes::length).reduce(0, Math::addExact);
        byte[] result = new byte[length];
        int offset = 0;
        for (CanonicalBytes value : values) {
            value.copyTo(result, offset);
            offset += value.length();
        }
        return CanonicalBytes.copyOf(result);
    }

    private static final class OptionalRowCheck {
        private static void requireSuccessorOrEnd(KafkaCompactionIndexV1 index, long offset) {
            var row = index.lookup(offset);
            if (row.isPresent() && row.orElseThrow().coverage().inclusiveStart() < offset) {
                throw new IllegalStateException("M5-B gap lookup returned an earlier removed offset");
            }
        }
    }
}
