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
import com.nereusstream.kafka.bookkeeper.compaction.KafkaCompactionRecordsV1.DispositionRow;
import com.nereusstream.kafka.bookkeeper.compaction.KafkaCompactionRecordsV1.HeaderValue;
import com.nereusstream.kafka.bookkeeper.compaction.KafkaCompactionRecordsV1.ParsedBatch;
import com.nereusstream.kafka.bookkeeper.compaction.KafkaCompactionRecordsV1.RecordValue;
import com.nereusstream.storage.object.materialization.M5MaterializationCodecV1;
import java.io.ByteArrayOutputStream;
import java.io.DataOutputStream;
import java.io.IOException;
import java.nio.charset.StandardCharsets;
import java.util.List;
import java.util.Objects;

/** Canonical domain-separated identities for M5-B plans, records, dispositions, and semantic proof. */
public final class KafkaCompactionCanonicalV1 {
    private KafkaCompactionCanonicalV1() {}

    public static Sha256Digest planRoot(CompactionPlan plan) {
        Objects.requireNonNull(plan, "plan");
        return hash("NEREUS_V2_M5_B_COMPACTION_PLAN_V1", output -> {
            writeBytes(output, M5MaterializationCodecV1.encodeSourceCut(plan.sourceCut()));
            var policy = plan.policy();
            output.writeInt(policy.kafkaFeatureLevel());
            output.writeByte(policy.messageMagic());
            output.writeByte(policy.cleanupPolicy().ordinal());
            output.writeInt(policy.minimumCleanableRatioPartsPerMillion());
            output.writeLong(policy.deleteRetentionMs());
            output.writeLong(policy.capturedNowMs());
            output.writeLong(policy.policyGeneration());
            writeText(output, policy.rewritePolicyVersion());
            writeText(output, policy.nativeKafkaOracleVersion());
            output.writeByte(policy.outputCompression().id);
            var caps = policy.caps();
            output.writeLong(caps.maximumDirtyBytes());
            output.writeInt(caps.maximumInputBatches());
            output.writeInt(caps.maximumRecords());
            output.writeInt(caps.maximumDistinctKeys());
            output.writeLong(caps.maximumKeyBytes());
            output.writeLong(caps.maximumOutputBytes());
            output.writeLong(caps.maximumIndexBytes());
            output.writeInt(caps.maximumTransactions());
            output.writeInt(caps.maximumTombstones());
            var frontiers = plan.frontiers();
            output.writeLong(frontiers.logStartOffset());
            output.writeLong(frontiers.durableOffset());
            output.writeLong(frontiers.logEndOffset());
            output.writeLong(frontiers.highWatermark());
            output.writeLong(frontiers.lastStableOffset());
            output.writeLong(frontiers.candidateStartOffset());
            output.writeLong(frontiers.candidateEndOffsetExclusive());
            writeProtocolRoots(output, plan);
            output.writeInt(plan.inputBatches().size());
            for (var batch : plan.inputBatches()) {
                writeDigest(output, batch.sourceIdentitySha256());
                output.writeInt(batch.sourceBatchOrdinal());
                output.writeInt(batch.canonicalBody().length());
                writeDigest(output, Sha256Digest.hash(batch.canonicalBody()));
            }
            output.writeInt(plan.keyProofs().size());
            for (var proof : plan.keyProofs()) {
                writeBytes(output, proof.key());
                output.writeLong(proof.latestEligibleOffset());
                output.writeBoolean(proof.completeDomain());
                output.writeBoolean(proof.latestCommitted());
                output.writeBoolean(proof.olderValueMayReappear());
                output.writeLong(proof.tombstoneDeadlineMs());
            }
            output.writeInt(plan.transactions().size());
            for (var transaction : plan.transactions()) {
                output.writeLong(transaction.producerId());
                output.writeLong(transaction.firstOffset());
                output.writeLong(transaction.endOffsetExclusive());
                output.writeByte(transaction.outcome().ordinal());
                output.writeInt(transaction.coordinatorEpoch());
            }
            output.writeInt(plan.leaderEpochs().size());
            for (var epoch : plan.leaderEpochs()) {
                output.writeInt(epoch.leaderEpoch());
                output.writeLong(epoch.startOffset());
                output.writeLong(epoch.endOffsetExclusive());
            }
            writeLongs(output, plan.undecidableOffsets());
            writeLongs(output, plan.recoveryRequiredOffsets());
        });
    }

    public static Sha256Digest compactionTaskId(Sha256Digest materializationTaskId, Sha256Digest planRoot) {
        return hash("NEREUS_V2_M5_B_COMPACTION_TASK_V1", output -> {
            writeDigest(output, materializationTaskId);
            writeDigest(output, planRoot);
        });
    }

    public static Sha256Digest recordIdentity(ParsedBatch batch, RecordValue record) {
        return hash("NEREUS_V2_M5_B_KAFKA_RECORD_V1", output -> {
            writeDigest(output, batch.bodySha256());
            output.writeLong(batch.producerId());
            output.writeShort(batch.producerEpoch());
            output.writeInt(batch.baseSequence());
            output.writeBoolean(batch.transactional());
            output.writeByte(batch.controlKind().ordinal());
            output.writeInt(batch.partitionLeaderEpoch());
            output.writeLong(record.offset());
            output.writeInt(record.sequence());
            output.writeLong(record.timestamp());
            writeOptionalBytes(output, record.key());
            writeOptionalBytes(output, record.value());
            output.writeInt(record.headers().size());
            for (HeaderValue header : record.headers()) {
                writeText(output, header.key());
                writeOptionalBytes(output, header.value());
            }
        });
    }

    public static Sha256Digest dispositionRoot(List<DispositionRow> rows) {
        return hash("NEREUS_V2_M5_B_DISPOSITION_ROOT_V1", output -> {
            output.writeInt(rows.size());
            for (DispositionRow row : rows) {
                output.writeLong(row.offset());
                writeDigest(output, row.inputBatchSha256());
                output.writeByte(row.disposition().ordinal());
                writeDigest(output, row.recordIdentitySha256());
            }
        });
    }

    public static Sha256Digest suppressionRoot(List<DispositionRow> rows) {
        return hash("NEREUS_V2_M5_B_COMPACTION_SUPPRESSION_GAP_ROOT_V1", output -> {
            List<DispositionRow> dropped =
                    rows.stream().filter(row -> !row.retained()).toList();
            output.writeInt(dropped.size());
            for (DispositionRow row : dropped) {
                output.writeLong(row.offset());
                output.writeByte(row.disposition().ordinal());
                writeDigest(output, row.recordIdentitySha256());
            }
        });
    }

    public static Sha256Digest protocolStateRoot(CompactionPlan plan) {
        return hash("NEREUS_V2_M5_B_PROTOCOL_STATE_ROOT_V1", output -> writeProtocolRoots(output, plan));
    }

    public static Sha256Digest outputRecordRoot(List<ParsedBatch> batches) {
        return hash("NEREUS_V2_M5_B_OUTPUT_RECORD_ROOT_V1", output -> {
            int count =
                    batches.stream().mapToInt(batch -> batch.records().size()).sum();
            output.writeInt(count);
            for (ParsedBatch batch : batches) {
                for (RecordValue record : batch.records()) {
                    output.writeLong(record.offset());
                    writeDigest(output, recordIdentity(batch, record));
                }
            }
        });
    }

    public static Sha256Digest semanticValidationRoot(
            Sha256Digest compactionTaskId,
            Sha256Digest planRoot,
            Sha256Digest dispositionRoot,
            Sha256Digest protocolStateRoot,
            Sha256Digest suppressionRoot,
            Sha256Digest outputRecordRoot,
            Sha256Digest payloadBodiesRoot,
            Sha256Digest indexBodiesRoot) {
        return hash("NEREUS_V2_M5_B_SEMANTIC_VALIDATION_ROOT_V1", output -> {
            writeDigest(output, compactionTaskId);
            writeDigest(output, planRoot);
            writeDigest(output, dispositionRoot);
            writeDigest(output, protocolStateRoot);
            writeDigest(output, suppressionRoot);
            writeDigest(output, outputRecordRoot);
            writeDigest(output, payloadBodiesRoot);
            writeDigest(output, indexBodiesRoot);
        });
    }

    private static void writeProtocolRoots(DataOutputStream output, CompactionPlan plan) throws IOException {
        var roots = plan.protocolRoots();
        writeDigest(output, roots.committedProducerStateSha256());
        writeDigest(output, roots.speculativeQueueSha256());
        writeDigest(output, roots.transactionIndexSha256());
        writeDigest(output, roots.abortedTransactionIndexSha256());
        writeDigest(output, roots.leaderEpochIndexSha256());
        writeDigest(output, roots.timestampIndexSha256());
        writeDigest(output, roots.recoveryCheckpointSha256());
        writeDigest(output, roots.activeTailSha256());
        writeDigest(output, roots.completeKeyDomainSha256());
    }

    private static Sha256Digest hash(String domain, Encoder encoder) {
        try {
            ByteArrayOutputStream bytes = new ByteArrayOutputStream();
            try (DataOutputStream output = new DataOutputStream(bytes)) {
                writeText(output, domain);
                encoder.encode(output);
            }
            return Sha256Digest.hash(CanonicalBytes.copyOf(bytes.toByteArray()));
        } catch (IOException impossible) {
            throw new IllegalStateException("in-memory M5-B identity encoding failed", impossible);
        }
    }

    private static void writeLongs(DataOutputStream output, List<Long> values) throws IOException {
        output.writeInt(values.size());
        for (long value : values) {
            output.writeLong(value);
        }
    }

    private static void writeOptionalBytes(DataOutputStream output, java.util.Optional<CanonicalBytes> value)
            throws IOException {
        output.writeBoolean(value.isPresent());
        if (value.isPresent()) {
            writeBytes(output, value.orElseThrow());
        }
    }

    private static void writeBytes(DataOutputStream output, CanonicalBytes value) throws IOException {
        output.writeInt(value.length());
        output.write(value.toByteArray());
    }

    private static void writeText(DataOutputStream output, String value) throws IOException {
        writeBytes(output, CanonicalBytes.copyOf(value.getBytes(StandardCharsets.UTF_8)));
    }

    private static void writeDigest(DataOutputStream output, Sha256Digest value) throws IOException {
        output.write(value.bytes().toByteArray());
    }

    @FunctionalInterface
    private interface Encoder {
        void encode(DataOutputStream output) throws IOException;
    }
}
