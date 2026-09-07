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
import com.nereusstream.kafka.bookkeeper.compaction.KafkaCompactionRecordsV1.Gap;
import com.nereusstream.kafka.bookkeeper.compaction.KafkaCompactionRecordsV1.ParsedBatch;
import com.nereusstream.kafka.bookkeeper.compaction.KafkaCompactionRecordsV1.RecordValue;
import com.nereusstream.storage.object.materialization.M5MaterializationRecordsV1.IndexKind;
import com.nereusstream.storage.object.materialization.M5MaterializationValidatorV1;
import java.io.ByteArrayOutputStream;
import java.util.ArrayList;
import java.util.LinkedHashMap;
import java.util.List;
import java.util.Map;
import java.util.Optional;
import java.util.OptionalLong;

/** Verified immutable carrier caches. The caller still owns native protocol interpretation and M4 read admission. */
public final class KafkaBookKeeperReadViewV2 {
    private final KafkaSealedBookKeeperDescriptorV2 descriptor;
    private final KafkaBookKeeperArtifactAssemblerV2.Artifacts artifacts;
    private final List<ParsedBatch> batches;
    private final List<KafkaCompactionIndexV1> indexes;
    private final List<Gap> gaps;

    KafkaBookKeeperReadViewV2(
            KafkaSealedBookKeeperDescriptorV2 descriptor, KafkaBookKeeperArtifactAssemblerV2.Artifacts artifacts) {
        this.descriptor = descriptor;
        this.artifacts = artifacts;
        this.batches =
                artifacts.batches().stream().map(KafkaRecordBatchCodecV1::parse).toList();
        this.indexes =
                artifacts.indexes().stream().map(KafkaCompactionIndexV1::decode).toList();
        if (batches.size() != descriptor.batchCount()
                || !artifacts.indexLocators().equals(descriptor.indexes())) {
            throw new IllegalStateException(
                    "recovered BK artifact count or physical index locators differ from descriptor");
        }
        var records = expectedRecords();
        this.gaps = gaps(records);
        requireProof();
        requireIndexes(records);
    }

    public KafkaSealedBookKeeperDescriptorV2 descriptor() {
        return descriptor;
    }

    public KafkaCompactionIndexV1 index(IndexKind kind) {
        return indexes.get(kind.ordinal());
    }

    public Optional<KafkaCompactionIndexV1.Row> lookup(long offset) {
        return index(IndexKind.OFFSET_OR_POSITION).lookup(offset);
    }

    public OptionalLong listOffset(long timestamp) {
        return index(IndexKind.TIMESTAMP).listOffset(timestamp);
    }

    public CanonicalBytes readBatch(int ordinal) {
        return artifacts.batches().get(ordinal);
    }

    public List<Gap> gaps() {
        return gaps;
    }

    /** Within this complete compacted cut, a gap must never be filled from obsolete predecessor bytes. */
    public boolean allowsPredecessorOffset(long offset) {
        if (offset < 0) {
            throw new IllegalArgumentException("negative Kafka offset");
        }
        return gaps.stream().noneMatch(gap -> gap.inclusiveStart() <= offset && offset < gap.exclusiveEnd());
    }

    KafkaBookKeeperArtifactAssemblerV2.Artifacts artifacts() {
        return artifacts;
    }

    private Map<Long, LocatedRecord> expectedRecords() {
        Map<Long, LocatedRecord> result = new LinkedHashMap<>();
        long byteOffset = 0;
        long previous = -1;
        var coverage = descriptor.sourceCut().coverage();
        for (int ordinal = 0; ordinal < batches.size(); ordinal++) {
            ParsedBatch batch = batches.get(ordinal);
            if (batch.records().isEmpty()) {
                throw new IllegalStateException("recovered BK data artifact is an empty batch");
            }
            for (RecordValue record : batch.records()) {
                if (record.offset() <= previous
                        || record.offset() < coverage.inclusiveStart()
                        || record.offset() >= coverage.exclusiveEnd()) {
                    throw new IllegalStateException("recovered BK record order or coverage differs");
                }
                result.put(record.offset(), new LocatedRecord(ordinal, byteOffset, batch, record));
                previous = record.offset();
            }
            byteOffset =
                    Math.addExact(byteOffset, artifacts.batches().get(ordinal).length());
        }
        return result;
    }

    private List<Gap> gaps(Map<Long, LocatedRecord> records) {
        List<Gap> result = new ArrayList<>();
        long next = descriptor.sourceCut().coverage().inclusiveStart();
        for (long offset : records.keySet()) {
            if (offset > next) {
                result.add(new Gap(next, offset));
            }
            next = Math.addExact(offset, 1);
        }
        if (next < descriptor.sourceCut().coverage().exclusiveEnd()) {
            result.add(new Gap(next, descriptor.sourceCut().coverage().exclusiveEnd()));
        }
        return List.copyOf(result);
    }

    private void requireProof() {
        ByteArrayOutputStream bytes = new ByteArrayOutputStream();
        artifacts.batches().forEach(body -> bytes.writeBytes(body.toByteArray()));
        Sha256Digest payload = M5MaterializationValidatorV1.semanticPayloadBodiesRoot(
                List.of(CanonicalBytes.copyOf(bytes.toByteArray())));
        Sha256Digest index = M5MaterializationValidatorV1.semanticIndexBodiesRoot(artifacts.indexes());
        var proof = descriptor.semanticProof();
        Sha256Digest semantic = KafkaCompactionCanonicalV1.semanticValidationRoot(
                KafkaCompactionCanonicalV1.compactionTaskId(
                        proof.taskIdSha256(), descriptor.task().compactionPlanRootSha256()),
                descriptor.task().compactionPlanRootSha256(),
                descriptor.dispositionRootSha256(),
                proof.protocolStateRootSha256(),
                proof.compactionSuppressionRootSha256(),
                KafkaCompactionCanonicalV1.outputRecordRoot(batches),
                payload,
                index);
        if (!payload.equals(proof.payloadBodiesRootSha256())
                || !index.equals(proof.indexBodiesRootSha256())
                || !semantic.equals(proof.semanticValidationRootSha256())
                || !KafkaBookKeeperArtifactAssemblerV2.gapRoot(gaps).equals(descriptor.gapRootSha256())) {
            throw new IllegalStateException("recovered BK semantic, gap or complete body root differs from descriptor");
        }
    }

    private void requireIndexes(Map<Long, LocatedRecord> records) {
        Map<Long, Integer> baseFlags = new LinkedHashMap<>();
        index(IndexKind.OFFSET_OR_POSITION)
                .rows()
                .forEach(row -> baseFlags.put(row.coverage().inclusiveStart(), row.flags()));
        for (int ordinal = 0; ordinal < indexes.size(); ordinal++) {
            KafkaCompactionIndexV1 index = indexes.get(ordinal);
            if (index.kind().ordinal() != ordinal
                    || !index.coverage().equals(descriptor.sourceCut().coverage())
                    || !index.materializationTaskIdSha256()
                            .equals(descriptor.semanticProof().taskIdSha256())
                    || !index.outputIdentitySha256().equals(descriptor.task().semanticOutputSha256())) {
                throw new IllegalStateException("recovered BK index identity differs from descriptor");
            }
            List<Long> expected = records.keySet().stream()
                    .filter(offset -> switch (index.kind()) {
                        case PRODUCER_RECOVERY -> records.get(offset).batch().producerId() != -1;
                        case TRANSACTION -> records.get(offset).batch().transactional();
                        case ABORTED_TRANSACTION ->
                            (baseFlags.getOrDefault(offset, 0) & KafkaCompactionIndexV1.FLAG_ABORTED) != 0;
                        default -> true;
                    })
                    .toList();
            List<Long> actual = index.rows().stream()
                    .filter(row -> (row.flags() & KafkaCompactionIndexV1.FLAG_GAP) == 0)
                    .map(row -> row.coverage().inclusiveStart())
                    .toList();
            if (!actual.equals(expected)) {
                throw new IllegalStateException("recovered BK index omits or invents protocol rows");
            }
            List<Gap> actualGaps = new ArrayList<>();
            for (var row : index.rows()) {
                if ((row.flags() & KafkaCompactionIndexV1.FLAG_GAP) != 0) {
                    actualGaps.add(new Gap(
                            row.coverage().inclusiveStart(), row.coverage().exclusiveEnd()));
                    continue;
                }
                LocatedRecord located = records.get(row.coverage().inclusiveStart());
                ParsedBatch batch = located.batch();
                RecordValue record = located.record();
                int flags = KafkaCompactionIndexV1.FLAG_RETAINED
                        | (batch.transactional() ? KafkaCompactionIndexV1.FLAG_TRANSACTIONAL : 0)
                        | (batch.controlKind() != KafkaCompactionRecordsV1.ControlKind.NONE
                                ? KafkaCompactionIndexV1.FLAG_CONTROL
                                : 0)
                        | (baseFlags.get(record.offset()) & KafkaCompactionIndexV1.FLAG_ABORTED);
                if (!batch.transactional() && (flags & KafkaCompactionIndexV1.FLAG_ABORTED) != 0
                        || row.flags() != flags
                        || row.outputBatchOrdinal() != located.ordinal()
                        || row.byteOffset() != located.byteOffset()
                        || row.byteLength() != batch.canonicalBody().length()
                        || row.coverage().exclusiveEnd() != Math.addExact(record.offset(), 1)
                        || row.minimumTimestamp() != record.timestamp()
                        || row.maximumTimestamp() != record.timestamp()
                        || row.producerId() != batch.producerId()
                        || row.producerEpoch() != batch.producerEpoch()
                        || row.sequence() != record.sequence()
                        || row.leaderEpoch() != batch.partitionLeaderEpoch()
                        || !row.identitySha256().equals(KafkaCompactionCanonicalV1.recordIdentity(batch, record))) {
                    throw new IllegalStateException(
                            "recovered BK index locator or protocol fields differ from exact records");
                }
            }
            if (!actualGaps.equals(index.kind() == IndexKind.CHECKSUM_COVERAGE ? gaps : List.of())) {
                throw new IllegalStateException("recovered BK index gap coverage differs");
            }
        }
    }

    private record LocatedRecord(int ordinal, long byteOffset, ParsedBatch batch, RecordValue record) {}
}
