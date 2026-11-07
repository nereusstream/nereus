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

package com.nereusstream.kafka.bookkeeper.read;

import com.nereusstream.domain.bytes.CanonicalBytes;
import com.nereusstream.domain.bytes.Sha256Digest;
import com.nereusstream.domain.codec.TopicIncarnationIdentityCodecV1;
import com.nereusstream.kafka.bookkeeper.adapter.KafkaNativeAssignedRecordBatchV1;
import com.nereusstream.kafka.bookkeeper.adapter.KafkaRawAssignedRecordBatchFactsV1;
import com.nereusstream.storage.object.materialization.M5LookupIndexV1;
import com.nereusstream.storage.object.materialization.M5MaterializationCodecV1;
import com.nereusstream.storage.object.materialization.M5MaterializationRecordsV1.GenerationObject;
import com.nereusstream.storage.object.materialization.M5MaterializationRecordsV1.IndexKind;
import com.nereusstream.storage.object.materialization.M5MaterializationRecordsV1.MaterializationPlan;
import com.nereusstream.storage.object.materialization.M5MaterializationRecordsV1.MaterializationSourceCut;
import com.nereusstream.storage.object.materialization.M5MaterializationRecordsV1.PayloadKind;
import com.nereusstream.storage.object.materialization.M5MaterializationRecordsV1.PositionDomain;
import com.nereusstream.storage.object.materialization.M5MaterializationRecordsV1.ProtocolCoverage;
import com.nereusstream.storage.object.materialization.M5MaterializationRecordsV1.RepresentationMode;
import com.nereusstream.storage.object.materialization.M5MaterializationValidatorV1.ValidatedGeneration;
import com.nereusstream.storage.object.materialization.Nms1CodecV1;
import com.nereusstream.storage.object.read.BindingReadRouteV1.FailureClass;
import com.nereusstream.storage.object.read.control.M4ReadControlCodecV1;
import com.nereusstream.storage.object.read.control.M4ReadControlRecordsV1.SourceProtectionIdentity;
import java.io.IOException;
import java.nio.ByteBuffer;
import java.util.ArrayList;
import java.util.Comparator;
import java.util.List;
import java.util.Objects;
import java.util.concurrent.CompletionStage;

/** Reads a single validated, byte-preserving Kafka prefix; the caller owns its captured selector and IO lifetime. */
public final class KafkaObjectMaterializedPrefixV1 {
    @FunctionalInterface
    public interface ExactVersionRead {
        CanonicalBytes read(GenerationObject object) throws IOException;
    }

    /** A qualified Object read failure after the original response and its buffers have drained. */
    public static final class ReadFailure extends IOException {
        private static final long serialVersionUID = 1L;
        private final FailureClass failureClass;

        public ReadFailure(FailureClass failureClass, String message, Throwable cause) {
            super(message, cause);
            this.failureClass = Objects.requireNonNull(failureClass, "failureClass");
        }

        public FailureClass failureClass() {
            return failureClass;
        }
    }

    /** The owner still owns this exact response. A failed Fetch observer grants no fallback or release. */
    public static final class UnresolvedRead extends IOException {
        private static final long serialVersionUID = 1L;
        private final CompletionStage<Void> originalDrain;

        public UnresolvedRead(Throwable cause, CompletionStage<Void> originalDrain) {
            super("original Kafka Object GET has not drained", cause);
            this.originalDrain = Objects.requireNonNull(originalDrain, "originalDrain");
        }

        public CompletionStage<Void> originalDrain() {
            return originalDrain;
        }
    }

    public record SelectedBatch(long startOffset, long endOffsetExclusive, CanonicalBytes rawAssignedRecordBatch) {}

    public record Selection(List<SelectedBatch> batches, boolean canReadTail) {
        public Selection {
            batches = List.copyOf(batches);
        }
    }

    private final MaterializationPlan plan;
    private final ValidatedGeneration validated;
    private final ExactVersionRead objects;
    private final GenerationObject offsets;

    public KafkaObjectMaterializedPrefixV1(
            MaterializationPlan plan, ValidatedGeneration validated, ExactVersionRead objects) {
        this.plan = Objects.requireNonNull(plan, "plan");
        this.validated = Objects.requireNonNull(validated, "validated");
        this.objects = Objects.requireNonNull(objects, "objects");
        var cut = plan.sourceCut();
        var generation = validated.generation();
        var manifest = validated.manifestView();
        if (cut.coverage().domain() != PositionDomain.KAFKA_OFFSET
                || plan.payloadKind() != PayloadKind.KAFKA_BATCH_PRESERVING_V1
                || generation.payloadKind() != plan.payloadKind()
                || generation.representationMode() != RepresentationMode.REWRITE_GENERATION
                || !generation.taskIdSha256().equals(plan.taskIdSha256())
                || !generation.outputIdentitySha256().equals(plan.outputIdentitySha256())
                || !generation.identity().equals(cut.identity())
                || !generation.coverage().equals(cut.coverage())
                || !generation.sourceSetSha256().equals(cut.sourceSetSha256())
                || !generation
                        .validationRootSha256()
                        .equals(M5MaterializationCodecV1.validationRootSha256(validated.validationRoot()))
                || !manifest.preferredGenerationSha256().equals(M5MaterializationCodecV1.generationSha256(generation))
                || !manifest.identity().equals(cut.identity())
                || !manifest.coverage().equals(cut.coverage())
                || !manifest.exactPredecessorSelectorValueSha256().equals(cut.predecessorSelectorValueSha256())
                || !manifest.exactPredecessorViewSha256().equals(cut.predecessorViewSha256())
                || generation.payloadObjects().size() != cut.sources().size()) {
            throw new IllegalArgumentException("Kafka Object generation differs from its validated plan/manifest");
        }
        offsets = generation.indexObjects().stream()
                .filter(value -> value.indexKind() == IndexKind.OFFSET_OR_POSITION)
                .findFirst()
                .orElseThrow(() -> new IllegalArgumentException("Kafka Object generation has no offset index"));
    }

    public ProtocolCoverage coverage() {
        return validated.manifestView().coverage();
    }

    public Sha256Digest manifestSha256() {
        return M5MaterializationCodecV1.manifestSha256(validated.manifestView());
    }

    public long sourceGeneration() {
        return validated.generation().sourceGeneration();
    }

    public MaterializationSourceCut sourceCut() {
        return plan.sourceCut();
    }

    public List<SourceProtectionIdentity> originalFallbackSources() {
        var predecessor = sourceCut().predecessorSelector();
        long generation = Math.addExact(predecessor.sourceGeneration(), 1);
        long epoch = Math.addExact(predecessor.readAdmissionEpoch(), 1);
        var protectedSources = sourceCut().sources().stream()
                .map(source -> new SourceProtectionIdentity(
                        source.sourceIdentitySha256(), generation, epoch, generation, predecessor.capability()))
                .sorted(Comparator.comparing(
                        source -> source.sourceIdentitySha256().toHex()))
                .toList();
        if (sourceGeneration() != generation
                || !validated
                        .generation()
                        .fallbackSetSha256()
                        .orElseThrow()
                        .equals(M4ReadControlCodecV1.calculateFallbackSetSha256(protectedSources))) {
            throw new IllegalStateException("Kafka Object prefix does not name its exact original BK fallback");
        }
        return protectedSources;
    }

    void requireSnapshot(KafkaBookKeeperReadSnapshotV1 snapshot) {
        var identity = plan.sourceCut().identity();
        var fence = snapshot.root().fence();
        if (!identity.binding().bindingId().equals(fence.bindingId())
                || !identity.binding()
                        .incarnationSha256()
                        .equals(Sha256Digest.hash(TopicIncarnationIdentityCodecV1.encode(fence.topicIncarnation())))
                || !identity.binding()
                        .storageEpochSha256()
                        .equals(fence.storageEpochId().digest())
                || identity.ownerEpoch() > fence.ownerEpoch()
                || coverage().inclusiveStart() > snapshot.root().frontiers().trimStartOffset()
                || coverage().exclusiveEnd() > snapshot.root().frontiers().durableEndOffset()) {
            throw new IllegalStateException("Kafka Object prefix differs from the captured Binding and frontiers");
        }
    }

    public Selection read(long offset, long isolationEnd, long byteBudget) throws IOException {
        if (offset < coverage().inclusiveStart() || offset >= coverage().exclusiveEnd() || byteBudget <= 0) {
            throw new IllegalArgumentException("Kafka Object Fetch is outside the captured prefix");
        }
        var indexBytes = readExact(offsets);
        final M5LookupIndexV1 index;
        try {
            index = M5LookupIndexV1.decode(indexBytes);
        } catch (IllegalArgumentException | IllegalStateException failure) {
            throw corrupt("Kafka Object offset index failed validation", failure);
        }
        if (!index.coverage().equals(coverage())
                || index.kind() != IndexKind.OFFSET_OR_POSITION
                || !index.taskIdSha256().equals(plan.taskIdSha256())
                || !index.outputIdentitySha256().equals(plan.outputIdentitySha256())) {
            throw corrupt("Kafka Object offset index belongs to another generation", null);
        }
        var row = index.lookup(offset).orElseThrow(() -> corrupt("Kafka Object offset is not in its index", null));
        if (row.objectOrdinal() >= plan.sourceCut().sources().size()) {
            throw corrupt("Kafka Object offset points outside its exact payload part", null);
        }
        var source = plan.sourceCut().sources().get(row.objectOrdinal());
        if (!row.coverage().equals(source.coverage())
                || row.byteOffset() != 0
                || row.byteLength() != source.canonicalLength()
                || !row.payloadUnitSha256().equals(source.bodySha256())) {
            throw corrupt("Kafka Object offset row differs from its exact source extent", null);
        }
        return selectBatches(offset, isolationEnd, byteBudget, row.objectOrdinal());
    }

    private Selection selectBatches(long offset, long isolationEnd, long byteBudget, int firstPart) throws IOException {
        var result = new ArrayList<SelectedBatch>();
        long used = 0;
        for (int ordinal = firstPart;
                ordinal < validated.generation().payloadObjects().size();
                ordinal++) {
            var source = plan.sourceCut().sources().get(ordinal);
            if (source.coverage().inclusiveStart() >= isolationEnd) {
                return new Selection(result, false);
            }
            var batches = readPart(ordinal);
            for (var batch : batches) {
                if (batch.endOffsetExclusive() <= offset) {
                    continue;
                }
                if (batch.endOffsetExclusive() > isolationEnd
                        || (!result.isEmpty() && batch.rawAssignedRecordBatch().length() > byteBudget - used)) {
                    return new Selection(result, false);
                }
                result.add(batch);
                used = Math.addExact(used, batch.rawAssignedRecordBatch().length());
                if (used >= byteBudget) {
                    return new Selection(result, false);
                }
            }
        }
        return new Selection(result, isolationEnd > coverage().exclusiveEnd());
    }

    private List<SelectedBatch> readPart(int ordinal) throws IOException {
        var source = plan.sourceCut().sources().get(ordinal);
        var descriptor = validated.generation().payloadObjects().get(ordinal);
        var partBytes = readExact(descriptor);
        final com.nereusstream.storage.object.materialization.Nms1ObjectV1 part;
        try {
            part = Nms1CodecV1.decode(partBytes);
        } catch (IllegalArgumentException | IllegalStateException failure) {
            throw corrupt("Kafka Object payload failed validation", failure);
        }
        var payload = part.payload();
        if (descriptor.ordinal() != ordinal
                || !descriptor.coverage().equals(source.coverage())
                || !descriptor
                        .identity()
                        .key()
                        .equals(plan.outputParts().get(ordinal).objectKey())
                || !part.identity().equals(plan.sourceCut().identity())
                || part.payloadKind() != plan.payloadKind()
                || !part.taskIdSha256().equals(plan.taskIdSha256())
                || !part.outputIdentitySha256().equals(plan.outputIdentitySha256())
                || !part.coverage().equals(source.coverage())
                || part.partOrdinal() != ordinal
                || part.partCount() != plan.outputParts().size()
                || payload.length() != source.canonicalLength()
                || !Sha256Digest.hash(payload).equals(source.bodySha256())) {
            throw corrupt("Kafka Object payload differs from its exact BK source and manifest", null);
        }
        return parseBatches(payload, source.coverage());
    }

    private static List<SelectedBatch> parseBatches(CanonicalBytes payload, ProtocolCoverage coverage)
            throws IOException {
        var input = ByteBuffer.wrap(payload.toByteArray());
        var batches = new ArrayList<SelectedBatch>();
        long nextOffset = coverage.inclusiveStart();
        while (input.hasRemaining()) {
            if (input.remaining() < Long.BYTES + Integer.BYTES) {
                throw corrupt("truncated Kafka Object batch header", null);
            }
            int declared = input.getInt(input.position() + Long.BYTES);
            long length = (long) declared + Long.BYTES + Integer.BYTES;
            if (declared <= 0 || length > input.remaining()) {
                throw corrupt("Kafka Object batch length escapes its source part", null);
            }
            var raw = new byte[(int) length];
            input.get(raw);
            var bytes = CanonicalBytes.copyOf(raw);
            final KafkaNativeAssignedRecordBatchV1 batch;
            try {
                batch = KafkaNativeAssignedRecordBatchV1.validate(KafkaRawAssignedRecordBatchFactsV1.parse(bytes));
            } catch (IllegalArgumentException | IllegalStateException failure) {
                throw corrupt("Kafka Object batch failed native Kafka validation", failure);
            }
            long end = batch.endOffsetExclusive();
            if (batch.baseOffset() != nextOffset || end > coverage.exclusiveEnd()) {
                throw corrupt("Kafka Object batches do not exactly cover their raw BK source", null);
            }
            batches.add(new SelectedBatch(batch.baseOffset(), end, bytes));
            nextOffset = end;
        }
        if (nextOffset != coverage.exclusiveEnd()) {
            throw corrupt("Kafka Object part has incomplete coverage", null);
        }
        return List.copyOf(batches);
    }

    private CanonicalBytes readExact(GenerationObject object) throws IOException {
        if (object.immutableProviderVersionToken().isEmpty()) {
            throw new IllegalStateException("Kafka materialized Object has no exact provider version");
        }
        var bytes = objects.read(object);
        if (bytes.length() != object.identity().bodyLength()
                || !Sha256Digest.hash(bytes).equals(object.identity().bodySha256())) {
            throw corrupt("Kafka materialized Object bytes differ from the selected version", null);
        }
        return bytes;
    }

    private static ReadFailure corrupt(String message, Throwable cause) {
        return new ReadFailure(FailureClass.CORRUPT_OR_FORMAT, message, cause);
    }
}
