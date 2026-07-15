/* Licensed under the Apache License, Version 2.0 */
package com.nereusstream.materialization;

import com.nereusstream.api.Checksum;
import com.nereusstream.api.PayloadFormat;
import com.nereusstream.api.ProjectionRef;
import com.nereusstream.core.physical.PhysicalObjectIdentity;
import com.nereusstream.core.physical.PhysicalObjectKind;
import com.nereusstream.metadata.oxia.CommitSliceRequest;
import com.nereusstream.metadata.oxia.VersionedMaterializationTask;
import com.nereusstream.metadata.oxia.codec.ReadTargetCodecRegistry;
import com.nereusstream.metadata.oxia.records.GenerationIndexRecord;
import com.nereusstream.metadata.oxia.records.GenerationLifecycle;
import com.nereusstream.metadata.oxia.records.MaterializationOutputRecord;
import com.nereusstream.metadata.oxia.records.MaterializationTaskRecord;
import com.nereusstream.metadata.oxia.records.SourceGenerationRecord;
import java.nio.charset.StandardCharsets;
import java.util.List;
import java.util.Optional;

/** Strict domain/durable mapper shared by publication and recovery. */
final class MaterializationRecordMapper {
    private MaterializationRecordMapper() {
    }

    static void requireTaskAndOutput(
            VersionedMaterializationTask durable,
            MaterializationTask task,
            MaterializationOutput output) {
        MaterializationTaskRecord value = durable.value();
        if (!output.taskId().equals(task.taskId())
                || !output.streamId().equals(task.streamId())
                || output.view() != task.view()
                || !output.coverage().equals(task.coverage())
                || !output.sourceSetSha256().equals(task.sourceSetSha256())
                || !value.taskId().equals(task.taskId())
                || value.taskSequence() != task.taskSequence()
                || !value.streamId().equals(task.streamId().value())
                || value.readViewId() != task.view().wireId()
                || value.taskKindId() != task.taskKind().wireId()
                || value.offsetStart() != task.coverage().startOffset()
                || value.offsetEnd() != task.coverage().endOffset()
                || !value.sources().equals(task.sources().stream()
                        .map(MaterializationRecordMapper::sourceRecord)
                        .toList())
                || !value.sourceSetSha256().equals(task.sourceSetSha256().value())
                || !value.policyId().equals(task.policy().policyId())
                || value.policyVersion() != task.policy().policyVersion()
                || !value.policySha256().equals(task.policyDigestSha256().value())
                || value.output().isEmpty()
                || !value.output().orElseThrow().equals(outputRecord(output))) {
            throw new IllegalArgumentException(
                    "durable materialization task/output does not match the requested publication");
        }
    }

    static SourceGenerationRecord sourceRecord(SourceGeneration source) {
        return new SourceGenerationRecord(
                source.view().wireId(),
                source.range().startOffset(),
                source.range().endOffset(),
                source.generation(),
                source.commitVersion(),
                source.indexKey(),
                source.indexMetadataVersion(),
                source.indexRecordSha256().value(),
                ReadTargetCodecRegistry.phase15().encode(source.readTarget()),
                source.targetIdentitySha256().value(),
                source.materializationPolicySha256().map(Checksum::value).orElse(""),
                source.payloadFormat().name(),
                projectionIdentity(source.projectionRef()),
                source.recordCount(),
                source.entryCount(),
                source.logicalBytes(),
                source.schemaRefs(),
                source.cumulativeSizeAtStart(),
                source.cumulativeSizeAtEnd());
    }

    static MaterializationOutputRecord outputRecord(MaterializationOutput output) {
        return new MaterializationOutputRecord(
                output.outputAttemptId(),
                output.objectId().value(),
                output.objectKey().value(),
                output.objectKeyHash().value(),
                output.objectLength(),
                output.storageCrc32c().value(),
                output.contentSha256().value(),
                output.etag(),
                output.physicalFormat(),
                output.logicalFormat(),
                ReadTargetCodecRegistry.phase15().encode(output.readTarget()),
                output.targetIdentitySha256().value(),
                output.sourceRecordCount(),
                output.outputRecordCount(),
                output.entryCount(),
                output.logicalBytes(),
                output.schemaRefs(),
                output.cumulativeSizeAtStart(),
                output.cumulativeSizeAtEnd(),
                output.sourceSetSha256().value(),
                projectionIdentity(output.projectionRef()));
    }

    static GenerationIndexRecord preparedIndex(
            MaterializationTask task,
            MaterializationOutput output,
            long generation,
            String publicationId,
            long nowMillis) {
        List<SourceGeneration> sources = task.sources();
        return new GenerationIndexRecord(
                1,
                task.streamId().value(),
                task.view().wireId(),
                task.coverage().startOffset(),
                task.coverage().endOffset(),
                generation,
                publicationId,
                task.taskId(),
                GenerationLifecycle.PREPARED,
                task.sourceSetSha256().value(),
                task.policyDigestSha256().value(),
                ReadTargetCodecRegistry.phase15().encode(output.readTarget()),
                output.targetIdentitySha256().value(),
                task.policyDigestSha256().value(),
                PayloadFormat.valueOf(output.logicalFormat()).name(),
                output.sourceRecordCount(),
                output.outputRecordCount(),
                output.entryCount(),
                output.logicalBytes(),
                output.cumulativeSizeAtStart(),
                output.cumulativeSizeAtEnd(),
                sources.get(0).commitVersion(),
                sources.get(sources.size() - 1).commitVersion(),
                output.schemaRefs(),
                projectionIdentity(output.projectionRef()),
                nowMillis,
                0,
                "",
                nowMillis,
                0);
    }

    static GenerationIndexRecord committedIndex(
            GenerationIndexRecord prepared,
            long nowMillis) {
        long committedAt = Math.max(nowMillis, prepared.createdAtMillis());
        return new GenerationIndexRecord(
                prepared.schemaVersion(),
                prepared.streamId(),
                prepared.readViewId(),
                prepared.offsetStart(),
                prepared.offsetEnd(),
                prepared.generation(),
                prepared.publicationId(),
                prepared.taskId(),
                GenerationLifecycle.COMMITTED,
                prepared.sourceSetSha256(),
                prepared.policySha256(),
                prepared.readTarget(),
                prepared.targetIdentitySha256(),
                prepared.materializationPolicySha256(),
                prepared.payloadFormat(),
                prepared.sourceRecordCount(),
                prepared.outputRecordCount(),
                prepared.entryCount(),
                prepared.logicalBytes(),
                prepared.cumulativeSizeAtStart(),
                prepared.cumulativeSizeAtEnd(),
                prepared.firstCommitVersion(),
                prepared.lastCommitVersion(),
                prepared.schemaRefs(),
                prepared.projectionRef(),
                prepared.createdAtMillis(),
                committedAt,
                "",
                committedAt,
                0);
    }

    static PhysicalObjectIdentity physicalIdentity(MaterializationOutput output) {
        PhysicalObjectKind kind = output.view() == com.nereusstream.api.ReadView.COMMITTED
                ? PhysicalObjectKind.COMMITTED_COMPACTED
                : PhysicalObjectKind.TOPIC_COMPACTED;
        return PhysicalObjectIdentity.create(
                output.objectKey(),
                Optional.of(output.objectId()),
                kind,
                output.objectLength(),
                output.storageCrc32c(),
                Optional.of(output.contentSha256()),
                output.etag().isEmpty() ? Optional.empty() : Optional.of(output.etag()));
    }

    static String projectionIdentity(Optional<ProjectionRef> projection) {
        if (projection.isEmpty()) {
            return CommitSliceRequest.emptyProjectionIdentity();
        }
        ProjectionRef value = projection.orElseThrow();
        StringBuilder result = new StringBuilder();
        append(result, "projectionRef");
        append(result, "present");
        append(result, value.type().name());
        append(result, value.value());
        return result.toString();
    }

    private static void append(StringBuilder target, String value) {
        target.append(value.getBytes(StandardCharsets.UTF_8).length)
                .append(':')
                .append(value);
    }
}
