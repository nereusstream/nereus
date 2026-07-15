/* Licensed under the Apache License, Version 2.0 */
package com.nereusstream.materialization;

import com.nereusstream.api.Checksum;
import com.nereusstream.api.ChecksumType;
import com.nereusstream.api.ObjectId;
import com.nereusstream.api.ObjectKey;
import com.nereusstream.api.ObjectKeyHash;
import com.nereusstream.api.OffsetRange;
import com.nereusstream.api.PayloadFormat;
import com.nereusstream.api.PublicationId;
import com.nereusstream.api.ProjectionRef;
import com.nereusstream.core.physical.PhysicalObjectIdentity;
import com.nereusstream.core.physical.PhysicalObjectKind;
import com.nereusstream.api.target.ObjectSliceReadTarget;
import com.nereusstream.metadata.oxia.CommitSliceRequest;
import com.nereusstream.metadata.oxia.ProjectionIdentity;
import com.nereusstream.metadata.oxia.VersionedMaterializationTask;
import com.nereusstream.metadata.oxia.codec.ReadTargetCodecRegistry;
import com.nereusstream.metadata.oxia.records.GenerationIndexRecord;
import com.nereusstream.metadata.oxia.records.GenerationLifecycle;
import com.nereusstream.metadata.oxia.records.MaterializationOutputRecord;
import com.nereusstream.metadata.oxia.records.MaterializationPolicyRecord;
import com.nereusstream.metadata.oxia.records.MaterializationTaskRecord;
import com.nereusstream.metadata.oxia.records.SourceGenerationRecord;
import com.nereusstream.metadata.oxia.records.TaskFailureClass;
import com.nereusstream.metadata.oxia.records.TaskLifecycle;
import java.nio.charset.StandardCharsets;
import java.util.List;
import java.util.Objects;
import java.util.Optional;
import java.util.OptionalLong;

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

    static MaterializationTaskRecord plannedTask(MaterializationTask task, long nowMillis) {
        Objects.requireNonNull(task, "task");
        if (nowMillis < 0) {
            throw new IllegalArgumentException("nowMillis must be non-negative");
        }
        return new MaterializationTaskRecord(
                1,
                task.taskId(),
                task.taskSequence(),
                task.streamId().value(),
                task.view().wireId(),
                task.taskKind().wireId(),
                task.coverage().startOffset(),
                task.coverage().endOffset(),
                task.sources().stream().map(MaterializationRecordMapper::sourceRecord).toList(),
                task.sourceSetSha256().value(),
                task.policy().policyId(),
                task.policy().policyVersion(),
                task.policyDigestSha256().value(),
                policyRecord(task.policy()),
                TaskLifecycle.PLANNED,
                0,
                Optional.empty(),
                Optional.empty(),
                OptionalLong.empty(),
                "",
                TaskFailureClass.NONE.wireId(),
                "",
                0,
                nowMillis,
                nowMillis,
                0);
    }

    static MaterializationTask domainTask(
            VersionedMaterializationTask durable) {
        Objects.requireNonNull(durable, "durable");
        return domainTask(durable, domainPolicy(durable.value().policy()));
    }

    static MaterializationTask domainTask(
            VersionedMaterializationTask durable,
            MaterializationPolicy policy) {
        Objects.requireNonNull(durable, "durable");
        Objects.requireNonNull(policy, "policy");
        MaterializationTaskRecord value = durable.value();
        List<SourceGeneration> sources = value.sources().stream()
                .map(MaterializationRecordMapper::sourceGeneration)
                .toList();
        MaterializationTask task = new MaterializationTask(
                value.taskId(),
                new com.nereusstream.api.StreamId(value.streamId()),
                com.nereusstream.api.ReadView.fromWireId(value.readViewId()),
                TaskKind.fromWireId(value.taskKindId()),
                new OffsetRange(value.offsetStart(), value.offsetEnd()),
                sources,
                new Checksum(ChecksumType.SHA256, value.sourceSetSha256()),
                policy,
                new Checksum(ChecksumType.SHA256, value.policySha256()));
        if (value.taskSequence() != task.taskSequence()
                || !value.policyId().equals(policy.policyId())
                || value.policyVersion() != policy.policyVersion()
                || !value.policySha256().equals(policy.digestSha256().value())) {
            throw new IllegalArgumentException("durable task does not match the supplied policy/sequence");
        }
        return task;
    }

    static MaterializationPolicyRecord policyRecord(MaterializationPolicy policy) {
        Objects.requireNonNull(policy, "policy");
        Optional<TopicCompactionSpec> topic = policy.topicCompaction();
        return new MaterializationPolicyRecord(
                policy.policyId(),
                policy.policyVersion(),
                policy.view().wireId(),
                policy.taskKind().wireId(),
                policy.targetPhysicalFormat(),
                policy.minMergeSourceRanges(),
                policy.maxSourceRanges(),
                policy.maxRangeRecords(),
                policy.targetObjectBytes(),
                policy.targetRowGroupRecords(),
                policy.compression(),
                topic.map(TopicCompactionSpec::strategyId).orElse(""),
                topic.map(TopicCompactionSpec::strategyVersion).orElse(0L),
                topic.map(TopicCompactionSpec::keyCodecId).orElse(""));
    }

    static MaterializationPolicy domainPolicy(MaterializationPolicyRecord policy) {
        Objects.requireNonNull(policy, "policy");
        Optional<TopicCompactionSpec> topic = policy.topicStrategyId().isEmpty()
                ? Optional.empty()
                : Optional.of(new TopicCompactionSpec(
                        policy.topicStrategyId(),
                        policy.topicStrategyVersion(),
                        policy.topicKeyCodecId()));
        return new MaterializationPolicy(
                policy.policyId(),
                policy.policyVersion(),
                com.nereusstream.api.ReadView.fromWireId(policy.readViewId()),
                TaskKind.fromWireId(policy.taskKindId()),
                policy.targetPhysicalFormat(),
                policy.minMergeSourceRanges(),
                policy.maxSourceRanges(),
                policy.maxRangeRecords(),
                policy.targetObjectBytes(),
                policy.targetRowGroupRecords(),
                policy.compression(),
                topic);
    }

    static SourceGeneration sourceGeneration(SourceGenerationRecord source) {
        Objects.requireNonNull(source, "source");
        return new SourceGeneration(
                com.nereusstream.api.ReadView.fromWireId(source.readViewId()),
                new OffsetRange(source.offsetStart(), source.offsetEnd()),
                source.generation(),
                source.commitVersion(),
                source.indexKey(),
                source.indexMetadataVersion(),
                new Checksum(ChecksumType.SHA256, source.indexRecordSha256()),
                ReadTargetCodecRegistry.phase15().decode(source.readTarget()),
                new Checksum(ChecksumType.SHA256, source.targetIdentitySha256()),
                source.materializationPolicySha256().isEmpty()
                        ? Optional.empty()
                        : Optional.of(new Checksum(
                                ChecksumType.SHA256, source.materializationPolicySha256())),
                PayloadFormat.valueOf(source.payloadFormat()),
                ProjectionIdentity.decode(source.projectionRef()),
                source.recordCount(),
                source.entryCount(),
                source.logicalBytes(),
                source.schemaRefs(),
                source.cumulativeSizeAtStart(),
                source.cumulativeSizeAtEnd());
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

    static MaterializationOutput domainOutput(
            MaterializationTask task,
            MaterializationOutputRecord output) {
        Objects.requireNonNull(task, "task");
        Objects.requireNonNull(output, "output");
        var decoded = ReadTargetCodecRegistry.phase15().decode(output.readTarget());
        if (!(decoded instanceof ObjectSliceReadTarget target)) {
            throw new IllegalArgumentException("materialization output target is not an object slice");
        }
        return new MaterializationOutput(
                task.taskId(),
                task.streamId(),
                task.view(),
                task.coverage(),
                output.outputAttemptId(),
                new ObjectId(output.objectId()),
                new ObjectKey(output.objectKey()),
                new ObjectKeyHash(output.objectKeyHash()),
                output.objectLength(),
                new Checksum(ChecksumType.CRC32C, output.storageCrc32c()),
                new Checksum(ChecksumType.SHA256, output.contentSha256()),
                output.etag(),
                output.physicalFormat(),
                output.logicalFormat(),
                target,
                new Checksum(ChecksumType.SHA256, output.targetIdentitySha256()),
                target.entryIndexRef(),
                output.sourceRecordCount(),
                output.outputRecordCount(),
                output.entryCount(),
                output.logicalBytes(),
                output.schemaRefs(),
                output.cumulativeSizeAtStart(),
                output.cumulativeSizeAtEnd(),
                new Checksum(ChecksumType.SHA256, output.sourceSetSha256()),
                ProjectionIdentity.decode(output.projectionRef()));
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

    static GenerationIndexRecord abortedIndex(
            GenerationIndexRecord prepared,
            String reason,
            long nowMillis) {
        if (prepared.lifecycle() != GenerationLifecycle.PREPARED) {
            throw new IllegalArgumentException("only a PREPARED generation can be aborted");
        }
        String exactReason = requireText(reason, "reason");
        long changedAt = Math.max(nowMillis, prepared.stateChangedAtMillis());
        return new GenerationIndexRecord(
                prepared.schemaVersion(),
                prepared.streamId(),
                prepared.readViewId(),
                prepared.offsetStart(),
                prepared.offsetEnd(),
                prepared.generation(),
                prepared.publicationId(),
                prepared.taskId(),
                GenerationLifecycle.ABORTED,
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
                0,
                exactReason,
                changedAt,
                0);
    }

    static MaterializationTaskRecord publishing(
            MaterializationTaskRecord current,
            PublicationId publicationId,
            long nowMillis) {
        return taskState(
                current,
                TaskLifecycle.PUBLISHING,
                OptionalLong.empty(),
                publicationId.value(),
                nowMillis);
    }

    static MaterializationTaskRecord attachGeneration(
            MaterializationTaskRecord current,
            long generation,
            long nowMillis) {
        if (generation <= 0 || current.publicationId().isEmpty()) {
            throw new IllegalArgumentException("publishing generation identity is invalid");
        }
        return taskState(
                current,
                TaskLifecycle.PUBLISHING,
                OptionalLong.of(generation),
                current.publicationId(),
                nowMillis);
    }

    static MaterializationTaskRecord published(
            MaterializationTaskRecord current,
            long nowMillis) {
        if (current.allocatedGeneration().isEmpty() || current.publicationId().isEmpty()) {
            throw new IllegalArgumentException("published task requires a frozen publication allocation");
        }
        return taskState(
                current,
                TaskLifecycle.PUBLISHED,
                current.allocatedGeneration(),
                current.publicationId(),
                nowMillis);
    }

    static MaterializationTaskRecord outputReadyAfterAbort(
            MaterializationTaskRecord current,
            long nowMillis) {
        if (current.lifecycle() != TaskLifecycle.PUBLISHING
                || current.allocatedGeneration().isEmpty()
                || current.publicationId().isEmpty()) {
            throw new IllegalArgumentException("only an allocated PUBLISHING task can clear an aborted publication");
        }
        return taskState(
                current,
                TaskLifecycle.OUTPUT_READY,
                OptionalLong.empty(),
                "",
                nowMillis);
    }

    static boolean sameGenerationPublicationIdentity(
            GenerationIndexRecord left,
            GenerationIndexRecord right) {
        return left.schemaVersion() == right.schemaVersion()
                && left.streamId().equals(right.streamId())
                && left.readViewId() == right.readViewId()
                && left.offsetStart() == right.offsetStart()
                && left.offsetEnd() == right.offsetEnd()
                && left.generation() == right.generation()
                && left.publicationId().equals(right.publicationId())
                && left.taskId().equals(right.taskId())
                && left.sourceSetSha256().equals(right.sourceSetSha256())
                && left.policySha256().equals(right.policySha256())
                && left.readTarget().equals(right.readTarget())
                && left.targetIdentitySha256().equals(right.targetIdentitySha256())
                && left.materializationPolicySha256().equals(right.materializationPolicySha256())
                && left.payloadFormat().equals(right.payloadFormat())
                && left.sourceRecordCount() == right.sourceRecordCount()
                && left.outputRecordCount() == right.outputRecordCount()
                && left.entryCount() == right.entryCount()
                && left.logicalBytes() == right.logicalBytes()
                && left.cumulativeSizeAtStart() == right.cumulativeSizeAtStart()
                && left.cumulativeSizeAtEnd() == right.cumulativeSizeAtEnd()
                && left.firstCommitVersion() == right.firstCommitVersion()
                && left.lastCommitVersion() == right.lastCommitVersion()
                && left.schemaRefs().equals(right.schemaRefs())
                && left.projectionRef().equals(right.projectionRef())
                && left.createdAtMillis() == right.createdAtMillis();
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

    private static String requireText(String value, String field) {
        Objects.requireNonNull(value, field);
        if (value.isBlank()) {
            throw new IllegalArgumentException(field + " cannot be blank");
        }
        return value;
    }

    private static MaterializationTaskRecord taskState(
            MaterializationTaskRecord current,
            TaskLifecycle lifecycle,
            OptionalLong generation,
            String publicationId,
            long nowMillis) {
        long updatedAt = Math.max(nowMillis, current.updatedAtMillis());
        return new MaterializationTaskRecord(
                current.schemaVersion(),
                current.taskId(),
                current.taskSequence(),
                current.streamId(),
                current.readViewId(),
                current.taskKindId(),
                current.offsetStart(),
                current.offsetEnd(),
                current.sources(),
                current.sourceSetSha256(),
                current.policyId(),
                current.policyVersion(),
                current.policySha256(),
                current.policy(),
                lifecycle,
                current.attempt(),
                current.workerClaim(),
                current.output(),
                generation,
                publicationId,
                TaskFailureClass.NONE.wireId(),
                "",
                0,
                current.createdAtMillis(),
                updatedAt,
                0);
    }
}
