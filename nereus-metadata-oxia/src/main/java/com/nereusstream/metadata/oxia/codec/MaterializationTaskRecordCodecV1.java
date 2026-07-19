/* Licensed under the Apache License, Version 2.0 */
package com.nereusstream.metadata.oxia.codec;

import com.nereusstream.metadata.oxia.records.MaterializationOutputRecord;
import com.nereusstream.metadata.oxia.records.MaterializationPolicyRecord;
import com.nereusstream.metadata.oxia.records.MaterializationTaskRecord;
import com.nereusstream.metadata.oxia.records.SourceGenerationRecord;
import com.nereusstream.metadata.oxia.records.TaskLifecycle;
import com.nereusstream.metadata.oxia.records.WorkerClaimRecord;
import java.util.ArrayList;
import java.util.List;
import java.util.Optional;
import java.util.OptionalLong;

public final class MaterializationTaskRecordCodecV1
        extends AbstractF4RecordCodecV1<MaterializationTaskRecord> {
    public MaterializationTaskRecordCodecV1() {
        super(MaterializationTaskRecord.class);
    }

    @Override
    public byte[] encode(MaterializationTaskRecord value) {
        try {
            F4Binary.Writer writer = writer();
            writer.writeString(value.taskId());
            writer.writeLong(value.taskSequence());
            writer.writeString(value.streamId());
            writer.writeUnsignedShort(value.readViewId());
            writer.writeUnsignedShort(value.taskKindId());
            writer.writeLong(value.offsetStart());
            writer.writeLong(value.offsetEnd());
            writer.writeInt(value.sources().size());
            value.sources().forEach(source -> writeSource(writer, source));
            writer.writeString(value.sourceSetSha256());
            writer.writeString(value.policyId());
            writer.writeLong(value.policyVersion());
            writer.writeString(value.policySha256());
            writePolicy(writer, value.policy());
            writer.writeUnsignedShort(value.lifecycle().wireId());
            writer.writeLong(value.attempt());
            writer.writeOptional(value.workerClaim().isPresent());
            value.workerClaim().ifPresent(claim -> writeClaim(writer, claim));
            writer.writeOptional(value.output().isPresent());
            value.output().ifPresent(output -> writeOutput(writer, output));
            writer.writeOptional(value.allocatedGeneration().isPresent());
            value.allocatedGeneration().ifPresent(writer::writeLong);
            writer.writeString(value.publicationId());
            writer.writeUnsignedShort(value.failureClassId());
            writer.writeString(value.failureMessage());
            writer.writeLong(value.retryNotBeforeMillis());
            writer.writeLong(value.createdAtMillis());
            writer.writeLong(value.updatedAtMillis());
            writer.writeLong(value.metadataVersion());
            return writer.toByteArray();
        } catch (RuntimeException failure) {
            throw malformed(failure);
        }
    }

    @Override
    public MaterializationTaskRecord decode(byte[] bytes) {
        try {
            F4Binary.Reader reader = reader(bytes);
            String taskId = reader.readString("taskId");
            long taskSequence = reader.readLong("taskSequence");
            String streamId = reader.readString("streamId");
            int readViewId = reader.readUnsignedShort("readViewId");
            int taskKindId = reader.readUnsignedShort("taskKindId");
            long offsetStart = reader.readLong("offsetStart");
            long offsetEnd = reader.readLong("offsetEnd");
            int sourceCount = reader.readCount("sourceCount", Long.BYTES * 8, 128);
            List<SourceGenerationRecord> sources = new ArrayList<>(sourceCount);
            for (int index = 0; index < sourceCount; index++) {
                sources.add(readSource(reader));
            }
            String sourceSetSha256 = reader.readString("sourceSetSha256");
            String policyId = reader.readString("policyId");
            long policyVersion = reader.readLong("policyVersion");
            String policySha256 = reader.readString("policySha256");
            MaterializationPolicyRecord policy = readPolicy(reader);
            TaskLifecycle lifecycle = TaskLifecycle.fromWireId(reader.readUnsignedShort("lifecycle"));
            long attempt = reader.readLong("attempt");
            Optional<WorkerClaimRecord> claim = reader.readOptional("workerClaimPresent")
                    ? Optional.of(readClaim(reader)) : Optional.empty();
            Optional<MaterializationOutputRecord> output = reader.readOptional("outputPresent")
                    ? Optional.of(readOutput(reader)) : Optional.empty();
            OptionalLong generation = reader.readOptional("allocatedGenerationPresent")
                    ? OptionalLong.of(reader.readLong("allocatedGeneration")) : OptionalLong.empty();
            MaterializationTaskRecord value = new MaterializationTaskRecord(
                    VERSION,
                    taskId,
                    taskSequence,
                    streamId,
                    readViewId,
                    taskKindId,
                    offsetStart,
                    offsetEnd,
                    sources,
                    sourceSetSha256,
                    policyId,
                    policyVersion,
                    policySha256,
                    policy,
                    lifecycle,
                    attempt,
                    claim,
                    output,
                    generation,
                    reader.readString("publicationId"),
                    reader.readUnsignedShort("failureClassId"),
                    reader.readString("failureMessage"),
                    reader.readLong("retryNotBeforeMillis"),
                    reader.readLong("createdAtMillis"),
                    reader.readLong("updatedAtMillis"),
                    reader.readLong("metadataVersion"));
            reader.requireConsumed();
            return value;
        } catch (RuntimeException failure) {
            throw malformed(failure);
        }
    }

    static void writePolicy(F4Binary.Writer writer, MaterializationPolicyRecord value) {
        writer.writeString(value.policyId());
        writer.writeLong(value.policyVersion());
        writer.writeUnsignedShort(value.readViewId());
        writer.writeUnsignedShort(value.taskKindId());
        writer.writeString(value.targetPhysicalFormat());
        writer.writeInt(value.minMergeSourceRanges());
        writer.writeInt(value.maxSourceRanges());
        writer.writeLong(value.maxRangeRecords());
        writer.writeLong(value.targetObjectBytes());
        writer.writeInt(value.targetRowGroupRecords());
        writer.writeString(value.compression());
        writer.writeString(value.topicStrategyId());
        writer.writeLong(value.topicStrategyVersion());
        writer.writeString(value.topicKeyCodecId());
    }

    static MaterializationPolicyRecord readPolicy(F4Binary.Reader reader) {
        return new MaterializationPolicyRecord(
                reader.readString("policySnapshotId"),
                reader.readLong("policySnapshotVersion"),
                reader.readUnsignedShort("policySnapshotReadViewId"),
                reader.readUnsignedShort("policySnapshotTaskKindId"),
                reader.readString("policySnapshotTargetPhysicalFormat"),
                reader.readInt("policySnapshotMinMergeSourceRanges"),
                reader.readInt("policySnapshotMaxSourceRanges"),
                reader.readLong("policySnapshotMaxRangeRecords"),
                reader.readLong("policySnapshotTargetObjectBytes"),
                reader.readInt("policySnapshotTargetRowGroupRecords"),
                reader.readString("policySnapshotCompression"),
                reader.readString("policySnapshotTopicStrategyId"),
                reader.readLong("policySnapshotTopicStrategyVersion"),
                reader.readString("policySnapshotTopicKeyCodecId"));
    }

    private static void writeSource(F4Binary.Writer writer, SourceGenerationRecord value) {
        writer.writeUnsignedShort(value.readViewId());
        writer.writeLong(value.offsetStart());
        writer.writeLong(value.offsetEnd());
        writer.writeLong(value.generation());
        writer.writeLong(value.commitVersion());
        writer.writeString(value.indexKey());
        writer.writeLong(value.indexMetadataVersion());
        writer.writeString(value.indexRecordSha256());
        F4Binary.writeReadTarget(writer, value.readTarget());
        writer.writeString(value.targetIdentitySha256());
        writer.writeString(value.materializationPolicySha256());
        writer.writeString(value.payloadFormat());
        writer.writeString(value.projectionRef());
        writer.writeInt(value.recordCount());
        writer.writeInt(value.entryCount());
        writer.writeLong(value.logicalBytes());
        F4Binary.writeSchemaRefs(writer, value.schemaRefs());
        writer.writeLong(value.cumulativeSizeAtStart());
        writer.writeLong(value.cumulativeSizeAtEnd());
    }

    private static SourceGenerationRecord readSource(F4Binary.Reader reader) {
        return new SourceGenerationRecord(
                reader.readUnsignedShort("sourceReadViewId"),
                reader.readLong("sourceOffsetStart"),
                reader.readLong("sourceOffsetEnd"),
                reader.readLong("sourceGeneration"),
                reader.readLong("sourceCommitVersion"),
                reader.readString("sourceIndexKey"),
                reader.readLong("sourceIndexMetadataVersion"),
                reader.readString("sourceIndexRecordSha256"),
                F4Binary.readReadTarget(reader, "sourceReadTarget"),
                reader.readString("sourceTargetIdentitySha256"),
                reader.readString("sourceMaterializationPolicySha256"),
                reader.readString("sourcePayloadFormat"),
                reader.readString("sourceProjectionRef"),
                reader.readInt("sourceRecordCount"),
                reader.readInt("sourceEntryCount"),
                reader.readLong("sourceLogicalBytes"),
                F4Binary.readSchemaRefs(reader, "sourceSchemaRefs"),
                reader.readLong("sourceCumulativeSizeAtStart"),
                reader.readLong("sourceCumulativeSizeAtEnd"));
    }

    static void writeClaim(F4Binary.Writer writer, WorkerClaimRecord value) {
        writer.writeString(value.claimId());
        writer.writeString(value.processRunId());
        writer.writeLong(value.attempt());
        writer.writeLong(value.claimedAtMillis());
        writer.writeLong(value.expiresAtMillis());
    }

    static WorkerClaimRecord readClaim(F4Binary.Reader reader) {
        return new WorkerClaimRecord(
                reader.readString("claimId"),
                reader.readString("processRunId"),
                reader.readLong("claimAttempt"),
                reader.readLong("claimedAtMillis"),
                reader.readLong("claimExpiresAtMillis"));
    }

    static void writeOutput(F4Binary.Writer writer, MaterializationOutputRecord value) {
        writer.writeString(value.outputAttemptId());
        writer.writeString(value.objectId());
        writer.writeString(value.objectKey());
        writer.writeString(value.objectKeyHash());
        writer.writeLong(value.objectLength());
        writer.writeString(value.storageCrc32c());
        writer.writeString(value.contentSha256());
        writer.writeString(value.etag());
        writer.writeString(value.physicalFormat());
        writer.writeString(value.logicalFormat());
        F4Binary.writeReadTarget(writer, value.readTarget());
        writer.writeString(value.targetIdentitySha256());
        writer.writeInt(value.sourceRecordCount());
        writer.writeInt(value.outputRecordCount());
        writer.writeInt(value.entryCount());
        writer.writeLong(value.logicalBytes());
        F4Binary.writeSchemaRefs(writer, value.schemaRefs());
        writer.writeLong(value.cumulativeSizeAtStart());
        writer.writeLong(value.cumulativeSizeAtEnd());
        writer.writeString(value.sourceSetSha256());
        writer.writeString(value.projectionRef());
    }

    static MaterializationOutputRecord readOutput(F4Binary.Reader reader) {
        return new MaterializationOutputRecord(
                reader.readString("outputAttemptId"),
                reader.readString("outputObjectId"),
                reader.readString("outputObjectKey"),
                reader.readString("outputObjectKeyHash"),
                reader.readLong("outputObjectLength"),
                reader.readString("outputStorageCrc32c"),
                reader.readString("outputContentSha256"),
                reader.readString("outputEtag"),
                reader.readString("outputPhysicalFormat"),
                reader.readString("outputLogicalFormat"),
                F4Binary.readReadTarget(reader, "outputReadTarget"),
                reader.readString("outputTargetIdentitySha256"),
                reader.readInt("outputSourceRecordCount"),
                reader.readInt("outputRecordCount"),
                reader.readInt("outputEntryCount"),
                reader.readLong("outputLogicalBytes"),
                F4Binary.readSchemaRefs(reader, "outputSchemaRefs"),
                reader.readLong("outputCumulativeSizeAtStart"),
                reader.readLong("outputCumulativeSizeAtEnd"),
                reader.readString("outputSourceSetSha256"),
                reader.readString("outputProjectionRef"));
    }
}
