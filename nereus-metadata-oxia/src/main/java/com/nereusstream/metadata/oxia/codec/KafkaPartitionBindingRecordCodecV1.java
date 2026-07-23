/* Licensed under the Apache License, Version 2.0 */
package com.nereusstream.metadata.oxia.codec;

import com.nereusstream.metadata.oxia.records.KafkaCheckpointReferenceRecord;
import com.nereusstream.metadata.oxia.records.KafkaCompactionCoverageRecord;
import com.nereusstream.metadata.oxia.records.KafkaPartitionBindingRecord;
import com.nereusstream.metadata.oxia.records.KafkaPartitionPendingOperationRecord;
import java.util.ArrayList;

/** Closed explicit field-order codec for the authoritative Kafka partition root. */
public final class KafkaPartitionBindingRecordCodecV1
        extends AbstractF4RecordCodecV1<KafkaPartitionBindingRecord> {
    private static final int MAX_CHECKPOINTS = 3;

    public KafkaPartitionBindingRecordCodecV1() {
        super(KafkaPartitionBindingRecord.class);
    }

    @Override
    public byte[] encode(KafkaPartitionBindingRecord value) {
        try {
            F4Binary.Writer writer = writer();
            writer.writeInt(value.formatVersion());
            writer.writeString(value.kafkaClusterId());
            writer.writeString(value.topicId());
            writer.writeInt(value.partitionId());
            writer.writeString(value.observedTopicName());
            writer.writeLong(value.incarnation());
            writer.writeString(value.streamName());
            writer.writeString(value.streamId());
            writer.writeInt(value.payloadMappingId());
            writer.writeString(value.storageProfile());
            writer.writeInt(value.lifecycleId());
            writer.writeLong(value.bindingEpoch());
            writer.writeLong(value.createdMetadataOffset());
            writer.writeLong(value.lastAppliedMetadataOffset());
            writer.writeInt(value.observedLeaderId());
            writer.writeInt(value.observedLeaderEpoch());
            writer.writeLong(value.observedBrokerEpoch());
            writer.writeLong(value.observedLogStartOffset());
            writer.writeLong(value.observedStableEndOffset());
            writeCoverage(writer, value.compactionCoverage());
            writer.writeInt(value.checkpointReferences().size());
            for (KafkaCheckpointReferenceRecord reference : value.checkpointReferences()) {
                writeCheckpoint(writer, reference);
            }
            writeOperation(writer, value.pendingOperation());
            writer.writeLong(value.createdAtMillis());
            writer.writeLong(value.updatedAtMillis());
            writer.writeLong(value.metadataVersion());
            return writer.toByteArray();
        } catch (RuntimeException failure) {
            throw malformed(failure);
        }
    }

    @Override
    public KafkaPartitionBindingRecord decode(byte[] bytes) {
        try {
            F4Binary.Reader reader = reader(bytes);
            int formatVersion = reader.readInt("formatVersion");
            String kafkaClusterId = reader.readString("kafkaClusterId");
            String topicId = reader.readString("topicId");
            int partitionId = reader.readInt("partitionId");
            String observedTopicName = reader.readString("observedTopicName");
            long incarnation = reader.readLong("incarnation");
            String streamName = reader.readString("streamName");
            String streamId = reader.readString("streamId");
            int payloadMappingId = reader.readInt("payloadMappingId");
            String storageProfile = reader.readString("storageProfile");
            int lifecycleId = reader.readInt("lifecycleId");
            long bindingEpoch = reader.readLong("bindingEpoch");
            long createdMetadataOffset = reader.readLong("createdMetadataOffset");
            long lastAppliedMetadataOffset = reader.readLong("lastAppliedMetadataOffset");
            int observedLeaderId = reader.readInt("observedLeaderId");
            int observedLeaderEpoch = reader.readInt("observedLeaderEpoch");
            long observedBrokerEpoch = reader.readLong("observedBrokerEpoch");
            long observedLogStartOffset = reader.readLong("observedLogStartOffset");
            long observedStableEndOffset = reader.readLong("observedStableEndOffset");
            KafkaCompactionCoverageRecord coverage = readCoverage(reader);
            int checkpointCount = reader.readCount("checkpointCount", 64, MAX_CHECKPOINTS);
            ArrayList<KafkaCheckpointReferenceRecord> checkpoints = new ArrayList<>(checkpointCount);
            for (int index = 0; index < checkpointCount; index++) {
                checkpoints.add(readCheckpoint(reader));
            }
            KafkaPartitionPendingOperationRecord operation = readOperation(reader);
            long createdAtMillis = reader.readLong("createdAtMillis");
            long updatedAtMillis = reader.readLong("updatedAtMillis");
            long metadataVersion = reader.readLong("metadataVersion");
            reader.requireConsumed();
            return new KafkaPartitionBindingRecord(
                    formatVersion, kafkaClusterId, topicId, partitionId, observedTopicName, incarnation,
                    streamName, streamId, payloadMappingId, storageProfile, lifecycleId, bindingEpoch,
                    createdMetadataOffset, lastAppliedMetadataOffset, observedLeaderId, observedLeaderEpoch,
                    observedBrokerEpoch, observedLogStartOffset, observedStableEndOffset, coverage,
                    checkpoints, operation, createdAtMillis, updatedAtMillis, metadataVersion);
        } catch (RuntimeException failure) {
            throw malformed(failure);
        }
    }

    private static void writeCoverage(F4Binary.Writer writer, KafkaCompactionCoverageRecord value) {
        writer.writeInt(value.coverageVersion());
        writer.writeLong(value.startOffset());
        writer.writeLong(value.endOffset());
        writer.writeLong(value.activationEpoch());
        writer.writeBytes(value.generationSetSha256());
        writer.writeBytes(value.policySha256());
        writer.writeLong(value.activatedAtMillis());
    }

    private static KafkaCompactionCoverageRecord readCoverage(F4Binary.Reader reader) {
        return new KafkaCompactionCoverageRecord(
                reader.readInt("coverageVersion"), reader.readLong("coverageStartOffset"),
                reader.readLong("coverageEndOffset"), reader.readLong("coverageActivationEpoch"),
                reader.readBytes("generationSetSha256"), reader.readBytes("policySha256"),
                reader.readLong("coverageActivatedAtMillis"));
    }

    private static void writeCheckpoint(F4Binary.Writer writer, KafkaCheckpointReferenceRecord value) {
        writer.writeInt(value.referenceVersion());
        writer.writeString(value.objectId());
        writer.writeString(value.objectKey());
        writer.writeLong(value.objectLength());
        writer.writeBytes(value.objectSha256());
        writer.writeLong(value.checkpointOffset());
        writer.writeLong(value.logStartOffsetAtCheckpoint());
        writer.writeLong(value.sourceCommitVersion());
        writer.writeBytes(value.sourceHeadSha256());
        writer.writeString(value.writerBuild());
        writer.writeLong(value.createdAtMillis());
    }

    private static KafkaCheckpointReferenceRecord readCheckpoint(F4Binary.Reader reader) {
        return new KafkaCheckpointReferenceRecord(
                reader.readInt("referenceVersion"), reader.readString("checkpointObjectId"),
                reader.readString("checkpointObjectKey"), reader.readLong("checkpointObjectLength"),
                reader.readFixedBytes("checkpointObjectSha256", 32), reader.readLong("checkpointOffset"),
                reader.readLong("checkpointLogStartOffset"), reader.readLong("checkpointSourceCommitVersion"),
                reader.readFixedBytes("checkpointSourceHeadSha256", 32), reader.readString("checkpointWriterBuild"),
                reader.readLong("checkpointCreatedAtMillis"));
    }

    private static void writeOperation(F4Binary.Writer writer, KafkaPartitionPendingOperationRecord value) {
        writer.writeInt(value.operationTypeId());
        writer.writeString(value.attemptId());
        writer.writeString(value.ownerId());
        writer.writeLong(value.ownerEpoch());
        writer.writeLong(value.leaseExpiresAtMillis());
        writer.writeLong(value.targetMetadataOffset());
        writer.writeLong(value.startedAtMillis());
        writer.writeString(value.lastErrorCode());
    }

    private static KafkaPartitionPendingOperationRecord readOperation(F4Binary.Reader reader) {
        return new KafkaPartitionPendingOperationRecord(
                reader.readInt("operationTypeId"), reader.readString("operationAttemptId"),
                reader.readString("operationOwnerId"), reader.readLong("operationOwnerEpoch"),
                reader.readLong("operationLeaseExpiresAtMillis"), reader.readLong("operationTargetMetadataOffset"),
                reader.readLong("operationStartedAtMillis"), reader.readString("operationLastErrorCode"));
    }
}
