/* Licensed under the Apache License, Version 2.0 */
package com.nereusstream.metadata.oxia;

import com.nereusstream.api.Checksum;
import com.nereusstream.api.PublicationId;
import com.nereusstream.api.ReadView;
import com.nereusstream.api.StreamId;
import com.nereusstream.metadata.oxia.records.GenerationIndexRecord;
import com.nereusstream.metadata.oxia.records.MaterializationCheckpointRecord;
import com.nereusstream.metadata.oxia.records.MaterializationStreamRegistrationRecord;
import com.nereusstream.metadata.oxia.records.MaterializationTaskRecord;
import com.nereusstream.metadata.oxia.records.RangeRetentionStatsRecord;
import com.nereusstream.metadata.oxia.records.RecoveryCheckpointRootRecord;
import java.util.Optional;
import java.util.concurrent.CompletableFuture;

/** Focused single-key/CAS metadata surface for generation publication and materialization workflow. */
public interface GenerationMetadataStore extends AutoCloseable {
    CompletableFuture<Optional<VersionedGenerationSequence>> getSequence(
            String cluster, StreamId streamId, ReadView view);

    CompletableFuture<VersionedGenerationSequence> getOrCreateSequence(
            String cluster, StreamId streamId, ReadView view);

    CompletableFuture<AllocatedGeneration> allocateGeneration(
            String cluster, StreamId streamId, ReadView view, PublicationId publicationId);

    CompletableFuture<Void> deleteSequence(
            String cluster, StreamId streamId, ReadView view, long expectedVersion);

    CompletableFuture<VersionedGenerationIndex> createPrepared(String cluster, GenerationIndexRecord record);

    CompletableFuture<VersionedGenerationIndex> compareAndSetIndex(
            String cluster, GenerationIndexRecord replacement, long expectedVersion);

    CompletableFuture<Optional<VersionedGenerationIndex>> getIndex(
            String cluster, GenerationIndexIdentity identity);

    CompletableFuture<GenerationScanPage> scanIndex(
            String cluster,
            StreamId streamId,
            ReadView view,
            long minOffsetEndInclusive,
            long maxOffsetEndInclusive,
            Optional<F4ScanToken> continuation,
            int limit);

    CompletableFuture<Void> deleteIndex(
            String cluster, GenerationIndexIdentity identity, long expectedVersion);

    CompletableFuture<VersionedMaterializationTask> createTask(String cluster, MaterializationTaskRecord task);

    CompletableFuture<Optional<VersionedMaterializationTask>> getTask(
            String cluster, StreamId streamId, String taskId);

    CompletableFuture<VersionedMaterializationTask> compareAndSetTask(
            String cluster, MaterializationTaskRecord task, long expectedVersion);

    CompletableFuture<TaskScanPage> scanTasks(
            String cluster, StreamId streamId, Optional<F4ScanToken> continuation, int limit);

    CompletableFuture<Void> deleteTask(
            String cluster, StreamId streamId, String taskId, long expectedVersion);

    CompletableFuture<Optional<VersionedMaterializationCheckpoint>> getMaterializationCheckpoint(
            String cluster, StreamId streamId, String policyId, long policyVersion);

    CompletableFuture<VersionedMaterializationCheckpoint> getOrCreateMaterializationCheckpoint(
            String cluster,
            StreamId streamId,
            String policyId,
            long policyVersion,
            Checksum policySha256);

    CompletableFuture<VersionedMaterializationCheckpoint> compareAndSetMaterializationCheckpoint(
            String cluster, MaterializationCheckpointRecord checkpoint, long expectedVersion);

    CompletableFuture<Void> deleteMaterializationCheckpoint(
            String cluster, StreamId streamId, String policyId, long policyVersion, long expectedVersion);

    CompletableFuture<VersionedRangeRetentionStats> createRangeRetentionStats(
            String cluster, RangeRetentionStatsRecord stats);

    CompletableFuture<Optional<VersionedRangeRetentionStats>> getRangeRetentionStats(
            String cluster, StreamId streamId, long offsetEnd, long commitVersion);

    CompletableFuture<VersionedRangeRetentionStats> compareAndSetRangeRetentionStats(
            String cluster, RangeRetentionStatsRecord stats, long expectedVersion);

    CompletableFuture<RangeRetentionStatsScanPage> scanRangeRetentionStats(
            String cluster,
            StreamId streamId,
            long minOffsetEndInclusive,
            long maxOffsetEndInclusive,
            Optional<F4ScanToken> continuation,
            int limit);

    CompletableFuture<Void> deleteRangeRetentionStats(
            String cluster, StreamId streamId, long offsetEnd, long commitVersion, long expectedVersion);

    CompletableFuture<VersionedMaterializationStreamRegistration> createOrVerifyStreamRegistration(
            String cluster, MaterializationStreamRegistrationRecord registration);

    CompletableFuture<Optional<VersionedMaterializationStreamRegistration>> getStreamRegistration(
            String cluster, StreamId streamId);

    CompletableFuture<VersionedMaterializationStreamRegistration> compareAndSetStreamRegistration(
            String cluster, MaterializationStreamRegistrationRecord registration, long expectedVersion);

    CompletableFuture<StreamRegistrationScanPage> scanStreamRegistrations(
            String cluster, int shard, Optional<F4ScanToken> continuation, int limit);

    CompletableFuture<Void> deleteStreamRegistration(
            String cluster, StreamId streamId, long expectedVersion);

    CompletableFuture<Optional<VersionedRecoveryCheckpointRoot>> getRecoveryRoot(
            String cluster, StreamId streamId);

    CompletableFuture<VersionedRecoveryCheckpointRoot> getOrCreateRecoveryRoot(
            String cluster, StreamId streamId);

    CompletableFuture<VersionedRecoveryCheckpointRoot> compareAndSetRecoveryRoot(
            String cluster, RecoveryCheckpointRootRecord root, long expectedVersion);

    CompletableFuture<Void> deleteRecoveryRoot(
            String cluster, StreamId streamId, long expectedVersion);

    @Override
    void close();
}
