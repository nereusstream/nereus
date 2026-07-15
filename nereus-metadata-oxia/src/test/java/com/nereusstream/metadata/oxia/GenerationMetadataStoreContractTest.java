/* Licensed under the Apache License, Version 2.0 */
package com.nereusstream.metadata.oxia;

import static com.nereusstream.metadata.oxia.F4MetadataTestValues.CLUSTER;
import static com.nereusstream.metadata.oxia.F4MetadataTestValues.HASH_B;
import static com.nereusstream.metadata.oxia.F4MetadataTestValues.STREAM;
import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.assertThatThrownBy;

import com.nereusstream.api.Checksum;
import com.nereusstream.api.ChecksumType;
import com.nereusstream.api.PublicationId;
import com.nereusstream.api.ReadView;
import com.nereusstream.api.StreamId;
import com.nereusstream.metadata.oxia.records.GenerationIndexRecord;
import com.nereusstream.metadata.oxia.records.GenerationLifecycle;
import com.nereusstream.metadata.oxia.records.MaterializationTaskRecord;
import com.nereusstream.metadata.oxia.records.TaskLifecycle;
import java.time.Clock;
import java.time.Instant;
import java.time.ZoneOffset;
import java.util.Optional;
import java.util.concurrent.CompletionException;
import org.junit.jupiter.api.Test;

class GenerationMetadataStoreContractTest {
    private static final StreamId STREAM_ID = new StreamId(STREAM);

    @Test
    void allocatesViewScopedGenerationsIdempotentlyAndConditionallyDeletesSequence() {
        GenerationMetadataStore store = store();
        PublicationId firstPublication = new PublicationId("p".repeat(26));
        PublicationId secondPublication = new PublicationId("q".repeat(26));

        assertThat(store.getSequence(CLUSTER, STREAM_ID, ReadView.COMMITTED).join()).isEmpty();
        VersionedGenerationSequence bootstrap = store.getOrCreateSequence(
                CLUSTER, STREAM_ID, ReadView.COMMITTED).join();
        assertThat(store.getOrCreateSequence(CLUSTER, STREAM_ID, ReadView.COMMITTED).join())
                .isEqualTo(bootstrap);

        AllocatedGeneration first = store.allocateGeneration(
                CLUSTER, STREAM_ID, ReadView.COMMITTED, firstPublication).join();
        AllocatedGeneration duplicate = store.allocateGeneration(
                CLUSTER, STREAM_ID, ReadView.COMMITTED, firstPublication).join();
        AllocatedGeneration second = store.allocateGeneration(
                CLUSTER, STREAM_ID, ReadView.COMMITTED, secondPublication).join();
        AllocatedGeneration otherView = store.allocateGeneration(
                CLUSTER, STREAM_ID, ReadView.TOPIC_COMPACTED, firstPublication).join();

        assertThat(first.generation().value()).isOne();
        assertThat(duplicate).isEqualTo(first);
        assertThat(second.generation().value()).isEqualTo(2);
        assertThat(otherView.generation().value()).isOne();

        VersionedGenerationSequence current = store.getSequence(
                CLUSTER, STREAM_ID, ReadView.COMMITTED).join().orElseThrow();
        assertThatThrownBy(() -> store.deleteSequence(
                        CLUSTER, STREAM_ID, ReadView.COMMITTED, bootstrap.metadataVersion()).join())
                .hasCauseInstanceOf(F4MetadataConditionFailedException.class);
        assertThat(store.getSequence(CLUSTER, STREAM_ID, ReadView.COMMITTED).join()).isPresent();
        store.deleteSequence(
                CLUSTER, STREAM_ID, ReadView.COMMITTED, current.metadataVersion()).join();
        assertThat(store.getSequence(CLUSTER, STREAM_ID, ReadView.COMMITTED).join()).isEmpty();
    }

    @Test
    void createPreparedIsIdempotentAcrossLifecycleAdvanceAndRejectsIdentityCollision() {
        GenerationMetadataStore store = store();
        GenerationIndexRecord prepared = F4MetadataTestValues.generation(GenerationLifecycle.PREPARED);
        GenerationIndexIdentity identity = new GenerationIndexIdentity(
                STREAM_ID, ReadView.COMMITTED, prepared.offsetEnd(), prepared.generation());

        VersionedGenerationIndex created = store.createPrepared(CLUSTER, prepared).join();
        assertThat(store.createPrepared(CLUSTER, prepared).join()).isEqualTo(created);

        VersionedGenerationIndex committed = store.compareAndSetIndex(
                CLUSTER,
                F4MetadataTestValues.generation(GenerationLifecycle.COMMITTED),
                created.metadataVersion()).join();
        assertThat(committed.value().lifecycle()).isEqualTo(GenerationLifecycle.COMMITTED);
        assertThat(store.createPrepared(CLUSTER, prepared).join()).isEqualTo(committed);
        assertThat(store.getIndex(CLUSTER, identity).join()).contains(committed);

        assertThatThrownBy(() -> store.createPrepared(
                        CLUSTER, generationWithTask(prepared, "other-task")).join())
                .satisfies(error -> assertThat(unwrap(error))
                        .isInstanceOfSatisfying(com.nereusstream.api.NereusException.class, nereus ->
                                assertThat(nereus.code()).isEqualTo(
                                        com.nereusstream.api.ErrorCode.METADATA_INVARIANT_VIOLATION)));
        assertThatThrownBy(() -> store.compareAndSetIndex(
                        CLUSTER,
                        F4MetadataTestValues.generation(GenerationLifecycle.DRAINING),
                        created.metadataVersion()).join())
                .hasCauseInstanceOf(F4MetadataConditionFailedException.class);

        GenerationScanPage page = store.scanIndex(
                CLUSTER, STREAM_ID, ReadView.COMMITTED, 1, 2, Optional.empty(), 1).join();
        assertThat(page.values()).containsExactly(committed);
        assertThat(page.continuation()).isPresent();
        assertThat(store.scanIndex(
                        CLUSTER,
                        STREAM_ID,
                        ReadView.COMMITTED,
                        1,
                        2,
                        page.continuation(),
                        1).join().values())
                .isEmpty();

        store.deleteIndex(CLUSTER, identity, committed.metadataVersion()).join();
        assertThat(store.getIndex(CLUSTER, identity).join()).isEmpty();
    }

    @Test
    void taskCheckpointRetentionAndRecoveryRootsUseExactCreateCasScanDeleteContracts() {
        GenerationMetadataStore store = store();

        MaterializationTaskRecord planned = F4MetadataTestValues.task(TaskLifecycle.PLANNED);
        VersionedMaterializationTask task = store.createTask(CLUSTER, planned).join();
        assertThat(store.createTask(CLUSTER, planned).join()).isEqualTo(task);
        VersionedMaterializationTask claimed = store.compareAndSetTask(
                CLUSTER,
                F4MetadataTestValues.task(TaskLifecycle.CLAIMED),
                task.metadataVersion()).join();
        assertThat(store.createTask(CLUSTER, planned).join()).isEqualTo(claimed);
        assertThatThrownBy(() -> store.createTask(
                        CLUSTER, taskWithPolicy(planned, "other-policy")).join())
                .satisfies(error -> assertThat(unwrap(error))
                        .isInstanceOf(com.nereusstream.api.NereusException.class));
        assertThat(store.scanTasks(CLUSTER, STREAM_ID, Optional.empty(), 10).join().values())
                .containsExactly(claimed);

        Checksum policy = new Checksum(ChecksumType.SHA256, HASH_B);
        VersionedMaterializationCheckpoint checkpoint = store.getOrCreateMaterializationCheckpoint(
                CLUSTER, STREAM_ID, "policy-f4", 1, policy).join();
        assertThat(store.getOrCreateMaterializationCheckpoint(
                        CLUSTER, STREAM_ID, "policy-f4", 1, policy).join())
                .isEqualTo(checkpoint);
        assertThatThrownBy(() -> store.getOrCreateMaterializationCheckpoint(
                        CLUSTER,
                        STREAM_ID,
                        "policy-f4",
                        1,
                        new Checksum(ChecksumType.SHA256, "f".repeat(64))).join())
                .satisfies(error -> assertThat(unwrap(error))
                        .isInstanceOf(com.nereusstream.api.NereusException.class));
        VersionedMaterializationCheckpoint advanced = store.compareAndSetMaterializationCheckpoint(
                CLUSTER,
                F4MetadataTestValues.advancedMaterializationCheckpoint(),
                checkpoint.metadataVersion()).join();

        VersionedRangeRetentionStats stats1 = store.createRangeRetentionStats(
                CLUSTER, F4MetadataTestValues.retentionStats(0, 2, 1)).join();
        assertThat(store.createRangeRetentionStats(
                        CLUSTER, F4MetadataTestValues.retentionStats(0, 2, 1)).join())
                .isEqualTo(stats1);
        VersionedRangeRetentionStats stats2 = store.createRangeRetentionStats(
                CLUSTER, F4MetadataTestValues.retentionStats(2, 4, 2)).join();
        RangeRetentionStatsScanPage firstPage = store.scanRangeRetentionStats(
                CLUSTER, STREAM_ID, 2, 4, Optional.empty(), 1).join();
        assertThat(firstPage.values()).containsExactly(stats1);
        assertThat(store.scanRangeRetentionStats(
                        CLUSTER, STREAM_ID, 2, 4, firstPage.continuation(), 1).join().values())
                .containsExactly(stats2);

        VersionedRecoveryCheckpointRoot empty = store.getOrCreateRecoveryRoot(CLUSTER, STREAM_ID).join();
        assertThat(store.getOrCreateRecoveryRoot(CLUSTER, STREAM_ID).join()).isEqualTo(empty);
        VersionedRecoveryCheckpointRoot full = store.compareAndSetRecoveryRoot(
                CLUSTER,
                F4MetadataTestValues.fullRecoveryRoot(),
                empty.metadataVersion()).join();
        assertThat(store.getRecoveryRoot(CLUSTER, STREAM_ID).join()).contains(full);

        store.deleteTask(CLUSTER, STREAM_ID, planned.taskId(), claimed.metadataVersion()).join();
        store.deleteMaterializationCheckpoint(
                CLUSTER, STREAM_ID, "policy-f4", 1, advanced.metadataVersion()).join();
        store.deleteRangeRetentionStats(
                CLUSTER, STREAM_ID, 2, 1, stats1.metadataVersion()).join();
        store.deleteRangeRetentionStats(
                CLUSTER, STREAM_ID, 4, 2, stats2.metadataVersion()).join();
        store.deleteRecoveryRoot(CLUSTER, STREAM_ID, full.metadataVersion()).join();
    }

    private static OxiaJavaGenerationMetadataStore store() {
        return new OxiaJavaGenerationMetadataStore(
                new PartitionedOxiaClient(new InMemoryPartitionedOxiaBackend()),
                Clock.fixed(Instant.ofEpochMilli(1_000), ZoneOffset.UTC));
    }

    private static GenerationIndexRecord generationWithTask(
            GenerationIndexRecord value,
            String taskId) {
        return new GenerationIndexRecord(
                value.schemaVersion(), value.streamId(), value.readViewId(), value.offsetStart(), value.offsetEnd(),
                value.generation(), value.publicationId(), taskId, value.lifecycle(), value.sourceSetSha256(),
                value.policySha256(), value.readTarget(), value.targetIdentitySha256(),
                value.materializationPolicySha256(), value.payloadFormat(), value.sourceRecordCount(),
                value.outputRecordCount(), value.entryCount(), value.logicalBytes(), value.cumulativeSizeAtStart(),
                value.cumulativeSizeAtEnd(), value.firstCommitVersion(), value.lastCommitVersion(),
                value.schemaRefs(), value.projectionRef(), value.createdAtMillis(), value.committedAtMillis(),
                value.stateReason(), value.stateChangedAtMillis(), 0);
    }

    private static MaterializationTaskRecord taskWithPolicy(
            MaterializationTaskRecord value,
            String policyId) {
        return new MaterializationTaskRecord(
                value.schemaVersion(), value.taskId(), value.taskSequence(), value.streamId(), value.readViewId(),
                value.taskKindId(), value.offsetStart(), value.offsetEnd(), value.sources(), value.sourceSetSha256(),
                policyId, value.policyVersion(), value.policySha256(), value.lifecycle(), value.attempt(),
                value.workerClaim(), value.output(), value.allocatedGeneration(), value.publicationId(),
                value.failureClassId(), value.failureMessage(), value.retryNotBeforeMillis(), value.createdAtMillis(),
                value.updatedAtMillis(), 0);
    }

    private static Throwable unwrap(Throwable supplied) {
        Throwable current = supplied;
        while (current instanceof CompletionException && current.getCause() != null) {
            current = current.getCause();
        }
        return current;
    }
}
