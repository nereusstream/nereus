/* Licensed under the Apache License, Version 2.0 */
package com.nereusstream.metadata.oxia;

import static org.assertj.core.api.Assertions.assertThatCode;
import static org.assertj.core.api.Assertions.assertThatThrownBy;

import com.nereusstream.api.ErrorCode;
import com.nereusstream.api.NereusException;
import com.nereusstream.metadata.oxia.records.GenerationIndexRecord;
import com.nereusstream.metadata.oxia.records.GenerationLifecycle;
import com.nereusstream.metadata.oxia.records.MaterializationCheckpointRecord;
import com.nereusstream.metadata.oxia.records.MaterializationPolicyRecord;
import com.nereusstream.metadata.oxia.records.MaterializationStreamRegistrationRecord;
import com.nereusstream.metadata.oxia.records.MaterializationTaskRecord;
import com.nereusstream.metadata.oxia.records.RangeRetentionStatsRecord;
import com.nereusstream.metadata.oxia.records.RecoveryCheckpointRootRecord;
import com.nereusstream.metadata.oxia.records.TaskLifecycle;
import org.junit.jupiter.api.Test;

class GenerationMetadataTransitionsTest {
    @Test
    void generationIndexAllowsOnlyTheClosedLifecycleAndImmutablePublicationIdentity() {
        GenerationIndexRecord prepared = F4MetadataTestValues.generation(GenerationLifecycle.PREPARED);
        GenerationIndexRecord committed = F4MetadataTestValues.generation(GenerationLifecycle.COMMITTED);
        GenerationIndexRecord draining = F4MetadataTestValues.generation(GenerationLifecycle.DRAINING);

        assertThatCode(() -> GenerationMetadataTransitions.requireValidIndexReplacement(prepared, committed))
                .doesNotThrowAnyException();
        assertInvariant(() -> GenerationMetadataTransitions.requireValidIndexReplacement(prepared, draining));
        assertInvariant(() -> GenerationMetadataTransitions.requireValidIndexReplacement(
                prepared, generationWithTask(committed, "different-task")));
        assertInvariant(() -> GenerationMetadataTransitions.requireValidIndexReplacement(committed, prepared));
    }

    @Test
    void taskLifecycleExpressesPublicationBeforeAllocationAndFreezesEveryWinningIdentity() {
        MaterializationTaskRecord planned = F4MetadataTestValues.task(TaskLifecycle.PLANNED);
        MaterializationTaskRecord claimed = F4MetadataTestValues.task(TaskLifecycle.CLAIMED);
        MaterializationTaskRecord outputReady = F4MetadataTestValues.task(TaskLifecycle.OUTPUT_READY);
        MaterializationTaskRecord publishing = F4MetadataTestValues.publishingTaskWithoutGeneration();
        MaterializationTaskRecord allocated = F4MetadataTestValues.task(TaskLifecycle.PUBLISHING);
        MaterializationTaskRecord published = F4MetadataTestValues.task(TaskLifecycle.PUBLISHED);

        assertThatCode(() -> GenerationMetadataTransitions.requireValidTaskReplacement(planned, claimed, 1_000))
                .doesNotThrowAnyException();
        assertThatCode(() -> GenerationMetadataTransitions.requireValidTaskReplacement(claimed, outputReady, 1_000))
                .doesNotThrowAnyException();
        assertThatCode(() -> GenerationMetadataTransitions.requireValidTaskReplacement(
                        outputReady, publishing, 1_000))
                .doesNotThrowAnyException();
        assertThatCode(() -> GenerationMetadataTransitions.requireValidTaskReplacement(
                        publishing, allocated, 1_000))
                .doesNotThrowAnyException();
        assertThatCode(() -> GenerationMetadataTransitions.requireValidTaskReplacement(
                        allocated, published, 1_000))
                .doesNotThrowAnyException();

        assertInvariant(() -> GenerationMetadataTransitions.requireValidTaskReplacement(
                outputReady, allocated, 1_000));
        assertInvariant(() -> GenerationMetadataTransitions.requireValidTaskReplacement(
                published, claimed, 1_000));
        assertInvariant(() -> GenerationMetadataTransitions.requireValidTaskReplacement(
                claimed, taskWithPolicy(outputReady, "other-policy"), 1_000));
    }

    @Test
    void advisoryAndRecoveryRootsRejectIdentityOrProgressRegression() {
        MaterializationCheckpointRecord empty = F4MetadataTestValues.emptyMaterializationCheckpoint();
        MaterializationCheckpointRecord advanced = F4MetadataTestValues.advancedMaterializationCheckpoint();
        assertThatCode(() -> GenerationMetadataTransitions.requireValidCheckpointReplacement(empty, advanced))
                .doesNotThrowAnyException();
        assertInvariant(() -> GenerationMetadataTransitions.requireValidCheckpointReplacement(advanced, empty));

        RangeRetentionStatsRecord stats = F4MetadataTestValues.retentionStats(0, 2, 1);
        assertThatCode(() -> GenerationMetadataTransitions.requireValidRetentionStatsReplacement(
                        stats, statsWithVerification(stats, 4_000)))
                .doesNotThrowAnyException();
        assertInvariant(() -> GenerationMetadataTransitions.requireValidRetentionStatsReplacement(
                stats, statsWithStart(stats, 1)));

        MaterializationStreamRegistrationRecord registration =
                F4MetadataTestValues.registration(F4MetadataTestValues.STREAM, 7);
        assertThatCode(() -> GenerationMetadataTransitions.requireValidRegistrationReplacement(
                        registration, registrationWithHint(registration, 8, 120)))
                .doesNotThrowAnyException();
        assertInvariant(() -> GenerationMetadataTransitions.requireValidRegistrationReplacement(
                registration, registrationWithHint(registration, 6, 120)));

        RecoveryCheckpointRootRecord bootstrap = F4MetadataTestValues.emptyRecoveryRoot();
        RecoveryCheckpointRootRecord full = F4MetadataTestValues.fullRecoveryRoot();
        assertThatCode(() -> GenerationMetadataTransitions.requireValidRecoveryRootReplacement(bootstrap, full))
                .doesNotThrowAnyException();
        assertInvariant(() -> GenerationMetadataTransitions.requireValidRecoveryRootReplacement(
                bootstrap, recoveryWithSequence(full, 2)));
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
                policyId, value.policyVersion(), value.policySha256(), policyWithId(value.policy(), policyId),
                value.lifecycle(), value.attempt(),
                value.workerClaim(), value.output(), value.allocatedGeneration(), value.publicationId(),
                value.failureClassId(), value.failureMessage(), value.retryNotBeforeMillis(), value.createdAtMillis(),
                value.updatedAtMillis(), 0);
    }

    private static MaterializationPolicyRecord policyWithId(
            MaterializationPolicyRecord value,
            String policyId) {
        return new MaterializationPolicyRecord(
                policyId, value.policyVersion(), value.readViewId(), value.taskKindId(),
                value.targetPhysicalFormat(), value.minMergeSourceRanges(), value.maxSourceRanges(),
                value.maxRangeRecords(), value.targetObjectBytes(), value.targetRowGroupRecords(),
                value.compression(), value.topicStrategyId(), value.topicStrategyVersion(), value.topicKeyCodecId());
    }

    private static RangeRetentionStatsRecord statsWithVerification(
            RangeRetentionStatsRecord value,
            long verifiedAtMillis) {
        return new RangeRetentionStatsRecord(
                value.schemaVersion(), value.streamId(), value.offsetStart(), value.offsetEnd(), value.commitVersion(),
                value.cumulativeSizeAtStart(), value.cumulativeSizeAtEnd(), value.minPublishTimeMillis(),
                value.maxPublishTimeMillis(), value.sourceIndexKey(), value.sourceIndexIdentitySha256(),
                value.sourceIndexMetadataVersion(), value.verifierBuild(), verifiedAtMillis, 0);
    }

    private static RangeRetentionStatsRecord statsWithStart(
            RangeRetentionStatsRecord value,
            long offsetStart) {
        return new RangeRetentionStatsRecord(
                value.schemaVersion(), value.streamId(), offsetStart, value.offsetEnd(), value.commitVersion(),
                value.cumulativeSizeAtStart(), value.cumulativeSizeAtEnd(), value.minPublishTimeMillis(),
                value.maxPublishTimeMillis(), value.sourceIndexKey(), value.sourceIndexIdentitySha256(),
                value.sourceIndexMetadataVersion(), value.verifierBuild(), value.verifiedAtMillis(), 0);
    }

    private static MaterializationStreamRegistrationRecord registrationWithHint(
            MaterializationStreamRegistrationRecord value,
            long hint,
            long updatedAtMillis) {
        return new MaterializationStreamRegistrationRecord(
                value.schemaVersion(), value.streamId(), value.projectionRef(), value.projectionIdentitySha256(),
                value.storageProfile(), value.registeredAtMillis(), hint, updatedAtMillis, 0);
    }

    private static RecoveryCheckpointRootRecord recoveryWithSequence(
            RecoveryCheckpointRootRecord value,
            long sequence) {
        return new RecoveryCheckpointRootRecord(
                value.schemaVersion(), value.streamId(), sequence, value.coveredStartOffset(), value.coveredEndOffset(),
                value.firstCommitVersion(), value.lastCommitVersion(), value.cumulativeSizeAtStart(),
                value.cumulativeSizeAtEnd(), value.firstCommitId(), value.lastCommitId(), value.checkpoints(),
                value.checkpointSetSha256(), value.sourceHeadCommitId(), value.sourceHeadCommitVersion(),
                value.publishedAtMillis(), 0);
    }

    private static void assertInvariant(Runnable operation) {
        assertThatThrownBy(operation::run)
                .isInstanceOfSatisfying(NereusException.class, error ->
                        org.assertj.core.api.Assertions.assertThat(error.code())
                                .isEqualTo(ErrorCode.METADATA_INVARIANT_VIOLATION));
    }
}
