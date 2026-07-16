/* Licensed under the Apache License, Version 2.0 */
package com.nereusstream.metadata.oxia;

import static com.nereusstream.metadata.oxia.F4MetadataTestValues.HASH_A;
import static com.nereusstream.metadata.oxia.F4MetadataTestValues.HASH_B;
import static com.nereusstream.metadata.oxia.F4MetadataTestValues.STREAM;
import static org.assertj.core.api.Assertions.assertThatThrownBy;

import com.nereusstream.api.ReadView;
import com.nereusstream.metadata.oxia.records.GenerationBackfillProofRecord;
import com.nereusstream.metadata.oxia.records.GenerationIndexRecord;
import com.nereusstream.metadata.oxia.records.GenerationLifecycle;
import com.nereusstream.metadata.oxia.records.GenerationProtocolActivationLifecycle;
import com.nereusstream.metadata.oxia.records.GenerationProtocolActivationRecord;
import com.nereusstream.metadata.oxia.records.GenerationSequenceRecord;
import com.nereusstream.metadata.oxia.records.GcDomainSnapshotProofRecord;
import com.nereusstream.metadata.oxia.records.GcRetirementManifestRecord;
import com.nereusstream.metadata.oxia.records.GcRetirementProtectionRecord;
import com.nereusstream.metadata.oxia.records.GcRetirementRemovalRecord;
import com.nereusstream.metadata.oxia.records.MaterializationCheckpointRecord;
import com.nereusstream.metadata.oxia.records.MaterializationTaskRecord;
import com.nereusstream.metadata.oxia.records.ObjectProtectionRecord;
import com.nereusstream.metadata.oxia.records.ObjectProtectionType;
import com.nereusstream.metadata.oxia.records.ObjectReaderLeaseRecord;
import com.nereusstream.metadata.oxia.records.PhysicalObjectLifecycle;
import com.nereusstream.metadata.oxia.records.PhysicalObjectRootRecord;
import com.nereusstream.metadata.oxia.records.RangeRetentionStatsRecord;
import com.nereusstream.metadata.oxia.records.RecoveryCheckpointRootRecord;
import com.nereusstream.metadata.oxia.records.ReferenceDomainVersionRecord;
import com.nereusstream.metadata.oxia.records.TaskLifecycle;
import java.util.List;
import org.junit.jupiter.api.Test;

class F4RecordValidationTest {
    @Test
    void rejectsGenerationAllocatorPublicationAndLifecycleContradictions() {
        assertThatThrownBy(() -> new GenerationSequenceRecord(
                        1, STREAM, ReadView.COMMITTED.wireId(), 2, 1, "p".repeat(26), 100, 0))
                .isInstanceOf(IllegalArgumentException.class);

        GenerationIndexRecord prepared = F4MetadataTestValues.generation(GenerationLifecycle.PREPARED);
        assertThatThrownBy(() -> generationLifecycle(prepared, GenerationLifecycle.COMMITTED, 0, ""))
                .isInstanceOf(IllegalArgumentException.class);
        assertThatThrownBy(() -> generationLifecycle(prepared, GenerationLifecycle.ABORTED, 0, ""))
                .isInstanceOf(IllegalArgumentException.class);
    }

    @Test
    void rejectsTaskCheckpointStatsAndRecoveryStateContradictions() {
        MaterializationTaskRecord planned = F4MetadataTestValues.task(TaskLifecycle.PLANNED);
        org.assertj.core.api.Assertions.assertThatCode(
                        F4MetadataTestValues::publishingTaskWithoutGeneration)
                .doesNotThrowAnyException();
        assertThatThrownBy(() -> taskAttempt(planned, 1))
                .isInstanceOf(IllegalArgumentException.class);
        assertThatThrownBy(() -> new MaterializationCheckpointRecord(
                        1, STREAM, "policy", 1, HASH_A, 0, 0, 1, "", 100, 0))
                .isInstanceOf(IllegalArgumentException.class);
        assertThatThrownBy(() -> new RangeRetentionStatsRecord(
                        1, STREAM, 0, 2, 1, 0, 10, 200, 100,
                        "/index", HASH_A, 1, "build", 300, 0))
                .isInstanceOf(IllegalArgumentException.class);
        assertThatThrownBy(() -> new RecoveryCheckpointRootRecord(
                        1, STREAM, 1, 0, 0, 0, 0, 0, 0,
                        "", "", List.of(), "", "", 0, 0, 0))
                .isInstanceOf(IllegalArgumentException.class);
    }

    @Test
    void rejectsPhysicalRootLeaseAndProtectionSafetyContradictions() {
        PhysicalObjectRootRecord active = F4MetadataTestValues.physicalRoot(PhysicalObjectLifecycle.ACTIVE);
        assertThatThrownBy(() -> activeWithTombstone(active))
                .isInstanceOf(IllegalArgumentException.class);
        assertThatThrownBy(() -> new ObjectReaderLeaseRecord(
                        1,
                        active.objectKeyHash(),
                        F4MetadataTestValues.PROCESS,
                        F4MetadataTestValues.LEASE,
                        1,
                        100,
                        200,
                        201,
                        0,
                        0))
                .isInstanceOf(IllegalArgumentException.class);
        assertThatThrownBy(() -> new ObjectProtectionRecord(
                        1,
                        active.objectKeyHash(),
                        ObjectProtectionType.CURSOR_SNAPSHOT_PENDING.wireId(),
                        "pending",
                        "/owner",
                        1,
                        HASH_B,
                        1,
                        100,
                        100,
                        0))
                .isInstanceOf(IllegalArgumentException.class);
        assertThatThrownBy(() -> new ObjectProtectionRecord(
                        1,
                        active.objectKeyHash(),
                        ObjectProtectionType.VISIBLE_GENERATION.wireId(),
                        "permanent",
                        "/owner",
                        1,
                        HASH_B,
                        1,
                        100,
                        200,
                        0))
                .isInstanceOf(IllegalArgumentException.class);
    }

    @Test
    void rejectsRetirementJournalIdentityCountAndOrderingContradictions() {
        GcRetirementManifestRecord manifest = F4MetadataTestValues.gcRetirementManifest();
        GcDomainSnapshotProofRecord proof = manifest.domainProofs().getFirst();
        assertThatThrownBy(() -> new GcRetirementManifestRecord(
                        1,
                        manifest.objectKeyHash(),
                        manifest.gcAttemptId(),
                        1,
                        manifest.queryIdentitySha256(),
                        manifest.domainProofs(),
                        manifest.protectionCount(),
                        manifest.metadataRemovalCount(),
                        manifest.referenceSetSha256(),
                        manifest.createdAtMillis(),
                        0))
                .isInstanceOf(IllegalArgumentException.class)
                .hasMessageContaining("must be 2");
        assertThatThrownBy(() -> new GcRetirementManifestRecord(
                        1,
                        manifest.objectKeyHash(),
                        manifest.gcAttemptId(),
                        GcRetirementManifestRecord.REFERENCE_SET_PROTOCOL_VERSION,
                        manifest.queryIdentitySha256(),
                        List.of(proof, proof),
                        manifest.protectionCount(),
                        manifest.metadataRemovalCount(),
                        manifest.referenceSetSha256(),
                        manifest.createdAtMillis(),
                        0))
                .isInstanceOf(IllegalArgumentException.class)
                .hasMessageContaining("sorted and unique");

        GcRetirementProtectionRecord protection = F4MetadataTestValues.gcRetirementProtection();
        assertThatThrownBy(() -> new GcRetirementProtectionRecord(
                        1,
                        protection.objectKeyHash(),
                        protection.gcAttemptId(),
                        protection.protectionKey(),
                        protection.protectionMetadataVersion() + 1,
                        protection.protectionDurableValueSha256(),
                        protection.protection(),
                        0))
                .isInstanceOf(IllegalArgumentException.class)
                .hasMessageContaining("object/version identity");

        GcRetirementRemovalRecord removal = F4MetadataTestValues.gcRetirementRemoval();
        assertThatThrownBy(() -> new GcRetirementRemovalRecord(
                        1,
                        removal.objectKeyHash(),
                        removal.gcAttemptId(),
                        "Generation Index",
                        removal.removalKey(),
                        removal.removalMetadataVersion(),
                        removal.removalDurableValueSha256(),
                        0))
                .isInstanceOf(IllegalArgumentException.class)
                .hasMessageContaining("not canonical");
    }

    @Test
    void rejectsActivationBackfillFactsFromAFutureReadinessEpoch() {
        assertThatThrownBy(() -> new GenerationProtocolActivationRecord(
                        1,
                        1,
                        GenerationProtocolActivationLifecycle.ACTIVE,
                        true,
                        false,
                        false,
                        7,
                        F4MetadataTestValues.referenceDomains(),
                        GenerationBackfillProofRecord.incomplete(8),
                        GenerationBackfillProofRecord.incomplete(7),
                        GenerationBackfillProofRecord.incomplete(7),
                        "",
                        F4MetadataTestValues.PROCESS,
                        100,
                        150,
                        150,
                        0))
                .isInstanceOf(IllegalArgumentException.class)
                .hasMessageContaining("cannot be newer");
    }

    @Test
    void rejectsMultipleProtocolVersionsForOneActivationDomainId() {
        List<ReferenceDomainVersionRecord> domains = new java.util.ArrayList<>(
                F4MetadataTestValues.referenceDomains());
        domains.add(1, new ReferenceDomainVersionRecord("append-recovery-v1", 2));

        assertThatThrownBy(() -> new GenerationProtocolActivationRecord(
                        1,
                        1,
                        GenerationProtocolActivationLifecycle.PREPARED,
                        false,
                        false,
                        false,
                        0,
                        domains,
                        GenerationBackfillProofRecord.incomplete(0),
                        GenerationBackfillProofRecord.incomplete(0),
                        GenerationBackfillProofRecord.incomplete(0),
                        "",
                        F4MetadataTestValues.PROCESS,
                        100,
                        0,
                        100,
                        0))
                .isInstanceOf(IllegalArgumentException.class)
                .hasMessageContaining("unique domain ids");
    }

    private static GenerationIndexRecord generationLifecycle(
            GenerationIndexRecord value,
            GenerationLifecycle lifecycle,
            long committedAt,
            String reason) {
        return new GenerationIndexRecord(
                value.schemaVersion(), value.streamId(), value.readViewId(), value.offsetStart(), value.offsetEnd(),
                value.generation(), value.publicationId(), value.taskId(), lifecycle, value.sourceSetSha256(),
                value.policySha256(), value.readTarget(), value.targetIdentitySha256(),
                value.materializationPolicySha256(), value.payloadFormat(), value.sourceRecordCount(),
                value.outputRecordCount(), value.entryCount(), value.logicalBytes(), value.cumulativeSizeAtStart(),
                value.cumulativeSizeAtEnd(), value.firstCommitVersion(), value.lastCommitVersion(),
                value.schemaRefs(), value.projectionRef(), value.createdAtMillis(), committedAt, reason,
                value.stateChangedAtMillis(), 0);
    }

    private static MaterializationTaskRecord taskAttempt(
            MaterializationTaskRecord value,
            long attempt) {
        return new MaterializationTaskRecord(
                value.schemaVersion(), value.taskId(), value.taskSequence(), value.streamId(), value.readViewId(),
                value.taskKindId(), value.offsetStart(), value.offsetEnd(), value.sources(), value.sourceSetSha256(),
                value.policyId(), value.policyVersion(), value.policySha256(), value.policy(), value.lifecycle(), attempt,
                value.workerClaim(), value.output(), value.allocatedGeneration(), value.publicationId(),
                value.failureClassId(), value.failureMessage(), value.retryNotBeforeMillis(), value.createdAtMillis(),
                value.updatedAtMillis(), 0);
    }

    private static PhysicalObjectRootRecord activeWithTombstone(PhysicalObjectRootRecord value) {
        return new PhysicalObjectRootRecord(
                value.schemaVersion(), value.objectKeyHash(), value.objectKey(), value.objectId(),
                value.objectKindId(), value.objectLength(), value.storageChecksumType(), value.storageChecksumValue(),
                value.contentSha256(), value.etag(), value.lifecycle(), value.lifecycleEpoch(),
                value.createdAtMillis(), value.orphanNotBeforeMillis(), value.gcAttemptId(),
                value.referenceSetSha256(), value.markedAtMillis(), value.deleteNotBeforeMillis(),
                value.deleteStartedAtMillis(), value.deletedAtMillis(), 300, HASH_A, value.stateReason(), 0);
    }
}
