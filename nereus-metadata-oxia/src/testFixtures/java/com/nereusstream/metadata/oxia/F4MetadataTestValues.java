/* Licensed under the Apache License, Version 2.0 */
package com.nereusstream.metadata.oxia;

import com.nereusstream.api.Checksum;
import com.nereusstream.api.ChecksumType;
import com.nereusstream.api.EntryIndexLocation;
import com.nereusstream.api.EntryIndexRef;
import com.nereusstream.api.ObjectId;
import com.nereusstream.api.ObjectKey;
import com.nereusstream.api.ObjectKeyHash;
import com.nereusstream.api.ObjectType;
import com.nereusstream.api.PayloadFormat;
import com.nereusstream.api.ReadView;
import com.nereusstream.api.SchemaRef;
import com.nereusstream.api.StorageProfile;
import com.nereusstream.api.target.ObjectSliceReadTarget;
import com.nereusstream.metadata.oxia.codec.ReadTargetCodecRegistry;
import com.nereusstream.metadata.oxia.records.GenerationIndexRecord;
import com.nereusstream.metadata.oxia.records.GenerationLifecycle;
import com.nereusstream.metadata.oxia.records.GenerationSequenceRecord;
import com.nereusstream.metadata.oxia.records.MaterializationCheckpointRecord;
import com.nereusstream.metadata.oxia.records.MaterializationOutputRecord;
import com.nereusstream.metadata.oxia.records.MaterializationPolicyRecord;
import com.nereusstream.metadata.oxia.records.MaterializationStreamRegistrationRecord;
import com.nereusstream.metadata.oxia.records.MaterializationTaskRecord;
import com.nereusstream.metadata.oxia.records.ObjectProtectionRecord;
import com.nereusstream.metadata.oxia.records.ObjectProtectionType;
import com.nereusstream.metadata.oxia.records.ObjectReaderLeaseRecord;
import com.nereusstream.metadata.oxia.records.PhysicalObjectLifecycle;
import com.nereusstream.metadata.oxia.records.PhysicalObjectRootRecord;
import com.nereusstream.metadata.oxia.records.RangeRetentionStatsRecord;
import com.nereusstream.metadata.oxia.records.ReadTargetRecord;
import com.nereusstream.metadata.oxia.records.RecoveryCheckpointReferenceRecord;
import com.nereusstream.metadata.oxia.records.RecoveryCheckpointRootRecord;
import com.nereusstream.metadata.oxia.records.SourceGenerationRecord;
import com.nereusstream.metadata.oxia.records.TaskFailureClass;
import com.nereusstream.metadata.oxia.records.TaskLifecycle;
import com.nereusstream.metadata.oxia.records.WorkerClaimRecord;
import java.util.List;
import java.util.Optional;
import java.util.OptionalLong;

/** Canonical complete-branch values shared by F4 codec and metadata-store contracts. */
public final class F4MetadataTestValues {
    public static final String CLUSTER = "cluster-f4";
    public static final String STREAM = "stream-f4";
    public static final String PUBLICATION = "p".repeat(26);
    public static final String ATTEMPT = "a".repeat(26);
    public static final String CLAIM = "c".repeat(26);
    public static final String PROCESS = "d".repeat(26);
    public static final String LEASE = "e".repeat(26);
    public static final String HASH_A = "a".repeat(64);
    public static final String HASH_B = "b".repeat(64);
    public static final String HASH_C = "c".repeat(64);
    public static final String HASH_D = "d".repeat(64);
    public static final String HASH_E = "e".repeat(64);

    private static final ObjectId OBJECT_ID = new ObjectId("object-f4");
    private static final ObjectKey OBJECT_KEY = new ObjectKey("objects/f4/compacted-object");
    private static final List<SchemaRef> SCHEMAS = List.of(new SchemaRef("pulsar", "schema-a", 3));

    private F4MetadataTestValues() {
    }

    public static ReadTargetRecord readTarget() {
        EntryIndexRef entryIndex = new EntryIndexRef(
                EntryIndexLocation.OBJECT_FOOTER,
                Optional.of(OBJECT_ID),
                Optional.of(OBJECT_KEY),
                Optional.empty(),
                96,
                32,
                new Checksum(ChecksumType.CRC32C, "01020304"));
        ObjectSliceReadTarget target = new ObjectSliceReadTarget(
                1,
                OBJECT_ID,
                OBJECT_KEY,
                ObjectType.STREAM_COMPACTED_OBJECT,
                "NEREUS_COMPACTED_PARQUET_V1",
                PayloadFormat.PULSAR_ENTRY_BATCH.name(),
                "slice-f4",
                0,
                128,
                new Checksum(ChecksumType.CRC32C, "05060708"),
                entryIndex);
        return ReadTargetCodecRegistry.phase15().encode(target);
    }

    public static GenerationSequenceRecord emptySequence() {
        return new GenerationSequenceRecord(1, STREAM, ReadView.COMMITTED.wireId(), 0, 0, "", 100, 0);
    }

    public static GenerationSequenceRecord allocatedSequence() {
        return new GenerationSequenceRecord(
                1, STREAM, ReadView.COMMITTED.wireId(), 7, 7, PUBLICATION, 101, 0);
    }

    public static GenerationIndexRecord generation(GenerationLifecycle lifecycle) {
        return generation(lifecycle, ReadView.COMMITTED, false);
    }

    public static GenerationIndexRecord topicCompactedGeneration() {
        return generation(GenerationLifecycle.COMMITTED, ReadView.TOPIC_COMPACTED, true);
    }

    private static GenerationIndexRecord generation(
            GenerationLifecycle lifecycle,
            ReadView view,
            boolean sparse) {
        boolean visible = lifecycle == GenerationLifecycle.COMMITTED
                || lifecycle == GenerationLifecycle.QUARANTINED
                || lifecycle == GenerationLifecycle.DRAINING
                || lifecycle == GenerationLifecycle.RETIRED;
        boolean reason = lifecycle == GenerationLifecycle.QUARANTINED
                || lifecycle == GenerationLifecycle.DRAINING
                || lifecycle == GenerationLifecycle.RETIRED
                || lifecycle == GenerationLifecycle.ABORTED;
        ReadTargetRecord target = readTarget();
        return new GenerationIndexRecord(
                1,
                STREAM,
                view.wireId(),
                0,
                2,
                3,
                PUBLICATION,
                "task-f4",
                lifecycle,
                HASH_A,
                HASH_B,
                target,
                target.identityChecksumValue(),
                HASH_B,
                PayloadFormat.PULSAR_ENTRY_BATCH.name(),
                2,
                sparse ? 1 : 2,
                2,
                100,
                0,
                100,
                1,
                1,
                SCHEMAS,
                "projection-f4",
                100,
                visible ? 110 : 0,
                reason ? lifecycle.name().toLowerCase() : "",
                120,
                0);
    }

    public static MaterializationStreamRegistrationRecord registration(String stream, long hint) {
        return new MaterializationStreamRegistrationRecord(
                1,
                stream,
                "projection-" + stream,
                HASH_C,
                StorageProfile.OBJECT_WAL_SYNC_OBJECT.name(),
                100,
                hint,
                110,
                0);
    }

    public static SourceGenerationRecord source() {
        ReadTargetRecord target = readTarget();
        return new SourceGenerationRecord(
                ReadView.COMMITTED.wireId(),
                0,
                2,
                0,
                1,
                "/source/index/f4",
                4,
                HASH_A,
                target,
                target.identityChecksumValue(),
                "",
                PayloadFormat.PULSAR_ENTRY_BATCH.name(),
                "projection-f4",
                2,
                2,
                100,
                SCHEMAS,
                0,
                100);
    }

    public static MaterializationOutputRecord output() {
        ReadTargetRecord target = readTarget();
        return new MaterializationOutputRecord(
                CLAIM,
                OBJECT_ID.value(),
                OBJECT_KEY.value(),
                ObjectKeyHash.from(OBJECT_KEY).value(),
                128,
                "01020304",
                HASH_D,
                "etag-f4",
                "NEREUS_COMPACTED_PARQUET_V1",
                PayloadFormat.PULSAR_ENTRY_BATCH.name(),
                target,
                target.identityChecksumValue(),
                2,
                2,
                2,
                100,
                SCHEMAS,
                0,
                100,
                HASH_A,
                "projection-f4");
    }

    public static MaterializationTaskRecord task(TaskLifecycle lifecycle) {
        boolean claimed = lifecycle == TaskLifecycle.CLAIMED;
        boolean outputReady = lifecycle == TaskLifecycle.OUTPUT_READY
                || lifecycle == TaskLifecycle.PUBLISHING
                || lifecycle == TaskLifecycle.PUBLISHED;
        boolean publishing = lifecycle == TaskLifecycle.PUBLISHING
                || lifecycle == TaskLifecycle.PUBLISHED;
        boolean failed = lifecycle == TaskLifecycle.RETRY_WAIT
                || lifecycle == TaskLifecycle.CANCELLED
                || lifecycle == TaskLifecycle.TERMINAL_FAILED;
        long attempt = lifecycle == TaskLifecycle.PLANNED ? 0 : 1;
        long updatedAt = 200;
        return new MaterializationTaskRecord(
                1,
                "task-f4",
                1,
                STREAM,
                ReadView.COMMITTED.wireId(),
                1,
                0,
                2,
                List.of(source()),
                HASH_A,
                "policy-f4",
                1,
                HASH_B,
                policy(),
                lifecycle,
                attempt,
                claimed
                        ? Optional.of(new WorkerClaimRecord(CLAIM, PROCESS, 1, 120, 220))
                        : Optional.empty(),
                outputReady ? Optional.of(output()) : Optional.empty(),
                publishing ? OptionalLong.of(3) : OptionalLong.empty(),
                publishing ? PUBLICATION : "",
                failed ? TaskFailureClass.RETRYABLE_METADATA.wireId() : TaskFailureClass.NONE.wireId(),
                failed ? "retryable failure" : "",
                lifecycle == TaskLifecycle.RETRY_WAIT ? 300 : 0,
                100,
                updatedAt,
                0);
    }

    public static MaterializationTaskRecord publishingTaskWithoutGeneration() {
        MaterializationTaskRecord allocated = task(TaskLifecycle.PUBLISHING);
        return new MaterializationTaskRecord(
                allocated.schemaVersion(), allocated.taskId(), allocated.taskSequence(), allocated.streamId(),
                allocated.readViewId(), allocated.taskKindId(), allocated.offsetStart(), allocated.offsetEnd(),
                allocated.sources(), allocated.sourceSetSha256(), allocated.policyId(), allocated.policyVersion(),
                allocated.policySha256(), allocated.policy(), allocated.lifecycle(), allocated.attempt(), allocated.workerClaim(),
                allocated.output(), OptionalLong.empty(), allocated.publicationId(), allocated.failureClassId(),
                allocated.failureMessage(), allocated.retryNotBeforeMillis(), allocated.createdAtMillis(),
                allocated.updatedAtMillis(), 0);
    }

    public static MaterializationPolicyRecord policy() {
        return new MaterializationPolicyRecord(
                "policy-f4",
                1,
                ReadView.COMMITTED.wireId(),
                1,
                "NEREUS_COMPACTED_PARQUET_V1",
                2,
                128,
                1_048_576,
                1L << 30,
                65_536,
                "ZSTD",
                "",
                0,
                "");
    }

    public static MaterializationCheckpointRecord emptyMaterializationCheckpoint() {
        return new MaterializationCheckpointRecord(
                1, STREAM, "policy-f4", 1, HASH_B, 0, 0, 0, "", 100, 0);
    }

    public static MaterializationCheckpointRecord advancedMaterializationCheckpoint() {
        return new MaterializationCheckpointRecord(
                1, STREAM, "policy-f4", 1, HASH_B, 20, 7, 4, "task-f4", 1_200, 0);
    }

    public static RangeRetentionStatsRecord retentionStats(long offsetStart, long offsetEnd, long commitVersion) {
        return new RangeRetentionStatsRecord(
                1,
                STREAM,
                offsetStart,
                offsetEnd,
                commitVersion,
                offsetStart * 10,
                offsetEnd * 10,
                1_000,
                2_000,
                "/source/index/" + offsetEnd,
                HASH_A,
                5,
                "test-build",
                3_000,
                0);
    }

    public static RecoveryCheckpointRootRecord emptyRecoveryRoot() {
        return new RecoveryCheckpointRootRecord(
                1, STREAM, 0, 0, 0, 0, 0, 0, 0, "", "", List.of(), "", "", 0, 0, 0);
    }

    public static RecoveryCheckpointReferenceRecord checkpointReference() {
        ObjectKey key = new ObjectKey("objects/f4/recovery-checkpoint");
        return new RecoveryCheckpointReferenceRecord(
                1,
                ATTEMPT,
                0,
                2,
                1,
                1,
                0,
                100,
                "commit-first",
                "commit-last",
                "commit-head",
                1,
                HASH_C,
                "checkpoint-object",
                key.value(),
                ObjectKeyHash.from(key).value(),
                256,
                "01020304",
                HASH_D,
                1,
                1);
    }

    public static RecoveryCheckpointRootRecord fullRecoveryRoot() {
        RecoveryCheckpointReferenceRecord reference = checkpointReference();
        List<RecoveryCheckpointReferenceRecord> references = List.of(reference);
        return new RecoveryCheckpointRootRecord(
                1,
                STREAM,
                1,
                0,
                2,
                1,
                1,
                0,
                100,
                reference.firstCommitId(),
                reference.lastCommitId(),
                references,
                RecoveryCheckpointRootDigests.checkpointSetSha256(references).value(),
                reference.sourceHeadCommitId(),
                reference.sourceHeadCommitVersion(),
                500,
                0);
    }

    public static PhysicalObjectRootRecord physicalRoot(PhysicalObjectLifecycle lifecycle) {
        return physicalRoot(lifecycle, false);
    }

    public static PhysicalObjectRootRecord deletedRootWithTombstone() {
        return physicalRoot(PhysicalObjectLifecycle.DELETED, true);
    }

    private static PhysicalObjectRootRecord physicalRoot(
            PhysicalObjectLifecycle lifecycle,
            boolean tombstone) {
        boolean gc = lifecycle == PhysicalObjectLifecycle.MARKED
                || lifecycle == PhysicalObjectLifecycle.DELETING
                || lifecycle == PhysicalObjectLifecycle.DELETED;
        boolean deleting = lifecycle == PhysicalObjectLifecycle.DELETING
                || lifecycle == PhysicalObjectLifecycle.DELETED;
        boolean deleted = lifecycle == PhysicalObjectLifecycle.DELETED;
        ObjectKey key = new ObjectKey("objects/f4/physical-root");
        return new PhysicalObjectRootRecord(
                1,
                ObjectKeyHash.from(key).value(),
                key.value(),
                "physical-object-f4",
                2,
                128,
                ChecksumType.CRC32C.name(),
                "01020304",
                HASH_D,
                "etag-f4",
                lifecycle,
                1,
                100,
                200,
                gc ? ATTEMPT : "",
                gc ? HASH_E : "",
                gc ? 300 : 0,
                gc ? 400 : 0,
                deleting ? 400 : 0,
                deleted ? 500 : 0,
                tombstone ? 600 : 0,
                tombstone ? HASH_A : "",
                lifecycle == PhysicalObjectLifecycle.QUARANTINED ? "identity mismatch" : "",
                0);
    }

    public static ObjectReaderLeaseRecord readerLease() {
        return new ObjectReaderLeaseRecord(
                1,
                physicalRoot(PhysicalObjectLifecycle.ACTIVE).objectKeyHash(),
                PROCESS,
                LEASE,
                1,
                100,
                300,
                250,
                0,
                0);
    }

    public static ObjectProtectionRecord protection(ObjectProtectionType type) {
        boolean pending = type == ObjectProtectionType.CURSOR_SNAPSHOT_PENDING
                || type == ObjectProtectionType.RECOVERY_CHECKPOINT_PENDING;
        return new ObjectProtectionRecord(
                1,
                physicalRoot(PhysicalObjectLifecycle.ACTIVE).objectKeyHash(),
                type.wireId(),
                "reference-" + type.name().toLowerCase(),
                "/owners/f4/" + type.name().toLowerCase(),
                7,
                HASH_B,
                1,
                100,
                pending ? 200 : 0,
                0);
    }
}
