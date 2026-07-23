/* Licensed under the Apache License, Version 2.0 */
package com.nereusstream.metadata.oxia;

import com.nereusstream.api.keys.DeterministicIds;
import com.nereusstream.metadata.oxia.records.KafkaCheckpointReferenceRecord;
import com.nereusstream.metadata.oxia.records.KafkaCompactionCoverageRecord;
import com.nereusstream.metadata.oxia.records.KafkaPartitionBindingRecord;
import com.nereusstream.metadata.oxia.records.KafkaPartitionLifecycle;
import com.nereusstream.metadata.oxia.records.KafkaPartitionOperationType;
import com.nereusstream.metadata.oxia.records.KafkaPartitionPendingOperationRecord;
import com.nereusstream.metadata.oxia.records.KafkaPayloadMapping;
import java.util.ArrayList;
import java.util.List;
import java.util.Objects;

/** Pure deterministic transitions for one authoritative Kafka partition root. */
public final class KafkaPartitionMetadataTransitions {
    public static final long INITIAL_INCARNATION = 1;

    private KafkaPartitionMetadataTransitions() { }

    public static KafkaPartitionBindingRecord creating(
            KafkaPartitionId id,
            String observedTopicName,
            String storageProfile,
            long createdMetadataOffset,
            long nowMillis,
            KafkaPartitionPendingOperationRecord createOperation) {
        Objects.requireNonNull(id, "id");
        if (createOperation.operationType() != KafkaPartitionOperationType.CREATE) {
            throw new IllegalArgumentException("creating root requires a CREATE operation");
        }
        return new KafkaPartitionBindingRecord(
                1, id.kafkaClusterId(), id.topicId(), id.partitionId(), observedTopicName,
                INITIAL_INCARNATION, "", "", KafkaPayloadMapping.KAFKA_RECORD_BATCH_V1.wireId(),
                storageProfile, KafkaPartitionLifecycle.CREATING.wireId(), 1,
                createdMetadataOffset, createdMetadataOffset, -1, -1, -1, 0, 0,
                KafkaCompactionCoverageRecord.EMPTY, List.of(), createOperation,
                nowMillis, nowMillis, 0);
    }

    public static KafkaPartitionBindingRecord activate(
            KafkaPartitionBindingRecord current,
            String streamName,
            String streamId,
            long appliedMetadataOffset,
            long nowMillis) {
        requireLifecycle(current, KafkaPartitionLifecycle.CREATING);
        if (current.pendingOperation().operationType() != KafkaPartitionOperationType.CREATE) {
            throw new IllegalArgumentException("CREATING root has no claimed CREATE operation");
        }
        KafkaPartitionBindingRecord update = copy(
                current, streamName, streamId, KafkaPartitionLifecycle.ACTIVE,
                appliedMetadataOffset, current.observedLeaderId(), current.observedLeaderEpoch(),
                current.observedBrokerEpoch(), current.observedLogStartOffset(), current.observedStableEndOffset(),
                current.compactionCoverage(), current.checkpointReferences(),
                KafkaPartitionPendingOperationRecord.EMPTY, nowMillis);
        validate(current, update);
        return update;
    }

    public static KafkaPartitionBindingRecord observe(
            KafkaPartitionBindingRecord current,
            String observedTopicName,
            long appliedMetadataOffset,
            int leaderId,
            int leaderEpoch,
            long brokerEpoch,
            long logStartOffset,
            long stableEndOffset,
            long nowMillis) {
        if (appliedMetadataOffset < current.lastAppliedMetadataOffset()) {
            throw new IllegalArgumentException("metadata offset cannot move backward");
        }
        if (leaderEpoch >= 0 && current.observedLeaderEpoch() >= 0
                && leaderEpoch < current.observedLeaderEpoch()) {
            throw new IllegalArgumentException("observed leader epoch cannot move backward");
        }
        KafkaPartitionBindingRecord update = new KafkaPartitionBindingRecord(
                current.formatVersion(), current.kafkaClusterId(), current.topicId(), current.partitionId(),
                observedTopicName, current.incarnation(), current.streamName(), current.streamId(),
                current.payloadMappingId(), current.storageProfile(), current.lifecycleId(),
                Math.addExact(current.bindingEpoch(), 1), current.createdMetadataOffset(), appliedMetadataOffset,
                leaderId, leaderEpoch, brokerEpoch, logStartOffset, stableEndOffset,
                current.compactionCoverage(), current.checkpointReferences(), current.pendingOperation(),
                current.createdAtMillis(), nowMillis, 0);
        validate(current, update);
        return update;
    }

    public static KafkaPartitionBindingRecord claimOperation(
            KafkaPartitionBindingRecord current,
            KafkaPartitionPendingOperationRecord requested,
            long nowMillis) {
        Objects.requireNonNull(requested, "requested");
        if (requested.isEmpty()) throw new IllegalArgumentException("claimed operation cannot be NONE");
        KafkaPartitionPendingOperationRecord existing = current.pendingOperation();
        boolean sameAttempt = !existing.isEmpty() && existing.attemptId().equals(requested.attemptId());
        boolean expired = !existing.isEmpty() && existing.leaseExpiresAtMillis() <= nowMillis;
        if (!existing.isEmpty() && !sameAttempt && !expired) {
            throw new KafkaMetadataConditionFailedException("partition operation lease is owned by another attempt");
        }
        if (sameAttempt && !existing.ownerId().equals(requested.ownerId()) && !expired) {
            throw new KafkaMetadataConditionFailedException("partition operation lease is owned by another worker");
        }
        if (sameAttempt && existing.ownerId().equals(requested.ownerId())
                && requested.ownerEpoch() < existing.ownerEpoch()) {
            throw new KafkaMetadataConditionFailedException("partition operation owner epoch is stale");
        }
        requireOperationLifecycle(current.lifecycle(), requested.operationType());
        KafkaPartitionBindingRecord update = copy(
                current, current.streamName(), current.streamId(), current.lifecycle(),
                Math.max(current.lastAppliedMetadataOffset(), requested.targetMetadataOffset()),
                current.observedLeaderId(), current.observedLeaderEpoch(), current.observedBrokerEpoch(),
                current.observedLogStartOffset(), current.observedStableEndOffset(), current.compactionCoverage(),
                current.checkpointReferences(), requested, nowMillis);
        validate(current, update);
        return update;
    }

    public static KafkaPartitionBindingRecord clearOperation(
            KafkaPartitionBindingRecord current,
            String attemptId,
            long nowMillis) {
        KafkaPartitionPendingOperationRecord operation = current.pendingOperation();
        if (operation.isEmpty() || !operation.attemptId().equals(attemptId)) {
            throw new KafkaMetadataConditionFailedException("partition operation attempt does not match");
        }
        KafkaPartitionBindingRecord update = copy(
                current, current.streamName(), current.streamId(), current.lifecycle(),
                current.lastAppliedMetadataOffset(), current.observedLeaderId(), current.observedLeaderEpoch(),
                current.observedBrokerEpoch(), current.observedLogStartOffset(), current.observedStableEndOffset(),
                current.compactionCoverage(), current.checkpointReferences(),
                KafkaPartitionPendingOperationRecord.EMPTY, nowMillis);
        validate(current, update);
        return update;
    }

    public static KafkaPartitionBindingRecord beginDelete(
            KafkaPartitionBindingRecord current,
            KafkaPartitionPendingOperationRecord deleteOperation,
            long nowMillis) {
        requireLifecycle(current, KafkaPartitionLifecycle.ACTIVE);
        if (deleteOperation.operationType() != KafkaPartitionOperationType.DELETE) {
            throw new IllegalArgumentException("delete transition requires DELETE operation");
        }
        KafkaPartitionBindingRecord update = copy(
                current, current.streamName(), current.streamId(), KafkaPartitionLifecycle.DELETING,
                Math.max(current.lastAppliedMetadataOffset(), deleteOperation.targetMetadataOffset()),
                current.observedLeaderId(), current.observedLeaderEpoch(), current.observedBrokerEpoch(),
                current.observedLogStartOffset(), current.observedStableEndOffset(), current.compactionCoverage(),
                current.checkpointReferences(), deleteOperation, nowMillis);
        validate(current, update);
        return update;
    }

    public static KafkaPartitionBindingRecord markDeleted(
            KafkaPartitionBindingRecord current, String deleteAttemptId, long nowMillis) {
        requireLifecycle(current, KafkaPartitionLifecycle.DELETING);
        if (current.pendingOperation().operationType() != KafkaPartitionOperationType.DELETE
                || !current.pendingOperation().attemptId().equals(deleteAttemptId)) {
            throw new KafkaMetadataConditionFailedException("delete attempt does not own the binding");
        }
        KafkaPartitionBindingRecord update = copy(
                current, current.streamName(), current.streamId(), KafkaPartitionLifecycle.DELETED,
                current.lastAppliedMetadataOffset(), -1, current.observedLeaderEpoch(), -1,
                current.observedLogStartOffset(), current.observedStableEndOffset(), current.compactionCoverage(),
                current.checkpointReferences(), KafkaPartitionPendingOperationRecord.EMPTY, nowMillis);
        validate(current, update);
        return update;
    }

    public static KafkaPartitionBindingRecord markCorrupt(
            KafkaPartitionBindingRecord current,
            String errorCode,
            long nowMillis) {
        KafkaPartitionPendingOperationRecord existing = current.pendingOperation();
        KafkaPartitionPendingOperationRecord repair = existing.isEmpty()
                ? new KafkaPartitionPendingOperationRecord(
                        KafkaPartitionOperationType.REPAIR.wireId(),
                        DeterministicIds.stableHashComponent(
                                "repair-v1\0" + current.identity().canonicalIdentity() + "\0" + current.bindingEpoch()),
                        "invariant-detector", 1, Math.addExact(nowMillis, 1),
                        current.lastAppliedMetadataOffset(), nowMillis, errorCode)
                : new KafkaPartitionPendingOperationRecord(
                        KafkaPartitionOperationType.REPAIR.wireId(), existing.attemptId(), existing.ownerId(),
                        existing.ownerEpoch(), Math.max(existing.leaseExpiresAtMillis(), Math.addExact(nowMillis, 1)),
                        Math.max(existing.targetMetadataOffset(), current.lastAppliedMetadataOffset()),
                        existing.startedAtMillis(), errorCode);
        KafkaPartitionBindingRecord update = copy(
                current, current.streamName(), current.streamId(), KafkaPartitionLifecycle.CORRUPT,
                current.lastAppliedMetadataOffset(), current.observedLeaderId(), current.observedLeaderEpoch(),
                current.observedBrokerEpoch(), current.observedLogStartOffset(), current.observedStableEndOffset(),
                current.compactionCoverage(), current.checkpointReferences(), repair, nowMillis);
        validate(current, update);
        return update;
    }

    public static KafkaPartitionBindingRecord prependCheckpoint(
            KafkaPartitionBindingRecord current,
            KafkaCheckpointReferenceRecord reference,
            long authoritativeTrimOffset,
            long authoritativeEndOffset,
            long nowMillis) {
        requireLifecycle(current, KafkaPartitionLifecycle.ACTIVE);
        Objects.requireNonNull(reference, "reference");
        if (authoritativeTrimOffset < 0 || authoritativeEndOffset < authoritativeTrimOffset
                || reference.logStartOffsetAtCheckpoint() > authoritativeTrimOffset
                || reference.checkpointOffset() < authoritativeTrimOffset
                || reference.checkpointOffset() > authoritativeEndOffset) {
            throw new IllegalArgumentException("checkpoint does not cover the authoritative stream window");
        }
        ArrayList<KafkaCheckpointReferenceRecord> references = new ArrayList<>();
        references.add(reference);
        current.checkpointReferences().stream()
                .filter(existing -> !existing.objectId().equals(reference.objectId()))
                .filter(existing -> existing.checkpointOffset() < reference.checkpointOffset())
                .limit(2)
                .forEach(references::add);
        KafkaPartitionBindingRecord update = copy(
                current, current.streamName(), current.streamId(), current.lifecycle(),
                current.lastAppliedMetadataOffset(), current.observedLeaderId(), current.observedLeaderEpoch(),
                current.observedBrokerEpoch(), authoritativeTrimOffset, authoritativeEndOffset,
                current.compactionCoverage(), references, current.pendingOperation(), nowMillis);
        validate(current, update);
        return update;
    }

    public static void validate(
            KafkaPartitionBindingRecord current, KafkaPartitionBindingRecord update) {
        Objects.requireNonNull(current, "current");
        Objects.requireNonNull(update, "update");
        if (!current.identity().equals(update.identity()) || current.incarnation() != update.incarnation()
                || current.payloadMappingId() != update.payloadMappingId()
                || !current.storageProfile().equals(update.storageProfile())
                || current.createdMetadataOffset() != update.createdMetadataOffset()
                || current.createdAtMillis() != update.createdAtMillis()) {
            throw invariant("immutable Kafka partition binding identity changed");
        }
        if ((!current.streamName().isEmpty() && !current.streamName().equals(update.streamName()))
                || (!current.streamId().isEmpty() && !current.streamId().equals(update.streamId()))) {
            throw invariant("bound stream identity changed");
        }
        if (update.bindingEpoch() != current.bindingEpoch() + 1 || update.metadataVersion() != 0
                || update.lastAppliedMetadataOffset() < current.lastAppliedMetadataOffset()
                || update.updatedAtMillis() < current.updatedAtMillis()) {
            throw invariant("Kafka partition binding version or monotonic field is invalid");
        }
        if (!allowedLifecycle(current.lifecycle(), update.lifecycle())) {
            throw invariant("illegal Kafka partition lifecycle transition");
        }
        KafkaCompactionCoverageRecord before = current.compactionCoverage();
        KafkaCompactionCoverageRecord after = update.compactionCoverage();
        if (before.coverageVersion() != 0
                && (after.coverageVersion() == 0 || after.endOffset() < before.endOffset()
                    || after.activationEpoch() < before.activationEpoch())) {
            throw invariant("Kafka compaction coverage cannot regress");
        }
    }

    public static String deterministicStreamName(KafkaPartitionId id, long incarnation) {
        if (incarnation <= 0) throw new IllegalArgumentException("incarnation must be positive");
        return "kafka/" + id.kafkaClusterId() + "/" + id.topicId() + "/"
                + id.partitionId() + "/incarnation-" + incarnation;
    }

    public static String deterministicCreateAttemptId(KafkaPartitionId id, long metadataOffset) {
        return deterministicAttempt("create-v1", id, metadataOffset);
    }

    public static String deterministicDeleteAttemptId(KafkaPartitionId id, long metadataOffset) {
        return deterministicAttempt("delete-v1", id, metadataOffset);
    }

    private static String deterministicAttempt(String type, KafkaPartitionId id, long metadataOffset) {
        if (metadataOffset < 0) throw new IllegalArgumentException("metadataOffset must be non-negative");
        return DeterministicIds.stableHashComponent(type + "\0" + id.canonicalIdentity() + "\0" + metadataOffset);
    }

    private static KafkaPartitionBindingRecord copy(
            KafkaPartitionBindingRecord current,
            String streamName,
            String streamId,
            KafkaPartitionLifecycle lifecycle,
            long appliedMetadataOffset,
            int leaderId,
            int leaderEpoch,
            long brokerEpoch,
            long logStart,
            long stableEnd,
            KafkaCompactionCoverageRecord coverage,
            List<KafkaCheckpointReferenceRecord> checkpoints,
            KafkaPartitionPendingOperationRecord operation,
            long nowMillis) {
        return new KafkaPartitionBindingRecord(
                current.formatVersion(), current.kafkaClusterId(), current.topicId(), current.partitionId(),
                current.observedTopicName(), current.incarnation(), streamName, streamId,
                current.payloadMappingId(), current.storageProfile(), lifecycle.wireId(),
                Math.addExact(current.bindingEpoch(), 1), current.createdMetadataOffset(), appliedMetadataOffset,
                leaderId, leaderEpoch, brokerEpoch, logStart, stableEnd, coverage, checkpoints, operation,
                current.createdAtMillis(), nowMillis, 0);
    }

    private static void requireLifecycle(
            KafkaPartitionBindingRecord current, KafkaPartitionLifecycle expected) {
        if (current.lifecycle() != expected) {
            throw new KafkaMetadataConditionFailedException(
                    "expected Kafka partition lifecycle " + expected + " but found " + current.lifecycle());
        }
    }

    private static void requireOperationLifecycle(
            KafkaPartitionLifecycle lifecycle, KafkaPartitionOperationType operation) {
        boolean allowed = switch (operation) {
            case CREATE -> lifecycle == KafkaPartitionLifecycle.CREATING;
            case DELETE -> lifecycle == KafkaPartitionLifecycle.DELETING;
            case REPAIR -> lifecycle == KafkaPartitionLifecycle.CORRUPT;
            case NONE -> false;
        };
        if (!allowed) throw new IllegalArgumentException("operation is incompatible with lifecycle");
    }

    private static boolean allowedLifecycle(
            KafkaPartitionLifecycle before, KafkaPartitionLifecycle after) {
        if (before == after) return before != KafkaPartitionLifecycle.DELETED;
        return switch (before) {
            case CREATING -> after == KafkaPartitionLifecycle.ACTIVE || after == KafkaPartitionLifecycle.CORRUPT;
            case ACTIVE -> after == KafkaPartitionLifecycle.DELETING || after == KafkaPartitionLifecycle.CORRUPT;
            case DELETING -> after == KafkaPartitionLifecycle.DELETED || after == KafkaPartitionLifecycle.CORRUPT;
            case CORRUPT -> after == KafkaPartitionLifecycle.CORRUPT;
            case DELETED -> false;
        };
    }

    private static IllegalArgumentException invariant(String message) {
        return new IllegalArgumentException(message);
    }
}
