/* Licensed under the Apache License, Version 2.0 */
package com.nereusstream.metadata.oxia;

import com.nereusstream.api.ErrorCode;
import com.nereusstream.api.NereusException;
import com.nereusstream.metadata.oxia.records.GenerationIndexRecord;
import com.nereusstream.metadata.oxia.records.GenerationLifecycle;
import com.nereusstream.metadata.oxia.records.MaterializationCheckpointRecord;
import com.nereusstream.metadata.oxia.records.MaterializationStreamRegistrationRecord;
import com.nereusstream.metadata.oxia.records.MaterializationTaskRecord;
import com.nereusstream.metadata.oxia.records.RangeRetentionStatsRecord;
import com.nereusstream.metadata.oxia.records.RecoveryCheckpointRootRecord;
import com.nereusstream.metadata.oxia.records.TaskLifecycle;
import com.nereusstream.metadata.oxia.records.WorkerClaimRecord;
import java.util.Objects;

/** Store-level transition guards for the Phase 4 stream-scoped metadata roots. */
final class GenerationMetadataTransitions {
    private GenerationMetadataTransitions() {
    }

    static void requireValidIndexReplacement(
            GenerationIndexRecord current,
            GenerationIndexRecord replacement) {
        if (!sameGenerationPublicationIdentity(current, replacement)) {
            throw invariant("generation index CAS attempted to change immutable publication identity");
        }
        GenerationLifecycle from = current.lifecycle();
        GenerationLifecycle to = replacement.lifecycle();
        boolean allowed = switch (from) {
            case PREPARED -> to == GenerationLifecycle.COMMITTED || to == GenerationLifecycle.ABORTED;
            case COMMITTED -> to == GenerationLifecycle.QUARANTINED || to == GenerationLifecycle.DRAINING;
            case QUARANTINED -> to == GenerationLifecycle.DRAINING;
            case DRAINING -> to == GenerationLifecycle.RETIRED;
            case RETIRED, ABORTED -> false;
        };
        if (!allowed) {
            throw invariant("illegal generation index lifecycle transition: " + from + " -> " + to);
        }
        if (replacement.stateChangedAtMillis() < current.stateChangedAtMillis()) {
            throw invariant("generation index state timestamp moved backward");
        }
        if (from == GenerationLifecycle.PREPARED && to == GenerationLifecycle.COMMITTED) {
            if (replacement.committedAtMillis() < current.createdAtMillis()) {
                throw invariant("generation commit timestamp precedes creation");
            }
        } else if (replacement.committedAtMillis() != current.committedAtMillis()) {
            throw invariant("generation transition changed the established commit timestamp");
        }
    }

    static void requireValidTaskReplacement(
            MaterializationTaskRecord current,
            MaterializationTaskRecord replacement,
            long nowMillis) {
        if (!sameTaskPlanningIdentity(current, replacement)) {
            throw invariant("materialization task CAS attempted to change immutable planning identity");
        }
        if (replacement.updatedAtMillis() < current.updatedAtMillis()) {
            throw invariant("materialization task update timestamp moved backward");
        }

        TaskLifecycle from = current.lifecycle();
        TaskLifecycle to = replacement.lifecycle();
        boolean allowed = switch (from) {
            case PLANNED -> to == TaskLifecycle.CLAIMED;
            case CLAIMED -> to == TaskLifecycle.CLAIMED
                    || to == TaskLifecycle.OUTPUT_READY
                    || to == TaskLifecycle.RETRY_WAIT
                    || to == TaskLifecycle.CANCELLED
                    || to == TaskLifecycle.TERMINAL_FAILED;
            case OUTPUT_READY -> to == TaskLifecycle.PUBLISHING || to == TaskLifecycle.RETRY_WAIT;
            case PUBLISHING -> to == TaskLifecycle.PUBLISHING
                    || to == TaskLifecycle.PUBLISHED
                    || to == TaskLifecycle.OUTPUT_READY
                    || to == TaskLifecycle.RETRY_WAIT;
            case RETRY_WAIT -> to == TaskLifecycle.CLAIMED;
            case PUBLISHED, CANCELLED, TERMINAL_FAILED -> false;
        };
        if (!allowed) {
            throw invariant("illegal materialization task lifecycle transition: " + from + " -> " + to);
        }

        if (to == TaskLifecycle.CLAIMED && from != TaskLifecycle.CLAIMED) {
            requireNewClaim(current, replacement, nowMillis);
        } else if (from == TaskLifecycle.CLAIMED && to == TaskLifecycle.CLAIMED) {
            requireHeartbeat(current, replacement);
        } else if (replacement.attempt() != current.attempt()) {
            throw invariant("materialization task attempt changed outside claim acquisition");
        }

        if (from == TaskLifecycle.CLAIMED && to == TaskLifecycle.OUTPUT_READY) {
            WorkerClaimRecord claim = current.workerClaim().orElseThrow();
            if (replacement.output().isEmpty()
                    || !replacement.output().orElseThrow().outputAttemptId().equals(claim.claimId())) {
                throw invariant("task output attempt does not match the winning worker claim");
            }
        }

        if (current.output().isPresent() && to != TaskLifecycle.CLAIMED
                && !current.output().equals(replacement.output())) {
            throw invariant("materialization task CAS changed an established output identity");
        }

        if (from == TaskLifecycle.OUTPUT_READY && to == TaskLifecycle.PUBLISHING
                && replacement.allocatedGeneration().isPresent()) {
            throw invariant("task must freeze publication id before allocating a generation");
        }
        if (from == TaskLifecycle.PUBLISHING && to == TaskLifecycle.PUBLISHING) {
            if (!current.publicationId().equals(replacement.publicationId())
                    || current.allocatedGeneration().isPresent()
                    || replacement.allocatedGeneration().isEmpty()) {
                throw invariant("same-state PUBLISHING CAS may only attach the first allocated generation");
            }
        }
        if (from == TaskLifecycle.PUBLISHING && to == TaskLifecycle.PUBLISHED
                && (!current.publicationId().equals(replacement.publicationId())
                        || !current.allocatedGeneration().equals(replacement.allocatedGeneration()))) {
            throw invariant("published task changed its frozen publication allocation");
        }
    }

    static void requireValidCheckpointReplacement(
            MaterializationCheckpointRecord current,
            MaterializationCheckpointRecord replacement) {
        if (current.schemaVersion() != replacement.schemaVersion()
                || !current.streamId().equals(replacement.streamId())
                || !current.policyId().equals(replacement.policyId())
                || current.policyVersion() != replacement.policyVersion()
                || !current.policySha256().equals(replacement.policySha256())) {
            throw invariant("materialization checkpoint CAS changed policy identity");
        }
        if (replacement.contiguousCoveredOffset() < current.contiguousCoveredOffset()
                || replacement.observedCommitVersion() < current.observedCommitVersion()
                || replacement.lastTaskSequence() < current.lastTaskSequence()
                || replacement.updatedAtMillis() < current.updatedAtMillis()) {
            throw invariant("ordinary materialization checkpoint CAS moved progress backward");
        }
        if (replacement.lastTaskSequence() == current.lastTaskSequence()
                && !replacement.lastTaskId().equals(current.lastTaskId())) {
            throw invariant("materialization checkpoint changed task identity without advancing sequence");
        }
    }

    static void requireValidRetentionStatsReplacement(
            RangeRetentionStatsRecord current,
            RangeRetentionStatsRecord replacement) {
        if (current.schemaVersion() != replacement.schemaVersion()
                || !current.streamId().equals(replacement.streamId())
                || current.offsetStart() != replacement.offsetStart()
                || current.offsetEnd() != replacement.offsetEnd()
                || current.commitVersion() != replacement.commitVersion()
                || current.cumulativeSizeAtStart() != replacement.cumulativeSizeAtStart()
                || current.cumulativeSizeAtEnd() != replacement.cumulativeSizeAtEnd()) {
            throw invariant("retention stats CAS changed immutable range/commit boundaries");
        }
        if (replacement.verifiedAtMillis() < current.verifiedAtMillis()) {
            throw invariant("retention stats verification timestamp moved backward");
        }
    }

    static void requireValidRegistrationReplacement(
            MaterializationStreamRegistrationRecord current,
            MaterializationStreamRegistrationRecord replacement) {
        if (current.schemaVersion() != replacement.schemaVersion()
                || !current.streamId().equals(replacement.streamId())
                || !current.projectionRef().equals(replacement.projectionRef())
                || !current.projectionIdentitySha256().equals(replacement.projectionIdentitySha256())
                || !current.storageProfile().equals(replacement.storageProfile())
                || current.registeredAtMillis() != replacement.registeredAtMillis()) {
            throw invariant("stream registration CAS changed immutable projection identity");
        }
        if (replacement.lastHintCommitVersion() < current.lastHintCommitVersion()
                || replacement.updatedAtMillis() < current.updatedAtMillis()) {
            throw invariant("stream registration refresh moved its hint backward");
        }
    }

    static void requireValidRecoveryRootReplacement(
            RecoveryCheckpointRootRecord current,
            RecoveryCheckpointRootRecord replacement) {
        if (current.schemaVersion() != replacement.schemaVersion()
                || !current.streamId().equals(replacement.streamId())) {
            throw invariant("recovery root CAS changed immutable stream identity");
        }
        long nextSequence;
        try {
            nextSequence = Math.addExact(current.checkpointSequence(), 1);
        } catch (ArithmeticException overflow) {
            throw invariant("recovery checkpoint sequence exhausted", overflow);
        }
        if (replacement.checkpointSequence() != nextSequence) {
            throw invariant("recovery root publication must increment checkpoint sequence exactly once");
        }
        if (replacement.checkpoints().isEmpty()) {
            throw invariant("recovery root cannot return to the empty bootstrap state");
        }
        if (replacement.coveredStartOffset() < current.coveredStartOffset()
                || replacement.coveredEndOffset() < current.coveredEndOffset()
                || replacement.lastCommitVersion() < current.lastCommitVersion()
                || replacement.cumulativeSizeAtEnd() < current.cumulativeSizeAtEnd()
                || replacement.sourceHeadCommitVersion() < current.sourceHeadCommitVersion()
                || replacement.publishedAtMillis() < current.publishedAtMillis()) {
            throw invariant("recovery root publication moved authoritative coverage backward");
        }
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

    static boolean sameTaskPlanningIdentity(
            MaterializationTaskRecord left,
            MaterializationTaskRecord right) {
        return sameTaskCreateIdentity(left, right)
                && left.createdAtMillis() == right.createdAtMillis();
    }

    /**
     * Identity used to converge deterministic put-if-absent task creation across processes.
     *
     * <p>The creation timestamp is deliberately excluded: it is frozen after one value wins the create, but it is
     * not an input to the deterministic task id and two planners need not observe the same wall-clock millisecond.
     */
    static boolean sameTaskCreateIdentity(
            MaterializationTaskRecord left,
            MaterializationTaskRecord right) {
        return left.schemaVersion() == right.schemaVersion()
                && left.taskId().equals(right.taskId())
                && left.taskSequence() == right.taskSequence()
                && left.streamId().equals(right.streamId())
                && left.readViewId() == right.readViewId()
                && left.taskKindId() == right.taskKindId()
                && left.offsetStart() == right.offsetStart()
                && left.offsetEnd() == right.offsetEnd()
                && left.sources().equals(right.sources())
                && left.sourceSetSha256().equals(right.sourceSetSha256())
                && left.policyId().equals(right.policyId())
                && left.policyVersion() == right.policyVersion()
                && left.policySha256().equals(right.policySha256())
                && left.policy().equals(right.policy());
    }

    private static void requireNewClaim(
            MaterializationTaskRecord current,
            MaterializationTaskRecord replacement,
            long nowMillis) {
        long expectedAttempt;
        try {
            expectedAttempt = Math.addExact(current.attempt(), 1);
        } catch (ArithmeticException overflow) {
            throw invariant("materialization task attempt exhausted", overflow);
        }
        if (replacement.attempt() != expectedAttempt) {
            throw invariant("new worker claim must increment task attempt exactly once");
        }
        if (current.lifecycle() == TaskLifecycle.RETRY_WAIT
                && (nowMillis < current.retryNotBeforeMillis()
                        || replacement.updatedAtMillis() < current.retryNotBeforeMillis())) {
            throw invariant("materialization retry was claimed before retryNotBefore");
        }
    }

    private static void requireHeartbeat(
            MaterializationTaskRecord current,
            MaterializationTaskRecord replacement) {
        if (replacement.attempt() != current.attempt()
                || current.workerClaim().isEmpty()
                || replacement.workerClaim().isEmpty()) {
            throw invariant("CLAIMED heartbeat lacks the current claim");
        }
        WorkerClaimRecord before = current.workerClaim().orElseThrow();
        WorkerClaimRecord after = replacement.workerClaim().orElseThrow();
        if (!before.claimId().equals(after.claimId())
                || !before.processRunId().equals(after.processRunId())
                || before.attempt() != after.attempt()
                || before.claimedAtMillis() != after.claimedAtMillis()
                || after.expiresAtMillis() <= before.expiresAtMillis()
                || !Objects.equals(current.output(), replacement.output())) {
            throw invariant("CLAIMED same-state CAS is not a same-owner expiry heartbeat");
        }
    }

    private static NereusException invariant(String message) {
        return invariant(message, null);
    }

    private static NereusException invariant(String message, Throwable cause) {
        return new NereusException(ErrorCode.METADATA_INVARIANT_VIOLATION, false, message, cause);
    }
}
