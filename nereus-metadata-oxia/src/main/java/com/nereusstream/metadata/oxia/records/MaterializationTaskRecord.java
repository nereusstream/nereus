/* Licensed under the Apache License, Version 2.0 */
package com.nereusstream.metadata.oxia.records;

import java.util.List;
import java.util.Objects;
import java.util.Optional;
import java.util.OptionalLong;

/** Durable workflow root; visibility remains owned only by GenerationIndexRecord. */
public record MaterializationTaskRecord(
        int schemaVersion,
        String taskId,
        long taskSequence,
        String streamId,
        int readViewId,
        int taskKindId,
        long offsetStart,
        long offsetEnd,
        List<SourceGenerationRecord> sources,
        String sourceSetSha256,
        String policyId,
        long policyVersion,
        String policySha256,
        TaskLifecycle lifecycle,
        long attempt,
        Optional<WorkerClaimRecord> workerClaim,
        Optional<MaterializationOutputRecord> output,
        OptionalLong allocatedGeneration,
        String publicationId,
        int failureClassId,
        String failureMessage,
        long retryNotBeforeMillis,
        long createdAtMillis,
        long updatedAtMillis,
        long metadataVersion) {
    public MaterializationTaskRecord {
        F4RecordValidation.requireSchemaVersion(schemaVersion);
        taskId = F4RecordValidation.requireText(taskId, "taskId", 256, false);
        F4RecordValidation.requirePositive(taskSequence, "taskSequence");
        streamId = F4RecordValidation.requireText(streamId, "streamId");
        if (readViewId != 1 && readViewId != 2) {
            throw new IllegalArgumentException("readViewId must identify a known view");
        }
        if (taskKindId != 1 && taskKindId != 2) {
            throw new IllegalArgumentException("taskKindId is unknown");
        }
        F4RecordValidation.requireRange(offsetStart, offsetEnd, "task coverage");
        sources = F4RecordValidation.immutableBoundedList(
                sources, F4RecordValidation.MAX_TASK_SOURCES, "sources");
        if (sources.isEmpty()) {
            throw new IllegalArgumentException("task must contain at least one source");
        }
        long cursor = offsetStart;
        for (SourceGenerationRecord source : sources) {
            if (source.readViewId() != readViewId || source.offsetStart() != cursor) {
                throw new IllegalArgumentException("task sources must be view-consistent and gap-free");
            }
            cursor = source.offsetEnd();
        }
        if (cursor != offsetEnd) {
            throw new IllegalArgumentException("task source coverage does not match task coverage");
        }
        sourceSetSha256 = F4RecordValidation.requireSha256(sourceSetSha256, "sourceSetSha256");
        policyId = F4RecordValidation.requireText(policyId, "policyId");
        F4RecordValidation.requirePositive(policyVersion, "policyVersion");
        policySha256 = F4RecordValidation.requireSha256(policySha256, "policySha256");
        Objects.requireNonNull(lifecycle, "lifecycle");
        F4RecordValidation.requireNonNegative(attempt, "attempt");
        workerClaim = Objects.requireNonNull(workerClaim, "workerClaim");
        output = Objects.requireNonNull(output, "output");
        allocatedGeneration = Objects.requireNonNull(allocatedGeneration, "allocatedGeneration");
        if (allocatedGeneration.isPresent() && allocatedGeneration.getAsLong() <= 0) {
            throw new IllegalArgumentException("allocated generation must be positive");
        }
        publicationId = F4RecordValidation.requireOptionalText(publicationId, "publicationId", 128);
        TaskFailureClass failureClass = TaskFailureClass.fromWireId(failureClassId);
        failureMessage = F4RecordValidation.requireOptionalText(
                failureMessage, "failureMessage", F4RecordValidation.MAX_REASON_BYTES);
        F4RecordValidation.requireNonNegative(retryNotBeforeMillis, "retryNotBeforeMillis");
        F4RecordValidation.requireNonNegative(createdAtMillis, "createdAtMillis");
        if (updatedAtMillis < createdAtMillis) {
            throw new IllegalArgumentException("task update cannot precede creation");
        }
        F4RecordValidation.requireMetadataVersion(metadataVersion);
        if (workerClaim.isPresent() && workerClaim.orElseThrow().attempt() != attempt) {
            throw new IllegalArgumentException("worker claim attempt does not match task attempt");
        }
        if (output.isPresent()) {
            MaterializationOutputRecord value = output.orElseThrow();
            if (!value.sourceSetSha256().equals(sourceSetSha256)) {
                throw new IllegalArgumentException("task output source identity does not match task");
            }
        }
        boolean publicationLifecycle = lifecycle == TaskLifecycle.PUBLISHING
                || lifecycle == TaskLifecycle.PUBLISHED;
        if (publicationLifecycle != !publicationId.isEmpty()) {
            throw new IllegalArgumentException("publication id does not match task lifecycle");
        }
        if (lifecycle == TaskLifecycle.PUBLISHED && allocatedGeneration.isEmpty()) {
            throw new IllegalArgumentException("PUBLISHED task requires an allocated generation");
        }
        if (!publicationLifecycle && allocatedGeneration.isPresent()) {
            throw new IllegalArgumentException("non-publication task carries an allocated generation");
        }
        if (!publicationId.isEmpty()) {
            F4RecordValidation.requireBase32Id(publicationId, "publicationId");
        }
        switch (lifecycle) {
            case PLANNED -> {
                if (attempt != 0 || workerClaim.isPresent() || output.isPresent()) {
                    throw new IllegalArgumentException("PLANNED task carries execution state");
                }
            }
            case CLAIMED -> {
                if (attempt <= 0 || workerClaim.isEmpty() || output.isPresent()) {
                    throw new IllegalArgumentException("CLAIMED task has invalid worker/output state");
                }
            }
            case OUTPUT_READY, PUBLISHING, PUBLISHED -> {
                if (attempt <= 0 || output.isEmpty()) {
                    throw new IllegalArgumentException("output lifecycle requires a completed attempt and output");
                }
            }
            case RETRY_WAIT -> {
                if (attempt <= 0 || workerClaim.isPresent() || retryNotBeforeMillis <= updatedAtMillis) {
                    throw new IllegalArgumentException("RETRY_WAIT task has invalid retry state");
                }
            }
            case CANCELLED, TERMINAL_FAILED -> {
                if (workerClaim.isPresent()) {
                    throw new IllegalArgumentException("terminal task cannot retain a worker claim");
                }
            }
        }
        boolean failureExpected = lifecycle == TaskLifecycle.RETRY_WAIT
                || lifecycle == TaskLifecycle.CANCELLED
                || lifecycle == TaskLifecycle.TERMINAL_FAILED;
        if (failureExpected != (failureClass != TaskFailureClass.NONE && !failureMessage.isEmpty())) {
            throw new IllegalArgumentException("failure fields do not match task lifecycle");
        }
        if (!failureExpected && retryNotBeforeMillis != 0) {
            throw new IllegalArgumentException("non-retry task cannot carry retryNotBeforeMillis");
        }
    }

    public MaterializationTaskRecord withMetadataVersion(long version) {
        return new MaterializationTaskRecord(
                schemaVersion, taskId, taskSequence, streamId, readViewId, taskKindId,
                offsetStart, offsetEnd, sources, sourceSetSha256, policyId, policyVersion, policySha256,
                lifecycle, attempt, workerClaim, output, allocatedGeneration, publicationId,
                failureClassId, failureMessage, retryNotBeforeMillis, createdAtMillis, updatedAtMillis, version);
    }
}
