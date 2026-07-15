/* Licensed under the Apache License, Version 2.0 */
package com.nereusstream.materialization;

import com.nereusstream.api.ErrorCode;
import com.nereusstream.api.NereusException;
import com.nereusstream.metadata.oxia.F4MetadataConditionFailedException;
import com.nereusstream.metadata.oxia.VersionedMaterializationTask;
import com.nereusstream.metadata.oxia.records.MaterializationTaskRecord;
import com.nereusstream.metadata.oxia.records.TaskFailureClass;
import com.nereusstream.metadata.oxia.records.TaskLifecycle;
import java.time.Clock;
import java.time.Duration;
import java.util.Objects;
import java.util.Optional;
import java.util.OptionalLong;
import java.util.concurrent.CompletableFuture;

/** Idempotent restart entry point for every non-terminal task lifecycle. */
public final class MaterializationTaskRecovery {
    private static final String EXPIRED_CLAIM_MESSAGE = "worker claim expired during task recovery";

    private final MaterializationTaskStore tasks;
    private final MaterializationTaskProtectionReconciler protections;
    private final GenerationPublicationReconciler publications;
    private final MaterializationTaskDispatcher dispatcher;
    private final Clock clock;
    private final long maximumClockSkewMillis;
    private final long retryDelayMillis;

    public MaterializationTaskRecovery(
            MaterializationTaskStore tasks,
            MaterializationTaskProtectionReconciler protections,
            GenerationPublicationReconciler publications,
            MaterializationTaskDispatcher dispatcher,
            Clock clock,
            Duration maximumClockSkew,
            Duration retryDelay) {
        this.tasks = Objects.requireNonNull(tasks, "tasks");
        this.protections = Objects.requireNonNull(protections, "protections");
        this.publications = Objects.requireNonNull(publications, "publications");
        this.dispatcher = Objects.requireNonNull(dispatcher, "dispatcher");
        this.clock = Objects.requireNonNull(clock, "clock");
        this.maximumClockSkewMillis = requireNonNegative(
                maximumClockSkew, "maximumClockSkew");
        this.retryDelayMillis = requirePositive(retryDelay, "retryDelay");
    }

    public CompletableFuture<MaterializationTaskRecoveryAction> recover(
            VersionedMaterializationTask durable) {
        return recover(durable, MaterializationTaskMutationGuard.noOp());
    }

    public CompletableFuture<MaterializationTaskRecoveryAction> recover(
            VersionedMaterializationTask durable,
            MaterializationTaskMutationGuard mutationGuard) {
        try {
            Objects.requireNonNull(durable, "durable");
            MaterializationTaskMutationGuard exactGuard = Objects.requireNonNull(
                    mutationGuard, "mutationGuard");
            MaterializationTask task = tasks.requireTask(durable);
            MaterializationTaskRecord record = durable.value();
            return switch (record.lifecycle()) {
                case PLANNED -> dispatch(durable, task, exactGuard);
                case RETRY_WAIT -> clock.millis() >= record.retryNotBeforeMillis()
                        ? dispatch(durable, task, exactGuard)
                        : completed(MaterializationTaskRecoveryAction.NONE);
                case CLAIMED -> recoverClaim(durable, exactGuard);
                case OUTPUT_READY, PUBLISHING -> reconcilePublication(durable, task, record);
                case PUBLISHED -> reconcileProtections(durable);
                case CANCELLED, TERMINAL_FAILED ->
                        completed(MaterializationTaskRecoveryAction.NONE);
            };
        } catch (RuntimeException failure) {
            return CompletableFuture.failedFuture(failure);
        }
    }

    private CompletableFuture<MaterializationTaskRecoveryAction> recoverClaim(
            VersionedMaterializationTask durable,
            MaterializationTaskMutationGuard mutationGuard) {
        return recoverClaim(durable, mutationGuard, false);
    }

    private CompletableFuture<MaterializationTaskRecoveryAction> recoverClaim(
            VersionedMaterializationTask durable,
            MaterializationTaskMutationGuard mutationGuard,
            boolean reloadedAfterConflict) {
        MaterializationTaskRecord current = durable.value();
        long safeExpiry = saturatingAdd(
                current.workerClaim().orElseThrow().expiresAtMillis(),
                maximumClockSkewMillis);
        long now = clock.millis();
        if (now < safeExpiry) {
            return completed(MaterializationTaskRecoveryAction.NONE);
        }
        long updatedAt = Math.max(now, current.updatedAtMillis());
        long retryNotBefore = saturatingAdd(updatedAt, retryDelayMillis);
        if (retryNotBefore <= updatedAt) {
            return CompletableFuture.failedFuture(new IllegalStateException(
                    "materialization retry time exhausted"));
        }
        MaterializationTaskRecord retry = new MaterializationTaskRecord(
                current.schemaVersion(),
                current.taskId(),
                current.taskSequence(),
                current.streamId(),
                current.readViewId(),
                current.taskKindId(),
                current.offsetStart(),
                current.offsetEnd(),
                current.sources(),
                current.sourceSetSha256(),
                current.policyId(),
                current.policyVersion(),
                current.policySha256(),
                current.policy(),
                TaskLifecycle.RETRY_WAIT,
                current.attempt(),
                Optional.empty(),
                Optional.empty(),
                OptionalLong.empty(),
                "",
                TaskFailureClass.CLOSED.wireId(),
                EXPIRED_CLAIM_MESSAGE,
                retryNotBefore,
                current.createdAtMillis(),
                updatedAt,
                0);
        return mutationGuard.revalidate()
                .thenCompose(ignored -> tasks.compareAndSet(retry, durable.metadataVersion()))
                .handle((updated, failure) -> {
                    if (failure == null) {
                        return completed(MaterializationTaskRecoveryAction.EXPIRED_CLAIM_REQUEUED);
                    }
                    Throwable exact = unwrap(failure);
                    if (!isConditionFailure(exact) || reloadedAfterConflict) {
                        return CompletableFuture.<MaterializationTaskRecoveryAction>failedFuture(exact);
                    }
                    return tasks.get(
                                    new com.nereusstream.api.StreamId(current.streamId()),
                                    current.taskId())
                            .thenCompose(optional -> {
                                if (optional.isEmpty()) {
                                    return CompletableFuture.failedFuture(exact);
                                }
                                VersionedMaterializationTask reloaded = optional.orElseThrow();
                                tasks.requireTask(reloaded);
                                if (isSameExpiredRetry(retry, reloaded.value())) {
                                    return completed(
                                            MaterializationTaskRecoveryAction.EXPIRED_CLAIM_REQUEUED);
                                }
                                if (reloaded.value().lifecycle() == TaskLifecycle.CLAIMED) {
                                    return recoverClaim(reloaded, mutationGuard, true);
                                }
                                return recover(reloaded, mutationGuard);
                            });
                })
                .thenCompose(value -> value);
    }

    private CompletableFuture<MaterializationTaskRecoveryAction> reconcilePublication(
            VersionedMaterializationTask durable,
            MaterializationTask task,
            MaterializationTaskRecord record) {
        MaterializationOutput output = MaterializationRecordMapper.domainOutput(
                task, record.output().orElseThrow());
        return protections.reconcile(durable)
                .thenCompose(ignored -> publications.reconcile(task, output))
                .thenApply(ignored -> MaterializationTaskRecoveryAction.PUBLICATION_RECONCILED);
    }

    private CompletableFuture<MaterializationTaskRecoveryAction> reconcileProtections(
            VersionedMaterializationTask durable) {
        return protections.reconcile(durable)
                .thenApply(ignored -> MaterializationTaskRecoveryAction.PROTECTIONS_RECONCILED);
    }

    private CompletableFuture<MaterializationTaskRecoveryAction> dispatch(
            VersionedMaterializationTask durable,
            MaterializationTask task,
            MaterializationTaskMutationGuard mutationGuard) {
        return mutationGuard.revalidate().thenCompose(ignored -> {
            CompletableFuture<Void> admitted;
            try {
                admitted = Objects.requireNonNull(
                        dispatcher.dispatch(durable, task), "task dispatch future");
            } catch (RuntimeException failure) {
                return CompletableFuture.failedFuture(failure);
            }
            return admitted.thenApply(unused -> MaterializationTaskRecoveryAction.DISPATCHED);
        });
    }

    private static CompletableFuture<MaterializationTaskRecoveryAction> completed(
            MaterializationTaskRecoveryAction action) {
        return CompletableFuture.completedFuture(action);
    }

    private static long saturatingAdd(long value, long delta) {
        if (value < 0 || delta < 0) {
            throw new IllegalArgumentException("time values must be non-negative");
        }
        return value > Long.MAX_VALUE - delta ? Long.MAX_VALUE : value + delta;
    }

    private static long requirePositive(Duration duration, String field) {
        long millis = requireNonNegative(duration, field);
        if (millis == 0) {
            throw new IllegalArgumentException(field + " must be positive");
        }
        return millis;
    }

    private static long requireNonNegative(Duration duration, String field) {
        Objects.requireNonNull(duration, field);
        if (duration.isNegative()) {
            throw new IllegalArgumentException(field + " must be non-negative");
        }
        try {
            return duration.toMillis();
        } catch (ArithmeticException failure) {
            throw new IllegalArgumentException(field + " is too large", failure);
        }
    }

    private static boolean isSameExpiredRetry(
            MaterializationTaskRecord expected,
            MaterializationTaskRecord actual) {
        return actual.lifecycle() == TaskLifecycle.RETRY_WAIT
                && actual.taskId().equals(expected.taskId())
                && actual.sourceSetSha256().equals(expected.sourceSetSha256())
                && actual.policySha256().equals(expected.policySha256())
                && actual.attempt() == expected.attempt()
                && actual.workerClaim().isEmpty()
                && actual.failureClassId() == TaskFailureClass.CLOSED.wireId()
                && actual.failureMessage().equals(EXPIRED_CLAIM_MESSAGE)
                && actual.retryNotBeforeMillis() == expected.retryNotBeforeMillis();
    }

    private static boolean isConditionFailure(Throwable failure) {
        return failure instanceof F4MetadataConditionFailedException
                || failure instanceof NereusException nereus
                        && nereus.code() == ErrorCode.METADATA_CONDITION_FAILED;
    }

    private static Throwable unwrap(Throwable failure) {
        Throwable current = failure;
        while ((current instanceof java.util.concurrent.CompletionException
                        || current instanceof java.util.concurrent.ExecutionException)
                && current.getCause() != null) {
            current = current.getCause();
        }
        return current;
    }
}
