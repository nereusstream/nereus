/* Licensed under the Apache License, Version 2.0 */
package com.nereusstream.materialization;

import com.nereusstream.api.ErrorCode;
import com.nereusstream.api.NereusException;
import com.nereusstream.api.StreamId;
import com.nereusstream.metadata.oxia.F4ScanToken;
import com.nereusstream.metadata.oxia.GenerationMetadataStore;
import com.nereusstream.metadata.oxia.TaskScanPage;
import com.nereusstream.metadata.oxia.VersionedGenerationCandidate;
import com.nereusstream.metadata.oxia.VersionedMaterializationTask;
import com.nereusstream.metadata.oxia.codec.F4MetadataCodecs;
import com.nereusstream.metadata.oxia.records.MaterializationTaskRecord;
import java.time.Clock;
import java.util.ArrayList;
import java.util.List;
import java.util.Objects;
import java.util.Optional;
import java.util.concurrent.CompletableFuture;
import java.util.function.Supplier;

/** Task-specific facade that freezes domain identities and revalidates every exact source before create. */
public final class MaterializationTaskStore {
    public static final int MAX_ENCODED_TASK_BYTES = 64 * 1024;

    private final String cluster;
    private final GenerationMetadataStore generations;
    private final Clock clock;

    public MaterializationTaskStore(
            String cluster,
            GenerationMetadataStore generations,
            Clock clock) {
        this.cluster = requireText(cluster, "cluster");
        this.generations = Objects.requireNonNull(generations, "generations");
        this.clock = Objects.requireNonNull(clock, "clock");
    }

    public CompletableFuture<VersionedMaterializationTask> create(MaterializationTask task) {
        return create(task, MaterializationTaskMutationGuard.noOp());
    }

    public CompletableFuture<VersionedMaterializationTask> create(
            MaterializationTask task,
            MaterializationTaskMutationGuard mutationGuard) {
        return async(() -> {
            MaterializationTask exactTask = Objects.requireNonNull(task, "task");
            MaterializationTaskMutationGuard exactGuard = Objects.requireNonNull(
                    mutationGuard, "mutationGuard");
            MaterializationTaskRecord planned = MaterializationRecordMapper.plannedTask(
                    exactTask, clock.millis());
            if (F4MetadataCodecs.encodeEnvelope(planned, MaterializationTaskRecord.class).length
                    > MAX_ENCODED_TASK_BYTES) {
                return CompletableFuture.failedFuture(new NereusException(
                        ErrorCode.METADATA_LIMIT_EXCEEDED,
                        false,
                        "encoded materialization task exceeds the 64 KiB protocol limit"));
            }
            List<CompletableFuture<Void>> sourceChecks = new ArrayList<>(exactTask.sources().size());
            for (SourceGeneration source : exactTask.sources()) {
                sourceChecks.add(generations.getCandidate(
                                cluster,
                                exactTask.streamId(),
                                source.view(),
                                source.range().endOffset(),
                                source.generation())
                        .thenAccept(candidate -> requireExactSource(candidate, exactTask.streamId(), source)));
            }
            return CompletableFuture.allOf(sourceChecks.toArray(CompletableFuture[]::new))
                    .thenCompose(ignored -> exactGuard.revalidate())
                    .thenCompose(ignored -> generations.createTask(cluster, planned))
                    .thenApply(durable -> {
                        MaterializationTask recovered = requireTask(durable, exactTask.policy());
                        if (!recovered.equals(exactTask)) {
                            throw invariant("created task does not round-trip to the exact planned identity");
                        }
                        return durable;
                    });
        });
    }

    public CompletableFuture<Optional<VersionedMaterializationTask>> get(
            StreamId streamId,
            String taskId) {
        return async(() -> generations.getTask(
                cluster,
                Objects.requireNonNull(streamId, "streamId"),
                requireText(taskId, "taskId")));
    }

    public CompletableFuture<TaskScanPage> scan(
            StreamId streamId,
            Optional<F4ScanToken> continuation,
            int limit) {
        return async(() -> generations.scanTasks(
                cluster,
                Objects.requireNonNull(streamId, "streamId"),
                Objects.requireNonNull(continuation, "continuation"),
                limit));
    }

    public CompletableFuture<VersionedMaterializationTask> compareAndSet(
            MaterializationTaskRecord replacement,
            long expectedVersion) {
        return async(() -> generations.compareAndSetTask(
                cluster,
                Objects.requireNonNull(replacement, "replacement"),
                expectedVersion));
    }

    public MaterializationTask requireTask(
            VersionedMaterializationTask durable) {
        return MaterializationRecordMapper.domainTask(durable);
    }

    public MaterializationTask requireTask(
            VersionedMaterializationTask durable,
            MaterializationPolicy policy) {
        return MaterializationRecordMapper.domainTask(durable, policy);
    }

    private static void requireExactSource(
            Optional<VersionedGenerationCandidate> candidate,
            StreamId streamId,
            SourceGeneration expected) {
        if (candidate.isEmpty()
                || !MaterializationSourceMapper.matchesExactSource(
                        candidate.orElseThrow(), streamId, expected)) {
            throw new NereusException(
                    ErrorCode.METADATA_CONDITION_FAILED,
                    true,
                    "materialization source changed before deterministic task creation");
        }
    }

    private static <T> CompletableFuture<T> async(Supplier<CompletableFuture<T>> operation) {
        try {
            return Objects.requireNonNull(operation.get(), "operation future");
        } catch (RuntimeException failure) {
            return CompletableFuture.failedFuture(failure);
        }
    }

    private static NereusException invariant(String message) {
        return new NereusException(ErrorCode.METADATA_INVARIANT_VIOLATION, false, message);
    }

    private static String requireText(String value, String field) {
        Objects.requireNonNull(value, field);
        if (value.isBlank()) {
            throw new IllegalArgumentException(field + " cannot be blank");
        }
        return value;
    }
}
