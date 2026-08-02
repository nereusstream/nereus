/*
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 *   https://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

package com.nereusstream.kafka.compaction;

import com.nereusstream.api.Checksum;
import com.nereusstream.api.ChecksumType;
import com.nereusstream.api.MetadataCanonicalizer;
import com.nereusstream.api.ObjectType;
import com.nereusstream.api.ProjectionRef;
import com.nereusstream.api.ReadView;
import com.nereusstream.api.SchemaRef;
import com.nereusstream.api.target.ObjectSliceReadTarget;
import com.nereusstream.kafka.compaction.KafkaCompactionParquetPublisher.PreparedObject;
import com.nereusstream.materialization.GenerationCommitResult;
import com.nereusstream.materialization.GenerationCommitter;
import com.nereusstream.materialization.MaterializationOutput;
import com.nereusstream.materialization.MaterializationPolicy;
import com.nereusstream.materialization.MaterializationTask;
import com.nereusstream.materialization.MaterializationTaskMutationGuard;
import com.nereusstream.materialization.MaterializationTaskStore;
import com.nereusstream.materialization.SourceGeneration;
import com.nereusstream.metadata.oxia.KafkaCompactionCoverageActivationMode;
import com.nereusstream.metadata.oxia.KafkaMetadataConditionFailedException;
import com.nereusstream.metadata.oxia.KafkaPartitionId;
import com.nereusstream.metadata.oxia.KafkaPartitionMetadataStore;
import com.nereusstream.metadata.oxia.VersionedKafkaPartitionBinding;
import com.nereusstream.metadata.oxia.VersionedMaterializationTask;
import com.nereusstream.metadata.oxia.codec.ReadTargetCodecRegistry;
import com.nereusstream.metadata.oxia.records.KafkaCompactionCoverageRecord;
import com.nereusstream.metadata.oxia.records.KafkaPartitionLifecycle;
import com.nereusstream.metadata.oxia.records.TaskLifecycle;
import com.nereusstream.objectstore.HeadObjectOptions;
import com.nereusstream.objectstore.HeadObjectResult;
import com.nereusstream.objectstore.ObjectStore;
import com.nereusstream.objectstore.PutObjectOptions;
import com.nereusstream.objectstore.PutObjectResult;
import com.nereusstream.objectstore.compacted.CompactedObjectFormatV2;
import com.nereusstream.objectstore.compacted.RangedCompactedObjectVerificationRequest;
import com.nereusstream.objectstore.compacted.RangedCompactedObjectVerifier;
import com.nereusstream.objectstore.compacted.RangedCompactedObjectWriteResult;
import java.time.Clock;
import java.time.Duration;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.HexFormat;
import java.util.List;
import java.util.Map;
import java.util.Objects;
import java.util.Optional;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.CompletionException;

/**
 * Uploads one staged NTC2 object, commits its F4 generation, then linearizes client visibility in
 * the Kafka partition binding.
 *
 * <p>The prepared object ownership transfers to {@link #publish}; every terminal path closes it. A
 * committed generation without a successful binding CAS remains non-mandatory and cannot expose
 * compacted semantics.
 */
public final class KafkaCompactionPublicationCoordinator {
    private static final int MAX_RECOVERY_ATTEMPTS = 8;
    private static final String CONTENT_TYPE = "application/vnd.apache.parquet";

    private final String cluster;
    private final ObjectStore objectStore;
    private final RangedCompactedObjectVerifier objectVerifier;
    private final MaterializationTaskStore tasks;
    private final GenerationCommitter generations;
    private final KafkaPartitionMetadataStore partitions;
    private final Duration timeout;
    private final Clock clock;

    public KafkaCompactionPublicationCoordinator(
            String cluster,
            ObjectStore objectStore,
            RangedCompactedObjectVerifier objectVerifier,
            MaterializationTaskStore tasks,
            GenerationCommitter generations,
            KafkaPartitionMetadataStore partitions,
            Duration timeout,
            Clock clock) {
        this.cluster = requireText(cluster, "cluster");
        this.objectStore = Objects.requireNonNull(objectStore, "objectStore");
        this.objectVerifier = Objects.requireNonNull(objectVerifier, "objectVerifier");
        this.tasks = Objects.requireNonNull(tasks, "tasks");
        this.generations = Objects.requireNonNull(generations, "generations");
        this.partitions = Objects.requireNonNull(partitions, "partitions");
        this.timeout = requirePositive(timeout, "timeout");
        this.clock = Objects.requireNonNull(clock, "clock");
    }

    public CompletableFuture<VersionedMaterializationTask> claim(
            VersionedMaterializationTask expected, String claimId, String processRunId, long expiresAtMillis) {
        try {
            VersionedMaterializationTask exact = Objects.requireNonNull(expected, "expected");
            String exactClaim = requireText(claimId, "claimId");
            String exactProcess = requireText(processRunId, "processRunId");
            if (expiresAtMillis <= clock.millis()) {
                throw new IllegalArgumentException("Kafka compaction claim must expire in the future");
            }
            return claim(exact, exactClaim, exactProcess, expiresAtMillis, 0);
        } catch (Throwable failure) {
            return CompletableFuture.failedFuture(failure);
        }
    }

    public CompletableFuture<PublicationResult> publish(
            KafkaPartitionId partition,
            VersionedMaterializationTask claimedTask,
            KafkaCompactionPlan recoveredPlan,
            PreparedObject prepared,
            KafkaCompactionCoverageActivationMode activationMode,
            Optional<KafkaCompactionGenerationSet> previousGenerationSet,
            MaterializationTaskMutationGuard authority) {
        PreparedObject owned = prepared;
        try {
            KafkaPartitionId exactPartition = Objects.requireNonNull(partition, "partition");
            VersionedMaterializationTask exactClaim = Objects.requireNonNull(claimedTask, "claimedTask");
            KafkaCompactionPlan plan = Objects.requireNonNull(recoveredPlan, "recoveredPlan");
            owned = Objects.requireNonNull(prepared, "prepared");
            KafkaCompactionCoverageActivationMode mode = Objects.requireNonNull(activationMode, "activationMode");
            Optional<KafkaCompactionGenerationSet> previous =
                    Objects.requireNonNull(previousGenerationSet, "previousGenerationSet");
            MaterializationTaskMutationGuard exactAuthority = Objects.requireNonNull(authority, "authority");
            MaterializationTask task = tasks.requireTask(exactClaim);
            validateClaimAndPrepared(exactClaim, task, plan, owned);

            PreparedObject closeOwned = owned;
            CompletableFuture<PublicationResult> result = loadBinding(exactPartition)
                    .thenApply(binding -> {
                        validateActivationBasis(binding, task, plan, mode, previous);
                        return binding;
                    })
                    .thenCompose(basis -> revalidatePublicationAuthority(
                                    exactPartition,
                                    exactClaim,
                                    task,
                                    plan,
                                    basis.value().compactionCoverage(),
                                    exactAuthority)
                            .thenCompose(ignored ->
                                    upload(exactPartition, exactClaim, task, plan, basis, closeOwned, exactAuthority))
                            .thenCompose(head -> {
                                MaterializationOutput output = output(task, closeOwned.written(), closeOwned, head);
                                return verifyPrepared(closeOwned)
                                        .thenCompose(ignored -> revalidatePublicationAuthority(
                                                exactPartition,
                                                exactClaim,
                                                task,
                                                plan,
                                                basis.value().compactionCoverage(),
                                                exactAuthority))
                                        .thenCompose(ignored -> freezeOutput(
                                                exactPartition,
                                                exactClaim,
                                                task,
                                                plan,
                                                basis.value().compactionCoverage(),
                                                output,
                                                exactAuthority,
                                                0))
                                        .thenCompose(ignored -> generations.publish(task, output, exactAuthority))
                                        .thenCompose(committed -> {
                                            KafkaCompactionGenerationSet desired =
                                                    desiredGenerationSet(mode, previous, committed);
                                            return activateCoverage(
                                                            exactPartition,
                                                            task,
                                                            plan,
                                                            mode,
                                                            previous,
                                                            basis.value().compactionCoverage(),
                                                            desired,
                                                            exactAuthority,
                                                            0)
                                                    .thenApply(activated -> new PublicationResult(
                                                            output,
                                                            committed,
                                                            desired,
                                                            activated,
                                                            !basis.value()
                                                                    .compactionCoverage()
                                                                    .equals(activated
                                                                            .value()
                                                                            .compactionCoverage())));
                                        });
                            }));
            result.whenComplete((ignored, failure) -> closeOwned.close());
            return result;
        } catch (Throwable failure) {
            if (owned != null) {
                owned.close();
            }
            return CompletableFuture.failedFuture(failure);
        }
    }

    /**
     * Re-enters generation publication and coverage activation from a durable task output.
     *
     * <p>This is the restart path for crashes after {@code OUTPUT_READY}, during generic generation
     * publication, or after the binding CAS response was lost. It never requires the staging file:
     * the durable output/read target has already passed the full NTC2 verification boundary before it
     * was frozen.
     */
    public CompletableFuture<PublicationResult> recoverPublication(
            KafkaPartitionId partition,
            VersionedMaterializationTask durableTask,
            KafkaCompactionPlan recoveredPlan,
            KafkaCompactionCoverageActivationMode activationMode,
            Optional<KafkaCompactionGenerationSet> previousGenerationSet,
            MaterializationTaskMutationGuard authority) {
        try {
            KafkaPartitionId exactPartition = Objects.requireNonNull(partition, "partition");
            VersionedMaterializationTask exactDurable = Objects.requireNonNull(durableTask, "durableTask");
            KafkaCompactionPlan plan = Objects.requireNonNull(recoveredPlan, "recoveredPlan");
            KafkaCompactionCoverageActivationMode mode = Objects.requireNonNull(activationMode, "activationMode");
            Optional<KafkaCompactionGenerationSet> previous =
                    Objects.requireNonNull(previousGenerationSet, "previousGenerationSet");
            MaterializationTaskMutationGuard exactAuthority = Objects.requireNonNull(authority, "authority");
            MaterializationTask task = tasks.requireTask(exactDurable);
            MaterializationOutput output = tasks.requireOutput(exactDurable)
                    .orElseThrow(() -> new KafkaMetadataConditionFailedException(
                            "Kafka compaction recovery task has no durable output"));
            validateRecoveredOutput(exactDurable, task, plan, output);

            return revalidateRecoveryAuthority(exactPartition, task, plan, output, Optional.empty(), exactAuthority)
                    .thenCompose(ignored -> generations.publish(task, output, exactAuthority))
                    .thenCompose(committed -> {
                        KafkaCompactionGenerationSet desired = desiredGenerationSet(mode, previous, committed);
                        return loadBinding(exactPartition).thenCompose(current -> {
                            validateBindingWindow(current, task, plan);
                            if (sameDesiredCoverage(current.value().compactionCoverage(), plan, desired, task)) {
                                return CompletableFuture.completedFuture(
                                        new PublicationResult(output, committed, desired, current, false));
                            }
                            validateActivationBasis(current, task, plan, mode, previous);
                            KafkaCompactionCoverageRecord basis =
                                    current.value().compactionCoverage();
                            return revalidateRecoveryAuthority(
                                            exactPartition, task, plan, output, Optional.of(basis), exactAuthority)
                                    .thenCompose(ignored -> activateCoverage(
                                            exactPartition,
                                            task,
                                            plan,
                                            mode,
                                            previous,
                                            basis,
                                            desired,
                                            exactAuthority,
                                            0))
                                    .thenApply(activated -> new PublicationResult(
                                            output,
                                            committed,
                                            desired,
                                            activated,
                                            !basis.equals(activated.value().compactionCoverage())));
                        });
                    });
        } catch (Throwable failure) {
            return CompletableFuture.failedFuture(failure);
        }
    }

    private CompletableFuture<VersionedMaterializationTask> claim(
            VersionedMaterializationTask expected,
            String claimId,
            String processRunId,
            long expiresAtMillis,
            int attempt) {
        if (attempt >= MAX_RECOVERY_ATTEMPTS) {
            return CompletableFuture.failedFuture(new KafkaMetadataConditionFailedException(
                    "Kafka compaction task claim exhausted bounded CAS recovery"));
        }
        return tasks.claim(expected, claimId, processRunId, expiresAtMillis)
                .handle((claimed, failure) -> {
                    if (failure == null) {
                        return CompletableFuture.completedFuture(claimed);
                    }
                    Throwable original = unwrap(failure);
                    MaterializationTask task = tasks.requireTask(expected);
                    return tasks.get(task.streamId(), task.taskId()).thenCompose(optional -> {
                        VersionedMaterializationTask reloaded =
                                optional.orElseThrow(() -> new KafkaMetadataConditionFailedException(
                                        "Kafka compaction task disappeared " + "during claim"));
                        if (sameClaim(reloaded, claimId, processRunId)
                                && reloaded.value().workerClaim().orElseThrow().expiresAtMillis() == expiresAtMillis) {
                            return CompletableFuture.completedFuture(reloaded);
                        }
                        if (reloaded.value().lifecycle() == expected.value().lifecycle()) {
                            return claim(reloaded, claimId, processRunId, expiresAtMillis, attempt + 1);
                        }
                        return CompletableFuture.failedFuture(original);
                    });
                })
                .thenCompose(value -> value);
    }

    private CompletableFuture<HeadObjectResult> upload(
            KafkaPartitionId partition,
            VersionedMaterializationTask claimed,
            MaterializationTask task,
            KafkaCompactionPlan plan,
            VersionedKafkaPartitionBinding basis,
            PreparedObject prepared,
            MaterializationTaskMutationGuard authority) {
        RangedCompactedObjectWriteResult written = prepared.written();
        PutObjectOptions options = new PutObjectOptions(
                CONTENT_TYPE,
                written.storageCrc32c(),
                true,
                Map.of(
                        "nereus.format",
                        CompactedObjectFormatV2.TOPIC_COMPACTED_FORMAT_ID,
                        "nereus-task-id",
                        task.taskId(),
                        "nereus-output-attempt",
                        prepared.request().outputAttemptId(),
                        "nereus-content-sha256",
                        written.contentSha256().value()),
                timeout);
        CompletableFuture<PutObjectResult> put;
        try {
            put = Objects.requireNonNull(
                    objectStore.putObject(written.objectKey(), written.stagingFile(), options, (key, attempt) -> {
                        if (!key.equals(written.objectKey()) || attempt <= 0) {
                            return CompletableFuture.failedFuture(
                                    new IllegalStateException("guarded NTC2 upload identity is invalid"));
                        }
                        return revalidatePublicationAuthority(
                                partition, claimed, task, plan, basis.value().compactionCoverage(), authority);
                    }),
                    "NTC2 upload future");
        } catch (Throwable failure) {
            return CompletableFuture.failedFuture(failure);
        }
        return put.handle((uploaded, failure) -> {
                    if (failure == null) {
                        validatePut(uploaded, written);
                        return head(written);
                    }
                    Throwable original = unwrap(failure);
                    return head(written).handle((reconciled, recoveryFailure) -> {
                        if (recoveryFailure == null) {
                            return reconciled;
                        }
                        throw new CompletionException(original);
                    });
                })
                .thenCompose(value -> value);
    }

    private CompletableFuture<HeadObjectResult> head(RangedCompactedObjectWriteResult written) {
        CompletableFuture<HeadObjectResult> future;
        try {
            future = Objects.requireNonNull(
                    objectStore.headObject(written.objectKey(), new HeadObjectOptions(timeout)), "NTC2 HEAD future");
        } catch (Throwable failure) {
            return CompletableFuture.failedFuture(failure);
        }
        return future.thenApply(head -> {
            if (!head.key().equals(written.objectKey())
                    || head.objectLength() != written.objectLength()
                    || !head.checksum().equals(written.storageCrc32c())) {
                throw new IllegalStateException("uploaded NTC2 HEAD differs from the staged object");
            }
            return head;
        });
    }

    private CompletableFuture<Void> verifyPrepared(PreparedObject prepared) {
        return objectVerifier.verifyExact(
                RangedCompactedObjectVerificationRequest.from(prepared.request(), prepared.written(), timeout),
                prepared.request());
    }

    private CompletableFuture<VersionedMaterializationTask> freezeOutput(
            KafkaPartitionId partition,
            VersionedMaterializationTask current,
            MaterializationTask task,
            KafkaCompactionPlan plan,
            KafkaCompactionCoverageRecord basisCoverage,
            MaterializationOutput output,
            MaterializationTaskMutationGuard authority,
            int attempt) {
        if (attempt >= MAX_RECOVERY_ATTEMPTS) {
            return CompletableFuture.failedFuture(new KafkaMetadataConditionFailedException(
                    "Kafka compaction output freeze exhausted bounded CAS recovery"));
        }
        return tasks.outputReady(current, output)
                .handle((ready, failure) -> {
                    if (failure == null) {
                        return CompletableFuture.completedFuture(ready);
                    }
                    Throwable original = unwrap(failure);
                    return tasks.get(task.streamId(), task.taskId()).thenCompose(optional -> {
                        VersionedMaterializationTask reloaded =
                                optional.orElseThrow(() -> new KafkaMetadataConditionFailedException(
                                        "Kafka compaction task disappeared " + "during output freeze"));
                        Optional<MaterializationOutput> durableOutput = tasks.requireOutput(reloaded);
                        if (durableOutput.isPresent()) {
                            if (durableOutput.orElseThrow().equals(output)
                                    && (reloaded.value().lifecycle() == TaskLifecycle.OUTPUT_READY
                                            || reloaded.value().lifecycle() == TaskLifecycle.PUBLISHING
                                            || reloaded.value().lifecycle() == TaskLifecycle.PUBLISHED)) {
                                return CompletableFuture.completedFuture(reloaded);
                            }
                            return CompletableFuture.failedFuture(new KafkaMetadataConditionFailedException(
                                    "Kafka compaction task contains a different " + "output"));
                        }
                        if (sameClaim(
                                        reloaded,
                                        output.outputAttemptId(),
                                        current.value()
                                                .workerClaim()
                                                .orElseThrow()
                                                .processRunId())
                                && claimIsLive(reloaded)) {
                            return revalidatePublicationAuthority(
                                            partition, reloaded, task, plan, basisCoverage, authority)
                                    .thenCompose(ignored -> freezeOutput(
                                            partition,
                                            reloaded,
                                            task,
                                            plan,
                                            basisCoverage,
                                            output,
                                            authority,
                                            attempt + 1));
                        }
                        return CompletableFuture.failedFuture(original);
                    });
                })
                .thenCompose(value -> value);
    }

    private CompletableFuture<VersionedKafkaPartitionBinding> activateCoverage(
            KafkaPartitionId partition,
            MaterializationTask task,
            KafkaCompactionPlan plan,
            KafkaCompactionCoverageActivationMode mode,
            Optional<KafkaCompactionGenerationSet> previous,
            KafkaCompactionCoverageRecord basisCoverage,
            KafkaCompactionGenerationSet desired,
            MaterializationTaskMutationGuard authority,
            int attempt) {
        if (attempt >= MAX_RECOVERY_ATTEMPTS) {
            return CompletableFuture.failedFuture(new KafkaMetadataConditionFailedException(
                    "Kafka compaction coverage activation exhausted bounded CAS recovery"));
        }
        return loadBinding(partition).thenCompose(current -> {
            if (sameDesiredCoverage(current.value().compactionCoverage(), plan, desired, task)) {
                return CompletableFuture.completedFuture(current);
            }
            if (!current.value().compactionCoverage().equals(basisCoverage)) {
                return CompletableFuture.failedFuture(new KafkaMetadataConditionFailedException(
                        "Kafka compaction coverage changed before activation"));
            }
            validateActivationBasis(current, task, plan, mode, previous);
            long activatedAt = Math.max(clock.millis(), current.value().updatedAtMillis());
            return authority
                    .revalidate()
                    .thenCompose(ignored -> partitions.activateCompactionCoverage(
                            current,
                            mode,
                            plan.candidate().decisionHorizon().endOffset(),
                            desired.coverage().startOffset(),
                            desired.coverage().endOffset(),
                            desired.digestBytes(),
                            checksumBytes(task.policyDigestSha256()),
                            activatedAt))
                    .handle((activated, failure) -> {
                        if (failure == null) {
                            return CompletableFuture.completedFuture(activated);
                        }
                        Throwable original = unwrap(failure);
                        return loadBinding(partition).thenCompose(reloaded -> {
                            if (sameDesiredCoverage(reloaded.value().compactionCoverage(), plan, desired, task)) {
                                return CompletableFuture.completedFuture(reloaded);
                            }
                            if (reloaded.value().compactionCoverage().equals(basisCoverage)) {
                                return activateCoverage(
                                        partition,
                                        task,
                                        plan,
                                        mode,
                                        previous,
                                        basisCoverage,
                                        desired,
                                        authority,
                                        attempt + 1);
                            }
                            return CompletableFuture.failedFuture(original);
                        });
                    })
                    .thenCompose(value -> value);
        });
    }

    private CompletableFuture<Void> revalidatePublicationAuthority(
            KafkaPartitionId partition,
            VersionedMaterializationTask claimed,
            MaterializationTask task,
            KafkaCompactionPlan plan,
            KafkaCompactionCoverageRecord basisCoverage,
            MaterializationTaskMutationGuard authority) {
        return tasks.get(task.streamId(), task.taskId())
                .thenCompose(optional -> {
                    VersionedMaterializationTask current = optional.orElseThrow(
                            () -> new KafkaMetadataConditionFailedException("Kafka compaction task disappeared"));
                    if (!sameClaimOwner(current, claimed, task) || !claimIsLive(current)) {
                        return CompletableFuture.failedFuture(new KafkaMetadataConditionFailedException(
                                "Kafka compaction worker claim changed or expired"));
                    }
                    return loadBinding(partition);
                })
                .thenAccept(binding -> {
                    validateBindingWindow(binding, task, plan);
                    if (!binding.value().compactionCoverage().equals(basisCoverage)) {
                        throw new KafkaMetadataConditionFailedException(
                                "Kafka compaction coverage changed during object preparation");
                    }
                })
                .thenCompose(ignored -> authority.revalidate());
    }

    private CompletableFuture<Void> revalidateRecoveryAuthority(
            KafkaPartitionId partition,
            MaterializationTask task,
            KafkaCompactionPlan plan,
            MaterializationOutput output,
            Optional<KafkaCompactionCoverageRecord> expectedCoverage,
            MaterializationTaskMutationGuard authority) {
        return tasks.get(task.streamId(), task.taskId())
                .thenCompose(optional -> {
                    VersionedMaterializationTask current = optional.orElseThrow(() ->
                            new KafkaMetadataConditionFailedException("Kafka compaction recovery task disappeared"));
                    MaterializationTask currentTask = tasks.requireTask(current);
                    Optional<MaterializationOutput> currentOutput = tasks.requireOutput(current);
                    if (!currentTask.equals(task)
                            || !isRecoverableOutputLifecycle(current.value().lifecycle())
                            || currentOutput.filter(output::equals).isEmpty()) {
                        return CompletableFuture.failedFuture(new KafkaMetadataConditionFailedException(
                                "Kafka compaction recovery task/output changed"));
                    }
                    return loadBinding(partition);
                })
                .thenAccept(binding -> {
                    validateBindingWindow(binding, task, plan);
                    expectedCoverage.ifPresent(expected -> {
                        if (!binding.value().compactionCoverage().equals(expected)) {
                            throw new KafkaMetadataConditionFailedException(
                                    "Kafka compaction coverage changed during publication recovery");
                        }
                    });
                })
                .thenCompose(ignored -> authority.revalidate());
    }

    private CompletableFuture<VersionedKafkaPartitionBinding> loadBinding(KafkaPartitionId partition) {
        try {
            return Objects.requireNonNull(partitions.get(partition), "partition get future")
                    .thenApply(optional -> optional.orElseThrow(
                            () -> new KafkaMetadataConditionFailedException("Kafka partition binding is absent")));
        } catch (Throwable failure) {
            return CompletableFuture.failedFuture(failure);
        }
    }

    private void validateClaimAndPrepared(
            VersionedMaterializationTask claimed,
            MaterializationTask task,
            KafkaCompactionPlan plan,
            PreparedObject prepared) {
        plan.requireMaterializationTask(task);
        if (claimed.value().lifecycle() != TaskLifecycle.CLAIMED
                || claimed.value().workerClaim().isEmpty()
                || !claimed.value()
                        .workerClaim()
                        .orElseThrow()
                        .claimId()
                        .equals(prepared.request().outputAttemptId())
                || task.view() != ReadView.TOPIC_COMPACTED
                || !task.policy().targetPhysicalFormat().equals(MaterializationPolicy.KAFKA_TOPIC_COMPACTED_FORMAT)
                || !prepared.request().cluster().equals(cluster)
                || !prepared.request().streamId().equals(task.streamId())
                || !prepared.request().sourceCoverage().equals(task.coverage())
                || !prepared.request().sourceSetSha256().equals(task.sourceSetSha256())
                || !prepared.request().policySha256().equals(task.policyDigestSha256())
                || !prepared.evidence().outputSourceSetSha256().equals(task.sourceSetSha256())
                || !prepared.written().physicalFormat().equals(MaterializationPolicy.KAFKA_TOPIC_COMPACTED_FORMAT)
                || prepared.request().targetRowGroupRecords() != task.policy().targetRowGroupRecords()
                || !prepared.request().compression().equals(task.policy().compression())) {
            throw new IllegalArgumentException("claimed task, recovered plan and prepared NTC2 object do not agree");
        }
        List<SourceGeneration> sources = task.sources();
        if (prepared.request().cumulativeSizeAtEnd()
                != sources.get(sources.size() - 1).cumulativeSizeAtEnd()) {
            throw new IllegalArgumentException("prepared NTC2 cumulative end differs from the exact task sources");
        }
    }

    private static void validateActivationBasis(
            VersionedKafkaPartitionBinding binding,
            MaterializationTask task,
            KafkaCompactionPlan plan,
            KafkaCompactionCoverageActivationMode mode,
            Optional<KafkaCompactionGenerationSet> previous) {
        validateBindingWindow(binding, task, plan);
        KafkaCompactionCoverageRecord coverage = binding.value().compactionCoverage();
        switch (mode) {
            case INITIAL -> {
                if (coverage.coverageVersion() != 0
                        || previous.isPresent()
                        || plan.candidate().previousMandatoryCoverage().isPresent()
                        || task.coverage().startOffset() != binding.value().observedLogStartOffset()) {
                    throw new KafkaMetadataConditionFailedException(
                            "initial Kafka compaction activation basis is not empty");
                }
            }
            case EXTEND -> {
                KafkaCompactionGenerationSet exactPrevious =
                        previous.orElseThrow(() -> new KafkaMetadataConditionFailedException(
                                "Kafka compaction extension lacks the previous generation set"));
                if (coverage.coverageVersion() == 0
                        || plan.candidate().previousMandatoryCoverage().isEmpty()
                        || !plan.candidate()
                                .previousMandatoryCoverage()
                                .orElseThrow()
                                .equals(exactPrevious.coverage())
                        || !exactPrevious
                                .coverage()
                                .equals(new com.nereusstream.api.OffsetRange(
                                        coverage.startOffset(), coverage.endOffset()))
                        || !Arrays.equals(exactPrevious.digestBytes(), coverage.generationSetSha256())
                        || !Arrays.equals(checksumBytes(task.policyDigestSha256()), coverage.policySha256())) {
                    throw new KafkaMetadataConditionFailedException(
                            "Kafka compaction extension basis does not match activated coverage");
                }
            }
            case REPLACE -> {
                KafkaCompactionGenerationSet exactPrevious =
                        previous.orElseThrow(() -> new KafkaMetadataConditionFailedException(
                                "Kafka compaction replacement lacks the previous generation set"));
                if (coverage.coverageVersion() == 0
                        || !exactPrevious
                                .coverage()
                                .equals(new com.nereusstream.api.OffsetRange(
                                        coverage.startOffset(), coverage.endOffset()))
                        || !Arrays.equals(exactPrevious.digestBytes(), coverage.generationSetSha256())) {
                    throw new KafkaMetadataConditionFailedException(
                            "Kafka compaction replacement basis does not match activated coverage");
                }
            }
        }
    }

    private static void validateBindingWindow(
            VersionedKafkaPartitionBinding binding, MaterializationTask task, KafkaCompactionPlan plan) {
        if (binding.value().lifecycle() != KafkaPartitionLifecycle.ACTIVE
                || !binding.value().streamId().equals(task.streamId().value())
                || binding.metadataVersion() < plan.bindingMetadataVersion()
                || binding.value().observedLogStartOffset() > task.coverage().startOffset()) {
            throw new KafkaMetadataConditionFailedException(
                    "Kafka partition binding no longer authorizes compaction publication");
        }
    }

    private static KafkaCompactionGenerationSet desiredGenerationSet(
            KafkaCompactionCoverageActivationMode mode,
            Optional<KafkaCompactionGenerationSet> previous,
            GenerationCommitResult committed) {
        return switch (mode) {
            case INITIAL -> KafkaCompactionGenerationSet.initial(committed);
            case EXTEND -> previous.orElseThrow().extend(committed);
            case REPLACE -> {
                KafkaCompactionGenerationSet replacement = KafkaCompactionGenerationSet.replacement(committed);
                KafkaCompactionGenerationSet before = previous.orElseThrow();
                if (replacement.coverage().startOffset() != before.coverage().startOffset()
                        || replacement.coverage().endOffset()
                                < before.coverage().endOffset()) {
                    throw new KafkaMetadataConditionFailedException(
                            "replacement generation does not cover the mandatory compacted range");
                }
                yield replacement;
            }
        };
    }

    private static MaterializationOutput output(
            MaterializationTask task,
            RangedCompactedObjectWriteResult written,
            PreparedObject prepared,
            HeadObjectResult head) {
        ObjectSliceReadTarget target = new ObjectSliceReadTarget(
                1,
                written.objectId(),
                written.objectKey(),
                ObjectType.STREAM_COMPACTED_OBJECT,
                written.physicalFormat(),
                CompactedObjectFormatV2.KAFKA_LOGICAL_FORMAT,
                task.taskId(),
                0,
                written.objectLength(),
                written.storageCrc32c(),
                written.entryIndexRef());
        Checksum targetIdentity = new Checksum(
                ChecksumType.SHA256,
                ReadTargetCodecRegistry.phase15().encode(target).identityChecksumValue());
        SourceFacts facts = sourceFacts(task);
        return new MaterializationOutput(
                task.taskId(),
                task.streamId(),
                task.view(),
                task.coverage(),
                prepared.request().outputAttemptId(),
                written.objectId(),
                written.objectKey(),
                written.objectKeyHash(),
                written.objectLength(),
                written.storageCrc32c(),
                written.contentSha256(),
                head.etag().orElse(""),
                written.physicalFormat(),
                CompactedObjectFormatV2.KAFKA_LOGICAL_FORMAT,
                target,
                targetIdentity,
                written.entryIndexRef(),
                Math.toIntExact(task.coverage().recordCount()),
                Math.toIntExact(written.outputRecordCount()),
                written.outputEntryCount(),
                prepared.request().logicalBytes(),
                facts.schemaRefs(),
                facts.cumulativeSizeAtStart(),
                prepared.request().cumulativeSizeAtEnd(),
                task.sourceSetSha256(),
                facts.projectionRef());
    }

    private static SourceFacts sourceFacts(MaterializationTask task) {
        List<SchemaRef> schemas = new ArrayList<>();
        Optional<ProjectionRef> projection = task.sources().get(0).projectionRef();
        for (SourceGeneration source : task.sources()) {
            if (!source.projectionRef().equals(projection)) {
                throw new IllegalArgumentException("Kafka compaction sources use different projection identities");
            }
            schemas.addAll(source.schemaRefs());
        }
        return new SourceFacts(
                MetadataCanonicalizer.canonicalSchemaRefs(schemas),
                task.sources().get(0).cumulativeSizeAtStart(),
                projection);
    }

    private static void validatePut(PutObjectResult uploaded, RangedCompactedObjectWriteResult written) {
        if (!uploaded.key().equals(written.objectKey())
                || uploaded.objectLength() != written.objectLength()
                || !uploaded.checksum().equals(written.storageCrc32c())) {
            throw new IllegalStateException("NTC2 PUT result differs from the staged object");
        }
    }

    private static boolean sameDesiredCoverage(
            KafkaCompactionCoverageRecord actual,
            KafkaCompactionPlan plan,
            KafkaCompactionGenerationSet desired,
            MaterializationTask task) {
        long expectedEpoch = plan.candidate()
                .previousMandatoryCoverage()
                .map(coverage -> Math.addExact(coverage.activationEpoch(), 1))
                .orElse(1L);
        return actual.coverageVersion() == 1
                && actual.startOffset() == desired.coverage().startOffset()
                && actual.endOffset() == desired.coverage().endOffset()
                && actual.activationEpoch() == expectedEpoch
                && Arrays.equals(actual.generationSetSha256(), desired.digestBytes())
                && Arrays.equals(actual.policySha256(), checksumBytes(task.policyDigestSha256()));
    }

    private static void validateRecoveredOutput(
            VersionedMaterializationTask durable,
            MaterializationTask task,
            KafkaCompactionPlan plan,
            MaterializationOutput output) {
        plan.requireMaterializationTask(task);
        if (!isRecoverableOutputLifecycle(durable.value().lifecycle())
                || !output.taskId().equals(task.taskId())
                || !output.streamId().equals(task.streamId())
                || output.view() != ReadView.TOPIC_COMPACTED
                || !output.coverage().equals(task.coverage())
                || !output.sourceSetSha256().equals(task.sourceSetSha256())
                || !output.physicalFormat().equals(MaterializationPolicy.KAFKA_TOPIC_COMPACTED_FORMAT)
                || !output.logicalFormat().equals(CompactedObjectFormatV2.KAFKA_LOGICAL_FORMAT)) {
            throw new IllegalArgumentException(
                    "durable Kafka compaction task/output cannot enter publication recovery");
        }
    }

    private static boolean isRecoverableOutputLifecycle(TaskLifecycle lifecycle) {
        return lifecycle == TaskLifecycle.OUTPUT_READY
                || lifecycle == TaskLifecycle.PUBLISHING
                || lifecycle == TaskLifecycle.PUBLISHED;
    }

    private boolean sameClaimOwner(
            VersionedMaterializationTask current, VersionedMaterializationTask expected, MaterializationTask task) {
        if (!tasks.requireTask(current).equals(task)
                || current.value().lifecycle() != TaskLifecycle.CLAIMED
                || current.value().workerClaim().isEmpty()
                || expected.value().workerClaim().isEmpty()) {
            return false;
        }
        var actualClaim = current.value().workerClaim().orElseThrow();
        var expectedClaim = expected.value().workerClaim().orElseThrow();
        return actualClaim.claimId().equals(expectedClaim.claimId())
                && actualClaim.processRunId().equals(expectedClaim.processRunId())
                && actualClaim.attempt() == expectedClaim.attempt()
                && actualClaim.claimedAtMillis() == expectedClaim.claimedAtMillis()
                && actualClaim.expiresAtMillis() >= expectedClaim.expiresAtMillis();
    }

    private static boolean sameClaim(VersionedMaterializationTask task, String claimId, String processRunId) {
        return task.value().lifecycle() == TaskLifecycle.CLAIMED
                && task.value().workerClaim().isPresent()
                && task.value().workerClaim().orElseThrow().claimId().equals(claimId)
                && task.value().workerClaim().orElseThrow().processRunId().equals(processRunId);
    }

    private boolean claimIsLive(VersionedMaterializationTask task) {
        return task.value().workerClaim().isPresent()
                && task.value().workerClaim().orElseThrow().expiresAtMillis() > clock.millis();
    }

    private static byte[] checksumBytes(Checksum checksum) {
        if (checksum.type() != ChecksumType.SHA256) {
            throw new IllegalArgumentException("Kafka compaction policy digest must use SHA256");
        }
        return HexFormat.of().parseHex(checksum.value());
    }

    private static Throwable unwrap(Throwable failure) {
        Throwable current = failure;
        while ((current instanceof CompletionException || current instanceof java.util.concurrent.ExecutionException)
                && current.getCause() != null) {
            current = current.getCause();
        }
        return current;
    }

    private static Duration requirePositive(Duration value, String field) {
        Objects.requireNonNull(value, field);
        if (value.isZero() || value.isNegative()) {
            throw new IllegalArgumentException(field + " must be positive");
        }
        return value;
    }

    private static String requireText(String value, String field) {
        Objects.requireNonNull(value, field);
        if (value.isBlank()) {
            throw new IllegalArgumentException(field + " cannot be blank");
        }
        return value;
    }

    private record SourceFacts(
            List<SchemaRef> schemaRefs, long cumulativeSizeAtStart, Optional<ProjectionRef> projectionRef) {}

    public record PublicationResult(
            MaterializationOutput output,
            GenerationCommitResult committedGeneration,
            KafkaCompactionGenerationSet generationSet,
            VersionedKafkaPartitionBinding binding,
            boolean coverageActivatedByThisCall) {
        public PublicationResult {
            Objects.requireNonNull(output, "output");
            Objects.requireNonNull(committedGeneration, "committedGeneration");
            Objects.requireNonNull(generationSet, "generationSet");
            Objects.requireNonNull(binding, "binding");
            if (output.view() != ReadView.TOPIC_COMPACTED
                    || !output.streamId().equals(committedGeneration.streamId())
                    || !generationSet.streamId().equals(output.streamId())
                    || binding.value().compactionCoverage().coverageVersion() != 1
                    || !Arrays.equals(
                            binding.value().compactionCoverage().generationSetSha256(), generationSet.digestBytes())) {
                throw new IllegalArgumentException("Kafka compaction publication result is inconsistent");
            }
        }
    }
}
