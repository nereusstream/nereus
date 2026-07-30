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
import com.nereusstream.api.ErrorCode;
import com.nereusstream.api.NereusException;
import com.nereusstream.api.StreamId;
import com.nereusstream.kafka.compaction.KafkaCompactionPassOneCollector.AbortedTransactionRange;
import com.nereusstream.kafka.compaction.KafkaCompactionPassOneCollector.MarkerDecision;
import com.nereusstream.kafka.compaction.KafkaCompactionPassOneCollector.OpenTransactionRange;
import com.nereusstream.kafka.compaction.KafkaCompactionPassOneCollector.Snapshot;
import com.nereusstream.kafka.compaction.KafkaCompactionPlanner.Candidate;
import com.nereusstream.kafka.compaction.KafkaCompactionPublicationCoordinator.PublicationResult;
import com.nereusstream.kafka.compaction.KafkaCompactionTerminalRetirer.RetirementResult;
import com.nereusstream.kafka.compaction.KafkaCompactionWriteRequestFactory.Input;
import com.nereusstream.materialization.MaterializationFailure;
import com.nereusstream.materialization.MaterializationPolicy;
import com.nereusstream.materialization.MaterializationTask;
import com.nereusstream.materialization.MaterializationTaskMutationGuard;
import com.nereusstream.materialization.MaterializationTaskProtectionReconciler;
import com.nereusstream.materialization.MaterializationTaskStore;
import com.nereusstream.materialization.SecureWorkerClaimIdGenerator;
import com.nereusstream.materialization.WorkerClaimIdGenerator;
import com.nereusstream.metadata.oxia.KafkaCompactionCoverageActivationMode;
import com.nereusstream.metadata.oxia.KafkaCompactionPlanMetadataStore;
import com.nereusstream.metadata.oxia.KafkaCompactionPlanScanPage;
import com.nereusstream.metadata.oxia.KafkaCompactionPlanScanToken;
import com.nereusstream.metadata.oxia.KafkaPartitionId;
import com.nereusstream.metadata.oxia.VersionedKafkaCompactionPlan;
import com.nereusstream.metadata.oxia.VersionedKafkaPartitionBinding;
import com.nereusstream.metadata.oxia.VersionedMaterializationTask;
import com.nereusstream.metadata.oxia.records.KafkaCompactionCoverageRecord;
import com.nereusstream.metadata.oxia.records.KafkaPartitionLifecycle;
import com.nereusstream.metadata.oxia.records.TaskFailureClass;
import com.nereusstream.metadata.oxia.records.TaskLifecycle;
import java.time.Clock;
import java.time.Duration;
import java.util.ArrayList;
import java.util.HexFormat;
import java.util.List;
import java.util.Objects;
import java.util.Optional;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.CompletionException;
import java.util.concurrent.RejectedExecutionException;
import java.util.concurrent.ScheduledExecutorService;
import java.util.concurrent.ScheduledFuture;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicReference;

/**
 * Recoverable, single-partition Kafka compaction pass.
 *
 * <p>Every run first scans the immutable KCP1 children for a task-rooted workflow. Existing work is
 * recovered before a new candidate may be admitted. Fresh work composes the planner, authoritative
 * COMMITTED source resolver, plan-first KCP1/task coordinator, exact two-pass streams, NTC2 writer,
 * durable publication, binding coverage activation, and task-first/KCP1-second retirement.
 *
 * <p>The instance is dedicated to one {@link KafkaPartitionId}. Concurrent callers receive
 * cancellation-isolated views of the same run. The capture provider must execute inside the
 * partition's serialized Kafka authority domain and return a guard that both revalidates that
 * capture and prevents concurrent task admission until the returned run completes.
 */
public final class KafkaCompactionPartitionPass {
  private static final String EXPIRED_CLAIM_MESSAGE =
      "Kafka compaction worker claim expired during partition-pass recovery";

  private final String cluster;
  private final KafkaPartitionId partition;
  private final CaptureProvider captures;
  private final KafkaCompactionPlanner planner;
  private final KafkaCompactionSourceResolver sources;
  private final KafkaCompactionPlanCoordinator planCoordinator;
  private final KafkaCompactionPlanMetadataStore plans;
  private final MaterializationTaskStore tasks;
  private final MaterializationTaskProtectionReconciler taskProtections;
  private final KafkaCompactionBatchSource batches;
  private final KafkaCompactionParquetPublisher parquet;
  private final KafkaCompactionPublicationCoordinator publications;
  private final KafkaCompactionTerminalRetirer retirer;
  private final KafkaActivatedGenerationSetResolver activatedGenerations;
  private final KafkaCompactionPlanRecordMapper planMapper;
  private final String processRunId;
  private final WorkerClaimIdGenerator claimIds;
  private final Configuration configuration;
  private final ScheduledExecutorService scheduler;
  private final Clock clock;
  private final AtomicReference<CompletableFuture<RunResult>> inFlight = new AtomicReference<>();

  public KafkaCompactionPartitionPass(
      String cluster,
      KafkaPartitionId partition,
      CaptureProvider captures,
      KafkaCompactionPlanner planner,
      KafkaCompactionSourceResolver sources,
      KafkaCompactionPlanCoordinator planCoordinator,
      KafkaCompactionPlanMetadataStore plans,
      MaterializationTaskStore tasks,
      MaterializationTaskProtectionReconciler taskProtections,
      KafkaCompactionBatchSource batches,
      KafkaCompactionParquetPublisher parquet,
      KafkaCompactionPublicationCoordinator publications,
      KafkaCompactionTerminalRetirer retirer,
      KafkaActivatedGenerationSetResolver activatedGenerations,
      String processRunId,
      Configuration configuration,
      ScheduledExecutorService scheduler,
      Clock clock) {
    this(
        cluster,
        partition,
        captures,
        planner,
        sources,
        planCoordinator,
        plans,
        tasks,
        taskProtections,
        batches,
        parquet,
        publications,
        retirer,
        activatedGenerations,
        processRunId,
        new SecureWorkerClaimIdGenerator(),
        configuration,
        scheduler,
        clock,
        new KafkaCompactionPlanRecordMapper());
  }

  KafkaCompactionPartitionPass(
      String cluster,
      KafkaPartitionId partition,
      CaptureProvider captures,
      KafkaCompactionPlanner planner,
      KafkaCompactionSourceResolver sources,
      KafkaCompactionPlanCoordinator planCoordinator,
      KafkaCompactionPlanMetadataStore plans,
      MaterializationTaskStore tasks,
      MaterializationTaskProtectionReconciler taskProtections,
      KafkaCompactionBatchSource batches,
      KafkaCompactionParquetPublisher parquet,
      KafkaCompactionPublicationCoordinator publications,
      KafkaCompactionTerminalRetirer retirer,
      KafkaActivatedGenerationSetResolver activatedGenerations,
      String processRunId,
      WorkerClaimIdGenerator claimIds,
      Configuration configuration,
      ScheduledExecutorService scheduler,
      Clock clock,
      KafkaCompactionPlanRecordMapper planMapper) {
    this.cluster = requireText(cluster, "cluster");
    this.partition = Objects.requireNonNull(partition, "partition");
    this.captures = Objects.requireNonNull(captures, "captures");
    this.planner = Objects.requireNonNull(planner, "planner");
    this.sources = Objects.requireNonNull(sources, "sources");
    this.planCoordinator = Objects.requireNonNull(planCoordinator, "planCoordinator");
    this.plans = Objects.requireNonNull(plans, "plans");
    this.tasks = Objects.requireNonNull(tasks, "tasks");
    this.taskProtections = Objects.requireNonNull(taskProtections, "taskProtections");
    this.batches = Objects.requireNonNull(batches, "batches");
    this.parquet = Objects.requireNonNull(parquet, "parquet");
    this.publications = Objects.requireNonNull(publications, "publications");
    this.retirer = Objects.requireNonNull(retirer, "retirer");
    this.activatedGenerations =
        Objects.requireNonNull(activatedGenerations, "activatedGenerations");
    this.processRunId = requireBase32(processRunId, "processRunId");
    this.claimIds = Objects.requireNonNull(claimIds, "claimIds");
    this.configuration = Objects.requireNonNull(configuration, "configuration");
    this.scheduler = Objects.requireNonNull(scheduler, "scheduler");
    this.clock = Objects.requireNonNull(clock, "clock");
    this.planMapper = Objects.requireNonNull(planMapper, "planMapper");
  }

  /** Runs or joins the one currently active pass for this partition. */
  public CompletableFuture<RunResult> runOnce() {
    for (; ; ) {
      CompletableFuture<RunResult> existing = inFlight.get();
      if (existing != null) {
        return view(existing);
      }
      CompletableFuture<RunResult> owner = new CompletableFuture<>();
      if (!inFlight.compareAndSet(null, owner)) {
        continue;
      }
      execute()
          .whenComplete(
              (value, failure) -> {
                if (failure == null) {
                  owner.complete(value);
                } else {
                  owner.completeExceptionally(unwrap(failure));
                }
                inFlight.compareAndSet(owner, null);
              });
      return view(owner);
    }
  }

  private CompletableFuture<RunResult> execute() {
    CompletableFuture<Capture> captured;
    try {
      captured =
          Objects.requireNonNull(captures.capture(partition), "Kafka compaction capture future");
    } catch (Throwable failure) {
      captured = CompletableFuture.failedFuture(failure);
    }
    return captured.thenCompose(
        capture -> {
          requireCapture(capture);
          return findExisting(capture)
              .thenCompose(
                  existing ->
                      existing.isPresent()
                          ? resume(capture, existing.orElseThrow())
                          : planFresh(capture));
        });
  }

  private CompletableFuture<Optional<ExistingWork>> findExisting(Capture capture) {
    ArrayList<VersionedKafkaCompactionPlan> scanned = new ArrayList<>();
    return scanPlans(Optional.empty(), scanned)
        .thenCompose(ignored -> loadExisting(capture, scanned, 0, Optional.empty()));
  }

  private CompletableFuture<Void> scanPlans(
      Optional<KafkaCompactionPlanScanToken> continuation,
      ArrayList<VersionedKafkaCompactionPlan> scanned) {
    int remaining = configuration.maxPlansPerPass() - scanned.size();
    if (remaining == 0) {
      if (continuation.isPresent()) {
        return limit("Kafka compaction active-work scan exceeded its plan bound");
      }
      return CompletableFuture.completedFuture(null);
    }
    int limit = Math.min(configuration.planScanPageSize(), remaining);
    CompletableFuture<KafkaCompactionPlanScanPage> page;
    try {
      page =
          Objects.requireNonNull(
              plans.scanCompactionPlans(partition, continuation, limit),
              "Kafka compaction plan scan future");
    } catch (Throwable failure) {
      return CompletableFuture.failedFuture(failure);
    }
    return page.thenCompose(
        value -> {
          if (value.plans().size() > limit
              || value.plans().stream()
                  .anyMatch(plan -> !plan.value().identity().equals(partition))) {
            return CompletableFuture.failedFuture(
                invariant("Kafka compaction plan scan violated its partition/page contract"));
          }
          scanned.addAll(value.plans());
          if (value.continuation().isEmpty()) {
            return CompletableFuture.completedFuture(null);
          }
          if (scanned.size() == configuration.maxPlansPerPass()) {
            return limit("Kafka compaction active-work scan has additional plans");
          }
          return scanPlans(value.continuation(), scanned);
        });
  }

  private CompletableFuture<Optional<ExistingWork>> loadExisting(
      Capture capture,
      List<VersionedKafkaCompactionPlan> scanned,
      int index,
      Optional<ExistingWork> found) {
    if (index == scanned.size()) {
      return CompletableFuture.completedFuture(found);
    }
    VersionedKafkaCompactionPlan durablePlan = scanned.get(index);
    KafkaCompactionPlan decoded;
    try {
      decoded = planMapper.fromRecord(durablePlan.value());
    } catch (Throwable failure) {
      return CompletableFuture.failedFuture(
          invariant("Kafka compaction active-work plan cannot be decoded", failure));
    }
    if (!decoded.streamId().equals(capture.streamId())) {
      return taskFor(decoded)
          .thenCompose(
              task -> {
                if (task.isPresent()) {
                  return CompletableFuture.failedFuture(
                      condition("Kafka compaction task belongs to a superseded partition stream"));
                }
                return loadExisting(capture, scanned, index + 1, found);
              });
    }
    return taskFor(decoded)
        .thenCompose(
            optional -> {
              if (optional.isEmpty()) {
                return loadExisting(capture, scanned, index + 1, found);
              }
              VersionedMaterializationTask durableTask = optional.orElseThrow();
              MaterializationTask task = tasks.requireTask(durableTask);
              decoded.requireMaterializationTask(task);
              return planCoordinator
                  .recover(partition, task)
                  .thenCompose(
                      recovered -> {
                        if (!recovered.durablePlan().equals(durablePlan)
                            || !recovered.plan().equals(decoded)) {
                          return CompletableFuture.failedFuture(
                              invariant(
                                  "Kafka compaction active-work plan changed during recovery"));
                        }
                        ExistingWork work =
                            new ExistingWork(decoded, durablePlan, durableTask, task);
                        if (found.isPresent()) {
                          return CompletableFuture.failedFuture(
                              invariant(
                                  "multiple task-rooted Kafka compaction plans exist for one"
                                      + " partition"));
                        }
                        return loadExisting(capture, scanned, index + 1, Optional.of(work));
                      });
            });
  }

  private CompletableFuture<Optional<VersionedMaterializationTask>> taskFor(
      KafkaCompactionPlan plan) {
    try {
      return Objects.requireNonNull(
          tasks.get(plan.streamId(), plan.materializationTaskId()),
          "Kafka compaction task lookup future");
    } catch (Throwable failure) {
      return CompletableFuture.failedFuture(failure);
    }
  }

  private CompletableFuture<RunResult> planFresh(Capture capture) {
    Candidate candidate;
    Snapshot passOne;
    try {
      candidate = planner.select(capture.plannerSnapshot());
      if (!candidate.shouldCompact()) {
        return CompletableFuture.completedFuture(RunResult.noCandidate());
      }
      passOne = capture.passOneInputs().freeze(candidate);
    } catch (Throwable failure) {
      return CompletableFuture.failedFuture(failure);
    }
    return sources
        .resolve(capture.streamId(), candidate, capture.outputPolicy())
        .thenCompose(
            resolved -> {
              MaterializationTaskMutationGuard guard =
                  sources.mutationGuard(resolved, capture.authorityGuard());
              return planCoordinator
                  .converge(
                      partition,
                      resolved.outputTask(),
                      capture.binding().metadataVersion(),
                      capture.plannerSnapshot().lastStableOffset(),
                      capture.plannerSnapshot().highWatermark(),
                      candidate,
                      resolved.decisionSources(),
                      passOne,
                      guard)
                  .thenCompose(
                      converged ->
                          tasks
                              .get(
                                  converged.outputTask().streamId(),
                                  converged.outputTask().taskId())
                              .thenCompose(
                                  optional -> {
                                    VersionedMaterializationTask durableTask =
                                        optional.orElseThrow(
                                            () ->
                                                invariant(
                                                    "Kafka compaction task disappeared after plan"
                                                        + " convergence"));
                                    if (!tasks
                                        .requireTask(durableTask)
                                        .equals(converged.outputTask())) {
                                      return CompletableFuture.failedFuture(
                                          invariant(
                                              "Kafka compaction converged task changed before"
                                                  + " claim"));
                                    }
                                    ExistingWork work =
                                        new ExistingWork(
                                            converged.plan(),
                                            converged.durablePlan(),
                                            durableTask,
                                            converged.outputTask());
                                    return resume(capture, work);
                                  }));
            });
  }

  private CompletableFuture<RunResult> resume(Capture capture, ExistingWork work) {
    return switch (work.durableTask().value().lifecycle()) {
      case PLANNED -> executePlanned(capture, work);
      case RETRY_WAIT ->
          clock.millis() < work.durableTask().value().retryNotBeforeMillis()
              ? CompletableFuture.completedFuture(
                  RunResult.taskOnly(RunOutcome.RETRY_DEFERRED, work.task().taskId()))
              : executePlanned(capture, work);
      case CLAIMED -> recoverClaim(capture, work);
      case OUTPUT_READY, PUBLISHING, PUBLISHED -> recoverPublication(capture, work);
      case CANCELLED, TERMINAL_FAILED ->
          retireTerminal(capture, work, RunOutcome.TERMINAL_RETIRED, Optional.empty());
    };
  }

  private CompletableFuture<RunResult> executePlanned(Capture capture, ExistingWork work) {
    return activation(work.plan(), work.task())
        .thenCompose(
            activation -> {
              String claimId = requireBase32(claimIds.next(), "claimId");
              long expiresAt = addTime(clock.millis(), configuration.claimDuration().toMillis());
              return publications
                  .claim(work.durableTask(), claimId, processRunId, expiresAt)
                  .thenCompose(
                      claimed ->
                          executeClaimed(
                              capture,
                              work.withTask(claimed, tasks.requireTask(claimed)),
                              activation,
                              claimId));
            });
  }

  private CompletableFuture<RunResult> executeClaimed(
      Capture capture, ExistingWork work, Activation activation, String claimId) {
    ClaimLease lease = new ClaimLease(work.durableTask());
    try {
      lease.start();
    } catch (Throwable failure) {
      return stopLeaseAndSettle(capture, work, claimId, lease, failure);
    }
    CompletableFuture<PublicationResult> operation;
    try {
      CompletableFuture<KafkaCompactionParquetPublisher.PreparedObject> preparing =
          taskProtections
              .reconcile(work.durableTask())
              .thenCompose(
                  ignored -> {
                    Input input = writeInput(capture, work.task(), claimId);
                    KafkaCompactionBatchSource.PassStreams streams = batches.open(work.plan());
                    return parquet.prepare(
                        work.plan(),
                        streams,
                        input,
                        capture.writeSettings().allowUncompressedFallback());
                  });
      operation =
          lease
              .guard(preparing)
              .thenCompose(
                  prepared ->
                      lease
                          .stop(true)
                          .<CompletableFuture<PublicationResult>>handle(
                              (latest, stopFailure) -> {
                                if (stopFailure != null) {
                                  prepared.close();
                                  return CompletableFuture.failedFuture(unwrap(stopFailure));
                                }
                                return publications.publish(
                                    partition,
                                    latest,
                                    work.plan(),
                                    prepared,
                                    activation.mode(),
                                    activation.previous(),
                                    capture.authorityGuard());
                              })
                          .thenCompose(value -> value));
    } catch (Throwable failure) {
      return stopLeaseAndSettle(capture, work, claimId, lease, failure);
    }
    return operation
        .thenCompose(publication -> retirePublished(capture, work, publication))
        .<CompletableFuture<RunResult>>handle(
            (result, failure) -> {
              if (failure == null) {
                return CompletableFuture.completedFuture(result);
              }
              return stopLeaseAndSettle(capture, work, claimId, lease, unwrap(failure));
            })
        .thenCompose(value -> value);
  }

  private CompletableFuture<RunResult> stopLeaseAndSettle(
      Capture capture, ExistingWork work, String claimId, ClaimLease lease, Throwable failure) {
    Throwable exact = unwrap(failure);
    return lease
        .stop(false)
        .handle(
            (ignored, stopFailure) -> {
              if (stopFailure != null) {
                Throwable stopped = unwrap(stopFailure);
                if (stopped != exact) {
                  exact.addSuppressed(stopped);
                }
              }
              return null;
            })
        .thenCompose(ignored -> settleClaimFailure(capture, work, claimId, exact));
  }

  private CompletableFuture<RunResult> recoverPublication(Capture capture, ExistingWork work) {
    return activation(work.plan(), work.task())
        .thenCompose(
            activation ->
                publications.recoverPublication(
                    partition,
                    work.durableTask(),
                    work.plan(),
                    activation.mode(),
                    activation.previous(),
                    capture.authorityGuard()))
        .thenCompose(publication -> retirePublished(capture, work, publication));
  }

  private CompletableFuture<RunResult> retirePublished(
      Capture capture, ExistingWork work, PublicationResult publication) {
    return reloadWork(work)
        .thenCompose(
            terminal -> {
              if (terminal.durableTask().value().lifecycle() != TaskLifecycle.PUBLISHED) {
                return CompletableFuture.failedFuture(
                    invariant("Kafka compaction publication did not leave a PUBLISHED task"));
              }
              return retireTerminal(
                  capture, terminal, RunOutcome.PUBLISHED_AND_RETIRED, Optional.of(publication));
            });
  }

  private CompletableFuture<RunResult> retireTerminal(
      Capture capture,
      ExistingWork work,
      RunOutcome outcome,
      Optional<PublicationResult> publication) {
    return retirer
        .retire(partition, work.durableTask(), work.durablePlan(), capture.authorityGuard())
        .thenApply(
            retirement ->
                new RunResult(
                    outcome,
                    Optional.of(work.task().taskId()),
                    publication,
                    Optional.of(retirement)));
  }

  private CompletableFuture<ExistingWork> reloadWork(ExistingWork expected) {
    return tasks
        .get(expected.task().streamId(), expected.task().taskId())
        .thenCompose(
            optional -> {
              VersionedMaterializationTask durableTask =
                  optional.orElseThrow(
                      () -> invariant("Kafka compaction task disappeared before retirement"));
              MaterializationTask task = tasks.requireTask(durableTask);
              expected.plan().requireMaterializationTask(task);
              return plans
                  .getCompactionPlan(partition, task.taskId())
                  .thenApply(
                      currentPlan -> {
                        VersionedKafkaCompactionPlan durablePlan =
                            currentPlan.orElseThrow(
                                () ->
                                    invariant(
                                        "Kafka compaction KCP1 disappeared before task"
                                            + " retirement"));
                        if (!durablePlan.equals(expected.durablePlan())) {
                          throw invariant("Kafka compaction KCP1 changed before task retirement");
                        }
                        return new ExistingWork(expected.plan(), durablePlan, durableTask, task);
                      });
            });
  }

  private CompletableFuture<RunResult> recoverClaim(Capture capture, ExistingWork work) {
    var claim = work.durableTask().value().workerClaim().orElseThrow();
    long safeExpiry =
        saturatingAdd(claim.expiresAtMillis(), configuration.maximumClockSkew().toMillis());
    if (clock.millis() < safeExpiry) {
      return CompletableFuture.completedFuture(
          RunResult.taskOnly(RunOutcome.CLAIM_ACTIVE, work.task().taskId()));
    }
    long retryAt = addTime(clock.millis(), configuration.retryDelay().toMillis());
    return capture
        .authorityGuard()
        .revalidate()
        .thenCompose(
            ignored ->
                tasks.failClaim(
                    work.durableTask(),
                    TaskLifecycle.RETRY_WAIT,
                    TaskFailureClass.CLOSED,
                    EXPIRED_CLAIM_MESSAGE,
                    retryAt))
        .<CompletableFuture<RunResult>>handle(
            (updated, failure) -> {
              if (failure == null) {
                return CompletableFuture.completedFuture(
                    RunResult.taskOnly(RunOutcome.EXPIRED_CLAIM_REQUEUED, work.task().taskId()));
              }
              Throwable original = unwrap(failure);
              return tasks
                  .get(work.task().streamId(), work.task().taskId())
                  .thenCompose(
                      optional -> {
                        VersionedMaterializationTask reloaded =
                            optional.orElseThrow(
                                () ->
                                    condition(
                                        "Kafka compaction task disappeared during claim recovery"));
                        if (sameExpiredRetry(reloaded, work.durableTask(), retryAt)) {
                          return CompletableFuture.completedFuture(
                              RunResult.taskOnly(
                                  RunOutcome.EXPIRED_CLAIM_REQUEUED, work.task().taskId()));
                        }
                        MaterializationTask task = tasks.requireTask(reloaded);
                        work.plan().requireMaterializationTask(task);
                        if (reloaded.value().lifecycle()
                            != work.durableTask().value().lifecycle()) {
                          return resume(capture, work.withTask(reloaded, task));
                        }
                        return CompletableFuture.failedFuture(original);
                      });
            })
        .thenCompose(value -> value);
  }

  private CompletableFuture<RunResult> settleClaimFailure(
      Capture capture, ExistingWork work, String claimId, Throwable failure) {
    Throwable exact = unwrap(failure);
    return tasks
        .get(work.task().streamId(), work.task().taskId())
        .thenCompose(
            optional -> {
              if (optional.isEmpty()) {
                return failed(exact);
              }
              VersionedMaterializationTask current = optional.orElseThrow();
              if (!sameClaim(current, work.task(), claimId, processRunId)) {
                return failed(exact);
              }
              FailureDecision decision = failureDecision(exact, current.value().attempt());
              long retryAt =
                  decision.lifecycle() == TaskLifecycle.RETRY_WAIT
                      ? addTime(clock.millis(), configuration.retryDelay().toMillis())
                      : 0;
              return capture
                  .authorityGuard()
                  .revalidate()
                  .thenCompose(
                      ignored ->
                          tasks.failClaim(
                              current,
                              decision.lifecycle(),
                              decision.failureClass(),
                              failureMessage(exact),
                              retryAt))
                  .<CompletableFuture<RunResult>>handle(
                      (updated, transitionFailure) -> {
                        if (transitionFailure != null) {
                          exact.addSuppressed(unwrap(transitionFailure));
                        }
                        return failed(exact);
                      })
                  .thenCompose(value -> value);
            })
        .exceptionallyCompose(
            transitionFailure -> {
              Throwable transition = unwrap(transitionFailure);
              if (transition != exact) {
                exact.addSuppressed(transition);
              }
              return failed(exact);
            });
  }

  private CompletableFuture<Activation> activation(
      KafkaCompactionPlan plan, MaterializationTask task) {
    Optional<KafkaCompactionPlanner.MandatoryCoverage> previous =
        plan.candidate().previousMandatoryCoverage();
    if (previous.isEmpty()) {
      return CompletableFuture.completedFuture(
          new Activation(KafkaCompactionCoverageActivationMode.INITIAL, Optional.empty()));
    }
    KafkaCompactionPlanner.MandatoryCoverage coverage = previous.orElseThrow();
    KafkaCompactionCoverageRecord durableCoverage =
        new KafkaCompactionCoverageRecord(
            1,
            coverage.startOffset(),
            coverage.endOffset(),
            coverage.activationEpoch(),
            checksumBytes(coverage.generationSetSha256()),
            checksumBytes(coverage.policySha256()),
            1);
    KafkaCompactionCoverageActivationMode mode =
        coverage.policySha256().equals(task.policyDigestSha256())
            ? KafkaCompactionCoverageActivationMode.EXTEND
            : KafkaCompactionCoverageActivationMode.REPLACE;
    return activatedGenerations
        .resolveGenerationSet(task.streamId(), durableCoverage)
        .thenApply(generations -> new Activation(mode, Optional.of(generations)));
  }

  private Input writeInput(Capture capture, MaterializationTask task, String claimId) {
    return new Input(
        cluster,
        task.streamId(),
        task.coverage(),
        claimId,
        task.policyDigestSha256(),
        task.sources().get(task.sources().size() - 1).cumulativeSizeAtEnd(),
        task.policy().targetRowGroupRecords(),
        task.policy().compression(),
        capture.writeSettings().writerBuild());
  }

  private void requireCapture(Capture capture) {
    Capture exact = Objects.requireNonNull(capture, "capture");
    VersionedKafkaPartitionBinding binding = exact.binding();
    KafkaCompactionPlanner.Snapshot snapshot = exact.plannerSnapshot();
    if (!binding.value().identity().equals(partition)
        || binding.value().lifecycle() != KafkaPartitionLifecycle.ACTIVE
        || !binding.value().streamId().equals(exact.streamId().value())
        || snapshot.virtualSegments().logStartOffset() != binding.value().observedLogStartOffset()
        || snapshot.virtualSegments().stableEndOffset()
            < binding.value().observedStableEndOffset()) {
      throw condition("Kafka compaction capture does not match the ACTIVE binding window");
    }
    requireMandatoryCoverage(snapshot, binding.value().compactionCoverage());
  }

  private static void requireMandatoryCoverage(
      KafkaCompactionPlanner.Snapshot snapshot, KafkaCompactionCoverageRecord coverage) {
    if (coverage.coverageVersion() == 0) {
      if (snapshot.mandatoryCoverage().isPresent()) {
        throw condition("Kafka compaction planner snapshot invents absent mandatory coverage");
      }
      return;
    }
    KafkaCompactionPlanner.MandatoryCoverage expected =
        snapshot
            .mandatoryCoverage()
            .orElseThrow(
                () ->
                    condition(
                        "Kafka compaction planner snapshot omits mandatory binding coverage"));
    if (expected.startOffset() != coverage.startOffset()
        || expected.endOffset() != coverage.endOffset()
        || expected.activationEpoch() != coverage.activationEpoch()
        || !expected.generationSetSha256().equals(sha256(coverage.generationSetSha256()))
        || !expected.policySha256().equals(sha256(coverage.policySha256()))) {
      throw condition("Kafka compaction planner snapshot differs from mandatory binding coverage");
    }
  }

  private FailureDecision failureDecision(Throwable failure, long attempt) {
    TaskFailureClass failureClass = classifyFailure(failure);
    TaskLifecycle lifecycle;
    if (failureClass == TaskFailureClass.SOURCE_CHANGED
        || failureClass == TaskFailureClass.SOURCE_RETIRED
        || failureClass == TaskFailureClass.CLOSED) {
      lifecycle = TaskLifecycle.CANCELLED;
    } else if (failureClass == TaskFailureClass.UNSUPPORTED_MAPPING
        || failureClass == TaskFailureClass.OUTPUT_INVARIANT
        || failureClass == TaskFailureClass.CORRUPT_SOURCE
        || attempt >= configuration.maxTaskAttempts()) {
      lifecycle = TaskLifecycle.TERMINAL_FAILED;
    } else {
      lifecycle = TaskLifecycle.RETRY_WAIT;
    }
    return new FailureDecision(lifecycle, failureClass);
  }

  static TaskFailureClass classifyFailure(Throwable failure) {
    Throwable exact = unwrap(failure);
    if (exact instanceof MaterializationFailure materialization) {
      TaskFailureClass typed =
          Objects.requireNonNull(materialization.failureClass(), "materialization failure class");
      return typed == TaskFailureClass.NONE ? TaskFailureClass.OUTPUT_INVARIANT : typed;
    }
    if (exact instanceof NereusException nereus) {
      return switch (nereus.code()) {
        case OBJECT_UPLOAD_FAILED, OBJECT_READ_FAILED, OBJECT_NOT_FOUND, TIMEOUT ->
            TaskFailureClass.RETRYABLE_OBJECT_STORE;
        case METADATA_UNAVAILABLE, METADATA_CONDITION_FAILED -> TaskFailureClass.RETRYABLE_METADATA;
        case BACKPRESSURE_REJECTED, METADATA_LIMIT_EXCEEDED, READ_LIMIT_TOO_SMALL ->
            TaskFailureClass.RETRYABLE_RESOURCE_LIMIT;
        case OBJECT_CHECKSUM_MISMATCH, PRIMARY_WAL_CHECKSUM_MISMATCH ->
            TaskFailureClass.CORRUPT_SOURCE;
        case UNSUPPORTED_FORMAT, UNSUPPORTED_READ_TARGET, UNSUPPORTED_STORAGE_PROFILE ->
            TaskFailureClass.UNSUPPORTED_MAPPING;
        case CANCELLED, STORAGE_CLOSED -> TaskFailureClass.CLOSED;
        default -> TaskFailureClass.OUTPUT_INVARIANT;
      };
    }
    return TaskFailureClass.OUTPUT_INVARIANT;
  }

  private boolean sameClaim(
      VersionedMaterializationTask current,
      MaterializationTask task,
      String claimId,
      String processRunId) {
    return current.value().lifecycle() == TaskLifecycle.CLAIMED
        && current.value().workerClaim().isPresent()
        && tasks.requireTask(current).equals(task)
        && current.value().workerClaim().orElseThrow().claimId().equals(claimId)
        && current.value().workerClaim().orElseThrow().processRunId().equals(processRunId);
  }

  private static boolean sameExpiredRetry(
      VersionedMaterializationTask actual, VersionedMaterializationTask claimed, long retryAt) {
    return actual.value().lifecycle() == TaskLifecycle.RETRY_WAIT
        && actual.value().taskId().equals(claimed.value().taskId())
        && actual.value().attempt() == claimed.value().attempt()
        && actual.value().failureClassId() == TaskFailureClass.CLOSED.wireId()
        && actual.value().retryNotBeforeMillis() == retryAt;
  }

  private static CompletableFuture<RunResult> view(CompletableFuture<RunResult> operation) {
    return operation.thenApply(value -> value);
  }

  private static <T> CompletableFuture<T> failed(Throwable failure) {
    return CompletableFuture.failedFuture(failure);
  }

  private static CompletableFuture<Void> limit(String message) {
    return CompletableFuture.failedFuture(
        new NereusException(ErrorCode.METADATA_LIMIT_EXCEEDED, true, message));
  }

  private static NereusException invariant(String message) {
    return invariant(message, null);
  }

  private static NereusException invariant(String message, Throwable cause) {
    return new NereusException(ErrorCode.METADATA_INVARIANT_VIOLATION, false, message, cause);
  }

  private static NereusException condition(String message) {
    return new NereusException(ErrorCode.METADATA_CONDITION_FAILED, true, message);
  }

  private static Throwable unwrap(Throwable failure) {
    Throwable current = Objects.requireNonNull(failure, "failure");
    while ((current instanceof CompletionException
            || current instanceof java.util.concurrent.ExecutionException)
        && current.getCause() != null) {
      current = current.getCause();
    }
    return current;
  }

  private static long addTime(long value, long delta) {
    if (value < 0 || delta <= 0 || value > Long.MAX_VALUE - delta) {
      throw new IllegalArgumentException("Kafka compaction time window overflows");
    }
    return value + delta;
  }

  private static long saturatingAdd(long value, long delta) {
    if (value < 0 || delta < 0) {
      throw new IllegalArgumentException("Kafka compaction time values must be non-negative");
    }
    return value > Long.MAX_VALUE - delta ? Long.MAX_VALUE : value + delta;
  }

  private static String failureMessage(Throwable failure) {
    String message = failure.getMessage();
    if (message == null || message.isBlank()) {
      message = failure.getClass().getSimpleName();
    }
    return message.length() <= 1_024 ? message : message.substring(0, 1_024);
  }

  private static Checksum sha256(byte[] value) {
    return new Checksum(com.nereusstream.api.ChecksumType.SHA256, HexFormat.of().formatHex(value));
  }

  private static byte[] checksumBytes(Checksum checksum) {
    if (checksum.type() != com.nereusstream.api.ChecksumType.SHA256) {
      throw new IllegalArgumentException("Kafka compaction digest must use SHA256");
    }
    return HexFormat.of().parseHex(checksum.value());
  }

  private static String requireText(String value, String field) {
    Objects.requireNonNull(value, field);
    if (value.isBlank()) {
      throw new IllegalArgumentException(field + " cannot be blank");
    }
    return value;
  }

  private static String requireBase32(String value, String field) {
    String exact = requireText(value, field);
    if (exact.length() < 26 || exact.length() > 128) {
      throw new IllegalArgumentException(field + " must encode at least 128 bits");
    }
    for (int index = 0; index < exact.length(); index++) {
      char character = exact.charAt(index);
      if (!((character >= 'a' && character <= 'z') || (character >= '2' && character <= '7'))) {
        throw new IllegalArgumentException(field + " must be lowercase base32 without padding");
      }
    }
    return exact;
  }

  @FunctionalInterface
  public interface CaptureProvider {
    CompletableFuture<Capture> capture(KafkaPartitionId partition);
  }

  /** One partition-lock/KRaft-authority capture consumed by an entire pass. */
  public record Capture(
      VersionedKafkaPartitionBinding binding,
      KafkaCompactionPlanner.Snapshot plannerSnapshot,
      MaterializationPolicy outputPolicy,
      PassOneInputs passOneInputs,
      WriteSettings writeSettings,
      MaterializationTaskMutationGuard authorityGuard) {
    public Capture {
      Objects.requireNonNull(binding, "binding");
      Objects.requireNonNull(plannerSnapshot, "plannerSnapshot");
      Objects.requireNonNull(outputPolicy, "outputPolicy");
      Objects.requireNonNull(passOneInputs, "passOneInputs");
      Objects.requireNonNull(writeSettings, "writeSettings");
      Objects.requireNonNull(authorityGuard, "authorityGuard");
    }

    public StreamId streamId() {
      return new StreamId(binding.value().streamId());
    }
  }

  /**
   * Frozen transaction/marker/resource facts used to derive the candidate-specific KCP1 snapshot.
   */
  public record PassOneInputs(
      long transactionStateEndOffset,
      long maxDecodedRecords,
      int maxKeyBytes,
      long maxInMemoryKeyBytes,
      List<AbortedTransactionRange> abortedTransactions,
      List<OpenTransactionRange> openTransactions,
      List<MarkerDecision> markerDecisions) {
    public PassOneInputs {
      abortedTransactions =
          List.copyOf(Objects.requireNonNull(abortedTransactions, "abortedTransactions"));
      openTransactions = List.copyOf(Objects.requireNonNull(openTransactions, "openTransactions"));
      markerDecisions = List.copyOf(Objects.requireNonNull(markerDecisions, "markerDecisions"));
      if (transactionStateEndOffset < 0
          || maxDecodedRecords <= 0
          || maxKeyBytes <= 0
          || maxInMemoryKeyBytes <= 0) {
        throw new IllegalArgumentException("invalid Kafka compaction pass-one capture");
      }
    }

    Snapshot freeze(Candidate candidate) {
      return new Snapshot(
          candidate.outputCoverage(),
          candidate.decisionHorizon(),
          transactionStateEndOffset,
          candidate.evaluatedAtMillis(),
          candidate.policy().deleteRetentionMs(),
          maxDecodedRecords,
          maxKeyBytes,
          maxInMemoryKeyBytes,
          abortedTransactions,
          openTransactions,
          markerDecisions);
    }
  }

  public record WriteSettings(String writerBuild, boolean allowUncompressedFallback) {
    public WriteSettings {
      writerBuild = requireText(writerBuild, "writerBuild");
    }
  }

  public record Configuration(
      Duration claimDuration,
      Duration claimRenewInterval,
      Duration maximumClockSkew,
      Duration retryDelay,
      int maxTaskAttempts,
      int planScanPageSize,
      int maxPlansPerPass) {
    public Configuration {
      claimDuration = requirePositive(claimDuration, "claimDuration");
      claimRenewInterval = requirePositive(claimRenewInterval, "claimRenewInterval");
      maximumClockSkew = requireNonNegative(maximumClockSkew, "maximumClockSkew");
      retryDelay = requirePositive(retryDelay, "retryDelay");
      if (claimRenewInterval.compareTo(claimDuration) >= 0) {
        throw new IllegalArgumentException("claimRenewInterval must be shorter than claimDuration");
      }
      if (maxTaskAttempts <= 0) {
        throw new IllegalArgumentException("maxTaskAttempts must be positive");
      }
      if (planScanPageSize <= 0 || planScanPageSize > 1_000) {
        throw new IllegalArgumentException("planScanPageSize must be in [1, 1000]");
      }
      if (maxPlansPerPass < planScanPageSize) {
        throw new IllegalArgumentException("maxPlansPerPass must be at least planScanPageSize");
      }
    }
  }

  public enum RunOutcome {
    NO_CANDIDATE,
    CLAIM_ACTIVE,
    RETRY_DEFERRED,
    EXPIRED_CLAIM_REQUEUED,
    PUBLISHED_AND_RETIRED,
    TERMINAL_RETIRED
  }

  /** Durable terminal accounting for one joined pass invocation. */
  public record RunResult(
      RunOutcome outcome,
      Optional<String> materializationTaskId,
      Optional<PublicationResult> publication,
      Optional<RetirementResult> retirement) {
    public RunResult {
      Objects.requireNonNull(outcome, "outcome");
      materializationTaskId =
          Objects.requireNonNull(materializationTaskId, "materializationTaskId");
      publication = Objects.requireNonNull(publication, "publication");
      retirement = Objects.requireNonNull(retirement, "retirement");
      if ((outcome == RunOutcome.NO_CANDIDATE) != materializationTaskId.isEmpty()
          || (outcome == RunOutcome.PUBLISHED_AND_RETIRED) != publication.isPresent()
          || ((outcome == RunOutcome.PUBLISHED_AND_RETIRED
                  || outcome == RunOutcome.TERMINAL_RETIRED)
              != retirement.isPresent())) {
        throw new IllegalArgumentException("invalid Kafka compaction partition-pass result");
      }
    }

    static RunResult noCandidate() {
      return new RunResult(
          RunOutcome.NO_CANDIDATE, Optional.empty(), Optional.empty(), Optional.empty());
    }

    static RunResult taskOnly(RunOutcome outcome, String taskId) {
      return new RunResult(
          outcome, Optional.of(requireText(taskId, "taskId")), Optional.empty(), Optional.empty());
    }
  }

  private record ExistingWork(
      KafkaCompactionPlan plan,
      VersionedKafkaCompactionPlan durablePlan,
      VersionedMaterializationTask durableTask,
      MaterializationTask task) {
    private ExistingWork {
      Objects.requireNonNull(plan, "plan");
      Objects.requireNonNull(durablePlan, "durablePlan");
      Objects.requireNonNull(durableTask, "durableTask");
      Objects.requireNonNull(task, "task");
      plan.requireMaterializationTask(task);
    }

    ExistingWork withTask(VersionedMaterializationTask replacement, MaterializationTask domain) {
      Objects.requireNonNull(replacement, "replacement");
      Objects.requireNonNull(domain, "domain");
      if (!domain.equals(task)) {
        throw invariant("Kafka compaction task domain changed across lifecycle transition");
      }
      return new ExistingWork(plan, durablePlan, replacement, task);
    }
  }

  private record Activation(
      KafkaCompactionCoverageActivationMode mode, Optional<KafkaCompactionGenerationSet> previous) {
    private Activation {
      Objects.requireNonNull(mode, "mode");
      previous = Objects.requireNonNull(previous, "previous");
      if ((mode == KafkaCompactionCoverageActivationMode.INITIAL) == previous.isPresent()) {
        throw new IllegalArgumentException("invalid Kafka compaction activation context");
      }
    }
  }

  private record FailureDecision(TaskLifecycle lifecycle, TaskFailureClass failureClass) {}

  private final class ClaimLease {
    private final Object lock = new Object();
    private final CompletableFuture<Void> failure = new CompletableFuture<>();
    private VersionedMaterializationTask latest;
    private ScheduledFuture<?> scheduled;
    private CompletableFuture<Void> active = CompletableFuture.completedFuture(null);
    private Throwable terminalFailure;
    private boolean stopped;

    private ClaimLease(VersionedMaterializationTask claimed) {
      this.latest = Objects.requireNonNull(claimed, "claimed");
    }

    private void start() {
      synchronized (lock) {
        scheduleLocked();
      }
    }

    private <T> CompletableFuture<T> guard(CompletableFuture<T> operation) {
      CompletableFuture<T> exact = Objects.requireNonNull(operation, "operation");
      CompletableFuture<T> guarded = new CompletableFuture<>();
      exact.whenComplete(
          (value, operationFailure) -> {
            if (operationFailure == null) {
              guarded.complete(value);
            } else {
              guarded.completeExceptionally(unwrap(operationFailure));
            }
          });
      failure.whenComplete(
          (ignored, heartbeatFailure) -> {
            if (heartbeatFailure != null
                && guarded.completeExceptionally(unwrap(heartbeatFailure))) {
              exact.cancel(true);
            }
          });
      guarded.whenComplete(
          (ignored, guardedFailure) -> {
            if (guarded.isCancelled()) {
              exact.cancel(true);
            }
          });
      return guarded;
    }

    private CompletableFuture<VersionedMaterializationTask> stop(boolean finalRenewal) {
      CompletableFuture<Void> prior;
      synchronized (lock) {
        if (!stopped) {
          stopped = true;
          if (scheduled != null) {
            scheduled.cancel(false);
            scheduled = null;
          }
        }
        prior = active;
      }
      CompletableFuture<Void> stoppedTail =
          prior.thenCompose(
              ignored -> {
                boolean renew;
                synchronized (lock) {
                  renew = finalRenewal && terminalFailure == null;
                }
                return renew ? renewOnce() : CompletableFuture.completedFuture(null);
              });
      return stoppedTail.thenCompose(
          ignored -> {
            synchronized (lock) {
              return terminalFailure == null
                  ? CompletableFuture.completedFuture(latest)
                  : CompletableFuture.failedFuture(terminalFailure);
            }
          });
    }

    private void scheduleLocked() {
      if (stopped || failure.isDone()) {
        return;
      }
      try {
        scheduled =
            scheduler.schedule(
                this::renewScheduled,
                configuration.claimRenewInterval().toMillis(),
                TimeUnit.MILLISECONDS);
      } catch (RejectedExecutionException rejected) {
        NereusException closed =
            new NereusException(
                ErrorCode.STORAGE_CLOSED,
                false,
                "Kafka compaction claim heartbeat was rejected",
                rejected);
        terminalFailure = closed;
        failure.completeExceptionally(closed);
        throw closed;
      }
    }

    private void renewScheduled() {
      CompletableFuture<Void> renewal;
      synchronized (lock) {
        if (stopped || failure.isDone()) {
          return;
        }
        scheduled = null;
        active = active.thenCompose(ignored -> renewOnce());
        renewal = active;
      }
      renewal.whenComplete(
          (ignored, renewFailure) -> {
            if (renewFailure != null) {
              Throwable exact = unwrap(renewFailure);
              synchronized (lock) {
                if (terminalFailure == null) {
                  terminalFailure = exact;
                }
              }
              failure.completeExceptionally(exact);
              return;
            }
            synchronized (lock) {
              scheduleLocked();
            }
          });
    }

    private CompletableFuture<Void> renewOnce() {
      VersionedMaterializationTask expected;
      long requestedExpiry;
      synchronized (lock) {
        expected = latest;
        long priorExpiry = expected.value().workerClaim().orElseThrow().expiresAtMillis();
        requestedExpiry =
            Math.max(
                addTime(priorExpiry, 1),
                addTime(clock.millis(), configuration.claimDuration().toMillis()));
      }
      return tasks
          .heartbeat(expected, requestedExpiry)
          .<CompletableFuture<VersionedMaterializationTask>>handle(
              (updated, heartbeatFailure) -> {
                if (heartbeatFailure == null) {
                  return CompletableFuture.completedFuture(updated);
                }
                Throwable original = unwrap(heartbeatFailure);
                MaterializationTask domain = tasks.requireTask(expected);
                return tasks
                    .get(domain.streamId(), domain.taskId())
                    .thenCompose(
                        optional -> {
                          VersionedMaterializationTask reloaded =
                              optional.orElseThrow(
                                  () ->
                                      condition(
                                          "Kafka compaction task disappeared during heartbeat"));
                          if (sameClaimOwner(reloaded, expected)
                              && reloaded.value().workerClaim().orElseThrow().expiresAtMillis()
                                  >= requestedExpiry) {
                            return CompletableFuture.completedFuture(reloaded);
                          }
                          return CompletableFuture.failedFuture(original);
                        });
              })
          .thenCompose(value -> value)
          .thenAccept(
              updated -> {
                synchronized (lock) {
                  latest = updated;
                }
              });
    }

    private boolean sameClaimOwner(
        VersionedMaterializationTask current, VersionedMaterializationTask expected) {
      if (current.value().lifecycle() != TaskLifecycle.CLAIMED
          || current.value().workerClaim().isEmpty()
          || expected.value().workerClaim().isEmpty()
          || !tasks.requireTask(current).equals(tasks.requireTask(expected))) {
        return false;
      }
      var actualClaim = current.value().workerClaim().orElseThrow();
      var expectedClaim = expected.value().workerClaim().orElseThrow();
      return actualClaim.claimId().equals(expectedClaim.claimId())
          && actualClaim.processRunId().equals(expectedClaim.processRunId())
          && actualClaim.attempt() == expectedClaim.attempt()
          && actualClaim.claimedAtMillis() == expectedClaim.claimedAtMillis();
    }
  }

  private static Duration requirePositive(Duration value, String field) {
    Objects.requireNonNull(value, field);
    if (value.isZero() || value.isNegative()) {
      throw new IllegalArgumentException(field + " must be positive");
    }
    return value;
  }

  private static Duration requireNonNegative(Duration value, String field) {
    Objects.requireNonNull(value, field);
    if (value.isNegative()) {
      throw new IllegalArgumentException(field + " must be non-negative");
    }
    return value;
  }
}
