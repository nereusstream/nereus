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

import com.nereusstream.api.PayloadFormat;
import com.nereusstream.api.ReadView;
import com.nereusstream.api.StreamId;
import com.nereusstream.materialization.CommittedSourceSetResolution;
import com.nereusstream.materialization.CommittedSourceSetResolver;
import com.nereusstream.materialization.ExactSourceSet;
import com.nereusstream.materialization.MaterializationPolicy;
import com.nereusstream.materialization.MaterializationTask;
import com.nereusstream.materialization.MaterializationTaskMutationGuard;
import com.nereusstream.materialization.SourceGeneration;
import com.nereusstream.materialization.TaskKind;
import com.nereusstream.materialization.TopicCompactionSpec;
import java.util.ArrayList;
import java.util.List;
import java.util.Objects;
import java.util.concurrent.CompletableFuture;

/**
 * Resolves a compaction candidate to one exact decision set and its output materialization task.
 */
public final class KafkaCompactionSourceResolver {
  private final CommittedSourceSetResolver committedSources;

  public KafkaCompactionSourceResolver(CommittedSourceSetResolver committedSources) {
    this.committedSources = Objects.requireNonNull(committedSources, "committedSources");
  }

  public CompletableFuture<ResolvedSources> resolve(
      StreamId streamId,
      KafkaCompactionPlanner.Candidate candidate,
      MaterializationPolicy outputPolicy) {
    try {
      StreamId exactStream = Objects.requireNonNull(streamId, "streamId");
      KafkaCompactionPlanner.Candidate exactCandidate =
          Objects.requireNonNull(candidate, "candidate");
      MaterializationPolicy exactPolicy = Objects.requireNonNull(outputPolicy, "outputPolicy");
      requireKafkaPolicy(exactPolicy);
      if (!exactCandidate.shouldCompact()) {
        throw new IllegalArgumentException(
            "Kafka compaction source resolution requires a non-empty candidate");
      }
      return committedSources
          .resolve(exactStream, exactCandidate.decisionHorizon())
          .thenApply(
              resolution -> {
                ExactSourceSet decisionSources = resolution.sourceSet();
                requireKafkaDecisionSources(exactCandidate, decisionSources);
                ExactSourceSet outputSources = outputPrefix(exactCandidate, decisionSources);
                MaterializationTask outputTask =
                    MaterializationTask.create(
                        exactStream,
                        exactCandidate.outputCoverage(),
                        outputSources.sources(),
                        exactPolicy);
                return new ResolvedSources(exactCandidate, resolution, outputSources, outputTask);
              });
    } catch (RuntimeException failure) {
      return CompletableFuture.failedFuture(failure);
    }
  }

  /**
   * Composes source/head revalidation before the caller's binding/leader authority check.
   *
   * <p>{@link com.nereusstream.materialization.MaterializationTaskStore} invokes this guard after
   * exact per-generation checks and immediately before its durable task mutation.
   */
  public MaterializationTaskMutationGuard mutationGuard(
      ResolvedSources expected, MaterializationTaskMutationGuard bindingAuthority) {
    ResolvedSources exact = Objects.requireNonNull(expected, "expected");
    MaterializationTaskMutationGuard authority =
        Objects.requireNonNull(bindingAuthority, "bindingAuthority");
    return () -> revalidateSources(exact).thenCompose(ignored -> revalidateAuthority(authority));
  }

  private CompletableFuture<Void> revalidateSources(ResolvedSources expected) {
    try {
      return Objects.requireNonNull(
          committedSources.revalidate(expected.resolution()),
          "COMMITTED source revalidation future");
    } catch (RuntimeException failure) {
      return CompletableFuture.failedFuture(failure);
    }
  }

  private static CompletableFuture<Void> revalidateAuthority(
      MaterializationTaskMutationGuard authority) {
    try {
      return Objects.requireNonNull(
          authority.revalidate(), "Kafka compaction binding authority future");
    } catch (RuntimeException failure) {
      return CompletableFuture.failedFuture(failure);
    }
  }

  private static void requireKafkaPolicy(MaterializationPolicy policy) {
    TopicCompactionSpec expected =
        new TopicCompactionSpec(
            KafkaCompactionStrategyV1.STRATEGY_ID,
            KafkaCompactionStrategyV1.STRATEGY_VERSION,
            KafkaCompactionPlan.Compatibility.current().keyCodecId());
    if (policy.view() != ReadView.TOPIC_COMPACTED
        || policy.taskKind() != TaskKind.TOPIC_KEY_COMPACTION
        || policy.taskKind().sourceView() != ReadView.COMMITTED
        || !policy
            .targetPhysicalFormat()
            .equals(MaterializationPolicy.KAFKA_TOPIC_COMPACTED_FORMAT)
        || policy.topicCompaction().filter(expected::equals).isEmpty()) {
      throw new IllegalArgumentException(
          "materialization policy is not the current Kafka compaction policy");
    }
  }

  private static void requireKafkaDecisionSources(
      KafkaCompactionPlanner.Candidate candidate, ExactSourceSet sources) {
    if (sources.view() != ReadView.COMMITTED
        || !sources.coverage().equals(candidate.decisionHorizon())
        || sources.sources().stream()
            .anyMatch(source -> source.payloadFormat() != PayloadFormat.KAFKA_RECORD_BATCH)) {
      throw new IllegalArgumentException(
          "authoritative decision sources do not match the Kafka compaction candidate");
    }
  }

  private static ExactSourceSet outputPrefix(
      KafkaCompactionPlanner.Candidate candidate, ExactSourceSet decisionSources) {
    List<SourceGeneration> prefix = new ArrayList<>();
    for (SourceGeneration source : decisionSources.sources()) {
      if (source.range().endOffset() <= candidate.outputCoverage().endOffset()) {
        prefix.add(source);
        continue;
      }
      break;
    }
    if (prefix.isEmpty()
        || prefix.get(prefix.size() - 1).range().endOffset()
            != candidate.outputCoverage().endOffset()) {
      throw new IllegalArgumentException(
          "Kafka compaction output boundary does not match an exact source prefix");
    }
    return ExactSourceSet.create(ReadView.COMMITTED, candidate.outputCoverage(), prefix);
  }

  public record ResolvedSources(
      KafkaCompactionPlanner.Candidate candidate,
      CommittedSourceSetResolution resolution,
      ExactSourceSet outputSources,
      MaterializationTask outputTask) {
    public ResolvedSources {
      Objects.requireNonNull(candidate, "candidate");
      Objects.requireNonNull(resolution, "resolution");
      Objects.requireNonNull(outputSources, "outputSources");
      Objects.requireNonNull(outputTask, "outputTask");
      if (!candidate.shouldCompact()
          || !resolution.sourceSet().coverage().equals(candidate.decisionHorizon())
          || !outputSources.coverage().equals(candidate.outputCoverage())
          || !outputTask.coverage().equals(candidate.outputCoverage())
          || !outputTask.streamId().equals(resolution.streamId())
          || !outputTask.sources().equals(outputSources.sources())
          || !outputTask.sourceSetSha256().equals(outputSources.sourceSetSha256())) {
        throw new IllegalArgumentException(
            "resolved Kafka compaction sources/task are inconsistent");
      }
    }

    public ExactSourceSet decisionSources() {
      return resolution.sourceSet();
    }
  }
}
