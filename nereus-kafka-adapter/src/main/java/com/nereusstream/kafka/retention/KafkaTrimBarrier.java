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

package com.nereusstream.kafka.retention;

import com.nereusstream.api.AcquiredAppendSession;
import com.nereusstream.api.AppendAuthority;
import com.nereusstream.api.AppendSession;
import com.nereusstream.api.ErrorCode;
import com.nereusstream.api.NereusException;
import com.nereusstream.api.StableStreamHeadSnapshot;
import com.nereusstream.api.StreamId;
import com.nereusstream.api.StreamState;
import com.nereusstream.api.StreamStorage;
import com.nereusstream.api.TrimOptions;
import com.nereusstream.kafka.checkpoint.KafkaVirtualSegmentState.LogConfigHistoryEntry;
import com.nereusstream.kafka.partition.KafkaPartitionIdentity;
import com.nereusstream.metadata.oxia.VersionedKafkaPartitionBinding;
import com.nereusstream.metadata.oxia.records.KafkaCheckpointReferenceRecord;
import com.nereusstream.metadata.oxia.records.KafkaPartitionBindingRecord;
import com.nereusstream.metadata.oxia.records.KafkaPartitionLifecycle;
import java.time.Duration;
import java.util.Arrays;
import java.util.Objects;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.CompletionException;

/**
 * Enforces verified-checkpoint-before-trim, frozen-input revalidation, and response-loss recovery.
 */
public final class KafkaTrimBarrier {
  private final KafkaRetentionPlanner planner;
  private final SnapshotLoader snapshots;
  private final CheckpointGate checkpoints;
  private final StreamStorage streams;
  private final Duration trimTimeout;
  private final DurableTrimListener durableTrimListener;

  public KafkaTrimBarrier(
      KafkaRetentionPlanner planner,
      SnapshotLoader snapshots,
      CheckpointGate checkpoints,
      StreamStorage streams,
      Duration trimTimeout,
      DurableTrimListener durableTrimListener) {
    this.planner = Objects.requireNonNull(planner, "planner");
    this.snapshots = Objects.requireNonNull(snapshots, "snapshots");
    this.checkpoints = Objects.requireNonNull(checkpoints, "checkpoints");
    this.streams = Objects.requireNonNull(streams, "streams");
    this.trimTimeout = Objects.requireNonNull(trimTimeout, "trimTimeout");
    this.durableTrimListener = Objects.requireNonNull(durableTrimListener, "durableTrimListener");
    if (trimTimeout.isZero() || trimTimeout.isNegative() || trimTimeout.toMillis() <= 0) {
      throw new IllegalArgumentException("Kafka trim timeout must include a positive millisecond");
    }
  }

  public CompletableFuture<Result> advance(Snapshot captured, KafkaRetentionPlanner.Plan plan) {
    Objects.requireNonNull(captured, "captured");
    Objects.requireNonNull(plan, "plan");
    KafkaRetentionPlanner.Plan exactPlan = planner.plan(captured.retention());
    if (!plan.equals(exactPlan) || !plan.shouldTrim()) {
      return CompletableFuture.failedFuture(
          invariant("Kafka trim requires the exact advancing plan for its captured snapshot"));
    }
    return advance(
        captured,
        new TrimIntent(
            TrimKind.RETENTION,
            plan.previousLogStartOffset(),
            plan.candidateLogStartOffset(),
            plan.trimReason()));
  }

  /**
   * Advances one already stock-validated DeleteRecords target. The target is the normalized logical
   * offset, so Kafka's {@code -1} high-watermark sentinel must be converted by the caller.
   */
  public CompletableFuture<Result> advanceDeleteRecords(Snapshot captured, long targetOffset) {
    Objects.requireNonNull(captured, "captured");
    long previous = captured.sourceHead().trimOffset();
    if (targetOffset <= previous
        || targetOffset > captured.retention().highWatermark()
        || targetOffset > captured.sourceHead().committedEndOffset()) {
      return CompletableFuture.failedFuture(
          new IllegalArgumentException(
              "Kafka DeleteRecords trim target must advance within the frozen high watermark"));
    }
    if (!deleteEnabled(captured.retention().policy())) {
      return CompletableFuture.failedFuture(
          new NereusException(
              ErrorCode.INVALID_ARGUMENT,
              false,
              "Kafka DeleteRecords requires cleanup.policy containing delete"));
    }
    KafkaPartitionBindingRecord root = captured.binding().value();
    String reason =
        "KAFKA_DELETE_RECORDS_V1:config="
            + captured.retention().policy().metadataOffset()
            + "/"
            + captured.retention().policy().configDigest().value()
            + ":from="
            + previous
            + ":to="
            + targetOffset
            + ":leader="
            + root.observedLeaderId()
            + "/"
            + root.observedLeaderEpoch()
            + ":brokerEpoch="
            + root.observedBrokerEpoch();
    return advance(
        captured, new TrimIntent(TrimKind.DELETE_RECORDS, previous, targetOffset, reason));
  }

  private CompletableFuture<Result> advance(Snapshot captured, TrimIntent intent) {
    long target = intent.targetOffset();
    return checkpoints
        .ensureVerified(captured, target)
        .thenApply(checkpoint -> requireSufficientCheckpoint(captured, target, checkpoint))
        .thenCompose(
            checkpoint ->
                snapshots
                    .loadCurrent()
                    .thenApply(
                        current -> {
                          revalidate(captured, current, checkpoint, intent);
                          return current;
                        })
                    .thenCompose(current -> trimAndConfirm(current, checkpoint, intent)));
  }

  private CompletableFuture<Result> trimAndConfirm(
      Snapshot current, VerifiedCheckpoint checkpoint, TrimIntent intent) {
    long target = intent.targetOffset();
    StreamId streamId = current.sourceHead().streamId();
    if (current.sourceHead().trimOffset() >= target) {
      return notifyDurable(current, checkpoint, intent, current.sourceHead(), true);
    }

    CompletableFuture<Void> trim;
    try {
      trim =
          Objects.requireNonNull(
              streams.trim(streamId, target, new TrimOptions(trimTimeout, intent.trimReason())),
              "StreamStorage returned a null Kafka trim future");
    } catch (Throwable failure) {
      trim = CompletableFuture.failedFuture(failure);
    }
    return trim.handle((ignored, failure) -> failure == null ? null : unwrap(failure))
        .thenCompose(
            trimFailure ->
                confirmDurableHead(current, target, trimFailure)
                    .thenCompose(
                        durable -> notifyDurable(current, checkpoint, intent, durable, false)));
  }

  private CompletableFuture<StableStreamHeadSnapshot> confirmDurableHead(
      Snapshot current, long target, Throwable trimFailure) {
    CompletableFuture<StableStreamHeadSnapshot> loaded;
    try {
      loaded =
          Objects.requireNonNull(
              streams.getStableHeadSnapshot(current.sourceHead().streamId()),
              "StreamStorage returned a null stable-head future");
    } catch (Throwable failure) {
      loaded = CompletableFuture.failedFuture(failure);
    }
    return loaded.handle(
        (head, loadFailure) -> {
          if (loadFailure != null) {
            Throwable exactLoadFailure = unwrap(loadFailure);
            if (trimFailure != null) {
              trimFailure.addSuppressed(exactLoadFailure);
              throw new CompletionException(trimFailure);
            }
            throw new CompletionException(exactLoadFailure);
          }
          requireSameAuthority(current.sourceHead(), head);
          if (head.trimOffset() < target) {
            if (trimFailure != null) {
              throw new CompletionException(trimFailure);
            }
            throw new CompletionException(
                invariant("Kafka trim completed without durably advancing stream trim"));
          }
          return head;
        });
  }

  private CompletableFuture<Result> notifyDurable(
      Snapshot current,
      VerifiedCheckpoint checkpoint,
      TrimIntent intent,
      StableStreamHeadSnapshot durableHead,
      boolean alreadyApplied) {
    try {
      CompletableFuture<Void> notified =
          Objects.requireNonNull(
              durableTrimListener.onDurableTrim(
                  current, durableHead.trimOffset(), checkpoint.reference()),
              "Kafka durable-trim listener returned a null future");
      return notified.thenApply(
          ignored ->
              new Result(
                  intent.previousLogStartOffset(),
                  intent.targetOffset(),
                  durableHead.trimOffset(),
                  checkpoint.reference(),
                  alreadyApplied));
    } catch (Throwable failure) {
      return CompletableFuture.failedFuture(failure);
    }
  }

  private void revalidate(
      Snapshot captured, Snapshot current, VerifiedCheckpoint checkpoint, TrimIntent intent) {
    long target = intent.targetOffset();
    KafkaPartitionBindingRecord before = captured.binding().value();
    KafkaPartitionBindingRecord after = current.binding().value();
    if (!captured.identity().equals(current.identity())
        || !before.identity().equals(after.identity())
        || before.incarnation() != after.incarnation()
        || !before.streamId().equals(after.streamId())
        || before.payloadMappingId() != after.payloadMappingId()
        || !before.storageProfile().equals(after.storageProfile())
        || before.observedLeaderId() != after.observedLeaderId()
        || before.observedLeaderEpoch() != after.observedLeaderEpoch()
        || before.observedBrokerEpoch() != after.observedBrokerEpoch()
        || after.lifecycle() != KafkaPartitionLifecycle.ACTIVE) {
      throw invariant("Kafka retention binding or leader authority changed before trim");
    }
    requireSameAuthority(captured.sourceHead(), current.sourceHead());
    boolean rooted =
        current.binding().value().checkpointReferences().contains(checkpoint.reference());
    if (intent.kind() == TrimKind.RETENTION) {
      if (!captured.retention().policy().equals(current.retention().policy()) || !rooted) {
        throw invariant("Kafka retention config or verified checkpoint root changed before trim");
      }
    } else {
      if (!rooted) {
        throw invariant("verified Kafka checkpoint root changed before trim");
      }
      if (!captured.retention().policy().equals(current.retention().policy())) {
        throw invariant("Kafka DeleteRecords config changed before trim");
      }
      if (!deleteEnabled(current.retention().policy())
          || (current.sourceHead().trimOffset() < target
              && (current.retention().highWatermark() < target
                  || current.sourceHead().committedEndOffset() < target))) {
        throw invariant("Kafka DeleteRecords target is no longer eligible after revalidation");
      }
      return;
    }
    KafkaRetentionPlanner.Plan currentPlan = planner.plan(current.retention());
    if (current.sourceHead().trimOffset() < target
        && (!currentPlan.shouldTrim()
            || currentPlan.candidateLogStartOffset() < target
            || current.retention().highWatermark() < target)) {
      throw invariant("Kafka retention candidate is no longer eligible after revalidation");
    }
  }

  private static VerifiedCheckpoint requireSufficientCheckpoint(
      Snapshot snapshot, long target, VerifiedCheckpoint checkpoint) {
    Objects.requireNonNull(checkpoint, "checkpoint");
    KafkaCheckpointReferenceRecord reference = checkpoint.reference();
    if (reference.checkpointOffset() < target
        || reference.logStartOffsetAtCheckpoint() > snapshot.sourceHead().trimOffset()
        || !Arrays.equals(reference.objectSha256(), checkpoint.verifiedObjectSha256())) {
      throw invariant("verified Kafka checkpoint is insufficient for requested trim");
    }
    return checkpoint;
  }

  private static void requireSameAuthority(
      StableStreamHeadSnapshot expected, StableStreamHeadSnapshot actual) {
    if (!expected.streamId().equals(actual.streamId())
        || actual.state() != StreamState.ACTIVE
        || actual.committedEndOffset() < expected.committedEndOffset()
        || actual.commitVersion() < expected.commitVersion()
        || expected.appendSession().isEmpty()
        || actual.appendSession().isEmpty()) {
      throw invariant("Kafka stream or append authority disappeared during trim");
    }
    if (actual.commitVersion() == expected.commitVersion()
        && (!actual.lastCommitId().equals(expected.lastCommitId())
            || !actual.durableHeadSha256().equals(expected.durableHeadSha256()))) {
      throw invariant("Kafka stream head changed at the same commit version during trim");
    }
    AcquiredAppendSession expectedAcquired = expected.appendSession().orElseThrow();
    AcquiredAppendSession actualAcquired = actual.appendSession().orElseThrow();
    AppendSession expectedSession = expectedAcquired.session();
    AppendSession actualSession = actualAcquired.session();
    if (!expectedAcquired.authority().equals(actualAcquired.authority())
        || !expectedSession.writerId().equals(actualSession.writerId())
        || expectedSession.epoch() != actualSession.epoch()
        || !expectedSession.fencingToken().equals(actualSession.fencingToken())
        || actualSession.leaseVersion() < expectedSession.leaseVersion()) {
      throw invariant("Kafka append authority changed during trim");
    }
  }

  private static NereusException invariant(String message) {
    return new NereusException(ErrorCode.METADATA_INVARIANT_VIOLATION, false, message);
  }

  private static boolean deleteEnabled(KafkaRetentionPlanner.Policy policy) {
    return (policy.cleanupPolicyFlags() & LogConfigHistoryEntry.CLEANUP_DELETE_FLAG) != 0;
  }

  private static Throwable unwrap(Throwable failure) {
    Throwable current = failure;
    while (current instanceof CompletionException && current.getCause() != null) {
      current = current.getCause();
    }
    return current;
  }

  public record Snapshot(
      KafkaPartitionIdentity identity,
      VersionedKafkaPartitionBinding binding,
      StableStreamHeadSnapshot sourceHead,
      KafkaRetentionPlanner.Snapshot retention) {
    public Snapshot {
      Objects.requireNonNull(identity, "identity");
      Objects.requireNonNull(binding, "binding");
      Objects.requireNonNull(sourceHead, "sourceHead");
      Objects.requireNonNull(retention, "retention");
      KafkaPartitionBindingRecord root = binding.value();
      if (!identity.durableId().equals(root.identity())
          || root.lifecycle() != KafkaPartitionLifecycle.ACTIVE
          || !root.streamId().equals(sourceHead.streamId().value())
          || sourceHead.state() != StreamState.ACTIVE
          || sourceHead.appendSession().isEmpty()
          || sourceHead.appendSession().orElseThrow().authority().isEmpty()
          || sourceHead.trimOffset() != retention.virtualSegments().logStartOffset()
          || sourceHead.committedEndOffset() != retention.virtualSegments().stableEndOffset()
          || root.observedLogStartOffset() != sourceHead.trimOffset()
          || root.observedStableEndOffset() != sourceHead.committedEndOffset()) {
        throw new IllegalArgumentException("Kafka trim snapshot is not one exact active view");
      }
      AppendAuthority authority =
          sourceHead.appendSession().orElseThrow().authority().orElseThrow();
      if (!authority.authorityType().equals("kafka-partition-leader-v1")
          || !authority.authorityId().equals(identity.durableId().canonicalIdentity())
          || authority.authorityEpoch() != root.observedLeaderEpoch()
          || authority.ownerEpoch() != root.observedBrokerEpoch()) {
        throw new IllegalArgumentException(
            "Kafka trim snapshot authority does not match the observed leader");
      }
    }
  }

  public record VerifiedCheckpoint(
      KafkaCheckpointReferenceRecord reference, byte[] verifiedObjectSha256) {
    public VerifiedCheckpoint {
      Objects.requireNonNull(reference, "reference");
      verifiedObjectSha256 =
          Objects.requireNonNull(verifiedObjectSha256, "verifiedObjectSha256").clone();
      if (verifiedObjectSha256.length != 32) {
        throw new IllegalArgumentException("verified Kafka checkpoint SHA-256 must be 32 bytes");
      }
    }

    @Override
    public byte[] verifiedObjectSha256() {
      return verifiedObjectSha256.clone();
    }
  }

  public record Result(
      long previousLogStartOffset,
      long requestedTrimOffset,
      long durableTrimOffset,
      KafkaCheckpointReferenceRecord checkpoint,
      boolean alreadyApplied) {
    public Result {
      Objects.requireNonNull(checkpoint, "checkpoint");
      if (previousLogStartOffset < 0
          || requestedTrimOffset <= previousLogStartOffset
          || durableTrimOffset < requestedTrimOffset) {
        throw new IllegalArgumentException("invalid Kafka trim result");
      }
    }
  }

  private record TrimIntent(
      TrimKind kind, long previousLogStartOffset, long targetOffset, String trimReason) {
    private TrimIntent {
      Objects.requireNonNull(kind, "kind");
      Objects.requireNonNull(trimReason, "trimReason");
      if (previousLogStartOffset < 0
          || targetOffset <= previousLogStartOffset
          || trimReason.isBlank()) {
        throw new IllegalArgumentException("invalid Kafka trim intent");
      }
    }
  }

  private enum TrimKind {
    RETENTION,
    DELETE_RECORDS
  }

  @FunctionalInterface
  public interface SnapshotLoader {
    CompletableFuture<Snapshot> loadCurrent();
  }

  @FunctionalInterface
  public interface CheckpointGate {
    CompletableFuture<VerifiedCheckpoint> ensureVerified(Snapshot snapshot, long targetOffset);
  }

  @FunctionalInterface
  public interface DurableTrimListener {
    CompletableFuture<Void> onDurableTrim(
        Snapshot revalidated, long durableTrimOffset, KafkaCheckpointReferenceRecord checkpoint);
  }
}
