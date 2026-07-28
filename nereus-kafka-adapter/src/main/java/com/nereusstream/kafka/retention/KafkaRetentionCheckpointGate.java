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

import com.nereusstream.api.ErrorCode;
import com.nereusstream.api.NereusException;
import com.nereusstream.kafka.checkpoint.KafkaCheckpointFailureQuarantine;
import com.nereusstream.metadata.oxia.records.KafkaCheckpointFailureSource;
import com.nereusstream.metadata.oxia.records.KafkaCheckpointReferenceRecord;
import java.util.Arrays;
import java.util.List;
import java.util.Objects;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.CompletionException;

/**
 * Selects the newest sufficient rooted NKC1 checkpoint, or publishes one at the frozen stable end.
 */
public final class KafkaRetentionCheckpointGate implements KafkaTrimBarrier.CheckpointGate {
  private final ExistingCheckpointVerifier verifier;
  private final CheckpointPublisher publisher;
  private final KafkaCheckpointFailureQuarantine quarantine;

  public KafkaRetentionCheckpointGate(
      ExistingCheckpointVerifier verifier,
      CheckpointPublisher publisher,
      KafkaCheckpointFailureQuarantine quarantine) {
    this.verifier = Objects.requireNonNull(verifier, "verifier");
    this.publisher = Objects.requireNonNull(publisher, "publisher");
    this.quarantine = Objects.requireNonNull(quarantine, "quarantine");
  }

  @Override
  public CompletableFuture<KafkaTrimBarrier.VerifiedCheckpoint> ensureVerified(
      KafkaTrimBarrier.Snapshot snapshot, long targetOffset) {
    Objects.requireNonNull(snapshot, "snapshot");
    if (targetOffset <= snapshot.sourceHead().trimOffset()
        || targetOffset > snapshot.retention().highWatermark()
        || targetOffset > snapshot.sourceHead().committedEndOffset()) {
      return CompletableFuture.failedFuture(
          new IllegalArgumentException(
              "Kafka retention checkpoint target is outside the frozen durable" + " window"));
    }
    List<KafkaCheckpointReferenceRecord> references =
        snapshot.binding().value().checkpointReferences();
    return tryExisting(snapshot, targetOffset, references, 0)
        .thenCompose(
            existing ->
                existing != null
                    ? CompletableFuture.completedFuture(existing)
                    : publishStableEnd(snapshot, targetOffset));
  }

  private CompletableFuture<KafkaTrimBarrier.VerifiedCheckpoint> tryExisting(
      KafkaTrimBarrier.Snapshot snapshot,
      long targetOffset,
      List<KafkaCheckpointReferenceRecord> references,
      int index) {
    if (index >= references.size()) {
      return CompletableFuture.completedFuture(null);
    }
    KafkaCheckpointReferenceRecord reference = references.get(index);
    if (reference.checkpointOffset() < targetOffset
        || reference.logStartOffsetAtCheckpoint() > snapshot.sourceHead().trimOffset()) {
      return tryExisting(snapshot, targetOffset, references, index + 1);
    }
    return quarantine
        .isQuarantined(
            snapshot.binding().value().identity(),
            snapshot.binding().value().incarnation(),
            reference)
        .thenCompose(
            quarantined ->
                quarantined
                    ? tryExisting(snapshot, targetOffset, references, index + 1)
                    : verifyExisting(snapshot, targetOffset, references, index, reference));
  }

  private CompletableFuture<KafkaTrimBarrier.VerifiedCheckpoint> verifyExisting(
      KafkaTrimBarrier.Snapshot snapshot,
      long targetOffset,
      List<KafkaCheckpointReferenceRecord> references,
      int index,
      KafkaCheckpointReferenceRecord reference) {
    CompletableFuture<KafkaTrimBarrier.VerifiedCheckpoint> verification;
    try {
      verification =
          Objects.requireNonNull(
              verifier.verify(snapshot, reference),
              "Kafka retention checkpoint verifier returned a null future");
    } catch (Throwable failure) {
      verification = CompletableFuture.failedFuture(failure);
    }
    return verification
        .thenApply(actual -> requireExact(reference, actual))
        .exceptionallyCompose(
            failure -> {
              Throwable exact = unwrap(failure);
              if (!canFallback(exact)) {
                return CompletableFuture.failedFuture(exact);
              }
              return quarantine
                  .quarantine(
                      snapshot.binding().value().identity(),
                      snapshot.binding().value().incarnation(),
                      reference,
                      KafkaCheckpointFailureSource.RETENTION,
                      exact)
                  .thenCompose(
                      ignored -> tryExisting(snapshot, targetOffset, references, index + 1));
            });
  }

  private CompletableFuture<KafkaTrimBarrier.VerifiedCheckpoint> publishStableEnd(
      KafkaTrimBarrier.Snapshot snapshot, long targetOffset) {
    CompletableFuture<KafkaTrimBarrier.VerifiedCheckpoint> publication;
    try {
      publication =
          Objects.requireNonNull(
              publisher.publish(snapshot),
              "Kafka retention checkpoint publisher returned a null future");
    } catch (Throwable failure) {
      publication = CompletableFuture.failedFuture(failure);
    }
    return publication.thenApply(
        checkpoint -> {
          Objects.requireNonNull(checkpoint, "checkpoint");
          KafkaCheckpointReferenceRecord reference = checkpoint.reference();
          if (reference.checkpointOffset() != snapshot.sourceHead().committedEndOffset()
              || reference.checkpointOffset() < targetOffset
              || reference.logStartOffsetAtCheckpoint() > snapshot.sourceHead().trimOffset()
              || !Arrays.equals(reference.objectSha256(), checkpoint.verifiedObjectSha256())) {
            throw invariant(
                "published Kafka retention checkpoint does not freeze the stable" + " end");
          }
          return checkpoint;
        });
  }

  private static KafkaTrimBarrier.VerifiedCheckpoint requireExact(
      KafkaCheckpointReferenceRecord expected, KafkaTrimBarrier.VerifiedCheckpoint actual) {
    Objects.requireNonNull(actual, "actual");
    if (!expected.equals(actual.reference())
        || !Arrays.equals(expected.objectSha256(), actual.verifiedObjectSha256())) {
      throw invariant("verified Kafka retention checkpoint conflicts with its rooted reference");
    }
    return actual;
  }

  private static boolean canFallback(Throwable failure) {
    if (!(failure instanceof NereusException nereus)) {
      return false;
    }
    return switch (nereus.code()) {
      case OBJECT_NOT_FOUND,
          OBJECT_CHECKSUM_MISMATCH,
          UNSUPPORTED_FORMAT,
          METADATA_INVARIANT_VIOLATION ->
          true;
      default -> false;
    };
  }

  private static Throwable unwrap(Throwable failure) {
    Throwable current = failure;
    while (current instanceof CompletionException && current.getCause() != null) {
      current = current.getCause();
    }
    return current;
  }

  private static NereusException invariant(String message) {
    return new NereusException(ErrorCode.METADATA_INVARIANT_VIOLATION, false, message);
  }

  @FunctionalInterface
  public interface ExistingCheckpointVerifier {
    CompletableFuture<KafkaTrimBarrier.VerifiedCheckpoint> verify(
        KafkaTrimBarrier.Snapshot snapshot, KafkaCheckpointReferenceRecord reference);
  }

  @FunctionalInterface
  public interface CheckpointPublisher {
    CompletableFuture<KafkaTrimBarrier.VerifiedCheckpoint> publish(
        KafkaTrimBarrier.Snapshot snapshot);
  }
}
