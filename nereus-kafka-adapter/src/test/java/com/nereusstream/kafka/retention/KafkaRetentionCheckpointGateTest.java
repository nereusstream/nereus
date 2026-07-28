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

import static com.nereusstream.kafka.retention.KafkaRetentionTestFixtures.binding;
import static com.nereusstream.kafka.retention.KafkaRetentionTestFixtures.checkpoint;
import static com.nereusstream.kafka.retention.KafkaRetentionTestFixtures.head;
import static com.nereusstream.kafka.retention.KafkaRetentionTestFixtures.retentionSnapshot;
import static com.nereusstream.kafka.retention.KafkaRetentionTestFixtures.snapshot;
import static com.nereusstream.kafka.retention.KafkaRetentionTestFixtures.verified;
import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.assertThatThrownBy;

import com.nereusstream.api.ErrorCode;
import com.nereusstream.api.NereusException;
import com.nereusstream.kafka.checkpoint.KafkaCheckpointFailureQuarantine;
import com.nereusstream.metadata.oxia.KafkaPartitionId;
import com.nereusstream.metadata.oxia.records.KafkaCheckpointFailureSource;
import com.nereusstream.metadata.oxia.records.KafkaCheckpointReferenceRecord;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.atomic.AtomicInteger;
import java.util.concurrent.atomic.AtomicReference;
import org.junit.jupiter.api.Test;

class KafkaRetentionCheckpointGateTest {
  private static final KafkaCheckpointReferenceRecord CHECKPOINT_10 = checkpoint(10);
  private static final KafkaCheckpointReferenceRecord CHECKPOINT_30 = checkpoint(30);
  private static final KafkaCheckpointReferenceRecord CHECKPOINT_40 = checkpoint(40);

  @Test
  void selectsAndVerifiesTheNewestSufficientRootWithoutPublication() {
    AtomicReference<KafkaCheckpointReferenceRecord> verifiedReference = new AtomicReference<>();
    AtomicInteger publications = new AtomicInteger();
    KafkaRetentionCheckpointGate gate =
        new KafkaRetentionCheckpointGate(
            (snapshot, reference) -> {
              verifiedReference.set(reference);
              return CompletableFuture.completedFuture(verified(reference));
            },
            snapshot -> {
              publications.incrementAndGet();
              return CompletableFuture.completedFuture(verified(CHECKPOINT_40));
            },
            KafkaCheckpointFailureQuarantine.transientObserver((reference, failure) -> {}));

    KafkaTrimBarrier.VerifiedCheckpoint result =
        gate.ensureVerified(
                snapshot(
                    binding(CHECKPOINT_30, CHECKPOINT_40),
                    head(0),
                    retentionSnapshot(250, 2_500, 20, 5_000)),
                20)
            .join();

    assertThat(result.reference()).isEqualTo(CHECKPOINT_40);
    assertThat(verifiedReference).hasValue(CHECKPOINT_40);
    assertThat(publications).hasValue(0);
  }

  @Test
  void publishesAtStableEndWhenNoRootedCheckpointReachesTheCandidate() {
    AtomicInteger verifications = new AtomicInteger();
    AtomicInteger publications = new AtomicInteger();
    KafkaRetentionCheckpointGate gate =
        new KafkaRetentionCheckpointGate(
            (snapshot, reference) -> {
              verifications.incrementAndGet();
              return CompletableFuture.completedFuture(verified(reference));
            },
            snapshot -> {
              publications.incrementAndGet();
              return CompletableFuture.completedFuture(verified(CHECKPOINT_40));
            },
            KafkaCheckpointFailureQuarantine.transientObserver((reference, failure) -> {}));

    KafkaTrimBarrier.VerifiedCheckpoint result =
        gate.ensureVerified(
                snapshot(binding(CHECKPOINT_10), head(0), retentionSnapshot(250, 2_500, 20, 5_000)),
                20)
            .join();

    assertThat(result.reference()).isEqualTo(CHECKPOINT_40);
    assertThat(verifications).hasValue(0);
    assertThat(publications).hasValue(1);
  }

  @Test
  void fallsBackFromCorruptNewestRootToTheNextSufficientRoot() {
    AtomicReference<KafkaCheckpointReferenceRecord> unusable = new AtomicReference<>();
    KafkaRetentionCheckpointGate gate =
        new KafkaRetentionCheckpointGate(
            (snapshot, reference) ->
                reference.equals(CHECKPOINT_40)
                    ? CompletableFuture.failedFuture(
                        new NereusException(
                            ErrorCode.OBJECT_CHECKSUM_MISMATCH, false, "corrupt checkpoint"))
                    : CompletableFuture.completedFuture(verified(reference)),
            snapshot ->
                CompletableFuture.failedFuture(new AssertionError("publication must not run")),
            KafkaCheckpointFailureQuarantine.transientObserver(
                (reference, failure) -> unusable.set(reference)));

    KafkaTrimBarrier.VerifiedCheckpoint result =
        gate.ensureVerified(
                snapshot(
                    binding(CHECKPOINT_30, CHECKPOINT_40),
                    head(0),
                    retentionSnapshot(250, 2_500, 20, 5_000)),
                20)
            .join();

    assertThat(result.reference()).isEqualTo(CHECKPOINT_30);
    assertThat(unusable).hasValue(CHECKPOINT_40);
  }

  @Test
  void waitsForDurableQuarantineBeforeTryingAnOlderRoot() {
    CompletableFuture<Void> durableWrite = new CompletableFuture<>();
    AtomicInteger verifications = new AtomicInteger();
    KafkaCheckpointFailureQuarantine quarantine =
        new KafkaCheckpointFailureQuarantine() {
          @Override
          public CompletableFuture<Boolean> isQuarantined(
              KafkaPartitionId identity,
              long partitionIncarnation,
              KafkaCheckpointReferenceRecord reference) {
            return CompletableFuture.completedFuture(false);
          }

          @Override
          public CompletableFuture<Void> quarantine(
              KafkaPartitionId identity,
              long partitionIncarnation,
              KafkaCheckpointReferenceRecord reference,
              KafkaCheckpointFailureSource source,
              Throwable failure) {
            return durableWrite;
          }
        };
    KafkaRetentionCheckpointGate gate =
        new KafkaRetentionCheckpointGate(
            (snapshot, reference) -> {
              verifications.incrementAndGet();
              return reference.equals(CHECKPOINT_40)
                  ? CompletableFuture.failedFuture(
                      new NereusException(
                          ErrorCode.OBJECT_CHECKSUM_MISMATCH, false, "corrupt checkpoint"))
                  : CompletableFuture.completedFuture(verified(reference));
            },
            snapshot ->
                CompletableFuture.failedFuture(new AssertionError("publication must not run")),
            quarantine);

    CompletableFuture<KafkaTrimBarrier.VerifiedCheckpoint> result =
        gate.ensureVerified(
            snapshot(
                binding(CHECKPOINT_30, CHECKPOINT_40),
                head(0),
                retentionSnapshot(250, 2_500, 20, 5_000)),
            20);

    assertThat(result).isNotDone();
    assertThat(verifications).hasValue(1);
    durableWrite.complete(null);
    assertThat(result.join().reference()).isEqualTo(CHECKPOINT_30);
    assertThat(verifications).hasValue(2);
  }

  @Test
  void failsClosedWhenDurableQuarantineCannotBeWritten() {
    AtomicInteger verifications = new AtomicInteger();
    KafkaCheckpointFailureQuarantine quarantine =
        new KafkaCheckpointFailureQuarantine() {
          @Override
          public CompletableFuture<Boolean> isQuarantined(
              KafkaPartitionId identity,
              long partitionIncarnation,
              KafkaCheckpointReferenceRecord reference) {
            return CompletableFuture.completedFuture(false);
          }

          @Override
          public CompletableFuture<Void> quarantine(
              KafkaPartitionId identity,
              long partitionIncarnation,
              KafkaCheckpointReferenceRecord reference,
              KafkaCheckpointFailureSource source,
              Throwable failure) {
            return CompletableFuture.failedFuture(
                new NereusException(
                    ErrorCode.METADATA_UNAVAILABLE,
                    true,
                    "checkpoint quarantine store unavailable"));
          }
        };
    KafkaRetentionCheckpointGate gate =
        new KafkaRetentionCheckpointGate(
            (snapshot, reference) -> {
              verifications.incrementAndGet();
              return reference.equals(CHECKPOINT_40)
                  ? CompletableFuture.failedFuture(
                      new NereusException(
                          ErrorCode.OBJECT_CHECKSUM_MISMATCH, false, "corrupt checkpoint"))
                  : CompletableFuture.completedFuture(verified(reference));
            },
            snapshot ->
                CompletableFuture.failedFuture(new AssertionError("publication must not run")),
            quarantine);

    assertThatThrownBy(
            () ->
                gate.ensureVerified(
                        snapshot(
                            binding(CHECKPOINT_30, CHECKPOINT_40),
                            head(0),
                            retentionSnapshot(250, 2_500, 20, 5_000)),
                        20)
                    .join())
        .hasRootCauseMessage("checkpoint quarantine store unavailable");
    assertThat(verifications).hasValue(1);
  }

  @Test
  void skipsDurablyQuarantinedRootWithoutObjectVerification() {
    AtomicInteger verifications = new AtomicInteger();
    KafkaCheckpointFailureQuarantine quarantine =
        new KafkaCheckpointFailureQuarantine() {
          @Override
          public CompletableFuture<Boolean> isQuarantined(
              KafkaPartitionId identity,
              long partitionIncarnation,
              KafkaCheckpointReferenceRecord reference) {
            return CompletableFuture.completedFuture(reference.equals(CHECKPOINT_40));
          }

          @Override
          public CompletableFuture<Void> quarantine(
              KafkaPartitionId identity,
              long partitionIncarnation,
              KafkaCheckpointReferenceRecord reference,
              KafkaCheckpointFailureSource source,
              Throwable failure) {
            return CompletableFuture.failedFuture(
                new AssertionError("existing quarantine must not be rewritten"));
          }
        };
    KafkaRetentionCheckpointGate gate =
        new KafkaRetentionCheckpointGate(
            (snapshot, reference) -> {
              verifications.incrementAndGet();
              return CompletableFuture.completedFuture(verified(reference));
            },
            snapshot ->
                CompletableFuture.failedFuture(new AssertionError("publication must not run")),
            quarantine);

    KafkaTrimBarrier.VerifiedCheckpoint result =
        gate.ensureVerified(
                snapshot(
                    binding(CHECKPOINT_30, CHECKPOINT_40),
                    head(0),
                    retentionSnapshot(250, 2_500, 20, 5_000)),
                20)
            .join();

    assertThat(result.reference()).isEqualTo(CHECKPOINT_30);
    assertThat(verifications).hasValue(1);
  }

  @Test
  void pausesInsteadOfPublishingWhenCheckpointStorageIsTemporarilyUnavailable() {
    AtomicInteger publications = new AtomicInteger();
    KafkaRetentionCheckpointGate gate =
        new KafkaRetentionCheckpointGate(
            (snapshot, reference) ->
                CompletableFuture.failedFuture(
                    new NereusException(
                        ErrorCode.METADATA_UNAVAILABLE, true, "checkpoint store unavailable")),
            snapshot -> {
              publications.incrementAndGet();
              return CompletableFuture.completedFuture(verified(CHECKPOINT_40));
            },
            KafkaCheckpointFailureQuarantine.transientObserver((reference, failure) -> {}));

    assertThatThrownBy(
            () ->
                gate.ensureVerified(
                        snapshot(
                            binding(CHECKPOINT_40),
                            head(0),
                            retentionSnapshot(250, 2_500, 20, 5_000)),
                        20)
                    .join())
        .hasRootCauseMessage("checkpoint store unavailable");
    assertThat(publications).hasValue(0);
  }

  @Test
  void rejectsPublicationThatDidNotFreezeTheCapturedStableEnd() {
    KafkaRetentionCheckpointGate gate =
        new KafkaRetentionCheckpointGate(
            (snapshot, reference) -> CompletableFuture.completedFuture(verified(reference)),
            snapshot -> CompletableFuture.completedFuture(verified(CHECKPOINT_30)),
            KafkaCheckpointFailureQuarantine.transientObserver((reference, failure) -> {}));

    assertThatThrownBy(
            () ->
                gate.ensureVerified(
                        snapshot(binding(), head(0), retentionSnapshot(250, 2_500, 20, 5_000)), 20)
                    .join())
        .hasRootCauseMessage("published Kafka retention checkpoint does not freeze the stable end");
  }

  @Test
  void rejectsPublicationWhoseVerifiedShaDoesNotMatchItsRoot() {
    KafkaRetentionCheckpointGate gate =
        new KafkaRetentionCheckpointGate(
            (snapshot, reference) -> CompletableFuture.completedFuture(verified(reference)),
            snapshot ->
                CompletableFuture.completedFuture(
                    new KafkaTrimBarrier.VerifiedCheckpoint(CHECKPOINT_40, new byte[32])),
            KafkaCheckpointFailureQuarantine.transientObserver((reference, failure) -> {}));

    assertThatThrownBy(
            () ->
                gate.ensureVerified(
                        snapshot(binding(), head(0), retentionSnapshot(250, 2_500, 20, 5_000)), 20)
                    .join())
        .hasRootCauseMessage("published Kafka retention checkpoint does not freeze the stable end");
  }
}
