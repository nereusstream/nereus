/*
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 *   https://www.apache.org/licenses/LICENSE-2.0
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

package com.nereusstream.kafka.compaction;

import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.assertThatThrownBy;

import com.nereusstream.api.Checksum;
import com.nereusstream.api.ChecksumType;
import com.nereusstream.api.GenerationId;
import com.nereusstream.api.ObjectKey;
import com.nereusstream.api.PublicationId;
import com.nereusstream.api.ReadView;
import com.nereusstream.kafka.compaction.KafkaCompactionParquetPublisher.Evidence;
import com.nereusstream.kafka.compaction.KafkaCompactionParquetPublisher.PreparedObject;
import com.nereusstream.materialization.GenerationCommitResult;
import com.nereusstream.materialization.GenerationCommitter;
import com.nereusstream.materialization.KafkaCompactionTaskTestSupport;
import com.nereusstream.materialization.MaterializationTask;
import com.nereusstream.materialization.MaterializationTaskStore;
import com.nereusstream.metadata.oxia.GenerationMetadataStore;
import com.nereusstream.metadata.oxia.GenerationMetadataStoreTestFactory;
import com.nereusstream.metadata.oxia.KafkaCompactionCoverageActivationMode;
import com.nereusstream.metadata.oxia.KafkaPartitionId;
import com.nereusstream.metadata.oxia.KafkaPartitionMetadataStore;
import com.nereusstream.metadata.oxia.KafkaPartitionMetadataTransitions;
import com.nereusstream.metadata.oxia.KafkaPartitionScanPage;
import com.nereusstream.metadata.oxia.VersionedKafkaPartitionBinding;
import com.nereusstream.metadata.oxia.VersionedMaterializationTask;
import com.nereusstream.metadata.oxia.records.KafkaCompactionCoverageRecord;
import com.nereusstream.metadata.oxia.records.KafkaPartitionBindingRecord;
import com.nereusstream.metadata.oxia.records.KafkaPartitionOperationType;
import com.nereusstream.metadata.oxia.records.KafkaPartitionPendingOperationRecord;
import com.nereusstream.metadata.oxia.records.KafkaPartitionRegistryRecord;
import com.nereusstream.metadata.oxia.testing.FakeKafkaPartitionMetadataStore;
import com.nereusstream.objectstore.DeleteObjectOptions;
import com.nereusstream.objectstore.DeleteObjectResult;
import com.nereusstream.objectstore.HeadObjectOptions;
import com.nereusstream.objectstore.HeadObjectResult;
import com.nereusstream.objectstore.ListObjectsOptions;
import com.nereusstream.objectstore.ListObjectsResult;
import com.nereusstream.objectstore.ObjectKeyPrefix;
import com.nereusstream.objectstore.ObjectStore;
import com.nereusstream.objectstore.PutObjectAttemptGuard;
import com.nereusstream.objectstore.PutObjectOptions;
import com.nereusstream.objectstore.PutObjectResult;
import com.nereusstream.objectstore.RangeReadOptions;
import com.nereusstream.objectstore.RangeReadResult;
import com.nereusstream.objectstore.ReplayableObjectUpload;
import com.nereusstream.objectstore.compacted.CompactedObjectFormatV2;
import com.nereusstream.objectstore.compacted.KafkaCompactionDispositionV2;
import com.nereusstream.objectstore.compacted.KafkaCompactionKeyEncodingV2;
import com.nereusstream.objectstore.compacted.KafkaTopicCompactedFormatSpecV2;
import com.nereusstream.objectstore.compacted.KafkaTopicCompactedObjectRow;
import com.nereusstream.objectstore.compacted.KafkaTopicCompactedObjectWriteRequest;
import com.nereusstream.objectstore.compacted.ParquetKafkaTopicCompactedReader;
import com.nereusstream.objectstore.compacted.ParquetKafkaTopicCompactedWriter;
import com.nereusstream.objectstore.compacted.ParquetRangedCompactedObjectReader;
import com.nereusstream.objectstore.compacted.RangedCompactedObjectVerifier;
import com.nereusstream.objectstore.compacted.RangedCompactedObjectWriteResult;
import com.nereusstream.objectstore.staging.StagingFileManager;
import com.nereusstream.objectstore.testing.LocalFileObjectStore;
import java.nio.ByteBuffer;
import java.nio.file.Files;
import java.nio.file.Path;
import java.nio.file.attribute.PosixFilePermissions;
import java.security.MessageDigest;
import java.time.Clock;
import java.time.Duration;
import java.time.Instant;
import java.time.ZoneOffset;
import java.util.HexFormat;
import java.util.List;
import java.util.Optional;
import java.util.OptionalLong;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.CompletionException;
import java.util.concurrent.Flow;
import java.util.concurrent.atomic.AtomicBoolean;
import java.util.concurrent.atomic.AtomicInteger;
import java.util.zip.CRC32C;
import org.apache.kafka.common.compress.Compression;
import org.apache.kafka.common.record.MemoryRecords;
import org.apache.kafka.common.record.SimpleRecord;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.io.TempDir;

class KafkaCompactionPublicationCoordinatorTest {
  private static final String CLUSTER = "test-cluster";
  private static final String KAFKA_CLUSTER = "kraft";
  private static final String CLAIM_ID = "a".repeat(26);
  private static final String PROCESS_ID = "b".repeat(26);
  private static final Clock CLOCK = Clock.fixed(Instant.ofEpochMilli(2_000), ZoneOffset.UTC);

  @TempDir Path temporaryDirectory;

  @Test
  void convergesPutAndCoverageResponseLossAfterGenerationCommit() throws Exception {
    try (Context context = context(true, true, false)) {
      VersionedMaterializationTask claimed =
          context.coordinator.claim(context.planned, CLAIM_ID, PROCESS_ID, 10_000).join();
      PreparedObject prepared = context.prepared(CLAIM_ID);

      KafkaCompactionPublicationCoordinator.PublicationResult result =
          context
              .coordinator
              .publish(
                  context.partition,
                  claimed,
                  context.fixture.plan(),
                  prepared,
                  KafkaCompactionCoverageActivationMode.INITIAL,
                  Optional.empty(),
                  () -> CompletableFuture.completedFuture(null))
              .join();

      assertThat(context.putAttempts).hasValue(1);
      assertThat(context.generationCommits).hasValue(1);
      assertThat(context.coverageAttempts).hasValue(1);
      assertThat(result.output().logicalFormat())
          .isEqualTo(CompactedObjectFormatV2.KAFKA_LOGICAL_FORMAT);
      assertThat(result.committedGeneration().view()).isEqualTo(ReadView.TOPIC_COMPACTED);
      assertThat(result.binding().value().compactionCoverage().startOffset()).isZero();
      assertThat(result.binding().value().compactionCoverage().endOffset()).isEqualTo(2);
      assertThat(result.binding().value().compactionCoverage().activationEpoch()).isEqualTo(1);
      assertThat(result.binding().value().compactionCoverage().generationSetSha256())
          .containsExactly(result.generationSet().digestBytes());
      assertThat(
              context
                  .objects
                  .headObject(
                      result.output().objectKey(), new HeadObjectOptions(Duration.ofSeconds(5)))
                  .join()
                  .checksum())
          .isEqualTo(result.output().storageCrc32c());
    }
  }

  @Test
  void leavesCommittedGenerationNonMandatoryWhenCoverageBasisChanges() throws Exception {
    try (Context context = context(false, false, true)) {
      VersionedMaterializationTask claimed =
          context.coordinator.claim(context.planned, CLAIM_ID, PROCESS_ID, 10_000).join();
      PreparedObject prepared = context.prepared(CLAIM_ID);

      assertThatThrownBy(
              () ->
                  context
                      .coordinator
                      .publish(
                          context.partition,
                          claimed,
                          context.fixture.plan(),
                          prepared,
                          KafkaCompactionCoverageActivationMode.INITIAL,
                          Optional.empty(),
                          () -> CompletableFuture.completedFuture(null))
                      .join())
          .isInstanceOf(CompletionException.class)
          .hasRootCauseMessage("Kafka compaction coverage changed before activation");

      assertThat(context.generationCommits).hasValue(1);
      KafkaCompactionCoverageRecord actual =
          context
              .partitions
              .get(context.partition)
              .join()
              .orElseThrow()
              .value()
              .compactionCoverage();
      assertThat(actual.coverageVersion()).isEqualTo(1);
      assertThat(actual.generationSetSha256()).containsExactly(bytes(9));
    }
  }

  @Test
  void generationSetDigestAndStrictNtc2VerifierAgreeWithFrozenOutput() throws Exception {
    try (Context context = context(false, false, false)) {
      VersionedMaterializationTask claimed =
          context.coordinator.claim(context.planned, CLAIM_ID, PROCESS_ID, 10_000).join();
      context.tasks.heartbeat(claimed, 12_000).join();
      PreparedObject prepared = context.prepared(CLAIM_ID);

      KafkaCompactionPublicationCoordinator.PublicationResult result =
          context
              .coordinator
              .publish(
                  context.partition,
                  claimed,
                  context.fixture.plan(),
                  prepared,
                  KafkaCompactionCoverageActivationMode.INITIAL,
                  Optional.empty(),
                  () -> CompletableFuture.completedFuture(null))
              .join();

      assertThat(result.output().payloadFormat())
          .isEqualTo(com.nereusstream.api.PayloadFormat.KAFKA_RECORD_BATCH);
      assertThat(result.output().physicalFormat())
          .isEqualTo(CompactedObjectFormatV2.TOPIC_COMPACTED_PHYSICAL_FORMAT);
      assertThat(result.output().outputRecordCount()).isOne();
      assertThat(result.output().entryCount()).isOne();
      assertThat(result.generationSet().coverage()).isEqualTo(result.output().coverage());
      new KafkaCompactionMaterializationFormatVerifier(context.verifier)
          .verify(context.fixture.outputTask(), result.output(), Duration.ofSeconds(20))
          .join();
    }
  }

  private Context context(
      boolean losePutResponse, boolean loseCoverageResponse, boolean mutateCoverageAfterCommit)
      throws Exception {
    KafkaCompactionPlanCodecV1Test.Fixture fixture =
        KafkaCompactionPlanCodecV1Test.fixture("UNCOMPRESSED");
    Path stagingPath =
        Files.createDirectory(temporaryDirectory.resolve("staging-" + System.nanoTime()));
    Files.setPosixFilePermissions(stagingPath, PosixFilePermissions.fromString("rwx------"));
    StagingFileManager staging =
        new StagingFileManager(
            stagingPath,
            64L << 20,
            StagingFileManager.MIN_UPLOAD_CHUNK_BYTES,
            Duration.ofHours(1),
            Runnable::run);
    LocalFileObjectStore objects =
        new LocalFileObjectStore(
            Files.createDirectory(temporaryDirectory.resolve("objects-" + System.nanoTime())));
    AtomicInteger putAttempts = new AtomicInteger();
    ObjectStore objectStore = new ResponseLossObjectStore(objects, losePutResponse, putAttempts);
    RangedCompactedObjectVerifier verifier =
        new RangedCompactedObjectVerifier(
            objectStore,
            new ParquetRangedCompactedObjectReader(objectStore, Runnable::run),
            new ParquetKafkaTopicCompactedReader(objectStore, Runnable::run));

    GenerationMetadataStore generationMetadata = GenerationMetadataStoreTestFactory.inMemory(CLOCK);
    generationMetadata
        .createTask(
            CLUSTER, KafkaCompactionTaskTestSupport.planned(fixture.outputTask(), 0).value())
        .join();
    MaterializationTaskStore tasks =
        new MaterializationTaskStore(CLUSTER, generationMetadata, CLOCK);
    VersionedMaterializationTask planned =
        tasks
            .get(fixture.outputTask().streamId(), fixture.outputTask().taskId())
            .join()
            .orElseThrow();

    FakeKafkaPartitionMetadataStore basePartitions =
        new FakeKafkaPartitionMetadataStore(CLUSTER, KAFKA_CLUSTER);
    KafkaPartitionId partition = new KafkaPartitionId(KAFKA_CLUSTER, topicId(71), 0);
    createActiveBinding(basePartitions, partition, fixture.outputTask(), fixture.plan());
    AtomicInteger coverageAttempts = new AtomicInteger();
    KafkaPartitionMetadataStore partitions =
        new ResponseLossPartitionStore(basePartitions, loseCoverageResponse, coverageAttempts);
    AtomicInteger generationCommits = new AtomicInteger();
    GenerationCommitter committer =
        (task, output) -> {
          generationCommits.incrementAndGet();
          CompletableFuture<Void> mutation =
              mutateCoverageAfterCommit
                  ? basePartitions
                      .get(partition)
                      .thenCompose(
                          current ->
                              basePartitions
                                  .activateCompactionCoverage(
                                      current.orElseThrow(),
                                      KafkaCompactionCoverageActivationMode.INITIAL,
                                      0,
                                      2,
                                      bytes(9),
                                      bytes(8),
                                      2_100)
                                  .thenApply(ignored -> null))
                  : CompletableFuture.completedFuture(null);
          return mutation.thenApply(
              ignored ->
                  new GenerationCommitResult(
                      task.streamId(),
                      task.view(),
                      task.coverage(),
                      new GenerationId(1),
                      new PublicationId("p".repeat(26)),
                      "generation-index/1",
                      31,
                      sha('7'),
                      true));
        };
    KafkaCompactionPublicationCoordinator coordinator =
        new KafkaCompactionPublicationCoordinator(
            CLUSTER,
            objectStore,
            verifier,
            tasks,
            committer,
            partitions,
            Duration.ofSeconds(20),
            CLOCK);
    return new Context(
        fixture,
        staging,
        objects,
        verifier,
        generationMetadata,
        basePartitions,
        partitions,
        partition,
        planned,
        tasks,
        coordinator,
        putAttempts,
        coverageAttempts,
        generationCommits);
  }

  private static void createActiveBinding(
      FakeKafkaPartitionMetadataStore store,
      KafkaPartitionId partition,
      MaterializationTask task,
      KafkaCompactionPlan plan) {
    KafkaPartitionPendingOperationRecord create =
        new KafkaPartitionPendingOperationRecord(
            KafkaPartitionOperationType.CREATE.wireId(),
            "c".repeat(26),
            "broker-run",
            1,
            10_000,
            1,
            1_000,
            "");
    KafkaPartitionBindingRecord creating =
        KafkaPartitionMetadataTransitions.creating(
            partition, "orders", "OBJECT_WAL_SYNC_OBJECT", 1, 1_000, create);
    VersionedKafkaPartitionBinding current = store.putCreatingIfAbsent(creating).join();
    current =
        store
            .compareAndSet(
                current,
                KafkaPartitionMetadataTransitions.activate(
                    current.value(),
                    KafkaPartitionMetadataTransitions.deterministicStreamName(partition, 1),
                    task.streamId().value(),
                    2,
                    1_100))
            .join();
    while (current.metadataVersion() < plan.bindingMetadataVersion()) {
      KafkaPartitionBindingRecord observed =
          KafkaPartitionMetadataTransitions.observe(
              current.value(),
              "orders",
              current.value().lastAppliedMetadataOffset(),
              1,
              1,
              1,
              0,
              plan.candidate().decisionHorizon().endOffset(),
              Math.max(1_200, current.value().updatedAtMillis() + 1));
      current = store.compareAndSet(current, observed).join();
    }
  }

  private static byte[] kafkaBatch() {
    ByteBuffer buffer =
        MemoryRecords.withRecords(
                0,
                Compression.NONE,
                new SimpleRecord(
                    1_000,
                    "key".getBytes(java.nio.charset.StandardCharsets.UTF_8),
                    "value".getBytes(java.nio.charset.StandardCharsets.UTF_8)))
            .buffer();
    byte[] bytes = new byte[buffer.remaining()];
    buffer.get(bytes);
    return bytes;
  }

  private static <T> Flow.Publisher<T> publisher(List<T> rows) {
    return subscriber ->
        subscriber.onSubscribe(
            new Flow.Subscription() {
              private int index;
              private boolean done;

              @Override
              public void request(long count) {
                if (done || count <= 0) {
                  return;
                }
                long remaining = count;
                while (!done && remaining-- > 0 && index < rows.size()) {
                  subscriber.onNext(rows.get(index++));
                }
                if (index == rows.size() && !done) {
                  done = true;
                  subscriber.onComplete();
                }
              }

              @Override
              public void cancel() {
                done = true;
              }
            });
  }

  private static int crc(byte[] value) {
    CRC32C crc = new CRC32C();
    crc.update(value, 0, value.length);
    return (int) crc.getValue();
  }

  private static Checksum sha(char value) {
    return new Checksum(ChecksumType.SHA256, String.valueOf(value).repeat(64));
  }

  private static Checksum sha(byte[] value) {
    try {
      return new Checksum(
          ChecksumType.SHA256,
          HexFormat.of().formatHex(MessageDigest.getInstance("SHA-256").digest(value)));
    } catch (Exception failure) {
      throw new AssertionError(failure);
    }
  }

  private static byte[] bytes(int seed) {
    byte[] value = new byte[32];
    for (int index = 0; index < value.length; index++) {
      value[index] = (byte) (seed + index);
    }
    return value;
  }

  private static String topicId(int value) {
    byte[] bytes = new byte[16];
    bytes[15] = (byte) value;
    return java.util.Base64.getUrlEncoder().withoutPadding().encodeToString(bytes);
  }

  private static final class ResponseLossObjectStore implements ObjectStore {
    private final ObjectStore delegate;
    private final AtomicBoolean lose;
    private final AtomicInteger attempts;

    private ResponseLossObjectStore(
        ObjectStore delegate, boolean loseFirstPut, AtomicInteger attempts) {
      this.delegate = delegate;
      this.lose = new AtomicBoolean(loseFirstPut);
      this.attempts = attempts;
    }

    @Override
    public CompletableFuture<PutObjectResult> putObject(
        ObjectKey key,
        ReplayableObjectUpload source,
        PutObjectOptions options,
        PutObjectAttemptGuard attemptGuard) {
      attempts.incrementAndGet();
      return delegate
          .putObject(key, source, options, attemptGuard)
          .thenCompose(
              result ->
                  lose.compareAndSet(true, false)
                      ? CompletableFuture.failedFuture(
                          new IllegalStateException("lost NTC2 PUT response"))
                      : CompletableFuture.completedFuture(result));
    }

    @Override
    public CompletableFuture<RangeReadResult> readRange(
        ObjectKey key, long offset, long length, RangeReadOptions options) {
      return delegate.readRange(key, offset, length, options);
    }

    @Override
    public CompletableFuture<HeadObjectResult> headObject(
        ObjectKey key, HeadObjectOptions options) {
      return delegate.headObject(key, options);
    }

    @Override
    public CompletableFuture<ListObjectsResult> listObjects(
        ObjectKeyPrefix prefix, Optional<String> continuationToken, ListObjectsOptions options) {
      return delegate.listObjects(prefix, continuationToken, options);
    }

    @Override
    public CompletableFuture<DeleteObjectResult> deleteObject(
        ObjectKey key, DeleteObjectOptions options) {
      return delegate.deleteObject(key, options);
    }

    @Override
    public void close() {}
  }

  private static final class ResponseLossPartitionStore implements KafkaPartitionMetadataStore {
    private final KafkaPartitionMetadataStore delegate;
    private final AtomicBoolean lose;
    private final AtomicInteger coverageAttempts;

    private ResponseLossPartitionStore(
        KafkaPartitionMetadataStore delegate,
        boolean loseFirstCoverageResponse,
        AtomicInteger coverageAttempts) {
      this.delegate = delegate;
      this.lose = new AtomicBoolean(loseFirstCoverageResponse);
      this.coverageAttempts = coverageAttempts;
    }

    @Override
    public CompletableFuture<Optional<VersionedKafkaPartitionBinding>> get(KafkaPartitionId id) {
      return delegate.get(id);
    }

    @Override
    public CompletableFuture<VersionedKafkaPartitionBinding> putCreatingIfAbsent(
        KafkaPartitionBindingRecord value) {
      return delegate.putCreatingIfAbsent(value);
    }

    @Override
    public CompletableFuture<VersionedKafkaPartitionBinding> compareAndSet(
        VersionedKafkaPartitionBinding expected, KafkaPartitionBindingRecord update) {
      boolean coverageChange =
          !expected.value().compactionCoverage().equals(update.compactionCoverage());
      if (coverageChange) {
        coverageAttempts.incrementAndGet();
      }
      return delegate
          .compareAndSet(expected, update)
          .thenCompose(
              result ->
                  coverageChange && lose.compareAndSet(true, false)
                      ? CompletableFuture.failedFuture(
                          new IllegalStateException("lost coverage CAS response"))
                      : CompletableFuture.completedFuture(result));
    }

    @Override
    public CompletableFuture<Void> putRegistryHint(KafkaPartitionRegistryRecord value) {
      return delegate.putRegistryHint(value);
    }

    @Override
    public CompletableFuture<KafkaPartitionScanPage> scanRegistry(
        int shard, Optional<String> continuation, int limit) {
      return delegate.scanRegistry(shard, continuation, limit);
    }

    @Override
    public void close() {}
  }

  private static final class Context implements AutoCloseable {
    private final KafkaCompactionPlanCodecV1Test.Fixture fixture;
    private final StagingFileManager staging;
    private final LocalFileObjectStore objects;
    private final RangedCompactedObjectVerifier verifier;
    private final GenerationMetadataStore generationMetadata;
    private final FakeKafkaPartitionMetadataStore basePartitions;
    private final KafkaPartitionMetadataStore partitions;
    private final KafkaPartitionId partition;
    private final VersionedMaterializationTask planned;
    private final MaterializationTaskStore tasks;
    private final KafkaCompactionPublicationCoordinator coordinator;
    private final AtomicInteger putAttempts;
    private final AtomicInteger coverageAttempts;
    private final AtomicInteger generationCommits;

    private Context(
        KafkaCompactionPlanCodecV1Test.Fixture fixture,
        StagingFileManager staging,
        LocalFileObjectStore objects,
        RangedCompactedObjectVerifier verifier,
        GenerationMetadataStore generationMetadata,
        FakeKafkaPartitionMetadataStore basePartitions,
        KafkaPartitionMetadataStore partitions,
        KafkaPartitionId partition,
        VersionedMaterializationTask planned,
        MaterializationTaskStore tasks,
        KafkaCompactionPublicationCoordinator coordinator,
        AtomicInteger putAttempts,
        AtomicInteger coverageAttempts,
        AtomicInteger generationCommits) {
      this.fixture = fixture;
      this.staging = staging;
      this.objects = objects;
      this.verifier = verifier;
      this.generationMetadata = generationMetadata;
      this.basePartitions = basePartitions;
      this.partitions = partitions;
      this.partition = partition;
      this.planned = planned;
      this.tasks = tasks;
      this.coordinator = coordinator;
      this.putAttempts = putAttempts;
      this.coverageAttempts = coverageAttempts;
      this.generationCommits = generationCommits;
    }

    private PreparedObject prepared(String outputAttemptId) {
      MaterializationTask task = fixture.outputTask();
      byte[] payload = kafkaBatch();
      KafkaTopicCompactedObjectRow row =
          new KafkaTopicCompactedObjectRow(
              0,
              1,
              KafkaCompactionDispositionV2.RETAIN_VALUE,
              KafkaCompactionKeyEncodingV2.keyed(
                  ByteBuffer.wrap("key".getBytes(java.nio.charset.StandardCharsets.UTF_8))),
              ByteBuffer.wrap(payload),
              crc(payload),
              0,
              0,
              sha(payload),
              OptionalLong.of(1_000));
      KafkaTopicCompactedObjectWriteRequest request =
          new KafkaTopicCompactedObjectWriteRequest(
              CLUSTER,
              task.streamId(),
              task.coverage(),
              outputAttemptId,
              task.sourceSetSha256(),
              task.policyDigestSha256(),
              1,
              1,
              payload.length,
              task.sources().get(task.sources().size() - 1).cumulativeSizeAtEnd(),
              task.policy().targetRowGroupRecords(),
              task.policy().compression(),
              "test-build",
              new KafkaTopicCompactedFormatSpecV2(
                  KafkaCompactionStrategyV1.STRATEGY_ID,
                  KafkaCompactionStrategyV1.STRATEGY_VERSION,
                  KafkaCompactionKeyEncodingV2.ID,
                  CompactedObjectFormatV2.KAFKA_REWRITE_CODEC,
                  KafkaTopicCompactionCodecV1.MESSAGE_FORMAT_SHA256,
                  1,
                  1));
      ParquetKafkaTopicCompactedWriter writer =
          new ParquetKafkaTopicCompactedWriter(staging, Runnable::run);
      RangedCompactedObjectWriteResult written =
          writer.write(request, publisher(List.of(row))).join();
      return new PreparedObject(
          request,
          written,
          new Evidence(
              fixture.plan().decisionSources().sources().size(),
              fixture.plan().outputSources().sources().size(),
              fixture.plan().decisionSources().sourceSetSha256(),
              task.sourceSetSha256(),
              sha('3'),
              sha('4'),
              0,
              1));
    }

    @Override
    public void close() {
      partitions.close();
      basePartitions.close();
      generationMetadata.close();
      objects.close();
      staging.close();
    }
  }
}
