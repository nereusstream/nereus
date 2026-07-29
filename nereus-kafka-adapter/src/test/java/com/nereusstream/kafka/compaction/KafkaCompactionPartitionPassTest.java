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

import static com.nereusstream.kafka.codec.KafkaRecordBatchTestSupport.bytes;
import static com.nereusstream.kafka.codec.KafkaRecordBatchTestSupport.readBatch;
import static org.assertj.core.api.Assertions.assertThat;

import com.nereusstream.api.Checksum;
import com.nereusstream.api.ChecksumType;
import com.nereusstream.api.GenerationId;
import com.nereusstream.api.OffsetRange;
import com.nereusstream.api.PublicationId;
import com.nereusstream.api.ReadBatch;
import com.nereusstream.api.ReadIsolation;
import com.nereusstream.api.ReadOptions;
import com.nereusstream.api.ReadSourceRef;
import com.nereusstream.api.ReadView;
import com.nereusstream.api.StreamId;
import com.nereusstream.kafka.checkpoint.KafkaVirtualSegmentState;
import com.nereusstream.kafka.checkpoint.KafkaVirtualSegmentState.LogConfigHistoryEntry;
import com.nereusstream.kafka.checkpoint.KafkaVirtualSegmentState.RollReason;
import com.nereusstream.kafka.checkpoint.KafkaVirtualSegmentState.SegmentState;
import com.nereusstream.kafka.checkpoint.KafkaVirtualSegmentState.VirtualSegment;
import com.nereusstream.kafka.compaction.KafkaCompactionPassOneCollector.Snapshot;
import com.nereusstream.kafka.compaction.KafkaCompactionPlanner.Candidate;
import com.nereusstream.kafka.compaction.KafkaCompactionPlanner.Policy;
import com.nereusstream.kafka.compaction.KafkaCompactionTwoPassExecutor.Limits;
import com.nereusstream.materialization.CommittedSourceSetResolution;
import com.nereusstream.materialization.CommittedSourceSetResolver;
import com.nereusstream.materialization.ExactSourceRangeReader;
import com.nereusstream.materialization.ExactSourceRead;
import com.nereusstream.materialization.ExactSourceReadSummary;
import com.nereusstream.materialization.ExactSourceSet;
import com.nereusstream.materialization.GenerationCommitResult;
import com.nereusstream.materialization.KafkaCompactionTaskTestSupport;
import com.nereusstream.materialization.MaterializationFailure;
import com.nereusstream.materialization.MaterializationPolicy;
import com.nereusstream.materialization.MaterializationPolicyFactory;
import com.nereusstream.materialization.MaterializationTask;
import com.nereusstream.materialization.MaterializationTaskStore;
import com.nereusstream.materialization.SourceGeneration;
import com.nereusstream.materialization.TopicCompactionSpec;
import com.nereusstream.metadata.oxia.GenerationMetadataStore;
import com.nereusstream.metadata.oxia.GenerationMetadataStoreTestFactory;
import com.nereusstream.metadata.oxia.KafkaCompactionPlanMetadataStore;
import com.nereusstream.metadata.oxia.KafkaCompactionPlanScanPage;
import com.nereusstream.metadata.oxia.KafkaCompactionPlanScanToken;
import com.nereusstream.metadata.oxia.KafkaPartitionId;
import com.nereusstream.metadata.oxia.KafkaPartitionMetadataTransitions;
import com.nereusstream.metadata.oxia.VersionedKafkaCompactionPlan;
import com.nereusstream.metadata.oxia.VersionedKafkaPartitionBinding;
import com.nereusstream.metadata.oxia.records.KafkaCompactionPlanRecord;
import com.nereusstream.metadata.oxia.records.KafkaPartitionBindingRecord;
import com.nereusstream.metadata.oxia.records.KafkaPartitionOperationType;
import com.nereusstream.metadata.oxia.records.KafkaPartitionPendingOperationRecord;
import com.nereusstream.metadata.oxia.records.TaskFailureClass;
import com.nereusstream.metadata.oxia.testing.FakeKafkaPartitionMetadataStore;
import com.nereusstream.objectstore.compacted.ParquetKafkaTopicCompactedReader;
import com.nereusstream.objectstore.compacted.ParquetKafkaTopicCompactedWriter;
import com.nereusstream.objectstore.compacted.ParquetRangedCompactedObjectReader;
import com.nereusstream.objectstore.compacted.RangedCompactedObjectVerifier;
import com.nereusstream.objectstore.staging.StagingFileManager;
import com.nereusstream.objectstore.testing.LocalFileObjectStore;
import java.nio.file.Files;
import java.nio.file.Path;
import java.nio.file.attribute.PosixFilePermissions;
import java.time.Clock;
import java.time.Duration;
import java.time.Instant;
import java.time.ZoneOffset;
import java.util.ArrayList;
import java.util.Base64;
import java.util.List;
import java.util.Optional;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.Executors;
import java.util.concurrent.Flow;
import java.util.concurrent.ScheduledExecutorService;
import java.util.concurrent.atomic.AtomicInteger;
import org.apache.kafka.common.compress.Compression;
import org.apache.kafka.common.record.MemoryRecords;
import org.apache.kafka.common.record.SimpleRecord;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.io.TempDir;

class KafkaCompactionPartitionPassTest {
  private static final String CLUSTER = "test-cluster";
  private static final String KAFKA_CLUSTER = "kraft";
  private static final Clock CLOCK = Clock.fixed(Instant.ofEpochMilli(2_000), ZoneOffset.UTC);

  @TempDir Path temporaryDirectory;

  @Test
  void resumesAPlannedTaskThroughNtc2ActivationAndDualRootRetirement() throws Exception {
    Fixture fixture = fixture();
    KafkaPartitionId partition = new KafkaPartitionId(KAFKA_CLUSTER, topicId(91), 0);
    FakePlanStore planStore = new FakePlanStore(partition, fixture.plan());
    GenerationMetadataStore generationMetadata = GenerationMetadataStoreTestFactory.inMemory(CLOCK);
    generationMetadata
        .createTask(
            CLUSTER, KafkaCompactionTaskTestSupport.planned(fixture.outputTask(), 0).value())
        .join();
    MaterializationTaskStore tasks =
        new MaterializationTaskStore(CLUSTER, generationMetadata, CLOCK);
    FakeKafkaPartitionMetadataStore partitions =
        new FakeKafkaPartitionMetadataStore(CLUSTER, KAFKA_CLUSTER);
    VersionedKafkaPartitionBinding binding =
        createActiveBinding(partitions, partition, fixture.outputTask(), fixture.plan());
    AtomicInteger sourceCloses = new AtomicInteger();
    ScheduledExecutorService scheduler = Executors.newSingleThreadScheduledExecutor();

    Path stagingPath = Files.createDirectory(temporaryDirectory.resolve("staging"));
    Files.setPosixFilePermissions(stagingPath, PosixFilePermissions.fromString("rwx------"));
    try (StagingFileManager staging =
            new StagingFileManager(
                stagingPath,
                64L << 20,
                StagingFileManager.MIN_UPLOAD_CHUNK_BYTES,
                Duration.ofHours(1),
                Runnable::run);
        LocalFileObjectStore objects =
            new LocalFileObjectStore(temporaryDirectory.resolve("objects"))) {
      KafkaCompactionBatchSource batchSource =
          new KafkaCompactionBatchSource(
              exactReader(fixture, sourceCloses), readOptions(), Runnable::run);
      KafkaCompactionStreamingExecutor executor =
          new KafkaCompactionStreamingExecutor(
              new KafkaTopicCompactionCodecV1(),
              new KafkaCompactionStrategyV1(),
              new KafkaCompactionRowMapper(),
              new Limits(10, 10, 1 << 20),
              staging,
              Runnable::run);
      KafkaCompactionParquetPublisher parquet =
          new KafkaCompactionParquetPublisher(
              executor,
              new KafkaCompactionWriteRequestFactory(),
              new ParquetKafkaTopicCompactedWriter(staging, Runnable::run));
      RangedCompactedObjectVerifier verifier =
          new RangedCompactedObjectVerifier(
              objects,
              new ParquetRangedCompactedObjectReader(objects, Runnable::run),
              new ParquetKafkaTopicCompactedReader(objects, Runnable::run));
      AtomicInteger generationPublications = new AtomicInteger();
      PublicationId publicationId = new PublicationId("p".repeat(26));
      KafkaCompactionPublicationCoordinator publication =
          new KafkaCompactionPublicationCoordinator(
              CLUSTER,
              objects,
              verifier,
              tasks,
              (task, output) ->
                  KafkaCompactionTaskTestSupport.publish(tasks, task, 1, publicationId)
                      .thenApply(
                          ignored -> {
                            generationPublications.incrementAndGet();
                            return new GenerationCommitResult(
                                task.streamId(),
                                task.view(),
                                task.coverage(),
                                new GenerationId(1),
                                publicationId,
                                "generation-index/1",
                                31,
                                sha256('7'),
                                true);
                          }),
              partitions,
              Duration.ofSeconds(20),
              CLOCK);
      KafkaCompactionSourceResolver unusedSources =
          new KafkaCompactionSourceResolver(new UnusedCommittedSources());
      KafkaCompactionPartitionPass pass =
          new KafkaCompactionPartitionPass(
              CLUSTER,
              partition,
              ignored ->
                  CompletableFuture.completedFuture(
                      new KafkaCompactionPartitionPass.Capture(
                          binding,
                          plannerSnapshot(fixture.plan()),
                          fixture.outputTask().policy(),
                          new KafkaCompactionPartitionPass.PassOneInputs(
                              fixture.plan().passOneSnapshot().transactionStateEndOffset(),
                              100,
                              1 << 20,
                              1 << 20,
                              List.of(),
                              List.of(),
                              List.of()),
                          new KafkaCompactionPartitionPass.WriteSettings("test-build", false),
                          () -> CompletableFuture.completedFuture(null))),
              new KafkaCompactionPlanner(),
              unusedSources,
              new KafkaCompactionPlanCoordinator(planStore, tasks, CLOCK),
              planStore,
              tasks,
              batchSource,
              parquet,
              publication,
              new KafkaCompactionTerminalRetirer(planStore, tasks),
              new KafkaActivatedGenerationSetResolver(CLUSTER, generationMetadata),
              "b".repeat(26),
              () -> "a".repeat(26),
              new KafkaCompactionPartitionPass.Configuration(
                  Duration.ofSeconds(30),
                  Duration.ofSeconds(10),
                  Duration.ZERO,
                  Duration.ofSeconds(5),
                  3,
                  8,
                  32),
              scheduler,
              CLOCK,
              new KafkaCompactionPlanRecordMapper());

      KafkaCompactionPartitionPass.RunResult result = pass.runOnce().join();

      assertThat(result.outcome())
          .isEqualTo(KafkaCompactionPartitionPass.RunOutcome.PUBLISHED_AND_RETIRED);
      assertThat(result.publication()).isPresent();
      assertThat(result.retirement()).isPresent();
      assertThat(sourceCloses).hasValue(3);
      assertThat(generationPublications).hasValue(1);
      assertThat(
              partitions
                  .get(partition)
                  .join()
                  .orElseThrow()
                  .value()
                  .compactionCoverage()
                  .endOffset())
          .isEqualTo(2);
      assertThat(tasks.get(fixture.outputTask().streamId(), fixture.outputTask().taskId()).join())
          .isEmpty();
      assertThat(planStore.current()).isEmpty();
    } finally {
      scheduler.shutdownNow();
      partitions.close();
      generationMetadata.close();
    }
  }

  @Test
  void preservesTypedMaterializationFailureClassesAcrossTheAdapterBoundary() {
    assertThat(
            KafkaCompactionPartitionPass.classifyFailure(
                new TestMaterializationFailure(TaskFailureClass.SOURCE_CHANGED)))
        .isEqualTo(TaskFailureClass.SOURCE_CHANGED);
  }

  private static Fixture fixture() {
    ReadBatch output =
        readBatch(
            new OffsetRange(0, 2),
            bytes(
                MemoryRecords.withRecords(
                    0,
                    Compression.NONE,
                    new SimpleRecord(1_000, "k".getBytes(), "old".getBytes()),
                    new SimpleRecord(1_001, null, "unkeyed".getBytes()))),
            "partition-pass-output");
    ReadBatch tail =
        readBatch(
            new OffsetRange(2, 3),
            bytes(
                MemoryRecords.withRecords(
                    2,
                    Compression.NONE,
                    new SimpleRecord(1_002, "k".getBytes(), "new".getBytes()))),
            "partition-pass-tail");
    ExactSourceSet outputSources = sourceSet(List.of(output));
    ExactSourceSet decisionSources = sourceSet(List.of(output, tail));
    MaterializationPolicy materializationPolicy =
        MaterializationPolicyFactory.kafkaTopicCompacted(
            new TopicCompactionSpec(
                KafkaCompactionStrategyV1.STRATEGY_ID,
                KafkaCompactionStrategyV1.STRATEGY_VERSION,
                "KCK2"),
            2,
            128,
            1_048_576,
            1 << 20,
            1_024,
            "UNCOMPRESSED");
    MaterializationTask outputTask =
        MaterializationTask.create(
            new StreamId("stream-compaction-partition-pass"),
            output.range(),
            outputSources.sources(),
            materializationPolicy);
    Policy policy =
        new Policy(11, sha256('e'), 0, 10_000, 100, LogConfigHistoryEntry.CLEANUP_COMPACT_FLAG);
    Candidate candidate =
        new Candidate(output.range(), new OffsetRange(0, 3), 1, policy, Optional.empty(), 1_000);
    Snapshot snapshot =
        new Snapshot(
            candidate.outputCoverage(),
            candidate.decisionHorizon(),
            3,
            1_000,
            100,
            100,
            1 << 20,
            1 << 20,
            List.of(),
            List.of(),
            List.of());
    KafkaCompactionPlan plan =
        KafkaCompactionPlan.create(outputTask, 17, 2, 3, candidate, decisionSources, snapshot);
    return new Fixture(outputTask, plan, output, tail);
  }

  private static KafkaCompactionPlanner.Snapshot plannerSnapshot(KafkaCompactionPlan plan) {
    LogConfigHistoryEntry config =
        LogConfigHistoryEntry.create(
            11,
            0,
            1_024,
            60_000,
            0,
            1_024,
            64,
            -1,
            -1,
            0,
            100,
            0,
            10_000,
            0.5,
            LogConfigHistoryEntry.CLEANUP_COMPACT_FLAG);
    KafkaVirtualSegmentState state =
        new KafkaVirtualSegmentState(
            0,
            3,
            List.of(
                segment(config, 0, 2, 0, 100, 200, 1_001, 0, 100, SegmentState.CLOSED),
                segment(config, 2, 3, 1, 200, 0, 1_002, 100, 150, SegmentState.ACTIVE)),
            List.of(config));
    return new KafkaCompactionPlanner.Snapshot(
        state,
        KafkaCompactionPlanner.Policy.from(config),
        Optional.empty(),
        plan.lastStableOffset(),
        plan.highWatermark(),
        1_000);
  }

  private static VirtualSegment segment(
      LogConfigHistoryEntry config,
      long start,
      long end,
      long sequence,
      long createdAt,
      long closedAt,
      long largestTimestamp,
      long firstBytes,
      long lastBytes,
      SegmentState state) {
    return new VirtualSegment(
        start,
        end,
        sequence,
        createdAt,
        closedAt,
        0,
        largestTimestamp,
        end - 1,
        lastBytes - firstBytes,
        firstBytes,
        lastBytes,
        config.configDigest(),
        sequence == 0 ? RollReason.INITIAL : RollReason.SIZE,
        state);
  }

  private static ExactSourceRangeReader exactReader(Fixture fixture, AtomicInteger closes) {
    return (source, options) -> {
      ReadBatch original = source.range().startOffset() == 0 ? fixture.output() : fixture.tail();
      ReadBatch exact =
          new ReadBatch(
              source.range(),
              source.payloadFormat(),
              original.payload(),
              source.schemaRefs(),
              source.projectionRef(),
              new ReadSourceRef(
                  source.range(),
                  source.generation(),
                  source.commitVersion(),
                  source.readTarget(),
                  source.targetIdentitySha256()));
      return CompletableFuture.completedFuture(
          new ExactSourceRead() {
            @Override
            public SourceGeneration source() {
              return source;
            }

            @Override
            public Flow.Publisher<ReadBatch> batches() {
              return publisher(List.of(exact));
            }

            @Override
            public CompletableFuture<ExactSourceReadSummary> completion() {
              return CompletableFuture.completedFuture(
                  new ExactSourceReadSummary(
                      source.range(),
                      source.recordCount(),
                      source.entryCount(),
                      source.logicalBytes(),
                      sha256('f')));
            }

            @Override
            public void close() {
              closes.incrementAndGet();
            }
          });
    };
  }

  private static ExactSourceSet sourceSet(List<ReadBatch> batches) {
    ArrayList<SourceGeneration> sources = new ArrayList<>();
    long cumulativeBytes = 0;
    for (ReadBatch batch : batches) {
      long nextCumulative = Math.addExact(cumulativeBytes, batch.payload().length);
      sources.add(
          new SourceGeneration(
              ReadView.COMMITTED,
              batch.source().resolvedRange(),
              batch.source().generation(),
              batch.source().commitVersion(),
              "test/f9-compaction-partition-pass/" + batch.range().startOffset(),
              batch.range().startOffset(),
              sha256('d'),
              batch.source().target(),
              batch.source().targetIdentity(),
              Optional.empty(),
              batch.payloadFormat(),
              batch.projectionRef(),
              Math.toIntExact(batch.range().recordCount()),
              1,
              batch.payload().length,
              batch.schemaRefs(),
              cumulativeBytes,
              nextCumulative));
      cumulativeBytes = nextCumulative;
    }
    return ExactSourceSet.create(
        ReadView.COMMITTED,
        new OffsetRange(
            batches.getFirst().range().startOffset(), batches.getLast().range().endOffset()),
        sources);
  }

  private static VersionedKafkaPartitionBinding createActiveBinding(
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
              task.coverage().startOffset(),
              Math.max(1_200, current.value().updatedAtMillis() + 1));
      current = store.compareAndSet(current, observed).join();
    }
    return current;
  }

  private static ReadOptions readOptions() {
    return new ReadOptions(64, 1 << 20, ReadIsolation.COMMITTED, Duration.ofSeconds(10));
  }

  private static <T> Flow.Publisher<T> publisher(List<T> values) {
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
                while (!done && remaining-- > 0 && index < values.size()) {
                  subscriber.onNext(values.get(index++));
                }
                if (index == values.size() && !done) {
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

  private static Checksum sha256(char value) {
    return new Checksum(ChecksumType.SHA256, Character.toString(value).repeat(64));
  }

  private static String topicId(int value) {
    byte[] bytes = new byte[16];
    bytes[15] = (byte) value;
    return Base64.getUrlEncoder().withoutPadding().encodeToString(bytes);
  }

  private record Fixture(
      MaterializationTask outputTask, KafkaCompactionPlan plan, ReadBatch output, ReadBatch tail) {}

  private static final class UnusedCommittedSources implements CommittedSourceSetResolver {
    @Override
    public CompletableFuture<CommittedSourceSetResolution> resolve(
        StreamId streamId, OffsetRange coverage) {
      return CompletableFuture.failedFuture(
          new AssertionError("existing task recovery must not resolve fresh sources"));
    }

    @Override
    public CompletableFuture<Void> revalidate(CommittedSourceSetResolution expected) {
      return CompletableFuture.failedFuture(
          new AssertionError("existing task recovery must not revalidate fresh sources"));
    }
  }

  private static final class TestMaterializationFailure extends RuntimeException
      implements MaterializationFailure {
    private final TaskFailureClass failureClass;

    private TestMaterializationFailure(TaskFailureClass failureClass) {
      this.failureClass = failureClass;
    }

    @Override
    public TaskFailureClass failureClass() {
      return failureClass;
    }
  }

  private static final class FakePlanStore implements KafkaCompactionPlanMetadataStore {
    private final KafkaPartitionId partition;
    private VersionedKafkaCompactionPlan current;

    private FakePlanStore(KafkaPartitionId partition, KafkaCompactionPlan plan) {
      this.partition = partition;
      KafkaCompactionPlanRecord record =
          new KafkaCompactionPlanRecordMapper().toRecord(partition, plan, 1_000);
      this.current =
          new VersionedKafkaCompactionPlan(
              "plans/" + record.materializationTaskId(),
              record.withMetadataVersion(1),
              1,
              sha256('9'));
    }

    @Override
    public CompletableFuture<Optional<VersionedKafkaCompactionPlan>> getCompactionPlan(
        KafkaPartitionId id, String materializationTaskId) {
      if (!id.equals(partition)
          || current != null
              && !current.value().materializationTaskId().equals(materializationTaskId)) {
        return CompletableFuture.completedFuture(Optional.empty());
      }
      return CompletableFuture.completedFuture(Optional.ofNullable(current));
    }

    @Override
    public CompletableFuture<VersionedKafkaCompactionPlan> putCompactionPlanIfAbsent(
        KafkaCompactionPlanRecord value) {
      if (current == null) {
        current =
            new VersionedKafkaCompactionPlan(
                "plans/" + value.materializationTaskId(),
                value.withMetadataVersion(1),
                1,
                sha256('9'));
      }
      return CompletableFuture.completedFuture(current);
    }

    @Override
    public CompletableFuture<KafkaCompactionPlanScanPage> scanCompactionPlans(
        KafkaPartitionId id, Optional<KafkaCompactionPlanScanToken> continuation, int limit) {
      if (!id.equals(partition) || continuation.isPresent() || limit <= 0) {
        return CompletableFuture.failedFuture(
            new IllegalArgumentException("invalid fake plan scan"));
      }
      return CompletableFuture.completedFuture(
          new KafkaCompactionPlanScanPage(
              current == null ? List.of() : List.of(current), Optional.empty()));
    }

    @Override
    public CompletableFuture<Void> deleteCompactionPlan(VersionedKafkaCompactionPlan expected) {
      if (!expected.equals(current)) {
        return CompletableFuture.failedFuture(new IllegalStateException("stale plan delete"));
      }
      current = null;
      return CompletableFuture.completedFuture(null);
    }

    private Optional<VersionedKafkaCompactionPlan> current() {
      return Optional.ofNullable(current);
    }
  }
}
