/* Licensed under the Apache License, Version 2.0 */
package com.nereusstream.materialization;

import static com.nereusstream.materialization.MaterializationPlannerTestSupport.CLUSTER;
import static com.nereusstream.materialization.MaterializationPlannerTestSupport.STREAM;
import static org.assertj.core.api.Assertions.assertThat;

import com.nereusstream.api.Checksum;
import com.nereusstream.api.ChecksumType;
import com.nereusstream.api.ErrorCode;
import com.nereusstream.api.NereusException;
import com.nereusstream.api.OffsetRange;
import com.nereusstream.api.ReadBatch;
import com.nereusstream.api.ReadOptions;
import com.nereusstream.api.ReadView;
import com.nereusstream.api.target.ObjectSliceReadTarget;
import com.nereusstream.core.physical.ObjectProtection;
import com.nereusstream.core.physical.ObjectProtectionManager;
import com.nereusstream.core.physical.ObjectProtectionOwner;
import com.nereusstream.core.physical.ObjectProtectionRequest;
import com.nereusstream.core.physical.PhysicalObjectIdentity;
import com.nereusstream.core.physical.PhysicalObjectKind;
import com.nereusstream.metadata.oxia.GenerationMetadataStore;
import com.nereusstream.metadata.oxia.GenerationMetadataStoreTestFactory;
import com.nereusstream.metadata.oxia.ObjectProtectionIdentity;
import com.nereusstream.metadata.oxia.VersionedGenerationCandidate;
import com.nereusstream.metadata.oxia.records.TaskLifecycle;
import com.nereusstream.metadata.oxia.records.TaskFailureClass;
import com.nereusstream.objectstore.compacted.CompactedObjectVerifier;
import com.nereusstream.objectstore.compacted.ParquetCompactedObjectReader;
import com.nereusstream.objectstore.compacted.ParquetCompactedObjectWriter;
import com.nereusstream.objectstore.staging.StagingFileManager;
import com.nereusstream.objectstore.testing.LocalFileObjectStore;
import java.nio.file.Files;
import java.nio.file.Path;
import java.nio.file.attribute.PosixFilePermissions;
import java.nio.ByteBuffer;
import java.time.Clock;
import java.time.Duration;
import java.time.Instant;
import java.time.ZoneOffset;
import java.util.ArrayList;
import java.util.List;
import java.util.Optional;
import java.util.OptionalLong;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.Executors;
import java.util.concurrent.Flow;
import java.util.concurrent.ScheduledExecutorService;
import java.util.concurrent.atomic.AtomicInteger;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.io.TempDir;

class MaterializationWorkerTest {
    private static final Clock CLOCK = Clock.fixed(Instant.ofEpochMilli(1_000), ZoneOffset.UTC);

    @TempDir
    Path temporaryDirectory;

    @Test
    void claimsStreamsUploadsVerifiesProtectsAndFreezesOutputReady() throws Exception {
        List<VersionedGenerationCandidate> candidates = List.of(
                MaterializationPlannerTestSupport.zero("/index/worker-2", 0, 2, 0, 100, 2),
                MaterializationPlannerTestSupport.zero("/index/worker-4", 2, 4, 100, 100, 4));
        MaterializationTask task = MaterializationPlannerTestSupport.planner(
                        candidates, List.of(), 0, 4)
                .plan(
                        STREAM,
                        new OffsetRange(0, 4),
                        MaterializationPlannerTestSupport.policy(),
                        1)
                .join()
                .get(0);
        GenerationMetadataStore durable = GenerationMetadataStoreTestFactory.inMemory(CLOCK);
        GenerationMetadataStore generations = MaterializationPlannerTestSupport.generationStore(
                candidates, List.of(), durable);
        MaterializationTaskStore taskStore = new MaterializationTaskStore(CLUSTER, generations, CLOCK);
        taskStore.create(task).join();
        TrackingProtections protections = new TrackingProtections();
        TrackingExactReader exactReader = new TrackingExactReader();
        Path stagingPath = Files.createDirectory(temporaryDirectory.resolve("staging"));
        Files.setPosixFilePermissions(stagingPath, PosixFilePermissions.fromString("rwx------"));
        ScheduledExecutorService scheduler = Executors.newSingleThreadScheduledExecutor();
        try (StagingFileManager staging = new StagingFileManager(
                        stagingPath,
                        32L << 20,
                        StagingFileManager.MIN_UPLOAD_CHUNK_BYTES,
                        Duration.ofHours(1),
                        Runnable::run);
                LocalFileObjectStore objects = new LocalFileObjectStore(
                        temporaryDirectory.resolve("objects"))) {
            var parquetReader = new ParquetCompactedObjectReader(objects, Runnable::run);
            var verifier = new DefaultMaterializationOutputVerifier(
                    objects,
                    new CompactedMaterializationFormatVerifier(
                            new CompactedObjectVerifier(objects, parquetReader)));
            DefaultMaterializationWorker worker = new DefaultMaterializationWorker(
                    CLUSTER,
                    "r".repeat(26),
                    taskStore,
                    generations,
                    (target, view) -> CompletableFuture.completedFuture(sourceIdentity(target)),
                    protections,
                    ignored -> exactReader,
                    new ParquetCompactedObjectWriter(staging, Runnable::run),
                    objects,
                    verifier,
                    () -> "c".repeat(26),
                    1,
                    64 * 1024,
                    Duration.ofSeconds(30),
                    Duration.ofSeconds(5),
                    Duration.ofSeconds(1),
                    3,
                    Duration.ofSeconds(10),
                    "worker-test",
                    scheduler,
                    Runnable::run,
                    CLOCK);

            MaterializationOutput output = worker.execute(task).join();

            var durableReady = taskStore.get(STREAM, task.taskId()).join().orElseThrow();
            assertThat(durableReady.value().lifecycle()).isEqualTo(TaskLifecycle.OUTPUT_READY);
            assertThat(durableReady.value().workerClaim()).isEmpty();
            assertThat(durableReady.value().output()).contains(
                    MaterializationRecordMapper.outputRecord(output));
            assertThat(output.outputAttemptId()).isEqualTo("c".repeat(26));
            assertThat(output.sourceRecordCount()).isEqualTo(4);
            assertThat(output.outputRecordCount()).isEqualTo(4);
            assertThat(output.logicalBytes()).isEqualTo(200);
            assertThat(objects.headObject(
                            output.objectKey(),
                            new com.nereusstream.objectstore.HeadObjectOptions(Duration.ofSeconds(1)))
                    .join()
                    .checksum()).isEqualTo(output.storageCrc32c());
            assertThat(exactReader.maxActive).hasValue(1);
            assertThat(exactReader.completedSources).hasValue(2);
            assertThat(protections.acquired).hasValue(3);
            assertThat(protections.transferred).hasValue(5);
            assertThat(protections.released).hasValue(0);
            assertThat(staging.reservedBytes()).isZero();
        } finally {
            scheduler.shutdownNow();
        }
    }

    @Test
    void releasesSourceProtectionsAndPersistsRetryAfterWriterFailure() {
        List<VersionedGenerationCandidate> candidates = List.of(
                MaterializationPlannerTestSupport.zero("/index/fail-2", 0, 2, 0, 100, 2),
                MaterializationPlannerTestSupport.zero("/index/fail-4", 2, 4, 100, 100, 4));
        MaterializationTask task = MaterializationPlannerTestSupport.planner(
                        candidates, List.of(), 0, 4)
                .plan(
                        STREAM,
                        new OffsetRange(0, 4),
                        MaterializationPlannerTestSupport.policy(),
                        1)
                .join()
                .get(0);
        GenerationMetadataStore durable = GenerationMetadataStoreTestFactory.inMemory(CLOCK);
        GenerationMetadataStore generations = MaterializationPlannerTestSupport.generationStore(
                candidates, List.of(), durable);
        MaterializationTaskStore taskStore = new MaterializationTaskStore(CLUSTER, generations, CLOCK);
        taskStore.create(task).join();
        TrackingProtections protections = new TrackingProtections();
        TrackingExactReader exactReader = new TrackingExactReader();
        ScheduledExecutorService scheduler = Executors.newSingleThreadScheduledExecutor();
        try (LocalFileObjectStore objects = new LocalFileObjectStore(
                temporaryDirectory.resolve("failed-objects"))) {
            DefaultMaterializationWorker worker = new DefaultMaterializationWorker(
                    CLUSTER,
                    "r".repeat(26),
                    taskStore,
                    generations,
                    (target, view) -> CompletableFuture.completedFuture(sourceIdentity(target)),
                    protections,
                    ignored -> exactReader,
                    (request, rows) -> CompletableFuture.failedFuture(new NereusException(
                            ErrorCode.OBJECT_UPLOAD_FAILED,
                            true,
                            "injected writer failure")),
                    objects,
                    (ignoredTask, ignoredOutput, ignoredTimeout) ->
                            CompletableFuture.completedFuture(null),
                    () -> "c".repeat(26),
                    1,
                    64 * 1024,
                    Duration.ofSeconds(30),
                    Duration.ofSeconds(5),
                    Duration.ofSeconds(1),
                    3,
                    Duration.ofSeconds(10),
                    "worker-test",
                    scheduler,
                    Runnable::run,
                    CLOCK);

            org.assertj.core.api.Assertions.assertThatThrownBy(() -> worker.execute(task).join())
                    .hasRootCauseMessage("injected writer failure");

            var retry = taskStore.get(STREAM, task.taskId()).join().orElseThrow().value();
            assertThat(retry.lifecycle()).isEqualTo(TaskLifecycle.RETRY_WAIT);
            assertThat(retry.failureClassId())
                    .isEqualTo(TaskFailureClass.RETRYABLE_OBJECT_STORE.wireId());
            assertThat(retry.workerClaim()).isEmpty();
            assertThat(retry.output()).isEmpty();
            assertThat(protections.acquired).hasValue(2);
            assertThat(protections.released).hasValue(2);
            assertThat(exactReader.completedSources).hasValue(0);
        } finally {
            scheduler.shutdownNow();
        }
    }

    @Test
    void executesTwoPassTopicCompactionAndFreezesSparseNtc1Output() throws Exception {
        List<VersionedGenerationCandidate> candidates = List.of(
                MaterializationPlannerTestSupport.zero("/index/topic-worker-2", 0, 2, 0, 100, 2),
                MaterializationPlannerTestSupport.zero("/index/topic-worker-4", 2, 4, 100, 100, 4));
        MaterializationPolicy policy = MaterializationPolicyFactory.topicCompacted(
                new TopicCompactionSpec("latest", 1, "worker-key-v1"),
                2,
                16,
                1_000,
                1_000_000,
                128,
                "ZSTD");
        MaterializationTask task = MaterializationPlannerTestSupport.planner(
                        candidates, List.of(), 0, 4)
                .plan(STREAM, new OffsetRange(0, 4), policy, 1)
                .join()
                .get(0);
        GenerationMetadataStore durable = GenerationMetadataStoreTestFactory.inMemory(CLOCK);
        GenerationMetadataStore generations = MaterializationPlannerTestSupport.generationStore(
                candidates, List.of(), durable);
        MaterializationTaskStore taskStore = new MaterializationTaskStore(CLUSTER, generations, CLOCK);
        taskStore.create(task).join();
        TrackingProtections protections = new TrackingProtections();
        TrackingExactReader exactReader = new TrackingExactReader();
        Path stagingPath = Files.createDirectory(temporaryDirectory.resolve("topic-staging"));
        Files.setPosixFilePermissions(stagingPath, PosixFilePermissions.fromString("rwx------"));
        ScheduledExecutorService scheduler = Executors.newSingleThreadScheduledExecutor();
        try (StagingFileManager staging = new StagingFileManager(
                        stagingPath,
                        32L << 20,
                        StagingFileManager.MIN_UPLOAD_CHUNK_BYTES,
                        Duration.ofHours(1),
                        Runnable::run);
                LocalFileObjectStore objects = new LocalFileObjectStore(
                        temporaryDirectory.resolve("topic-objects"))) {
            var parquetReader = new ParquetCompactedObjectReader(objects, Runnable::run);
            var verifier = new DefaultMaterializationOutputVerifier(
                    objects,
                    new CompactedMaterializationFormatVerifier(
                            new CompactedObjectVerifier(objects, parquetReader)));
            TopicCompactionDecoder decoder = new TopicCompactionDecoder() {
                @Override
                public String id() {
                    return "worker-key-v1";
                }

                @Override
                public Optional<CompactionRecord> decode(long offset, ByteBuffer exactPayload) {
                    if (offset == 1) {
                        return Optional.empty();
                    }
                    byte key = offset == 0 || offset == 2 ? (byte) 'a' : (byte) 'b';
                    return Optional.of(new CompactionRecord(
                            offset,
                            ByteBuffer.wrap(new byte[] {key}),
                            offset == 3
                                    ? CompactionDisposition.TOMBSTONE
                                    : CompactionDisposition.VALUE,
                            OptionalLong.of(10_000 + offset),
                            OptionalLong.empty()));
                }
            };
            TopicCompactionStrategy strategy = new TopicCompactionStrategy() {
                @Override
                public String id() {
                    return "latest";
                }

                @Override
                public long version() {
                    return 1;
                }

                @Override
                public boolean retainTombstone(
                        CompactionRecord tombstone,
                        long planningTimeMillis) {
                    return true;
                }
            };
            TopicCompactionRegistry registry = new TopicCompactionRegistry(
                    List.of(decoder), List.of(strategy));
            DefaultMaterializationWorker worker = new DefaultMaterializationWorker(
                    CLUSTER,
                    "r".repeat(26),
                    taskStore,
                    generations,
                    (target, view) -> CompletableFuture.completedFuture(sourceIdentity(target)),
                    protections,
                    ignored -> exactReader,
                    new ParquetCompactedObjectWriter(staging, Runnable::run),
                    objects,
                    verifier,
                    new DefaultTopicCompactionEngine(
                            staging,
                            DefaultTopicCompactionEngine.MIN_IN_MEMORY_KEY_BYTES,
                            1024,
                            4,
                            Runnable::run),
                    registry,
                    () -> "c".repeat(26),
                    1,
                    64 * 1024,
                    Duration.ofSeconds(30),
                    Duration.ofSeconds(5),
                    Duration.ofSeconds(1),
                    3,
                    Duration.ofSeconds(10),
                    "topic-worker-test",
                    scheduler,
                    Runnable::run,
                    CLOCK);

            MaterializationOutput output = worker.execute(task).join();

            assertThat(output.view()).isEqualTo(ReadView.TOPIC_COMPACTED);
            assertThat(output.physicalFormat())
                    .isEqualTo(MaterializationPolicy.TOPIC_COMPACTED_FORMAT);
            assertThat(output.sourceRecordCount()).isEqualTo(4);
            assertThat(output.outputRecordCount()).isEqualTo(3);
            assertThat(taskStore.get(STREAM, task.taskId()).join().orElseThrow()
                            .value().lifecycle())
                    .isEqualTo(TaskLifecycle.OUTPUT_READY);
            assertThat(exactReader.completedSources).hasValue(4);
            assertThat(exactReader.maxActive).hasValue(1);
            assertThat(protections.acquired).hasValue(3);
            assertThat(staging.reservedBytes()).isZero();
        } finally {
            scheduler.shutdownNow();
        }
    }

    private static PhysicalObjectIdentity sourceIdentity(ObjectSliceReadTarget target) {
        return PhysicalObjectIdentity.create(
                target.objectKey(),
                Optional.of(target.objectId()),
                PhysicalObjectKind.OBJECT_WAL,
                100,
                new Checksum(ChecksumType.CRC32C, "11223344"),
                Optional.empty(),
                Optional.empty());
    }

    private static final class TrackingExactReader implements ExactSourceRangeReader {
        private final AtomicInteger active = new AtomicInteger();
        private final AtomicInteger maxActive = new AtomicInteger();
        private final AtomicInteger completedSources = new AtomicInteger();

        @Override
        public CompletableFuture<ExactSourceRead> read(
                SourceGeneration source,
                ReadOptions options) {
            int now = active.incrementAndGet();
            maxActive.accumulateAndGet(now, Math::max);
            return CompletableFuture.completedFuture(new ExactSourceRead() {
                private final CompletableFuture<ExactSourceReadSummary> completion =
                        new CompletableFuture<>();
                private boolean closed;

                @Override
                public SourceGeneration source() {
                    return source;
                }

                @Override
                public Flow.Publisher<ReadBatch> batches() {
                    return subscriber -> subscriber.onSubscribe(new Flow.Subscription() {
                        private long cursor = source.range().startOffset();
                        private boolean terminal;

                        @Override
                        public void request(long count) {
                            if (terminal) {
                                return;
                            }
                            if (count <= 0) {
                                terminal = true;
                                subscriber.onError(new IllegalArgumentException("demand"));
                                return;
                            }
                            long emitted = 0;
                            while (!terminal
                                    && emitted < count
                                    && cursor < source.range().endOffset()) {
                                ObjectSliceReadTarget target =
                                        (ObjectSliceReadTarget) source.readTarget();
                                byte[] payload = new byte[50];
                                java.util.Arrays.fill(payload, (byte) (cursor + 1));
                                subscriber.onNext(new ReadBatch(
                                        new OffsetRange(cursor, cursor + 1),
                                        source.payloadFormat(),
                                        payload,
                                        source.schemaRefs(),
                                        target.entryIndexRef(),
                                        source.projectionRef(),
                                        target.objectId(),
                                        target.objectOffset(),
                                        target.objectLength()));
                                cursor++;
                                emitted++;
                            }
                            if (!terminal && cursor == source.range().endOffset()) {
                                terminal = true;
                                active.decrementAndGet();
                                completedSources.incrementAndGet();
                                completion.complete(new ExactSourceReadSummary(
                                        source.range(),
                                        source.recordCount(),
                                        source.entryCount(),
                                        source.logicalBytes(),
                                        MaterializationPlannerTestSupport.sha('a')));
                                subscriber.onComplete();
                            }
                        }

                        @Override
                        public void cancel() {
                            terminal = true;
                        }
                    });
                }

                @Override
                public CompletableFuture<ExactSourceReadSummary> completion() {
                    return completion;
                }

                @Override
                public void close() {
                    closed = true;
                }
            });
        }
    }

    private static final class TrackingProtections implements ObjectProtectionManager {
        private final AtomicInteger acquired = new AtomicInteger();
        private final AtomicInteger transferred = new AtomicInteger();
        private final AtomicInteger released = new AtomicInteger();
        private final AtomicInteger version = new AtomicInteger();

        @Override
        public CompletableFuture<ObjectProtection> acquire(
                ObjectProtectionRequest request,
                OwnerRevalidator ownerRevalidator) {
            return ownerRevalidator.revalidate(request.owner()).thenApply(ignored -> {
                acquired.incrementAndGet();
                return protection(request.object(), request.identity(), request.owner(), version.getAndIncrement());
            });
        }

        @Override
        public CompletableFuture<ObjectProtection> acquireOrTransfer(
                ObjectProtectionRequest request,
                OwnerRevalidator ownerRevalidator) {
            return acquire(request, ownerRevalidator);
        }

        @Override
        public CompletableFuture<ObjectProtection> revalidate(
                ObjectProtection protection,
                OwnerRevalidator ownerRevalidator) {
            return ownerRevalidator.revalidate(protection.owner()).thenApply(ignored -> protection);
        }

        @Override
        public CompletableFuture<ObjectProtection> transfer(
                ObjectProtection protection,
                ObjectProtectionOwner newOwner,
                OwnerRevalidator newOwnerRevalidator) {
            return newOwnerRevalidator.revalidate(newOwner).thenApply(ignored -> {
                transferred.incrementAndGet();
                return protection(
                        protection.object(), protection.identity(), newOwner, version.getAndIncrement());
            });
        }

        @Override
        public CompletableFuture<Void> release(
                ObjectProtection protection,
                RemovalAuthorizer removalAuthorizer) {
            return removalAuthorizer.authorizeRemoval(protection).thenRun(released::incrementAndGet);
        }

        @Override
        public void close() {
        }

        private static ObjectProtection protection(
                PhysicalObjectIdentity object,
                ObjectProtectionIdentity identity,
                ObjectProtectionOwner owner,
                long version) {
            return new ObjectProtection(
                    object,
                    identity,
                    owner,
                    1,
                    1_000,
                    0,
                    version,
                    new Checksum(ChecksumType.SHA256, "a".repeat(64)));
        }
    }
}
