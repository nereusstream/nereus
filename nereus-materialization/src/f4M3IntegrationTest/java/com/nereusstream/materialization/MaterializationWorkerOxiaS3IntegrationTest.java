/* Licensed under the Apache License, Version 2.0 */
package com.nereusstream.materialization;

import static org.assertj.core.api.Assertions.assertThat;

import com.nereusstream.api.AppendSessionOptions;
import com.nereusstream.api.Checksum;
import com.nereusstream.api.ChecksumType;
import com.nereusstream.api.EntryIndexLocation;
import com.nereusstream.api.EntryIndexRef;
import com.nereusstream.api.ObjectId;
import com.nereusstream.api.ObjectKey;
import com.nereusstream.api.ObjectKeyHash;
import com.nereusstream.api.ObjectType;
import com.nereusstream.api.PayloadFormat;
import com.nereusstream.api.ProjectionRef;
import com.nereusstream.api.ProjectionType;
import com.nereusstream.api.PublicationId;
import com.nereusstream.api.ReadBatch;
import com.nereusstream.api.ReadOptions;
import com.nereusstream.api.ReadView;
import com.nereusstream.api.ResolvedRange;
import com.nereusstream.api.StorageProfile;
import com.nereusstream.api.StreamCreateOptions;
import com.nereusstream.api.StreamId;
import com.nereusstream.api.StreamName;
import com.nereusstream.api.target.ObjectSliceReadTarget;
import com.nereusstream.core.append.DefaultGenerationZeroPhysicalReferencePublisher;
import com.nereusstream.core.append.GenerationZeroPhysicalReferencePublisher;
import com.nereusstream.core.append.ProtectedStableAppend;
import com.nereusstream.core.capability.GenerationActivationProof;
import com.nereusstream.core.capability.GenerationOperation;
import com.nereusstream.core.capability.GenerationProtocolActivationGuard;
import com.nereusstream.core.physical.DefaultObjectProtectionManager;
import com.nereusstream.core.physical.DefaultObjectReadPinManager;
import com.nereusstream.core.physical.ObjectProtectionManager;
import com.nereusstream.core.physical.ObjectReadPinManager;
import com.nereusstream.core.physical.PhysicalObjectIdentity;
import com.nereusstream.core.physical.PhysicalObjectKind;
import com.nereusstream.core.read.MetadataPhysicalObjectIdentityResolver;
import com.nereusstream.core.read.PhysicalObjectIdentityResolver;
import com.nereusstream.core.read.ReadTargetDispatcher;
import com.nereusstream.core.read.ReadTargetReader;
import com.nereusstream.core.read.ReadTargetReaderKey;
import com.nereusstream.core.read.ReadTargetReaderRegistry;
import com.nereusstream.metadata.oxia.CommitAppendRequest;
import com.nereusstream.metadata.oxia.F4MetadataConditionFailedException;
import com.nereusstream.metadata.oxia.GenerationIndexIdentity;
import com.nereusstream.metadata.oxia.GenerationMetadataStore;
import com.nereusstream.metadata.oxia.ObjectProtectionScanPage;
import com.nereusstream.metadata.oxia.OxiaClientConfiguration;
import com.nereusstream.metadata.oxia.OxiaJavaClientMetadataStore;
import com.nereusstream.metadata.oxia.OxiaJavaGenerationMetadataStore;
import com.nereusstream.metadata.oxia.OxiaJavaPhysicalObjectMetadataStore;
import com.nereusstream.metadata.oxia.ProjectionIdentity;
import com.nereusstream.metadata.oxia.SharedOxiaClientRuntime;
import com.nereusstream.metadata.oxia.StableAppendResult;
import com.nereusstream.metadata.oxia.VersionedGenerationIndex;
import com.nereusstream.metadata.oxia.VersionedMaterializationTask;
import com.nereusstream.metadata.oxia.records.EntryIndexReferenceRecord;
import com.nereusstream.metadata.oxia.records.GenerationLifecycle;
import com.nereusstream.metadata.oxia.records.MaterializationStreamRegistrationRecord;
import com.nereusstream.metadata.oxia.records.MaterializationTaskRecord;
import com.nereusstream.metadata.oxia.records.ObjectManifestRecord;
import com.nereusstream.metadata.oxia.records.PhysicalObjectLifecycle;
import com.nereusstream.metadata.oxia.records.PhysicalObjectRootRecord;
import com.nereusstream.metadata.oxia.records.StreamSliceManifestRecord;
import com.nereusstream.metadata.oxia.records.TaskLifecycle;
import com.nereusstream.objectstore.Crc32cChecksums;
import com.nereusstream.objectstore.ObjectStore;
import com.nereusstream.objectstore.ObjectStoreConfiguration;
import com.nereusstream.objectstore.ObjectStoreSecretResolver;
import com.nereusstream.objectstore.PutObjectOptions;
import com.nereusstream.objectstore.PutObjectResult;
import com.nereusstream.objectstore.RangeReadOptions;
import com.nereusstream.objectstore.S3CompatibleObjectStoreProvider;
import com.nereusstream.objectstore.compacted.CompactedObjectReadRequest;
import com.nereusstream.objectstore.compacted.CompactedObjectReadResult;
import com.nereusstream.objectstore.compacted.CompactedObjectVerifier;
import com.nereusstream.objectstore.compacted.ParquetCompactedObjectReader;
import com.nereusstream.objectstore.compacted.ParquetCompactedObjectWriter;
import com.nereusstream.objectstore.staging.StagingFileManager;
import com.nereusstream.objectstore.wal.WalReadResult;
import com.nereusstream.objectstore.wal.WalSliceReadStats;
import io.oxia.testcontainers.OxiaContainer;
import java.lang.reflect.InvocationTargetException;
import java.lang.reflect.Proxy;
import java.nio.ByteBuffer;
import java.nio.charset.StandardCharsets;
import java.nio.file.Files;
import java.nio.file.Path;
import java.nio.file.attribute.PosixFilePermissions;
import java.security.MessageDigest;
import java.time.Clock;
import java.time.Duration;
import java.util.ArrayList;
import java.util.HexFormat;
import java.util.List;
import java.util.Map;
import java.util.Optional;
import java.util.UUID;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.Executors;
import java.util.concurrent.ScheduledExecutorService;
import java.util.concurrent.atomic.AtomicBoolean;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.io.TempDir;
import org.testcontainers.containers.localstack.LocalStackContainer;
import org.testcontainers.junit.jupiter.Container;
import org.testcontainers.junit.jupiter.Testcontainers;
import org.testcontainers.utility.DockerImageName;
import software.amazon.awssdk.auth.credentials.AwsBasicCredentials;
import software.amazon.awssdk.auth.credentials.StaticCredentialsProvider;
import software.amazon.awssdk.regions.Region;
import software.amazon.awssdk.services.s3.S3AsyncClient;
import software.amazon.awssdk.services.s3.S3Configuration;
import software.amazon.awssdk.services.s3.model.CreateBucketRequest;

@Testcontainers
class MaterializationWorkerOxiaS3IntegrationTest {
    private static final Clock CLOCK = Clock.systemUTC();
    private static final Duration TIMEOUT = Duration.ofSeconds(30);
    private static final DockerImageName OXIA_IMAGE =
            DockerImageName.parse("oxia/oxia:0.16.3");
    private static final DockerImageName LOCALSTACK_IMAGE =
            DockerImageName.parse("localstack/localstack:4.14.0");

    @Container
    private static final OxiaContainer OXIA = new OxiaContainer(OXIA_IMAGE).withShards(4);

    @Container
    private static final LocalStackContainer LOCALSTACK = new LocalStackContainer(LOCALSTACK_IMAGE)
            .withServices(LocalStackContainer.Service.S3);

    @TempDir
    Path temporaryDirectory;

    @Test
    void twoIndependentWorkersConvergeThenRestartPublishesExactParquet() throws Exception {
        try (Fixture fixture = Fixture.create()) {
            MaterializationOutput output;
            try (Process first = Process.open(
                            fixture,
                            "r".repeat(26),
                            "c".repeat(26),
                            temporaryDirectory.resolve("first"),
                            store -> store);
                    Process second = Process.open(
                            fixture,
                            "s".repeat(26),
                            "d".repeat(26),
                            temporaryDirectory.resolve("second"),
                            store -> store)) {
                CompletableFuture<MaterializationOutput> left =
                        first.worker.execute(fixture.task);
                CompletableFuture<MaterializationOutput> right =
                        second.worker.execute(fixture.task);
                MaterializationOutput leftOutput = left.handle((value, failure) -> value).join();
                MaterializationOutput rightOutput = right.handle((value, failure) -> value).join();
                assertThat(leftOutput != null || rightOutput != null).isTrue();
                output = leftOutput != null ? leftOutput : rightOutput;
                if (leftOutput != null && rightOutput != null) {
                    assertThat(rightOutput).isEqualTo(leftOutput);
                }
                MaterializationTaskRecord ready = first.task().value();
                assertThat(ready.lifecycle()).isEqualTo(TaskLifecycle.OUTPUT_READY);
                assertThat(ready.attempt()).isEqualTo(1);
                assertThat(ready.output())
                        .contains(MaterializationRecordMapper.outputRecord(output));
                first.assertNoReaderLeases();
                second.assertNoReaderLeases();
            }

            try (Process restarted = Process.open(
                    fixture,
                    "t".repeat(26),
                    "e".repeat(26),
                    temporaryDirectory.resolve("restarted"),
                    store -> store)) {
                MaterializationOutput recovered = restarted.worker.execute(fixture.task).join();
                assertThat(recovered).isEqualTo(output);
                assertThat(restarted.task().value().attempt()).isEqualTo(1);

                GenerationCommitResult committed = restarted.committer()
                        .publish(fixture.task, recovered)
                        .join();
                VersionedGenerationIndex index = restarted.generations.getIndex(
                                fixture.cluster,
                                new GenerationIndexIdentity(
                                        fixture.stream,
                                        ReadView.COMMITTED,
                                        committed.coverage().endOffset(),
                                        committed.generation().value()))
                        .join()
                        .orElseThrow();
                assertThat(index.value().lifecycle()).isEqualTo(GenerationLifecycle.COMMITTED);
                assertThat(restarted.task().value().lifecycle()).isEqualTo(TaskLifecycle.PUBLISHED);
                restarted.assertExactOutput(recovered, fixture.sourcePayloads);
                ObjectProtectionScanPage outputProtections = restarted.physical.scanProtections(
                                fixture.cluster,
                                recovered.objectKeyHash(),
                                Optional.empty(),
                                10)
                        .join();
                assertThat(outputProtections.values()).hasSizeGreaterThanOrEqualTo(2);
            }
        }
    }

    @Test
    void lostOutputReadyResponseConvergesAndFreshProcessReusesTheOutput() throws Exception {
        AtomicBoolean loseOutputReady = new AtomicBoolean(true);
        try (Fixture fixture = Fixture.create();
                Process interrupted = Process.open(
                        fixture,
                        "u".repeat(26),
                        "f".repeat(26),
                        temporaryDirectory.resolve("lost-output"),
                        store -> loseOutputReadyResponse(store, loseOutputReady))) {
            MaterializationOutput first = interrupted.worker.execute(fixture.task).join();
            assertThat(loseOutputReady).isFalse();
            assertThat(interrupted.task().value().lifecycle())
                    .isEqualTo(TaskLifecycle.OUTPUT_READY);
            interrupted.assertExactOutput(first, fixture.sourcePayloads);

            try (Process restarted = Process.open(
                    fixture,
                    "v".repeat(26),
                    "g".repeat(26),
                    temporaryDirectory.resolve("lost-output-restart"),
                    store -> store)) {
                MaterializationOutput recovered = restarted.worker.execute(fixture.task).join();
                assertThat(recovered).isEqualTo(first);
                assertThat(restarted.task().value().attempt()).isEqualTo(1);
            }
        }
    }

    private static final class Fixture implements AutoCloseable {
        private final String cluster;
        private final String bucket;
        private final StreamId stream;
        private final ProjectionRef projection;
        private final MaterializationTask task;
        private final List<ObjectSliceReadTarget> sourceTargets;
        private final List<byte[]> sourcePayloads;

        private Fixture(
                String cluster,
                String bucket,
                StreamId stream,
                ProjectionRef projection,
                MaterializationTask task,
                List<ObjectSliceReadTarget> sourceTargets,
                List<byte[]> sourcePayloads) {
            this.cluster = cluster;
            this.bucket = bucket;
            this.stream = stream;
            this.projection = projection;
            this.task = task;
            this.sourceTargets = List.copyOf(sourceTargets);
            this.sourcePayloads = sourcePayloads.stream().map(byte[]::clone).toList();
        }

        private static Fixture create() {
            String suffix = UUID.randomUUID().toString().replace("-", "");
            String cluster = "f4-m3-" + suffix;
            String bucket = "nereus-f4-m3-" + suffix.substring(0, 20);
            createBucket(bucket);
            try (S3CompatibleObjectStoreProvider provider = new S3CompatibleObjectStoreProvider();
                    ObjectStore objects = createObjectStore(provider, bucket);
                    SharedOxiaClientRuntime runtime =
                            SharedOxiaClientRuntime.connect(oxiaConfiguration(), CLOCK);
                    OxiaJavaClientMetadataStore l0 =
                            OxiaJavaClientMetadataStore.usingSharedRuntime(
                                    oxiaConfiguration(), runtime, CLOCK);
                    OxiaJavaGenerationMetadataStore generations =
                            OxiaJavaGenerationMetadataStore.usingSharedRuntime(
                                    oxiaConfiguration(), runtime, CLOCK);
                    OxiaJavaPhysicalObjectMetadataStore physical =
                            OxiaJavaPhysicalObjectMetadataStore.usingSharedRuntime(
                                    oxiaConfiguration(), runtime, CLOCK);
                    DefaultObjectProtectionManager appendProtections =
                            new DefaultObjectProtectionManager(
                                    cluster,
                                    physical,
                                    Duration.ofMinutes(2),
                                    Duration.ofSeconds(1),
                                    Duration.ofMinutes(5),
                                    CLOCK)) {
                GenerationZeroPhysicalReferencePublisher appendReferences =
                        new DefaultGenerationZeroPhysicalReferencePublisher(
                                cluster, l0, physical, appendProtections);
                ProjectionRef projection = new ProjectionRef(
                        ProjectionType.VIRTUAL_LEDGER, "projection-" + suffix);
                StreamId stream = new StreamId(l0.createOrGetStream(
                                cluster,
                                new StreamName("stream-" + suffix),
                                new StreamCreateOptions(
                                        StorageProfile.OBJECT_WAL_SYNC_OBJECT, Map.of()))
                        .join()
                        .streamId());
                var session = l0.acquireAppendSession(
                                cluster,
                                stream,
                                new AppendSessionOptions(
                                        "writer-m3", Duration.ofMinutes(2), false))
                        .join();
                List<byte[]> payloads = List.of(
                        "exact-entry-zero".getBytes(StandardCharsets.UTF_8),
                        "exact-entry-one".getBytes(StandardCharsets.UTF_8));
                List<ObjectSliceReadTarget> targets = new ArrayList<>();
                long lastCommitVersion = 0;
                for (int index = 0; index < payloads.size(); index++) {
                    byte[] payload = payloads.get(index);
                    ObjectId objectId = new ObjectId("source-" + index + "-" + suffix);
                    ObjectKey objectKey = new ObjectKey(
                            "f4-m3/" + suffix + "/source-" + index);
                    PutObjectResult put = put(objects, objectKey, payload);
                    ObjectSliceReadTarget target = target(
                            objectId, objectKey, "source-slice-" + index, payload);
                    targets.add(target);
                    PhysicalObjectIdentity identity = PhysicalObjectIdentity.create(
                            objectKey,
                            Optional.of(objectId),
                            PhysicalObjectKind.OBJECT_WAL,
                            payload.length,
                            put.checksum(),
                            Optional.of(sha256(payload)),
                            Optional.of(put.etag()));
                    physical.createRoot(cluster, activeRoot(identity)).join();
                    CommitAppendRequest appendRequest = new CommitAppendRequest(
                            stream,
                            "writer-m3",
                            "writer-run-m3",
                            session.epoch(),
                            session.fencingToken(),
                            index,
                            target,
                            PayloadFormat.PULSAR_ENTRY_BATCH,
                            1,
                            1,
                            payload.length,
                            List.of(),
                            index + 1L,
                            index + 1L,
                            Optional.of(projection));
                    l0.putObjectManifest(
                            cluster,
                            appendManifest(appendRequest, target, identity))
                            .join();
                    StableAppendResult stable = commitProtectedGenerationZero(
                            cluster, l0, appendReferences, appendRequest);
                    lastCommitVersion = stable.reachableAppend()
                            .committedAppend()
                            .commitVersion();
                }
                String projectionValue = MaterializationRecordMapper.projectionIdentity(
                        Optional.of(projection));
                long now = CLOCK.millis();
                generations.createOrVerifyStreamRegistration(
                                cluster,
                                new MaterializationStreamRegistrationRecord(
                                        1,
                                        stream.value(),
                                        projectionValue,
                                        sha256(projectionValue.getBytes(StandardCharsets.UTF_8)).value(),
                                        StorageProfile.OBJECT_WAL_SYNC_OBJECT.name(),
                                        now,
                                        lastCommitVersion,
                                        now,
                                        0))
                        .join();
                MaterializationPolicy policy = MaterializationPolicyFactory.losslessCommitted(
                        2, 16, 1_000, 1L << 20, 128, "ZSTD");
                MaterializationTask task = new DefaultMaterializationPlanner(
                                cluster, l0, generations, 10)
                        .plan(
                                stream,
                                new com.nereusstream.api.OffsetRange(0, payloads.size()),
                                policy,
                                1)
                        .join()
                        .getFirst();
                new MaterializationTaskStore(cluster, generations, CLOCK).create(task).join();
                return new Fixture(
                        cluster, bucket, stream, projection, task, targets, payloads);
            }
        }

        @Override
        public void close() {
        }
    }

    private static final class Process implements AutoCloseable {
        private final Fixture fixture;
        private final SharedOxiaClientRuntime runtime;
        private final OxiaJavaClientMetadataStore l0;
        private final OxiaJavaGenerationMetadataStore durableGenerations;
        private final GenerationMetadataStore generations;
        private final OxiaJavaPhysicalObjectMetadataStore physical;
        private final S3CompatibleObjectStoreProvider objectStoreProvider;
        private final ObjectStore objectStore;
        private final ObjectProtectionManager protections;
        private final ObjectReadPinManager pins;
        private final StagingFileManager staging;
        private final ScheduledExecutorService scheduler;
        private final DefaultMaterializationOutputVerifier verifier;
        private final DefaultMaterializationWorker worker;

        private Process(
                Fixture fixture,
                SharedOxiaClientRuntime runtime,
                OxiaJavaClientMetadataStore l0,
                OxiaJavaGenerationMetadataStore durableGenerations,
                GenerationMetadataStore generations,
                OxiaJavaPhysicalObjectMetadataStore physical,
                S3CompatibleObjectStoreProvider objectStoreProvider,
                ObjectStore objectStore,
                ObjectProtectionManager protections,
                ObjectReadPinManager pins,
                StagingFileManager staging,
                ScheduledExecutorService scheduler,
                DefaultMaterializationOutputVerifier verifier,
                DefaultMaterializationWorker worker) {
            this.fixture = fixture;
            this.runtime = runtime;
            this.l0 = l0;
            this.durableGenerations = durableGenerations;
            this.generations = generations;
            this.physical = physical;
            this.objectStoreProvider = objectStoreProvider;
            this.objectStore = objectStore;
            this.protections = protections;
            this.pins = pins;
            this.staging = staging;
            this.scheduler = scheduler;
            this.verifier = verifier;
            this.worker = worker;
        }

        private static Process open(
                Fixture fixture,
                String processRunId,
                String claimId,
                Path processDirectory,
                GenerationStoreDecorator decorator) throws Exception {
            Files.createDirectories(processDirectory);
            Path stagingDirectory = Files.createDirectory(processDirectory.resolve("staging"));
            Files.setPosixFilePermissions(
                    stagingDirectory, PosixFilePermissions.fromString("rwx------"));
            OxiaClientConfiguration configuration = oxiaConfiguration();
            SharedOxiaClientRuntime runtime =
                    SharedOxiaClientRuntime.connect(configuration, CLOCK);
            OxiaJavaClientMetadataStore l0 =
                    OxiaJavaClientMetadataStore.usingSharedRuntime(configuration, runtime, CLOCK);
            OxiaJavaGenerationMetadataStore durableGenerations =
                    OxiaJavaGenerationMetadataStore.usingSharedRuntime(configuration, runtime, CLOCK);
            GenerationMetadataStore generations = decorator.decorate(durableGenerations);
            OxiaJavaPhysicalObjectMetadataStore physical =
                    OxiaJavaPhysicalObjectMetadataStore.usingSharedRuntime(
                            configuration, runtime, CLOCK);
            S3CompatibleObjectStoreProvider provider = new S3CompatibleObjectStoreProvider();
            ObjectStore objects = createObjectStore(provider, fixture.bucket);
            ObjectProtectionManager protections = new DefaultObjectProtectionManager(
                    fixture.cluster,
                    physical,
                    Duration.ofMinutes(2),
                    Duration.ofSeconds(1),
                    Duration.ofMinutes(5),
                    CLOCK);
            ObjectReadPinManager pins = new DefaultObjectReadPinManager(
                    fixture.cluster,
                    processRunId,
                    physical,
                    Duration.ofMinutes(2),
                    Duration.ofSeconds(1),
                    Duration.ofMinutes(5),
                    CLOCK);
            StagingFileManager staging = new StagingFileManager(
                    stagingDirectory,
                    64L << 20,
                    StagingFileManager.MIN_UPLOAD_CHUNK_BYTES,
                    Duration.ofHours(1),
                    Runnable::run);
            ScheduledExecutorService scheduler = Executors.newScheduledThreadPool(2);
            PhysicalObjectIdentityResolver identities =
                    new MetadataPhysicalObjectIdentityResolver(
                            fixture.cluster, l0, physical);
            ReadTargetReader sourceReader =
                    new S3WalEntryReader(objects, fixture.sourceTargets.getFirst());
            ReadTargetDispatcher dispatcher = new ReadTargetDispatcher(
                    new ReadTargetReaderRegistry(List.of(sourceReader)));
            ExactSourceRangeReaderFactory exactReaders = stream ->
                    new DefaultExactSourceRangeReader(
                            fixture.cluster,
                            stream,
                            generations,
                            identities,
                            pins,
                            dispatcher,
                            1,
                            1 << 20,
                            CLOCK,
                            Runnable::run);
            ParquetCompactedObjectReader parquetReader =
                    new ParquetCompactedObjectReader(objects, Runnable::run);
            DefaultMaterializationOutputVerifier verifier =
                    new DefaultMaterializationOutputVerifier(
                            objects,
                            new CompactedMaterializationFormatVerifier(
                                    new CompactedObjectVerifier(objects, parquetReader)));
            DefaultMaterializationWorker worker = new DefaultMaterializationWorker(
                    fixture.cluster,
                    processRunId,
                    new MaterializationTaskStore(fixture.cluster, generations, CLOCK),
                    generations,
                    identities,
                    protections,
                    exactReaders,
                    new ParquetCompactedObjectWriter(staging, Runnable::run),
                    objects,
                    verifier,
                    () -> claimId,
                    1,
                    1 << 20,
                    Duration.ofMinutes(2),
                    Duration.ofSeconds(10),
                    Duration.ofMillis(1),
                    3,
                    TIMEOUT,
                    "f4-m3-integration",
                    scheduler,
                    Runnable::run,
                    CLOCK);
            return new Process(
                    fixture,
                    runtime,
                    l0,
                    durableGenerations,
                    generations,
                    physical,
                    provider,
                    objects,
                    protections,
                    pins,
                    staging,
                    scheduler,
                    verifier,
                    worker);
        }

        private VersionedMaterializationTask task() {
            return generations.getTask(
                            fixture.cluster, fixture.stream, fixture.task.taskId())
                    .join()
                    .orElseThrow();
        }

        private DefaultGenerationCommitter committer() {
            return new DefaultGenerationCommitter(
                    fixture.cluster,
                    l0,
                    generations,
                    physical,
                    protections,
                    activationGuard(),
                    verifier,
                    () -> new PublicationId("p".repeat(26)),
                    TIMEOUT,
                    scheduler,
                    CLOCK);
        }

        private void assertExactOutput(
                MaterializationOutput output,
                List<byte[]> expectedPayloads) {
            ObjectSliceReadTarget target = (ObjectSliceReadTarget) output.readTarget();
            CompactedObjectReadResult result = new ParquetCompactedObjectReader(
                            objectStore, Runnable::run)
                    .read(new CompactedObjectReadRequest(
                            output.streamId(),
                            output.view(),
                            output.coverage(),
                            output.coverage().startOffset(),
                            target,
                            PayloadFormat.PULSAR_ENTRY_BATCH,
                            100,
                            1 << 20,
                            TIMEOUT))
                    .join();
            assertThat(result.rows()).hasSize(expectedPayloads.size());
            for (int index = 0; index < expectedPayloads.size(); index++) {
                assertThat(result.rows().get(index).streamOffset()).isEqualTo(index);
                assertThat(bytes(result.rows().get(index).exactPayload()))
                        .isEqualTo(expectedPayloads.get(index));
            }
            assertThat(result.sourceCoverageEndOffset())
                    .isEqualTo(output.coverage().endOffset());
        }

        private void assertNoReaderLeases() {
            for (ObjectSliceReadTarget source : fixture.sourceTargets) {
                assertThat(physical.scanReaderLeases(
                                fixture.cluster,
                                ObjectKeyHash.from(source.objectKey()),
                                Optional.empty(),
                                10)
                        .join()
                        .values())
                        .isEmpty();
            }
        }

        @Override
        public void close() {
            staging.close();
            pins.close();
            protections.close();
            scheduler.shutdownNow();
            objectStore.close();
            objectStoreProvider.close();
            physical.close();
            durableGenerations.close();
            l0.close();
            runtime.close();
        }
    }

    private static final class S3WalEntryReader implements ReadTargetReader {
        private final ObjectStore objectStore;
        private final ReadTargetReaderKey key;

        private S3WalEntryReader(
                ObjectStore objectStore,
                ObjectSliceReadTarget prototype) {
            this.objectStore = objectStore;
            this.key = ReadTargetReaderKey.from(prototype);
        }

        @Override
        public ReadTargetReaderKey key() {
            return key;
        }

        @Override
        public long reservationBytes(ResolvedRange range) {
            return ((ObjectSliceReadTarget) range.readTarget()).objectLength();
        }

        @Override
        public CompletableFuture<WalReadResult> readWithStats(
                StreamId streamId,
                long startOffset,
                List<ResolvedRange> ranges,
                ReadOptions options) {
            if (ranges.size() != 1
                    || startOffset != ranges.getFirst().offsetRange().startOffset()
                    || ranges.getFirst().offsetRange().recordCount() != 1) {
                return CompletableFuture.failedFuture(new IllegalArgumentException(
                        "F4-M3 source fixture expects one exact Entry range"));
            }
            ResolvedRange range = ranges.getFirst();
            ObjectSliceReadTarget target = (ObjectSliceReadTarget) range.readTarget();
            return objectStore.readRange(
                            target.objectKey(),
                            target.objectOffset(),
                            target.objectLength(),
                            new RangeReadOptions(
                                    Optional.of(target.sliceChecksum()), options.timeout()))
                    .thenApply(read -> {
                        byte[] payload = bytes(read.payload());
                        ReadBatch batch = new ReadBatch(
                                range.offsetRange(),
                                range.payloadFormat(),
                                payload,
                                range.schemaRefs(),
                                target.entryIndexRef(),
                                range.projectionRef(),
                                target.objectId(),
                                target.objectOffset(),
                                target.objectLength());
                        WalSliceReadStats stats = new WalSliceReadStats(
                                target.objectId(),
                                target.objectOffset(),
                                target.objectLength(),
                                target.entryIndexRef().length(),
                                payload.length);
                        return new WalReadResult(List.of(batch), List.of(stats));
                    });
        }
    }

    private static GenerationMetadataStore loseOutputReadyResponse(
            GenerationMetadataStore delegate,
            AtomicBoolean loseOutputReady) {
        return (GenerationMetadataStore) Proxy.newProxyInstance(
                GenerationMetadataStore.class.getClassLoader(),
                new Class<?>[] {GenerationMetadataStore.class},
                (proxy, method, args) -> {
                    if (method.getDeclaringClass() == Object.class) {
                        return method.invoke(delegate, args);
                    }
                    Object result;
                    try {
                        result = method.invoke(delegate, args);
                    } catch (InvocationTargetException failure) {
                        throw failure.getCause();
                    }
                    if (!method.getName().equals("compareAndSetTask")
                            || !(args[1] instanceof MaterializationTaskRecord replacement)
                            || replacement.lifecycle() != TaskLifecycle.OUTPUT_READY
                            || !loseOutputReady.compareAndSet(true, false)) {
                        return result;
                    }
                    @SuppressWarnings("unchecked")
                    CompletableFuture<VersionedMaterializationTask> written =
                            (CompletableFuture<VersionedMaterializationTask>) result;
                    return written.thenCompose(ignored -> CompletableFuture.failedFuture(
                            new F4MetadataConditionFailedException(
                                    "injected lost OUTPUT_READY response")));
                });
    }

    private static GenerationProtocolActivationGuard activationGuard() {
        return new GenerationProtocolActivationGuard() {
            @Override
            public CompletableFuture<GenerationActivationProof> requireReady(
                    GenerationOperation operation,
                    com.nereusstream.core.capability.GenerationActivationSubject subject,
                    boolean activateLiveProjectionIfAbsent) {
                return CompletableFuture.completedFuture(GenerationActivationProof.create(
                        operation,
                        subject,
                        1,
                        1,
                        1,
                        sha256("f4-m3-reference-domains".getBytes(StandardCharsets.UTF_8)),
                        true,
                        false,
                        CLOCK.millis()));
            }

            @Override
            public CompletableFuture<Void> revalidate(GenerationActivationProof proof) {
                return CompletableFuture.completedFuture(null);
            }
        };
    }

    private static ObjectSliceReadTarget target(
            ObjectId objectId,
            ObjectKey objectKey,
            String sliceId,
            byte[] payload) {
        byte[] inlineIndex = new byte[] {1};
        EntryIndexRef entryIndex = new EntryIndexRef(
                EntryIndexLocation.INLINE,
                Optional.empty(),
                Optional.empty(),
                Optional.of(inlineIndex),
                0,
                0,
                Crc32cChecksums.checksum(inlineIndex));
        return new ObjectSliceReadTarget(
                1,
                objectId,
                objectKey,
                ObjectType.MULTI_STREAM_WAL_OBJECT,
                "WAL_OBJECT_V1",
                PayloadFormat.PULSAR_ENTRY_BATCH.name(),
                sliceId,
                0,
                payload.length,
                Crc32cChecksums.checksum(payload),
                entryIndex);
    }

    private static StableAppendResult commitProtectedGenerationZero(
            String cluster,
            OxiaJavaClientMetadataStore metadata,
            GenerationZeroPhysicalReferencePublisher references,
            CommitAppendRequest request) {
        var prepared = metadata.prepareStableAppend(cluster, request).join();
        ProtectedStableAppend protectedAppend = references.protectBeforeHead(prepared, TIMEOUT).join();
        StableAppendResult stable = metadata.commitPreparedStableAppend(
                        cluster,
                        prepared,
                        protectedAppend.protectionIdentity(),
                        protectedAppend.rootMetadataVersion(),
                        protectedAppend.rootLifecycleEpoch(),
                        protectedAppend.protectionMetadataVersion(),
                        protectedAppend.protectionRecordSha256())
                .join();
        var materialized = metadata.materializeGenerationZero(
                        cluster, stable.reachableAppend())
                .join();
        references.protectVisibleIndex(materialized, TIMEOUT).join();
        return stable;
    }

    private static ObjectManifestRecord appendManifest(
            CommitAppendRequest request,
            ObjectSliceReadTarget target,
            PhysicalObjectIdentity identity) {
        long now = CLOCK.millis();
        Checksum contentSha256 = identity.contentSha256().orElseThrow();
        return new ObjectManifestRecord(
                target.objectId().value(),
                target.objectKey().value(),
                ObjectType.MULTI_STREAM_WAL_OBJECT.name(),
                "UPLOADED",
                1,
                0,
                "f4-m3-integration",
                request.writerId(),
                request.writerRunIdHash(),
                request.epoch(),
                now,
                now,
                identity.objectLength(),
                contentSha256.type().name(),
                contentSha256.value(),
                identity.storageChecksum().type().name(),
                identity.storageChecksum().value(),
                List.of(new StreamSliceManifestRecord(
                        0,
                        request.streamId().value(),
                        target.sliceId(),
                        request.epoch(),
                        target.objectOffset(),
                        target.objectLength(),
                        request.recordCount(),
                        request.entryCount(),
                        request.logicalBytes(),
                        request.schemaRefs(),
                        EntryIndexReferenceRecord.fromApi(target.entryIndexRef()),
                        target.sliceChecksum().type().name(),
                        target.sliceChecksum().value(),
                        request.payloadFormat().name(),
                        "UPLOADED")),
                now + Duration.ofDays(1).toMillis(),
                0);
    }

    private static PhysicalObjectRootRecord activeRoot(PhysicalObjectIdentity identity) {
        long now = CLOCK.millis();
        return new PhysicalObjectRootRecord(
                1,
                identity.objectKeyHash().value(),
                identity.objectKey().value(),
                identity.objectId().orElseThrow().value(),
                identity.kind().wireId(),
                identity.objectLength(),
                identity.storageChecksum().type().name(),
                identity.storageChecksum().value(),
                identity.contentSha256().orElseThrow().value(),
                identity.etag().orElseThrow(),
                PhysicalObjectLifecycle.ACTIVE,
                1,
                now,
                now + Duration.ofMinutes(5).toMillis(),
                "",
                "",
                0,
                0,
                0,
                0,
                0,
                "",
                "",
                0);
    }

    private static PutObjectResult put(
            ObjectStore objectStore,
            ObjectKey key,
            byte[] payload) {
        Checksum checksum = Crc32cChecksums.checksum(payload);
        return objectStore.putObject(
                        key,
                        ByteBuffer.wrap(payload),
                        new PutObjectOptions(
                                "application/octet-stream",
                                checksum,
                                true,
                                Map.of("phase", "f4-m3"),
                                TIMEOUT))
                .join();
    }

    private static Checksum sha256(byte[] value) {
        try {
            return new Checksum(
                    ChecksumType.SHA256,
                    HexFormat.of().formatHex(
                            MessageDigest.getInstance("SHA-256").digest(value)));
        } catch (java.security.NoSuchAlgorithmException impossible) {
            throw new IllegalStateException(impossible);
        }
    }

    private static byte[] bytes(ByteBuffer source) {
        ByteBuffer copy = source.asReadOnlyBuffer();
        byte[] value = new byte[copy.remaining()];
        copy.get(value);
        return value;
    }

    private static OxiaClientConfiguration oxiaConfiguration() {
        return new OxiaClientConfiguration(
                OXIA.getServiceAddress(),
                "default",
                Duration.ofSeconds(10),
                TIMEOUT,
                100,
                1_024);
    }

    private static void createBucket(String bucket) {
        try (S3AsyncClient admin = adminClient()) {
            admin.createBucket(CreateBucketRequest.builder().bucket(bucket).build()).join();
        }
    }

    private static S3AsyncClient adminClient() {
        return S3AsyncClient.builder()
                .endpointOverride(LOCALSTACK.getEndpointOverride(LocalStackContainer.Service.S3))
                .region(Region.of(LOCALSTACK.getRegion()))
                .serviceConfiguration(S3Configuration.builder()
                        .pathStyleAccessEnabled(true)
                        .build())
                .credentialsProvider(StaticCredentialsProvider.create(
                        AwsBasicCredentials.create(
                                LOCALSTACK.getAccessKey(), LOCALSTACK.getSecretKey())))
                .build();
    }

    private static ObjectStore createObjectStore(
            S3CompatibleObjectStoreProvider provider,
            String bucket) {
        try {
            return provider.create(objectStoreConfiguration(bucket), secretResolver());
        } catch (Exception failure) {
            throw new IllegalStateException("cannot create LocalStack object store", failure);
        }
    }

    private static ObjectStoreConfiguration objectStoreConfiguration(String bucket) {
        return new ObjectStoreConfiguration(
                S3CompatibleObjectStoreProvider.class.getName(),
                LOCALSTACK.getEndpointOverride(LocalStackContainer.Service.S3),
                LOCALSTACK.getRegion(),
                bucket,
                "objects",
                true,
                Duration.ofSeconds(10),
                4,
                Optional.of("access"),
                Optional.of("secret"),
                Optional.empty());
    }

    private static ObjectStoreSecretResolver secretResolver() {
        return reference -> Optional.of(
                "access".equals(reference)
                        ? LOCALSTACK.getAccessKey().toCharArray()
                        : LOCALSTACK.getSecretKey().toCharArray());
    }

    @FunctionalInterface
    private interface GenerationStoreDecorator {
        GenerationMetadataStore decorate(GenerationMetadataStore store);
    }
}
