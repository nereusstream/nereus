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
import com.nereusstream.api.OffsetRange;
import com.nereusstream.api.PayloadFormat;
import com.nereusstream.api.ProjectionRef;
import com.nereusstream.api.ProjectionType;
import com.nereusstream.api.ReadBatch;
import com.nereusstream.api.ReadIsolation;
import com.nereusstream.api.ReadOptions;
import com.nereusstream.api.ReadView;
import com.nereusstream.api.ResolvedRange;
import com.nereusstream.api.SchemaRef;
import com.nereusstream.api.StorageProfile;
import com.nereusstream.api.StreamCreateOptions;
import com.nereusstream.api.StreamId;
import com.nereusstream.api.StreamName;
import com.nereusstream.api.target.ObjectSliceReadTarget;
import com.nereusstream.core.StreamStorageConfig;
import com.nereusstream.core.capability.GenerationActivationProof;
import com.nereusstream.core.capability.GenerationOperation;
import com.nereusstream.core.capability.GenerationProtocolActivationGuard;
import com.nereusstream.core.physical.DefaultObjectProtectionManager;
import com.nereusstream.core.physical.DefaultObjectReadPinManager;
import com.nereusstream.core.physical.ObjectProtectionManager;
import com.nereusstream.core.physical.ObjectReadPinManager;
import com.nereusstream.core.physical.PhysicalObjectIdentity;
import com.nereusstream.core.physical.PhysicalObjectKind;
import com.nereusstream.core.read.GenerationReadResolver;
import com.nereusstream.core.read.GenerationReadRetryPolicy;
import com.nereusstream.core.read.MetadataGenerationReadFailureHandler;
import com.nereusstream.core.read.MetadataPhysicalObjectIdentityResolver;
import com.nereusstream.core.read.PinnedResolvedRange;
import com.nereusstream.core.read.ReadCoordinator;
import com.nereusstream.core.read.ReadMetricsObserver;
import com.nereusstream.core.read.ReadResolver;
import com.nereusstream.core.read.ReadTargetReader;
import com.nereusstream.core.read.ReadTargetReaderKey;
import com.nereusstream.core.read.ReadTargetReaderRegistry;
import com.nereusstream.metadata.oxia.CommitAppendRequest;
import com.nereusstream.metadata.oxia.F4MetadataConditionFailedException;
import com.nereusstream.metadata.oxia.GenerationIndexIdentity;
import com.nereusstream.metadata.oxia.GenerationIndexValidator;
import com.nereusstream.metadata.oxia.GenerationMetadataStore;
import com.nereusstream.metadata.oxia.ObjectProtectionScanPage;
import com.nereusstream.metadata.oxia.OxiaClientConfiguration;
import com.nereusstream.metadata.oxia.OxiaJavaClientMetadataStore;
import com.nereusstream.metadata.oxia.OxiaJavaGenerationMetadataStore;
import com.nereusstream.metadata.oxia.OxiaJavaPhysicalObjectMetadataStore;
import com.nereusstream.metadata.oxia.OxiaMetadataStore;
import com.nereusstream.metadata.oxia.PhysicalObjectMetadataStore;
import com.nereusstream.metadata.oxia.ProjectionIdentity;
import com.nereusstream.metadata.oxia.SharedOxiaClientRuntime;
import com.nereusstream.metadata.oxia.StableAppendResult;
import com.nereusstream.metadata.oxia.VersionedGenerationCandidate;
import com.nereusstream.metadata.oxia.VersionedGenerationIndex;
import com.nereusstream.metadata.oxia.VersionedGenerationZeroIndex;
import com.nereusstream.metadata.oxia.VersionedMaterializationTask;
import com.nereusstream.metadata.oxia.codec.ReadTargetCodecRegistry;
import com.nereusstream.metadata.oxia.records.AppendSessionRecord;
import com.nereusstream.metadata.oxia.records.GenerationIndexRecord;
import com.nereusstream.metadata.oxia.records.GenerationLifecycle;
import com.nereusstream.metadata.oxia.records.MaterializationOutputRecord;
import com.nereusstream.metadata.oxia.records.MaterializationStreamRegistrationRecord;
import com.nereusstream.metadata.oxia.records.MaterializationTaskRecord;
import com.nereusstream.metadata.oxia.records.ObjectProtectionType;
import com.nereusstream.metadata.oxia.records.PhysicalObjectLifecycle;
import com.nereusstream.metadata.oxia.records.PhysicalObjectRootRecord;
import com.nereusstream.metadata.oxia.records.TaskFailureClass;
import com.nereusstream.metadata.oxia.records.TaskLifecycle;
import com.nereusstream.metadata.oxia.records.WorkerClaimRecord;
import com.nereusstream.objectstore.Crc32cChecksums;
import com.nereusstream.objectstore.DeleteObjectOptions;
import com.nereusstream.objectstore.DeleteObjectResult;
import com.nereusstream.objectstore.ObjectStore;
import com.nereusstream.objectstore.ObjectStoreConfiguration;
import com.nereusstream.objectstore.ObjectStoreSecretResolver;
import com.nereusstream.objectstore.PutObjectOptions;
import com.nereusstream.objectstore.PutObjectResult;
import com.nereusstream.objectstore.RangeReadOptions;
import com.nereusstream.objectstore.S3CompatibleObjectStoreProvider;
import com.nereusstream.objectstore.wal.WalReadResult;
import com.nereusstream.objectstore.wal.WalSliceReadStats;
import io.oxia.testcontainers.OxiaContainer;
import java.lang.reflect.InvocationTargetException;
import java.lang.reflect.Proxy;
import java.nio.ByteBuffer;
import java.nio.charset.StandardCharsets;
import java.security.MessageDigest;
import java.time.Clock;
import java.time.Duration;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.HexFormat;
import java.util.List;
import java.util.Map;
import java.util.Optional;
import java.util.OptionalLong;
import java.util.UUID;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.Executors;
import java.util.concurrent.ScheduledExecutorService;
import java.util.concurrent.atomic.AtomicBoolean;
import org.junit.jupiter.api.Test;
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
class GenerationPublicationOxiaS3IntegrationTest {
    private static final Clock CLOCK = Clock.systemUTC();
    private static final Duration TIMEOUT = Duration.ofSeconds(30);
    private static final DockerImageName OXIA_IMAGE = DockerImageName.parse("oxia/oxia:0.16.3");
    private static final DockerImageName LOCALSTACK_IMAGE =
            DockerImageName.parse("localstack/localstack:4.14.0");

    @Container
    private static final OxiaContainer OXIA = new OxiaContainer(OXIA_IMAGE).withShards(4);

    @Container
    private static final LocalStackContainer LOCALSTACK = new LocalStackContainer(LOCALSTACK_IMAGE)
            .withServices(LocalStackContainer.Service.S3);

    @Test
    void lostCommittedResponseConvergesAfterIndependentOxiaRuntimeRestart() {
        try (Fixture fixture = Fixture.create()) {
            GenerationCommitResult first;
            try (Process process = fixture.openProcess()) {
                first = process.committer(
                                loseFirstCommittedResponse(process.generations), fixture)
                        .publish(fixture.task, fixture.output)
                        .join();
                assertThat(first.committedByThisCall()).isFalse();
            }

            try (Process restarted = fixture.openProcess()) {
                GenerationCommitResult recovered = new GenerationPublicationReconciler(
                                restarted.committer(restarted.generations, fixture))
                        .reconcile(fixture.task, fixture.output)
                        .join();
                assertThat(recovered.generation()).isEqualTo(first.generation());
                assertThat(recovered.publicationId()).isEqualTo(first.publicationId());
                assertThat(recovered.committedByThisCall()).isFalse();
                assertPublishedAndIndexOwned(fixture, restarted, recovered);
            }
        }
    }

    @Test
    void independentPublishersConvergeThenLocalStackLossPinsQuarantinesAndFallsBack() {
        try (Fixture fixture = Fixture.create()) {
            GenerationCommitResult left;
            GenerationCommitResult right;
            try (Process first = fixture.openProcess(); Process second = fixture.openProcess()) {
                CompletableFuture<GenerationCommitResult> firstCall =
                        first.committer(first.generations, fixture)
                                .publish(fixture.task, fixture.output);
                CompletableFuture<GenerationCommitResult> secondCall =
                        second.committer(second.generations, fixture)
                                .publish(fixture.task, fixture.output);
                CompletableFuture.allOf(firstCall, secondCall).join();
                left = firstCall.join();
                right = secondCall.join();
                assertThat(right.generation()).isEqualTo(left.generation());
                assertThat(right.publicationId()).isEqualTo(left.publicationId());
                assertThat(List.of(left, right).stream()
                                .filter(GenerationCommitResult::committedByThisCall))
                        .hasSize(1);
            }

            DeleteObjectResult deleted = fixture.objectStore.deleteObject(
                    fixture.output.objectKey(),
                    new DeleteObjectOptions(
                            fixture.output.objectLength(),
                            fixture.output.storageCrc32c(),
                            Optional.of(fixture.output.etag()),
                            TIMEOUT)).join();
            assertThat(deleted.status()).isEqualTo(DeleteObjectResult.Status.DELETED);

            try (Process restarted = fixture.openProcess()) {
                byte[] payload = readWithProductionPinAndFallback(fixture, restarted);
                assertThat(payload).isEqualTo(fixture.sourcePayloads.getFirst());

                VersionedGenerationIndex quarantined = restarted.generations.getIndex(
                                fixture.cluster,
                                new GenerationIndexIdentity(
                                        fixture.stream,
                                        ReadView.COMMITTED,
                                        left.coverage().endOffset(),
                                        left.generation().value()))
                        .join().orElseThrow();
                assertThat(quarantined.value().lifecycle())
                        .isEqualTo(GenerationLifecycle.QUARANTINED);
                assertThat(restarted.physical.getRoot(
                                fixture.cluster, fixture.output.objectKeyHash())
                        .join().orElseThrow().value().lifecycle())
                        .isEqualTo(PhysicalObjectLifecycle.QUARANTINED);
                assertThat(restarted.physical.scanReaderLeases(
                                fixture.cluster,
                                fixture.output.objectKeyHash(),
                                Optional.empty(),
                                10).join().values())
                        .isEmpty();
                assertThat(restarted.physical.scanReaderLeases(
                                fixture.cluster,
                                ObjectKeyHash.from(fixture.sourceTargets.getFirst().objectKey()),
                                Optional.empty(),
                                10).join().values())
                        .isEmpty();
            }
        }
    }

    private static void assertPublishedAndIndexOwned(
            Fixture fixture,
            Process process,
            GenerationCommitResult committed) {
        VersionedGenerationIndex index = process.generations.getIndex(
                        fixture.cluster,
                        new GenerationIndexIdentity(
                                fixture.stream,
                                ReadView.COMMITTED,
                                committed.coverage().endOffset(),
                                committed.generation().value()))
                .join().orElseThrow();
        assertThat(index.value().lifecycle()).isEqualTo(GenerationLifecycle.COMMITTED);
        assertThat(process.generations.getTask(
                        fixture.cluster, fixture.stream, fixture.task.taskId())
                .join().orElseThrow().value().lifecycle())
                .isEqualTo(TaskLifecycle.PUBLISHED);
        ObjectProtectionScanPage protections = process.physical.scanProtections(
                        fixture.cluster,
                        fixture.output.objectKeyHash(),
                        Optional.empty(),
                        10).join();
        assertThat(protections.values()).singleElement().satisfies(protection -> {
            assertThat(protection.value().protectionTypeId())
                    .isEqualTo(ObjectProtectionType.VISIBLE_GENERATION.wireId());
            assertThat(protection.value().ownerKey()).isEqualTo(index.key());
            assertThat(protection.value().ownerMetadataVersion()).isEqualTo(index.metadataVersion());
            assertThat(protection.value().ownerIdentitySha256())
                    .isEqualTo(index.durableValueSha256().value());
        });
    }

    private static byte[] readWithProductionPinAndFallback(
            Fixture fixture,
            Process process) {
        List<ReadTargetReader> exactReaders = List.of(
                new S3SliceReader(fixture.objectStore, fixture.outputTarget),
                new S3SliceReader(fixture.objectStore, fixture.sourceTargets.getFirst()));
        ReadTargetReaderRegistry registry = new ReadTargetReaderRegistry(exactReaders);
        ObjectReadPinManager pins = new DefaultObjectReadPinManager(
                fixture.cluster,
                "r".repeat(26),
                process.physical,
                Duration.ofMinutes(2),
                Duration.ofSeconds(1),
                Duration.ofMinutes(5),
                CLOCK);
        StreamStorageConfig config = StreamStorageConfig.defaults(fixture.cluster, "reader-m2");
        ReadResolver l0Resolver = new ReadResolver(
                config,
                process.l0,
                CLOCK,
                ReadMetricsObserver.noop(),
                Runnable::run);
        GenerationReadResolver generations = new GenerationReadResolver(
                fixture.cluster,
                process.l0,
                process.generations,
                GenerationIndexValidator.phase15Targets(),
                registry,
                new MetadataPhysicalObjectIdentityResolver(
                        fixture.cluster, process.l0, process.physical),
                pins,
                10_000,
                CLOCK,
                Runnable::run);
        ReadCoordinator coordinator = new ReadCoordinator(
                config,
                l0Resolver,
                generations,
                registry,
                new MetadataGenerationReadFailureHandler(
                        fixture.cluster, process.generations, process.physical, CLOCK),
                new GenerationReadRetryPolicy(0),
                ReadMetricsObserver.noop(),
                Runnable::run);
        try {
            return coordinator.read(
                            fixture.stream,
                            0,
                            ReadView.COMMITTED,
                            new ReadOptions(16, 1 << 20, ReadIsolation.COMMITTED, TIMEOUT))
                    .join().result().batches().getFirst().payload();
        } finally {
            coordinator.close();
            pins.close();
            exactReaders.forEach(ReadTargetReader::close);
        }
    }

    private static GenerationMetadataStore loseFirstCommittedResponse(
            GenerationMetadataStore delegate) {
        AtomicBoolean lose = new AtomicBoolean(true);
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
                    if (method.getName().equals("compareAndSetIndex")
                            && args[1] instanceof GenerationIndexRecord replacement
                            && replacement.lifecycle() == GenerationLifecycle.COMMITTED
                            && lose.compareAndSet(true, false)) {
                        @SuppressWarnings("unchecked")
                        CompletableFuture<VersionedGenerationIndex> write =
                                (CompletableFuture<VersionedGenerationIndex>) result;
                        return write.thenCompose(ignored -> CompletableFuture.failedFuture(
                                new F4MetadataConditionFailedException(
                                        "injected lost COMMITTED response")));
                    }
                    return result;
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
                        sha256("f4-m2-reference-domains".getBytes(StandardCharsets.UTF_8)),
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

    private static final class Fixture implements AutoCloseable {
        private final String cluster;
        private final StreamId stream;
        private final ProjectionRef projection;
        private final MaterializationTask task;
        private final MaterializationOutput output;
        private final ObjectSliceReadTarget outputTarget;
        private final List<ObjectSliceReadTarget> sourceTargets;
        private final List<byte[]> sourcePayloads;
        private final byte[] outputPayload;
        private final ObjectStore objectStore;
        private final S3CompatibleObjectStoreProvider provider;

        private Fixture(
                String cluster,
                StreamId stream,
                ProjectionRef projection,
                MaterializationTask task,
                MaterializationOutput output,
                ObjectSliceReadTarget outputTarget,
                List<ObjectSliceReadTarget> sourceTargets,
                List<byte[]> sourcePayloads,
                byte[] outputPayload,
                ObjectStore objectStore,
                S3CompatibleObjectStoreProvider provider) {
            this.cluster = cluster;
            this.stream = stream;
            this.projection = projection;
            this.task = task;
            this.output = output;
            this.outputTarget = outputTarget;
            this.sourceTargets = List.copyOf(sourceTargets);
            this.sourcePayloads = sourcePayloads.stream().map(byte[]::clone).toList();
            this.outputPayload = outputPayload.clone();
            this.objectStore = objectStore;
            this.provider = provider;
        }

        private static Fixture create() {
            String suffix = UUID.randomUUID().toString().replace("-", "");
            String cluster = "f4-m2-" + suffix;
            String bucket = "nereus-f4-m2-" + suffix.substring(0, 20);
            createBucket(bucket);
            S3CompatibleObjectStoreProvider provider = new S3CompatibleObjectStoreProvider();
            ObjectStore objectStore;
            try {
                objectStore = provider.create(
                        objectStoreConfiguration(bucket),
                        secretResolver());
            } catch (Exception failure) {
                provider.close();
                throw new IllegalStateException("cannot create LocalStack object store", failure);
            }
            try {
                return createDomain(cluster, suffix, objectStore, provider);
            } catch (Throwable failure) {
                objectStore.close();
                provider.close();
                throw failure;
            }
        }

        private static Fixture createDomain(
                String cluster,
                String suffix,
                ObjectStore objectStore,
                S3CompatibleObjectStoreProvider provider) {
            OxiaClientConfiguration configuration = oxiaConfiguration();
            List<byte[]> sourcePayloads = List.of(
                    "source-zero".getBytes(StandardCharsets.UTF_8),
                    "source-one".getBytes(StandardCharsets.UTF_8));
            byte[] outputPayload = ByteBuffer.allocate(
                            sourcePayloads.stream().mapToInt(value -> value.length).sum())
                    .put(sourcePayloads.get(0))
                    .put(sourcePayloads.get(1))
                    .array();
            ProjectionRef projection = new ProjectionRef(
                    ProjectionType.VIRTUAL_LEDGER, "projection-" + suffix);
            try (SharedOxiaClientRuntime runtime =
                            SharedOxiaClientRuntime.connect(configuration, CLOCK);
                    OxiaJavaClientMetadataStore l0 =
                            OxiaJavaClientMetadataStore.usingSharedRuntime(
                                    configuration, runtime, CLOCK);
                    OxiaJavaGenerationMetadataStore generations =
                            OxiaJavaGenerationMetadataStore.usingSharedRuntime(
                                    configuration, runtime, CLOCK);
                    OxiaJavaPhysicalObjectMetadataStore physical =
                            OxiaJavaPhysicalObjectMetadataStore.usingSharedRuntime(
                                    configuration, runtime, CLOCK)) {
                StreamId stream = new StreamId(l0.createOrGetStream(
                                cluster,
                                new StreamName("stream-" + suffix),
                                new StreamCreateOptions(
                                        StorageProfile.OBJECT_WAL_SYNC_OBJECT, Map.of()))
                        .join().streamId());
                AppendSessionRecord session = l0.acquireAppendSession(
                        cluster,
                        stream,
                        new AppendSessionOptions("writer-m2", Duration.ofMinutes(2), false))
                        .join();
                List<ObjectSliceReadTarget> targets = new ArrayList<>();
                List<SourceGeneration> sources = new ArrayList<>();
                long cumulative = 0;
                for (int index = 0; index < sourcePayloads.size(); index++) {
                    byte[] payload = sourcePayloads.get(index);
                    ObjectId objectId = new ObjectId("source-" + index + "-" + suffix);
                    ObjectKey objectKey = new ObjectKey(
                            "f4-m2/" + suffix + "/source-" + index);
                    PutObjectResult put = put(objectStore, objectKey, payload);
                    ObjectSliceReadTarget target = target(
                            objectId,
                            objectKey,
                            ObjectType.MULTI_STREAM_WAL_OBJECT,
                            "WAL_OBJECT_V1",
                            "source-slice-" + index,
                            payload);
                    targets.add(target);
                    StableAppendResult stable = l0.commitStableAppend(
                            cluster,
                            new CommitAppendRequest(
                                    stream,
                                    "writer-m2",
                                    "writer-run-m2",
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
                                    Optional.of(projection)))
                            .join();
                    l0.materializeGenerationZero(cluster, stable.reachableAppend()).join();
                    PhysicalObjectIdentity sourceIdentity = PhysicalObjectIdentity.create(
                            objectKey,
                            Optional.of(objectId),
                            PhysicalObjectKind.OBJECT_WAL,
                            payload.length,
                            put.checksum(),
                            Optional.of(sha256(payload)),
                            Optional.of(put.etag()));
                    physical.createRoot(cluster, activeRoot(sourceIdentity)).join();
                    VersionedGenerationCandidate candidate = generations.getCandidate(
                                    cluster,
                                    stream,
                                    ReadView.COMMITTED,
                                    index + 1L,
                                    0)
                            .join().orElseThrow();
                    VersionedGenerationZeroIndex zero =
                            (VersionedGenerationZeroIndex) candidate;
                    long nextCumulative = Math.addExact(cumulative, payload.length);
                    sources.add(new SourceGeneration(
                            ReadView.COMMITTED,
                            zero.value().range(),
                            0,
                            zero.value().commitVersion(),
                            zero.key(),
                            zero.metadataVersion(),
                            zero.durableValueSha256(),
                            target,
                            targetIdentity(target),
                            Optional.empty(),
                            PayloadFormat.PULSAR_ENTRY_BATCH,
                            Optional.of(projection),
                            1,
                            1,
                            payload.length,
                            List.of(),
                            cumulative,
                            nextCumulative));
                    cumulative = nextCumulative;
                }

                MaterializationPolicy policy = new MaterializationPolicy(
                        "policy-f4-m2",
                        1,
                        ReadView.COMMITTED,
                        TaskKind.LOSSLESS_REWRITE,
                        MaterializationPolicy.COMMITTED_FORMAT,
                        2,
                        128,
                        MaterializationPolicy.MAX_RANGE_RECORDS,
                        1L << 20,
                        65_536,
                        "ZSTD",
                        Optional.empty());
                MaterializationTask task = MaterializationTask.create(
                        stream, new OffsetRange(0, 2), sources, policy);
                ObjectId outputId = new ObjectId("output-" + suffix);
                ObjectKey outputKey = new ObjectKey("f4-m2/" + suffix + "/output");
                PutObjectResult outputPut = put(objectStore, outputKey, outputPayload);
                ObjectSliceReadTarget outputTarget = target(
                        outputId,
                        outputKey,
                        ObjectType.STREAM_COMPACTED_OBJECT,
                        MaterializationPolicy.COMMITTED_FORMAT,
                        "output-slice",
                        outputPayload);
                MaterializationOutput output = new MaterializationOutput(
                        task.taskId(),
                        stream,
                        ReadView.COMMITTED,
                        task.coverage(),
                        "c".repeat(26),
                        outputId,
                        outputKey,
                        ObjectKeyHash.from(outputKey),
                        outputPayload.length,
                        outputPut.checksum(),
                        sha256(outputPayload),
                        outputPut.etag(),
                        MaterializationPolicy.COMMITTED_FORMAT,
                        PayloadFormat.PULSAR_ENTRY_BATCH.name(),
                        outputTarget,
                        targetIdentity(outputTarget),
                        outputTarget.entryIndexRef(),
                        2,
                        2,
                        2,
                        outputPayload.length,
                        List.of(),
                        0,
                        outputPayload.length,
                        task.sourceSetSha256(),
                        Optional.of(projection));
                createOutputReadyTask(cluster, generations, task, output);
                long now = CLOCK.millis();
                String projectionIdentity = MaterializationRecordMapper.projectionIdentity(
                        Optional.of(projection));
                generations.createOrVerifyStreamRegistration(
                        cluster,
                        new MaterializationStreamRegistrationRecord(
                                1,
                                stream.value(),
                                projectionIdentity,
                                sha256(projectionIdentity.getBytes(StandardCharsets.UTF_8)).value(),
                                StorageProfile.OBJECT_WAL_SYNC_OBJECT.name(),
                                now,
                                task.taskSequence(),
                                now,
                                0)).join();
                return new Fixture(
                        cluster,
                        stream,
                        projection,
                        task,
                        output,
                        outputTarget,
                        targets,
                        sourcePayloads,
                        outputPayload,
                        objectStore,
                        provider);
            }
        }

        private Process openProcess() {
            return Process.open(this);
        }

        @Override
        public void close() {
            objectStore.close();
            provider.close();
        }
    }

    private static final class Process implements AutoCloseable {
        private final SharedOxiaClientRuntime runtime;
        private final OxiaJavaClientMetadataStore l0;
        private final OxiaJavaGenerationMetadataStore generations;
        private final OxiaJavaPhysicalObjectMetadataStore physical;
        private final ObjectProtectionManager protections;
        private final ScheduledExecutorService scheduler;

        private Process(
                SharedOxiaClientRuntime runtime,
                OxiaJavaClientMetadataStore l0,
                OxiaJavaGenerationMetadataStore generations,
                OxiaJavaPhysicalObjectMetadataStore physical,
                ObjectProtectionManager protections,
                ScheduledExecutorService scheduler) {
            this.runtime = runtime;
            this.l0 = l0;
            this.generations = generations;
            this.physical = physical;
            this.protections = protections;
            this.scheduler = scheduler;
        }

        private static Process open(Fixture fixture) {
            OxiaClientConfiguration configuration = oxiaConfiguration();
            SharedOxiaClientRuntime runtime =
                    SharedOxiaClientRuntime.connect(configuration, CLOCK);
            OxiaJavaClientMetadataStore l0 =
                    OxiaJavaClientMetadataStore.usingSharedRuntime(
                            configuration, runtime, CLOCK);
            OxiaJavaGenerationMetadataStore generations =
                    OxiaJavaGenerationMetadataStore.usingSharedRuntime(
                            configuration, runtime, CLOCK);
            OxiaJavaPhysicalObjectMetadataStore physical =
                    OxiaJavaPhysicalObjectMetadataStore.usingSharedRuntime(
                            configuration, runtime, CLOCK);
            ObjectProtectionManager protections = new DefaultObjectProtectionManager(
                    fixture.cluster,
                    physical,
                    Duration.ofMinutes(2),
                    Duration.ofSeconds(1),
                    Duration.ofMinutes(5),
                    CLOCK);
            ScheduledExecutorService scheduler = Executors.newScheduledThreadPool(2);
            return new Process(runtime, l0, generations, physical, protections, scheduler);
        }

        private DefaultGenerationCommitter committer(
                GenerationMetadataStore store,
                Fixture fixture) {
            MaterializationFormatVerifier formatVerifier = (output, timeout) ->
                    fixture.objectStore.readRange(
                                    output.objectKey(),
                                    0,
                                    output.objectLength(),
                                    new RangeReadOptions(
                                            Optional.of(output.storageCrc32c()), timeout))
                            .thenAccept(read -> {
                                byte[] bytes = bytes(read.payload());
                                if (!Arrays.equals(bytes, fixture.outputPayload)
                                        || !sha256(bytes).equals(output.contentSha256())) {
                                    throw new IllegalStateException(
                                            "materialization output content mismatch");
                                }
                            });
            return new DefaultGenerationCommitter(
                    fixture.cluster,
                    l0,
                    store,
                    physical,
                    protections,
                    activationGuard(),
                    new DefaultMaterializationOutputVerifier(
                            fixture.objectStore, formatVerifier),
                    TIMEOUT,
                    scheduler,
                    CLOCK);
        }

        @Override
        public void close() {
            protections.close();
            scheduler.shutdownNow();
            physical.close();
            generations.close();
            l0.close();
            runtime.close();
        }
    }

    private static final class S3SliceReader implements ReadTargetReader {
        private final ObjectStore objectStore;
        private final ReadTargetReaderKey key;

        private S3SliceReader(ObjectStore objectStore, ObjectSliceReadTarget prototype) {
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
                long startOffset,
                List<ResolvedRange> ranges,
                ReadOptions options) {
            if (ranges.size() != 1
                    || startOffset != ranges.getFirst().offsetRange().startOffset()) {
                return CompletableFuture.failedFuture(new IllegalArgumentException(
                        "F4-M2 fixture reader expects one complete resolved range"));
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

    private static void createOutputReadyTask(
            String cluster,
            GenerationMetadataStore store,
            MaterializationTask task,
            MaterializationOutput output) {
        long now = CLOCK.millis();
        MaterializationTaskRecord planned = new MaterializationTaskRecord(
                1,
                task.taskId(),
                task.taskSequence(),
                task.streamId().value(),
                task.view().wireId(),
                task.taskKind().wireId(),
                task.coverage().startOffset(),
                task.coverage().endOffset(),
                task.sources().stream().map(MaterializationRecordMapper::sourceRecord).toList(),
                task.sourceSetSha256().value(),
                task.policy().policyId(),
                task.policy().policyVersion(),
                task.policyDigestSha256().value(),
                TaskLifecycle.PLANNED,
                0,
                Optional.empty(),
                Optional.empty(),
                OptionalLong.empty(),
                "",
                TaskFailureClass.NONE.wireId(),
                "",
                0,
                now,
                now,
                0);
        VersionedMaterializationTask created = store.createTask(cluster, planned).join();
        MaterializationTaskRecord claimed = taskState(
                planned,
                TaskLifecycle.CLAIMED,
                Optional.of(new WorkerClaimRecord(
                        "c".repeat(26), "d".repeat(26), 1, now + 1, now + 60_000)),
                Optional.empty(),
                now + 1);
        VersionedMaterializationTask claimedValue = store.compareAndSetTask(
                cluster, claimed, created.metadataVersion()).join();
        MaterializationTaskRecord ready = taskState(
                claimed,
                TaskLifecycle.OUTPUT_READY,
                Optional.empty(),
                Optional.of(MaterializationRecordMapper.outputRecord(output)),
                now + 2);
        store.compareAndSetTask(cluster, ready, claimedValue.metadataVersion()).join();
    }

    private static MaterializationTaskRecord taskState(
            MaterializationTaskRecord source,
            TaskLifecycle lifecycle,
            Optional<WorkerClaimRecord> claim,
            Optional<MaterializationOutputRecord> output,
            long updatedAt) {
        return new MaterializationTaskRecord(
                source.schemaVersion(),
                source.taskId(),
                source.taskSequence(),
                source.streamId(),
                source.readViewId(),
                source.taskKindId(),
                source.offsetStart(),
                source.offsetEnd(),
                source.sources(),
                source.sourceSetSha256(),
                source.policyId(),
                source.policyVersion(),
                source.policySha256(),
                lifecycle,
                1,
                claim,
                output,
                OptionalLong.empty(),
                "",
                TaskFailureClass.NONE.wireId(),
                "",
                0,
                source.createdAtMillis(),
                updatedAt,
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
                                Map.of("phase", "f4-m2"),
                                TIMEOUT))
                .join();
    }

    private static ObjectSliceReadTarget target(
            ObjectId objectId,
            ObjectKey objectKey,
            ObjectType objectType,
            String physicalFormat,
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
                objectType,
                physicalFormat,
                PayloadFormat.PULSAR_ENTRY_BATCH.name(),
                sliceId,
                0,
                payload.length,
                Crc32cChecksums.checksum(payload),
                entryIndex);
    }

    private static Checksum targetIdentity(ObjectSliceReadTarget target) {
        return new Checksum(
                ChecksumType.SHA256,
                ReadTargetCodecRegistry.phase15()
                        .encode(target)
                        .identityChecksumValue());
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

    private static byte[] bytes(ByteBuffer buffer) {
        ByteBuffer copy = buffer.asReadOnlyBuffer();
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
}
