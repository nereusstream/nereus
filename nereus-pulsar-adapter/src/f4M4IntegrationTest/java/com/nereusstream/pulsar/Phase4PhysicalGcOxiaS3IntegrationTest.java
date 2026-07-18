/* Licensed under the Apache License, Version 2.0 */
package com.nereusstream.pulsar;

import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.assertThatThrownBy;

import com.nereusstream.api.Checksum;
import com.nereusstream.api.ChecksumType;
import com.nereusstream.api.ErrorCode;
import com.nereusstream.api.NereusException;
import com.nereusstream.api.ObjectKey;
import com.nereusstream.api.ObjectKeyHash;
import com.nereusstream.api.OffsetRange;
import com.nereusstream.api.PayloadFormat;
import com.nereusstream.api.ReadView;
import com.nereusstream.api.StreamId;
import com.nereusstream.core.capability.GenerationCapabilityReadiness;
import com.nereusstream.core.capability.GenerationCapabilityReadinessProvider;
import com.nereusstream.core.physical.DefaultObjectProtectionManager;
import com.nereusstream.core.physical.ObjectProtectionManager;
import com.nereusstream.core.physical.PhysicalObjectKind;
import com.nereusstream.managedledger.cursor.CursorStorageConfig;
import com.nereusstream.managedledger.generation.ManagedLedgerGenerationProtocolActivationGuard;
import com.nereusstream.managedledger.generation.ManagedLedgerPhysicalDeletionActivationRequest;
import com.nereusstream.managedledger.generation.ManagedLedgerPhysicalDeletionActivationResult;
import com.nereusstream.materialization.MaterializationConfig;
import com.nereusstream.materialization.gc.PhysicalGcConfig;
import com.nereusstream.metadata.oxia.CursorMetadataStore;
import com.nereusstream.metadata.oxia.CursorMetadataStoreConfig;
import com.nereusstream.metadata.oxia.GenerationMetadataStore;
import com.nereusstream.metadata.oxia.GenerationProtocolActivationStore;
import com.nereusstream.metadata.oxia.ManagedLedgerProjectionMetadataStore;
import com.nereusstream.metadata.oxia.OxiaClientConfiguration;
import com.nereusstream.metadata.oxia.OxiaJavaClientMetadataStore;
import com.nereusstream.metadata.oxia.OxiaJavaGenerationMetadataStore;
import com.nereusstream.metadata.oxia.OxiaJavaPhysicalObjectMetadataStore;
import com.nereusstream.metadata.oxia.OxiaMetadataStore;
import com.nereusstream.metadata.oxia.PhysicalObjectMetadataStore;
import com.nereusstream.metadata.oxia.ProjectionMetadataStoreConfig;
import com.nereusstream.metadata.oxia.SharedOxiaClientRuntime;
import com.nereusstream.metadata.oxia.VersionedGenerationProtocolActivation;
import com.nereusstream.metadata.oxia.VersionedPhysicalObjectRoot;
import com.nereusstream.metadata.oxia.records.GenerationBackfillProofRecord;
import com.nereusstream.metadata.oxia.records.GenerationProtocolActivationLifecycle;
import com.nereusstream.metadata.oxia.records.GenerationProtocolActivationRecord;
import com.nereusstream.metadata.oxia.records.PhysicalObjectLifecycle;
import com.nereusstream.metadata.oxia.records.PhysicalObjectRootRecord;
import com.nereusstream.metadata.oxia.retirement.ObjectAuditRetirementStore;
import com.nereusstream.metadata.oxia.retirement.OxiaJavaObjectAuditRetirementStore;
import com.nereusstream.metadata.oxia.retirement.OxiaJavaSourceRetirementMetadataStore;
import com.nereusstream.metadata.oxia.retirement.SourceRetirementMetadataStore;
import com.nereusstream.objectstore.Crc32cChecksums;
import com.nereusstream.objectstore.DefaultObjectStoreDeleteCapabilityProbe;
import com.nereusstream.objectstore.DeleteObjectOptions;
import com.nereusstream.objectstore.DeleteObjectResult;
import com.nereusstream.objectstore.HeadObjectOptions;
import com.nereusstream.objectstore.HeadObjectResult;
import com.nereusstream.objectstore.ListObjectsOptions;
import com.nereusstream.objectstore.ListObjectsResult;
import com.nereusstream.objectstore.ObjectKeyPrefix;
import com.nereusstream.objectstore.ObjectStore;
import com.nereusstream.objectstore.ObjectStoreConfiguration;
import com.nereusstream.objectstore.ObjectStoreDeleteCapabilityProbe;
import com.nereusstream.objectstore.ObjectStoreSecretResolver;
import com.nereusstream.objectstore.PutObjectAttemptGuard;
import com.nereusstream.objectstore.PutObjectOptions;
import com.nereusstream.objectstore.PutObjectResult;
import com.nereusstream.objectstore.RangeReadOptions;
import com.nereusstream.objectstore.RangeReadResult;
import com.nereusstream.objectstore.ReplayableObjectUpload;
import com.nereusstream.objectstore.S3CompatibleObjectStoreProvider;
import com.nereusstream.objectstore.checkpoint.RecoveryCheckpointCodecV1;
import com.nereusstream.objectstore.compacted.CompactedObjectFormatV1;
import com.nereusstream.objectstore.compacted.CompactedObjectWriteRequest;
import io.oxia.testcontainers.OxiaContainer;
import java.lang.reflect.InvocationTargetException;
import java.lang.reflect.Method;
import java.lang.reflect.Proxy;
import java.nio.ByteBuffer;
import java.nio.charset.StandardCharsets;
import java.nio.file.Files;
import java.nio.file.Path;
import java.nio.file.attribute.PosixFilePermissions;
import java.security.MessageDigest;
import java.security.NoSuchAlgorithmException;
import java.time.Clock;
import java.time.Duration;
import java.time.Instant;
import java.time.ZoneId;
import java.time.ZoneOffset;
import java.util.HexFormat;
import java.util.List;
import java.util.Map;
import java.util.Optional;
import java.util.UUID;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.CompletionException;
import java.util.concurrent.ExecutionException;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.ScheduledExecutorService;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicBoolean;
import java.util.concurrent.atomic.AtomicInteger;
import java.util.concurrent.atomic.AtomicLong;
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
class Phase4PhysicalGcOxiaS3IntegrationTest {
    private static final DockerImageName OXIA_IMAGE =
            DockerImageName.parse("oxia/oxia:0.16.3");
    private static final DockerImageName LOCALSTACK_IMAGE =
            DockerImageName.parse("localstack/localstack:4.14.0");
    private static final long READINESS_EPOCH = 7;
    private static final Duration ACTIVATION_TIMEOUT = Duration.ofSeconds(90);
    private static final Duration REQUEST_TIMEOUT = Duration.ofSeconds(10);
    private static final Duration DRAIN_GRACE = Duration.ofMinutes(6);

    @Container
    private static final OxiaContainer OXIA = new OxiaContainer(OXIA_IMAGE).withShards(4);

    @Container
    private static final LocalStackContainer LOCALSTACK = new LocalStackContainer(LOCALSTACK_IMAGE)
            .withServices(LocalStackContainer.Service.S3);

    @TempDir
    Path temporaryDirectory;

    @Test
    void activationAndRestartRecoverOwnerlessDeleteAcrossExactDurableScope() throws Exception {
        MutableClock clock = new MutableClock(1_000_000);
        String suffix = UUID.randomUUID().toString().replace("-", "");
        String cluster = "f4-m4-" + suffix;
        String bucket = "nereus-f4-m4-" + suffix.substring(0, 24);
        createBucket(bucket);
        ObjectStoreConfiguration correctScope = objectStoreConfiguration(bucket, "scope-a");
        ObjectStoreConfiguration wrongScope = objectStoreConfiguration(bucket, "scope-b");
        GenerationCapabilityReadiness readiness = new GenerationCapabilityReadiness(
                READINESS_EPOCH, sha256("broker-set/" + suffix), 1);
        PhysicalGcConfig gcConfig = physicalGcConfig();
        TargetObject target;

        try (Process first = Process.open(
                cluster,
                "a".repeat(26),
                correctScope,
                readiness,
                gcConfig,
                stagingDirectory("first"),
                clock,
                StoreDecorator.identity())) {
            first.seedPublication();
            target = first.createOwnerlessCompactedObject();

            first.runtime.start();
            assertThat(first.runtime.lifecycleService().isRunning()).isFalse();
            assertThat(first.root(target).value().lifecycle())
                    .isEqualTo(PhysicalObjectLifecycle.ACTIVE);

            ManagedLedgerPhysicalDeletionActivationResult activation = first.runtime
                    .activate(new ManagedLedgerPhysicalDeletionActivationRequest(
                            "b".repeat(26), 4, ACTIVATION_TIMEOUT))
                    .join();
            assertThat(activation.status())
                    .isEqualTo(ManagedLedgerPhysicalDeletionActivationResult.Status.ACTIVATED);
            assertThat(activation.objectStoreCapabilitySha256())
                    .isEqualTo(first.runtime.expectedObjectStoreCapabilitySha256());
            assertThat(first.runtime.lifecycleService().isRunning()).isTrue();

            first.runtime.lifecycleService().scanNow().join();
            VersionedPhysicalObjectRoot marked = first.root(target);
            assertThat(marked.value().lifecycle()).isEqualTo(PhysicalObjectLifecycle.MARKED);
            assertThat(marked.value().deleteNotBeforeMillis())
                    .isEqualTo(Math.addExact(clock.millis(), DRAIN_GRACE.toMillis()));
            first.assertObjectPresent(target);
        }

        clock.advance(Duration.ofMinutes(7));
        try (Process wrong = Process.open(
                cluster,
                "c".repeat(26),
                wrongScope,
                readiness,
                gcConfig,
                stagingDirectory("wrong-scope"),
                clock,
                StoreDecorator.identity())) {
            assertThatThrownBy(wrong.runtime::start).satisfies(failure -> {
                Throwable exact = unwrap(failure);
                assertThat(exact).isInstanceOf(NereusException.class);
                NereusException nereus = (NereusException) exact;
                assertThat(nereus.code()).isEqualTo(ErrorCode.METADATA_INVARIANT_VIOLATION);
                assertThat(nereus.retriable()).isFalse();
            });
            assertThat(wrong.runtime.lifecycleService().isRunning()).isFalse();
            assertThat(wrong.root(target).value().lifecycle())
                    .isEqualTo(PhysicalObjectLifecycle.MARKED);
        }

        AtomicBoolean deleteResponseLost = new AtomicBoolean();
        AtomicInteger emptyListCalls = new AtomicInteger();
        try (Process recovered = Process.open(
                cluster,
                "d".repeat(26),
                correctScope,
                readiness,
                gcConfig,
                stagingDirectory("recovered"),
                clock,
                raw -> new EmptyInventoryLostDeleteResponseObjectStore(
                        raw, target.key(), deleteResponseLost, emptyListCalls))) {
            recovered.assertObjectPresent(target);
            recovered.runtime.start();
            assertThat(recovered.runtime.lifecycleService().isRunning()).isTrue();

            recovered.runtime.lifecycleService().scanNow().join();

            assertThat(recovered.root(target).value().lifecycle())
                    .isEqualTo(PhysicalObjectLifecycle.DELETED);
            assertThat(deleteResponseLost).isTrue();
            assertThat(emptyListCalls).hasPositiveValue();
            assertThatThrownBy(() -> recovered.objectStore
                            .headObject(target.key(), new HeadObjectOptions(REQUEST_TIMEOUT))
                            .join())
                    .satisfies(failure -> {
                        Throwable exact = unwrap(failure);
                        assertThat(exact).isInstanceOf(NereusException.class);
                        assertThat(((NereusException) exact).code())
                                .isEqualTo(ErrorCode.OBJECT_NOT_FOUND);
                    });
        }
    }

    @Test
    void processRestartAfterDeleteBeforeDeletedRootCasRecoversDurableDeletingIntent()
            throws Exception {
        MutableClock clock = new MutableClock(2_000_000);
        String suffix = UUID.randomUUID().toString().replace("-", "");
        String cluster = "f4-m4-post-delete-" + suffix;
        String bucket = "nereus-f4-m4-" + suffix.substring(0, 24);
        createBucket(bucket);
        ObjectStoreConfiguration scope = objectStoreConfiguration(bucket, "post-delete");
        GenerationCapabilityReadiness readiness = new GenerationCapabilityReadiness(
                READINESS_EPOCH, sha256("broker-set/post-delete/" + suffix), 1);
        PhysicalGcConfig gcConfig = physicalGcConfig();
        TargetObject target;

        try (Process first = Process.open(
                cluster,
                "f".repeat(26),
                scope,
                readiness,
                gcConfig,
                stagingDirectory("post-delete-first"),
                clock,
                StoreDecorator.identity())) {
            first.seedPublication();
            target = first.createOwnerlessCompactedObject();

            ManagedLedgerPhysicalDeletionActivationResult activation = first.runtime
                    .activate(new ManagedLedgerPhysicalDeletionActivationRequest(
                            "g".repeat(26), 4, ACTIVATION_TIMEOUT))
                    .join();
            assertThat(activation.status())
                    .isEqualTo(ManagedLedgerPhysicalDeletionActivationResult.Status.ACTIVATED);
            first.runtime.lifecycleService().scanNow().join();
            assertThat(first.root(target).value().lifecycle())
                    .isEqualTo(PhysicalObjectLifecycle.MARKED);
            first.assertObjectPresent(target);
        }

        clock.advance(Duration.ofMinutes(7));
        AtomicBoolean targetDeleteCompleted = new AtomicBoolean();
        AtomicBoolean crashInjected = new AtomicBoolean();
        CompletableFuture<Void> crashObserved = new CompletableFuture<>();
        try (Process interrupted = Process.open(
                cluster,
                "h".repeat(26),
                scope,
                readiness,
                gcConfig,
                stagingDirectory("post-delete-interrupted"),
                clock,
                raw -> new TargetDeleteTrackingObjectStore(
                        raw, target.key(), targetDeleteCompleted),
                PhysicalStoreDecorator.failBeforeDeletedRootCas(
                        target.hash(),
                        targetDeleteCompleted,
                        crashInjected,
                        crashObserved))) {
            interrupted.runtime.start();
            crashObserved.get(30, TimeUnit.SECONDS);

            assertThat(targetDeleteCompleted).isTrue();
            assertThat(crashInjected).isTrue();
            assertThat(interrupted.root(target).value().lifecycle())
                    .isEqualTo(PhysicalObjectLifecycle.DELETING);
            interrupted.assertObjectAbsent(target);
        }

        try (Process recovered = Process.open(
                cluster,
                "i".repeat(26),
                scope,
                readiness,
                gcConfig,
                stagingDirectory("post-delete-recovered"),
                clock,
                StoreDecorator.identity())) {
            recovered.runtime.start();
            recovered.runtime.lifecycleService().scanNow().join();

            assertThat(recovered.root(target).value().lifecycle())
                    .isEqualTo(PhysicalObjectLifecycle.DELETED);
            recovered.assertObjectAbsent(target);
        }
    }

    private Path stagingDirectory(String name) throws Exception {
        Path path = Files.createDirectories(temporaryDirectory.resolve(name));
        Files.setPosixFilePermissions(path, PosixFilePermissions.fromString("rwx------"));
        return path.toAbsolutePath().normalize();
    }

    private static PhysicalGcConfig physicalGcConfig() {
        return new PhysicalGcConfig(
                true,
                false,
                64,
                64,
                2,
                PhysicalGcConfig.MAX_STREAMS_PER_CANDIDATE,
                10_000,
                10_000,
                Duration.ofDays(1),
                Duration.ofMinutes(5),
                Duration.ofMinutes(1),
                Duration.ofSeconds(5),
                DRAIN_GRACE,
                Duration.ofHours(1),
                Duration.ofHours(25),
                Duration.ofDays(7),
                Duration.ofSeconds(60),
                Duration.ofSeconds(60));
    }

    private static OxiaClientConfiguration oxiaConfiguration() {
        return new OxiaClientConfiguration(
                OXIA.getServiceAddress(),
                "default",
                Duration.ofSeconds(10),
                Duration.ofSeconds(30),
                10_000,
                1_024);
    }

    private static ObjectStoreConfiguration objectStoreConfiguration(
            String bucket, String prefix) {
        return new ObjectStoreConfiguration(
                S3CompatibleObjectStoreProvider.class.getName(),
                LOCALSTACK.getEndpointOverride(LocalStackContainer.Service.S3),
                LOCALSTACK.getRegion(),
                bucket,
                prefix,
                true,
                REQUEST_TIMEOUT,
                8,
                Optional.of("access"),
                Optional.of("secret"),
                Optional.empty());
    }

    private static ObjectStoreSecretResolver secretResolver() {
        return reference -> switch (reference) {
            case "access" -> Optional.of(LOCALSTACK.getAccessKey().toCharArray());
            case "secret" -> Optional.of(LOCALSTACK.getSecretKey().toCharArray());
            default -> Optional.empty();
        };
    }

    private static void createBucket(String bucket) {
        try (S3AsyncClient admin = S3AsyncClient.builder()
                .endpointOverride(LOCALSTACK.getEndpointOverride(LocalStackContainer.Service.S3))
                .region(Region.of(LOCALSTACK.getRegion()))
                .serviceConfiguration(S3Configuration.builder()
                        .pathStyleAccessEnabled(true)
                        .build())
                .credentialsProvider(StaticCredentialsProvider.create(
                        AwsBasicCredentials.create(
                                LOCALSTACK.getAccessKey(), LOCALSTACK.getSecretKey())))
                .build()) {
            admin.createBucket(CreateBucketRequest.builder().bucket(bucket).build()).join();
        }
    }

    private static Checksum sha256(String value) {
        return sha256(value.getBytes(StandardCharsets.UTF_8));
    }

    private static Checksum sha256(byte[] value) {
        try {
            return new Checksum(
                    ChecksumType.SHA256,
                    HexFormat.of().formatHex(
                            MessageDigest.getInstance("SHA-256").digest(value)));
        } catch (NoSuchAlgorithmException impossible) {
            throw new IllegalStateException("SHA-256 is unavailable", impossible);
        }
    }

    private static Throwable unwrap(Throwable supplied) {
        Throwable failure = supplied;
        while ((failure instanceof CompletionException
                        || failure instanceof ExecutionException)
                && failure.getCause() != null) {
            failure = failure.getCause();
        }
        return failure;
    }

    private static RecoveryCheckpointCodecV1 unusedCheckpointCodec() {
        return (RecoveryCheckpointCodecV1) Proxy.newProxyInstance(
                RecoveryCheckpointCodecV1.class.getClassLoader(),
                new Class<?>[] {RecoveryCheckpointCodecV1.class},
                (proxy, method, arguments) -> {
                    if (method.getDeclaringClass() == Object.class) {
                        return switch (method.getName()) {
                            case "toString" -> "unused F4-M4 checkpoint codec";
                            case "hashCode" -> System.identityHashCode(proxy);
                            case "equals" -> proxy == arguments[0];
                            default -> throw new UnsupportedOperationException(method.getName());
                        };
                    }
                    return CompletableFuture.failedFuture(new UnsupportedOperationException(
                            "ownerless integration fixture must not read a recovery checkpoint"));
                });
    }

    private record TargetObject(ObjectKey key, ObjectKeyHash hash) {
    }

    private static final class Process implements AutoCloseable {
        private final String cluster;
        private final String processRunId;
        private final MutableClock clock;
        private final PhysicalGcConfig gcConfig;
        private final ObjectStoreConfiguration objectStoreConfiguration;
        private final S3CompatibleObjectStoreProvider objectStoreProvider;
        private final ObjectStore rawObjectStore;
        private final ObjectStore objectStore;
        private final SharedOxiaClientRuntime oxiaRuntime;
        private final OxiaMetadataStore l0;
        private final GenerationMetadataStore generations;
        private final PhysicalObjectMetadataStore rawPhysical;
        private final PhysicalObjectMetadataStore physical;
        private final ManagedLedgerProjectionMetadataStore projections;
        private final CursorMetadataStore cursors;
        private final GenerationProtocolActivationStore activations;
        private final ObjectProtectionManager protections;
        private final ScheduledExecutorService scheduler;
        private final ExecutorService callbacks;
        private final Phase4PhysicalGcRuntime runtime;
        private final AtomicBoolean closed = new AtomicBoolean();

        private Process(
                String cluster,
                String processRunId,
                ObjectStoreConfiguration objectStoreConfiguration,
                GenerationCapabilityReadiness readiness,
                PhysicalGcConfig gcConfig,
                Path stagingDirectory,
                MutableClock clock,
                StoreDecorator decorator,
                PhysicalStoreDecorator physicalDecorator) throws Exception {
            this.cluster = cluster;
            this.processRunId = processRunId;
            this.clock = clock;
            this.gcConfig = gcConfig;
            this.objectStoreConfiguration = objectStoreConfiguration;
            objectStoreProvider = new S3CompatibleObjectStoreProvider();
            rawObjectStore = objectStoreProvider.create(
                    objectStoreConfiguration, secretResolver());
            objectStore = decorator.decorate(rawObjectStore);
            OxiaClientConfiguration oxia = oxiaConfiguration();
            oxiaRuntime = SharedOxiaClientRuntime.connect(oxia, clock);
            l0 = OxiaJavaClientMetadataStore.usingSharedRuntime(oxia, oxiaRuntime, clock);
            generations = OxiaJavaGenerationMetadataStore.usingSharedRuntime(
                    oxia, oxiaRuntime, clock);
            rawPhysical = OxiaJavaPhysicalObjectMetadataStore.usingSharedRuntime(
                    oxia, oxiaRuntime, clock);
            physical = physicalDecorator.decorate(rawPhysical);
            projections = ManagedLedgerProjectionMetadataStore.usingSharedRuntime(
                    oxia,
                    oxiaRuntime,
                    ProjectionMetadataStoreConfig.defaults(),
                    clock);
            cursors = CursorMetadataStore.usingSharedRuntime(
                    oxia,
                    oxiaRuntime,
                    CursorMetadataStoreConfig.defaults());
            activations = GenerationProtocolActivationStore.usingSharedRuntime(
                    oxia,
                    oxiaRuntime,
                    clock,
                    processRunId,
                    NereusGenerationProtocolReferenceDomains.currentV1());
            SourceRetirementMetadataStore sourceRetirement =
                    OxiaJavaSourceRetirementMetadataStore.usingSharedRuntime(
                            oxia, oxiaRuntime);
            ObjectAuditRetirementStore objectAudit =
                    OxiaJavaObjectAuditRetirementStore.usingSharedRuntime(
                            oxia, oxiaRuntime);
            protections = new DefaultObjectProtectionManager(
                    cluster,
                    physical,
                    gcConfig.pendingProtectionDuration(),
                    gcConfig.maximumClockSkew(),
                    gcConfig.orphanGrace(),
                    clock);
            Phase4GcReferenceDomainAssembly referenceDomains =
                    Phase4GcReferenceDomainAssembly.create(
                            cluster, gcConfig, activations, generations, projections);
            StaticReadinessProvider readinessProvider = new StaticReadinessProvider(readiness);
            ObjectStoreDeleteCapabilityProbe capabilityProbe =
                    new DefaultObjectStoreDeleteCapabilityProbe(
                            objectStore, objectStoreConfiguration, clock);
            ManagedLedgerGenerationProtocolActivationGuard activationGuard =
                    new ManagedLedgerGenerationProtocolActivationGuard(
                            cluster,
                            true,
                            readinessProvider,
                            activations,
                            NereusGenerationProtocolReferenceDomains.currentV1(),
                            capabilityProbe.expectedCapabilitySha256(),
                            projections,
                            l0,
                            generations,
                            referenceDomains.projectionDomain(),
                            clock);
            scheduler = Executors.newSingleThreadScheduledExecutor();
            callbacks = Executors.newFixedThreadPool(2);
            MaterializationConfig materialization = MaterializationConfig.defaults(stagingDirectory);
            CursorStorageConfig cursorConfig = CursorStorageConfig.defaults();
            runtime = new Phase4PhysicalGcRuntime(
                    cluster,
                    gcConfig,
                    materialization,
                    cursorConfig,
                    l0,
                    generations,
                    projections,
                    cursors,
                    physical,
                    activations,
                    referenceDomains,
                    activationGuard,
                    readinessProvider,
                    protections,
                    sourceRetirement,
                    objectAudit,
                    objectStore,
                    capabilityProbe,
                    objectStoreConfiguration.requestTimeout(),
                    unusedCheckpointCodec(),
                    scheduler,
                    callbacks,
                    clock);
        }

        private static Process open(
                String cluster,
                String processRunId,
                ObjectStoreConfiguration objectStoreConfiguration,
                GenerationCapabilityReadiness readiness,
                PhysicalGcConfig gcConfig,
                Path stagingDirectory,
                MutableClock clock,
                StoreDecorator decorator) throws Exception {
            return open(
                    cluster,
                    processRunId,
                    objectStoreConfiguration,
                    readiness,
                    gcConfig,
                    stagingDirectory,
                    clock,
                    decorator,
                    PhysicalStoreDecorator.identity());
        }

        private static Process open(
                String cluster,
                String processRunId,
                ObjectStoreConfiguration objectStoreConfiguration,
                GenerationCapabilityReadiness readiness,
                PhysicalGcConfig gcConfig,
                Path stagingDirectory,
                MutableClock clock,
                StoreDecorator decorator,
                PhysicalStoreDecorator physicalDecorator) throws Exception {
            return new Process(
                    cluster,
                    processRunId,
                    objectStoreConfiguration,
                    readiness,
                    gcConfig,
                    stagingDirectory,
                    clock,
                    decorator,
                    physicalDecorator);
        }

        private void seedPublication() {
            VersionedGenerationProtocolActivation current =
                    activations.getOrCreate(cluster).join();
            long now = clock.millis();
            GenerationProtocolActivationRecord value = current.value();
            GenerationProtocolActivationRecord active = new GenerationProtocolActivationRecord(
                    value.schemaVersion(),
                    value.protocolVersion(),
                    GenerationProtocolActivationLifecycle.ACTIVE,
                    true,
                    false,
                    false,
                    READINESS_EPOCH,
                    value.requiredReferenceDomains(),
                    new GenerationBackfillProofRecord(
                            processRunId,
                            READINESS_EPOCH,
                            sha256("registration-coverage/" + cluster).value(),
                            true,
                            now),
                    GenerationBackfillProofRecord.incomplete(READINESS_EPOCH),
                    GenerationBackfillProofRecord.incomplete(READINESS_EPOCH),
                    "",
                    value.activatingBrokerRunId(),
                    value.preparedAtMillis(),
                    now,
                    now,
                    0);
            activations.compareAndSet(cluster, active, current.metadataVersion()).join();
        }

        private TargetObject createOwnerlessCompactedObject() {
            byte[] payload = ("phase4-m4-ownerless/" + cluster)
                    .getBytes(StandardCharsets.UTF_8);
            Checksum contentSha256 = sha256(payload);
            CompactedObjectWriteRequest request = new CompactedObjectWriteRequest(
                    cluster,
                    ReadView.COMMITTED,
                    new StreamId("ownerless-stream-" + cluster),
                    new OffsetRange(0, 1),
                    "e".repeat(26),
                    sha256("source-set/" + cluster),
                    sha256("policy/" + cluster),
                    PayloadFormat.PULSAR_ENTRY_BATCH,
                    CompactedObjectFormatV1.COMMITTED_FORMAT_ID,
                    Optional.empty(),
                    1,
                    1,
                    1,
                    payload.length,
                    List.of(),
                    0,
                    payload.length,
                    1,
                    "UNCOMPRESSED",
                    "f4-m4-integration",
                    Optional.empty());
            ObjectKey key = CompactedObjectFormatV1.objectKey(request, contentSha256);
            PutObjectResult stored = objectStore.putObject(
                            key,
                            ByteBuffer.wrap(payload),
                            new PutObjectOptions(
                                    "application/octet-stream",
                                    Crc32cChecksums.checksum(payload),
                                    true,
                                    Map.of("nereus-integration", "f4-m4"),
                                    REQUEST_TIMEOUT))
                    .join();
            long now = clock.millis();
            PhysicalObjectRootRecord root = new PhysicalObjectRootRecord(
                    1,
                    ObjectKeyHash.from(key).value(),
                    key.value(),
                    CompactedObjectFormatV1.objectId(key).value(),
                    PhysicalObjectKind.COMMITTED_COMPACTED.wireId(),
                    stored.objectLength(),
                    stored.checksum().type().name(),
                    stored.checksum().value(),
                    contentSha256.value(),
                    stored.etag(),
                    PhysicalObjectLifecycle.ACTIVE,
                    1,
                    now,
                    now,
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
            physical.createRoot(cluster, root).join();
            return new TargetObject(key, ObjectKeyHash.from(key));
        }

        private VersionedPhysicalObjectRoot root(TargetObject target) {
            return physical.getRoot(cluster, target.hash()).join().orElseThrow();
        }

        private void assertObjectPresent(TargetObject target) {
            HeadObjectResult head = objectStore
                    .headObject(target.key(), new HeadObjectOptions(REQUEST_TIMEOUT))
                    .join();
            assertThat(head.key()).isEqualTo(target.key());
            assertThat(head.objectLength()).isPositive();
        }

        private void assertObjectAbsent(TargetObject target) {
            assertThatThrownBy(() -> objectStore
                            .headObject(target.key(), new HeadObjectOptions(REQUEST_TIMEOUT))
                            .join())
                    .satisfies(failure -> {
                        Throwable exact = unwrap(failure);
                        assertThat(exact).isInstanceOf(NereusException.class);
                        assertThat(((NereusException) exact).code())
                                .isEqualTo(ErrorCode.OBJECT_NOT_FOUND);
                    });
        }

        @Override
        public void close() {
            if (!closed.compareAndSet(false, true)) {
                return;
            }
            runtime.close();
            protections.close();
            activations.close();
            cursors.close();
            projections.close();
            rawPhysical.close();
            generations.close();
            l0.close();
            oxiaRuntime.close();
            if (objectStore != rawObjectStore) {
                objectStore.close();
            }
            rawObjectStore.close();
            objectStoreProvider.close();
            callbacks.shutdownNow();
            scheduler.shutdownNow();
        }
    }

    private record StaticReadinessProvider(GenerationCapabilityReadiness readiness)
            implements GenerationCapabilityReadinessProvider {
        private StaticReadinessProvider {
            java.util.Objects.requireNonNull(readiness, "readiness");
        }

        @Override
        public CompletableFuture<GenerationCapabilityReadiness>
                requireGenerationCapabilityReadiness() {
            return CompletableFuture.completedFuture(readiness);
        }

        @Override
        public Optional<GenerationCapabilityReadiness>
                currentGenerationCapabilityReadiness() {
            return Optional.of(readiness);
        }
    }

    @FunctionalInterface
    private interface StoreDecorator {
        ObjectStore decorate(ObjectStore raw);

        static StoreDecorator identity() {
            return raw -> raw;
        }
    }

    @FunctionalInterface
    private interface PhysicalStoreDecorator {
        PhysicalObjectMetadataStore decorate(PhysicalObjectMetadataStore raw);

        static PhysicalStoreDecorator identity() {
            return raw -> raw;
        }

        static PhysicalStoreDecorator failBeforeDeletedRootCas(
                ObjectKeyHash target,
                AtomicBoolean targetDeleteCompleted,
                AtomicBoolean crashInjected,
                CompletableFuture<Void> crashObserved) {
            return raw -> (PhysicalObjectMetadataStore) Proxy.newProxyInstance(
                    PhysicalObjectMetadataStore.class.getClassLoader(),
                    new Class<?>[] {PhysicalObjectMetadataStore.class},
                    (proxy, method, arguments) -> {
                        if (method.getDeclaringClass() == Object.class) {
                            return proxyObjectMethod(
                                    proxy,
                                    method,
                                    arguments,
                                    "post-DELETE physical metadata store");
                        }
                        if (method.getName().equals("close")) {
                            return null;
                        }
                        if (method.getName().equals("compareAndSetRoot")
                                && arguments != null
                                && arguments.length == 3
                                && arguments[1] instanceof PhysicalObjectRootRecord replacement
                                && replacement.objectKeyHash().equals(target.value())
                                && replacement.lifecycle() == PhysicalObjectLifecycle.DELETED
                                && targetDeleteCompleted.get()
                                && crashInjected.compareAndSet(false, true)) {
                            crashObserved.complete(null);
                            return CompletableFuture.failedFuture(new NereusException(
                                    ErrorCode.STORAGE_CLOSED,
                                    false,
                                    "injected process death after target DELETE before DELETED root CAS"));
                        }
                        return invokeDelegate(raw, method, arguments);
                    });
        }
    }

    private abstract static class ForwardingObjectStore implements ObjectStore {
        protected final ObjectStore delegate;

        private ForwardingObjectStore(ObjectStore delegate) {
            this.delegate = delegate;
        }

        @Override
        public CompletableFuture<PutObjectResult> putObject(
                ObjectKey key,
                ReplayableObjectUpload source,
                PutObjectOptions options) {
            return delegate.putObject(key, source, options);
        }

        @Override
        public CompletableFuture<PutObjectResult> putObject(
                ObjectKey key,
                ReplayableObjectUpload source,
                PutObjectOptions options,
                PutObjectAttemptGuard attemptGuard) {
            return delegate.putObject(key, source, options, attemptGuard);
        }

        @Override
        public CompletableFuture<RangeReadResult> readRange(
                ObjectKey key,
                long offset,
                long length,
                RangeReadOptions options) {
            return delegate.readRange(key, offset, length, options);
        }

        @Override
        public CompletableFuture<HeadObjectResult> headObject(
                ObjectKey key, HeadObjectOptions options) {
            return delegate.headObject(key, options);
        }

        @Override
        public CompletableFuture<ListObjectsResult> listObjects(
                ObjectKeyPrefix prefix,
                Optional<String> continuationToken,
                ListObjectsOptions options) {
            return delegate.listObjects(prefix, continuationToken, options);
        }

        @Override
        public CompletableFuture<DeleteObjectResult> deleteObject(
                ObjectKey key, DeleteObjectOptions options) {
            return delegate.deleteObject(key, options);
        }

        @Override
        public void close() {
            // The process fixture owns and closes the delegate exactly once.
        }
    }

    private static final class TargetDeleteTrackingObjectStore
            extends ForwardingObjectStore {
        private final ObjectKey target;
        private final AtomicBoolean targetDeleteCompleted;

        private TargetDeleteTrackingObjectStore(
                ObjectStore delegate,
                ObjectKey target,
                AtomicBoolean targetDeleteCompleted) {
            super(delegate);
            this.target = target;
            this.targetDeleteCompleted = targetDeleteCompleted;
        }

        @Override
        public CompletableFuture<DeleteObjectResult> deleteObject(
                ObjectKey key, DeleteObjectOptions options) {
            return delegate.deleteObject(key, options).thenApply(result -> {
                if (key.equals(target)) {
                    targetDeleteCompleted.set(true);
                }
                return result;
            });
        }
    }

    private static final class EmptyInventoryLostDeleteResponseObjectStore
            extends ForwardingObjectStore {
        private final ObjectKey target;
        private final AtomicBoolean deleteResponseLost;
        private final AtomicInteger emptyListCalls;

        private EmptyInventoryLostDeleteResponseObjectStore(
                ObjectStore delegate,
                ObjectKey target,
                AtomicBoolean deleteResponseLost,
                AtomicInteger emptyListCalls) {
            super(delegate);
            this.target = target;
            this.deleteResponseLost = deleteResponseLost;
            this.emptyListCalls = emptyListCalls;
        }

        @Override
        public CompletableFuture<ListObjectsResult> listObjects(
                ObjectKeyPrefix prefix,
                Optional<String> continuationToken,
                ListObjectsOptions options) {
            emptyListCalls.incrementAndGet();
            return CompletableFuture.completedFuture(
                    new ListObjectsResult(prefix, List.of(), Optional.empty()));
        }

        @Override
        public CompletableFuture<DeleteObjectResult> deleteObject(
                ObjectKey key, DeleteObjectOptions options) {
            return delegate.deleteObject(key, options).thenCompose(result -> {
                if (key.equals(target) && deleteResponseLost.compareAndSet(false, true)) {
                    return CompletableFuture.failedFuture(new NereusException(
                            ErrorCode.TIMEOUT,
                            true,
                            "injected response loss after successful target DELETE"));
                }
                return CompletableFuture.completedFuture(result);
            });
        }
    }

    private static Object proxyObjectMethod(
            Object proxy,
            Method method,
            Object[] arguments,
            String description) {
        return switch (method.getName()) {
            case "toString" -> description;
            case "hashCode" -> System.identityHashCode(proxy);
            case "equals" -> proxy == arguments[0];
            default -> throw new UnsupportedOperationException(method.getName());
        };
    }

    private static Object invokeDelegate(
            Object delegate, Method method, Object[] arguments) throws Throwable {
        try {
            return method.invoke(delegate, arguments);
        } catch (InvocationTargetException failure) {
            throw failure.getCause();
        }
    }

    private static final class MutableClock extends Clock {
        private final AtomicLong millis;

        private MutableClock(long initialMillis) {
            millis = new AtomicLong(initialMillis);
        }

        private void advance(Duration duration) {
            millis.addAndGet(duration.toMillis());
        }

        @Override
        public ZoneId getZone() {
            return ZoneOffset.UTC;
        }

        @Override
        public Clock withZone(ZoneId zone) {
            return this;
        }

        @Override
        public Instant instant() {
            return Instant.ofEpochMilli(millis());
        }

        @Override
        public long millis() {
            return millis.get();
        }
    }
}
