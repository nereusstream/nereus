/* Licensed under the Apache License, Version 2.0 */
package com.nereusstream.pulsar;

import static org.assertj.core.api.Assertions.assertThat;

import com.nereusstream.api.AppendBatch;
import com.nereusstream.api.AppendEntry;
import com.nereusstream.api.AppendOptions;
import com.nereusstream.api.AppendResult;
import com.nereusstream.api.Checksum;
import com.nereusstream.api.ChecksumType;
import com.nereusstream.api.DurabilityLevel;
import com.nereusstream.api.OffsetRange;
import com.nereusstream.api.PayloadFormat;
import com.nereusstream.api.ProjectionRef;
import com.nereusstream.api.ProjectionType;
import com.nereusstream.api.ReadIsolation;
import com.nereusstream.api.ReadOptions;
import com.nereusstream.api.ReadView;
import com.nereusstream.api.StorageProfile;
import com.nereusstream.api.StreamCreateOptions;
import com.nereusstream.api.StreamId;
import com.nereusstream.api.StreamName;
import com.nereusstream.api.target.BookKeeperEntryRangeReadTarget;
import com.nereusstream.api.target.ObjectSliceReadTarget;
import com.nereusstream.bookkeeper.BookKeeperClientOperations;
import com.nereusstream.bookkeeper.BookKeeperDigestType;
import com.nereusstream.bookkeeper.BookKeeperLedgerAllocator;
import com.nereusstream.bookkeeper.BookKeeperLedgerHandleCache;
import com.nereusstream.bookkeeper.BookKeeperLedgerIdNamespaceReservation;
import com.nereusstream.bookkeeper.BookKeeperLedgerIdNamespaceReservationVerifier;
import com.nereusstream.bookkeeper.BookKeeperLedgerRecovery;
import com.nereusstream.bookkeeper.BookKeeperMaterializationSourceProtectionAdapter;
import com.nereusstream.bookkeeper.BookKeeperPrimaryPhysicalReferenceAdapter;
import com.nereusstream.bookkeeper.BookKeeperPrimaryWalAppender;
import com.nereusstream.bookkeeper.BookKeeperPrimaryWalReader;
import com.nereusstream.bookkeeper.BookKeeperReaderLeaseManager;
import com.nereusstream.bookkeeper.BookKeeperSecretRef;
import com.nereusstream.bookkeeper.BookKeeperWalConfiguration;
import com.nereusstream.bookkeeper.BookKeeperWalRuntime;
import com.nereusstream.bookkeeper.BookKeeperWriterStateMachine;
import com.nereusstream.bookkeeper.DefaultBookKeeperClientOperations;
import com.nereusstream.core.DefaultStreamStorage;
import com.nereusstream.core.StreamStorageConfig;
import com.nereusstream.core.append.AppendAdmissionGuard;
import com.nereusstream.core.capability.GenerationActivationProof;
import com.nereusstream.core.capability.GenerationActivationSubject;
import com.nereusstream.core.capability.GenerationOperation;
import com.nereusstream.core.capability.GenerationProtocolActivationGuard;
import com.nereusstream.core.physical.DefaultObjectProtectionManager;
import com.nereusstream.core.physical.DefaultObjectReadPinManager;
import com.nereusstream.core.physical.ObjectProtectionManager;
import com.nereusstream.core.physical.ObjectReadPinManager;
import com.nereusstream.core.read.ReadMetricsObserver;
import com.nereusstream.core.recovery.MetadataAppendRecoverySearcher;
import com.nereusstream.core.trim.TrimMetricsObserver;
import com.nereusstream.materialization.MaterializationConfig;
import com.nereusstream.metadata.oxia.BookKeeperMetadataStoreConfig;
import com.nereusstream.metadata.oxia.GenerationScanPage;
import com.nereusstream.metadata.oxia.OxiaClientConfiguration;
import com.nereusstream.metadata.oxia.OxiaJavaBookKeeperMetadataStore;
import com.nereusstream.metadata.oxia.OxiaJavaClientMetadataStore;
import com.nereusstream.metadata.oxia.OxiaJavaGenerationMetadataStore;
import com.nereusstream.metadata.oxia.OxiaJavaPhysicalObjectMetadataStore;
import com.nereusstream.metadata.oxia.ProjectionIdentity;
import com.nereusstream.metadata.oxia.SharedOxiaClientRuntime;
import com.nereusstream.metadata.oxia.VersionedGenerationCandidate;
import com.nereusstream.metadata.oxia.VersionedGenerationIndex;
import com.nereusstream.metadata.oxia.VersionedMaterializationCheckpoint;
import com.nereusstream.metadata.oxia.records.GenerationLifecycle;
import com.nereusstream.metadata.oxia.records.MaterializationStreamRegistrationRecord;
import com.nereusstream.objectstore.ObjectStore;
import com.nereusstream.objectstore.ObjectStoreConfiguration;
import com.nereusstream.objectstore.ObjectStoreSecretResolver;
import com.nereusstream.objectstore.S3CompatibleObjectStoreProvider;
import com.nereusstream.objectstore.wal.DefaultWalObjectReader;
import io.oxia.testcontainers.OxiaContainer;
import java.nio.charset.StandardCharsets;
import java.nio.file.Path;
import java.security.MessageDigest;
import java.time.Clock;
import java.time.Duration;
import java.util.HexFormat;
import java.util.List;
import java.util.Map;
import java.util.Optional;
import java.util.UUID;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.ScheduledExecutorService;
import org.apache.bookkeeper.client.BookKeeper;
import org.apache.bookkeeper.common.allocator.PoolingPolicy;
import org.apache.bookkeeper.conf.ServerConfiguration;
import org.apache.pulsar.metadata.bookkeeper.BKCluster;
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

/** BK-M3 end-to-end acceptance over real Oxia, BookKeeper, and S3-compatible Object storage. */
@Testcontainers
class BookKeeperAsyncObjectOxiaBkS3IntegrationTest {
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

    @TempDir
    Path temporaryDirectory;

    @Test
    void stableHeadFallsBackToBookKeeperThenFreshRuntimePublishesAndReadsExactObject() throws Exception {
        Fixture fixture = Fixture.create();
        String metadataServiceUri = "oxia://" + OXIA.getServiceAddress();
        try (BKCluster bookKeeperCluster = startBookKeeper(metadataServiceUri)) {
            StreamId stream;
            AppendResult firstAppend;
            List<byte[]> expected = List.of(new byte[] {1, 2, 3}, new byte[] {4, 5});
            try (Process first = Process.open(
                    fixture, bookKeeperCluster, "first", temporaryDirectory.resolve("first"))) {
                stream = first.storage.createOrGetStream(
                                new StreamName("persistent://tenant/namespace/" + fixture.suffix),
                                new StreamCreateOptions(
                                        StorageProfile.BOOKKEEPER_WAL_ASYNC_OBJECT, Map.of()))
                        .join()
                        .streamId();
                firstAppend = first.append(stream, expected);

                assertThat(firstAppend.commitVersion()).isPositive();
                assertThat(firstAppend.readTarget()).isInstanceOf(BookKeeperEntryRangeReadTarget.class);
                assertExactRead(first.storage, stream, expected, BookKeeperEntryRangeReadTarget.class);
                assertThat(first.committedHigherGenerations(stream)).isEmpty();
                first.register(stream, firstAppend.commitVersion());
            }

            try (Process restarted = Process.open(
                    fixture, bookKeeperCluster, "restarted", temporaryDirectory.resolve("restarted"))) {
                assertExactRead(restarted.storage, stream, expected, BookKeeperEntryRangeReadTarget.class);

                restarted.phase4.start();
                var scan = restarted.phase4.materializationService().scanNow().join();
                assertThat(scan.registrationsAdmitted()).isOne();
                assertThat(scan.plannedTasksConverged()).isOne();

                List<VersionedGenerationIndex> committed = restarted.committedHigherGenerations(stream);
                assertThat(committed).hasSize(1);
                assertThat(committed.getFirst().value().lifecycle()).isEqualTo(GenerationLifecycle.COMMITTED);
                assertExactRead(restarted.storage, stream, expected, ObjectSliceReadTarget.class);
                List<VersionedMaterializationCheckpoint> checkpoints =
                        restarted.materializationCheckpoints(stream);
                var retirementProof = restarted.phase4.committedGenerationRetirementAuthority()
                        .proveRetirement(stream, firstAppend.range(), firstAppend.commitVersion())
                        .join();
                assertThat(retirementProof)
                        .withFailMessage(
                                "committed=%s checkpoints=%s", committed, checkpoints)
                        .isPresent();
            }
        }
    }

    private static void assertExactRead(
            DefaultStreamStorage storage,
            StreamId stream,
            List<byte[]> expected,
            Class<?> targetType) {
        var read = storage.read(
                        stream,
                        0,
                        new ReadOptions(100, 1 << 20, ReadIsolation.COMMITTED, TIMEOUT))
                .join();
        assertThat(read.nextOffset()).isEqualTo(expected.size());
        assertThat(read.batches()).hasSize(expected.size());
        for (int index = 0; index < expected.size(); index++) {
            assertThat(read.batches().get(index).range()).isEqualTo(new OffsetRange(index, index + 1L));
            assertThat(read.batches().get(index).payload()).isEqualTo(expected.get(index));
            assertThat(read.batches().get(index).source().target()).isInstanceOf(targetType);
        }
    }

    private record Fixture(
            String suffix,
            String cluster,
            String deployment,
            String bucket,
            BookKeeperWalConfiguration bookKeeper,
            BookKeeperLedgerIdNamespaceReservation reservation,
            ProjectionRef projection) {
        private static Fixture create() {
            String suffix = UUID.randomUUID().toString().replace("-", "");
            String cluster = "bk-m3-" + suffix;
            String deployment = "deployment-" + suffix;
            String bucket = "nereus-bk-m3-" + suffix.substring(0, 20);
            BookKeeperWalConfiguration bookKeeper = bookKeeperConfiguration();
            createBucket(bucket);
            return new Fixture(
                    suffix,
                    cluster,
                    deployment,
                    bucket,
                    bookKeeper,
                    BookKeeperAsyncObjectOxiaBkS3IntegrationTest.reservation(bookKeeper, deployment),
                    new ProjectionRef(ProjectionType.VIRTUAL_LEDGER, "projection-" + suffix));
        }
    }

    private static final class Process implements AutoCloseable {
        private final Fixture fixture;
        private final MaterializationConfig materializationConfig;
        private final BookKeeper client;
        private final SharedOxiaClientRuntime oxiaRuntime;
        private final OxiaJavaClientMetadataStore l0;
        private final OxiaJavaBookKeeperMetadataStore bookKeeperMetadata;
        private final OxiaJavaGenerationMetadataStore generations;
        private final OxiaJavaPhysicalObjectMetadataStore physical;
        private final S3CompatibleObjectStoreProvider objectStoreProvider;
        private final ObjectStore objectStore;
        private final ObjectProtectionManager protections;
        private final ObjectReadPinManager readPins;
        private final ScheduledExecutorService scheduler;
        private final ExecutorService workers;
        private final ExecutorService callbacks;
        private final BookKeeperWalRuntime bookKeeperRuntime;
        private final Phase4ObjectWalRuntime phase4;
        private final DefaultStreamStorage storage;

        private Process(
                Fixture fixture,
                MaterializationConfig materializationConfig,
                BookKeeper client,
                SharedOxiaClientRuntime oxiaRuntime,
                OxiaJavaClientMetadataStore l0,
                OxiaJavaBookKeeperMetadataStore bookKeeperMetadata,
                OxiaJavaGenerationMetadataStore generations,
                OxiaJavaPhysicalObjectMetadataStore physical,
                S3CompatibleObjectStoreProvider objectStoreProvider,
                ObjectStore objectStore,
                ObjectProtectionManager protections,
                ObjectReadPinManager readPins,
                ScheduledExecutorService scheduler,
                ExecutorService workers,
                ExecutorService callbacks,
                BookKeeperWalRuntime bookKeeperRuntime,
                Phase4ObjectWalRuntime phase4,
                DefaultStreamStorage storage) {
            this.fixture = fixture;
            this.materializationConfig = materializationConfig;
            this.client = client;
            this.oxiaRuntime = oxiaRuntime;
            this.l0 = l0;
            this.bookKeeperMetadata = bookKeeperMetadata;
            this.generations = generations;
            this.physical = physical;
            this.objectStoreProvider = objectStoreProvider;
            this.objectStore = objectStore;
            this.protections = protections;
            this.readPins = readPins;
            this.scheduler = scheduler;
            this.workers = workers;
            this.callbacks = callbacks;
            this.bookKeeperRuntime = bookKeeperRuntime;
            this.phase4 = phase4;
            this.storage = storage;
        }

        private static Process open(
                Fixture fixture,
                BKCluster bookKeeperCluster,
                String process,
                Path processDirectory) throws Exception {
            OxiaClientConfiguration oxia = oxiaConfiguration();
            SharedOxiaClientRuntime oxiaRuntime = SharedOxiaClientRuntime.connect(oxia, CLOCK);
            BookKeeperMetadataStoreConfig metadataConfig = new BookKeeperMetadataStoreConfig(
                    fixture.bookKeeper.maxAppendRangesPerLedger(),
                    fixture.bookKeeper.protectionSlotsPerRange(),
                    fixture.bookKeeper.maxReaderLeasesPerLedger(),
                    fixture.bookKeeper.maxUncertainAllocations());
            OxiaJavaClientMetadataStore l0 = OxiaJavaClientMetadataStore.usingSharedRuntime(
                    oxia, oxiaRuntime, CLOCK, metadataConfig);
            OxiaJavaBookKeeperMetadataStore bookKeeperMetadata =
                    OxiaJavaBookKeeperMetadataStore.usingSharedRuntime(
                            oxia, oxiaRuntime, CLOCK, metadataConfig);
            OxiaJavaGenerationMetadataStore generations =
                    OxiaJavaGenerationMetadataStore.usingSharedRuntime(oxia, oxiaRuntime, CLOCK);
            OxiaJavaPhysicalObjectMetadataStore physical =
                    OxiaJavaPhysicalObjectMetadataStore.usingSharedRuntime(oxia, oxiaRuntime, CLOCK);
            StreamStorageConfig storageConfig = StreamStorageConfig.defaults(
                    fixture.cluster, "writer-" + process);
            BookKeeper client = bookKeeperCluster.newClient();
            BookKeeperClientOperations operations = new DefaultBookKeeperClientOperations(client);
            BookKeeperLedgerIdNamespaceReservationVerifier namespace =
                    new BookKeeperLedgerIdNamespaceReservationVerifier(
                            (scope, bits, prefix, timeout) -> CompletableFuture.completedFuture(
                                    Optional.of(fixture.reservation)),
                            fixture.deployment);
            byte[] password = "bk-m3-secret".getBytes(StandardCharsets.UTF_8);
            BookKeeperWriterStateMachine writer = new BookKeeperWriterStateMachine(
                    fixture.cluster,
                    fixture.bookKeeper,
                    bookKeeperMetadata,
                    CLOCK,
                    storageConfig.processRunId());
            BookKeeperLedgerAllocator allocator = new BookKeeperLedgerAllocator(
                    fixture.cluster,
                    fixture.bookKeeper,
                    bookKeeperMetadata,
                    bookKeeperMetadata,
                    namespace,
                    operations,
                    ignored -> password.clone(),
                    writer,
                    CLOCK);
            BookKeeperLedgerRecovery recovery = new BookKeeperLedgerRecovery(
                    fixture.cluster,
                    fixture.bookKeeper,
                    bookKeeperMetadata,
                    bookKeeperMetadata,
                    namespace,
                    operations,
                    ignored -> password.clone(),
                    writer,
                    CLOCK);
            BookKeeperPrimaryWalAppender appender = new BookKeeperPrimaryWalAppender(
                    fixture.cluster,
                    fixture.bookKeeper,
                    bookKeeperMetadata,
                    bookKeeperMetadata,
                    allocator,
                    recovery,
                    writer,
                    operations,
                    CLOCK);
            BookKeeperPrimaryWalReader reader = new BookKeeperPrimaryWalReader(
                    fixture.cluster,
                    fixture.bookKeeper,
                    bookKeeperMetadata,
                    operations,
                    ignored -> password.clone(),
                    new BookKeeperLedgerHandleCache(8, 8 * 1024, 1_024, Duration.ofMinutes(1)),
                    new BookKeeperReaderLeaseManager(
                            fixture.cluster,
                            fixture.bookKeeper,
                            bookKeeperMetadata,
                            CLOCK,
                            storageConfig.processRunId()));
            BookKeeperPrimaryPhysicalReferenceAdapter references =
                    new BookKeeperPrimaryPhysicalReferenceAdapter(
                            fixture.cluster,
                            fixture.bookKeeper,
                            bookKeeperMetadata,
                            bookKeeperMetadata,
                            CLOCK);
            BookKeeperWalRuntime bookKeeperRuntime = new BookKeeperWalRuntime(appender, reader, references);

            S3CompatibleObjectStoreProvider objectStoreProvider = new S3CompatibleObjectStoreProvider();
            ObjectStore objectStore = createObjectStore(objectStoreProvider, fixture.bucket);
            ObjectProtectionManager protections = new DefaultObjectProtectionManager(
                    fixture.cluster,
                    physical,
                    Duration.ofMinutes(2),
                    Duration.ofSeconds(1),
                    Duration.ofMinutes(5),
                    CLOCK);
            ObjectReadPinManager readPins = new DefaultObjectReadPinManager(
                    fixture.cluster,
                    storageConfig.processRunId(),
                    physical,
                    Duration.ofMinutes(2),
                    Duration.ofSeconds(1),
                    Duration.ofMinutes(5),
                    CLOCK);
            ScheduledExecutorService scheduler = Executors.newScheduledThreadPool(4);
            ExecutorService workers = Executors.newFixedThreadPool(4);
            ExecutorService callbacks = Executors.newFixedThreadPool(2);
            MaterializationConfig materializationConfig =
                    MaterializationConfig.defaults(processDirectory.resolve("staging").toAbsolutePath());
            Phase4ObjectWalRuntime phase4 = new Phase4ObjectWalRuntime(
                    fixture.cluster,
                    storageConfig.processRunId(),
                    storageConfig,
                    materializationConfig,
                    Duration.ofMinutes(1),
                    l0,
                    generations,
                    physical,
                    objectStore,
                    new DefaultWalObjectReader(objectStore),
                    bookKeeperRuntime.generationZeroPhysicalReferences(),
                    protections,
                    readPins,
                    activationGuard(),
                    List.of(bookKeeperRuntime.materializationSourceProvider(
                            new BookKeeperMaterializationSourceProtectionAdapter(
                                    fixture.cluster,
                                    fixture.bookKeeper,
                                    bookKeeperMetadata,
                                    CLOCK))),
                    scheduler,
                    workers,
                    callbacks,
                    CLOCK);
            DefaultStreamStorage storage = bookKeeperRuntime.newGenerationAwareStorage(
                    storageConfig,
                    l0,
                    new MetadataAppendRecoverySearcher(fixture.cluster, l0),
                    AppendAdmissionGuard.noOp(),
                    phase4.readComponents(),
                    CLOCK,
                    callbacks,
                    ReadMetricsObserver.noop(),
                    TrimMetricsObserver.noop());
            return new Process(
                    fixture,
                    materializationConfig,
                    client,
                    oxiaRuntime,
                    l0,
                    bookKeeperMetadata,
                    generations,
                    physical,
                    objectStoreProvider,
                    objectStore,
                    protections,
                    readPins,
                    scheduler,
                    workers,
                    callbacks,
                    bookKeeperRuntime,
                    phase4,
                    storage);
        }

        private AppendResult append(StreamId stream, List<byte[]> payloads) {
            List<AppendEntry> entries = java.util.stream.IntStream.range(0, payloads.size())
                    .mapToObj(index -> new AppendEntry(payloads.get(index), 1, index + 1L, Map.of()))
                    .toList();
            AppendBatch batch = new AppendBatch(
                    PayloadFormat.OPAQUE_RECORD_BATCH,
                    entries,
                    entries.size(),
                    entries.size(),
                    1,
                    entries.size(),
                    List.of(),
                    Map.of(),
                    Optional.empty());
            return storage.append(
                            stream,
                            batch,
                            new AppendOptions(
                                    Optional.empty(),
                                    DurabilityLevel.WAL_DURABLE,
                                    TIMEOUT,
                                    true,
                                    Map.of()))
                    .join();
        }

        private void register(StreamId stream, long commitVersion) {
            String projectionIdentity = ProjectionIdentity.encode(Optional.of(fixture.projection));
            long now = CLOCK.millis();
            generations.createOrVerifyStreamRegistration(
                            fixture.cluster,
                            new MaterializationStreamRegistrationRecord(
                                    1,
                                    stream.value(),
                                    projectionIdentity,
                                    sha256(projectionIdentity.getBytes(StandardCharsets.UTF_8)).value(),
                                    StorageProfile.BOOKKEEPER_WAL_ASYNC_OBJECT.name(),
                                    now,
                                    commitVersion,
                                    now,
                                    0))
                    .join();
        }

        private List<VersionedGenerationIndex> committedHigherGenerations(StreamId stream) {
            GenerationScanPage page = generations.scanIndex(
                            fixture.cluster,
                            stream,
                            ReadView.COMMITTED,
                            0,
                            Long.MAX_VALUE,
                            Optional.empty(),
                            100)
                    .join();
            assertThat(page.continuation()).isEmpty();
            return page.values().stream()
                    .filter(VersionedGenerationIndex.class::isInstance)
                    .map(VersionedGenerationIndex.class::cast)
                    .filter(value -> value.value().lifecycle() == GenerationLifecycle.COMMITTED)
                    .toList();
        }

        private List<VersionedMaterializationCheckpoint> materializationCheckpoints(
                StreamId stream) {
            var page = generations.scanMaterializationCheckpoints(
                            fixture.cluster, stream, Optional.empty(), 100)
                    .join();
            assertThat(page.continuation()).isEmpty();
            var exact = generations.getMaterializationCheckpoint(
                            fixture.cluster,
                            stream,
                            materializationConfig.committedPolicy().policyId(),
                            materializationConfig.committedPolicy().policyVersion())
                    .join();
            assertThat(page.values())
                    .withFailMessage("exact checkpoint=%s", exact)
                    .containsExactlyElementsOf(exact.stream().toList());
            return page.values();
        }

        @Override
        public void close() throws Exception {
            storage.close();
            phase4.close();
            bookKeeperRuntime.close();
            callbacks.shutdownNow();
            scheduler.shutdownNow();
            readPins.close();
            protections.close();
            objectStore.close();
            objectStoreProvider.close();
            physical.close();
            generations.close();
            bookKeeperMetadata.close();
            l0.close();
            oxiaRuntime.close();
            client.close();
        }
    }

    private static BookKeeperWalConfiguration bookKeeperConfiguration() {
        return new BookKeeperWalConfiguration(
                "primary",
                "11".repeat(32),
                12,
                0x801,
                "reservation-1",
                1,
                1,
                1,
                BookKeeperDigestType.CRC32C,
                new BookKeeperSecretRef("secret://bookkeeper/password", "v1"),
                8,
                1024 * 1024,
                8,
                8,
                32,
                16,
                Duration.ofHours(1),
                1,
                8,
                8L * 1024 * 1024,
                TIMEOUT,
                TIMEOUT,
                TIMEOUT,
                TIMEOUT,
                Duration.ofMinutes(2),
                Duration.ofSeconds(30),
                Duration.ofMinutes(1),
                64);
    }

    private static BookKeeperLedgerIdNamespaceReservation reservation(
            BookKeeperWalConfiguration configuration,
            String deployment) {
        return new BookKeeperLedgerIdNamespaceReservation(
                1,
                configuration.ledgerIdNamespaceReservationId(),
                deployment,
                configuration.clusterAlias(),
                configuration.providerScopeSha256(),
                configuration.ledgerIdPrefixBits(),
                configuration.ledgerIdPrefixValue(),
                BookKeeperLedgerIdNamespaceReservation.Lifecycle.ACTIVE,
                1,
                1,
                0,
                "a".repeat(64),
                1,
                sha('b'),
                "/bookkeeper/ledger-id-namespace/reservation-1");
    }

    private static GenerationProtocolActivationGuard activationGuard() {
        return new GenerationProtocolActivationGuard() {
            @Override
            public CompletableFuture<GenerationActivationProof> requireReady(
                    GenerationOperation operation,
                    GenerationActivationSubject subject,
                    boolean activateLiveProjectionIfAbsent) {
                return CompletableFuture.completedFuture(GenerationActivationProof.create(
                        operation,
                        subject,
                        1,
                        1,
                        1,
                        sha('f'),
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

    private static Checksum sha(char value) {
        return new Checksum(ChecksumType.SHA256, Character.toString(value).repeat(64));
    }

    private static Checksum sha256(byte[] bytes) {
        try {
            return new Checksum(
                    ChecksumType.SHA256,
                    HexFormat.of().formatHex(MessageDigest.getInstance("SHA-256").digest(bytes)));
        } catch (Exception failure) {
            throw new IllegalStateException(failure);
        }
    }

    private static BKCluster startBookKeeper(String metadataServiceUri) throws Exception {
        ServerConfiguration configuration = new ServerConfiguration();
        configuration.setProperty("dbStorage_writeCacheMaxSizeMb", 32);
        configuration.setProperty("dbStorage_readAheadCacheMaxSizeMb", 4);
        configuration.setProperty("dbStorage_rocksDB_writeBufferSizeMB", 4);
        configuration.setProperty("dbStorage_rocksDB_blockCacheSize", 4 * 1024 * 1024);
        configuration.setJournalSyncData(false);
        configuration.setJournalWriteData(false);
        configuration.setProperty("journalMaxGroupWaitMSec", 0L);
        configuration.setProperty("journalPreAllocSizeMB", 1);
        configuration.setFlushInterval(60_000);
        configuration.setGcWaitTime(60_000);
        configuration.setAllowLoopback(true);
        configuration.setAdvertisedAddress("127.0.0.1");
        configuration.setAllowEphemeralPorts(true);
        configuration.setNumAddWorkerThreads(0);
        configuration.setNumReadWorkerThreads(0);
        configuration.setNumHighPriorityWorkerThreads(0);
        configuration.setNumJournalCallbackThreads(0);
        configuration.setServerNumIOThreads(1);
        configuration.setNumLongPollWorkerThreads(1);
        configuration.setAllocatorPoolingPolicy(PoolingPolicy.UnpooledHeap);
        configuration.setLedgerStorageClass("org.apache.bookkeeper.bookie.storage.ldb.DbLedgerStorage");
        configuration.setDiskUsageThreshold(0.999F);
        configuration.setDiskUsageWarnThreshold(0.99F);
        return BKCluster.builder()
                .baseServerConfiguration(configuration)
                .metadataServiceUri(metadataServiceUri)
                .numBookies(1)
                .clearOldData(true)
                .build();
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
                .serviceConfiguration(S3Configuration.builder().pathStyleAccessEnabled(true).build())
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
            throw new IllegalStateException("cannot create LocalStack Object store", failure);
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
}
