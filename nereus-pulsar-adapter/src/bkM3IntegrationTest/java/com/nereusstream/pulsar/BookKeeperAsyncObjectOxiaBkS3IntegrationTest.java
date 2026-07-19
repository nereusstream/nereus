/* Licensed under the Apache License, Version 2.0 */
package com.nereusstream.pulsar;

import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.assertThatThrownBy;

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
import com.nereusstream.api.ReadBatch;
import com.nereusstream.api.ReadIsolation;
import com.nereusstream.api.ReadOptions;
import com.nereusstream.api.ReadView;
import com.nereusstream.api.StorageProfile;
import com.nereusstream.api.StreamCreateOptions;
import com.nereusstream.api.StreamId;
import com.nereusstream.api.StreamName;
import com.nereusstream.api.target.BookKeeperEntryRangeReadTarget;
import com.nereusstream.api.target.ObjectSliceReadTarget;
import com.nereusstream.bookkeeper.BookKeeperAsyncObjectRetirementAuthority;
import com.nereusstream.bookkeeper.BookKeeperClientOperations;
import com.nereusstream.bookkeeper.BookKeeperDigestType;
import com.nereusstream.bookkeeper.BookKeeperLedgerAllocator;
import com.nereusstream.bookkeeper.BookKeeperLedgerGcAction;
import com.nereusstream.bookkeeper.BookKeeperLedgerGcConfiguration;
import com.nereusstream.bookkeeper.BookKeeperLedgerHandleCache;
import com.nereusstream.bookkeeper.BookKeeperLedgerIdNamespaceReservation;
import com.nereusstream.bookkeeper.BookKeeperLedgerIdNamespaceReservationVerifier;
import com.nereusstream.bookkeeper.BookKeeperLedgerRecovery;
import com.nereusstream.bookkeeper.BookKeeperLedgerRetentionManager;
import com.nereusstream.bookkeeper.BookKeeperMaterializationSourceProtectionAdapter;
import com.nereusstream.bookkeeper.BookKeeperOperationDeadline;
import com.nereusstream.bookkeeper.BookKeeperPrimaryPhysicalReferenceAdapter;
import com.nereusstream.bookkeeper.BookKeeperPrimaryWalAppender;
import com.nereusstream.bookkeeper.BookKeeperPrimaryWalReader;
import com.nereusstream.bookkeeper.BookKeeperProtocolActivationProof;
import com.nereusstream.bookkeeper.BookKeeperReaderLeaseManager;
import com.nereusstream.bookkeeper.BookKeeperSecretRef;
import com.nereusstream.bookkeeper.BookKeeperWalConfiguration;
import com.nereusstream.bookkeeper.BookKeeperWalOnlyReferenceRetirementCoordinator;
import com.nereusstream.bookkeeper.BookKeeperWalOnlyRetirementAuthority;
import com.nereusstream.bookkeeper.BookKeeperWalReferenceManager;
import com.nereusstream.bookkeeper.BookKeeperWalRetentionGate;
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
import com.nereusstream.metadata.oxia.BookKeeperScanToken;
import com.nereusstream.metadata.oxia.BookKeeperVersionedValue;
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
import com.nereusstream.metadata.oxia.records.BookKeeperLedgerLifecycle;
import com.nereusstream.metadata.oxia.records.BookKeeperLedgerProtectionRecord;
import com.nereusstream.metadata.oxia.records.BookKeeperProtectionType;
import com.nereusstream.metadata.oxia.records.GenerationLifecycle;
import com.nereusstream.metadata.oxia.records.MaterializationStreamRegistrationRecord;
import com.nereusstream.metadata.oxia.records.ProtectionLifecycle;
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
import java.time.Instant;
import java.time.ZoneId;
import java.util.ArrayList;
import java.util.HexFormat;
import java.util.List;
import java.util.Map;
import java.util.Optional;
import java.util.UUID;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.ScheduledExecutorService;
import java.util.concurrent.atomic.AtomicLong;
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
            AppendResult secondAppend;
            List<byte[]> firstPayloads = List.of(new byte[] {1, 2, 3}, new byte[] {4, 5});
            List<byte[]> expected = List.of(
                    firstPayloads.get(0), firstPayloads.get(1), new byte[] {6, 7, 8, 9});
            try (Process first = Process.open(
                    fixture, bookKeeperCluster, "first", temporaryDirectory.resolve("first"))) {
                stream = first.storage.createOrGetStream(
                                new StreamName("persistent://tenant/namespace/" + fixture.suffix),
                                new StreamCreateOptions(
                                        StorageProfile.BOOKKEEPER_WAL_ASYNC_OBJECT, Map.of()))
                        .join()
                        .streamId();
                firstAppend = first.append(stream, firstPayloads);
                secondAppend = first.append(stream, List.of(expected.get(2)));

                assertThat(firstAppend.commitVersion()).isPositive();
                assertThat(firstAppend.readTarget()).isInstanceOf(BookKeeperEntryRangeReadTarget.class);
                assertThat(((BookKeeperEntryRangeReadTarget) secondAppend.readTarget()).ledgerId())
                        .isNotEqualTo(((BookKeeperEntryRangeReadTarget) firstAppend.readTarget()).ledgerId());
                assertExactRead(first.storage, stream, expected, BookKeeperEntryRangeReadTarget.class);
                assertThat(first.committedHigherGenerations(stream)).isEmpty();
                first.register(stream, secondAppend.commitVersion());
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

                long retiredLedgerId = ((BookKeeperEntryRangeReadTarget) firstAppend.readTarget()).ledgerId();
                var sealedRoot = restarted.bookKeeperMetadata.getRoot(
                                fixture.cluster,
                                fixture.bookKeeper.providerScopeSha256(),
                                retiredLedgerId)
                        .join()
                        .orElseThrow();
                assertThat(sealedRoot.value().lifecycle()).isEqualTo(BookKeeperLedgerLifecycle.SEALED);
                assertThat(restarted.bookKeeperProtections(retiredLedgerId))
                        .extracting(value -> value.value().protectionType())
                        .contains(BookKeeperProtectionType.MATERIALIZATION_SOURCE);

                fixture.clock.advance(restarted.materializationConfig.metadataAuditGrace().plusMillis(1));
                var retirementScan = restarted.phase4.materializationService().scanNow().join();
                assertThat(retirementScan.registrationsAdmitted()).isOne();
                assertThat(retirementScan.workflowMetadataRetired()).isPositive();
                assertThat(restarted.bookKeeperProtections(retiredLedgerId))
                        .extracting(value -> value.value().protectionType())
                        .doesNotContain(BookKeeperProtectionType.MATERIALIZATION_SOURCE);

                BookKeeperWalOnlyRetirementAuthority common = new BookKeeperWalOnlyRetirementAuthority(
                        fixture.cluster, restarted.l0, restarted.bookKeeperMetadata);
                BookKeeperAsyncObjectRetirementAuthority authority =
                        new BookKeeperAsyncObjectRetirementAuthority(
                                fixture.cluster,
                                fixture.bookKeeper,
                                common,
                                restarted.phase4.committedGenerationRetirementAuthority(),
                                restarted.bookKeeperMetadata);
                BookKeeperWalReferenceManager references = new BookKeeperWalReferenceManager(
                        fixture.cluster, fixture.bookKeeper, restarted.bookKeeperMetadata, authority);
                var retired = new BookKeeperWalOnlyReferenceRetirementCoordinator(
                                fixture.cluster,
                                fixture.bookKeeper,
                                restarted.bookKeeperMetadata,
                                authority,
                                references)
                        .retireEligible(sealedRoot, TIMEOUT)
                        .join();
                assertThat(retired.scannedProtections()).isEqualTo(3);
                assertThat(retired.newlyRetiredProtections()).isEqualTo(3);
                assertThat(retired.fullyRetired()).isTrue();
                assertThat(restarted.bookKeeperProtections(retiredLedgerId))
                        .extracting(value -> value.value().lifecycle())
                        .containsOnly(ProtectionLifecycle.RETIRED);

                BookKeeperLedgerGcConfiguration gc = new BookKeeperLedgerGcConfiguration(
                        1, Duration.ZERO, Duration.ofMinutes(2), Duration.ofSeconds(10), true, false);
                BookKeeperProtocolActivationProof activation = activation(
                        fixture.bookKeeper, fixture.reservation);
                BookKeeperWalRetentionGate gate = new BookKeeperWalRetentionGate(
                        fixture.cluster,
                        fixture.bookKeeper,
                        gc,
                        restarted.bookKeeperMetadata,
                        restarted.bookKeeperMetadata,
                        restarted.namespaceVerifier,
                        ignored -> CompletableFuture.completedFuture(activation),
                        restarted.rawOperations,
                        fixture.clock);
                BookKeeperLedgerRetentionManager retention = new BookKeeperLedgerRetentionManager(
                        fixture.cluster,
                        fixture.bookKeeper,
                        gc,
                        restarted.bookKeeperMetadata,
                        restarted.namespaceVerifier,
                        ignored -> CompletableFuture.completedFuture(activation),
                        restarted.rawOperations,
                        gate,
                        fixture.clock);
                var candidate = gate.evaluate(sealedRoot, TIMEOUT).join().candidate().orElseThrow();
                var marked = retention.mark(candidate, TIMEOUT).join();
                assertThat(marked.action()).isEqualTo(BookKeeperLedgerGcAction.MARKED);
                fixture.clock.advance(Duration.ofMinutes(2).plusMillis(1));
                var deleting = retention.converge(marked.root().orElseThrow(), TIMEOUT).join();
                assertThat(deleting.action()).isEqualTo(BookKeeperLedgerGcAction.DELETING);
                var firstAbsence = retention.converge(deleting.root().orElseThrow(), TIMEOUT).join();
                assertThat(firstAbsence.action()).isEqualTo(BookKeeperLedgerGcAction.FIRST_ABSENCE_RECORDED);
                fixture.clock.advance(Duration.ofSeconds(10).plusMillis(1));
                var deleted = retention.converge(firstAbsence.root().orElseThrow(), TIMEOUT).join();
                assertThat(deleted.action()).isEqualTo(BookKeeperLedgerGcAction.DELETED);
                assertThatThrownBy(() -> restarted.rawOperations
                                .metadata(retiredLedgerId, new BookKeeperOperationDeadline(TIMEOUT))
                                .join())
                        .hasCauseInstanceOf(com.nereusstream.api.NereusException.class);
                assertExactRead(restarted.storage, stream, expected, ObjectSliceReadTarget.class);
            }
        }
    }

    private static void assertExactRead(
            DefaultStreamStorage storage,
            StreamId stream,
            List<byte[]> expected,
            Class<?> targetType) {
        long nextOffset = 0;
        List<ReadBatch> batches = new ArrayList<>();
        while (nextOffset < expected.size()) {
            var read = storage.read(
                            stream,
                            nextOffset,
                            new ReadOptions(100, 1 << 20, ReadIsolation.COMMITTED, TIMEOUT))
                    .join();
            assertThat(read.nextOffset()).isGreaterThan(nextOffset);
            batches.addAll(read.batches());
            nextOffset = read.nextOffset();
        }
        assertThat(nextOffset).isEqualTo(expected.size());
        assertThat(batches).hasSize(expected.size());
        for (int index = 0; index < expected.size(); index++) {
            assertThat(batches.get(index).range()).isEqualTo(new OffsetRange(index, index + 1L));
            assertThat(batches.get(index).payload()).isEqualTo(expected.get(index));
            assertThat(batches.get(index).source().target()).isInstanceOf(targetType);
        }
    }

    private record Fixture(
            String suffix,
            String cluster,
            String deployment,
            String bucket,
            BookKeeperWalConfiguration bookKeeper,
            BookKeeperLedgerIdNamespaceReservation reservation,
            ProjectionRef projection,
            MutableClock clock) {
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
                    new ProjectionRef(ProjectionType.VIRTUAL_LEDGER, "projection-" + suffix),
                    new MutableClock(1_000_000));
        }
    }

    private static final class Process implements AutoCloseable {
        private final Fixture fixture;
        private final MaterializationConfig materializationConfig;
        private final BookKeeper client;
        private final BookKeeperClientOperations rawOperations;
        private final BookKeeperLedgerIdNamespaceReservationVerifier namespaceVerifier;
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
                BookKeeperClientOperations rawOperations,
                BookKeeperLedgerIdNamespaceReservationVerifier namespaceVerifier,
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
            this.rawOperations = rawOperations;
            this.namespaceVerifier = namespaceVerifier;
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
            SharedOxiaClientRuntime oxiaRuntime = SharedOxiaClientRuntime.connect(oxia, fixture.clock);
            BookKeeperMetadataStoreConfig metadataConfig = new BookKeeperMetadataStoreConfig(
                    fixture.bookKeeper.maxAppendRangesPerLedger(),
                    fixture.bookKeeper.protectionSlotsPerRange(),
                    fixture.bookKeeper.maxReaderLeasesPerLedger(),
                    fixture.bookKeeper.maxUncertainAllocations());
            OxiaJavaClientMetadataStore l0 = OxiaJavaClientMetadataStore.usingSharedRuntime(
                    oxia, oxiaRuntime, fixture.clock, metadataConfig);
            OxiaJavaBookKeeperMetadataStore bookKeeperMetadata =
                    OxiaJavaBookKeeperMetadataStore.usingSharedRuntime(
                            oxia, oxiaRuntime, fixture.clock, metadataConfig);
            OxiaJavaGenerationMetadataStore generations =
                    OxiaJavaGenerationMetadataStore.usingSharedRuntime(oxia, oxiaRuntime, fixture.clock);
            OxiaJavaPhysicalObjectMetadataStore physical =
                    OxiaJavaPhysicalObjectMetadataStore.usingSharedRuntime(oxia, oxiaRuntime, fixture.clock);
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
                    fixture.clock,
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
                    fixture.clock);
            BookKeeperLedgerRecovery recovery = new BookKeeperLedgerRecovery(
                    fixture.cluster,
                    fixture.bookKeeper,
                    bookKeeperMetadata,
                    bookKeeperMetadata,
                    namespace,
                    operations,
                    ignored -> password.clone(),
                    writer,
                    fixture.clock);
            BookKeeperPrimaryWalAppender appender = new BookKeeperPrimaryWalAppender(
                    fixture.cluster,
                    fixture.bookKeeper,
                    bookKeeperMetadata,
                    bookKeeperMetadata,
                    allocator,
                    recovery,
                    writer,
                    operations,
                    fixture.clock);
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
                            fixture.clock,
                            storageConfig.processRunId()));
            BookKeeperPrimaryPhysicalReferenceAdapter references =
                    new BookKeeperPrimaryPhysicalReferenceAdapter(
                            fixture.cluster,
                            fixture.bookKeeper,
                            bookKeeperMetadata,
                            bookKeeperMetadata,
                            fixture.clock);
            BookKeeperWalRuntime bookKeeperRuntime = new BookKeeperWalRuntime(appender, reader, references);

            S3CompatibleObjectStoreProvider objectStoreProvider = new S3CompatibleObjectStoreProvider();
            ObjectStore objectStore = createObjectStore(objectStoreProvider, fixture.bucket);
            ObjectProtectionManager protections = new DefaultObjectProtectionManager(
                    fixture.cluster,
                    physical,
                    Duration.ofMinutes(2),
                    Duration.ofSeconds(1),
                    Duration.ofMinutes(5),
                    fixture.clock);
            ObjectReadPinManager readPins = new DefaultObjectReadPinManager(
                    fixture.cluster,
                    storageConfig.processRunId(),
                    physical,
                    Duration.ofMinutes(2),
                    Duration.ofSeconds(1),
                    Duration.ofMinutes(5),
                    fixture.clock);
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
                    activationGuard(fixture.clock),
                    List.of(bookKeeperRuntime.materializationSourceProvider(
                            new BookKeeperMaterializationSourceProtectionAdapter(
                                    fixture.cluster,
                                    fixture.bookKeeper,
                                    bookKeeperMetadata,
                                    fixture.clock))),
                    scheduler,
                    workers,
                    callbacks,
                    fixture.clock);
            DefaultStreamStorage storage = bookKeeperRuntime.newGenerationAwareStorage(
                    storageConfig,
                    l0,
                    new MetadataAppendRecoverySearcher(fixture.cluster, l0),
                    AppendAdmissionGuard.noOp(),
                    phase4.readComponents(),
                    fixture.clock,
                    callbacks,
                    ReadMetricsObserver.noop(),
                    TrimMetricsObserver.noop());
            return new Process(
                    fixture,
                    materializationConfig,
                    client,
                    operations,
                    namespace,
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
            long now = fixture.clock.millis();
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

        private List<BookKeeperVersionedValue<BookKeeperLedgerProtectionRecord>> bookKeeperProtections(
                long ledgerId) {
            var page = bookKeeperMetadata.scanProtections(
                            fixture.cluster,
                            fixture.bookKeeper.providerScopeSha256(),
                            ledgerId,
                            Optional.<BookKeeperScanToken>empty(),
                            100)
                    .join();
            assertThat(page.continuation()).isEmpty();
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
                2,
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

    private static GenerationProtocolActivationGuard activationGuard(Clock clock) {
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
                        clock.millis()));
            }

            @Override
            public CompletableFuture<Void> revalidate(GenerationActivationProof proof) {
                return CompletableFuture.completedFuture(null);
            }
        };
    }

    private static BookKeeperProtocolActivationProof activation(
            BookKeeperWalConfiguration configuration,
            BookKeeperLedgerIdNamespaceReservation reservation) {
        return new BookKeeperProtocolActivationProof(
                1,
                configuration.clusterAlias(),
                configuration.providerScopeSha256(),
                configuration.configurationBindingSha256().value(),
                reservation.ledgerIdNamespaceSha256().value(),
                1,
                sha('1'),
                sha('2'),
                sha('3'),
                sha('4'),
                true,
                1,
                sha('5'));
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

    private static final class MutableClock extends Clock {
        private final AtomicLong millis;

        private MutableClock(long millis) {
            this.millis = new AtomicLong(millis);
        }

        private void advance(Duration duration) {
            millis.updateAndGet(value -> Math.addExact(value, duration.toMillis()));
        }

        @Override
        public ZoneId getZone() {
            return ZoneId.of("UTC");
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
