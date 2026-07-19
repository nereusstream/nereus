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
import com.nereusstream.api.PayloadFormat;
import com.nereusstream.api.ReadIsolation;
import com.nereusstream.api.ReadOptions;
import com.nereusstream.api.ResolveOptions;
import com.nereusstream.api.StorageProfile;
import com.nereusstream.api.StreamCreateOptions;
import com.nereusstream.api.StreamId;
import com.nereusstream.api.StreamName;
import com.nereusstream.api.TrimOptions;
import com.nereusstream.api.target.BookKeeperEntryRangeReadTarget;
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
import com.nereusstream.core.read.ReadMetricsObserver;
import com.nereusstream.core.recovery.MetadataAppendRecoverySearcher;
import com.nereusstream.core.trim.TrimMetricsObserver;
import com.nereusstream.metadata.oxia.BookKeeperMetadataStoreConfig;
import com.nereusstream.metadata.oxia.OxiaClientConfiguration;
import com.nereusstream.metadata.oxia.OxiaJavaBookKeeperMetadataStore;
import com.nereusstream.metadata.oxia.OxiaJavaClientMetadataStore;
import com.nereusstream.metadata.oxia.SharedOxiaClientRuntime;
import com.nereusstream.metadata.oxia.BookKeeperVersionedValue;
import com.nereusstream.metadata.oxia.records.BookKeeperLedgerRootRecord;
import io.netty.buffer.ByteBuf;
import io.oxia.testcontainers.OxiaContainer;
import java.nio.charset.StandardCharsets;
import java.time.Clock;
import java.time.Duration;
import java.time.Instant;
import java.time.ZoneId;
import java.util.List;
import java.util.Map;
import java.util.Optional;
import java.util.UUID;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.atomic.AtomicBoolean;
import org.apache.bookkeeper.client.BookKeeper;
import org.apache.bookkeeper.client.api.LedgerEntries;
import org.apache.bookkeeper.client.api.LedgerMetadata;
import org.apache.bookkeeper.client.api.ReadHandle;
import org.apache.bookkeeper.client.api.WriteAdvHandle;
import org.apache.bookkeeper.common.allocator.PoolingPolicy;
import org.apache.bookkeeper.conf.ServerConfiguration;
import org.apache.pulsar.metadata.bookkeeper.BKCluster;
import org.junit.jupiter.api.Test;
import org.testcontainers.junit.jupiter.Container;
import org.testcontainers.junit.jupiter.Testcontainers;
import org.testcontainers.utility.DockerImageName;

/** BK-M2 destructive acceptance over production metadata and provider adapters. */
@Testcontainers
class BookKeeperWalOnlyOxiaBkIntegrationTest {
    private static final Duration TIMEOUT = Duration.ofSeconds(30);
    private static final DockerImageName OXIA_IMAGE = DockerImageName.parse("oxia/oxia:0.16.3");

    @Container
    private static final OxiaContainer OXIA = new OxiaContainer(OXIA_IMAGE).withShards(4);

    @Test
    void restartPreservesExactTargetsAndLostDeleteResponseConvergesAfterRollover() throws Exception {
        String suffix = UUID.randomUUID().toString().replace("-", "");
        String cluster = "bk-m2-" + suffix;
        String deployment = "deployment-" + suffix;
        MutableClock clock = new MutableClock(1_000_000);
        BookKeeperWalConfiguration configuration = configuration();
        BookKeeperLedgerIdNamespaceReservation reservation = reservation(configuration, deployment);
        String metadataServiceUri = "oxia://" + OXIA.getServiceAddress();

        try (BKCluster bookKeeperCluster = startBookKeeper(metadataServiceUri)) {
            StreamId streamId;
            List<AppendResult> firstResults;
            BookKeeperVersionedValue<BookKeeperLedgerRootRecord> firstAbsenceRoot;
            long retiredLedgerId;
            try (Process first = Process.open(
                    bookKeeperCluster, cluster, deployment, "process-first", configuration, reservation, clock)) {
                streamId = first.storage.createOrGetStream(
                                new StreamName("persistent://tenant/namespace/bk-m2-" + suffix),
                                new StreamCreateOptions(StorageProfile.BOOKKEEPER_WAL_ONLY, Map.of()))
                        .join()
                        .streamId();
                firstResults = List.of(
                        first.append(streamId, new byte[] {1, 2}),
                        first.append(streamId, new byte[] {3}),
                        first.append(streamId, new byte[] {4, 5, 6}));

                assertThat(firstResults).extracting(result -> result.range().startOffset())
                        .containsExactly(0L, 1L, 2L);
                BookKeeperEntryRangeReadTarget firstTarget = target(firstResults.get(0));
                assertThat(target(firstResults.get(1)).ledgerId()).isEqualTo(firstTarget.ledgerId());
                assertThat(target(firstResults.get(2)).ledgerId()).isNotEqualTo(firstTarget.ledgerId());
                assertRead(first, streamId, List.of(new byte[] {1, 2}, new byte[] {3}, new byte[] {4, 5, 6}));
            }

            try (Process restarted = Process.open(
                    bookKeeperCluster, cluster, deployment, "process-restarted", configuration, reservation, clock)) {
                assertRead(restarted, streamId,
                        List.of(new byte[] {1, 2}, new byte[] {3}, new byte[] {4, 5, 6}));
                var resolved = restarted.storage.resolve(streamId, 0, new ResolveOptions(10, false, true)).join();
                assertThat(resolved.ranges()).hasSize(3);
                assertThat(resolved.ranges()).extracting(range -> range.readTarget())
                        .containsExactly(
                                firstResults.get(0).readTarget(),
                                firstResults.get(1).readTarget(),
                                firstResults.get(2).readTarget());

                AppendResult afterRestart = restarted.append(streamId, new byte[] {7});
                assertThat(afterRestart.range().startOffset()).isEqualTo(3);
                assertThat(target(afterRestart).ledgerId()).isNotEqualTo(target(firstResults.get(2)).ledgerId());

                retiredLedgerId = target(firstResults.get(0)).ledgerId();
                var sealedRoot = restarted.bookKeeperMetadata.getRoot(
                                cluster, configuration.providerScopeSha256(), retiredLedgerId)
                        .join()
                        .orElseThrow();
                assertThat(sealedRoot.value().lifecycle().name()).isEqualTo("SEALED");

                restarted.storage.trim(streamId, 2, new TrimOptions(TIMEOUT, "bk-m2 destructive acceptance"))
                        .join();
                BookKeeperWalOnlyRetirementAuthority authority = new BookKeeperWalOnlyRetirementAuthority(
                        cluster, restarted.l0, restarted.bookKeeperMetadata);
                BookKeeperWalReferenceManager references = new BookKeeperWalReferenceManager(
                        cluster, configuration, restarted.bookKeeperMetadata, authority);
                var retired = new BookKeeperWalOnlyReferenceRetirementCoordinator(
                                cluster, configuration, restarted.bookKeeperMetadata, authority, references)
                        .retireEligible(sealedRoot, TIMEOUT)
                        .join();
                assertThat(retired.scannedProtections()).isEqualTo(6);
                assertThat(retired.newlyRetiredProtections()).isEqualTo(6);
                assertThat(retired.fullyRetired()).isTrue();

                BookKeeperLedgerGcConfiguration gc = new BookKeeperLedgerGcConfiguration(
                        1, Duration.ZERO, Duration.ofMinutes(2), Duration.ofSeconds(10), true, false);
                BookKeeperProtocolActivationProof activation = activation(configuration, reservation);
                LostDeleteResponseOperations deletionOperations =
                        new LostDeleteResponseOperations(restarted.rawOperations);
                BookKeeperWalRetentionGate gate = new BookKeeperWalRetentionGate(
                        cluster,
                        configuration,
                        gc,
                        restarted.bookKeeperMetadata,
                        restarted.bookKeeperMetadata,
                        restarted.namespaceVerifier,
                        ignored -> CompletableFuture.completedFuture(activation),
                        deletionOperations,
                        clock);
                BookKeeperLedgerRetentionManager retention = new BookKeeperLedgerRetentionManager(
                        cluster,
                        configuration,
                        gc,
                        restarted.bookKeeperMetadata,
                        restarted.namespaceVerifier,
                        ignored -> CompletableFuture.completedFuture(activation),
                        deletionOperations,
                        gate,
                        clock);

                var candidate = gate.evaluate(sealedRoot, TIMEOUT).join().candidate().orElseThrow();
                var marked = retention.mark(candidate, TIMEOUT).join();
                assertThat(marked.action()).isEqualTo(BookKeeperLedgerGcAction.MARKED);
                assertThat(retention.converge(marked.root().orElseThrow(), TIMEOUT).join().action())
                        .isEqualTo(BookKeeperLedgerGcAction.WAITING_DRAIN);

                clock.advance(Duration.ofMinutes(2).plusMillis(1));
                var deleting = retention.converge(marked.root().orElseThrow(), TIMEOUT).join();
                assertThat(deleting.action()).isEqualTo(BookKeeperLedgerGcAction.DELETING);
                var firstAbsence = retention.converge(deleting.root().orElseThrow(), TIMEOUT).join();
                assertThat(firstAbsence.action()).isEqualTo(BookKeeperLedgerGcAction.FIRST_ABSENCE_RECORDED);
                assertThat(deletionOperations.lostResponseInjected()).isTrue();
                assertThat(retention.converge(firstAbsence.root().orElseThrow(), TIMEOUT).join().action())
                        .isEqualTo(BookKeeperLedgerGcAction.WAITING_SECOND_ABSENCE);
                firstAbsenceRoot = firstAbsence.root().orElseThrow();
            }

            clock.advance(Duration.ofSeconds(10).plusMillis(1));
            try (Process gcRestarted = Process.open(
                    bookKeeperCluster, cluster, deployment, "process-gc-restarted",
                    configuration, reservation, clock)) {
                BookKeeperLedgerGcConfiguration gc = new BookKeeperLedgerGcConfiguration(
                        1, Duration.ZERO, Duration.ofMinutes(2), Duration.ofSeconds(10), true, false);
                BookKeeperProtocolActivationProof activation = activation(configuration, reservation);
                BookKeeperWalRetentionGate gate = new BookKeeperWalRetentionGate(
                        cluster,
                        configuration,
                        gc,
                        gcRestarted.bookKeeperMetadata,
                        gcRestarted.bookKeeperMetadata,
                        gcRestarted.namespaceVerifier,
                        ignored -> CompletableFuture.completedFuture(activation),
                        gcRestarted.rawOperations,
                        clock);
                BookKeeperLedgerRetentionManager retention = new BookKeeperLedgerRetentionManager(
                        cluster,
                        configuration,
                        gc,
                        gcRestarted.bookKeeperMetadata,
                        gcRestarted.namespaceVerifier,
                        ignored -> CompletableFuture.completedFuture(activation),
                        gcRestarted.rawOperations,
                        gate,
                        clock);
                var deleted = retention.converge(firstAbsenceRoot, TIMEOUT).join();
                assertThat(deleted.action()).isEqualTo(BookKeeperLedgerGcAction.DELETED);
                assertThatThrownBy(() -> gcRestarted.rawOperations
                                .metadata(retiredLedgerId, new BookKeeperOperationDeadline(TIMEOUT))
                                .join())
                        .hasCauseInstanceOf(com.nereusstream.api.NereusException.class);

                assertRead(gcRestarted, streamId, List.of(new byte[] {4, 5, 6}, new byte[] {7}), 2);
            }
        }
    }

    private static void assertRead(Process process, StreamId streamId, List<byte[]> payloads) {
        assertRead(process, streamId, payloads, 0);
    }

    private static void assertRead(Process process, StreamId streamId, List<byte[]> payloads, long startOffset) {
        var read = process.storage.read(
                        streamId, startOffset, new ReadOptions(100, 1024, ReadIsolation.COMMITTED, TIMEOUT))
                .join();
        assertThat(read.nextOffset()).isEqualTo(startOffset + payloads.size());
        assertThat(read.batches()).hasSize(payloads.size());
        for (int index = 0; index < payloads.size(); index++) {
            assertThat(read.batches().get(index).payload()).containsExactly(payloads.get(index));
            assertThat(read.batches().get(index).source().target())
                    .isInstanceOf(BookKeeperEntryRangeReadTarget.class);
        }
    }

    private static BookKeeperEntryRangeReadTarget target(AppendResult result) {
        return (BookKeeperEntryRangeReadTarget) result.readTarget();
    }

    private static BookKeeperWalConfiguration configuration() {
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
                2,
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
            BookKeeperWalConfiguration configuration, String deployment) {
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

    private static final class Process implements AutoCloseable {
        private final BookKeeper client;
        private final SharedOxiaClientRuntime oxiaRuntime;
        private final OxiaJavaClientMetadataStore l0;
        private final OxiaJavaBookKeeperMetadataStore bookKeeperMetadata;
        private final BookKeeperLedgerIdNamespaceReservationVerifier namespaceVerifier;
        private final DefaultBookKeeperClientOperations rawOperations;
        private final BookKeeperWalRuntime runtime;
        private final DefaultStreamStorage storage;

        private Process(
                BookKeeper client,
                SharedOxiaClientRuntime oxiaRuntime,
                OxiaJavaClientMetadataStore l0,
                OxiaJavaBookKeeperMetadataStore bookKeeperMetadata,
                BookKeeperLedgerIdNamespaceReservationVerifier namespaceVerifier,
                DefaultBookKeeperClientOperations rawOperations,
                BookKeeperWalRuntime runtime,
                DefaultStreamStorage storage) {
            this.client = client;
            this.oxiaRuntime = oxiaRuntime;
            this.l0 = l0;
            this.bookKeeperMetadata = bookKeeperMetadata;
            this.namespaceVerifier = namespaceVerifier;
            this.rawOperations = rawOperations;
            this.runtime = runtime;
            this.storage = storage;
        }

        private static Process open(
                BKCluster cluster,
                String clusterName,
                String deployment,
                String processRunId,
                BookKeeperWalConfiguration configuration,
                BookKeeperLedgerIdNamespaceReservation reservation,
                Clock clock) throws Exception {
            OxiaClientConfiguration oxia = OxiaClientConfiguration.defaults(OXIA.getServiceAddress());
            SharedOxiaClientRuntime oxiaRuntime = SharedOxiaClientRuntime.connect(oxia, clock);
            BookKeeperMetadataStoreConfig metadataConfiguration = new BookKeeperMetadataStoreConfig(
                    configuration.maxAppendRangesPerLedger(),
                    configuration.protectionSlotsPerRange(),
                    configuration.maxReaderLeasesPerLedger(),
                    configuration.maxUncertainAllocations());
            OxiaJavaClientMetadataStore l0 = OxiaJavaClientMetadataStore.usingSharedRuntime(
                    oxia, oxiaRuntime, clock, metadataConfiguration);
            OxiaJavaBookKeeperMetadataStore bookKeeperMetadata =
                    OxiaJavaBookKeeperMetadataStore.usingSharedRuntime(
                            oxia, oxiaRuntime, clock, metadataConfiguration);
            BookKeeper client = cluster.newClient();
            DefaultBookKeeperClientOperations operations = new DefaultBookKeeperClientOperations(client);
            BookKeeperLedgerIdNamespaceReservationVerifier namespaceVerifier =
                    new BookKeeperLedgerIdNamespaceReservationVerifier(
                            (scope, bits, prefix, timeout) -> CompletableFuture.completedFuture(
                                    Optional.of(reservation)),
                            deployment);
            byte[] password = "bk-m2-secret".getBytes(StandardCharsets.UTF_8);
            BookKeeperWriterStateMachine writerState = new BookKeeperWriterStateMachine(
                    clusterName, configuration, bookKeeperMetadata, clock, processRunId);
            BookKeeperLedgerAllocator allocator = new BookKeeperLedgerAllocator(
                    clusterName,
                    configuration,
                    bookKeeperMetadata,
                    bookKeeperMetadata,
                    namespaceVerifier,
                    operations,
                    ignored -> password.clone(),
                    writerState,
                    clock);
            BookKeeperLedgerRecovery recovery = new BookKeeperLedgerRecovery(
                    clusterName,
                    configuration,
                    bookKeeperMetadata,
                    bookKeeperMetadata,
                    namespaceVerifier,
                    operations,
                    ignored -> password.clone(),
                    writerState,
                    clock);
            BookKeeperPrimaryWalAppender appender = new BookKeeperPrimaryWalAppender(
                    clusterName,
                    configuration,
                    bookKeeperMetadata,
                    bookKeeperMetadata,
                    allocator,
                    recovery,
                    writerState,
                    operations,
                    clock);
            BookKeeperLedgerHandleCache handles = new BookKeeperLedgerHandleCache(
                    8, 8 * 1024, 1024, Duration.ofMinutes(1));
            BookKeeperReaderLeaseManager leases = new BookKeeperReaderLeaseManager(
                    clusterName, configuration, bookKeeperMetadata, clock, processRunId);
            BookKeeperPrimaryWalReader reader = new BookKeeperPrimaryWalReader(
                    clusterName,
                    configuration,
                    bookKeeperMetadata,
                    operations,
                    ignored -> password.clone(),
                    handles,
                    leases);
            BookKeeperPrimaryPhysicalReferenceAdapter references =
                    new BookKeeperPrimaryPhysicalReferenceAdapter(
                            clusterName, configuration, bookKeeperMetadata, bookKeeperMetadata, clock);
            BookKeeperWalRuntime runtime = new BookKeeperWalRuntime(appender, reader, references);
            StreamStorageConfig storageConfiguration = StreamStorageConfig.defaults(clusterName, "writer-1");
            DefaultStreamStorage storage = runtime.newGenerationZeroStorage(
                    storageConfiguration,
                    l0,
                    new MetadataAppendRecoverySearcher(clusterName, l0),
                    AppendAdmissionGuard.noOp(),
                    clock,
                    Runnable::run,
                    ReadMetricsObserver.noop(),
                    TrimMetricsObserver.noop());
            return new Process(
                    client,
                    oxiaRuntime,
                    l0,
                    bookKeeperMetadata,
                    namespaceVerifier,
                    operations,
                    runtime,
                    storage);
        }

        private AppendResult append(StreamId streamId, byte[] payload) {
            AppendBatch batch = new AppendBatch(
                    PayloadFormat.OPAQUE_RECORD_BATCH,
                    List.of(new AppendEntry(payload, 1, 1, Map.of())),
                    1,
                    1,
                    1,
                    1,
                    List.of(),
                    Map.of(),
                    Optional.empty());
            return storage.append(
                            streamId,
                            batch,
                            new AppendOptions(
                                    Optional.empty(),
                                    DurabilityLevel.WAL_DURABLE_AND_INDEX_COMMITTED,
                                    TIMEOUT,
                                    true,
                                    Map.of()))
                    .join();
        }

        @Override
        public void close() throws Exception {
            storage.close();
            runtime.close();
            l0.close();
            oxiaRuntime.close();
            client.close();
        }
    }

    private static final class LostDeleteResponseOperations implements BookKeeperClientOperations {
        private final BookKeeperClientOperations delegate;
        private final AtomicBoolean inject = new AtomicBoolean(true);
        private final AtomicBoolean injected = new AtomicBoolean();

        private LostDeleteResponseOperations(BookKeeperClientOperations delegate) {
            this.delegate = delegate;
        }

        @Override
        public CompletableFuture<WriteAdvHandle> createAdvanced(
                long ledgerId,
                BookKeeperWalConfiguration configuration,
                byte[] password,
                Map<String, byte[]> customMetadata,
                BookKeeperOperationDeadline deadline) {
            return delegate.createAdvanced(ledgerId, configuration, password, customMetadata, deadline);
        }

        @Override
        public CompletableFuture<ReadHandle> open(
                long ledgerId,
                BookKeeperDigestType digestType,
                byte[] password,
                boolean recovery,
                BookKeeperOperationDeadline deadline) {
            return delegate.open(ledgerId, digestType, password, recovery, deadline);
        }

        @Override
        public CompletableFuture<Long> write(
                WriteAdvHandle handle,
                long entryId,
                ByteBuf entry,
                BookKeeperOperationDeadline deadline) {
            return delegate.write(handle, entryId, entry, deadline);
        }

        @Override
        public CompletableFuture<LedgerEntries> readUnconfirmed(
                ReadHandle handle,
                long firstEntryId,
                long lastEntryIdInclusive,
                BookKeeperOperationDeadline deadline) {
            return delegate.readUnconfirmed(handle, firstEntryId, lastEntryIdInclusive, deadline);
        }

        @Override
        public CompletableFuture<LedgerMetadata> metadata(
                long ledgerId, BookKeeperOperationDeadline deadline) {
            return delegate.metadata(ledgerId, deadline);
        }

        @Override
        public CompletableFuture<Void> delete(long ledgerId, BookKeeperOperationDeadline deadline) {
            return delegate.delete(ledgerId, deadline).thenCompose(ignored -> {
                if (inject.compareAndSet(true, false)) {
                    injected.set(true);
                    return CompletableFuture.failedFuture(new IllegalStateException("injected lost delete response"));
                }
                return CompletableFuture.completedFuture(null);
            });
        }

        private boolean lostResponseInjected() {
            return injected.get();
        }
    }

    private static final class MutableClock extends Clock {
        private long millis;

        private MutableClock(long millis) {
            this.millis = millis;
        }

        private void advance(Duration duration) {
            millis = Math.addExact(millis, duration.toMillis());
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
            return Instant.ofEpochMilli(millis);
        }

        @Override
        public long millis() {
            return millis;
        }
    }
}
