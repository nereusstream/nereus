/* Licensed under the Apache License, Version 2.0 */
package com.nereusstream.bookkeeper;

import static org.assertj.core.api.Assertions.assertThat;

import com.nereusstream.api.AppendSession;
import com.nereusstream.api.Checksum;
import com.nereusstream.api.ChecksumType;
import com.nereusstream.api.StreamId;
import com.nereusstream.metadata.oxia.BookKeeperMetadataStoreConfig;
import com.nereusstream.metadata.oxia.OxiaClientConfiguration;
import com.nereusstream.metadata.oxia.OxiaJavaBookKeeperMetadataStore;
import com.nereusstream.metadata.oxia.SharedOxiaClientRuntime;
import com.nereusstream.metadata.oxia.records.BookKeeperLedgerLifecycle;
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
import java.util.Random;
import java.util.UUID;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.atomic.AtomicInteger;
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

/** Real-service global ledger-id contention proof using the package-private deterministic candidate seam. */
@Testcontainers
class BookKeeperAllocatorOxiaBkContentionIntegrationTest {
    private static final Duration TIMEOUT = Duration.ofSeconds(30);

    @Container
    private static final OxiaContainer OXIA = new OxiaContainer(
            DockerImageName.parse("oxia/oxia:0.16.3")).withShards(4);

    @Test
    void twoStreamsChoosingOneCandidateConvergeToTwoOwnedLedgersWithoutDelete() throws Exception {
        String suffix = UUID.randomUUID().toString().replace("-", "");
        String cluster = "bk-m2-global-contention-" + suffix;
        String deployment = "deployment-" + suffix;
        String metadataServiceUri = "oxia://" + OXIA.getServiceAddress();
        Clock clock = Clock.fixed(Instant.ofEpochMilli(7_000_000), ZoneId.of("UTC"));
        BookKeeperWalConfiguration configuration = configuration();
        BookKeeperLedgerIdNamespaceReservation reservation = reservation(configuration, deployment);
        OxiaClientConfiguration oxia = OxiaClientConfiguration.defaults(OXIA.getServiceAddress());
        long seed = 19_871L;
        long contendedCandidate = configuration.ledgerIdNamespace().candidate(new Random(seed));

        try (BKCluster bookKeeperCluster = startBookKeeper(metadataServiceUri);
                SharedOxiaClientRuntime runtime = SharedOxiaClientRuntime.connect(oxia, clock);
                OxiaJavaBookKeeperMetadataStore metadata = OxiaJavaBookKeeperMetadataStore.usingSharedRuntime(
                        oxia,
                        runtime,
                        clock,
                        new BookKeeperMetadataStoreConfig(
                                configuration.maxAppendRangesPerLedger(),
                                configuration.protectionSlotsPerRange(),
                                configuration.maxReaderLeasesPerLedger(),
                                configuration.maxUncertainAllocations()));
                BookKeeper client = bookKeeperCluster.newClient()) {
            CountingOperations operations = new CountingOperations(
                    new DefaultBookKeeperClientOperations(client));
            BookKeeperLedgerIdNamespaceReservationVerifier namespaceVerifier =
                    new BookKeeperLedgerIdNamespaceReservationVerifier(
                            (scope, bits, prefix, timeout) -> CompletableFuture.completedFuture(
                                    Optional.of(reservation)),
                            deployment);
            byte[] password = "bk-m2-secret".getBytes(StandardCharsets.UTF_8);
            StreamId streamA = new StreamId("stream-contention-a-" + suffix);
            StreamId streamB = new StreamId("stream-contention-b-" + suffix);
            AppendSession sessionA = new AppendSession(streamA, "writer-a", 1, "token-a", 1, 60_000);
            AppendSession sessionB = new AppendSession(streamB, "writer-b", 1, "token-b", 1, 60_000);
            AtomicInteger allocationA = new AtomicInteger();
            AtomicInteger allocationB = new AtomicInteger();

            BookKeeperLedgerAllocator allocatorA = allocator(
                    cluster,
                    configuration,
                    metadata,
                    namespaceVerifier,
                    operations,
                    password,
                    new BookKeeperWriterStateMachine(
                            cluster, configuration, metadata, clock, "process-contention-a"),
                    clock,
                    new Random(seed),
                    () -> "allocation-a-" + allocationA.getAndIncrement());
            BookKeeperLedgerAllocator allocatorB = allocator(
                    cluster,
                    configuration,
                    metadata,
                    namespaceVerifier,
                    operations,
                    password,
                    new BookKeeperWriterStateMachine(
                            cluster, configuration, metadata, clock, "process-contention-b"),
                    clock,
                    new Random(seed),
                    () -> "allocation-b-" + allocationB.getAndIncrement());

            CompletableFuture<AllocatedBookKeeperLedger> allocationFutureA = CompletableFuture.supplyAsync(() ->
                    allocatorA.allocate(new BookKeeperLedgerAllocationRequest(streamA, sessionA, TIMEOUT)).join());
            CompletableFuture<AllocatedBookKeeperLedger> allocationFutureB = CompletableFuture.supplyAsync(() ->
                    allocatorB.allocate(new BookKeeperLedgerAllocationRequest(streamB, sessionB, TIMEOUT)).join());
            CompletableFuture.allOf(allocationFutureA, allocationFutureB).join();

            AllocatedBookKeeperLedger allocatedA = allocationFutureA.join();
            AllocatedBookKeeperLedger allocatedB = allocationFutureB.join();
            try {
                assertThat(List.of(
                                allocatedA.root().value().ledgerId(),
                                allocatedB.root().value().ledgerId()))
                        .contains(contendedCandidate)
                        .doesNotHaveDuplicates();
                assertThat(allocatedA.root().value().lifecycle()).isEqualTo(BookKeeperLedgerLifecycle.ACTIVE);
                assertThat(allocatedB.root().value().lifecycle()).isEqualTo(BookKeeperLedgerLifecycle.ACTIVE);
                assertThat(metadata.getRoot(
                                cluster,
                                configuration.providerScopeSha256(),
                                allocatedA.root().value().ledgerId())
                        .join()).contains(allocatedA.root());
                assertThat(metadata.getRoot(
                                cluster,
                                configuration.providerScopeSha256(),
                                allocatedB.root().value().ledgerId())
                        .join()).contains(allocatedB.root());
                assertThat(operations.deletes()).isZero();
            } finally {
                allocatedA.handle().closeAsync().join();
                allocatedB.handle().closeAsync().join();
                java.util.Arrays.fill(password, (byte) 0);
            }
        }
    }

    private static BookKeeperLedgerAllocator allocator(
            String cluster,
            BookKeeperWalConfiguration configuration,
            OxiaJavaBookKeeperMetadataStore metadata,
            BookKeeperLedgerIdNamespaceReservationVerifier namespaceVerifier,
            BookKeeperClientOperations operations,
            byte[] password,
            BookKeeperWriterStateMachine writerState,
            Clock clock,
            Random random,
            java.util.function.Supplier<String> allocationIds) {
        return new BookKeeperLedgerAllocator(
                cluster,
                configuration,
                metadata,
                metadata,
                namespaceVerifier,
                operations,
                ignored -> password.clone(),
                writerState,
                clock,
                random,
                allocationIds);
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
                8,
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
                new Checksum(ChecksumType.SHA256, "b".repeat(64)),
                "/bookkeeper/ledger-id-namespace/reservation-1");
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
                .numBookies(2)
                .clearOldData(true)
                .build();
    }

    private static final class CountingOperations implements BookKeeperClientOperations {
        private final BookKeeperClientOperations delegate;
        private final AtomicInteger deletes = new AtomicInteger();

        private CountingOperations(BookKeeperClientOperations delegate) {
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
                long ledgerId,
                BookKeeperOperationDeadline deadline) {
            return delegate.metadata(ledgerId, deadline);
        }

        @Override
        public CompletableFuture<Void> delete(long ledgerId, BookKeeperOperationDeadline deadline) {
            deletes.incrementAndGet();
            return delegate.delete(ledgerId, deadline);
        }

        private int deletes() {
            return deletes.get();
        }
    }
}
