/* Licensed under the Apache License, Version 2.0 */
package com.nereusstream.pulsar;

import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.assertThatThrownBy;

import com.nereusstream.api.AppendBatch;
import com.nereusstream.api.AppendEntry;
import com.nereusstream.api.AppendAttemptId;
import com.nereusstream.api.AppendOptions;
import com.nereusstream.api.AppendResult;
import com.nereusstream.api.AppendSession;
import com.nereusstream.api.AppendSessionOptions;
import com.nereusstream.api.Checksum;
import com.nereusstream.api.ChecksumType;
import com.nereusstream.api.DurabilityLevel;
import com.nereusstream.api.ErrorCode;
import com.nereusstream.api.NereusException;
import com.nereusstream.api.PayloadFormat;
import com.nereusstream.api.ReadIsolation;
import com.nereusstream.api.ReadOptions;
import com.nereusstream.api.ResolveOptions;
import com.nereusstream.api.ResolvedRange;
import com.nereusstream.api.StorageProfile;
import com.nereusstream.api.StreamCreateOptions;
import com.nereusstream.api.StreamId;
import com.nereusstream.api.StreamName;
import com.nereusstream.api.TrimOptions;
import com.nereusstream.api.target.BookKeeperEntryRangeReadTarget;
import com.nereusstream.bookkeeper.BookKeeperClientOperations;
import com.nereusstream.bookkeeper.BookKeeperAppendRecoveryCoordinator;
import com.nereusstream.bookkeeper.BookKeeperDigestType;
import com.nereusstream.bookkeeper.BookKeeperLedgerAllocationRequest;
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
import com.nereusstream.bookkeeper.BookKeeperPreparedPrimaryAppend;
import com.nereusstream.bookkeeper.BookKeeperPrimaryWalAppender;
import com.nereusstream.bookkeeper.BookKeeperPrimaryWalReader;
import com.nereusstream.bookkeeper.BookKeeperProtocolActivationProof;
import com.nereusstream.bookkeeper.BookKeeperReaderLeaseManager;
import com.nereusstream.bookkeeper.BookKeeperRetentionBlocker;
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
import com.nereusstream.core.wal.DurablePrimaryAppend;
import com.nereusstream.core.wal.PrimaryAppendRequest;
import com.nereusstream.metadata.oxia.BookKeeperKeyspace;
import com.nereusstream.metadata.oxia.BookKeeperMetadataStoreConfig;
import com.nereusstream.metadata.oxia.BookKeeperScanToken;
import com.nereusstream.metadata.oxia.BookKeeperVersionedValue;
import com.nereusstream.metadata.oxia.OxiaClientConfiguration;
import com.nereusstream.metadata.oxia.OxiaJavaBookKeeperMetadataStore;
import com.nereusstream.metadata.oxia.OxiaJavaClientMetadataStore;
import com.nereusstream.metadata.oxia.SharedOxiaClientRuntime;
import com.nereusstream.metadata.oxia.records.AllocationSlotLifecycle;
import com.nereusstream.metadata.oxia.records.AppendReservationLifecycle;
import com.nereusstream.metadata.oxia.records.AppendSessionRecord;
import com.nereusstream.metadata.oxia.records.BookKeeperAllocationSlotRecord;
import com.nereusstream.metadata.oxia.records.BookKeeperLedgerLifecycle;
import com.nereusstream.metadata.oxia.records.BookKeeperLedgerReaderLeaseRecord;
import com.nereusstream.metadata.oxia.records.BookKeeperLedgerRootRecord;
import com.nereusstream.metadata.oxia.records.BookKeeperWriterLifecycle;
import com.nereusstream.metadata.oxia.records.LedgerAllocationLifecycle;
import com.nereusstream.metadata.oxia.testing.BookKeeperMetadataStoreContractScenario;
import io.netty.buffer.ByteBuf;
import io.netty.buffer.Unpooled;
import io.oxia.testcontainers.OxiaContainer;
import java.lang.reflect.InvocationTargetException;
import java.lang.reflect.Proxy;
import java.nio.charset.StandardCharsets;
import java.time.Clock;
import java.time.Duration;
import java.time.Instant;
import java.time.ZoneId;
import java.util.ArrayList;
import java.util.Iterator;
import java.util.LinkedHashMap;
import java.util.List;
import java.util.Map;
import java.util.Optional;
import java.util.UUID;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.atomic.AtomicBoolean;
import java.util.concurrent.atomic.AtomicInteger;
import java.util.concurrent.atomic.AtomicReference;
import java.util.function.Function;
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

    @Test
    void multiEntryAppendUsesOneExactConsecutiveBookKeeperRange() throws Exception {
        String suffix = UUID.randomUUID().toString().replace("-", "");
        String cluster = "bk-m2-multi-" + suffix;
        String deployment = "deployment-" + suffix;
        String metadataServiceUri = "oxia://" + OXIA.getServiceAddress();
        BookKeeperWalConfiguration configuration = configuration(8);
        BookKeeperLedgerIdNamespaceReservation reservation = reservation(configuration, deployment);
        Clock clock = Clock.fixed(Instant.ofEpochMilli(1_500_000), ZoneId.of("UTC"));

        try (BKCluster bookKeeperCluster = startBookKeeper(metadataServiceUri);
                Process process = Process.open(
                        bookKeeperCluster,
                        cluster,
                        deployment,
                        "process-multi",
                        configuration,
                        reservation,
                        clock)) {
            StreamId stream = process.storage.createOrGetStream(
                            new StreamName("persistent://tenant/namespace/bk-multi-" + suffix),
                            new StreamCreateOptions(StorageProfile.BOOKKEEPER_WAL_ONLY, Map.of()))
                    .join()
                    .streamId();

            AppendResult result = process.append(
                    stream,
                    new byte[] {1, 2},
                    new byte[] {3},
                    new byte[] {4, 5, 6});
            BookKeeperEntryRangeReadTarget target = target(result);

            assertThat(result.range().startOffset()).isZero();
            assertThat(result.range().endOffset()).isEqualTo(3);
            assertThat(target.firstEntryId()).isZero();
            assertThat(target.entryCount()).isEqualTo(3);
            assertRead(process, stream, List.of(
                    new byte[] {1, 2}, new byte[] {3}, new byte[] {4, 5, 6}));
        }
    }

    @Test
    void realReaderNeverRecoveryOpensVerifiesWholeRangeBeforeClippingAndFailsClosedOnChecksumDrift()
            throws Exception {
        String suffix = UUID.randomUUID().toString().replace("-", "");
        String cluster = "bk-m2-reader-" + suffix;
        String deployment = "deployment-" + suffix;
        String metadataServiceUri = "oxia://" + OXIA.getServiceAddress();
        BookKeeperWalConfiguration configuration = configuration(8);
        BookKeeperLedgerIdNamespaceReservation reservation = reservation(configuration, deployment);
        Clock clock = Clock.fixed(Instant.ofEpochMilli(1_550_000), ZoneId.of("UTC"));
        AtomicReference<CountingOperations> counting = new AtomicReference<>();

        try (BKCluster bookKeeperCluster = startBookKeeper(metadataServiceUri);
                Process process = Process.open(
                        bookKeeperCluster,
                        cluster,
                        deployment,
                        "process-reader",
                        configuration,
                        reservation,
                        clock,
                        raw -> counting(raw, counting))) {
            StreamId stream = process.storage.createOrGetStream(
                            new StreamName("persistent://tenant/namespace/bk-reader-" + suffix),
                            new StreamCreateOptions(StorageProfile.BOOKKEEPER_WAL_ONLY, Map.of()))
                    .join()
                    .streamId();
            process.append(stream, new byte[] {1}, new byte[] {2}, new byte[] {3});

            assertRead(process, stream, List.of(new byte[] {2}, new byte[] {3}), 1);
            assertThat(counting.get().normalOpenCalls()).isOne();
            assertThat(counting.get().recoveryOpenCalls()).isZero();
            assertThat(counting.get().lastReadRange()).containsExactly(0, 2);

            ResolvedRange resolved = process.storage.resolve(stream, 0, new ResolveOptions(10, false, true))
                    .join()
                    .ranges()
                    .get(0);
            BookKeeperEntryRangeReadTarget exact = (BookKeeperEntryRangeReadTarget) resolved.readTarget();
            BookKeeperEntryRangeReadTarget corrupt = new BookKeeperEntryRangeReadTarget(
                    exact.version(),
                    exact.clusterAlias(),
                    exact.ledgerId(),
                    exact.firstEntryId(),
                    exact.entryCount(),
                    exact.entryMapping(),
                    sha('f'));
            ResolvedRange corruptRange = new ResolvedRange(
                    resolved.offsetRange(),
                    resolved.generation(),
                    corrupt,
                    resolved.payloadFormat(),
                    resolved.recordCount(),
                    resolved.entryCount(),
                    resolved.logicalBytes(),
                    resolved.schemaRefs(),
                    resolved.projectionRef(),
                    resolved.commitVersion());
            assertThatThrownBy(() -> process.reader.readPhysicalWithStats(
                            stream,
                            0,
                            List.of(corruptRange),
                            new ReadOptions(100, 1024, ReadIsolation.COMMITTED, TIMEOUT))
                    .join()).hasCauseInstanceOf(NereusException.class)
                    .satisfies(error -> assertThat(((NereusException) error.getCause()).code())
                            .isEqualTo(ErrorCode.PRIMARY_WAL_CHECKSUM_MISMATCH));
            assertThat(counting.get().recoveryOpenCalls()).isZero();
            assertThat(counting.get().lastReadRange()).containsExactly(0, 2);
        }
    }

    @Test
    void realReaderFailsClosedOnCountIdAndConfigurationDrift() throws Exception {
        String suffix = UUID.randomUUID().toString().replace("-", "");
        String cluster = "bk-m2-reader-drift-" + suffix;
        String deployment = "deployment-" + suffix;
        String metadataServiceUri = "oxia://" + OXIA.getServiceAddress();
        BookKeeperWalConfiguration configuration = configuration(8);
        BookKeeperLedgerIdNamespaceReservation reservation = reservation(configuration, deployment);
        Clock clock = Clock.fixed(Instant.ofEpochMilli(1_560_000), ZoneId.of("UTC"));

        try (BKCluster bookKeeperCluster = startBookKeeper(metadataServiceUri)) {
            StreamId stream;
            try (Process writer = Process.open(
                    bookKeeperCluster,
                    cluster,
                    deployment,
                    "process-reader-drift-writer",
                    configuration,
                    reservation,
                    clock)) {
                stream = writer.storage.createOrGetStream(
                                new StreamName("persistent://tenant/namespace/bk-reader-drift-" + suffix),
                                new StreamCreateOptions(StorageProfile.BOOKKEEPER_WAL_ONLY, Map.of()))
                        .join()
                        .streamId();
                writer.append(stream, new byte[] {1}, new byte[] {2}, new byte[] {3});
            }

            for (ReadCorruption corruption : List.of(
                    ReadCorruption.SHORT_COUNT,
                    ReadCorruption.WRONG_ENTRY_ID,
                    ReadCorruption.WRONG_CONFIGURATION)) {
                AtomicReference<CountingOperations> counting = new AtomicReference<>();
                try (Process reader = Process.open(
                        bookKeeperCluster,
                        cluster,
                        deployment,
                        "process-reader-drift-" + corruption.name().toLowerCase(java.util.Locale.ROOT),
                        configuration,
                        reservation,
                        clock,
                        raw -> counting(raw, counting, corruption))) {
                    ResolvedRange resolved = reader.storage.resolve(
                                    stream, 0, new ResolveOptions(10, false, true))
                            .join()
                            .ranges()
                            .get(0);

                    assertThatThrownBy(() -> reader.reader.readPhysicalWithStats(
                                    stream,
                                    0,
                                    List.of(resolved),
                                    new ReadOptions(100, 1024, ReadIsolation.COMMITTED, TIMEOUT))
                            .join()).hasCauseInstanceOf(NereusException.class)
                            .satisfies(error -> assertThat(((NereusException) error.getCause()).code())
                                    .isEqualTo(ErrorCode.METADATA_INVARIANT_VIOLATION));
                    assertThat(counting.get().normalOpenCalls()).isOne();
                    assertThat(counting.get().recoveryOpenCalls()).isZero();
                    assertThat(counting.get().readCalls())
                            .isEqualTo(corruption == ReadCorruption.WRONG_CONFIGURATION ? 0 : 1);
                }
            }
        }
    }

    @Test
    void partialRangeAndMixedLedgerTrimNeverDeleteLiveBookKeeperBytes() throws Exception {
        String suffix = UUID.randomUUID().toString().replace("-", "");
        String cluster = "bk-m2-retain-live-" + suffix;
        String deployment = "deployment-" + suffix;
        String metadataServiceUri = "oxia://" + OXIA.getServiceAddress();
        BookKeeperWalConfiguration configuration =
                configuration(100, 1024 * 1024, 3, Duration.ofHours(1));
        BookKeeperLedgerIdNamespaceReservation reservation = reservation(configuration, deployment);
        Clock clock = Clock.fixed(Instant.ofEpochMilli(1_575_000), ZoneId.of("UTC"));

        try (BKCluster bookKeeperCluster = startBookKeeper(metadataServiceUri);
                Process process = Process.open(
                        bookKeeperCluster,
                        cluster,
                        deployment,
                        "process-retain-live",
                        configuration,
                        reservation,
                        clock)) {
            BookKeeperWalOnlyRetirementAuthority authority = new BookKeeperWalOnlyRetirementAuthority(
                    cluster, process.l0, process.bookKeeperMetadata);
            BookKeeperWalReferenceManager references = new BookKeeperWalReferenceManager(
                    cluster, configuration, process.bookKeeperMetadata, authority);
            BookKeeperWalOnlyReferenceRetirementCoordinator retirement =
                    new BookKeeperWalOnlyReferenceRetirementCoordinator(
                            cluster, configuration, process.bookKeeperMetadata, authority, references);

            StreamId partialStream = process.storage.createOrGetStream(
                            new StreamName("persistent://tenant/namespace/bk-partial-trim-" + suffix),
                            new StreamCreateOptions(StorageProfile.BOOKKEEPER_WAL_ONLY, Map.of()))
                    .join()
                    .streamId();
            AppendResult partial = process.append(
                    partialStream, new byte[] {1}, new byte[] {2}, new byte[] {3});
            process.append(partialStream, new byte[] {4});
            process.append(partialStream, new byte[] {5});
            process.append(partialStream, new byte[] {6});
            long partialLedger = target(partial).ledgerId();
            var partialRoot = process.bookKeeperMetadata.getRoot(
                            cluster, configuration.providerScopeSha256(), partialLedger)
                    .join()
                    .orElseThrow();
            assertThat(partialRoot.value().lifecycle()).isEqualTo(BookKeeperLedgerLifecycle.SEALED);
            process.storage.trim(partialStream, 1, new TrimOptions(TIMEOUT, "partial range trim")).join();

            var partialRetirement = retirement.retireEligible(partialRoot, TIMEOUT).join();
            assertThat(partialRetirement.newlyRetiredProtections()).isZero();
            assertThat(partialRetirement.fullyRetired()).isFalse();
            assertThat(process.rawOperations.metadata(
                            partialLedger, new BookKeeperOperationDeadline(TIMEOUT))
                    .join().getLastEntryId()).isEqualTo(4);

            StreamId mixedStream = process.storage.createOrGetStream(
                            new StreamName("persistent://tenant/namespace/bk-mixed-trim-" + suffix),
                            new StreamCreateOptions(StorageProfile.BOOKKEEPER_WAL_ONLY, Map.of()))
                    .join()
                    .streamId();
            AppendResult mixedFirst = process.append(mixedStream, new byte[] {11});
            process.append(mixedStream, new byte[] {12});
            process.append(mixedStream, new byte[] {13});
            process.append(mixedStream, new byte[] {14});
            long mixedLedger = target(mixedFirst).ledgerId();
            var mixedRoot = process.bookKeeperMetadata.getRoot(
                            cluster, configuration.providerScopeSha256(), mixedLedger)
                    .join()
                    .orElseThrow();
            assertThat(mixedRoot.value().lifecycle()).isEqualTo(BookKeeperLedgerLifecycle.SEALED);
            process.storage.trim(mixedStream, 1, new TrimOptions(TIMEOUT, "mixed ledger trim")).join();

            var mixedRetirement = retirement.retireEligible(mixedRoot, TIMEOUT).join();
            assertThat(mixedRetirement.newlyRetiredProtections()).isEqualTo(3);
            assertThat(mixedRetirement.fullyRetired()).isFalse();
            assertThat(process.rawOperations.metadata(
                            mixedLedger, new BookKeeperOperationDeadline(TIMEOUT))
                    .join().getLastEntryId()).isEqualTo(2);
            assertRead(process, mixedStream, List.of(new byte[] {12}, new byte[] {13}, new byte[] {14}), 1);
        }
    }

    @Test
    void referenceAfterMarkUnmarksAndSafeGcModesNeverDelete() throws Exception {
        String suffix = UUID.randomUUID().toString().replace("-", "");
        String cluster = "bk-m2-gc-negative-" + suffix;
        String deployment = "deployment-" + suffix;
        String metadataServiceUri = "oxia://" + OXIA.getServiceAddress();
        BookKeeperWalConfiguration configuration = configuration(8);
        BookKeeperLedgerIdNamespaceReservation reservation = reservation(configuration, deployment);
        MutableClock clock = new MutableClock(1_600_000);

        try (BKCluster bookKeeperCluster = startBookKeeper(metadataServiceUri);
                Process process = Process.open(
                        bookKeeperCluster,
                        cluster,
                        deployment,
                        "process-gc-negative",
                        configuration,
                        reservation,
                        clock)) {
            StreamId stream = process.storage.createOrGetStream(
                            new StreamName("persistent://tenant/namespace/bk-gc-negative-" + suffix),
                            new StreamCreateOptions(StorageProfile.BOOKKEEPER_WAL_ONLY, Map.of()))
                    .join()
                    .streamId();
            AppendResult first = process.append(stream, new byte[] {1}, new byte[] {2});
            process.append(stream, new byte[] {3});
            process.append(stream, new byte[] {4});
            long ledgerId = target(first).ledgerId();
            var sealed = process.bookKeeperMetadata.getRoot(
                            cluster, configuration.providerScopeSha256(), ledgerId)
                    .join()
                    .orElseThrow();
            assertThat(sealed.value().lifecycle()).isEqualTo(BookKeeperLedgerLifecycle.SEALED);
            process.storage.trim(stream, 3, new TrimOptions(TIMEOUT, "gc negative boundaries")).join();
            BookKeeperWalOnlyRetirementAuthority authority = new BookKeeperWalOnlyRetirementAuthority(
                    cluster, process.l0, process.bookKeeperMetadata);
            BookKeeperWalReferenceManager references = new BookKeeperWalReferenceManager(
                    cluster, configuration, process.bookKeeperMetadata, authority);
            assertThat(new BookKeeperWalOnlyReferenceRetirementCoordinator(
                            cluster, configuration, process.bookKeeperMetadata, authority, references)
                    .retireEligible(sealed, TIMEOUT)
                    .join().fullyRetired()).isTrue();

            BookKeeperLedgerGcConfiguration enabled = new BookKeeperLedgerGcConfiguration(
                    1, Duration.ZERO, Duration.ofMinutes(2), Duration.ofSeconds(10), true, false);
            BookKeeperProtocolActivationProof activation = activation(configuration, reservation);
            BookKeeperWalRetentionGate gate = new BookKeeperWalRetentionGate(
                    cluster,
                    configuration,
                    enabled,
                    process.bookKeeperMetadata,
                    process.bookKeeperMetadata,
                    process.namespaceVerifier,
                    ignored -> CompletableFuture.completedFuture(activation),
                    process.rawOperations,
                    clock);
            BookKeeperLedgerRetentionManager retention = new BookKeeperLedgerRetentionManager(
                    cluster,
                    configuration,
                    enabled,
                    process.bookKeeperMetadata,
                    process.namespaceVerifier,
                    ignored -> CompletableFuture.completedFuture(activation),
                    process.rawOperations,
                    gate,
                    clock);
            var candidate = gate.evaluate(sealed, TIMEOUT).join().candidate().orElseThrow();
            var marked = retention.mark(candidate, TIMEOUT).join();
            assertThat(marked.action()).isEqualTo(BookKeeperLedgerGcAction.MARKED);

            var lateReader = process.bookKeeperMetadata.createReaderLease(
                            cluster,
                            configuration.providerScopeSha256(),
                            new BookKeeperLedgerReaderLeaseRecord(
                                    1,
                                    sealed.value().ledgerIdentitySha256(),
                                    ledgerId,
                                    sealed.value().lifecycleEpoch(),
                                    0,
                                    "reader-racing-real-mark/0",
                                    1,
                                    clock.millis(),
                                    clock.millis() + Duration.ofMinutes(10).toMillis(),
                                    0))
                    .join();
            clock.advance(Duration.ofMinutes(2).plusMillis(1));
            var unmarked = retention.converge(marked.root().orElseThrow(), TIMEOUT).join();
            assertThat(unmarked.action()).isEqualTo(BookKeeperLedgerGcAction.UNMARKED);
            assertThat(unmarked.root().orElseThrow().value().lifecycle())
                    .isEqualTo(BookKeeperLedgerLifecycle.SEALED);
            assertThat(process.rawOperations.metadata(ledgerId, new BookKeeperOperationDeadline(TIMEOUT))
                    .join().getLedgerId()).isEqualTo(ledgerId);
            process.bookKeeperMetadata.deleteReaderLease(
                            cluster,
                            configuration.providerScopeSha256(),
                            ledgerId,
                            lateReader.value().readerSlot(),
                            lateReader.metadataVersion())
                    .join();

            var currentRoot = unmarked.root().orElseThrow();
            var currentCandidate = gate.evaluate(currentRoot, TIMEOUT).join().candidate().orElseThrow();
            BookKeeperLedgerRetentionManager disabled = new BookKeeperLedgerRetentionManager(
                    cluster,
                    configuration,
                    BookKeeperLedgerGcConfiguration.safeDefault(),
                    process.bookKeeperMetadata,
                    process.namespaceVerifier,
                    ignored -> CompletableFuture.completedFuture(activation),
                    process.rawOperations,
                    gate,
                    clock);
            assertThat(disabled.mark(currentCandidate, TIMEOUT).join().action())
                    .isEqualTo(BookKeeperLedgerGcAction.DISABLED);
            assertThat(disabled.converge(currentRoot, TIMEOUT).join().action())
                    .isEqualTo(BookKeeperLedgerGcAction.DISABLED);

            BookKeeperLedgerGcConfiguration dryRunConfiguration = new BookKeeperLedgerGcConfiguration(
                    1, Duration.ZERO, Duration.ofMinutes(2), Duration.ofSeconds(10), true, true);
            BookKeeperLedgerRetentionManager dryRun = new BookKeeperLedgerRetentionManager(
                    cluster,
                    configuration,
                    dryRunConfiguration,
                    process.bookKeeperMetadata,
                    process.namespaceVerifier,
                    ignored -> CompletableFuture.completedFuture(activation),
                    process.rawOperations,
                    gate,
                    clock);
            assertThat(dryRun.mark(currentCandidate, TIMEOUT).join().action())
                    .isEqualTo(BookKeeperLedgerGcAction.DRY_RUN_ADMITTED);
            assertThat(dryRun.converge(currentRoot, TIMEOUT).join().action())
                    .isEqualTo(BookKeeperLedgerGcAction.DRY_RUN_ADMITTED);
            assertThat(process.bookKeeperMetadata.getRoot(
                            cluster, configuration.providerScopeSha256(), ledgerId)
                    .join()).contains(currentRoot);
            assertThat(process.rawOperations.metadata(ledgerId, new BookKeeperOperationDeadline(TIMEOUT))
                    .join().getLedgerId()).isEqualTo(ledgerId);
        }
    }

    @Test
    void byteRangeAndAgeRolloverPreserveWholeBatchesAndDenseOffsets() throws Exception {
        String suffix = UUID.randomUUID().toString().replace("-", "");
        String metadataServiceUri = "oxia://" + OXIA.getServiceAddress();

        try (BKCluster bookKeeperCluster = startBookKeeper(metadataServiceUri)) {
            assertRolloverBoundary(
                    bookKeeperCluster,
                    suffix,
                    "bytes",
                    configuration(100, 3, 100, Duration.ofHours(1)),
                    new MutableClock(1_600_000),
                    Duration.ZERO);
            assertRolloverBoundary(
                    bookKeeperCluster,
                    suffix,
                    "ranges",
                    configuration(100, 1024 * 1024, 1, Duration.ofHours(1)),
                    new MutableClock(1_700_000),
                    Duration.ZERO);
            assertRolloverBoundary(
                    bookKeeperCluster,
                    suffix,
                    "age",
                    configuration(100, 1024 * 1024, 100, Duration.ofSeconds(1)),
                    new MutableClock(1_800_000),
                    Duration.ofSeconds(1));
        }
    }

    private static void assertRolloverBoundary(
            BKCluster bookKeeperCluster,
            String suffix,
            String boundary,
            BookKeeperWalConfiguration configuration,
            MutableClock clock,
            Duration advanceBeforeSecondAppend) throws Exception {
        String cluster = "bk-m2-rollover-" + boundary + "-" + suffix;
        String deployment = "deployment-" + boundary + "-" + suffix;
        BookKeeperLedgerIdNamespaceReservation reservation = reservation(configuration, deployment);
        try (Process process = Process.open(
                bookKeeperCluster,
                cluster,
                deployment,
                "process-rollover-" + boundary,
                configuration,
                reservation,
                clock)) {
            StreamId stream = process.storage.createOrGetStream(
                            new StreamName("persistent://tenant/namespace/bk-rollover-" + boundary + "-" + suffix),
                            new StreamCreateOptions(StorageProfile.BOOKKEEPER_WAL_ONLY, Map.of()))
                    .join()
                    .streamId();

            AppendResult first = process.append(stream, new byte[] {1, 2});
            clock.advance(advanceBeforeSecondAppend);
            AppendResult second = process.append(stream, new byte[] {3, 4});

            assertThat(first.range().startOffset()).isZero();
            assertThat(first.range().endOffset()).isOne();
            assertThat(second.range().startOffset()).isOne();
            assertThat(second.range().endOffset()).isEqualTo(2);
            assertThat(target(second).ledgerId()).isNotEqualTo(target(first).ledgerId());
            assertThat(target(second).firstEntryId()).isZero();
            assertRead(process, stream, List.of(new byte[] {1, 2}, new byte[] {3, 4}));
        }
    }

    @Test
    void firstMiddleAndLastWriteFailureSealTheLedgerBeforeReuse() throws Exception {
        String suffix = UUID.randomUUID().toString().replace("-", "");
        String metadataServiceUri = "oxia://" + OXIA.getServiceAddress();
        Clock clock = Clock.fixed(Instant.ofEpochMilli(1_750_000), ZoneId.of("UTC"));

        try (BKCluster bookKeeperCluster = startBookKeeper(metadataServiceUri)) {
            for (int failedWrite = 1; failedWrite <= 3; failedWrite++) {
                int exactFailedWrite = failedWrite;
                String cluster = "bk-m2-partial-" + failedWrite + "-" + suffix;
                String deployment = "deployment-" + failedWrite + "-" + suffix;
                BookKeeperWalConfiguration configuration = configuration(8);
                BookKeeperLedgerIdNamespaceReservation reservation = reservation(configuration, deployment);
                AtomicReference<FailNthWriteOperations> injected = new AtomicReference<>();
                try (Process process = Process.open(
                        bookKeeperCluster,
                        cluster,
                        deployment,
                        "process-partial-" + failedWrite,
                        configuration,
                        reservation,
                        clock,
                        raw -> {
                            FailNthWriteOperations operations = new FailNthWriteOperations(raw, exactFailedWrite);
                            injected.set(operations);
                            return operations;
                        })) {
                    StreamId stream = process.storage.createOrGetStream(
                                    new StreamName("persistent://tenant/namespace/bk-partial-"
                                            + failedWrite + "-" + suffix),
                                    new StreamCreateOptions(StorageProfile.BOOKKEEPER_WAL_ONLY, Map.of()))
                            .join()
                            .streamId();

                    assertThatThrownBy(() -> process.append(
                                    stream, new byte[] {1}, new byte[] {2}, new byte[] {3}))
                            .hasCauseInstanceOf(NereusException.class);
                    assertThat(injected.get().failureInjected()).isTrue();

                    var failedReservation = process.bookKeeperMetadata.scanReservations(
                                    cluster, stream, Optional.empty(), 10)
                            .join()
                            .values()
                            .get(0);
                    assertThat(failedReservation.value().lifecycle())
                            .isEqualTo(AppendReservationLifecycle.ABANDONED);
                    long failedLedger = failedReservation.value().ledgerId();
                    assertThat(process.bookKeeperMetadata.getRoot(
                                    cluster, configuration.providerScopeSha256(), failedLedger)
                            .join()).get().satisfies(root ->
                                    assertThat(root.value().lifecycle())
                                            .isEqualTo(BookKeeperLedgerLifecycle.SEALED));
                    assertThat(process.rawOperations.metadata(
                                    failedLedger, new BookKeeperOperationDeadline(TIMEOUT))
                            .join().isClosed()).isTrue();

                    AppendResult retry = process.append(stream, new byte[] {9});
                    assertThat(target(retry).ledgerId()).isNotEqualTo(failedLedger);
                    assertThat(target(retry).firstEntryId()).isZero();
                }
            }
        }
    }

    @Test
    void restartRecoveryReusesCurrentSessionRangeAndFencesExpiredSessionRange() throws Exception {
        String suffix = UUID.randomUUID().toString().replace("-", "");
        String cluster = "bk-m2-recovery-" + suffix;
        String deployment = "deployment-" + suffix;
        String metadataServiceUri = "oxia://" + OXIA.getServiceAddress();
        MutableClock clock = new MutableClock(4_000_000);
        BookKeeperWalConfiguration configuration = configuration(8);
        BookKeeperLedgerIdNamespaceReservation reservation = reservation(configuration, deployment);

        try (BKCluster bookKeeperCluster = startBookKeeper(metadataServiceUri)) {
            StreamId currentStream;
            AppendSession currentSession;
            AppendAttemptId currentAttempt = new AppendAttemptId("attempt-current-" + suffix);
            DurablePrimaryAppend currentDurable;
            AtomicReference<CountingOperations> firstCounter = new AtomicReference<>();
            try (Process first = Process.open(
                    bookKeeperCluster,
                    cluster,
                    deployment,
                    "process-recovery-first",
                    configuration,
                    reservation,
                    clock,
                    raw -> counting(raw, firstCounter))) {
                currentStream = first.storage.createOrGetStream(
                                new StreamName("persistent://tenant/namespace/bk-recovery-current-" + suffix),
                                new StreamCreateOptions(StorageProfile.BOOKKEEPER_WAL_ONLY, Map.of()))
                        .join()
                        .streamId();
                currentSession = first.acquire(currentStream, "writer-1");
                currentDurable = first.persistOnly(
                        currentStream, currentSession, currentAttempt, new byte[] {1}, new byte[] {2});
                assertThat(firstCounter.get().writeCalls()).isEqualTo(2);
            }

            AtomicReference<CountingOperations> restartCounter = new AtomicReference<>();
            try (Process restarted = Process.open(
                    bookKeeperCluster,
                    cluster,
                    deployment,
                    "process-recovery-restarted",
                    configuration,
                    reservation,
                    clock,
                    raw -> counting(raw, restartCounter))) {
                AppendResult recovered = restarted.recoveryCoordinator.recoverAfterRestart(
                                currentSession,
                                currentAttempt,
                                DurabilityLevel.WAL_DURABLE_AND_INDEX_COMMITTED,
                                TIMEOUT)
                        .join();
                assertThat(recovered.readTarget()).isEqualTo(currentDurable.readTarget());
                assertThat(restartCounter.get().writeCalls()).isZero();
                assertRead(restarted, currentStream, List.of(new byte[] {1}, new byte[] {2}));
            }

            StreamId fencedStream;
            AppendSession oldSession;
            AppendAttemptId oldAttempt = new AppendAttemptId("attempt-fenced-" + suffix);
            DurablePrimaryAppend oldDurable;
            try (Process old = Process.open(
                    bookKeeperCluster,
                    cluster,
                    deployment,
                    "process-fenced-old",
                    configuration,
                    reservation,
                    clock)) {
                fencedStream = old.storage.createOrGetStream(
                                new StreamName("persistent://tenant/namespace/bk-recovery-fenced-" + suffix),
                                new StreamCreateOptions(StorageProfile.BOOKKEEPER_WAL_ONLY, Map.of()))
                        .join()
                        .streamId();
                oldSession = old.acquire(fencedStream, "writer-old");
                oldDurable = old.persistOnly(
                        fencedStream, oldSession, oldAttempt, new byte[] {3}, new byte[] {4});
            }

            clock.advance(Duration.ofMinutes(2));
            try (Process newOwner = Process.open(
                    bookKeeperCluster,
                    cluster,
                    deployment,
                    "process-fenced-new",
                    configuration,
                    reservation,
                    clock)) {
                AppendSession replacementSession = newOwner.acquire(fencedStream, "writer-new");
                assertThatThrownBy(() -> newOwner.recoveryCoordinator.recoverAfterRestart(
                                replacementSession,
                                oldAttempt,
                                DurabilityLevel.WAL_DURABLE_AND_INDEX_COMMITTED,
                                TIMEOUT)
                        .join()).hasCauseInstanceOf(NereusException.class);
                var oldTarget = (BookKeeperEntryRangeReadTarget) oldDurable.readTarget();
                assertThat(newOwner.bookKeeperMetadata.getReservation(
                                cluster,
                                fencedStream,
                                com.nereusstream.bookkeeper.BookKeeperAppendReservationIds.forAttempt(
                                        fencedStream, oldAttempt))
                        .join()).get().satisfies(value ->
                                assertThat(value.value().lifecycle())
                                        .isEqualTo(AppendReservationLifecycle.ABANDONED));

                DurablePrimaryAppend replacement = newOwner.persistOnly(
                        fencedStream,
                        replacementSession,
                        new AppendAttemptId("attempt-replacement-" + suffix),
                        new byte[] {9});
                BookKeeperEntryRangeReadTarget replacementTarget =
                        (BookKeeperEntryRangeReadTarget) replacement.readTarget();
                assertThat(replacementTarget.ledgerId()).isNotEqualTo(oldTarget.ledgerId());
                assertThat(replacementTarget.firstEntryId()).isZero();
            }
        }
    }

    @Test
    void newOwnerRecoveryOpenFencesLiveOldHandleAndPreventsOldHeadCommit() throws Exception {
        String suffix = UUID.randomUUID().toString().replace("-", "");
        String cluster = "bk-m2-live-fence-" + suffix;
        String deployment = "deployment-" + suffix;
        String metadataServiceUri = "oxia://" + OXIA.getServiceAddress();
        MutableClock clock = new MutableClock(5_000_000);
        BookKeeperWalConfiguration configuration = configuration(8);
        BookKeeperLedgerIdNamespaceReservation reservation = reservation(configuration, deployment);
        AtomicReference<CountingOperations> oldOperations = new AtomicReference<>();

        try (BKCluster bookKeeperCluster = startBookKeeper(metadataServiceUri);
                Process oldProcess = Process.open(
                        bookKeeperCluster,
                        cluster,
                        deployment,
                        "process-live-old",
                        configuration,
                        reservation,
                        clock,
                        raw -> counting(raw, oldOperations))) {
            StreamId stream = oldProcess.storage.createOrGetStream(
                            new StreamName("persistent://tenant/namespace/bk-live-fence-" + suffix),
                            new StreamCreateOptions(StorageProfile.BOOKKEEPER_WAL_ONLY, Map.of()))
                    .join()
                    .streamId();
            AppendSession oldSession = oldProcess.acquire(stream, "writer-old");
            AppendAttemptId oldAttempt = new AppendAttemptId("attempt-live-old-" + suffix);
            DurablePrimaryAppend oldDurable = oldProcess.persistOnly(
                    stream, oldSession, oldAttempt, new byte[] {1});
            WriteAdvHandle oldHandle = oldOperations.get().createdHandle();
            long oldLedger = ((BookKeeperEntryRangeReadTarget) oldDurable.readTarget()).ledgerId();

            clock.advance(Duration.ofMinutes(1).plusMillis(1));
            try (Process newProcess = Process.open(
                    bookKeeperCluster,
                    cluster,
                    deployment,
                    "process-live-new",
                    configuration,
                    reservation,
                    clock)) {
                AppendSession newSession = newProcess.acquire(stream, "writer-new");
                assertThatThrownBy(() -> newProcess.recoveryCoordinator.recoverAfterRestart(
                                newSession,
                                oldAttempt,
                                DurabilityLevel.WAL_DURABLE_AND_INDEX_COMMITTED,
                                TIMEOUT)
                        .join()).hasCauseInstanceOf(NereusException.class)
                        .satisfies(error -> assertThat(((NereusException) error.getCause()).code())
                                .isEqualTo(ErrorCode.FENCED_APPEND));

                assertThatThrownBy(() -> oldProcess.appender
                                .validateBeforeHeadCommit(oldDurable, oldSession, TIMEOUT)
                                .join())
                        .hasCauseInstanceOf(NereusException.class)
                        .satisfies(error -> assertThat(((NereusException) error.getCause()).code())
                                .isEqualTo(ErrorCode.FENCED_APPEND));

                ByteBuf staleEntry = Unpooled.wrappedBuffer(new byte[] {9});
                try {
                    assertThatThrownBy(() -> oldProcess.rawOperations.write(
                                    oldHandle,
                                    1,
                                    staleEntry,
                                    new BookKeeperOperationDeadline(TIMEOUT))
                            .join()).hasCauseInstanceOf(NereusException.class)
                            .satisfies(error -> assertThat(((NereusException) error.getCause()).code())
                                    .isEqualTo(ErrorCode.FENCED_APPEND));
                } finally {
                    staleEntry.release();
                }

                DurablePrimaryAppend replacement = newProcess.persistOnly(
                        stream,
                        newSession,
                        new AppendAttemptId("attempt-live-new-" + suffix),
                        new byte[] {2});
                BookKeeperEntryRangeReadTarget replacementTarget =
                        (BookKeeperEntryRangeReadTarget) replacement.readTarget();
                assertThat(replacementTarget.ledgerId()).isNotEqualTo(oldLedger);
                assertThat(replacementTarget.firstEntryId()).isZero();
            }
        }
    }

    private static CountingOperations counting(
            BookKeeperClientOperations raw,
            AtomicReference<CountingOperations> target) {
        CountingOperations operations = new CountingOperations(raw);
        target.set(operations);
        return operations;
    }

    private static CountingOperations counting(
            BookKeeperClientOperations raw,
            AtomicReference<CountingOperations> target,
            ReadCorruption corruption) {
        CountingOperations operations = new CountingOperations(raw, corruption);
        target.set(operations);
        return operations;
    }

    @Test
    void createResponseLossRecoverySealsTheExactLedgerAndKeepsTheHazardSlot() throws Exception {
        String suffix = UUID.randomUUID().toString().replace("-", "");
        String cluster = "bk-m2-create-loss-" + suffix;
        String deployment = "deployment-" + suffix;
        String metadataServiceUri = "oxia://" + OXIA.getServiceAddress();
        Clock clock = Clock.fixed(Instant.ofEpochMilli(2_000_000), ZoneId.of("UTC"));
        BookKeeperWalConfiguration configuration = configuration();
        BookKeeperLedgerIdNamespaceReservation reservation = reservation(configuration, deployment);
        OxiaClientConfiguration oxia = OxiaClientConfiguration.defaults(OXIA.getServiceAddress());

        try (BKCluster bookKeeperCluster = startBookKeeper(metadataServiceUri);
                SharedOxiaClientRuntime oxiaRuntime = SharedOxiaClientRuntime.connect(oxia, clock);
                OxiaJavaBookKeeperMetadataStore metadata = OxiaJavaBookKeeperMetadataStore.usingSharedRuntime(
                        oxia,
                        oxiaRuntime,
                        clock,
                        new BookKeeperMetadataStoreConfig(
                                configuration.maxAppendRangesPerLedger(),
                                configuration.protectionSlotsPerRange(),
                                configuration.maxReaderLeasesPerLedger(),
                                configuration.maxUncertainAllocations()));
                BookKeeper client = bookKeeperCluster.newClient()) {
            DefaultBookKeeperClientOperations raw = new DefaultBookKeeperClientOperations(client);
            LostCreateResponseOperations operations = new LostCreateResponseOperations(raw);
            BookKeeperLedgerIdNamespaceReservationVerifier namespaceVerifier =
                    new BookKeeperLedgerIdNamespaceReservationVerifier(
                            (scope, bits, prefix, timeout) -> CompletableFuture.completedFuture(
                                    Optional.of(reservation)),
                            deployment);
            byte[] password = "bk-m2-secret".getBytes(StandardCharsets.UTF_8);
            StreamId stream = new StreamId("stream-create-loss-" + suffix);
            AppendSession session = new AppendSession(stream, "writer-1", 1, "token-1", 1, 10_000);
            BookKeeperWriterStateMachine writerState = new BookKeeperWriterStateMachine(
                    cluster, configuration, metadata, clock, "process-create-loss");
            BookKeeperLedgerAllocator allocator = new BookKeeperLedgerAllocator(
                    cluster,
                    configuration,
                    metadata,
                    metadata,
                    namespaceVerifier,
                    operations,
                    ignored -> password.clone(),
                    writerState,
                    clock);

            assertThatThrownBy(() -> allocator.allocate(
                            new BookKeeperLedgerAllocationRequest(stream, session, TIMEOUT))
                    .join()).hasCauseInstanceOf(com.nereusstream.api.NereusException.class);

            var allocation = metadata.scanAllocations(
                            cluster, stream, Optional.<BookKeeperScanToken>empty(), 10)
                    .join()
                    .values()
                    .get(0);
            long uncertainLedgerId = allocation.value().candidateLedgerId();
            assertThat(allocation.value().lifecycle()).isEqualTo(LedgerAllocationLifecycle.PHYSICAL_CREATED);
            assertThat(allocation.value().lateCreateHazard()).isTrue();
            assertThat(metadata.getAllocationSlot(cluster, allocation.value().allocationSlot()).join())
                    .get()
                    .extracting(slot -> slot.value().lifecycle())
                    .isEqualTo(AllocationSlotLifecycle.CREATE_UNCERTAIN);
            assertThat(metadata.getRoot(cluster, configuration.providerScopeSha256(), uncertainLedgerId).join())
                    .get()
                    .satisfies(root -> {
                        assertThat(root.value().lifecycle()).isEqualTo(BookKeeperLedgerLifecycle.SEALED);
                        assertThat(root.value().lateCreateHazard()).isTrue();
                    });
            assertThat(metadata.getWriter(cluster, stream).join())
                    .get()
                    .extracting(writer -> writer.value().lifecycle())
                    .isEqualTo(BookKeeperWriterLifecycle.IDLE);
            assertThat(raw.metadata(uncertainLedgerId, new BookKeeperOperationDeadline(TIMEOUT)).join().isClosed())
                    .isTrue();

            var replacement = allocator.allocate(
                            new BookKeeperLedgerAllocationRequest(stream, session, TIMEOUT))
                    .join();
            assertThat(replacement.root().value().ledgerId()).isNotEqualTo(uncertainLedgerId);
            replacement.handle().closeAsync().join();

            DelayedCreateOperations delayedOperations = new DelayedCreateOperations(raw);
            StreamId lateStream = new StreamId("stream-late-create-" + suffix);
            AppendSession lateSession = new AppendSession(
                    lateStream, "writer-2", 1, "token-2", 1, 10_000);
            BookKeeperWriterStateMachine lateWriterState = new BookKeeperWriterStateMachine(
                    cluster, configuration, metadata, clock, "process-late-create");
            BookKeeperLedgerAllocator lateAllocator = new BookKeeperLedgerAllocator(
                    cluster,
                    configuration,
                    metadata,
                    metadata,
                    namespaceVerifier,
                    delayedOperations,
                    ignored -> password.clone(),
                    lateWriterState,
                    clock);
            assertThatThrownBy(() -> lateAllocator.allocate(
                            new BookKeeperLedgerAllocationRequest(lateStream, lateSession, TIMEOUT))
                    .join()).hasCauseInstanceOf(com.nereusstream.api.NereusException.class);

            WriteAdvHandle delayedHandle = delayedOperations.createNow().join();
            long lateLedgerId = delayedHandle.getId();
            var lateRecovery = lateAllocator.reconcileUncertainAllocations(TIMEOUT).join();
            assertThat(lateRecovery.uncertainSlots()).isEqualTo(2);
            assertThat(lateRecovery.recoveredLedgers()).isEqualTo(2);
            assertThat(metadata.getRoot(cluster, configuration.providerScopeSha256(), lateLedgerId).join())
                    .get()
                    .satisfies(root -> {
                        assertThat(root.value().lifecycle()).isEqualTo(BookKeeperLedgerLifecycle.SEALED);
                        assertThat(root.value().lateCreateHazard()).isTrue();
                    });
            assertThat(raw.metadata(lateLedgerId, new BookKeeperOperationDeadline(TIMEOUT)).join().isClosed())
                    .isTrue();
            var lateRoot = metadata.getRoot(
                            cluster, configuration.providerScopeSha256(), lateLedgerId)
                    .join()
                    .orElseThrow();
            BookKeeperLedgerGcConfiguration gc = new BookKeeperLedgerGcConfiguration(
                    1, Duration.ZERO, Duration.ofMinutes(2), Duration.ofSeconds(10), true, false);
            BookKeeperWalRetentionGate retentionGate = new BookKeeperWalRetentionGate(
                    cluster,
                    configuration,
                    gc,
                    metadata,
                    metadata,
                    namespaceVerifier,
                    ignored -> CompletableFuture.completedFuture(activation(configuration, reservation)),
                    raw,
                    clock);
            assertThat(retentionGate.evaluate(lateRoot, TIMEOUT).join().blockers())
                    .contains(BookKeeperRetentionBlocker.LATE_CREATE_HAZARD)
                    .contains(BookKeeperRetentionBlocker.ALLOCATION_SLOT_PRESENT);
            delayedHandle.closeAsync().join();
        }
    }

    @Test
    void realOxiaColdScanCoversEveryRootAndAllocationSlotShard() throws Exception {
        String cluster = "bk-m2-shards-" + UUID.randomUUID().toString().replace("-", "");
        Clock clock = Clock.fixed(Instant.ofEpochMilli(3_000_000), ZoneId.of("UTC"));
        OxiaClientConfiguration oxia = OxiaClientConfiguration.defaults(OXIA.getServiceAddress());
        BookKeeperMetadataStoreConfig metadataConfiguration =
                new BookKeeperMetadataStoreConfig(128, 4, 128, 256);
        BookKeeperKeyspace keys = metadataConfiguration.keyspace(cluster);
        Map<Integer, BookKeeperLedgerRootRecord> roots = oneRootPerShard(keys);

        try (SharedOxiaClientRuntime firstRuntime = SharedOxiaClientRuntime.connect(oxia, clock);
                OxiaJavaBookKeeperMetadataStore first = OxiaJavaBookKeeperMetadataStore.usingSharedRuntime(
                        oxia, firstRuntime, clock, metadataConfiguration)) {
            roots.values().forEach(root -> first.createRoot(cluster, root).join());
            for (int slot = 0; slot < BookKeeperKeyspace.ALLOCATION_SLOT_SHARDS; slot++) {
                long ledgerId = 100_000L + slot;
                first.createAllocationSlot(cluster, new BookKeeperAllocationSlotRecord(
                        1,
                        slot,
                        "allocation-slot-" + slot,
                        BookKeeperMetadataStoreContractScenario.STREAM.value(),
                        ledgerId,
                        keys.ledgerIdentitySha256(
                                BookKeeperMetadataStoreContractScenario.PROVIDER_SCOPE, ledgerId),
                        BookKeeperMetadataStoreContractScenario.CONFIGURATION,
                        AllocationSlotLifecycle.CREATE_UNCERTAIN,
                        100,
                        101,
                        0)).join();
            }
        }

        try (SharedOxiaClientRuntime restartedRuntime = SharedOxiaClientRuntime.connect(oxia, clock);
                OxiaJavaBookKeeperMetadataStore restarted = OxiaJavaBookKeeperMetadataStore.usingSharedRuntime(
                        oxia, restartedRuntime, clock, metadataConfiguration)) {
            for (int shard = 0; shard < BookKeeperKeyspace.LEDGER_SHARDS; shard++) {
                var page = restarted.scanRoots(cluster, shard, Optional.empty(), 1).join();
                assertThat(page.values())
                        .singleElement()
                        .extracting(value -> value.value().ledgerIdentitySha256())
                        .isEqualTo(roots.get(shard).ledgerIdentitySha256());
                assertThat(page.continuation()).isEmpty();
            }
            for (int shard = 0; shard < BookKeeperKeyspace.ALLOCATION_SLOT_SHARDS; shard++) {
                var page = restarted.scanAllocationSlots(cluster, shard, Optional.empty(), 1).join();
                assertThat(page.values())
                        .singleElement()
                        .extracting(value -> value.value().slot())
                        .isEqualTo(shard);
                assertThat(page.continuation()).isEmpty();
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

    private static Map<Integer, BookKeeperLedgerRootRecord> oneRootPerShard(BookKeeperKeyspace keys) {
        Map<Integer, BookKeeperLedgerRootRecord> roots = new LinkedHashMap<>();
        for (long ledgerId = 1;
                ledgerId < 100_000 && roots.size() < BookKeeperKeyspace.LEDGER_SHARDS;
                ledgerId++) {
            String identity = keys.ledgerIdentitySha256(
                    BookKeeperMetadataStoreContractScenario.PROVIDER_SCOPE, ledgerId);
            int shard = keys.ledgerShard(identity);
            roots.putIfAbsent(
                    shard,
                    BookKeeperMetadataStoreContractScenario.root(
                            keys, ledgerId, BookKeeperLedgerLifecycle.ACTIVE, 2));
        }
        assertThat(roots).hasSize(BookKeeperKeyspace.LEDGER_SHARDS);
        return roots;
    }

    private static BookKeeperWalConfiguration configuration() {
        return configuration(2);
    }

    private static BookKeeperWalConfiguration configuration(long maxEntriesPerLedger) {
        return configuration(maxEntriesPerLedger, 1024 * 1024, 2, Duration.ofHours(1));
    }

    private static BookKeeperWalConfiguration configuration(
            long maxEntriesPerLedger,
            long maxBytesPerLedger,
            int maxAppendRangesPerLedger,
            Duration maxLedgerAge) {
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
                maxEntriesPerLedger,
                maxBytesPerLedger,
                maxAppendRangesPerLedger,
                8,
                32,
                16,
                maxLedgerAge,
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
        private final String clusterName;
        private final BookKeeper client;
        private final SharedOxiaClientRuntime oxiaRuntime;
        private final OxiaJavaClientMetadataStore l0;
        private final OxiaJavaBookKeeperMetadataStore bookKeeperMetadata;
        private final BookKeeperLedgerIdNamespaceReservationVerifier namespaceVerifier;
        private final DefaultBookKeeperClientOperations rawOperations;
        private final BookKeeperPrimaryWalAppender appender;
        private final BookKeeperAppendRecoveryCoordinator recoveryCoordinator;
        private final BookKeeperPrimaryWalReader reader;
        private final BookKeeperWalRuntime runtime;
        private final DefaultStreamStorage storage;

        private Process(
                String clusterName,
                BookKeeper client,
                SharedOxiaClientRuntime oxiaRuntime,
                OxiaJavaClientMetadataStore l0,
                OxiaJavaBookKeeperMetadataStore bookKeeperMetadata,
                BookKeeperLedgerIdNamespaceReservationVerifier namespaceVerifier,
                DefaultBookKeeperClientOperations rawOperations,
                BookKeeperPrimaryWalAppender appender,
                BookKeeperAppendRecoveryCoordinator recoveryCoordinator,
                BookKeeperPrimaryWalReader reader,
                BookKeeperWalRuntime runtime,
                DefaultStreamStorage storage) {
            this.clusterName = clusterName;
            this.client = client;
            this.oxiaRuntime = oxiaRuntime;
            this.l0 = l0;
            this.bookKeeperMetadata = bookKeeperMetadata;
            this.namespaceVerifier = namespaceVerifier;
            this.rawOperations = rawOperations;
            this.appender = appender;
            this.recoveryCoordinator = recoveryCoordinator;
            this.reader = reader;
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
            return open(
                    cluster,
                    clusterName,
                    deployment,
                    processRunId,
                    configuration,
                    reservation,
                    clock,
                    Function.identity());
        }

        private static Process open(
                BKCluster cluster,
                String clusterName,
                String deployment,
                String processRunId,
                BookKeeperWalConfiguration configuration,
                BookKeeperLedgerIdNamespaceReservation reservation,
                Clock clock,
                Function<BookKeeperClientOperations, BookKeeperClientOperations> operationDecorator)
                throws Exception {
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
            DefaultBookKeeperClientOperations rawOperations = new DefaultBookKeeperClientOperations(client);
            BookKeeperClientOperations operations = java.util.Objects.requireNonNull(
                    operationDecorator.apply(rawOperations), "decorated BookKeeper operations");
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
            BookKeeperAppendRecoveryCoordinator recoveryCoordinator = new BookKeeperAppendRecoveryCoordinator(
                    clusterName,
                    configuration,
                    bookKeeperMetadata,
                    l0,
                    references,
                    recovery,
                    clock);
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
                    clusterName,
                    client,
                    oxiaRuntime,
                    l0,
                    bookKeeperMetadata,
                    namespaceVerifier,
                    rawOperations,
                    appender,
                    recoveryCoordinator,
                    reader,
                    runtime,
                    storage);
        }

        private AppendResult append(StreamId streamId, byte[]... payloads) {
            List<AppendEntry> entries = java.util.stream.IntStream.range(0, payloads.length)
                    .mapToObj(index -> new AppendEntry(payloads[index], 1, index + 1L, Map.of()))
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

        private AppendSession acquire(StreamId stream, String writerId) {
            AppendSessionRecord record = l0.acquireAppendSession(
                            storageConfigCluster(),
                            stream,
                            new AppendSessionOptions(writerId, Duration.ofMinutes(1), true))
                    .join();
            return new AppendSession(
                    stream,
                    record.writerId(),
                    record.epoch(),
                    record.fencingToken(),
                    record.leaseVersion(),
                    record.expiresAtMillis());
        }

        private DurablePrimaryAppend persistOnly(
                StreamId stream,
                AppendSession session,
                AppendAttemptId attempt,
                byte[]... payloads) {
            List<AppendEntry> entries = java.util.stream.IntStream.range(0, payloads.length)
                    .mapToObj(index -> new AppendEntry(payloads[index], 1, index + 1L, Map.of()))
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
            try (BookKeeperPreparedPrimaryAppend prepared = appender.prepare(new PrimaryAppendRequest(
                    stream, batch, session, 0, attempt, TIMEOUT))) {
                return appender.persist(prepared, TIMEOUT).join();
            }
        }

        private String storageConfigCluster() {
            return clusterName;
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

    private static final class CountingOperations implements BookKeeperClientOperations {
        private final BookKeeperClientOperations delegate;
        private final ReadCorruption readCorruption;
        private final AtomicInteger writes = new AtomicInteger();
        private final AtomicInteger normalOpens = new AtomicInteger();
        private final AtomicInteger recoveryOpens = new AtomicInteger();
        private final AtomicInteger reads = new AtomicInteger();
        private final AtomicReference<WriteAdvHandle> createdHandle = new AtomicReference<>();
        private final AtomicReference<long[]> lastReadRange = new AtomicReference<>();

        private CountingOperations(BookKeeperClientOperations delegate) {
            this(delegate, ReadCorruption.NONE);
        }

        private CountingOperations(BookKeeperClientOperations delegate, ReadCorruption readCorruption) {
            this.delegate = delegate;
            this.readCorruption = readCorruption;
        }

        @Override
        public CompletableFuture<WriteAdvHandle> createAdvanced(
                long ledgerId,
                BookKeeperWalConfiguration configuration,
                byte[] password,
                Map<String, byte[]> customMetadata,
                BookKeeperOperationDeadline deadline) {
            return delegate.createAdvanced(ledgerId, configuration, password, customMetadata, deadline)
                    .thenApply(handle -> {
                        createdHandle.set(handle);
                        return handle;
                    });
        }

        @Override
        public CompletableFuture<ReadHandle> open(
                long ledgerId,
                BookKeeperDigestType digestType,
                byte[] password,
                boolean recovery,
                BookKeeperOperationDeadline deadline) {
            if (recovery) recoveryOpens.incrementAndGet();
            else normalOpens.incrementAndGet();
            return delegate.open(ledgerId, digestType, password, recovery, deadline)
                    .thenApply(handle -> !recovery && readCorruption == ReadCorruption.WRONG_CONFIGURATION
                            ? withWrongConfiguration(handle)
                            : handle);
        }

        @Override
        public CompletableFuture<Long> write(
                WriteAdvHandle handle,
                long entryId,
                ByteBuf entry,
                BookKeeperOperationDeadline deadline) {
            writes.incrementAndGet();
            return delegate.write(handle, entryId, entry, deadline);
        }

        @Override
        public CompletableFuture<LedgerEntries> readUnconfirmed(
                ReadHandle handle,
                long firstEntryId,
                long lastEntryIdInclusive,
                BookKeeperOperationDeadline deadline) {
            lastReadRange.set(new long[] {firstEntryId, lastEntryIdInclusive});
            reads.incrementAndGet();
            return delegate.readUnconfirmed(handle, firstEntryId, lastEntryIdInclusive, deadline)
                    .thenApply(entries -> switch (readCorruption) {
                        case SHORT_COUNT -> new CorruptLedgerEntries(entries, true, false);
                        case WRONG_ENTRY_ID -> new CorruptLedgerEntries(entries, false, true);
                        default -> entries;
                    });
        }

        @Override
        public CompletableFuture<LedgerMetadata> metadata(
                long ledgerId, BookKeeperOperationDeadline deadline) {
            return delegate.metadata(ledgerId, deadline);
        }

        @Override
        public CompletableFuture<Void> delete(long ledgerId, BookKeeperOperationDeadline deadline) {
            return delegate.delete(ledgerId, deadline);
        }

        private int writeCalls() {
            return writes.get();
        }

        private WriteAdvHandle createdHandle() {
            return java.util.Objects.requireNonNull(createdHandle.get(), "created BookKeeper write handle");
        }

        private int normalOpenCalls() {
            return normalOpens.get();
        }

        private int recoveryOpenCalls() {
            return recoveryOpens.get();
        }

        private int readCalls() {
            return reads.get();
        }

        private long[] lastReadRange() {
            return java.util.Objects.requireNonNull(lastReadRange.get(), "BookKeeper read range").clone();
        }

        private static ReadHandle withWrongConfiguration(ReadHandle handle) {
            LedgerMetadata exact = handle.getLedgerMetadata();
            LedgerMetadata corrupt = (LedgerMetadata) Proxy.newProxyInstance(
                    LedgerMetadata.class.getClassLoader(),
                    new Class<?>[] {LedgerMetadata.class},
                    (ignored, method, arguments) -> {
                        if (method.getName().equals("getAckQuorumSize")) {
                            return exact.getAckQuorumSize() + 1;
                        }
                        return invoke(method, exact, arguments);
                    });
            return (ReadHandle) Proxy.newProxyInstance(
                    ReadHandle.class.getClassLoader(),
                    new Class<?>[] {ReadHandle.class},
                    (ignored, method, arguments) -> {
                        if (method.getName().equals("getLedgerMetadata")) return corrupt;
                        return invoke(method, handle, arguments);
                    });
        }

        private static Object invoke(
                java.lang.reflect.Method method, Object target, Object[] arguments) throws Throwable {
            try {
                return method.invoke(target, arguments);
            } catch (InvocationTargetException failure) {
                throw failure.getCause();
            }
        }
    }

    private enum ReadCorruption {
        NONE,
        SHORT_COUNT,
        WRONG_ENTRY_ID,
        WRONG_CONFIGURATION
    }

    private static final class CorruptLedgerEntries implements LedgerEntries {
        private final LedgerEntries delegate;
        private final List<org.apache.bookkeeper.client.api.LedgerEntry> entries;

        private CorruptLedgerEntries(
                LedgerEntries delegate, boolean omitLast, boolean changeFirstEntryId) {
            this.delegate = delegate;
            List<org.apache.bookkeeper.client.api.LedgerEntry> copied = new ArrayList<>();
            delegate.forEach(copied::add);
            if (omitLast && !copied.isEmpty()) copied.remove(copied.size() - 1);
            if (changeFirstEntryId && !copied.isEmpty()) {
                var first = copied.get(0);
                copied.set(0, new CorruptLedgerEntry(first, first.getEntryId() + 1));
            }
            entries = List.copyOf(copied);
        }

        @Override
        public org.apache.bookkeeper.client.api.LedgerEntry getEntry(long entryId) {
            return entries.stream()
                    .filter(entry -> entry.getEntryId() == entryId)
                    .findFirst()
                    .orElse(null);
        }

        @Override
        public Iterator<org.apache.bookkeeper.client.api.LedgerEntry> iterator() {
            return entries.iterator();
        }

        @Override
        public void close() {
            delegate.close();
        }
    }

    private static final class CorruptLedgerEntry implements org.apache.bookkeeper.client.api.LedgerEntry {
        private final org.apache.bookkeeper.client.api.LedgerEntry delegate;
        private final long entryId;

        private CorruptLedgerEntry(org.apache.bookkeeper.client.api.LedgerEntry delegate, long entryId) {
            this.delegate = delegate;
            this.entryId = entryId;
        }

        @Override public long getLedgerId() { return delegate.getLedgerId(); }
        @Override public long getEntryId() { return entryId; }
        @Override public long getLength() { return delegate.getLength(); }
        @Override public byte[] getEntryBytes() { return delegate.getEntryBytes(); }
        @Override public java.nio.ByteBuffer getEntryNioBuffer() { return delegate.getEntryNioBuffer(); }
        @Override public ByteBuf getEntryBuffer() { return delegate.getEntryBuffer(); }
        @Override public org.apache.bookkeeper.client.api.LedgerEntry duplicate() {
            return new CorruptLedgerEntry(delegate.duplicate(), entryId);
        }
        @Override public void close() { delegate.close(); }
    }

    private static final class FailNthWriteOperations implements BookKeeperClientOperations {
        private final BookKeeperClientOperations delegate;
        private final int failedWrite;
        private final AtomicInteger writes = new AtomicInteger();
        private final AtomicBoolean injected = new AtomicBoolean();

        private FailNthWriteOperations(BookKeeperClientOperations delegate, int failedWrite) {
            this.delegate = delegate;
            this.failedWrite = failedWrite;
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
            if (writes.incrementAndGet() == failedWrite && injected.compareAndSet(false, true)) {
                return CompletableFuture.failedFuture(new NereusException(
                        ErrorCode.PRIMARY_WAL_WRITE_FAILED,
                        true,
                        "injected real BookKeeper write cut " + failedWrite));
            }
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
            return delegate.delete(ledgerId, deadline);
        }

        private boolean failureInjected() {
            return injected.get();
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

    private static final class LostCreateResponseOperations implements BookKeeperClientOperations {
        private final BookKeeperClientOperations delegate;
        private final AtomicBoolean inject = new AtomicBoolean(true);

        private LostCreateResponseOperations(BookKeeperClientOperations delegate) {
            this.delegate = delegate;
        }

        @Override
        public CompletableFuture<WriteAdvHandle> createAdvanced(
                long ledgerId,
                BookKeeperWalConfiguration configuration,
                byte[] password,
                Map<String, byte[]> customMetadata,
                BookKeeperOperationDeadline deadline) {
            return delegate.createAdvanced(ledgerId, configuration, password, customMetadata, deadline)
                    .thenCompose(handle -> {
                        if (inject.compareAndSet(true, false)) {
                            return CompletableFuture.failedFuture(new com.nereusstream.api.NereusException(
                                    com.nereusstream.api.ErrorCode.TIMEOUT,
                                    true,
                                    "injected lost CreateAdv response"));
                        }
                        return CompletableFuture.completedFuture(handle);
                    });
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
            return delegate.delete(ledgerId, deadline);
        }
    }

    private static final class DelayedCreateOperations implements BookKeeperClientOperations {
        private final BookKeeperClientOperations delegate;
        private PendingCreate pending;

        private DelayedCreateOperations(BookKeeperClientOperations delegate) {
            this.delegate = delegate;
        }

        @Override
        public synchronized CompletableFuture<WriteAdvHandle> createAdvanced(
                long ledgerId,
                BookKeeperWalConfiguration configuration,
                byte[] password,
                Map<String, byte[]> customMetadata,
                BookKeeperOperationDeadline deadline) {
            if (pending != null) {
                return CompletableFuture.failedFuture(new IllegalStateException(
                        "only one delayed CreateAdv is supported by this fixture"));
            }
            Map<String, byte[]> copiedMetadata = customMetadata.entrySet().stream().collect(
                    java.util.stream.Collectors.toUnmodifiableMap(
                            Map.Entry::getKey, entry -> entry.getValue().clone()));
            pending = new PendingCreate(
                    ledgerId, configuration, password.clone(), copiedMetadata, deadline);
            return CompletableFuture.failedFuture(new com.nereusstream.api.NereusException(
                    com.nereusstream.api.ErrorCode.TIMEOUT,
                    true,
                    "injected create request delayed beyond the original response"));
        }

        private synchronized CompletableFuture<WriteAdvHandle> createNow() {
            PendingCreate exact = java.util.Objects.requireNonNull(pending, "pending delayed create");
            pending = null;
            return delegate.createAdvanced(
                            exact.ledgerId(),
                            exact.configuration(),
                            exact.password(),
                            exact.customMetadata(),
                            exact.deadline())
                    .whenComplete((ignored, failure) -> java.util.Arrays.fill(exact.password(), (byte) 0));
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
            return delegate.delete(ledgerId, deadline);
        }

        private record PendingCreate(
                long ledgerId,
                BookKeeperWalConfiguration configuration,
                byte[] password,
                Map<String, byte[]> customMetadata,
                BookKeeperOperationDeadline deadline) { }
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
