/* Licensed under the Apache License, Version 2.0 */
package com.nereusstream.bookkeeper;

import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.assertThatThrownBy;

import com.nereusstream.metadata.oxia.BookKeeperMetadataStoreConfig;
import com.nereusstream.metadata.oxia.BookKeeperVersionedValue;
import com.nereusstream.metadata.oxia.FakeBookKeeperMetadataStore;
import com.nereusstream.metadata.oxia.records.BookKeeperLedgerLifecycle;
import com.nereusstream.metadata.oxia.records.BookKeeperLedgerRootRecord;
import java.time.Duration;
import java.util.Set;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.Executors;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicInteger;
import org.junit.jupiter.api.Test;

final class BookKeeperLedgerRetentionScannerTest {
    private static final String CLUSTER = "cluster-a";
    private static final String NAMESPACE_SHA256 = "22".repeat(32);

    @Test
    void scansEveryShardAndRoutesOnlyTheExactBinding() {
        BookKeeperWalConfiguration wal = BookKeeperTestConfigurations.valid();
        BookKeeperLedgerGcConfiguration gc = enabledGc(wal);
        try (FakeBookKeeperMetadataStore metadata = metadata(wal)) {
            metadata.createRoot(CLUSTER, root(wal, 101, BookKeeperLedgerLifecycle.SEALED, true)).join();
            metadata.createRoot(CLUSTER, root(wal, 102, BookKeeperLedgerLifecycle.MARKED, true)).join();
            metadata.createRoot(CLUSTER, root(wal, 103, BookKeeperLedgerLifecycle.SEALED, false)).join();
            AtomicInteger triggerCalls = new AtomicInteger();
            AtomicInteger retirementCalls = new AtomicInteger();
            AtomicInteger convergenceCalls = new AtomicInteger();
            BookKeeperLedgerRetentionScanner scanner = new BookKeeperLedgerRetentionScanner(
                    CLUSTER,
                    wal,
                    gc,
                    NAMESPACE_SHA256,
                    metadata,
                    (root, timeout) -> {
                        triggerCalls.incrementAndGet();
                        return CompletableFuture.completedFuture(null);
                    },
                    (root, timeout) -> {
                        retirementCalls.incrementAndGet();
                        return CompletableFuture.completedFuture(
                                new BookKeeperWalReferenceRetirementResult(3, 2, 1));
                    },
                    (root, timeout) -> CompletableFuture.completedFuture(
                            BookKeeperRetentionEvaluation.blocked(
                                    Set.of(BookKeeperRetentionBlocker.PROTECTION_PRESENT))),
                    (candidate, timeout) -> CompletableFuture.failedFuture(
                            new AssertionError("blocked root must not be marked")),
                    (root, timeout) -> {
                        convergenceCalls.incrementAndGet();
                        return CompletableFuture.completedFuture(BookKeeperLedgerGcResult.of(
                                BookKeeperLedgerGcAction.WAITING_DRAIN, root));
                    });

            BookKeeperLedgerRetentionScanResult result = scanner.scanOnce().join();

            assertThat(result.mutationEnabled()).isTrue();
            assertThat(result.shardsScanned()).isEqualTo(256);
            assertThat(result.rootsScanned()).isEqualTo(3);
            assertThat(result.matchingRoots()).isEqualTo(2);
            assertThat(result.sealedRoots()).isOne();
            assertThat(result.inFlightRoots()).isOne();
            assertThat(result.materializationTriggers()).isOne();
            assertThat(result.protectionsRetired()).isEqualTo(2);
            assertThat(result.rootsBlocked()).isOne();
            assertThat(result.rootsAdvanced()).isOne();
            assertThat(result.rootsFailed()).isZero();
            assertThat(triggerCalls).hasValue(1);
            assertThat(retirementCalls).hasValue(1);
            assertThat(convergenceCalls).hasValue(1);
        }
    }

    @Test
    void materializationHintFailureDoesNotOwnReferenceOrDeletionCorrectness() {
        BookKeeperWalConfiguration wal = BookKeeperTestConfigurations.valid();
        try (FakeBookKeeperMetadataStore metadata = metadata(wal)) {
            metadata.createRoot(CLUSTER, root(wal, 201, BookKeeperLedgerLifecycle.SEALED, true)).join();
            AtomicInteger retirementCalls = new AtomicInteger();
            BookKeeperLedgerRetentionScanner scanner = new BookKeeperLedgerRetentionScanner(
                    CLUSTER,
                    wal,
                    enabledGc(wal),
                    NAMESPACE_SHA256,
                    metadata,
                    (root, timeout) -> CompletableFuture.failedFuture(
                            new IllegalStateException("injected materialization hint failure")),
                    (root, timeout) -> {
                        retirementCalls.incrementAndGet();
                        return CompletableFuture.completedFuture(
                                new BookKeeperWalReferenceRetirementResult(3, 0, 3));
                    },
                    (root, timeout) -> CompletableFuture.completedFuture(
                            BookKeeperRetentionEvaluation.blocked(
                                    Set.of(BookKeeperRetentionBlocker.PROTECTION_PRESENT))),
                    (candidate, timeout) -> CompletableFuture.failedFuture(new AssertionError()),
                    (root, timeout) -> CompletableFuture.failedFuture(new AssertionError()));

            BookKeeperLedgerRetentionScanResult result = scanner.scanOnce().join();

            assertThat(result.materializationTriggerFailures()).isOne();
            assertThat(result.rootsBlocked()).isOne();
            assertThat(result.rootsFailed()).isZero();
            assertThat(retirementCalls).hasValue(1);
        }
    }

    @Test
    void oneRootFailureIsAccountedAndDoesNotStopLaterRootsOrShards() {
        BookKeeperWalConfiguration wal = BookKeeperTestConfigurations.valid();
        try (FakeBookKeeperMetadataStore metadata = metadata(wal)) {
            metadata.createRoot(CLUSTER, root(wal, 301, BookKeeperLedgerLifecycle.SEALED, true)).join();
            metadata.createRoot(CLUSTER, root(wal, 302, BookKeeperLedgerLifecycle.SEALED, true)).join();
            AtomicInteger retirementCalls = new AtomicInteger();
            BookKeeperLedgerRetentionScanner scanner = new BookKeeperLedgerRetentionScanner(
                    CLUSTER,
                    wal,
                    enabledGc(wal),
                    NAMESPACE_SHA256,
                    metadata,
                    (root, timeout) -> CompletableFuture.completedFuture(null),
                    (root, timeout) -> retirementCalls.incrementAndGet() == 1
                            ? CompletableFuture.failedFuture(
                                    new IllegalStateException("injected root failure"))
                            : CompletableFuture.completedFuture(
                                    new BookKeeperWalReferenceRetirementResult(3, 0, 3)),
                    (root, timeout) -> CompletableFuture.completedFuture(
                            BookKeeperRetentionEvaluation.blocked(
                                    Set.of(BookKeeperRetentionBlocker.PROTECTION_PRESENT))),
                    (candidate, timeout) -> CompletableFuture.failedFuture(new AssertionError()),
                    (root, timeout) -> CompletableFuture.failedFuture(new AssertionError()));

            BookKeeperLedgerRetentionScanResult result = scanner.scanOnce().join();

            assertThat(result.shardsScanned()).isEqualTo(256);
            assertThat(result.rootsScanned()).isEqualTo(2);
            assertThat(result.rootsFailed()).isOne();
            assertThat(result.rootsBlocked()).isOne();
            assertThat(retirementCalls).hasValue(2);
        }
    }

    @Test
    void disabledAndDryRunModesNeverEvenScanTheDurableInventory() {
        BookKeeperWalConfiguration wal = BookKeeperTestConfigurations.valid();
        try (FakeBookKeeperMetadataStore metadata = metadata(wal)) {
            BookKeeperLedgerGcConfiguration disabled = BookKeeperLedgerGcConfiguration.safeDefault();
            BookKeeperLedgerRetentionScanner scanner = scannerWithForbiddenCallbacks(
                    wal, disabled, metadata);
            assertThat(scanner.scanOnce().join().mutationEnabled()).isFalse();

            BookKeeperLedgerGcConfiguration dryRun = new BookKeeperLedgerGcConfiguration(
                    1,
                    Duration.ZERO,
                    wal.readerLeaseTtl(),
                    Duration.ofDays(1),
                    true,
                    true);
            assertThat(scannerWithForbiddenCallbacks(wal, dryRun, metadata)
                            .scanOnce()
                            .join()
                            .mutationEnabled())
                    .isFalse();
        }
    }

    @Test
    void serviceCoalescesOverlappingHintsAndClosesWithoutOwningExecutors() throws Exception {
        var scheduler = Executors.newSingleThreadScheduledExecutor();
        try {
            CountDownLatch firstStarted = new CountDownLatch(1);
            CompletableFuture<BookKeeperLedgerRetentionScanResult> first = new CompletableFuture<>();
            AtomicInteger calls = new AtomicInteger();
            BookKeeperLedgerRetentionScanResult result = emptyEnabledResult();
            BookKeeperLedgerRetentionService service = new BookKeeperLedgerRetentionService(
                    () -> {
                        int call = calls.incrementAndGet();
                        if (call == 1) {
                            firstStarted.countDown();
                            return first;
                        }
                        return CompletableFuture.completedFuture(result);
                    },
                    Duration.ofHours(1),
                    Duration.ofSeconds(5),
                    scheduler,
                    Runnable::run);
            service.start().join();
            assertThat(firstStarted.await(5, TimeUnit.SECONDS)).isTrue();

            CompletableFuture<BookKeeperLedgerRetentionScanResult> hinted = service.scanNow();
            assertThat(calls).hasValue(1);
            first.complete(result);
            assertThat(hinted.get(5, TimeUnit.SECONDS)).isEqualTo(result);
            for (int attempt = 0; attempt < 100 && calls.get() < 2; attempt++) {
                Thread.onSpinWait();
                Thread.sleep(1);
            }
            assertThat(calls).hasValue(2);

            service.closeAsync().get(5, TimeUnit.SECONDS);
            assertThat(service.isRunning()).isFalse();
            assertThat(scheduler.isShutdown()).isFalse();
            assertThatThrownBy(() -> service.scanNow().join())
                    .hasMessageContaining("closing");
        } finally {
            scheduler.shutdownNow();
        }
    }

    private static BookKeeperLedgerRetentionScanner scannerWithForbiddenCallbacks(
            BookKeeperWalConfiguration wal,
            BookKeeperLedgerGcConfiguration gc,
            FakeBookKeeperMetadataStore metadata) {
        return new BookKeeperLedgerRetentionScanner(
                CLUSTER,
                wal,
                gc,
                NAMESPACE_SHA256,
                metadata,
                (root, timeout) -> CompletableFuture.failedFuture(new AssertionError()),
                (root, timeout) -> CompletableFuture.failedFuture(new AssertionError()),
                (root, timeout) -> CompletableFuture.failedFuture(new AssertionError()),
                (candidate, timeout) -> CompletableFuture.failedFuture(new AssertionError()),
                (root, timeout) -> CompletableFuture.failedFuture(new AssertionError()));
    }

    private static BookKeeperLedgerRetentionScanResult emptyEnabledResult() {
        return new BookKeeperLedgerRetentionScanResult(
                true, 256, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0);
    }

    private static FakeBookKeeperMetadataStore metadata(BookKeeperWalConfiguration wal) {
        return new FakeBookKeeperMetadataStore(new BookKeeperMetadataStoreConfig(
                wal.maxAppendRangesPerLedger(),
                wal.protectionSlotsPerRange(),
                wal.maxReaderLeasesPerLedger(),
                wal.maxUncertainAllocations()));
    }

    private static BookKeeperLedgerGcConfiguration enabledGc(BookKeeperWalConfiguration wal) {
        return new BookKeeperLedgerGcConfiguration(
                1,
                Duration.ZERO,
                wal.readerLeaseTtl(),
                Duration.ofDays(1),
                true,
                false);
    }

    private static BookKeeperLedgerRootRecord root(
            BookKeeperWalConfiguration wal,
            long ledgerId,
            BookKeeperLedgerLifecycle lifecycle,
            boolean exactBinding) {
        var keys = new com.nereusstream.metadata.oxia.BookKeeperKeyspace(
                CLUSTER,
                wal.maxAppendRangesPerLedger(),
                wal.protectionSlotsPerRange(),
                wal.maxReaderLeasesPerLedger(),
                wal.maxUncertainAllocations());
        boolean marked = lifecycle == BookKeeperLedgerLifecycle.MARKED
                || lifecycle == BookKeeperLedgerLifecycle.DELETING
                || lifecycle == BookKeeperLedgerLifecycle.DELETED;
        return new BookKeeperLedgerRootRecord(
                1,
                keys.ledgerIdentitySha256(wal.providerScopeSha256(), ledgerId),
                wal.clusterAlias(),
                wal.providerScopeSha256(),
                ledgerId,
                "stream-" + ledgerId,
                0,
                "allocation-" + ledgerId,
                0,
                exactBinding ? wal.configurationBindingSha256().value() : "aa".repeat(32),
                NAMESPACE_SHA256,
                false,
                "writer-1",
                "33".repeat(32),
                1,
                "44".repeat(32),
                wal.ensembleSize(),
                wal.writeQuorumSize(),
                wal.ackQuorumSize(),
                wal.digestType().name(),
                "55".repeat(32),
                lifecycle,
                3,
                1,
                2,
                3,
                4,
                -1,
                0,
                "test seal",
                marked ? "gc-" + ledgerId : "",
                marked ? "66".repeat(32) : "",
                marked ? 5 : 0,
                marked ? 6 : 0,
                lifecycle == BookKeeperLedgerLifecycle.DELETING ? 7 : 0,
                0,
                0,
                "",
                0);
    }
}
