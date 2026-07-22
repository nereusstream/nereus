/* Licensed under the Apache License, Version 2.0 */
package com.nereusstream.bookkeeper;

import static org.assertj.core.api.Assertions.assertThat;

import com.nereusstream.api.Checksum;
import com.nereusstream.api.ChecksumType;
import com.nereusstream.api.StreamId;
import com.nereusstream.metadata.oxia.BookKeeperKeyspace;
import com.nereusstream.metadata.oxia.BookKeeperMetadataStoreConfig;
import com.nereusstream.metadata.oxia.FakeBookKeeperMetadataStore;
import com.nereusstream.metadata.oxia.ResponseLossPartitionedOxiaBackend;
import com.nereusstream.metadata.oxia.records.BookKeeperLedgerLifecycle;
import com.nereusstream.metadata.oxia.records.BookKeeperLedgerProtectionRecord;
import com.nereusstream.metadata.oxia.records.BookKeeperLedgerReaderLeaseRecord;
import com.nereusstream.metadata.oxia.records.BookKeeperLedgerRootRecord;
import com.nereusstream.metadata.oxia.records.BookKeeperProtectionType;
import com.nereusstream.metadata.oxia.records.ProtectionLifecycle;
import java.time.Clock;
import java.time.Duration;
import java.time.Instant;
import java.time.ZoneOffset;
import java.util.Set;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.atomic.AtomicInteger;
import org.junit.jupiter.api.Test;

/** BK-M6 high-cardinality boundaries over the real BK metadata codecs, keyspace, and scanners. */
final class BookKeeperPrimaryWalScaleTest {
    private static final String CLUSTER = "cluster-a";
    private static final String DEPLOYMENT = "deployment-a";
    private static final Clock CLOCK = Clock.fixed(Instant.ofEpochMilli(10_000), ZoneOffset.UTC);

    @Test
    void scansHotAndAllShards() {
        BookKeeperWalConfiguration configuration = configuration(16, 4, 16, 32, 31);
        BookKeeperLedgerIdNamespaceReservation namespace = namespace(configuration);
        BookKeeperKeyspace keys = keys(configuration);
        ResponseLossPartitionedOxiaBackend backend = new ResponseLossPartitionedOxiaBackend();
        int expectedRoots = 1_001 + BookKeeperKeyspace.LEDGER_SHARDS - 1;
        try (FakeBookKeeperMetadataStore seed = metadata(configuration, backend)) {
            long suffix = 1;
            int[] remaining = new int[BookKeeperKeyspace.LEDGER_SHARDS];
            java.util.Arrays.fill(remaining, 1);
            remaining[0] = 1_001;
            int created = 0;
            while (created < expectedRoots) {
                long ledgerId = ledgerId(configuration, suffix++);
                int shard = keys.ledgerShard(keys.ledgerIdentitySha256(
                        configuration.providerScopeSha256(), ledgerId));
                if (remaining[shard] == 0) {
                    continue;
                }
                seed.createRoot(CLUSTER, root(
                                configuration,
                                namespace,
                                ledgerId,
                                BookKeeperLedgerLifecycle.QUARANTINED))
                        .join();
                remaining[shard]--;
                created++;
            }
            assertThat(remaining).containsOnly(0);
        }

        try (FakeBookKeeperMetadataStore restarted = metadata(configuration, backend)) {
            BookKeeperLedgerRetentionScanResult result = scanner(
                            configuration,
                            namespace,
                            restarted,
                            forbidden(),
                            forbiddenRetirement(),
                            forbiddenEvaluation())
                    .scanOnce()
                    .join();
            assertThat(result.shardsScanned()).isEqualTo(BookKeeperKeyspace.LEDGER_SHARDS);
            assertThat(result.rootsScanned()).isEqualTo(expectedRoots);
            assertThat(result.matchingRoots()).isEqualTo(expectedRoots);
            assertThat(result.sealedRoots()).isZero();
            assertThat(result.rootsFailed()).isZero();
        }
    }

    @Test
    void scansMaximumLedgerInventory() {
        BookKeeperWalConfiguration configuration = configuration(256, 64, 64, 32, 127);
        BookKeeperLedgerIdNamespaceReservation namespace = namespace(configuration);
        long ledgerId = ledgerId(configuration, 1);
        int expected = Math.multiplyExact(
                configuration.maxAppendRangesPerLedger(),
                configuration.protectionSlotsPerRange());
        ResponseLossPartitionedOxiaBackend backend = new ResponseLossPartitionedOxiaBackend();
        try (FakeBookKeeperMetadataStore seed = metadata(configuration, backend)) {
            BookKeeperLedgerRootRecord root = root(
                    configuration, namespace, ledgerId, BookKeeperLedgerLifecycle.ACTIVE);
            seed.createRoot(CLUSTER, root).join();
            for (int range = 0; range < configuration.maxAppendRangesPerLedger(); range++) {
                for (int slot = 0; slot < configuration.protectionSlotsPerRange(); slot++) {
                    seed.createProtection(
                                    CLUSTER,
                                    configuration.providerScopeSha256(),
                                    protection(root, range, slot))
                            .join();
                }
            }
        }

        try (FakeBookKeeperMetadataStore restarted = metadata(configuration, backend)) {
            BookKeeperRootCoverageProof proof = proofProducer(
                            configuration, namespace, restarted)
                    .produce(readiness(), Duration.ofSeconds(30))
                    .join();
            assertThat(proof.shardsScanned()).isEqualTo(BookKeeperKeyspace.LEDGER_SHARDS);
            assertThat(proof.matchingRoots()).isOne();
            assertThat(proof.protectionsScanned()).isEqualTo(expected);
            assertThat(proof.readerLeasesScanned()).isZero();
        }
    }

    @Test
    void restartsCompleteInventory() {
        BookKeeperWalConfiguration configuration = configuration(32, 8, 128, 32, 17);
        BookKeeperLedgerIdNamespaceReservation namespace = namespace(configuration);
        long ledgerId = ledgerId(configuration, 2);
        ResponseLossPartitionedOxiaBackend backend = new ResponseLossPartitionedOxiaBackend();
        try (FakeBookKeeperMetadataStore seed = metadata(configuration, backend)) {
            BookKeeperLedgerRootRecord root = root(
                    configuration, namespace, ledgerId, BookKeeperLedgerLifecycle.ACTIVE);
            seed.createRoot(CLUSTER, root).join();
            for (int range = 0; range < configuration.maxAppendRangesPerLedger(); range++) {
                for (int slot = 0; slot < configuration.protectionSlotsPerRange(); slot++) {
                    seed.createProtection(
                                    CLUSTER,
                                    configuration.providerScopeSha256(),
                                    protection(root, range, slot))
                            .join();
                }
            }
            for (int slot = 0; slot < configuration.maxReaderLeasesPerLedger(); slot++) {
                seed.createReaderLease(
                                CLUSTER,
                                configuration.providerScopeSha256(),
                                reader(root, slot))
                        .join();
            }
        }

        try (FakeBookKeeperMetadataStore restarted = metadata(configuration, backend)) {
            BookKeeperRootCoverageProof first = proofProducer(
                            configuration, namespace, restarted)
                    .produce(readiness(), Duration.ofSeconds(30))
                    .join();
            BookKeeperRootCoverageProof second = proofProducer(
                            configuration, namespace, restarted)
                    .produce(readiness(), Duration.ofSeconds(30))
                    .join();
            assertThat(first.protectionsScanned()).isEqualTo(Math.multiplyExact(
                    configuration.maxAppendRangesPerLedger(),
                    configuration.protectionSlotsPerRange()));
            assertThat(first.readerLeasesScanned())
                    .isEqualTo(configuration.maxReaderLeasesPerLedger());
            assertThat(second).isEqualTo(first);
        }
    }

    @Test
    void visitsTenThousandWithoutStackGrowth() {
        BookKeeperWalConfiguration configuration = configuration(16, 4, 16, 32, 113);
        BookKeeperLedgerIdNamespaceReservation namespace = namespace(configuration);
        ResponseLossPartitionedOxiaBackend backend = new ResponseLossPartitionedOxiaBackend();
        try (FakeBookKeeperMetadataStore seed = metadata(configuration, backend)) {
            for (int index = 0; index < 10_000; index++) {
                seed.createRoot(
                                CLUSTER,
                                root(
                                        configuration,
                                        namespace,
                                        ledgerId(configuration, index + 1L),
                                        BookKeeperLedgerLifecycle.SEALED))
                        .join();
            }
        }

        AtomicInteger retirementCalls = new AtomicInteger();
        try (FakeBookKeeperMetadataStore restarted = metadata(configuration, backend)) {
            BookKeeperLedgerRetentionScanResult result = scanner(
                            configuration,
                            namespace,
                            restarted,
                            (root, timeout) -> CompletableFuture.completedFuture(null),
                            (root, timeout) -> {
                                retirementCalls.incrementAndGet();
                                return CompletableFuture.completedFuture(
                                        new BookKeeperWalReferenceRetirementResult(0, 0, 0));
                            },
                            (root, timeout) -> CompletableFuture.completedFuture(
                                    BookKeeperRetentionEvaluation.blocked(
                                            Set.of(BookKeeperRetentionBlocker.PROTECTION_PRESENT))))
                    .scanOnce()
                    .join();
            assertThat(result.rootsScanned()).isEqualTo(10_000);
            assertThat(result.sealedRoots()).isEqualTo(10_000);
            assertThat(result.rootsBlocked()).isEqualTo(10_000);
            assertThat(result.rootsFailed()).isZero();
            assertThat(retirementCalls).hasValue(10_000);
        }
    }

    private static BookKeeperLedgerRetentionScanner scanner(
            BookKeeperWalConfiguration configuration,
            BookKeeperLedgerIdNamespaceReservation namespace,
            FakeBookKeeperMetadataStore metadata,
            java.util.function.BiFunction<
                            com.nereusstream.metadata.oxia.BookKeeperVersionedValue<BookKeeperLedgerRootRecord>,
                            Duration,
                            CompletableFuture<Void>>
                    materialization,
            java.util.function.BiFunction<
                            com.nereusstream.metadata.oxia.BookKeeperVersionedValue<BookKeeperLedgerRootRecord>,
                            Duration,
                            CompletableFuture<BookKeeperWalReferenceRetirementResult>>
                    retirement,
            java.util.function.BiFunction<
                            com.nereusstream.metadata.oxia.BookKeeperVersionedValue<BookKeeperLedgerRootRecord>,
                            Duration,
                            CompletableFuture<BookKeeperRetentionEvaluation>>
                    evaluation) {
        return new BookKeeperLedgerRetentionScanner(
                CLUSTER,
                configuration,
                new BookKeeperLedgerGcConfiguration(
                        1,
                        Duration.ZERO,
                        configuration.readerLeaseTtl(),
                        Duration.ofDays(1),
                        true,
                        false),
                namespace.ledgerIdNamespaceSha256().value(),
                metadata,
                materialization,
                retirement,
                evaluation,
                (candidate, timeout) -> CompletableFuture.failedFuture(new AssertionError("unexpected mark")),
                (root, timeout) -> CompletableFuture.failedFuture(new AssertionError("unexpected convergence")));
    }

    private static java.util.function.BiFunction<
                    com.nereusstream.metadata.oxia.BookKeeperVersionedValue<BookKeeperLedgerRootRecord>,
                    Duration,
                    CompletableFuture<Void>>
            forbidden() {
        return (root, timeout) -> CompletableFuture.failedFuture(new AssertionError("unexpected materialization"));
    }

    private static java.util.function.BiFunction<
                    com.nereusstream.metadata.oxia.BookKeeperVersionedValue<BookKeeperLedgerRootRecord>,
                    Duration,
                    CompletableFuture<BookKeeperWalReferenceRetirementResult>>
            forbiddenRetirement() {
        return (root, timeout) -> CompletableFuture.failedFuture(new AssertionError("unexpected retirement"));
    }

    private static java.util.function.BiFunction<
                    com.nereusstream.metadata.oxia.BookKeeperVersionedValue<BookKeeperLedgerRootRecord>,
                    Duration,
                    CompletableFuture<BookKeeperRetentionEvaluation>>
            forbiddenEvaluation() {
        return (root, timeout) -> CompletableFuture.failedFuture(new AssertionError("unexpected evaluation"));
    }

    private static BookKeeperRootCoverageProofProducer proofProducer(
            BookKeeperWalConfiguration configuration,
            BookKeeperLedgerIdNamespaceReservation namespace,
            FakeBookKeeperMetadataStore metadata) {
        return new BookKeeperRootCoverageProofProducer(
                CLUSTER,
                configuration,
                namespace.ledgerIdNamespaceSha256().value(),
                metadata);
    }

    private static BookKeeperLedgerProtectionRecord protection(
            BookKeeperLedgerRootRecord root, int range, int slot) {
        return new BookKeeperLedgerProtectionRecord(
                1,
                root.ledgerIdentitySha256(),
                root.clusterAlias(),
                root.ledgerId(),
                root.lifecycleEpoch(),
                range,
                slot,
                BookKeeperProtectionType.REACHABLE_APPEND.wireId(),
                "scale-reference-" + range + "-" + slot,
                range,
                1,
                "55".repeat(32),
                root.streamId(),
                range,
                range + 1L,
                range + 1L,
                "/scale/owners/" + range + "/" + slot,
                1,
                "66".repeat(32),
                ProtectionLifecycle.ACTIVE,
                10_000,
                0,
                0);
    }

    private static BookKeeperLedgerReaderLeaseRecord reader(
            BookKeeperLedgerRootRecord root, int slot) {
        return new BookKeeperLedgerReaderLeaseRecord(
                1,
                root.ledgerIdentitySha256(),
                root.ledgerId(),
                root.lifecycleEpoch(),
                slot,
                "scale-process-" + slot,
                1,
                10_000,
                20_000,
                0);
    }

    private static BookKeeperLedgerRootRecord root(
            BookKeeperWalConfiguration configuration,
            BookKeeperLedgerIdNamespaceReservation namespace,
            long ledgerId,
            BookKeeperLedgerLifecycle lifecycle) {
        String stream = "scale-stream-" + ledgerId;
        String allocation = "scale-allocation-" + ledgerId;
        BookKeeperLedgerCustomMetadata custom = BookKeeperLedgerCustomMetadata.create(
                CLUSTER,
                configuration,
                namespace,
                new StreamId(stream),
                0,
                allocation);
        boolean sealed = lifecycle == BookKeeperLedgerLifecycle.SEALED;
        boolean quarantined = lifecycle == BookKeeperLedgerLifecycle.QUARANTINED;
        return new BookKeeperLedgerRootRecord(
                1,
                keys(configuration).ledgerIdentitySha256(
                        configuration.providerScopeSha256(), ledgerId),
                configuration.clusterAlias(),
                configuration.providerScopeSha256(),
                ledgerId,
                stream,
                0,
                allocation,
                Math.floorMod((int) ledgerId, configuration.maxUncertainAllocations()),
                configuration.configurationBindingSha256().value(),
                namespace.ledgerIdNamespaceSha256().value(),
                false,
                "scale-writer",
                "33".repeat(32),
                1,
                "44".repeat(32),
                configuration.ensembleSize(),
                configuration.writeQuorumSize(),
                configuration.ackQuorumSize(),
                configuration.digestType().name(),
                custom.sha256().value(),
                lifecycle,
                1,
                1,
                quarantined ? 0 : 2,
                sealed ? 3 : 0,
                sealed ? 4 : 0,
                -1,
                0,
                sealed ? "scale seal" : "",
                "",
                "",
                0,
                0,
                0,
                0,
                0,
                quarantined ? "scale quarantine" : "",
                0);
    }

    private static BookKeeperBrokerReadiness readiness() {
        return new BookKeeperBrokerReadiness(
                9,
                new Checksum(ChecksumType.SHA256, "88".repeat(32)),
                2);
    }

    private static BookKeeperLedgerIdNamespaceReservation namespace(
            BookKeeperWalConfiguration configuration) {
        return new BookKeeperLedgerIdNamespaceReservation(
                1,
                configuration.ledgerIdNamespaceReservationId(),
                DEPLOYMENT,
                configuration.clusterAlias(),
                configuration.providerScopeSha256(),
                configuration.ledgerIdPrefixBits(),
                configuration.ledgerIdPrefixValue(),
                BookKeeperLedgerIdNamespaceReservation.Lifecycle.ACTIVE,
                1,
                1,
                0,
                "77".repeat(32),
                3,
                new Checksum(ChecksumType.SHA256, "99".repeat(32)),
                "/bookkeeper/scale-reservation");
    }

    private static long ledgerId(BookKeeperWalConfiguration configuration, long suffix) {
        int suffixBits = configuration.ledgerIdNamespace().suffixBits();
        long result = (configuration.ledgerIdPrefixValue() << suffixBits) | suffix;
        if (!configuration.ledgerIdNamespace().contains(result)) {
            throw new AssertionError("scale ledger id left the reserved namespace");
        }
        return result;
    }

    private static BookKeeperKeyspace keys(BookKeeperWalConfiguration configuration) {
        return metadataConfig(configuration).keyspace(CLUSTER);
    }

    private static FakeBookKeeperMetadataStore metadata(
            BookKeeperWalConfiguration configuration,
            ResponseLossPartitionedOxiaBackend backend) {
        return new FakeBookKeeperMetadataStore(metadataConfig(configuration), CLOCK, backend);
    }

    private static BookKeeperMetadataStoreConfig metadataConfig(
            BookKeeperWalConfiguration configuration) {
        return new BookKeeperMetadataStoreConfig(
                configuration.maxAppendRangesPerLedger(),
                configuration.protectionSlotsPerRange(),
                configuration.maxReaderLeasesPerLedger(),
                configuration.maxUncertainAllocations());
    }

    private static BookKeeperWalConfiguration configuration(
            int maxRanges,
            int protectionSlots,
            int readerSlots,
            int uncertainSlots,
            int pageSize) {
        BookKeeperWalConfiguration value = BookKeeperTestConfigurations.valid();
        return new BookKeeperWalConfiguration(
                value.clusterAlias(),
                value.providerScopeSha256(),
                value.ledgerIdPrefixBits(),
                value.ledgerIdPrefixValue(),
                value.ledgerIdNamespaceReservationId(),
                value.ensembleSize(),
                value.writeQuorumSize(),
                value.ackQuorumSize(),
                value.digestType(),
                value.passwordRef(),
                value.maxEntriesPerLedger(),
                value.maxBytesPerLedger(),
                maxRanges,
                protectionSlots,
                readerSlots,
                uncertainSlots,
                value.maxLedgerAge(),
                value.maxWritesInFlight(),
                value.maxReadsInFlight(),
                value.maxReadBytesInFlight(),
                value.operationTimeout(),
                value.allocationTimeout(),
                value.sealTimeout(),
                value.deleteTimeout(),
                value.readerLeaseTtl(),
                value.readerLeaseRenewInterval(),
                value.retentionScanInterval(),
                pageSize);
    }
}
