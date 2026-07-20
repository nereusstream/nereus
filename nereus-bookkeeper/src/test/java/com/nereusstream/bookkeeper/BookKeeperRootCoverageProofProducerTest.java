/* Licensed under the Apache License, Version 2.0 */
package com.nereusstream.bookkeeper;

import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.assertThatThrownBy;

import com.nereusstream.api.Checksum;
import com.nereusstream.api.ChecksumType;
import com.nereusstream.api.StreamId;
import com.nereusstream.metadata.oxia.BookKeeperKeyspace;
import com.nereusstream.metadata.oxia.BookKeeperMetadataStoreConfig;
import com.nereusstream.metadata.oxia.FakeBookKeeperMetadataStore;
import com.nereusstream.metadata.oxia.records.BookKeeperLedgerLifecycle;
import com.nereusstream.metadata.oxia.records.BookKeeperLedgerRootRecord;
import java.time.Duration;
import org.junit.jupiter.api.Test;

final class BookKeeperRootCoverageProofProducerTest {
    private static final String CLUSTER = "cluster-a";
    private static final String DEPLOYMENT = "deployment-a";

    @Test
    void provesEveryShardAndHashesOnlyTheExactBindingDeterministically() {
        BookKeeperWalConfiguration configuration = BookKeeperTestConfigurations.valid();
        BookKeeperLedgerIdNamespaceReservation namespace = namespace(configuration);
        try (FakeBookKeeperMetadataStore metadata = metadata(configuration)) {
            metadata.createRoot(
                    CLUSTER,
                    root(configuration, namespace, candidate(configuration, 7), true))
                    .join();
            metadata.createRoot(
                    CLUSTER,
                    root(configuration, namespace, candidate(configuration, 11), false))
                    .join();
            BookKeeperRootCoverageProofProducer producer =
                    new BookKeeperRootCoverageProofProducer(
                            CLUSTER,
                            configuration,
                            namespace.ledgerIdNamespaceSha256().value(),
                            metadata);
            BookKeeperBrokerReadiness readiness = readiness();

            BookKeeperRootCoverageProof first = producer
                    .produce(readiness, Duration.ofSeconds(10))
                    .join();
            BookKeeperRootCoverageProof second = producer
                    .produce(readiness, Duration.ofSeconds(10))
                    .join();

            assertThat(first.shardsScanned()).isEqualTo(256);
            assertThat(first.rootsScanned()).isEqualTo(2);
            assertThat(first.matchingRoots()).isOne();
            assertThat(first.protectionsScanned()).isZero();
            assertThat(first.readerLeasesScanned()).isZero();
            assertThat(second.coverageSha256()).isEqualTo(first.coverageSha256());
        }
    }

    @Test
    void rejectsAnExactBindingWhoseProviderMetadataDigestCannotBeReconstructed() {
        BookKeeperWalConfiguration configuration = BookKeeperTestConfigurations.valid();
        BookKeeperLedgerIdNamespaceReservation namespace = namespace(configuration);
        long ledgerId = candidate(configuration, 17);
        try (FakeBookKeeperMetadataStore metadata = metadata(configuration)) {
            BookKeeperLedgerRootRecord valid = root(configuration, namespace, ledgerId, true);
            BookKeeperLedgerRootRecord corrupt = new BookKeeperLedgerRootRecord(
                    valid.schemaVersion(),
                    valid.ledgerIdentitySha256(),
                    valid.clusterAlias(),
                    valid.providerScopeSha256(),
                    valid.ledgerId(),
                    valid.streamId(),
                    valid.segmentSequence(),
                    valid.allocationId(),
                    valid.allocationSlot(),
                    valid.configurationBindingSha256(),
                    valid.ledgerIdNamespaceSha256(),
                    valid.lateCreateHazard(),
                    valid.writerId(),
                    valid.writerRunIdHash(),
                    valid.appendSessionEpoch(),
                    valid.fencingTokenHash(),
                    valid.ensembleSize(),
                    valid.writeQuorumSize(),
                    valid.ackQuorumSize(),
                    valid.digestType(),
                    "ff".repeat(32),
                    valid.lifecycle(),
                    valid.lifecycleEpoch(),
                    valid.createdAtMillis(),
                    valid.activatedAtMillis(),
                    valid.sealStartedAtMillis(),
                    valid.sealedAtMillis(),
                    valid.sealedLastEntryId(),
                    valid.sealedLength(),
                    valid.sealReason(),
                    valid.gcAttemptId(),
                    valid.referenceSetSha256(),
                    valid.markedAtMillis(),
                    valid.deleteNotBeforeMillis(),
                    valid.deleteStartedAtMillis(),
                    valid.firstAbsentAtMillis(),
                    valid.deletedAtMillis(),
                    valid.stateReason(),
                    0);
            metadata.createRoot(CLUSTER, corrupt).join();
            BookKeeperRootCoverageProofProducer producer =
                    new BookKeeperRootCoverageProofProducer(
                            CLUSTER,
                            configuration,
                            namespace.ledgerIdNamespaceSha256().value(),
                            metadata);

            assertThatThrownBy(() -> producer
                            .produce(readiness(), Duration.ofSeconds(10))
                            .join())
                    .hasRootCauseMessage(
                            "BookKeeper root custom-metadata digest cannot be reconstructed");
        }
    }

    private static BookKeeperLedgerRootRecord root(
            BookKeeperWalConfiguration configuration,
            BookKeeperLedgerIdNamespaceReservation namespace,
            long ledgerId,
            boolean exactBinding) {
        String stream = "coverage-stream-" + ledgerId;
        String allocation = "coverage-allocation-" + ledgerId;
        BookKeeperLedgerCustomMetadata custom = BookKeeperLedgerCustomMetadata.create(
                CLUSTER,
                configuration,
                namespace,
                new StreamId(stream),
                0,
                allocation);
        BookKeeperKeyspace keys = metadataConfig(configuration).keyspace(CLUSTER);
        return new BookKeeperLedgerRootRecord(
                1,
                keys.ledgerIdentitySha256(configuration.providerScopeSha256(), ledgerId),
                configuration.clusterAlias(),
                configuration.providerScopeSha256(),
                ledgerId,
                stream,
                0,
                allocation,
                0,
                exactBinding
                        ? configuration.configurationBindingSha256().value()
                        : "aa".repeat(32),
                namespace.ledgerIdNamespaceSha256().value(),
                false,
                "coverage-writer",
                "33".repeat(32),
                0,
                "44".repeat(32),
                configuration.ensembleSize(),
                configuration.writeQuorumSize(),
                configuration.ackQuorumSize(),
                configuration.digestType().name(),
                custom.sha256().value(),
                BookKeeperLedgerLifecycle.QUARANTINED,
                1,
                1,
                0,
                0,
                0,
                -1,
                0,
                "",
                "",
                "",
                0,
                0,
                0,
                0,
                0,
                "coverage fixture",
                0);
    }

    private static long candidate(BookKeeperWalConfiguration configuration, long suffix) {
        int suffixBits = configuration.ledgerIdNamespace().suffixBits();
        long ledgerId = (configuration.ledgerIdPrefixValue() << suffixBits) | suffix;
        assertThat(configuration.ledgerIdNamespace().contains(ledgerId)).isTrue();
        return ledgerId;
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
                "66".repeat(32),
                3,
                new Checksum(ChecksumType.SHA256, "77".repeat(32)),
                "/bookkeeper/coverage-reservation");
    }

    private static BookKeeperMetadataStoreConfig metadataConfig(
            BookKeeperWalConfiguration configuration) {
        return new BookKeeperMetadataStoreConfig(
                configuration.maxAppendRangesPerLedger(),
                configuration.protectionSlotsPerRange(),
                configuration.maxReaderLeasesPerLedger(),
                configuration.maxUncertainAllocations());
    }

    private static FakeBookKeeperMetadataStore metadata(
            BookKeeperWalConfiguration configuration) {
        return new FakeBookKeeperMetadataStore(metadataConfig(configuration));
    }
}
