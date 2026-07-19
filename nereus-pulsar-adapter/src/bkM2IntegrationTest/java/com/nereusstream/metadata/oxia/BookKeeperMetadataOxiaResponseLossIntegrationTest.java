/* Licensed under the Apache License, Version 2.0 */
package com.nereusstream.metadata.oxia;

import static org.assertj.core.api.Assertions.assertThat;

import com.nereusstream.metadata.oxia.records.BookKeeperWriterStateRecord;
import com.nereusstream.metadata.oxia.records.LedgerAllocationIntentRecord;
import com.nereusstream.metadata.oxia.records.LedgerAllocationLifecycle;
import com.nereusstream.metadata.oxia.testing.BookKeeperMetadataStoreContractScenario;
import io.oxia.testcontainers.OxiaContainer;
import java.time.Clock;
import java.util.List;
import java.util.Optional;
import java.util.UUID;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.atomic.AtomicInteger;
import java.util.concurrent.atomic.AtomicReference;
import org.junit.jupiter.api.Test;
import org.testcontainers.junit.jupiter.Container;
import org.testcontainers.junit.jupiter.Testcontainers;
import org.testcontainers.utility.DockerImageName;

/** Real-Oxia proof for the exact reload logic used by every BK metadata allocation stage. */
@Testcontainers
class BookKeeperMetadataOxiaResponseLossIntegrationTest {
    @Container
    private static final OxiaContainer OXIA = new OxiaContainer(
            DockerImageName.parse("oxia/oxia:0.16.3")).withShards(4);

    @Test
    void productionAdapterReloadsEveryAppliedBookKeeperMutationResponseLoss() {
        String cluster = "bk-m2-real-oxia-response-loss-"
                + UUID.randomUUID().toString().replace("-", "");
        Clock clock = Clock.systemUTC();
        OxiaClientConfiguration configuration = OxiaClientConfiguration.defaults(OXIA.getServiceAddress());
        BookKeeperMetadataStoreConfig storeConfiguration = new BookKeeperMetadataStoreConfig(8, 8, 8, 16);

        try (SharedOxiaClientRuntime runtime = SharedOxiaClientRuntime.connect(configuration, clock)) {
            AppliedResponseLossBackend responseLoss = new AppliedResponseLossBackend(runtime.client());
            BookKeeperVersionedValue<BookKeeperWriterStateRecord> updated;
            String allocationId = "allocation-real-loss-" + UUID.randomUUID().toString().replace("-", "");
            try (OxiaJavaBookKeeperMetadataStore store = new OxiaJavaBookKeeperMetadataStore(
                    new PartitionedOxiaClient(responseLoss), clock, storeConfiguration)) {
                responseLoss.arm(Operation.PUT_IF_ABSENT);
                BookKeeperVersionedValue<BookKeeperWriterStateRecord> created = store.createWriter(
                                cluster,
                                BookKeeperMetadataStoreContractScenario.activeWriter(3, 3, 300, 2))
                        .join();

                responseLoss.arm(Operation.PUT_IF_VERSION);
                updated = store.compareAndSetWriter(
                                cluster,
                                BookKeeperMetadataStoreContractScenario.activeWriter(4, 5, 350, 3),
                                created.metadataVersion())
                        .join();
                assertThat(updated.metadataVersion()).isGreaterThan(created.metadataVersion());

                LedgerAllocationIntentRecord allocation = allocation(allocationId);
                BookKeeperVersionedValue<LedgerAllocationIntentRecord> stored =
                        store.createAllocation(cluster, allocation).join();
                responseLoss.arm(Operation.DELETE_IF_VERSION);
                store.deleteAllocation(
                                cluster,
                                BookKeeperMetadataStoreContractScenario.STREAM,
                                allocationId,
                                stored.metadataVersion())
                        .join();
            }

            try (OxiaJavaBookKeeperMetadataStore restarted = new OxiaJavaBookKeeperMetadataStore(
                    runtime.client(), clock, storeConfiguration)) {
                assertThat(restarted.getWriter(
                                cluster, BookKeeperMetadataStoreContractScenario.STREAM)
                        .join()).contains(updated);
                assertThat(restarted.getAllocation(
                                cluster,
                                BookKeeperMetadataStoreContractScenario.STREAM,
                                allocationId)
                        .join()).isEmpty();
            }
            assertThat(responseLoss.lostResponses()).isEqualTo(3);
        }
    }

    private static LedgerAllocationIntentRecord allocation(String allocationId) {
        LedgerAllocationIntentRecord base = BookKeeperMetadataStoreContractScenario.allocation(
                LedgerAllocationLifecycle.PREPARED, false, "", 120);
        return new LedgerAllocationIntentRecord(
                base.schemaVersion(),
                allocationId,
                base.streamId(),
                base.segmentSequence(),
                base.clusterAlias(),
                base.candidateLedgerId(),
                base.allocationSlot(),
                base.configurationBindingSha256(),
                base.writerId(),
                base.writerRunIdHash(),
                base.appendSessionEpoch(),
                base.fencingTokenHash(),
                base.writerStateEpoch(),
                base.lifecycle(),
                base.lateCreateHazard(),
                base.bookKeeperMetadataSha256(),
                base.createdAtMillis(),
                base.updatedAtMillis(),
                base.stateReason(),
                0);
    }

    private enum Operation {
        PUT_IF_ABSENT,
        PUT_IF_VERSION,
        DELETE_IF_VERSION
    }

    private static final class AppliedResponseLossBackend implements PartitionedOxiaClient.Backend {
        private final PartitionedOxiaClient delegate;
        private final AtomicReference<Operation> armed = new AtomicReference<>();
        private final AtomicInteger lostResponses = new AtomicInteger();

        private AppliedResponseLossBackend(PartitionedOxiaClient delegate) {
            this.delegate = java.util.Objects.requireNonNull(delegate, "delegate");
        }

        private void arm(Operation operation) {
            if (!armed.compareAndSet(null, operation)) {
                throw new IllegalStateException("one Oxia response-loss cut is already armed");
            }
        }

        @Override
        public CompletableFuture<Optional<PartitionedOxiaClient.VersionedValue>> get(
                String key, PartitionKey partitionKey) {
            return delegate.get(key, partitionKey);
        }

        @Override
        public CompletableFuture<PartitionedOxiaClient.WriteResult> putIfAbsent(
                String key, byte[] value, PartitionKey partitionKey) {
            return loseAfterApply(
                    Operation.PUT_IF_ABSENT,
                    delegate.putIfAbsent(key, value, partitionKey));
        }

        @Override
        public CompletableFuture<PartitionedOxiaClient.WriteResult> putIfVersion(
                String key, byte[] value, long expectedVersion, PartitionKey partitionKey) {
            return loseAfterApply(
                    Operation.PUT_IF_VERSION,
                    delegate.putIfVersion(key, value, expectedVersion, partitionKey));
        }

        @Override
        public CompletableFuture<Void> deleteIfVersion(
                String key, long expectedVersion, PartitionKey partitionKey) {
            return loseAfterApply(
                    Operation.DELETE_IF_VERSION,
                    delegate.deleteIfVersion(key, expectedVersion, partitionKey));
        }

        @Override
        public CompletableFuture<List<String>> list(
                String fromInclusive, String toExclusive, PartitionKey partitionKey) {
            return delegate.list(fromInclusive, toExclusive, partitionKey);
        }

        @Override
        public CompletableFuture<List<PartitionedOxiaClient.VersionedValue>> rangeScan(
                String fromInclusive,
                String toExclusive,
                int limit,
                PartitionKey partitionKey) {
            return delegate.rangeScan(fromInclusive, toExclusive, limit, partitionKey);
        }

        @Override
        public WatchRegistration watchPrefix(
                String prefix, PartitionKey partitionKey, Runnable invalidationCallback) {
            return delegate.watchPrefix(prefix, partitionKey, invalidationCallback);
        }

        private <T> CompletableFuture<T> loseAfterApply(
                Operation operation,
                CompletableFuture<T> applied) {
            if (!armed.compareAndSet(operation, null)) {
                return applied;
            }
            return applied.thenCompose(ignored -> {
                lostResponses.incrementAndGet();
                return CompletableFuture.failedFuture(
                        new IllegalStateException("injected applied " + operation + " response loss"));
            });
        }

        private int lostResponses() {
            return lostResponses.get();
        }
    }
}
