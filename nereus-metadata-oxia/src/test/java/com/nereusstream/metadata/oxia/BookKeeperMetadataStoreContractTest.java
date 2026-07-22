/* Licensed under the Apache License, Version 2.0 */
package com.nereusstream.metadata.oxia;

import static com.nereusstream.metadata.oxia.testing.BookKeeperMetadataStoreContractScenario.CLUSTER;
import static com.nereusstream.metadata.oxia.testing.BookKeeperMetadataStoreContractScenario.CONFIGURATION;
import static com.nereusstream.metadata.oxia.testing.BookKeeperMetadataStoreContractScenario.PROVIDER_SCOPE;
import static com.nereusstream.metadata.oxia.testing.BookKeeperMetadataStoreContractScenario.STREAM;
import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.assertThatThrownBy;

import com.nereusstream.api.StreamId;
import com.nereusstream.metadata.oxia.records.AllocationSlotLifecycle;
import com.nereusstream.metadata.oxia.records.BookKeeperAllocationSlotRecord;
import com.nereusstream.metadata.oxia.records.BookKeeperLedgerLifecycle;
import com.nereusstream.metadata.oxia.records.BookKeeperLedgerReaderLeaseRecord;
import com.nereusstream.metadata.oxia.records.BookKeeperLedgerRootRecord;
import com.nereusstream.metadata.oxia.records.BookKeeperWriterStateRecord;
import com.nereusstream.metadata.oxia.records.LedgerAllocationIntentRecord;
import com.nereusstream.metadata.oxia.records.LedgerAllocationLifecycle;
import com.nereusstream.metadata.oxia.testing.BookKeeperMetadataStoreContractScenario;
import java.time.Clock;
import java.time.Instant;
import java.time.ZoneOffset;
import java.util.LinkedHashMap;
import java.util.List;
import java.util.Map;
import java.util.Optional;
import org.junit.jupiter.api.Test;

class BookKeeperMetadataStoreContractTest {
    private static final BookKeeperMetadataStoreConfig CONFIG =
            new BookKeeperMetadataStoreConfig(128, 4, 128, 256);

    @Test
    void productionAndFakeAdaptersPassTheSameExactStoreScenario() {
        BookKeeperMetadataStoreContractScenario.Result productionResult;
        try (OxiaJavaBookKeeperMetadataStore production = productionStore()) {
            productionResult = BookKeeperMetadataStoreContractScenario.run(production, production, CONFIG);
        }
        BookKeeperMetadataStoreContractScenario.Result fakeResult;
        try (FakeBookKeeperMetadataStore fake = new FakeBookKeeperMetadataStore(CONFIG)) {
            fakeResult = BookKeeperMetadataStoreContractScenario.run(fake, fake, CONFIG);
        }
        assertThat(fakeResult).isEqualTo(productionResult);
    }

    @Test
    void paginatesFreshProcessStateAndRejectsContinuationScopeOrPageSizeDrift() {
        try (OxiaJavaBookKeeperMetadataStore first = productionStore()) {
            List<String> allocationIds = List.of("allocation-a", "allocation-b", "allocation-c");
            allocationIds.forEach(id -> first.createAllocation(CLUSTER, allocation(id)).join());

            BookKeeperScanPage<BookKeeperVersionedValue<LedgerAllocationIntentRecord>> page =
                    first.scanAllocations(CLUSTER, STREAM, Optional.empty(), 2).join();
            assertThat(page.values()).hasSize(2);
            BookKeeperScanToken token = page.continuation().orElseThrow();
            assertThat(first.scanAllocations(CLUSTER, STREAM, Optional.of(token), 2).join())
                    .satisfies(last -> {
                        assertThat(last.values()).hasSize(1);
                        assertThat(last.continuation()).isEmpty();
                    });

            assertThatThrownBy(() -> first.scanAllocations(CLUSTER, STREAM, Optional.of(token), 1))
                    .isInstanceOf(IllegalArgumentException.class)
                    .hasMessageContaining("another scan scope");
            assertThatThrownBy(() -> first.scanAllocations(
                    CLUSTER, new StreamId(STREAM.value() + "-other"), Optional.of(token), 2))
                    .isInstanceOf(IllegalArgumentException.class)
                    .hasMessageContaining("another scan scope");
            assertThatThrownBy(() -> first.scanAllocations(CLUSTER, STREAM, Optional.empty(), 1_025))
                    .isInstanceOf(IllegalArgumentException.class);
        }
    }

    @Test
    void scansEveryLedgerRootShardAndDoesNotLeakLedgerChildrenIntoRootPages() {
        try (OxiaJavaBookKeeperMetadataStore store = productionStore()) {
            BookKeeperKeyspace keys = CONFIG.keyspace(CLUSTER);
            Map<Integer, BookKeeperLedgerRootRecord> roots = oneRootPerShard(keys);
            roots.values().forEach(root -> store.createRoot(CLUSTER, root).join());

            BookKeeperLedgerRootRecord childOwner = roots.values().iterator().next();
            String identity = childOwner.ledgerIdentitySha256();
            store.createReaderLease(CLUSTER, PROVIDER_SCOPE, new BookKeeperLedgerReaderLeaseRecord(
                    1, identity, childOwner.ledgerId(), 2, 3, "process-1", 1, 120, 220, 0)).join();

            for (int shard = 0; shard < BookKeeperKeyspace.LEDGER_SHARDS; shard++) {
                BookKeeperScanPage<BookKeeperVersionedValue<BookKeeperLedgerRootRecord>> page =
                        store.scanRoots(CLUSTER, shard, Optional.empty(), 2).join();
                assertThat(page.values())
                        .singleElement()
                        .extracting(value -> value.value().ledgerIdentitySha256())
                        .isEqualTo(roots.get(shard).ledgerIdentitySha256());
                assertThat(page.continuation()).isEmpty();
            }
        }
    }

    @Test
    void scansEveryFixedAllocationSlotShardWithStableOrdering() {
        try (OxiaJavaBookKeeperMetadataStore store = productionStore()) {
            BookKeeperKeyspace keys = CONFIG.keyspace(CLUSTER);
            AllocationSlotLifecycle[] lifecycles = AllocationSlotLifecycle.values();
            for (int shard = 0; shard < BookKeeperKeyspace.ALLOCATION_SLOT_SHARDS; shard++) {
                for (int lifecycle = 0; lifecycle < lifecycles.length; lifecycle++) {
                    int slot = shard + lifecycle * BookKeeperKeyspace.ALLOCATION_SLOT_SHARDS;
                    long ledgerId = 1_000L + slot;
                    store.createAllocationSlot(CLUSTER, new BookKeeperAllocationSlotRecord(
                            1, slot, "allocation-slot-" + slot, STREAM.value(), ledgerId,
                            keys.ledgerIdentitySha256(PROVIDER_SCOPE, ledgerId), CONFIGURATION,
                            lifecycles[lifecycle], 100, 100 + lifecycle, 0)).join();
                }
            }

            for (int shard = 0; shard < BookKeeperKeyspace.ALLOCATION_SLOT_SHARDS; shard++) {
                BookKeeperScanPage<BookKeeperVersionedValue<BookKeeperAllocationSlotRecord>> first =
                        store.scanAllocationSlots(CLUSTER, shard, Optional.empty(), 2).join();
                BookKeeperScanPage<BookKeeperVersionedValue<BookKeeperAllocationSlotRecord>> second =
                        store.scanAllocationSlots(
                                CLUSTER, shard, first.continuation(), 2).join();
                assertThat(first.values()).hasSize(2);
                assertThat(second.values()).hasSize(1);
                assertThat(first.values().stream().map(value -> value.value().slot()).toList())
                        .containsExactly(shard, shard + BookKeeperKeyspace.ALLOCATION_SLOT_SHARDS);
                assertThat(second.values().get(0).value().slot())
                        .isEqualTo(shard + 2 * BookKeeperKeyspace.ALLOCATION_SLOT_SHARDS);
                assertThat(java.util.stream.Stream.concat(
                                first.values().stream(), second.values().stream())
                        .map(value -> value.value().lifecycle())
                        .toList())
                        .containsExactly(lifecycles);
                assertThat(second.continuation()).isEmpty();
            }
        }
    }

    @Test
    void transitionValidatorsRejectWireOrderedButProtocolIllegalSkips() {
        try (OxiaJavaBookKeeperMetadataStore store = productionStore()) {
            LedgerAllocationIntentRecord prepared = allocation("allocation-illegal");
            BookKeeperVersionedValue<LedgerAllocationIntentRecord> created =
                    store.createAllocation(CLUSTER, prepared).join();
            LedgerAllocationIntentRecord illegal = new LedgerAllocationIntentRecord(
                    prepared.schemaVersion(), prepared.allocationId(), prepared.streamId(), prepared.segmentSequence(),
                    prepared.clusterAlias(), prepared.candidateLedgerId(), prepared.allocationSlot(),
                    prepared.configurationBindingSha256(), prepared.writerId(), prepared.writerRunIdHash(),
                    prepared.appendSessionEpoch(), prepared.fencingTokenHash(), prepared.writerStateEpoch(),
                    LedgerAllocationLifecycle.ACTIVATED, false,
                    BookKeeperMetadataStoreContractScenario.CUSTOM_METADATA,
                    prepared.createdAtMillis(), 130, "", 0);
            assertThatThrownBy(() -> store.compareAndSetAllocation(
                    CLUSTER, illegal, created.metadataVersion()).join())
                    .hasRootCauseInstanceOf(IllegalArgumentException.class)
                    .rootCause()
                    .hasMessageContaining("illegal allocation lifecycle");
        }
    }

    @Test
    void appliedCreateCasAndDeleteConvergeAfterResponseLossAndFreshStoreConstruction() {
        ResponseLossPartitionedOxiaBackend backend = new ResponseLossPartitionedOxiaBackend();
        BookKeeperVersionedValue<BookKeeperWriterStateRecord> updated;
        try (OxiaJavaBookKeeperMetadataStore first = productionStore(backend)) {
            backend.loseNextResponse(ResponseLossPartitionedOxiaBackend.Operation.PUT_IF_ABSENT);
            BookKeeperVersionedValue<BookKeeperWriterStateRecord> created =
                    first.createWriter(CLUSTER,
                            BookKeeperMetadataStoreContractScenario.activeWriter(3, 3, 300, 2)).join();

            backend.loseNextResponse(ResponseLossPartitionedOxiaBackend.Operation.PUT_IF_VERSION);
            updated = first.compareAndSetWriter(CLUSTER,
                    BookKeeperMetadataStoreContractScenario.activeWriter(4, 5, 350, 3),
                    created.metadataVersion()).join();
            assertThat(updated.metadataVersion()).isGreaterThan(created.metadataVersion());

            LedgerAllocationIntentRecord allocation = allocation("allocation-response-loss");
            BookKeeperVersionedValue<LedgerAllocationIntentRecord> stored =
                    first.createAllocation(CLUSTER, allocation).join();
            backend.loseNextResponse(ResponseLossPartitionedOxiaBackend.Operation.DELETE_IF_VERSION);
            first.deleteAllocation(CLUSTER, STREAM, allocation.allocationId(), stored.metadataVersion()).join();
        }

        try (OxiaJavaBookKeeperMetadataStore restarted = productionStore(backend)) {
            assertThat(restarted.getWriter(CLUSTER, STREAM).join()).contains(updated);
            assertThat(restarted.getAllocation(
                    CLUSTER, STREAM, "allocation-response-loss").join()).isEmpty();
        }
    }

    private static LedgerAllocationIntentRecord allocation(String allocationId) {
        LedgerAllocationIntentRecord base = BookKeeperMetadataStoreContractScenario.allocation(
                LedgerAllocationLifecycle.PREPARED, false, "", 120);
        return new LedgerAllocationIntentRecord(base.schemaVersion(), allocationId, base.streamId(),
                base.segmentSequence(), base.clusterAlias(), base.candidateLedgerId(), base.allocationSlot(),
                base.configurationBindingSha256(), base.writerId(), base.writerRunIdHash(),
                base.appendSessionEpoch(), base.fencingTokenHash(), base.writerStateEpoch(), base.lifecycle(),
                base.lateCreateHazard(), base.bookKeeperMetadataSha256(), base.createdAtMillis(),
                base.updatedAtMillis(), base.stateReason(), 0);
    }

    private static Map<Integer, BookKeeperLedgerRootRecord> oneRootPerShard(BookKeeperKeyspace keys) {
        Map<Integer, BookKeeperLedgerRootRecord> result = new LinkedHashMap<>();
        for (long ledgerId = 1; ledgerId < 100_000 && result.size() < BookKeeperKeyspace.LEDGER_SHARDS; ledgerId++) {
            String identity = keys.ledgerIdentitySha256(PROVIDER_SCOPE, ledgerId);
            int shard = keys.ledgerShard(identity);
            result.putIfAbsent(shard, root(keys, ledgerId));
        }
        assertThat(result).hasSize(BookKeeperKeyspace.LEDGER_SHARDS);
        return result;
    }

    private static BookKeeperLedgerRootRecord root(BookKeeperKeyspace keys, long ledgerId) {
        BookKeeperLedgerRootRecord sample = BookKeeperMetadataStoreContractScenario.root(
                keys, ledgerId, BookKeeperLedgerLifecycle.ACTIVE, 2);
        return new BookKeeperLedgerRootRecord(sample.schemaVersion(), sample.ledgerIdentitySha256(),
                sample.clusterAlias(), sample.providerScopeSha256(), sample.ledgerId(), sample.streamId(),
                sample.segmentSequence(), "allocation-" + ledgerId, sample.allocationSlot(),
                sample.configurationBindingSha256(), sample.ledgerIdNamespaceSha256(), sample.lateCreateHazard(),
                sample.writerId(), sample.writerRunIdHash(), sample.appendSessionEpoch(), sample.fencingTokenHash(),
                sample.ensembleSize(), sample.writeQuorumSize(), sample.ackQuorumSize(), sample.digestType(),
                sample.customMetadataSha256(), sample.lifecycle(), sample.lifecycleEpoch(), sample.createdAtMillis(),
                sample.activatedAtMillis(), sample.sealStartedAtMillis(), sample.sealedAtMillis(),
                sample.sealedLastEntryId(), sample.sealedLength(), sample.sealReason(), sample.gcAttemptId(),
                sample.referenceSetSha256(), sample.markedAtMillis(), sample.deleteNotBeforeMillis(),
                sample.deleteStartedAtMillis(), sample.firstAbsentAtMillis(), sample.deletedAtMillis(),
                sample.stateReason(), 0);
    }

    private static OxiaJavaBookKeeperMetadataStore productionStore() {
        return productionStore(new InMemoryPartitionedOxiaBackend());
    }

    private static OxiaJavaBookKeeperMetadataStore productionStore(PartitionedOxiaClient.Backend backend) {
        return new OxiaJavaBookKeeperMetadataStore(
                new PartitionedOxiaClient(backend),
                Clock.fixed(Instant.ofEpochMilli(1_000), ZoneOffset.UTC), CONFIG);
    }
}
