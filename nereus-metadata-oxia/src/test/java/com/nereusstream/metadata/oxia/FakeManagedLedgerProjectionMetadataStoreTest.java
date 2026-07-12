/* Licensed under the Apache License, Version 2.0 */
package com.nereusstream.metadata.oxia;

import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.assertThatThrownBy;

import com.nereusstream.api.ErrorCode;
import com.nereusstream.api.NereusException;
import com.nereusstream.api.StorageProfile;
import com.nereusstream.api.StreamMetadata;
import com.nereusstream.api.StreamState;
import com.nereusstream.metadata.oxia.FakeManagedLedgerProjectionMetadataStore.DurableState;
import com.nereusstream.metadata.oxia.FakeManagedLedgerProjectionMetadataStore.FailurePoint;
import com.nereusstream.metadata.oxia.records.ManagedLedgerProjectionIdentity;
import com.nereusstream.metadata.oxia.records.TopicProjectionRecord;
import java.time.Clock;
import java.time.Duration;
import java.time.Instant;
import java.time.ZoneOffset;
import java.util.List;
import java.util.Map;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.CompletionException;
import java.util.concurrent.TimeUnit;
import java.util.stream.IntStream;
import org.junit.jupiter.api.Test;

class FakeManagedLedgerProjectionMetadataStoreTest {
    private static final String CLUSTER = "cluster/a";
    private static final String NAME = "tenant/ns/persistent/topic";
    private static final Clock CLOCK = Clock.fixed(Instant.ofEpochMilli(999), ZoneOffset.UTC);

    @Test
    void concurrentFirstCreatePublishesOneAuthorityAndLeaksOnlyLosingIds() {
        try (FakeManagedLedgerProjectionMetadataStore store = store(new DurableState())) {
            ProjectionCreateRequest request = request(NAME, 3, 1);
            List<CompletableFuture<TopicProjectionRecord>> futures = IntStream.range(0, 32)
                    .mapToObj(ignored -> store.createFirstProjection(CLUSTER, request))
                    .toList();

            CompletableFuture.allOf(futures.toArray(CompletableFuture[]::new)).join();
            List<TopicProjectionRecord> results = futures.stream().map(CompletableFuture::join).toList();
            TopicProjectionRecord winner = results.getFirst();

            assertThat(results).allSatisfy(result -> assertThat(result).isEqualTo(winner));
            assertThat(winner.virtualLedgerId()).isGreaterThanOrEqualTo(
                    ManagedLedgerProjectionNames.MIN_VIRTUAL_LEDGER_ID);
            assertThat(winner.createdAtMillis()).isEqualTo(100);
            assertThat(store.successfulWrites(FailurePoint.AFTER_ALLOCATOR_WRITE)).isEqualTo(32);
            assertThat(store.successfulWrites(FailurePoint.AFTER_TOPIC_WRITE)).isEqualTo(1);
            assertThat(store.successfulWrites(FailurePoint.AFTER_VIRTUAL_LEDGER_WRITE)).isEqualTo(1);
            assertThat(store.successfulWrites(FailurePoint.AFTER_POSITION_INDEX_WRITE)).isEqualTo(1);
        }
    }

    @Test
    void restartHydratesBackendVersionsAndFindsAlreadyValidDerivedRecords() {
        DurableState state = new DurableState();
        TopicProjectionRecord created;
        try (FakeManagedLedgerProjectionMetadataStore first = store(state)) {
            created = first.createFirstProjection(CLUSTER, request(NAME, 3, 1)).join();
        }

        try (FakeManagedLedgerProjectionMetadataStore restarted = store(state)) {
            TopicProjectionRecord recovered = restarted.getProjection(CLUSTER, NAME).join().orElseThrow();
            ProjectionRepairResult repair = restarted.repairProjectionIndexes(CLUSTER, recovered).join();

            assertThat(recovered).isEqualTo(created);
            assertThat(recovered.metadataVersion()).isPositive();
            assertThat(repair).isEqualTo(new ProjectionRepairResult(
                    ProjectionRepairStatus.ALREADY_VALID,
                    ProjectionRepairStatus.ALREADY_VALID));
        }
    }

    @Test
    void failuresAfterTopicAndEachDerivedWriteRepairIdempotently() {
        for (FailurePoint point : List.of(
                FailurePoint.AFTER_TOPIC_WRITE,
                FailurePoint.AFTER_VIRTUAL_LEDGER_WRITE,
                FailurePoint.AFTER_POSITION_INDEX_WRITE)) {
            DurableState state = new DurableState();
            try (FakeManagedLedgerProjectionMetadataStore failing = store(state)) {
                failing.failNext(point);
                assertNereusFailure(
                        () -> failing.createFirstProjection(CLUSTER, request(NAME, 3, 1)).join(),
                        ErrorCode.METADATA_UNAVAILABLE);
            }

            try (FakeManagedLedgerProjectionMetadataStore restarted = store(state)) {
                TopicProjectionRecord authority = restarted.getProjection(CLUSTER, NAME).join().orElseThrow();
                restarted.repairProjectionIndexes(CLUSTER, authority).join();
                assertThat(restarted.repairProjectionIndexes(CLUSTER, authority).join())
                        .isEqualTo(new ProjectionRepairResult(
                                ProjectionRepairStatus.ALREADY_VALID,
                                ProjectionRepairStatus.ALREADY_VALID));
            }
        }
    }

    @Test
    void allocatorFailureLeavesAGapWithoutPublishingAuthority() {
        DurableState state = new DurableState();
        try (FakeManagedLedgerProjectionMetadataStore store = store(state)) {
            store.failNext(FailurePoint.AFTER_ALLOCATOR_WRITE);
            assertNereusFailure(
                    () -> store.createFirstProjection(CLUSTER, request(NAME, 3, 1)).join(),
                    ErrorCode.METADATA_UNAVAILABLE);
            assertThat(store.getProjection(CLUSTER, NAME).join()).isEmpty();

            TopicProjectionRecord created = store.createFirstProjection(CLUSTER, request(NAME, 3, 1)).join();
            assertThat(created.virtualLedgerId())
                    .isEqualTo(ManagedLedgerProjectionNames.MIN_VIRTUAL_LEDGER_ID + 1);
        }
    }

    @Test
    void propertiesAndLifecycleUseIdentityGuardedSingleKeyCas() {
        try (FakeManagedLedgerProjectionMetadataStore store = store(new DurableState())) {
            TopicProjectionRecord created = store.createFirstProjection(CLUSTER, request(NAME, 3, 1)).join();
            TopicProjectionRecord properties = store.updateProperties(
                    CLUSTER, NAME, created.projectionIdentity(), created.metadataVersion(), Map.of("owner", "two"))
                    .join();
            TopicProjectionRecord sealed = store.mirrorFacadeState(
                    CLUSTER, NAME, properties.projectionIdentity(), properties.metadataVersion(),
                    ManagedLedgerFacadeState.SEALED).join();
            TopicProjectionRecord same = store.mirrorFacadeState(
                    CLUSTER, NAME, sealed.projectionIdentity(), sealed.metadataVersion(),
                    ManagedLedgerFacadeState.SEALED).join();

            assertThat(properties.properties()).containsExactly(Map.entry("owner", "two"));
            assertThat(properties.metadataVersion()).isGreaterThan(created.metadataVersion());
            assertThat(sealed.stateVersion()).isEqualTo(1);
            assertThat(same.metadataVersion()).isEqualTo(sealed.metadataVersion());
            assertNereusFailure(
                    () -> store.mirrorFacadeState(
                            CLUSTER, NAME, sealed.projectionIdentity(), sealed.metadataVersion(),
                            ManagedLedgerFacadeState.DELETED).join(),
                    ErrorCode.METADATA_INVARIANT_VIOLATION);

            ManagedLedgerProjectionIdentity stale = new ManagedLedgerProjectionIdentity(
                    created.storageClassBindingGeneration() + 1,
                    created.incarnation(),
                    created.streamId(),
                    created.virtualLedgerId());
            assertThatThrownBy(() -> store.updateProperties(
                            CLUSTER, NAME, stale, sealed.metadataVersion(), Map.of()).join())
                    .satisfies(error -> assertThat(rootCause(error))
                            .isInstanceOf(ManagedLedgerProjectionIdentityMismatchException.class));
        }
    }

    @Test
    void concurrentRecreationPublishesOneNewIncarnationAndLedger() {
        try (FakeManagedLedgerProjectionMetadataStore store = store(new DurableState())) {
            TopicProjectionRecord open = store.createFirstProjection(CLUSTER, request(NAME, 3, 1)).join();
            TopicProjectionRecord deleting = store.mirrorFacadeState(
                    CLUSTER, NAME, open.projectionIdentity(), open.metadataVersion(),
                    ManagedLedgerFacadeState.DELETING).join();
            TopicProjectionRecord deleted = store.mirrorFacadeState(
                    CLUSTER, NAME, deleting.projectionIdentity(), deleting.metadataVersion(),
                    ManagedLedgerFacadeState.DELETED).join();
            ProjectionCreateRequest next = request(NAME, 4, 2);
            List<CompletableFuture<TopicProjectionRecord>> futures = IntStream.range(0, 24)
                    .mapToObj(ignored -> store.recreateDeletedProjection(
                            CLUSTER, deleted.projectionIdentity(), deleted.metadataVersion(), next))
                    .toList();

            CompletableFuture.allOf(futures.toArray(CompletableFuture[]::new)).join();
            List<TopicProjectionRecord> results = futures.stream().map(CompletableFuture::join).toList();
            TopicProjectionRecord winner = results.getFirst();

            assertThat(results).allSatisfy(value -> assertThat(value).isEqualTo(winner));
            assertThat(winner.incarnation()).isEqualTo(2);
            assertThat(winner.storageClassBindingGeneration()).isEqualTo(4);
            assertThat(winner.streamId()).isNotEqualTo(deleted.streamId());
            assertThat(winner.virtualLedgerId()).isNotEqualTo(deleted.virtualLedgerId());
            assertThat(winner.stateVersion()).isZero();
        }
    }

    @Test
    void exactNameValidationRejectsInjectedTopicHashCollision() {
        DurableState state = new DurableState();
        String otherName = "tenant/ns/persistent/other";
        try (FakeManagedLedgerProjectionMetadataStore store = store(state)) {
            store.createFirstProjection(CLUSTER, request(otherName, 3, 1)).join();
        }
        ManagedLedgerProjectionKeyspace keyspace = new ManagedLedgerProjectionKeyspace(CLUSTER);
        String otherKey = keyspace.topicProjectionKey(otherName);
        FakeManagedLedgerProjectionMetadataStore.StoredValue other =
                state.storedValue(otherKey).orElseThrow();
        state.inject(
                keyspace.topicProjectionKey(NAME),
                keyspace.topicProjectionPartitionKey(NAME).value(),
                other.envelope());

        try (FakeManagedLedgerProjectionMetadataStore restarted = store(state)) {
            assertNereusFailure(
                    () -> restarted.getProjection(CLUSTER, NAME).join(),
                    ErrorCode.METADATA_INVARIANT_VIOLATION);
        }
    }

    @Test
    void repairRequiresCurrentTopicAuthorityAndRejectsConflictingDerivedIdentity() {
        DurableState source = new DurableState();
        TopicProjectionRecord authority;
        try (FakeManagedLedgerProjectionMetadataStore store = store(source)) {
            authority = store.createFirstProjection(CLUSTER, request(NAME, 3, 1)).join();
        }
        try (FakeManagedLedgerProjectionMetadataStore empty = store(new DurableState())) {
            assertNereusFailure(
                    () -> empty.repairProjectionIndexes(CLUSTER, authority).join(),
                    ErrorCode.METADATA_INVARIANT_VIOLATION);
            assertThat(empty.successfulWrites(FailurePoint.AFTER_VIRTUAL_LEDGER_WRITE)).isZero();
        }

        String otherName = "tenant/ns/persistent/other-derived";
        try (FakeManagedLedgerProjectionMetadataStore store = store(source)) {
            TopicProjectionRecord other = store.createFirstProjection(CLUSTER, request(otherName, 4, 1)).join();
            ManagedLedgerProjectionKeyspace keyspace = new ManagedLedgerProjectionKeyspace(CLUSTER);
            String otherKey = keyspace.positionIndexKey(new com.nereusstream.api.StreamId(other.streamId()));
            FakeManagedLedgerProjectionMetadataStore.StoredValue otherPosition =
                    source.storedValue(otherKey).orElseThrow();
            source.inject(
                    keyspace.positionIndexKey(new com.nereusstream.api.StreamId(authority.streamId())),
                    keyspace.streamPartitionKey(new com.nereusstream.api.StreamId(authority.streamId())).value(),
                    otherPosition.envelope());

            assertNereusFailure(
                    () -> store.repairProjectionIndexes(CLUSTER, authority).join(),
                    ErrorCode.METADATA_INVARIANT_VIOLATION);
        }
    }

    @Test
    void pendingOperationBoundRejectsBeforeASecondBackendCall() throws Exception {
        DurableState state = new DurableState();
        ProjectionMetadataStoreConfig config = new ProjectionMetadataStoreConfig(
                Duration.ofSeconds(5), 1, ProjectionMetadataStoreConfig.F2_MAX_VALUE_BYTES);
        try (FakeManagedLedgerProjectionMetadataStore store =
                     new FakeManagedLedgerProjectionMetadataStore(state, config, CLOCK)) {
            state.blockBackendOperations();
            CompletableFuture<?> blocked = store.getProjection(CLUSTER, NAME);
            awaitBackendCalls(state, 1);
            long calls = state.backendCalls();

            assertNereusFailure(
                    () -> store.getProjection(CLUSTER, NAME).join(),
                    ErrorCode.BACKPRESSURE_REJECTED);
            assertThat(state.backendCalls()).isEqualTo(calls);

            state.releaseBackendOperations();
            assertThat(blocked.get(5, TimeUnit.SECONDS)).isEqualTo(java.util.Optional.empty());
        } finally {
            state.releaseBackendOperations();
        }
    }

    @Test
    void oneMonotonicDeadlineCoversTheBlockedBackendFuture() {
        DurableState state = new DurableState();
        ProjectionMetadataStoreConfig config = new ProjectionMetadataStoreConfig(
                Duration.ofMillis(30), 1, ProjectionMetadataStoreConfig.F2_MAX_VALUE_BYTES);
        try (FakeManagedLedgerProjectionMetadataStore store =
                     new FakeManagedLedgerProjectionMetadataStore(state, config, CLOCK)) {
            state.blockBackendOperations();
            assertNereusFailure(() -> store.getProjection(CLUSTER, NAME).join(), ErrorCode.TIMEOUT);
            assertThat(state.backendCalls()).isEqualTo(1);
        } finally {
            state.releaseBackendOperations();
        }
    }

    @Test
    void closeIsIdempotentAndRejectsFurtherCalls() {
        FakeManagedLedgerProjectionMetadataStore store = store(new DurableState());
        store.close();
        store.close();

        assertNereusFailure(() -> store.getProjection(CLUSTER, NAME).join(), ErrorCode.STORAGE_CLOSED);
    }

    private static FakeManagedLedgerProjectionMetadataStore store(DurableState state) {
        return new FakeManagedLedgerProjectionMetadataStore(
                state, ProjectionMetadataStoreConfig.defaults(), CLOCK);
    }

    private static ProjectionCreateRequest request(String name, long binding, long incarnation) {
        return new ProjectionCreateRequest(
                name,
                binding,
                incarnation,
                new StreamMetadata(
                        ManagedLedgerProjectionNames.streamId(name, incarnation),
                        ManagedLedgerProjectionNames.streamName(name, incarnation),
                        StreamState.ACTIVE,
                        StorageProfile.OBJECT_WAL_SYNC_OBJECT,
                        Map.of(ManagedLedgerProjectionNames.PAYLOAD_MAPPING_ATTRIBUTE,
                                ManagedLedgerProjectionNames.PAYLOAD_MAPPING_V1),
                        100,
                        7,
                        0,
                        0,
                        0),
                Map.of("owner", "one"));
    }

    private static void assertNereusFailure(Runnable operation, ErrorCode code) {
        assertThatThrownBy(operation::run).satisfies(error -> assertThat(rootCause(error))
                .isInstanceOfSatisfying(NereusException.class, nereus -> {
                    assertThat(nereus.code()).isEqualTo(code);
                }));
    }

    private static Throwable rootCause(Throwable error) {
        Throwable current = error;
        while ((current instanceof CompletionException || current instanceof java.util.concurrent.ExecutionException)
                && current.getCause() != null) {
            current = current.getCause();
        }
        return current;
    }

    private static void awaitBackendCalls(DurableState state, long expected) throws InterruptedException {
        long deadline = System.nanoTime() + TimeUnit.SECONDS.toNanos(5);
        while (state.backendCalls() < expected && System.nanoTime() < deadline) {
            Thread.sleep(1);
        }
        assertThat(state.backendCalls()).isGreaterThanOrEqualTo(expected);
    }
}
