/* Licensed under the Apache License, Version 2.0 */
package com.nereusstream.managedledger.cursor;

import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.assertThatThrownBy;

import java.util.ArrayList;
import java.util.List;
import java.util.Map;
import java.util.Optional;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.stream.IntStream;
import org.apache.bookkeeper.mledger.ManagedLedgerException;
import org.junit.jupiter.api.Test;

class CursorStorageConcurrencyTest {
    @Test
    void sameOwnerCasRacesUnionDisjointAndSharedPartialAcknowledgements() {
        try (CursorStorageTestSupport.Context context =
                new CursorStorageTestSupport.Context(0, 30)) {
            CursorOwnerSession owner = context.owner(CursorStorageTestSupport.OWNER_1);
            CursorHandle firstHandle = open(context.storage, owner, "subscription-a", 30);
            DefaultCursorStorage secondStorage = secondStorage(context);
            ExecutorService executor = Executors.newFixedThreadPool(2);
            try {
                CursorHandle secondHandle = open(secondStorage, owner, "subscription-a", 30);
                context.metadataStore.barrierNextCursorCas(2);

                CompletableFuture<CursorMutationResult> first = CompletableFuture.supplyAsync(
                        () -> context.storage.individualAck(
                                        firstHandle,
                                        List.of(whole(2)))
                                .join(),
                        executor);
                CompletableFuture<CursorMutationResult> second = CompletableFuture.supplyAsync(
                        () -> secondStorage.individualAck(
                                        secondHandle,
                                        List.of(whole(4)))
                                .join(),
                        executor);
                CompletableFuture.allOf(first, second).join();

                CursorState disjoint = context.storage
                        .claimAndLoadActiveCursors(owner)
                        .join()
                        .getFirst()
                        .state();
                assertThat(disjoint.acknowledgements().wholeAckRanges())
                        .containsExactly(new OffsetRange(2, 3), new OffsetRange(4, 5));

                context.metadataStore.barrierNextCursorCas(2);
                CompletableFuture<CursorMutationResult> leftPartial = CompletableFuture.supplyAsync(
                        () -> context.storage.individualAck(
                                        firstHandle,
                                        List.of(partial(6, 0xf0L)))
                                .join(),
                        executor);
                CompletableFuture<CursorMutationResult> rightPartial = CompletableFuture.supplyAsync(
                        () -> secondStorage.individualAck(
                                        secondHandle,
                                        List.of(partial(6, 0xccL)))
                                .join(),
                        executor);
                CompletableFuture.allOf(leftPartial, rightPartial).join();

                CursorState merged = context.storage
                        .claimAndLoadActiveCursors(owner)
                        .join()
                        .getFirst()
                        .state();
                assertThat(merged.acknowledgements().partialBatchAcks())
                        .containsEntry(6L, new BatchAckState(8, new long[] {0xc0L}));

                CursorOwnerSession successor = context.owner(CursorStorageTestSupport.OWNER_2);
                context.storage.claimAndLoadActiveCursors(successor).join();
                assertThatThrownBy(() -> secondStorage.individualAck(
                                secondHandle,
                                List.of(whole(8)))
                        .join())
                        .hasCauseInstanceOf(
                                ManagedLedgerException.ManagedLedgerFencedException.class);
                assertThatThrownBy(() -> secondStorage
                                .delete(owner, "subscription-a")
                                .join())
                        .hasCauseInstanceOf(
                                ManagedLedgerException.ManagedLedgerFencedException.class);
            } finally {
                executor.shutdownNow();
                secondStorage.close();
            }
        }
    }

    @Test
    void oneHundredSameNameCreatesAndRecreatesPublishOneGenerationEach() {
        try (CursorStorageTestSupport.Context context =
                new CursorStorageTestSupport.Context(0, 20)) {
            CursorOwnerSession owner = context.owner(CursorStorageTestSupport.OWNER_1);

            List<CursorHandle> firstGeneration = openManySameName(context, owner, 100);
            assertThat(firstGeneration)
                    .allSatisfy(handle -> assertThat(handle.identity().cursorGeneration())
                            .isEqualTo(1));
            assertThat(firstGeneration.stream().distinct()).hasSize(1);

            context.storage.delete(owner, "subscription-a").join();
            List<CursorHandle> secondGeneration = openManySameName(context, owner, 100);
            assertThat(secondGeneration)
                    .allSatisfy(handle -> assertThat(handle.identity().cursorGeneration())
                            .isEqualTo(2));
            assertThat(secondGeneration.stream().distinct()).hasSize(1);
            assertThat(firstGeneration.getFirst().isClosed()).isTrue();
            assertThat(context.activationCalls).hasValue(1);
        }
    }

    @Test
    void concurrentDistinctCreatesStopExactlyAtTheConfiguredRecordCap() {
        CursorStorageConfig config = CursorStorageTestSupport.configWithRecordCap(3);
        try (CursorStorageTestSupport.Context context =
                new CursorStorageTestSupport.Context(0, 20, config)) {
            CursorOwnerSession owner = context.owner(CursorStorageTestSupport.OWNER_1);
            List<CompletableFuture<OpenOutcome>> attempts = IntStream.range(0, 8)
                    .mapToObj(index -> context.storage
                            .open(
                                    owner,
                                    "subscription-" + index,
                                    new CursorOpenRequest(
                                            new InitialCursorPosition.Earliest(),
                                            Map.of(),
                                            Map.of(),
                                            0,
                                            20))
                            .handle((handle, error) -> new OpenOutcome(handle, unwrap(error))))
                    .toList();
            List<OpenOutcome> outcomes = attempts.stream()
                    .map(CompletableFuture::join)
                    .toList();

            assertThat(outcomes.stream().filter(OpenOutcome::succeeded)).hasSize(3);
            assertThat(outcomes.stream().filter(outcome -> !outcome.succeeded()))
                    .hasSize(5)
                    .allSatisfy(outcome -> assertThat(outcome.error())
                            .isInstanceOf(ManagedLedgerException.TooManyRequestsException.class));
            assertThat(context.storage.claimAndLoadActiveCursors(owner).join()).hasSize(3);
        }
    }

    private static List<CursorHandle> openManySameName(
            CursorStorageTestSupport.Context context,
            CursorOwnerSession owner,
            int count) {
        List<CompletableFuture<CursorHandle>> futures = new ArrayList<>();
        for (int index = 0; index < count; index++) {
            futures.add(context.storage.open(
                    owner,
                    "subscription-a",
                    new CursorOpenRequest(
                            new InitialCursorPosition.Earliest(),
                            Map.of(),
                            Map.of(),
                            0,
                            20)));
        }
        CompletableFuture.allOf(futures.toArray(CompletableFuture[]::new)).join();
        return futures.stream().map(CompletableFuture::join).toList();
    }

    private static CursorHandle open(
            CursorStorage storage,
            CursorOwnerSession owner,
            String name,
            long endOffset) {
        return storage.open(
                        owner,
                        name,
                        new CursorOpenRequest(
                                new InitialCursorPosition.Earliest(),
                                Map.of(),
                                Map.of(),
                                0,
                                endOffset))
                .join();
    }

    private static DefaultCursorStorage secondStorage(
            CursorStorageTestSupport.Context context) {
        return new DefaultCursorStorage(
                CursorStorageTestSupport.CLUSTER,
                context.streamStorage,
                context.projectionStore,
                context.metadataStore,
                context.snapshotStore,
                context.retention,
                ignored -> CompletableFuture.completedFuture(null),
                new CursorStateMachine(context.config),
                new CursorStatePersistencePlanner(
                        CursorStorageTestSupport.CLUSTER, context.config),
                context.config,
                CursorStorageTestSupport.CLOCK,
                context.scheduler);
    }

    private static CursorAckRequest whole(long offset) {
        return new CursorAckRequest(offset, Optional.empty(), Map.of());
    }

    private static CursorAckRequest partial(long offset, long remaining) {
        return new CursorAckRequest(
                offset,
                Optional.of(new BatchAckState(8, new long[] {remaining})),
                Map.of());
    }

    private static Throwable unwrap(Throwable error) {
        Throwable current = error;
        while ((current instanceof java.util.concurrent.CompletionException
                        || current instanceof java.util.concurrent.ExecutionException)
                && current.getCause() != null) {
            current = current.getCause();
        }
        return current;
    }

    private record OpenOutcome(CursorHandle handle, Throwable error) {
        boolean succeeded() {
            return error == null;
        }
    }
}
