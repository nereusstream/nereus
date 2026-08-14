/*
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 *   https://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

package com.nereusstream.kafka.bookkeeper.read;

import com.nereusstream.kafka.bookkeeper.adapter.KafkaNativeAssignedRecordBatchV1;
import com.nereusstream.kafka.bookkeeper.adapter.KafkaRawAssignedRecordBatchFactsV1;
import com.nereusstream.kafka.bookkeeper.commit.KafkaTransactionStateV1.CompletedTransactionV1;
import com.nereusstream.kafka.bookkeeper.nbke2.Nbke2CodecV1;
import com.nereusstream.kafka.bookkeeper.nbke2.Nbke2DataV1;
import com.nereusstream.kafka.bookkeeper.nbke2.Nbke2FrameV1;
import com.nereusstream.kafka.bookkeeper.nbke2.Nbke2RangeIndexBlockV1;
import com.nereusstream.kafka.bookkeeper.protocol.KafkaReadIsolationV1;
import com.nereusstream.storage.api.bookkeeper.BookKeeperCellSession;
import com.nereusstream.storage.api.bookkeeper.ExactLedgerEntryV1;
import com.nereusstream.storage.api.bookkeeper.RunLedgerHandleV1;
import com.nereusstream.storage.api.bookkeeper.RunLedgerReadOutcomeV1;
import com.nereusstream.storage.api.bookkeeper.RunLedgerReadResultV1;
import java.util.ArrayList;
import java.util.LinkedHashMap;
import java.util.List;
import java.util.Map;
import java.util.Objects;
import java.util.Optional;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.CompletionStage;

/** K6 exact-entry reader with bounded validated index-block cache and disposable sequential cursors. */
public final class KafkaBookKeeperTargetedReaderV1 {
    private final BookKeeperCellSession session;
    private final int maximumCachedIndexBlocks;
    private final Map<IndexCacheKey, KafkaPackedBatchLocatorIndexV1> indexCache;

    public KafkaBookKeeperTargetedReaderV1(BookKeeperCellSession session, int maximumCachedIndexBlocks) {
        this.session = Objects.requireNonNull(session, "session");
        if (maximumCachedIndexBlocks <= 0) {
            throw new IllegalArgumentException("index-block cache bound must be positive");
        }
        this.maximumCachedIndexBlocks = maximumCachedIndexBlocks;
        this.indexCache = new LinkedHashMap<>(16, 0.75f, true);
    }

    public CompletionStage<KafkaBookKeeperReadResultV1> readOne(
            KafkaBookKeeperReadSnapshotV1 snapshot, long requestedOffset, KafkaReadIsolationV1 isolation) {
        Objects.requireNonNull(snapshot, "snapshot");
        Objects.requireNonNull(isolation, "isolation");
        Optional<KafkaBookKeeperReadResultV1> boundFailure = initialBoundResult(snapshot, requestedOffset, isolation);
        if (boundFailure.isPresent()) {
            return completed(boundFailure.orElseThrow());
        }
        long upperBound = snapshot.root().readUpperBound(isolation);
        return locate(snapshot, requestedOffset, lookupStepCap(snapshot)).thenCompose(plan -> {
            if (plan.failure().isPresent()) {
                return completed(emptyResult(plan.failure().orElseThrow(), false));
            }
            Optional<LocatedBatch> candidate = plan.located();
            if (candidate.isEmpty() || !withinUpperBound(candidate.orElseThrow().locator(), upperBound)) {
                return completed(
                        emptyResult(Failure.end("no complete locator remains below the captured upper bound"), false));
            }
            LocatedBatch located = candidate.orElseThrow();
            return readData(located)
                    .thenApply(data -> data.failure().isPresent()
                            ? emptyResult(data.failure().orElseThrow(), false)
                            : foundResult(
                                    snapshot,
                                    isolation,
                                    requestedOffset,
                                    List.of(data.batch().orElseThrow()),
                                    Optional.empty(),
                                    false));
        });
    }

    public CompletionStage<KafkaBookKeeperReadResultV1> readSequential(
            KafkaBookKeeperReadSnapshotV1 snapshot, KafkaBookKeeperSequentialReadRequestV1 request) {
        Objects.requireNonNull(snapshot, "snapshot");
        Objects.requireNonNull(request, "request");
        Optional<KafkaBookKeeperReadResultV1> boundFailure =
                initialBoundResult(snapshot, request.requestedOffset(), request.isolation());
        if (boundFailure.isPresent()) {
            return completed(boundFailure.orElseThrow());
        }
        return locateInitial(snapshot, request).thenCompose(initial -> {
            if (initial.plan().failure().isPresent()) {
                return completed(emptyResult(initial.plan().failure().orElseThrow(), initial.cursorAccepted()));
            }
            Optional<LocatedBatch> candidate = initial.plan().located();
            long upperBound = snapshot.root().readUpperBound(request.isolation());
            if (candidate.isEmpty() || !withinUpperBound(candidate.orElseThrow().locator(), upperBound)) {
                return completed(emptyResult(
                        Failure.end("no complete locator remains below the captured upper bound"),
                        initial.cursorAccepted()));
            }
            return collectSequential(
                    snapshot,
                    request,
                    candidate.orElseThrow(),
                    initial.cursorAccepted(),
                    new ArrayList<>(),
                    0L,
                    sequentialBatchCap(snapshot));
        });
    }

    public synchronized int cachedIndexBlockCount() {
        return indexCache.size();
    }

    private CompletionStage<KafkaBookKeeperReadResultV1> collectSequential(
            KafkaBookKeeperReadSnapshotV1 snapshot,
            KafkaBookKeeperSequentialReadRequestV1 request,
            LocatedBatch current,
            boolean cursorAccepted,
            List<KafkaBookKeeperReadBatchV1> batches,
            long accumulatedBytes,
            long remainingBatches) {
        if (remainingBatches <= 0) {
            return completed(emptyResult(
                    Failure.corrupt("sequential locator traversal exceeded its snapshot bound"), cursorAccepted));
        }
        return readData(current).thenCompose(data -> {
            if (data.failure().isPresent()) {
                return completed(emptyResult(data.failure().orElseThrow(), cursorAccepted));
            }
            KafkaBookKeeperReadBatchV1 batch = data.batch().orElseThrow();
            batches.add(batch);
            long nextAccumulated = Math.addExact(
                    accumulatedBytes, batch.rawAssignedRecordBatch().length());
            return successor(snapshot, current, lookupStepCap(snapshot)).thenCompose(next -> {
                if (next.failure().isPresent()) {
                    return completed(emptyResult(next.failure().orElseThrow(), cursorAccepted));
                }
                Optional<LocatedBatch> nextLocated = next.located();
                long upperBound = snapshot.root().readUpperBound(request.isolation());
                if (nextLocated.isEmpty()
                        || !withinUpperBound(nextLocated.orElseThrow().locator(), upperBound)) {
                    return completed(foundResult(
                            snapshot,
                            request.isolation(),
                            request.requestedOffset(),
                            batches,
                            Optional.empty(),
                            cursorAccepted));
                }
                LocatedBatch nextBatch = nextLocated.orElseThrow();
                if (nextAccumulated > request.byteBudget() - nextBatch.locator().rawPayloadBytes()) {
                    return completed(foundResult(
                            snapshot,
                            request.isolation(),
                            request.requestedOffset(),
                            batches,
                            Optional.of(cursor(snapshot, nextBatch)),
                            cursorAccepted));
                }
                return collectSequential(
                        snapshot, request, nextBatch, cursorAccepted, batches, nextAccumulated, remainingBatches - 1);
            });
        });
    }

    private CompletionStage<InitialPlan> locateInitial(
            KafkaBookKeeperReadSnapshotV1 snapshot, KafkaBookKeeperSequentialReadRequestV1 request) {
        Optional<KafkaBookKeeperReadCursorV1> supplied = request.cursor();
        if (supplied.isEmpty() || !cursorRootMatches(snapshot, request, supplied.orElseThrow())) {
            return locate(snapshot, request.requestedOffset(), lookupStepCap(snapshot))
                    .thenApply(plan -> new InitialPlan(plan, false));
        }
        KafkaBookKeeperReadCursorV1 cursor = supplied.orElseThrow();
        Optional<KafkaBookKeeperReadRunV1> run =
                snapshot.runTable().find(cursor.runIdentity().runId());
        if (run.isEmpty()
                || !run.orElseThrow().runBinding().equals(cursor.runIdentity())
                || run.orElseThrow().sourceGeneration() != cursor.sourceGeneration()) {
            return locate(snapshot, request.requestedOffset(), lookupStepCap(snapshot))
                    .thenApply(plan -> new InitialPlan(plan, false));
        }
        KafkaBookKeeperReadRunV1 matchedRun = run.orElseThrow();
        if (matchedRun.active()) {
            if (cursor.indexBlockIdentity() != -1) {
                return locate(snapshot, request.requestedOffset(), lookupStepCap(snapshot))
                        .thenApply(plan -> new InitialPlan(plan, false));
            }
            Optional<LocatedBatch> located = cursorLocator(
                    matchedRun, Optional.empty(), matchedRun.activeIndex().orElseThrow(), cursor);
            if (located.isPresent()) {
                return completed(new InitialPlan(Plan.located(located.orElseThrow()), true));
            }
            return locate(snapshot, request.requestedOffset(), lookupStepCap(snapshot))
                    .thenApply(plan -> new InitialPlan(plan, false));
        }
        Optional<KafkaIndexBlockPointerV1> pointer =
                matchedRun.sealedDirectory().orElseThrow().findByEntryId(cursor.indexBlockIdentity());
        if (pointer.isEmpty()) {
            return locate(snapshot, request.requestedOffset(), lookupStepCap(snapshot))
                    .thenApply(plan -> new InitialPlan(plan, false));
        }
        return loadIndexBlock(matchedRun, pointer.orElseThrow()).thenCompose(loaded -> {
            if (loaded.failure().isPresent()) {
                return completed(new InitialPlan(Plan.failed(loaded.failure().orElseThrow()), true));
            }
            Optional<LocatedBatch> located =
                    cursorLocator(matchedRun, pointer, loaded.index().orElseThrow(), cursor);
            if (located.isPresent()) {
                return completed(new InitialPlan(Plan.located(located.orElseThrow()), true));
            }
            return locate(snapshot, request.requestedOffset(), lookupStepCap(snapshot))
                    .thenApply(plan -> new InitialPlan(plan, false));
        });
    }

    private Optional<LocatedBatch> cursorLocator(
            KafkaBookKeeperReadRunV1 run,
            Optional<KafkaIndexBlockPointerV1> pointer,
            KafkaPackedBatchLocatorIndexV1 index,
            KafkaBookKeeperReadCursorV1 cursor) {
        try {
            KafkaPackedBatchLocatorV1 locator = index.at(cursor.locatorOrdinal());
            if (locator.indexIdentity() != cursor.indexBlockIdentity()
                    || locator.entryId() != cursor.nextEntryId()
                    || locator.startOffset() != cursor.nextKafkaOffset()) {
                return Optional.empty();
            }
            return Optional.of(new LocatedBatch(run, pointer, index, locator));
        } catch (IllegalArgumentException invalid) {
            return Optional.empty();
        }
    }

    private boolean cursorRootMatches(
            KafkaBookKeeperReadSnapshotV1 snapshot,
            KafkaBookKeeperSequentialReadRequestV1 request,
            KafkaBookKeeperReadCursorV1 cursor) {
        return cursor.capturedFence().equals(snapshot.root().fence())
                && cursor.snapshotStateVersion() == snapshot.root().stateVersion()
                && cursor.nextKafkaOffset() == request.requestedOffset();
    }

    private CompletionStage<Plan> locate(
            KafkaBookKeeperReadSnapshotV1 snapshot, long requestedOffset, int remainingSteps) {
        if (remainingSteps <= 0) {
            return completed(Plan.failed(Failure.corrupt("locator traversal exceeded its snapshot bound")));
        }
        Optional<KafkaBookKeeperReadRunV1> run = snapshot.runTable().floorOrSuccessor(requestedOffset);
        return run.map(value -> locateInRun(
                        snapshot, value, Math.max(requestedOffset, value.startOffset()), remainingSteps - 1))
                .orElseGet(() -> completed(Plan.empty()));
    }

    private CompletionStage<Plan> locateInRun(
            KafkaBookKeeperReadSnapshotV1 snapshot,
            KafkaBookKeeperReadRunV1 run,
            long requestedOffset,
            int remainingSteps) {
        if (!run.handle().providerScopeId().equals(session.providerScopeId())) {
            return completed(Plan.failed(Failure.provider("read run belongs to another provider scope")));
        }
        if (run.active()) {
            Optional<KafkaPackedBatchLocatorV1> locator =
                    run.activeIndex().orElseThrow().floorOrSuccessor(requestedOffset);
            if (locator.isPresent()) {
                return completed(Plan.located(new LocatedBatch(
                        run, Optional.empty(), run.activeIndex().orElseThrow(), locator.orElseThrow())));
            }
            return locateSuccessorRun(snapshot, run, requestedOffset, remainingSteps);
        }
        Optional<KafkaIndexBlockPointerV1> pointer =
                run.sealedDirectory().orElseThrow().floorOrSuccessor(requestedOffset);
        if (pointer.isEmpty()) {
            return locateSuccessorRun(snapshot, run, requestedOffset, remainingSteps);
        }
        return locateInBlock(snapshot, run, pointer.orElseThrow(), requestedOffset, remainingSteps);
    }

    private CompletionStage<Plan> locateInBlock(
            KafkaBookKeeperReadSnapshotV1 snapshot,
            KafkaBookKeeperReadRunV1 run,
            KafkaIndexBlockPointerV1 pointer,
            long requestedOffset,
            int remainingSteps) {
        if (remainingSteps <= 0) {
            return completed(Plan.failed(Failure.corrupt("range-index traversal exceeded its snapshot bound")));
        }
        return loadIndexBlock(run, pointer).thenCompose(loaded -> {
            if (loaded.failure().isPresent()) {
                return completed(Plan.failed(loaded.failure().orElseThrow()));
            }
            KafkaPackedBatchLocatorIndexV1 index = loaded.index().orElseThrow();
            Optional<KafkaPackedBatchLocatorV1> locator = index.floorOrSuccessor(requestedOffset);
            if (locator.isPresent()) {
                return completed(
                        Plan.located(new LocatedBatch(run, Optional.of(pointer), index, locator.orElseThrow())));
            }
            Optional<KafkaIndexBlockPointerV1> successor =
                    run.sealedDirectory().orElseThrow().successor(pointer.ordinal());
            if (successor.isPresent()) {
                return locateInBlock(snapshot, run, successor.orElseThrow(), requestedOffset, remainingSteps - 1);
            }
            return locateSuccessorRun(snapshot, run, requestedOffset, remainingSteps - 1);
        });
    }

    private CompletionStage<Plan> successor(
            KafkaBookKeeperReadSnapshotV1 snapshot, LocatedBatch current, int remainingSteps) {
        if (remainingSteps <= 0) {
            return completed(Plan.failed(Failure.corrupt("successor traversal exceeded its snapshot bound")));
        }
        Optional<KafkaPackedBatchLocatorV1> local =
                current.index().successor(current.locator().ordinal());
        if (local.isPresent()) {
            return completed(Plan.located(
                    new LocatedBatch(current.run(), current.blockPointer(), current.index(), local.orElseThrow())));
        }
        if (current.run().active()) {
            return locateSuccessorRun(snapshot, current.run(), current.locator().endOffsetExclusive(), remainingSteps);
        }
        KafkaIndexBlockPointerV1 pointer = current.blockPointer().orElseThrow();
        Optional<KafkaIndexBlockPointerV1> nextBlock =
                current.run().sealedDirectory().orElseThrow().successor(pointer.ordinal());
        if (nextBlock.isPresent()) {
            return locateInBlock(
                    snapshot,
                    current.run(),
                    nextBlock.orElseThrow(),
                    current.locator().endOffsetExclusive(),
                    remainingSteps - 1);
        }
        return locateSuccessorRun(snapshot, current.run(), current.locator().endOffsetExclusive(), remainingSteps - 1);
    }

    private CompletionStage<Plan> locateSuccessorRun(
            KafkaBookKeeperReadSnapshotV1 snapshot,
            KafkaBookKeeperReadRunV1 current,
            long requestedOffset,
            int remainingSteps) {
        if (remainingSteps <= 0) {
            return completed(Plan.failed(Failure.corrupt("run traversal exceeded its snapshot bound")));
        }
        Optional<KafkaBookKeeperReadRunV1> successor = snapshot.runTable().successor(current);
        return successor
                .map(run ->
                        locateInRun(snapshot, run, Math.max(requestedOffset, run.startOffset()), remainingSteps - 1))
                .orElseGet(() -> completed(Plan.empty()));
    }

    private CompletionStage<LoadedIndex> loadIndexBlock(
            KafkaBookKeeperReadRunV1 run, KafkaIndexBlockPointerV1 pointer) {
        IndexCacheKey key = new IndexCacheKey(run.handle(), run.sourceGeneration(), pointer.indexBlockEntryId());
        KafkaPackedBatchLocatorIndexV1 cached;
        synchronized (this) {
            cached = indexCache.get(key);
        }
        if (cached != null) {
            return completed(LoadedIndex.loaded(cached));
        }
        return readExact(run.handle(), pointer.indexBlockEntryId()).thenApply(read -> {
            if (read.failure().isPresent()) {
                return LoadedIndex.failed(read.failure().orElseThrow());
            }
            try {
                ExactLedgerEntryV1 entry = read.entry().orElseThrow();
                Nbke2FrameV1 decoded = Nbke2CodecV1.decode(
                        entry.payload().toByteArray(),
                        run.handle().ledgerIdentity().ledgerId(),
                        pointer.indexBlockEntryId());
                if (!(decoded instanceof Nbke2RangeIndexBlockV1 block)
                        || !block.runBinding().equals(run.runBinding())
                        || block.anchorOffset() != pointer.startOffset()
                        || block.coveredThroughOffset() != pointer.coveredThroughOffset()) {
                    return LoadedIndex.failed(
                            Failure.corrupt("range-index frame differs from its run/directory identity"));
                }
                KafkaPackedBatchLocatorIndexV1 index =
                        KafkaPackedBatchLocatorIndexV1.fromRangeIndexBlock(pointer.indexBlockEntryId(), block);
                synchronized (this) {
                    indexCache.put(key, index);
                    while (indexCache.size() > maximumCachedIndexBlocks) {
                        IndexCacheKey eldest = indexCache.keySet().iterator().next();
                        indexCache.remove(eldest);
                    }
                }
                return LoadedIndex.loaded(index);
            } catch (RuntimeException corruption) {
                return LoadedIndex.failed(Failure.corrupt("range-index frame failed entry-local validation"));
            }
        });
    }

    private CompletionStage<ReadData> readData(LocatedBatch located) {
        KafkaPackedBatchLocatorV1 locator = located.locator();
        return readExact(located.run().handle(), locator.entryId()).thenApply(read -> {
            if (read.failure().isPresent()) {
                return ReadData.failed(read.failure().orElseThrow());
            }
            try {
                ExactLedgerEntryV1 entry = read.entry().orElseThrow();
                Nbke2FrameV1 decoded = Nbke2CodecV1.decode(
                        entry.payload().toByteArray(),
                        located.run().handle().ledgerIdentity().ledgerId(),
                        locator.entryId());
                if (!(decoded instanceof Nbke2DataV1 data)
                        || !data.runBinding().equals(located.run().runBinding())
                        || data.baseOffset() != locator.startOffset()
                        || data.endOffsetExclusive() != locator.endOffsetExclusive()
                        || data.rawAssignedRecordBatch().length() != locator.rawPayloadBytes()) {
                    return ReadData.failed(Failure.corrupt("DATA frame differs from its selected locator"));
                }
                KafkaNativeAssignedRecordBatchV1 batch = KafkaNativeAssignedRecordBatchV1.validate(
                        KafkaRawAssignedRecordBatchFactsV1.parse(data.rawAssignedRecordBatch()));
                if (batch.baseOffset() != locator.startOffset()
                        || batch.endOffsetExclusive() != locator.endOffsetExclusive()
                        || batch.partitionLeaderEpoch()
                                != located.run().runBinding().kafkaLeaderEpoch()) {
                    return ReadData.failed(
                            Failure.corrupt("raw Kafka RecordBatch differs from its locator/run leader epoch"));
                }
                return ReadData.read(new KafkaBookKeeperReadBatchV1(
                        batch.baseOffset(),
                        batch.endOffsetExclusive(),
                        locator.entryId(),
                        batch.rawAssignedRecordBatch()));
            } catch (RuntimeException corruption) {
                return ReadData.failed(Failure.corrupt("DATA frame failed NBKE2 or raw Kafka header/CRC validation"));
            }
        });
    }

    private CompletionStage<EntryRead> readExact(RunLedgerHandleV1 handle, long entryId) {
        CompletionStage<RunLedgerReadResultV1> accepted;
        try {
            accepted = Objects.requireNonNull(
                    session.readExactEntry(handle, entryId), "provider returned a null read stage");
        } catch (RuntimeException failure) {
            return completed(EntryRead.failed(Failure.provider("provider rejected the exact-entry read")));
        }
        return accepted.handle((result, failure) -> {
            if (failure != null || result == null) {
                return EntryRead.failed(Failure.provider("provider exact-entry read completed exceptionally"));
            }
            if (result.outcome() == RunLedgerReadOutcomeV1.DEFINITIVELY_ABSENT) {
                return EntryRead.failed(Failure.absent("selected BookKeeper entry is definitively absent"));
            }
            if (result.outcome() == RunLedgerReadOutcomeV1.FENCED) {
                return EntryRead.failed(Failure.fenced("BookKeeper read was fenced"));
            }
            if (result.outcome() == RunLedgerReadOutcomeV1.PROVIDER_FAILURE) {
                return EntryRead.failed(Failure.provider("BookKeeper provider read failed"));
            }
            ExactLedgerEntryV1 entry = result.exactEntry().orElseThrow();
            if (!entry.handle().equals(handle) || entry.entryId() != entryId) {
                return EntryRead.failed(Failure.corrupt("provider exact entry differs from its request identity"));
            }
            return EntryRead.read(entry);
        });
    }

    private Optional<KafkaBookKeeperReadResultV1> initialBoundResult(
            KafkaBookKeeperReadSnapshotV1 snapshot, long requestedOffset, KafkaReadIsolationV1 isolation) {
        if (requestedOffset < 0) {
            throw new IllegalArgumentException("requested offset must be non-negative");
        }
        long trimStart = snapshot.root().frontiers().trimStartOffset();
        long readableEnd = snapshot.root().frontiers().readableEndOffset();
        if (requestedOffset < trimStart || requestedOffset > readableEnd) {
            return Optional.of(
                    emptyResult(Failure.outOfRange("requested offset is outside captured Log Start/LEO"), false));
        }
        if (requestedOffset >= snapshot.root().readUpperBound(isolation)) {
            return Optional.of(emptyResult(
                    Failure.end("requested offset has not passed the captured isolation upper bound"), false));
        }
        return Optional.empty();
    }

    private KafkaBookKeeperReadResultV1 foundResult(
            KafkaBookKeeperReadSnapshotV1 snapshot,
            KafkaReadIsolationV1 isolation,
            long requestedOffset,
            List<KafkaBookKeeperReadBatchV1> batches,
            Optional<KafkaBookKeeperReadCursorV1> cursor,
            boolean cursorAccepted) {
        long returnedEnd = batches.get(batches.size() - 1).endOffsetExclusive();
        List<CompletedTransactionV1> aborted = isolation == KafkaReadIsolationV1.READ_COMMITTED
                ? snapshot.transactionState().abortedTransactions().stream()
                        .filter(transaction -> transaction.markerEndOffsetExclusive() > requestedOffset
                                && transaction.firstOffset() < returnedEnd)
                        .toList()
                : List.of();
        return new KafkaBookKeeperReadResultV1(
                KafkaBookKeeperReadOutcomeV1.FOUND,
                batches,
                aborted,
                cursor,
                cursorAccepted,
                "validated complete RecordBatch bytes");
    }

    private static KafkaBookKeeperReadResultV1 emptyResult(Failure failure, boolean cursorAccepted) {
        return new KafkaBookKeeperReadResultV1(
                failure.outcome(), List.of(), List.of(), Optional.empty(), cursorAccepted, failure.detail());
    }

    private static boolean withinUpperBound(KafkaPackedBatchLocatorV1 locator, long upperBound) {
        return locator.startOffset() < upperBound && locator.endOffsetExclusive() <= upperBound;
    }

    private static KafkaBookKeeperReadCursorV1 cursor(KafkaBookKeeperReadSnapshotV1 snapshot, LocatedBatch next) {
        return new KafkaBookKeeperReadCursorV1(
                next.run().runBinding(),
                snapshot.root().fence(),
                next.run().sourceGeneration(),
                next.locator().indexIdentity(),
                next.locator().ordinal(),
                next.locator().entryId(),
                next.locator().startOffset(),
                snapshot.root().stateVersion());
    }

    private static int lookupStepCap(KafkaBookKeeperReadSnapshotV1 snapshot) {
        int steps = snapshot.runTable().runs().size();
        for (KafkaBookKeeperReadRunV1 run : snapshot.runTable().runs()) {
            steps = Math.addExact(
                    steps,
                    run.active() ? 1 : run.sealedDirectory().orElseThrow().size());
        }
        return Math.addExact(steps, 1);
    }

    private static long sequentialBatchCap(KafkaBookKeeperReadSnapshotV1 snapshot) {
        long batches = 0;
        for (KafkaBookKeeperReadRunV1 run : snapshot.runTable().runs()) {
            batches = Math.addExact(
                    batches,
                    run.active()
                            ? run.activeIndex().orElseThrow().size()
                            : Math.multiplyExact(
                                    (long) run.sealedDirectory().orElseThrow().size(),
                                    com.nereusstream.kafka.bookkeeper.nbke2.Nbke2ConstantsV1.FORMAT_MAX_LOCATOR_COUNT));
        }
        return batches;
    }

    private static <T> CompletionStage<T> completed(T value) {
        return CompletableFuture.completedFuture(value);
    }

    private record IndexCacheKey(RunLedgerHandleV1 handle, long sourceGeneration, long entryId) {}

    private record LocatedBatch(
            KafkaBookKeeperReadRunV1 run,
            Optional<KafkaIndexBlockPointerV1> blockPointer,
            KafkaPackedBatchLocatorIndexV1 index,
            KafkaPackedBatchLocatorV1 locator) {}

    private record Plan(Optional<LocatedBatch> located, Optional<Failure> failure) {
        private Plan {
            Objects.requireNonNull(located, "located");
            Objects.requireNonNull(failure, "failure");
            if (located.isPresent() && failure.isPresent()) {
                throw new IllegalArgumentException("lookup plan cannot be both located and failed");
            }
        }

        static Plan located(LocatedBatch located) {
            return new Plan(Optional.of(located), Optional.empty());
        }

        static Plan empty() {
            return new Plan(Optional.empty(), Optional.empty());
        }

        static Plan failed(Failure failure) {
            return new Plan(Optional.empty(), Optional.of(failure));
        }
    }

    private record InitialPlan(Plan plan, boolean cursorAccepted) {}

    private record LoadedIndex(Optional<KafkaPackedBatchLocatorIndexV1> index, Optional<Failure> failure) {
        static LoadedIndex loaded(KafkaPackedBatchLocatorIndexV1 index) {
            return new LoadedIndex(Optional.of(index), Optional.empty());
        }

        static LoadedIndex failed(Failure failure) {
            return new LoadedIndex(Optional.empty(), Optional.of(failure));
        }
    }

    private record EntryRead(Optional<ExactLedgerEntryV1> entry, Optional<Failure> failure) {
        static EntryRead read(ExactLedgerEntryV1 entry) {
            return new EntryRead(Optional.of(entry), Optional.empty());
        }

        static EntryRead failed(Failure failure) {
            return new EntryRead(Optional.empty(), Optional.of(failure));
        }
    }

    private record ReadData(Optional<KafkaBookKeeperReadBatchV1> batch, Optional<Failure> failure) {
        static ReadData read(KafkaBookKeeperReadBatchV1 batch) {
            return new ReadData(Optional.of(batch), Optional.empty());
        }

        static ReadData failed(Failure failure) {
            return new ReadData(Optional.empty(), Optional.of(failure));
        }
    }

    private record Failure(KafkaBookKeeperReadOutcomeV1 outcome, String detail) {
        static Failure end(String detail) {
            return new Failure(KafkaBookKeeperReadOutcomeV1.END_OF_SNAPSHOT, detail);
        }

        static Failure outOfRange(String detail) {
            return new Failure(KafkaBookKeeperReadOutcomeV1.OFFSET_OUT_OF_RANGE, detail);
        }

        static Failure absent(String detail) {
            return new Failure(KafkaBookKeeperReadOutcomeV1.DEFINITIVELY_ABSENT, detail);
        }

        static Failure fenced(String detail) {
            return new Failure(KafkaBookKeeperReadOutcomeV1.FENCED, detail);
        }

        static Failure provider(String detail) {
            return new Failure(KafkaBookKeeperReadOutcomeV1.PROVIDER_FAILURE, detail);
        }

        static Failure corrupt(String detail) {
            return new Failure(KafkaBookKeeperReadOutcomeV1.CORRUPT, detail);
        }
    }
}
