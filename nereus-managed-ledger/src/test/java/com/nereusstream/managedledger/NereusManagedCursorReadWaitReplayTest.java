/* Licensed under the Apache License, Version 2.0 */
package com.nereusstream.managedledger;

import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.assertThatThrownBy;

import com.nereusstream.api.TrimOptions;
import com.nereusstream.core.DefaultStreamStorage;
import com.nereusstream.core.StreamStorageConfig;
import com.nereusstream.managedledger.cursor.TestCursorStorage;
import com.nereusstream.managedledger.integration.NereusCreationGuard;
import com.nereusstream.managedledger.integration.NereusCreationPermit;
import com.nereusstream.metadata.oxia.FakeManagedLedgerProjectionMetadataStore;
import com.nereusstream.metadata.oxia.testing.FakeOxiaMetadataStore;
import com.nereusstream.objectstore.testing.LocalFileObjectStore;
import com.nereusstream.objectstore.wal.DefaultWalObjectReader;
import com.nereusstream.objectstore.wal.DefaultWalObjectWriter;
import java.nio.file.Path;
import java.time.Clock;
import java.time.Duration;
import java.util.List;
import java.util.Map;
import java.util.Optional;
import java.util.Set;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.CompletionException;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicInteger;
import java.util.function.Predicate;
import org.apache.bookkeeper.mledger.AsyncCallbacks;
import org.apache.bookkeeper.mledger.Entry;
import org.apache.bookkeeper.mledger.ManagedCursor;
import org.apache.bookkeeper.mledger.ManagedCursor.FindPositionConstraint;
import org.apache.bookkeeper.mledger.ManagedLedgerConfig;
import org.apache.bookkeeper.mledger.ManagedLedgerException;
import org.apache.bookkeeper.mledger.ManagedLedgerFactoryConfig;
import org.apache.bookkeeper.mledger.Position;
import org.apache.bookkeeper.mledger.PositionFactory;
import org.apache.bookkeeper.mledger.ScanOutcome;
import org.apache.bookkeeper.mledger.util.ManagedLedgerUtils;
import org.apache.pulsar.common.api.proto.CommandSubscribe.InitialPosition;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.io.TempDir;

class NereusManagedCursorReadWaitReplayTest {
    private static final String NAME = "tenant/ns/persistent/cursor-read-wait-replay";

    @TempDir
    Path root;

    @Test
    void readsWaitsReplayFindAndScanUseCommittedAckAwareCoordinates() throws Exception {
        Clock clock = Clock.systemUTC();
        LocalFileObjectStore objectStore = new LocalFileObjectStore(root);
        FakeOxiaMetadataStore metadata = new FakeOxiaMetadataStore(clock::millis);
        FakeManagedLedgerProjectionMetadataStore projections =
                new FakeManagedLedgerProjectionMetadataStore();
        DefaultStreamStorage streamStorage = new DefaultStreamStorage(
                StreamStorageConfig.defaults("cluster/a", "cursor-read-wait-replay-test"),
                metadata,
                new DefaultWalObjectWriter(objectStore, "cursor-read-wait-replay-test", clock),
                new DefaultWalObjectReader(objectStore),
                clock,
                Runnable::run);
        try (NereusManagedLedgerRuntime runtime = ManagedLedgerRuntimeTestSupport.runtime(
                streamStorage, projections, new TestCursorStorage())) {
            NereusManagedLedgerFactory factory = new NereusManagedLedgerFactory(
                    runtime,
                    fixedGuard(1),
                    config(),
                    new ManagedLedgerFactoryConfig(),
                    false);
            NereusManagedLedger ledger = (NereusManagedLedger) factory.open(NAME, config());
            Position first = ledger.addEntry(new byte[] {0});
            Position second = ledger.addEntry(new byte[] {1});
            Position third = ledger.addEntry(new byte[] {2});
            Position fourth = ledger.addEntry(new byte[] {3});
            Position fifth = ledger.addEntry(new byte[] {4});

            ManagedCursor reader = ledger.openCursor("reader");
            reader.delete(second);
            List<Entry> bounded = read(
                    reader,
                    10,
                    ManagedLedgerUtils.NO_MAX_SIZE_LIMIT,
                    third,
                    null,
                    false).join();
            assertThat(bounded).extracting(Entry::getPosition).containsExactly(first, third);
            bounded.set(0, bounded.get(0));
            bounded.forEach(Entry::release);
            assertThat(reader.getReadPosition().getEntryId()).isEqualTo(3);

            reader.seek(first, true);
            List<Entry> byteBounded = read(reader, 10, 1, null, null, false).join();
            assertThat(byteBounded).extracting(Entry::getPosition).containsExactly(first);
            byteBounded.forEach(Entry::release);
            assertThat(reader.getReadPosition().getEntryId()).isEqualTo(2);

            reader.seek(first, true);
            List<Entry> predicateSkipped = read(
                    reader,
                    1,
                    ManagedLedgerUtils.NO_MAX_SIZE_LIMIT,
                    null,
                    position -> position.equals(first),
                    false).join();
            assertThat(predicateSkipped).extracting(Entry::getPosition).containsExactly(third);
            predicateSkipped.forEach(Entry::release);

            Position foreign = PositionFactory.create(first.getLedgerId() + 1, first.getEntryId());
            ReplayResult replay = replay(reader, Set.of(first, second, foreign));
            assertThat(replay.skipped()).isEqualTo(Set.of(second, foreign));
            assertThat(replay.entries().join()).extracting(Entry::getPosition).containsExactly(first);
            replay.entries().join().set(0, replay.entries().join().get(0));
            replay.entries().join().forEach(Entry::release);

            assertThat(reader.findNewestMatching(
                    FindPositionConstraint.SearchActiveEntries,
                    entry -> entry.getEntryId() == second.getEntryId())).isNull();
            assertThat(reader.findNewestMatching(
                    FindPositionConstraint.SearchAllAvailableEntries,
                    entry -> entry.getEntryId() == second.getEntryId())).isEqualTo(second);

            long readBeforeScan = reader.getReadPosition().getEntryId();
            AtomicInteger cappedPredicates = new AtomicInteger();
            assertThat(reader.scan(
                    Optional.empty(),
                    entry -> {
                        cappedPredicates.incrementAndGet();
                        return true;
                    },
                    1,
                    3,
                    TimeUnit.SECONDS.toMillis(5)).join()).isEqualTo(ScanOutcome.ABORTED);
            assertThat(cappedPredicates).hasValue(3);
            assertThat(reader.getReadPosition().getEntryId()).isEqualTo(readBeforeScan);
            assertThat(reader.scan(
                    Optional.of(first),
                    entry -> false,
                    2,
                    10,
                    TimeUnit.SECONDS.toMillis(5)).join()).isEqualTo(ScanOutcome.USER_INTERRUPTED);
            assertThat(reader.scan(
                    Optional.of(fifth),
                    entry -> true,
                    2,
                    10,
                    TimeUnit.SECONDS.toMillis(5)).join()).isEqualTo(ScanOutcome.COMPLETED);

            ManagedCursor waiter = ledger.openCursor("waiter", InitialPosition.Latest);
            CompletableFuture<List<Entry>> tailRead = read(
                    waiter,
                    1,
                    ManagedLedgerUtils.NO_MAX_SIZE_LIMIT,
                    null,
                    null,
                    true);
            assertThat(tailRead).isNotDone();
            Position appended = ledger.addEntry(new byte[] {5});
            assertThat(tailRead.join()).singleElement().satisfies(entry -> {
                assertThat(entry.getPosition()).isEqualTo(appended);
                entry.release();
            });

            CompletableFuture<List<Entry>> firstWaiter = read(
                    waiter,
                    1,
                    ManagedLedgerUtils.NO_MAX_SIZE_LIMIT,
                    null,
                    null,
                    true);
            runtime.scheduler().submit(() -> { }).get(5, TimeUnit.SECONDS);
            CompletableFuture<List<Entry>> concurrentWaiter = read(
                    waiter,
                    1,
                    ManagedLedgerUtils.NO_MAX_SIZE_LIMIT,
                    null,
                    null,
                    true);
            assertThatThrownBy(concurrentWaiter::join)
                    .isInstanceOf(CompletionException.class)
                    .hasCauseInstanceOf(ManagedLedgerException.ConcurrentWaitCallbackException.class);
            assertThat(waiter.cancelPendingReadRequest()).isTrue();
            assertThat(firstWaiter).isNotDone();

            streamStorage.trim(
                    ledger.projection().streamId(),
                    1,
                    new TrimOptions(Duration.ofSeconds(5), "cursor replay trim")).join();
            ledger.refreshMetadata().join();
            ReplayResult trimmedReplay = replay(reader, Set.of(first, fourth));
            assertThat(trimmedReplay.skipped()).isEqualTo(Set.of(first));
            assertThat(trimmedReplay.entries().join())
                    .extracting(Entry::getPosition)
                    .containsExactly(fourth);
            trimmedReplay.entries().join().forEach(Entry::release);

            ledger.close();
            factory.shutdown();
        } finally {
            metadata.close();
            objectStore.close();
        }
    }

    private static CompletableFuture<List<Entry>> read(
            ManagedCursor cursor,
            int count,
            long maxSizeBytes,
            Position maxPosition,
            Predicate<Position> skip,
            boolean wait) {
        CompletableFuture<List<Entry>> result = new CompletableFuture<>();
        AsyncCallbacks.ReadEntriesCallback callback = new AsyncCallbacks.ReadEntriesCallback() {
            @Override
            public void readEntriesComplete(List<Entry> entries, Object ctx) {
                result.complete(entries);
            }

            @Override
            public void readEntriesFailed(ManagedLedgerException exception, Object ctx) {
                result.completeExceptionally(exception);
            }
        };
        if (skip != null && wait) {
            cursor.asyncReadEntriesWithSkipOrWait(
                    count, maxSizeBytes, callback, null, maxPosition, skip);
        } else if (skip != null) {
            cursor.asyncReadEntriesWithSkip(
                    count, maxSizeBytes, callback, null, maxPosition, skip);
        } else if (wait) {
            cursor.asyncReadEntriesOrWait(count, maxSizeBytes, callback, null, maxPosition);
        } else {
            cursor.asyncReadEntries(count, maxSizeBytes, callback, null, maxPosition);
        }
        return result;
    }

    private static ReplayResult replay(ManagedCursor cursor, Set<Position> positions) {
        CompletableFuture<List<Entry>> entries = new CompletableFuture<>();
        Set<? extends Position> skipped = cursor.asyncReplayEntries(
                positions,
                new AsyncCallbacks.ReadEntriesCallback() {
                    @Override
                    public void readEntriesComplete(List<Entry> result, Object ctx) {
                        entries.complete(result);
                    }

                    @Override
                    public void readEntriesFailed(ManagedLedgerException exception, Object ctx) {
                        entries.completeExceptionally(exception);
                    }
                },
                null,
                true);
        return new ReplayResult(Set.copyOf(skipped), entries);
    }

    private static ManagedLedgerConfig config() {
        ManagedLedgerConfig config = new ManagedLedgerConfig();
        config.setStorageClassName("nereus");
        config.setCreateIfMissing(true);
        config.setProperties(Map.of());
        return config;
    }

    private static NereusCreationGuard fixedGuard(long generation) {
        return name -> CompletableFuture.completedFuture(new NereusCreationPermit() {
            @Override
            public String persistenceName() {
                return name;
            }

            @Override
            public long bindingGeneration() {
                return generation;
            }

            @Override
            public CompletableFuture<Void> validateBeforeProjectionPublish() {
                return CompletableFuture.completedFuture(null);
            }
        });
    }

    private record ReplayResult(
            Set<? extends Position> skipped,
            CompletableFuture<List<Entry>> entries) {
    }
}
