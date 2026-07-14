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
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.CompletionException;
import org.apache.bookkeeper.mledger.AsyncCallbacks;
import org.apache.bookkeeper.mledger.Entry;
import org.apache.bookkeeper.mledger.ManagedCursor;
import org.apache.bookkeeper.mledger.ManagedCursor.IndividualDeletedEntries;
import org.apache.bookkeeper.mledger.ManagedLedgerConfig;
import org.apache.bookkeeper.mledger.ManagedLedgerException;
import org.apache.bookkeeper.mledger.ManagedLedgerFactoryConfig;
import org.apache.bookkeeper.mledger.Position;
import org.apache.bookkeeper.mledger.PositionFactory;
import org.apache.pulsar.common.api.proto.CommandSubscribe.InitialPosition;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.io.TempDir;

class NereusManagedCursorResetSeekTest {
    private static final String NAME = "tenant/ns/persistent/cursor-reset-seek";

    @TempDir
    Path root;

    @Test
    void destructiveResetOrdersReadsAndLocalNavigationRespectsAckAndTrim() throws Exception {
        Clock clock = Clock.systemUTC();
        LocalFileObjectStore objectStore = new LocalFileObjectStore(root);
        FakeOxiaMetadataStore metadata = new FakeOxiaMetadataStore(clock::millis);
        FakeManagedLedgerProjectionMetadataStore projections =
                new FakeManagedLedgerProjectionMetadataStore();
        DefaultStreamStorage streamStorage = new DefaultStreamStorage(
                StreamStorageConfig.defaults("cluster/a", "cursor-reset-seek-test"),
                metadata,
                new DefaultWalObjectWriter(objectStore, "cursor-reset-seek-test", clock),
                new DefaultWalObjectReader(objectStore),
                clock,
                Runnable::run);
        TestCursorStorage cursorStorage = new TestCursorStorage();
        try (NereusManagedLedgerRuntime runtime = ManagedLedgerRuntimeTestSupport.runtime(
                streamStorage, projections, cursorStorage)) {
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

            ManagedCursor ordered = ledger.openCursor("ordered");
            CompletableFuture<Void> resetGate = new CompletableFuture<>();
            cursorStorage.delayNextResetCompletionUntil(resetGate);
            CompletableFuture<Position> reset = reset(ordered, PositionFactory.EARLIEST, false);
            CompletableFuture<List<Entry>> queuedRead = read(ordered, 1, false);
            assertThat(reset).isNotDone();
            assertThat(queuedRead).isNotDone();
            resetGate.complete(null);
            assertThat(reset.join().getEntryId()).isZero();
            assertThat(queuedRead.join()).singleElement().satisfies(entry -> {
                assertThat(entry.getPosition()).isEqualTo(first);
                entry.release();
            });

            ManagedCursor waiter = ledger.openCursor("waiter", InitialPosition.Latest);
            CompletableFuture<List<Entry>> pending = read(waiter, 1, true);
            assertThat(pending).isNotDone();
            assertThat(reset(waiter, PositionFactory.EARLIEST, false).join().getEntryId())
                    .isZero();
            assertThatThrownBy(pending::join)
                    .isInstanceOf(CompletionException.class)
                    .hasCauseInstanceOf(ManagedLedgerException.class)
                    .hasMessageContaining("cursor reset invalidated a pending cursor read");

            ManagedCursor excluded = ledger.openCursor("excluded");
            excluded.delete(second);
            Entry secondEligible = excluded.getNthEntry(2, IndividualDeletedEntries.Exclude);
            assertThat(secondEligible.getPosition()).isEqualTo(third);
            secondEligible.release();
            Entry secondPhysical = excluded.getNthEntry(2, IndividualDeletedEntries.Include);
            assertThat(secondPhysical.getPosition()).isEqualTo(second);
            secondPhysical.release();
            excluded.skipEntries(2, IndividualDeletedEntries.Exclude);
            assertThat(excluded.getMarkDeletedPosition()).isEqualTo(third);

            ManagedCursor included = ledger.openCursor("included");
            included.delete(second);
            included.skipEntries(2, IndividualDeletedEntries.Include);
            assertThat(included.getMarkDeletedPosition()).isEqualTo(second);

            ordered.markDelete(first);
            ordered.seek(first, false);
            assertThat(ordered.getReadPosition().getEntryId()).isEqualTo(1);
            ordered.seek(first, true);
            assertThat(ordered.getReadPosition().getEntryId()).isZero();
            streamStorage.trim(
                    ledger.projection().streamId(),
                    2,
                    new TrimOptions(Duration.ofSeconds(5), "cursor rewind trim clamp")).join();
            ledger.refreshMetadata().join();
            ordered.rewind();
            assertThat(ordered.getReadPosition().getEntryId()).isEqualTo(2);

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
        if (wait) {
            cursor.asyncReadEntriesOrWait(1, callback, null, null);
        } else {
            cursor.asyncReadEntries(count, callback, null, null);
        }
        return result;
    }

    private static CompletableFuture<Position> reset(
            ManagedCursor cursor,
            Position position,
            boolean force) {
        CompletableFuture<Position> result = new CompletableFuture<>();
        cursor.asyncResetCursor(position, force, new AsyncCallbacks.ResetCursorCallback() {
            @Override
            public void resetComplete(Object context) {
                result.complete((Position) context);
            }

            @Override
            public void resetFailed(ManagedLedgerException exception, Object context) {
                result.completeExceptionally(exception);
            }
        });
        return result;
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
}
