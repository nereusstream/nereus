/* Licensed under the Apache License, Version 2.0 */
package com.nereusstream.managedledger;

import static org.assertj.core.api.Assertions.assertThat;

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
import java.util.List;
import java.util.Map;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.ScheduledExecutorService;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicInteger;
import java.util.concurrent.atomic.AtomicReference;
import org.apache.bookkeeper.mledger.AsyncCallbacks;
import org.apache.bookkeeper.mledger.Entry;
import org.apache.bookkeeper.mledger.ManagedCursor;
import org.apache.bookkeeper.mledger.ManagedCursor.IndividualDeletedEntries;
import org.apache.bookkeeper.mledger.ManagedLedgerConfig;
import org.apache.bookkeeper.mledger.ManagedLedgerException;
import org.apache.bookkeeper.mledger.ManagedLedgerFactoryConfig;
import org.apache.bookkeeper.mledger.Position;
import org.apache.pulsar.common.api.proto.CommandSubscribe.InitialPosition;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.io.TempDir;

class NereusManagedCursorCallbackSafetyTest {
    private static final String NAME = "tenant/ns/persistent/cursor-callback-safety";

    @TempDir
    Path root;

    @Test
    void callbacksUseDesignatedExecutorExactlyOnceAndReleaseEntriesWhenCallerThrows() throws Exception {
        Clock clock = Clock.systemUTC();
        LocalFileObjectStore objectStore = new LocalFileObjectStore(root);
        FakeOxiaMetadataStore metadata = new FakeOxiaMetadataStore(clock::millis);
        FakeManagedLedgerProjectionMetadataStore projections =
                new FakeManagedLedgerProjectionMetadataStore();
        DefaultStreamStorage streamStorage = new DefaultStreamStorage(
                StreamStorageConfig.defaults("cluster/a", "cursor-callback-safety-test"),
                metadata,
                new DefaultWalObjectWriter(objectStore, "cursor-callback-safety-test", clock),
                new DefaultWalObjectReader(objectStore),
                clock,
                Runnable::run);
        ScheduledExecutorService scheduler = Executors.newSingleThreadScheduledExecutor(
                runnable -> new Thread(runnable, "cursor-safety-scheduler"));
        ExecutorService callbacks = Executors.newSingleThreadExecutor(
                runnable -> new Thread(runnable, "cursor-safety-callback"));
        try (NereusManagedLedgerRuntime runtime = ManagedLedgerRuntimeTestSupport.runtime(
                streamStorage,
                projections,
                new TestCursorStorage(),
                scheduler,
                callbacks)) {
            NereusManagedLedgerFactory factory = new NereusManagedLedgerFactory(
                    runtime,
                    fixedGuard(1),
                    config(),
                    new ManagedLedgerFactoryConfig(),
                    false);
            NereusManagedLedger ledger = (NereusManagedLedger) factory.open(NAME, config());
            Position first = ledger.addEntry(new byte[] {0});
            ledger.addEntry(new byte[] {1});
            ManagedCursor cursor = ledger.openCursor("callback");

            AtomicInteger invalidReadCalls = new AtomicInteger();
            CompletableFuture<String> invalidRead = new CompletableFuture<>();
            cursor.asyncReadEntries(0, new AsyncCallbacks.ReadEntriesCallback() {
                @Override
                public void readEntriesComplete(List<Entry> entries, Object ctx) {
                    invalidRead.completeExceptionally(new AssertionError("unexpected read success"));
                }

                @Override
                public void readEntriesFailed(ManagedLedgerException exception, Object ctx) {
                    invalidReadCalls.incrementAndGet();
                    invalidRead.complete(Thread.currentThread().getName());
                }
            }, null, null);
            assertThat(invalidRead.join()).isEqualTo("cursor-safety-callback");
            assertThat(invalidReadCalls).hasValue(1);

            AtomicInteger readCalls = new AtomicInteger();
            AtomicReference<Entry> callbackEntry = new AtomicReference<>();
            CompletableFuture<String> readCallback = new CompletableFuture<>();
            cursor.asyncReadEntries(1, new AsyncCallbacks.ReadEntriesCallback() {
                @Override
                public void readEntriesComplete(List<Entry> entries, Object ctx) {
                    readCalls.incrementAndGet();
                    callbackEntry.set(entries.getFirst());
                    readCallback.complete(Thread.currentThread().getName());
                    throw new IllegalStateException("caller read callback failure");
                }

                @Override
                public void readEntriesFailed(ManagedLedgerException exception, Object ctx) {
                    readCallback.completeExceptionally(exception);
                }
            }, null, null);
            assertThat(readCallback.join()).isEqualTo("cursor-safety-callback");
            callbacks.submit(() -> { }).get(5, TimeUnit.SECONDS);
            assertThat(readCalls).hasValue(1);
            assertThat(callbackEntry.get().release()).isFalse();

            AtomicReference<Entry> nthEntry = new AtomicReference<>();
            CompletableFuture<String> nthCallback = new CompletableFuture<>();
            cursor.asyncGetNthEntry(
                    1,
                    IndividualDeletedEntries.Exclude,
                    new AsyncCallbacks.ReadEntryCallback() {
                        @Override
                        public void readEntryComplete(Entry entry, Object ctx) {
                            nthEntry.set(entry);
                            nthCallback.complete(Thread.currentThread().getName());
                            throw new IllegalStateException("caller nth callback failure");
                        }

                        @Override
                        public void readEntryFailed(
                                ManagedLedgerException exception,
                                Object ctx) {
                            nthCallback.completeExceptionally(exception);
                        }
                    },
                    null);
            assertThat(nthCallback.join()).isEqualTo("cursor-safety-callback");
            callbacks.submit(() -> { }).get(5, TimeUnit.SECONDS);
            assertThat(nthEntry.get().release()).isFalse();

            AtomicInteger markDeleteCalls = new AtomicInteger();
            CompletableFuture<String> markDelete = new CompletableFuture<>();
            cursor.asyncMarkDelete(first, new AsyncCallbacks.MarkDeleteCallback() {
                @Override
                public void markDeleteComplete(Object ctx) {
                    markDeleteCalls.incrementAndGet();
                    assertThat(cursor.putProperty("callback", 1L)).isTrue();
                    markDelete.complete(Thread.currentThread().getName());
                }

                @Override
                public void markDeleteFailed(ManagedLedgerException exception, Object ctx) {
                    markDelete.completeExceptionally(exception);
                }
            }, null);
            assertThat(markDelete.join()).isEqualTo("cursor-safety-callback");
            assertThat(markDeleteCalls).hasValue(1);

            AtomicInteger invalidDeleteCalls = new AtomicInteger();
            CompletableFuture<String> invalidDelete = new CompletableFuture<>();
            cursor.asyncDelete((Position) null, new AsyncCallbacks.DeleteCallback() {
                @Override
                public void deleteComplete(Object ctx) {
                    invalidDelete.completeExceptionally(new AssertionError("unexpected delete success"));
                }

                @Override
                public void deleteFailed(ManagedLedgerException exception, Object ctx) {
                    invalidDeleteCalls.incrementAndGet();
                    invalidDelete.complete(Thread.currentThread().getName());
                }
            }, null);
            assertThat(invalidDelete.join()).isEqualTo("cursor-safety-callback");
            assertThat(invalidDeleteCalls).hasValue(1);

            ManagedCursor waiter = ledger.openCursor("waiter", InitialPosition.Latest);
            AtomicInteger pendingCalls = new AtomicInteger();
            CompletableFuture<String> pending = new CompletableFuture<>();
            waiter.asyncReadEntriesOrWait(1, new AsyncCallbacks.ReadEntriesCallback() {
                @Override
                public void readEntriesComplete(List<Entry> entries, Object ctx) {
                    pending.completeExceptionally(new AssertionError("unexpected pending read success"));
                }

                @Override
                public void readEntriesFailed(ManagedLedgerException exception, Object ctx) {
                    pendingCalls.incrementAndGet();
                    pending.complete(Thread.currentThread().getName());
                }
            }, null, null);
            scheduler.submit(() -> { }).get(5, TimeUnit.SECONDS);

            AtomicInteger closeCalls = new AtomicInteger();
            CompletableFuture<String> close = new CompletableFuture<>();
            waiter.asyncClose(new AsyncCallbacks.CloseCallback() {
                @Override
                public void closeComplete(Object ctx) {
                    closeCalls.incrementAndGet();
                    close.complete(Thread.currentThread().getName());
                }

                @Override
                public void closeFailed(ManagedLedgerException exception, Object ctx) {
                    close.completeExceptionally(exception);
                }
            }, null);
            assertThat(pending.join()).isEqualTo("cursor-safety-callback");
            assertThat(close.join()).isEqualTo("cursor-safety-callback");
            assertThat(pendingCalls).hasValue(1);
            assertThat(closeCalls).hasValue(1);

            ledger.close();
            factory.shutdown();
        } finally {
            metadata.close();
            objectStore.close();
        }
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
