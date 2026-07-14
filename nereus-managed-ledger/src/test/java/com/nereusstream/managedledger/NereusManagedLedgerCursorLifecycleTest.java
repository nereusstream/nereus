/* Licensed under the Apache License, Version 2.0 */
package com.nereusstream.managedledger;

import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.assertThatThrownBy;

import com.nereusstream.core.DefaultStreamStorage;
import com.nereusstream.core.StreamStorageConfig;
import com.nereusstream.managedledger.cursor.CursorLifecycle;
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
import java.util.Map;
import java.util.concurrent.CompletableFuture;
import org.apache.bookkeeper.mledger.AsyncCallbacks;
import org.apache.bookkeeper.mledger.ManagedCursor;
import org.apache.bookkeeper.mledger.ManagedLedgerConfig;
import org.apache.bookkeeper.mledger.ManagedLedgerException;
import org.apache.bookkeeper.mledger.ManagedLedgerFactoryConfig;
import org.apache.bookkeeper.mledger.PositionFactory;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.io.TempDir;

class NereusManagedLedgerCursorLifecycleTest {
    private static final String NAME = "tenant/ns/persistent/cursor-lifecycle";

    @TempDir
    Path root;

    @Test
    void deleteRoutesTemporaryCursorLocallyAndDurableCursorThroughTombstone() throws Exception {
        Clock clock = Clock.systemUTC();
        LocalFileObjectStore objectStore = new LocalFileObjectStore(root);
        FakeOxiaMetadataStore metadata = new FakeOxiaMetadataStore(clock::millis);
        FakeManagedLedgerProjectionMetadataStore projections =
                new FakeManagedLedgerProjectionMetadataStore();
        DefaultStreamStorage streamStorage = new DefaultStreamStorage(
                StreamStorageConfig.defaults("cluster/a", "cursor-lifecycle-test"),
                metadata,
                new DefaultWalObjectWriter(objectStore, "cursor-lifecycle-test", clock),
                new DefaultWalObjectReader(objectStore),
                clock,
                Runnable::run);
        TestCursorStorage cursorStorage = new TestCursorStorage();
        try (NereusManagedLedgerRuntime runtime =
                ManagedLedgerRuntimeTestSupport.runtime(streamStorage, projections, cursorStorage)) {
            NereusManagedLedgerFactory factory = new NereusManagedLedgerFactory(
                    runtime,
                    fixedGuard(1),
                    config(),
                    new ManagedLedgerFactoryConfig(),
                    false);
            NereusManagedLedger ledger = (NereusManagedLedger) factory.open(NAME, config());

            ManagedCursor temporary = ledger.newNonDurableCursor(
                    PositionFactory.EARLIEST, "temporary");
            ledger.deleteCursor("temporary");
            assertThat(cursorStorage.deleteInvocationCount()).isZero();
            assertThat(ledger.getCursors()).doesNotContain(temporary);

            ledger.deleteCursor("missing");
            assertThat(cursorStorage.deleteInvocationCount()).isEqualTo(1);

            NereusManagedCursor first = (NereusManagedCursor) ledger.openCursor("durable");
            assertThat(first.durableHandle().identity().cursorGeneration()).isEqualTo(1);
            ledger.deleteCursor("durable");
            assertThat(cursorStorage.deleteInvocationCount()).isEqualTo(2);
            assertThat(cursorStorage.tombstone(ledger.cursorOwnerSession(), "durable"))
                    .hasValueSatisfying(state -> {
                        assertThat(state.lifecycle()).isEqualTo(CursorLifecycle.DELETED);
                        assertThat(state.identity().cursorGeneration()).isEqualTo(1);
                    });
            assertThatThrownBy(() -> first.putCursorProperty("after", "delete").join())
                    .hasRootCauseInstanceOf(
                            ManagedLedgerException.CursorAlreadyClosedException.class);

            ledger.deleteCursor("durable");
            assertThat(cursorStorage.deleteInvocationCount()).isEqualTo(3);
            NereusManagedCursor recreated = (NereusManagedCursor) ledger.openCursor("durable");
            assertThat(recreated.durableHandle().identity().cursorGeneration()).isEqualTo(2);

            assertThat(recreated.putProperty("flush", 1L)).isTrue();
            CompletableFuture<Void> flushGate = new CompletableFuture<>();
            cursorStorage.delayNextFlushCompletionUntil(flushGate);
            CompletableFuture<Void> firstClose = close(ledger);
            CompletableFuture<Void> repeatedClose = close(ledger);
            assertThat(firstClose).isNotDone();
            assertThat(repeatedClose).isNotDone();
            flushGate.complete(null);
            CompletableFuture.allOf(firstClose, repeatedClose).join();
            assertThat(factory.getManagedLedgers()).isEmpty();
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

    private static CompletableFuture<Void> close(NereusManagedLedger ledger) {
        CompletableFuture<Void> result = new CompletableFuture<>();
        ledger.asyncClose(new AsyncCallbacks.CloseCallback() {
            @Override
            public void closeComplete(Object ctx) {
                result.complete(null);
            }

            @Override
            public void closeFailed(ManagedLedgerException exception, Object ctx) {
                result.completeExceptionally(exception);
            }
        }, null);
        return result;
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
