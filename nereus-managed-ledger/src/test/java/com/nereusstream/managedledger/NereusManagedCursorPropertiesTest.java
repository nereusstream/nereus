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
import java.util.Map;
import java.util.concurrent.CompletableFuture;
import org.apache.bookkeeper.mledger.AsyncCallbacks;
import org.apache.bookkeeper.mledger.ManagedCursor;
import org.apache.bookkeeper.mledger.ManagedLedgerConfig;
import org.apache.bookkeeper.mledger.ManagedLedgerFactoryConfig;
import org.apache.bookkeeper.mledger.Position;
import org.apache.bookkeeper.mledger.PositionFactory;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.io.TempDir;

class NereusManagedCursorPropertiesTest {
    private static final String NAME = "tenant/ns/persistent/cursor-properties";

    @TempDir
    Path root;

    @Test
    void propertyEditRacingResetRebasesOnTheResetState() throws Exception {
        Clock clock = Clock.systemUTC();
        LocalFileObjectStore objectStore = new LocalFileObjectStore(root);
        FakeOxiaMetadataStore metadata = new FakeOxiaMetadataStore(clock::millis);
        FakeManagedLedgerProjectionMetadataStore projections =
                new FakeManagedLedgerProjectionMetadataStore();
        DefaultStreamStorage streamStorage = new DefaultStreamStorage(
                StreamStorageConfig.defaults("cluster/a", "cursor-property-test"),
                metadata,
                new DefaultWalObjectWriter(objectStore, "cursor-property-test", clock),
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
            ManagedCursor cursor = ledger.openCursor("durable");
            assertThat(cursor.putProperty("old", 1L)).isTrue();

            CompletableFuture<Void> resetGate = new CompletableFuture<>();
            cursorStorage.delayNextResetCompletionUntil(resetGate);
            CompletableFuture<Position> reset = reset(cursor, PositionFactory.EARLIEST, false);
            assertThat(reset).isNotDone();
            assertThat(cursor.putProperty("new", 2L)).isTrue();

            resetGate.complete(null);
            assertThat(reset.join().getEntryId()).isZero();
            assertThat(cursor.getProperties()).containsExactlyEntriesOf(Map.of("new", 2L));

            cursor.close();
            ManagedCursor reopened = ledger.openCursor("durable");
            assertThat(reopened.getProperties()).containsExactlyEntriesOf(Map.of("new", 2L));

            ledger.close();
            factory.shutdown();
        } finally {
            metadata.close();
            objectStore.close();
        }
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
            public void resetFailed(
                    org.apache.bookkeeper.mledger.ManagedLedgerException exception,
                    Object context) {
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
