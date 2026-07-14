/* Licensed under the Apache License, Version 2.0 */
package com.nereusstream.managedledger;

import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.assertThatThrownBy;

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
import org.apache.bookkeeper.mledger.Entry;
import org.apache.bookkeeper.mledger.ManagedCursor;
import org.apache.bookkeeper.mledger.ManagedLedgerConfig;
import org.apache.bookkeeper.mledger.ManagedLedgerException;
import org.apache.bookkeeper.mledger.ManagedLedgerFactoryConfig;
import org.apache.bookkeeper.mledger.Position;
import org.apache.pulsar.common.api.proto.CommandSubscribe.InitialPosition;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.io.TempDir;

class NereusManagedCursorDurableTest {
    private static final String NAME = "tenant/ns/persistent/cursor-durable";

    @TempDir
    Path root;

    @Test
    void durableAckTruthSurvivesReopenWhileDispatchAndDuplicateStateRemainLocal() throws Exception {
        Clock clock = Clock.systemUTC();
        LocalFileObjectStore objectStore = new LocalFileObjectStore(root);
        FakeOxiaMetadataStore metadata = new FakeOxiaMetadataStore(clock::millis);
        FakeManagedLedgerProjectionMetadataStore projections =
                new FakeManagedLedgerProjectionMetadataStore();
        DefaultStreamStorage streamStorage = new DefaultStreamStorage(
                StreamStorageConfig.defaults("cluster/a", "cursor-durable-test"),
                metadata,
                new DefaultWalObjectWriter(objectStore, "cursor-durable-test", clock),
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

            ManagedCursor durable = ledger.openCursor(
                    "durable",
                    InitialPosition.Earliest,
                    Map.of("initial", 1L),
                    Map.of("owner", "test"));
            assertThat(durable.isDurable()).isTrue();
            assertThat(durable.getPersistentMarkDeletedPosition())
                    .isEqualTo(durable.getMarkDeletedPosition());
            assertThat(durable.getProperties()).containsExactlyEntriesOf(Map.of("initial", 1L));
            assertThat(durable.getCursorProperties())
                    .containsExactlyEntriesOf(Map.of("owner", "test"));

            durable.markDelete(first, Map.of("checkpoint", 2L));
            durable.delete(third);
            durable.putCursorProperty("region", "a").join();
            List<Entry> dispatched = durable.readEntries(1);
            assertThat(dispatched).singleElement()
                    .satisfies(entry -> assertThat(entry.getPosition()).isEqualTo(second));
            dispatched.forEach(Entry::release);
            assertThat(durable.getReadPosition().getEntryId()).isEqualTo(3);
            durable.close();

            durable = ledger.openCursor("durable");
            assertThat(durable.getReadPosition()).isEqualTo(second);
            assertThat(durable.getMarkDeletedPosition()).isEqualTo(first);
            assertThat(durable.isMessageDeleted(third)).isTrue();
            assertThat(durable.getProperties())
                    .containsExactlyEntriesOf(Map.of("checkpoint", 2L));
            assertThat(durable.getCursorProperties())
                    .containsExactlyEntriesOf(Map.of("owner", "test", "region", "a"));

            ManagedCursor duplicate = durable.duplicateNonDurableCursor("analysis");
            assertThat(duplicate.isDurable()).isFalse();
            assertThat(duplicate.getPersistentMarkDeletedPosition()).isNull();
            assertThat(duplicate.getReadPosition()).isEqualTo(second);
            assertThat(duplicate.isMessageDeleted(third)).isTrue();
            List<Entry> duplicateRead = duplicate.readEntries(1);
            assertThat(duplicateRead).singleElement()
                    .satisfies(entry -> assertThat(entry.getPosition()).isEqualTo(second));
            duplicateRead.forEach(Entry::release);
            duplicate.markDelete(second);
            assertThat(durable.getMarkDeletedPosition()).isEqualTo(first);
            ledger.deleteCursor("analysis");
            assertThat(cursorStorage.deleteInvocationCount()).isZero();

            durable.clearBacklog();
            assertThat(durable.getMarkDeletedPosition()).isEqualTo(third);
            Position afterClear = ledger.addEntry(new byte[] {3});
            assertThat(durable.getNumberOfEntriesInBacklog(true)).isEqualTo(1);
            durable.close();
            ManagedCursor reopened = ledger.openCursor("durable");
            assertThat(reopened.getReadPosition()).isEqualTo(afterClear);
            assertThat(reopened.getNumberOfEntriesInBacklog(true)).isEqualTo(1);

            reopened.close();
            assertThat(reopened.isClosed()).isTrue();
            assertThatThrownBy(() -> reopened.putCursorProperty("closed", "value").join())
                    .hasRootCauseInstanceOf(
                            ManagedLedgerException.CursorAlreadyClosedException.class);

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
