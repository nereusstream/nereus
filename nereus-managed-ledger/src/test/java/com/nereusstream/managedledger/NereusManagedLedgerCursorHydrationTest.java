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
import java.util.Map;
import java.util.Set;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.CompletionException;
import java.util.function.Supplier;
import java.util.stream.StreamSupport;
import org.apache.bookkeeper.mledger.AsyncCallbacks;
import org.apache.bookkeeper.mledger.ManagedCursor;
import org.apache.bookkeeper.mledger.ManagedLedger;
import org.apache.bookkeeper.mledger.ManagedLedgerConfig;
import org.apache.bookkeeper.mledger.ManagedLedgerException;
import org.apache.bookkeeper.mledger.ManagedLedgerFactoryConfig;
import org.apache.bookkeeper.mledger.Position;
import org.apache.bookkeeper.mledger.PositionFactory;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.io.TempDir;

class NereusManagedLedgerCursorHydrationTest {
    private static final String NAME = "tenant/ns/persistent/cursor-hydration";

    @TempDir
    Path root;

    @Test
    void writableOpenPublishesOnlyACompleteHydratedRegistryAndFailsClosedOnCorruption()
            throws Exception {
        Clock clock = Clock.systemUTC();
        LocalFileObjectStore objectStore = new LocalFileObjectStore(root);
        FakeOxiaMetadataStore metadata = new FakeOxiaMetadataStore(clock::millis);
        FakeManagedLedgerProjectionMetadataStore projections =
                new FakeManagedLedgerProjectionMetadataStore();
        DefaultStreamStorage streamStorage = new DefaultStreamStorage(
                StreamStorageConfig.defaults("cluster/a", "cursor-hydration-test"),
                metadata,
                new DefaultWalObjectWriter(objectStore, "cursor-hydration-test", clock),
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
            NereusManagedLedger first = (NereusManagedLedger) factory.open(NAME, config());
            Position firstEntry = first.addEntry(new byte[] {0});
            Position secondEntry = first.addEntry(new byte[] {1});
            ManagedCursor alpha = first.openCursor("alpha");
            alpha.markDelete(firstEntry);
            first.openCursor("beta");
            first.newNonDurableCursor(PositionFactory.EARLIEST, "temporary");
            assertThat(cursorStorage.claimInvocationCount()).isEqualTo(1);
            first.close();

            CompletableFuture<Void> hydrationGate = new CompletableFuture<>();
            cursorStorage.delayNextClaimCompletionUntil(hydrationGate);
            CompletableFuture<ManagedLedger> reopening = open(
                    factory, NAME, () -> CompletableFuture.completedFuture(true));
            assertThat(reopening).isNotDone();
            hydrationGate.complete(null);
            NereusManagedLedger hydrated = (NereusManagedLedger) reopening.join();
            assertThat(cursorNames(hydrated.getCursors())).containsExactlyInAnyOrder("alpha", "beta");
            assertThat(cursorNames(hydrated.getActiveCursors()))
                    .containsExactlyInAnyOrder("alpha", "beta");
            ManagedCursor hydratedAlpha = StreamSupport.stream(
                            hydrated.getCursors().spliterator(), false)
                    .filter(cursor -> cursor.getName().equals("alpha"))
                    .findFirst()
                    .orElseThrow();
            assertThat(hydratedAlpha.getReadPosition()).isEqualTo(secondEntry);
            assertThat(hydratedAlpha.getMarkDeletedPosition()).isEqualTo(firstEntry);
            assertThat(cursorStorage.claimInvocationCount()).isEqualTo(2);
            cursorNames(hydrated.getCursors());
            assertThat(cursorStorage.claimInvocationCount()).isEqualTo(2);
            hydrated.close();

            cursorStorage.failNextClaim(new IllegalArgumentException("corrupt cursor root"));
            CompletableFuture<ManagedLedger> corruptOpen = open(
                    factory, NAME, () -> CompletableFuture.completedFuture(true));
            assertThatThrownBy(corruptOpen::join)
                    .isInstanceOf(CompletionException.class)
                    .hasRootCauseMessage("corrupt cursor root");
            assertThat(factory.getManagedLedgers()).isEmpty();
            assertThat(cursorStorage.claimInvocationCount()).isEqualTo(3);

            NereusManagedLedger recovered = (NereusManagedLedger) open(
                    factory, NAME, () -> CompletableFuture.completedFuture(true)).join();
            assertThat(cursorNames(recovered.getCursors()))
                    .containsExactlyInAnyOrder("alpha", "beta");
            assertThat(cursorStorage.claimInvocationCount()).isEqualTo(4);

            recovered.close();
            factory.shutdown();
        } finally {
            metadata.close();
            objectStore.close();
        }
    }

    private static Set<String> cursorNames(Iterable<ManagedCursor> cursors) {
        return StreamSupport.stream(cursors.spliterator(), false)
                .map(ManagedCursor::getName)
                .collect(java.util.stream.Collectors.toUnmodifiableSet());
    }

    private static CompletableFuture<ManagedLedger> open(
            NereusManagedLedgerFactory factory,
            String name,
            Supplier<CompletableFuture<Boolean>> ownershipChecker) {
        CompletableFuture<ManagedLedger> result = new CompletableFuture<>();
        factory.asyncOpen(
                name,
                config(),
                new AsyncCallbacks.OpenLedgerCallback() {
                    @Override
                    public void openLedgerComplete(ManagedLedger ledger, Object ctx) {
                        result.complete(ledger);
                    }

                    @Override
                    public void openLedgerFailed(ManagedLedgerException exception, Object ctx) {
                        result.completeExceptionally(exception);
                    }
                },
                ownershipChecker,
                null);
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
