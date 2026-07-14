/* Licensed under the Apache License, Version 2.0 */
package com.nereusstream.managedledger;

import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.assertThatThrownBy;

import com.google.common.collect.Range;
import com.nereusstream.core.DefaultStreamStorage;
import com.nereusstream.core.StreamStorageConfig;
import com.nereusstream.api.AppendAttemptId;
import com.nereusstream.api.AppendBatch;
import com.nereusstream.api.AppendOptions;
import com.nereusstream.api.AppendOutcome;
import com.nereusstream.api.AppendRecoveryOptions;
import com.nereusstream.api.AppendResult;
import com.nereusstream.api.AppendSession;
import com.nereusstream.api.AppendSessionOptions;
import com.nereusstream.api.Checksum;
import com.nereusstream.api.ChecksumType;
import com.nereusstream.api.DeleteOptions;
import com.nereusstream.api.EntryIndexLocation;
import com.nereusstream.api.EntryIndexRef;
import com.nereusstream.api.ErrorCode;
import com.nereusstream.api.NereusException;
import com.nereusstream.api.ObjectId;
import com.nereusstream.api.ObjectKey;
import com.nereusstream.api.ObjectType;
import com.nereusstream.api.OffsetRange;
import com.nereusstream.api.PayloadFormat;
import com.nereusstream.api.ReadOptions;
import com.nereusstream.api.ReadResult;
import com.nereusstream.api.ResolveOptions;
import com.nereusstream.api.ResolveResult;
import com.nereusstream.api.SealOptions;
import com.nereusstream.api.StorageProfile;
import com.nereusstream.api.StreamCreateOptions;
import com.nereusstream.api.StreamId;
import com.nereusstream.api.StreamMetadata;
import com.nereusstream.api.StreamName;
import com.nereusstream.api.StreamState;
import com.nereusstream.api.StreamStorage;
import com.nereusstream.api.TrimOptions;
import com.nereusstream.api.target.ObjectSliceReadTarget;
import com.nereusstream.managedledger.integration.NereusCreationGuard;
import com.nereusstream.managedledger.integration.NereusCreationPermit;
import com.nereusstream.metadata.oxia.FakeManagedLedgerProjectionMetadataStore;
import com.nereusstream.metadata.oxia.ManagedLedgerFacadeState;
import com.nereusstream.metadata.oxia.ManagedLedgerProjectionNames;
import com.nereusstream.metadata.oxia.ProjectionMetadataStoreConfig;
import com.nereusstream.metadata.oxia.testing.FakeOxiaMetadataStore;
import com.nereusstream.metadata.oxia.records.TopicProjectionRecord;
import com.nereusstream.objectstore.HeadObjectOptions;
import com.nereusstream.objectstore.testing.LocalFileObjectStore;
import com.nereusstream.objectstore.wal.DefaultWalObjectReader;
import com.nereusstream.objectstore.wal.DefaultWalObjectWriter;
import com.nereusstream.objectstore.wal.PreparedWalObject;
import com.nereusstream.objectstore.wal.WalObjectWriter;
import com.nereusstream.objectstore.wal.WalWriteRequest;
import com.nereusstream.objectstore.wal.WalWriteResult;
import java.nio.charset.StandardCharsets;
import java.nio.file.Path;
import java.time.Clock;
import java.time.Duration;
import java.util.List;
import java.util.Map;
import java.util.Optional;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.CompletionException;
import java.util.concurrent.CopyOnWriteArrayList;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicBoolean;
import java.util.concurrent.atomic.AtomicInteger;
import java.util.concurrent.atomic.AtomicLong;
import java.util.stream.IntStream;
import io.netty.buffer.ByteBuf;
import org.apache.bookkeeper.mledger.AsyncCallbacks;
import org.apache.bookkeeper.mledger.AsyncCallbacks.AddEntryCallback;
import org.apache.bookkeeper.mledger.AsyncCallbacks.ReadEntryCallback;
import org.apache.bookkeeper.mledger.AsyncCallbacks.OpenReadOnlyManagedLedgerCallback;
import org.apache.bookkeeper.mledger.Entry;
import org.apache.bookkeeper.mledger.ManagedLedger;
import org.apache.bookkeeper.mledger.ManagedCursor;
import org.apache.bookkeeper.mledger.ManagedLedgerConfig;
import org.apache.bookkeeper.mledger.ManagedLedgerException;
import org.apache.bookkeeper.mledger.ManagedLedgerFactoryConfig;
import org.apache.bookkeeper.mledger.Position;
import org.apache.bookkeeper.mledger.PositionFactory;
import org.apache.bookkeeper.mledger.ReadOnlyCursor;
import org.apache.bookkeeper.mledger.ReadOnlyManagedLedger;
import org.apache.pulsar.common.api.proto.CommandSubscribe.InitialPosition;
import org.apache.pulsar.common.naming.TopicName;
import org.apache.pulsar.common.policies.data.ManagedLedgerInternalStats;
import org.apache.pulsar.common.policies.data.PersistentOfflineTopicStats;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.io.TempDir;

class NereusManagedLedgerFacadeTest {
    private static final String NAME = "tenant/ns/persistent/facade";

    @TempDir
    Path root;

    @Test
    void factoryLedgerAppendReadPropertiesLifecycleAndReopenUseL0Truth() throws Exception {
        Clock clock = Clock.systemUTC();
        LocalFileObjectStore objectStore = new LocalFileObjectStore(root);
        FakeOxiaMetadataStore metadata = new FakeOxiaMetadataStore(clock::millis);
        FakeManagedLedgerProjectionMetadataStore projections =
                new FakeManagedLedgerProjectionMetadataStore();
        DefaultStreamStorage storage = new DefaultStreamStorage(
                StreamStorageConfig.defaults("cluster/a", "pulsar-test"),
                metadata,
                new DefaultWalObjectWriter(objectStore, "facade-test", clock),
                new DefaultWalObjectReader(objectStore),
                clock,
                Runnable::run);
        try (NereusManagedLedgerRuntime runtime =
                ManagedLedgerRuntimeTestSupport.runtime(storage, projections)) {
            NereusManagedLedgerFactory factory = new NereusManagedLedgerFactory(
                    runtime,
                    fixedGuard(7),
                    config(true),
                    new ManagedLedgerFactoryConfig(),
                    false);
            assertThat(factory.getManagedLedgerPropertiesAsync(NAME).join()).isEmpty();

            List<CompletableFuture<ManagedLedger>> concurrentOpens = IntStream.range(0, 100)
                    .mapToObj(ignored -> CompletableFuture.supplyAsync(() -> {
                        try {
                            return factory.open(NAME, config(true));
                        } catch (InterruptedException e) {
                            Thread.currentThread().interrupt();
                            throw new CompletionException(e);
                        } catch (ManagedLedgerException e) {
                            throw new CompletionException(e);
                        }
                    }))
                    .toList();
            CompletableFuture.allOf(concurrentOpens.toArray(CompletableFuture[]::new)).join();
            NereusManagedLedger ledger = (NereusManagedLedger) concurrentOpens.getFirst().join();
            assertThat(concurrentOpens).allSatisfy(open -> assertThat(open.join()).isSameAs(ledger));
            assertThat(factory.open(NAME, config(true))).isSameAs(ledger);
            ManagedCursor tailCursor = ledger.newNonDurableCursor(PositionFactory.LATEST, "tail-reader");
            CompletableFuture<List<Entry>> tailRead = readOrWait(tailCursor, 1);
            assertThat(tailRead).isNotDone();
            byte[] payload = "pulsar-entry".getBytes(StandardCharsets.UTF_8);
            Position position = ledger.addEntry(payload, 3);
            List<Entry> awakened = tailRead.join();
            assertThat(awakened).singleElement()
                    .satisfies(value -> assertThat(value.getData()).isEqualTo(payload));
            awakened.forEach(Entry::release);
            tailCursor.close();

            assertThat(position.getLedgerId()).isEqualTo(ledger.projection().virtualLedgerId());
            assertThat(position.getEntryId()).isZero();
            assertThat(ledger.getNumberOfEntries()).isEqualTo(1);
            assertThat(ledger.getTotalSize()).isEqualTo(payload.length);
            assertThat(ledger.getLastConfirmedEntry()).isEqualTo(position);
            assertThat(ledger.getFirstPosition().getEntryId()).isEqualTo(-1);
            assertThat(ledger.getNumberOfEntries(Range.closed(position, position))).isEqualTo(1);
            assertThat(ledger.getNumberOfActiveEntries()).isZero();
            assertThat(ledger.getEstimatedBacklogSize()).isZero();
            assertThat(ledger.getEarliestMessagePublishTimeInBacklog().join()).isZero();
            assertThat(ledger.getLedgersInfo().get(position.getLedgerId()).getEntries()).isEqualTo(1);
            assertThat(ledger.getStats().getStoredMessagesLogicalSize()).isEqualTo(payload.length);
            assertThat(ledger.getStats().getAddEntrySucceedTotal()).isEqualTo(1);
            assertThat(ledger.getStats().getAddEntryBytesTotal()).isEqualTo(payload.length);
            assertThat(ledger.getStats().getAddEntryWithReplicasBytesTotal()).isEqualTo(payload.length);
            assertThat(factory.getCacheStats().getCacheUsedSize()).isZero();
            assertThat(factory.getEntryCacheManager().getSize()).isZero();

            Entry entry = read(ledger, position).join();
            assertThat(entry.getData()).isEqualTo(payload);
            assertThat(entry.release()).isTrue();
            assertThat(ledger.getStats().getEntriesReadTotalCount()).isEqualTo(1);
            assertThat(ledger.getStats().getReadEntriesBytesTotal()).isEqualTo(payload.length);

            ManagedCursor durableCursor = ledger.openCursor("subscription");
            assertThat(durableCursor.isDurable()).isTrue();
            assertThat(durableCursor.getNumberOfEntriesInBacklog(true)).isEqualTo(1);
            List<Entry> durableRead = durableCursor.readEntries(1);
            assertThat(durableRead).hasSize(1);
            durableRead.forEach(Entry::release);
            assertThatThrownBy(() -> durableCursor.markDelete(position))
                    .isInstanceOf(ManagedLedgerException.class)
                    .hasMessageStartingWith("NEREUS_UNSUPPORTED_OPERATION:");
            ManagedCursor nonDurable = ledger.newNonDurableCursor(
                    PositionFactory.EARLIEST, "reader");
            assertThat(nonDurable.isDurable()).isFalse();
            assertThat(nonDurable.getReadPosition().getEntryId()).isZero();
            assertThat(nonDurable.getMarkDeletedPosition().getEntryId()).isEqualTo(-1);
            nonDurable.markDelete(position);
            assertThat(nonDurable.getMarkDeletedPosition()).isEqualTo(position);
            assertThat(nonDurable.getNumberOfEntriesInBacklog(true)).isZero();
            nonDurable.close();
            ManagedCursor explicitEarliest = ledger.newNonDurableCursor(
                    PositionFactory.EARLIEST, "explicit-earliest", InitialPosition.Latest, false);
            assertThat(explicitEarliest.getReadPosition().getEntryId()).isZero();
            assertThat(explicitEarliest.getMarkDeletedPosition().getEntryId()).isEqualTo(-1);
            explicitEarliest.close();
            ledger.deleteCursor("subscription");
            assertThat(ledger.asyncFindPosition(candidate -> true).join().getEntryId()).isEqualTo(1);
            assertThat(ledger.getLastDispatchablePosition(candidate -> true, position).join())
                    .isEqualTo(position);
            ManagedLedgerInternalStats internal = ledger.getManagedLedgerInternalStats(true).join();
            assertThat(internal.numberOfEntries).isEqualTo(1);
            assertThat(internal.ledgers).singleElement().satisfies(info -> {
                assertThat(info.ledgerId).isEqualTo(position.getLedgerId());
                assertThat(info.metadata).contains("nereus{streamId=");
            });
            assertThat(factory.asyncExists(NAME).join()).isTrue();
            assertThat(factory.getManagedLedgerInfo(NAME).ledgers).singleElement()
                    .satisfies(info -> assertThat(info.ledgerId).isEqualTo(position.getLedgerId()));
            PersistentOfflineTopicStats offline = new PersistentOfflineTopicStats(NAME, "test-broker");
            factory.estimateUnloadedTopicBacklog(
                    offline, TopicName.get("persistent://tenant/ns/facade"), true, new Object());
            assertThat(offline.totalMessages).isEqualTo(1);
            assertThat(offline.storageSize).isEqualTo(payload.length);
            assertThat(offline.dataLedgerDetails).hasSize(1);

            ReadOnlyManagedLedger readOnly = openReadOnly(factory, NAME, config(true)).join();
            assertThat(readOnly.getNumberOfEntries()).isEqualTo(1);
            Entry readOnlyEntry = read(readOnly, position).join();
            assertThat(readOnlyEntry.getData()).isEqualTo(payload);
            readOnlyEntry.release();
            ReadOnlyCursor readOnlyCursor = factory.openReadOnlyCursor(
                    NAME, PositionFactory.EARLIEST, config(true));
            List<Entry> cursorEntries = readOnlyCursor.readEntries(10);
            assertThat(cursorEntries).singleElement()
                    .satisfies(cursorEntry -> assertThat(cursorEntry.getData()).isEqualTo(payload));
            cursorEntries.forEach(Entry::release);
            assertThat(readOnlyCursor.getReadPosition().getEntryId()).isEqualTo(1);
            assertThat(readOnlyCursor.hasMoreEntries()).isFalse();
            readOnlyCursor.close();
            assertThatThrownBy(() -> openReadOnly(
                    factory, "tenant/ns/persistent/missing", config(true)).join())
                    .isInstanceOf(CompletionException.class)
                    .hasRootCauseInstanceOf(ManagedLedgerException.ManagedLedgerNotFoundException.class);

            ledger.setProperty("owner", "nereus");
            assertThat(ledger.getProperties()).containsEntry("owner", "nereus");
            assertThat(factory.getManagedLedgerPropertiesAsync(NAME).join())
                    .containsEntry("owner", "nereus");

            assertThat(ledger.terminate()).isEqualTo(position);
            assertThat(ledger.isTerminated()).isTrue();
            assertThatThrownBy(() -> ledger.addEntry(new byte[] {1}))
                    .isInstanceOf(ManagedLedgerException.ManagedLedgerTerminatedException.class);
            Entry sealedEntry = read(ledger, position).join();
            assertThat(sealedEntry.getData()).isEqualTo(payload);
            sealedEntry.release();

            ledger.delete();
            assertThat(factory.inspectStorageState(NAME).join().state())
                    .isEqualTo(NereusDurableStorageState.DELETED);
            assertThat(factory.asyncExists(NAME).join()).isFalse();
            assertThatThrownBy(() -> factory.getManagedLedgerPropertiesAsync(NAME).join())
                    .hasRootCauseInstanceOf(NereusException.class);
            ledger.close();
            assertThat(factory.getManagedLedgers()).isEmpty();

            String unloadName = "tenant/ns/persistent/unload-only";
            NereusManagedLedger unloadOnly = (NereusManagedLedger) factory.open(unloadName, config(true));
            unloadOnly.close();
            assertThat(factory.inspectStorageState(unloadName).join().state())
                    .isEqualTo(NereusDurableStorageState.ACTIVE);
            factory.shutdown();
        } finally {
            metadata.close();
            objectStore.close();
        }
    }

    @Test
    void nonDurableCursorStartSeekResetAndTrimMatchStockContract() throws Exception {
        Clock clock = Clock.systemUTC();
        LocalFileObjectStore objectStore = new LocalFileObjectStore(root.resolve("cursor-contract"));
        FakeOxiaMetadataStore metadata = new FakeOxiaMetadataStore(clock::millis);
        FakeManagedLedgerProjectionMetadataStore projections =
                new FakeManagedLedgerProjectionMetadataStore();
        DefaultStreamStorage storage = new DefaultStreamStorage(
                StreamStorageConfig.defaults("cluster/a", "cursor-contract-test"),
                metadata,
                new DefaultWalObjectWriter(objectStore, "cursor-contract-test", clock),
                new DefaultWalObjectReader(objectStore),
                clock,
                Runnable::run);
        try (NereusManagedLedgerRuntime runtime =
                ManagedLedgerRuntimeTestSupport.runtime(storage, projections)) {
            NereusManagedLedgerFactory factory = new NereusManagedLedgerFactory(
                    runtime,
                    fixedGuard(7),
                    config(true),
                    new ManagedLedgerFactoryConfig(),
                    false);
            NereusManagedLedger ledger = (NereusManagedLedger) factory.open(NAME, config(true));
            Position first = ledger.addEntry("entry-0".getBytes(StandardCharsets.UTF_8));
            Position second = ledger.addEntry("entry-1".getBytes(StandardCharsets.UTF_8));
            Position third = ledger.addEntry("entry-2".getBytes(StandardCharsets.UTF_8));
            long ledgerId = ledger.projection().virtualLedgerId();

            ManagedCursor afterFirst = ledger.newNonDurableCursor(first, "after-first");
            assertCursorBoundary(afterFirst, 1, 0);
            List<Entry> afterFirstEntries = afterFirst.readEntries(1);
            assertThat(afterFirstEntries).singleElement()
                    .satisfies(entry -> assertThat(entry.getPosition()).isEqualTo(second));
            afterFirstEntries.forEach(Entry::release);

            ManagedCursor beforeFirst = ledger.newNonDurableCursor(
                    PositionFactory.create(ledgerId, -1), "before-first");
            assertCursorBoundary(beforeFirst, 0, -1);

            ManagedCursor defaultLatest = ledger.newNonDurableCursor(null, "default-latest");
            assertCursorBoundary(defaultLatest, 3, 2);

            ManagedCursor latestWithEarliestDefault = ledger.newNonDurableCursor(
                    PositionFactory.LATEST,
                    "latest-with-earliest-default",
                    InitialPosition.Earliest,
                    false);
            assertCursorBoundary(latestWithEarliestDefault, 0, -1);

            ManagedCursor earliestWithLatestDefault = ledger.newNonDurableCursor(
                    PositionFactory.EARLIEST,
                    "earliest-with-latest-default",
                    InitialPosition.Latest,
                    false);
            assertCursorBoundary(earliestWithLatestDefault, 0, -1);

            ManagedCursor futureWithEarliestDefault = ledger.newNonDurableCursor(
                    PositionFactory.create(ledgerId, 99),
                    "future-with-earliest-default",
                    InitialPosition.Earliest,
                    false);
            assertCursorBoundary(futureWithEarliestDefault, 0, -1);

            assertThatThrownBy(() -> ledger.newNonDurableCursor(
                    PositionFactory.create(ledgerId + 1, 0), "wrong-ledger"))
                    .isInstanceOf(IllegalArgumentException.class);

            ManagedCursor anonymousOne = ledger.newNonDurableCursor(first);
            ManagedCursor anonymousTwo = ledger.newNonDurableCursor(first);
            assertThat(anonymousOne).isNotSameAs(anonymousTwo);

            ManagedCursor navigation = ledger.newNonDurableCursor(
                    PositionFactory.EARLIEST, "navigation");
            assertThat(navigation.getPersistentMarkDeletedPosition()).isNull();
            navigation.markDelete(second);
            navigation.seek(first, false);
            assertCursorBoundary(navigation, 2, 1);
            navigation.seek(first, true);
            assertCursorBoundary(navigation, 0, 1);
            navigation.rewind();
            assertCursorBoundary(navigation, 2, 1);
            navigation.resetCursor(second);
            assertCursorBoundary(navigation, 1, 0);
            navigation.resetCursor(PositionFactory.EARLIEST);
            assertCursorBoundary(navigation, 0, -1);
            navigation.resetCursor(PositionFactory.LATEST);
            assertCursorBoundary(navigation, 3, 2);
            assertThat(resetCursorAsync(navigation, first, true).join()).isEqualTo(first);
            assertCursorBoundary(navigation, 0, -1);
            assertThat(resetCursorAsync(
                    navigation, PositionFactory.create(ledgerId, 99), false).join())
                    .isEqualTo(PositionFactory.create(ledgerId, 3));
            assertCursorBoundary(navigation, 3, 2);
            assertThatThrownBy(() -> navigation.rewind(true))
                    .isInstanceOf(UnsupportedOperationException.class)
                    .hasMessageStartingWith("NEREUS_UNSUPPORTED_OPERATION:");

            ManagedCursor trimFollower = ledger.newNonDurableCursor(
                    PositionFactory.EARLIEST, "trim-follower");
            storage.trim(
                    ledger.projection().streamId(),
                    2,
                    new TrimOptions(Duration.ofSeconds(5), "cursor contract trim")).join();
            ledger.refreshMetadata().join();
            assertThat(trimFollower.getNumberOfEntries()).isEqualTo(1);
            assertThat(trimFollower.getNumberOfEntriesInBacklog(true)).isEqualTo(1);
            List<Entry> retained = trimFollower.readEntries(1);
            assertThat(retained).singleElement()
                    .satisfies(entry -> assertThat(entry.getPosition()).isEqualTo(third));
            retained.forEach(Entry::release);

            ManagedCursor trimmedStart = ledger.newNonDurableCursor(first, "trimmed-start");
            assertCursorBoundary(trimmedStart, 2, 1);
            ManagedCursor firstRetainedBatchStart = ledger.newNonDurableCursor(
                    PositionFactory.create(ledgerId, 1), "first-retained-batch-start");
            assertCursorBoundary(firstRetainedBatchStart, 2, 1);
            assertThatThrownBy(() -> resetCursorAsync(navigation, first, true).join())
                    .isInstanceOf(CompletionException.class)
                    .hasCauseInstanceOf(ManagedLedgerException.InvalidCursorPositionException.class);

            List.of(
                    afterFirst,
                    beforeFirst,
                    defaultLatest,
                    latestWithEarliestDefault,
                    earliestWithLatestDefault,
                    futureWithEarliestDefault,
                    anonymousOne,
                    anonymousTwo,
                    navigation,
                    trimFollower,
                    trimmedStart,
                    firstRetainedBatchStart).forEach(cursor -> {
                        try {
                            cursor.close();
                        } catch (InterruptedException | ManagedLedgerException error) {
                            throw new AssertionError(error);
                        }
                    });
            ledger.close();
            factory.shutdown();
        }
    }

    @Test
    void uncertainAppendPublishesFenceAndTerminalRecoveryClearsTheSameGeneration() {
        FenceStreamStorage storage = new FenceStreamStorage();
        FakeManagedLedgerProjectionMetadataStore projections =
                new FakeManagedLedgerProjectionMetadataStore();
        try (NereusManagedLedgerRuntime runtime =
                ManagedLedgerRuntimeTestSupport.runtime(storage, projections)) {
            NereusManagedLedger ledger = new NereusManagedLedger(
                    runtime, opened(), config(true), () -> { });
            CompletableFuture<ManagedLedgerException> callbackFailure = new CompletableFuture<>();

            ledger.asyncAddEntry(new byte[] {1, 2, 3}, new AddEntryCallback() {
                @Override
                public void addComplete(Position position, ByteBuf entryData, Object ctx) {
                    callbackFailure.completeExceptionally(new AssertionError("unexpected append success"));
                }

                @Override
                public void addFailed(ManagedLedgerException exception, Object ctx) {
                    try {
                        assertThat(storage.recoveries).hasValue(1);
                        assertThat(ledger.currentWriteFence()).isPresent();
                        callbackFailure.complete(exception);
                    } catch (Throwable assertion) {
                        callbackFailure.completeExceptionally(assertion);
                    }
                }
            }, null);

            assertThat(callbackFailure.join()).isNotNull();
            NereusWriteFenceSnapshot fence = ledger.currentWriteFence().orElseThrow();
            CompletableFuture<NereusWriteFenceResolution> terminal =
                    ledger.awaitWriteFence(fence.generation());
            assertThat(terminal).isNotDone();

            storage.terminalRecovery.complete(appendResult(0, 1, 3, 1));

            assertThat(terminal.join()).isEqualTo(NereusWriteFenceResolution.COMMITTED);
            assertThat(ledger.currentWriteFence()).isEmpty();
            assertThat(ledger.getNumberOfEntries()).isEqualTo(1);
            assertThat(ledger.awaitWriteFence(fence.generation()).join())
                    .isEqualTo(NereusWriteFenceResolution.COMMITTED);
            ledger.close();
        } catch (ManagedLedgerException e) {
            throw new AssertionError(e);
        }
    }

    @Test
    void committedResponseLossRecoversOneSuccessCallbackAndOnePosition() throws Exception {
        Clock clock = Clock.systemUTC();
        LocalFileObjectStore objectStore = new LocalFileObjectStore(root.resolve("response-loss"));
        FakeOxiaMetadataStore metadata = new FakeOxiaMetadataStore(clock::millis);
        FakeManagedLedgerProjectionMetadataStore projections =
                new FakeManagedLedgerProjectionMetadataStore();
        DefaultStreamStorage storage = new DefaultStreamStorage(
                StreamStorageConfig.defaults("cluster/a", "pulsar-response-loss"),
                metadata,
                new DefaultWalObjectWriter(objectStore, "response-loss", clock),
                new DefaultWalObjectReader(objectStore),
                clock,
                Runnable::run);
        try (NereusManagedLedgerRuntime runtime =
                ManagedLedgerRuntimeTestSupport.runtime(storage, projections)) {
            NereusManagedLedgerFactory factory = new NereusManagedLedgerFactory(
                    runtime,
                    fixedGuard(7),
                    config(true),
                    new ManagedLedgerFactoryConfig(),
                    false);
            NereusManagedLedger ledger = (NereusManagedLedger) factory.open(NAME, config(true));
            metadata.failNext(FakeOxiaMetadataStore.FailurePoint.AFTER_HEAD_CAS_BEFORE_DERIVED_INDEX);
            AtomicInteger successes = new AtomicInteger();
            AtomicInteger failures = new AtomicInteger();
            CompletableFuture<Position> callback = new CompletableFuture<>();
            byte[] payload = "committed-before-response-loss".getBytes(StandardCharsets.UTF_8);

            ledger.asyncAddEntry(payload, new AddEntryCallback() {
                @Override
                public void addComplete(Position position, ByteBuf entryData, Object ctx) {
                    try {
                        successes.incrementAndGet();
                        assertThat(entryData.isReadOnly()).isTrue();
                        assertThat(entryData.readableBytes()).isEqualTo(payload.length);
                        callback.complete(position);
                    } catch (Throwable error) {
                        callback.completeExceptionally(error);
                    }
                }

                @Override
                public void addFailed(ManagedLedgerException exception, Object ctx) {
                    failures.incrementAndGet();
                    callback.completeExceptionally(exception);
                }
            }, null);

            Position recovered = callback.join();
            assertThat(successes).hasValue(1);
            assertThat(failures).hasValue(0);
            assertThat(recovered.getLedgerId()).isEqualTo(ledger.projection().virtualLedgerId());
            assertThat(recovered.getEntryId()).isZero();
            assertThat(ledger.currentWriteFence()).isEmpty();
            assertThat(ledger.getNumberOfEntries()).isEqualTo(1);
            Entry entry = read(ledger, recovered).join();
            assertThat(entry.getData()).isEqualTo(payload);
            entry.release();
            ledger.close();
            factory.shutdown();
        } finally {
            metadata.close();
            objectStore.close();
        }
    }

    @Test
    void closeTrimReopenTerminateDeleteAndRecreatePreservePositionAndObjectContracts()
            throws Exception {
        Clock clock = Clock.systemUTC();
        LocalFileObjectStore objectStore = new LocalFileObjectStore(root.resolve("lifecycle"));
        FakeOxiaMetadataStore metadata = new FakeOxiaMetadataStore(clock::millis);
        FakeManagedLedgerProjectionMetadataStore projections =
                new FakeManagedLedgerProjectionMetadataStore();
        DefaultStreamStorage storage = new DefaultStreamStorage(
                StreamStorageConfig.defaults("cluster/a", "pulsar-lifecycle"),
                metadata,
                new DefaultWalObjectWriter(objectStore, "lifecycle", clock),
                new DefaultWalObjectReader(objectStore),
                clock,
                Runnable::run);
        AtomicLong bindingGeneration = new AtomicLong(7);
        try (NereusManagedLedgerRuntime runtime =
                ManagedLedgerRuntimeTestSupport.runtime(storage, projections)) {
            NereusManagedLedgerFactory factory = new NereusManagedLedgerFactory(
                    runtime,
                    mutableGuard(bindingGeneration),
                    config(true),
                    new ManagedLedgerFactoryConfig(),
                    false);
            NereusManagedLedger first = (NereusManagedLedger) factory.open(NAME, config(true));
            Position trimmed0 = first.addEntry("trimmed-0".getBytes(StandardCharsets.UTF_8));
            Position trimmed1 = first.addEntry("trimmed-1".getBytes(StandardCharsets.UTF_8));
            Position retained = first.addEntry("retained-2".getBytes(StandardCharsets.UTF_8));
            long firstLedgerId = first.projection().virtualLedgerId();
            long firstIncarnation = first.projection().incarnation();
            StreamId firstStreamId = first.projection().streamId();
            first.close();

            storage.trim(firstStreamId, 2, new TrimOptions(Duration.ofSeconds(5), "f2 acceptance trim"))
                    .join();
            NereusManagedLedger reopened = (NereusManagedLedger) factory.open(NAME, config(true));
            assertThat(reopened).isNotSameAs(first);
            assertThat(reopened.projection().virtualLedgerId()).isEqualTo(firstLedgerId);
            assertThat(reopened.projection().incarnation()).isEqualTo(firstIncarnation);
            assertThat(reopened.getFirstPosition().getEntryId()).isEqualTo(1);
            assertThat(reopened.getLastConfirmedEntry()).isEqualTo(retained);
            assertThat(reopened.getNumberOfEntries()).isEqualTo(1);
            assertReadFails(reopened, trimmed0);
            assertReadFails(reopened, trimmed1);
            Entry retainedEntry = read(reopened, retained).join();
            assertThat(retainedEntry.getData()).isEqualTo("retained-2".getBytes(StandardCharsets.UTF_8));
            retainedEntry.release();

            ObjectSliceReadTarget retainedTarget = (ObjectSliceReadTarget) metadata.scanOffsetIndex(
                    "cluster/a", firstStreamId, 2, 1).join().getFirst().readTarget();
            long retainedObjectLength = objectStore.headObject(
                    retainedTarget.objectKey(), new HeadObjectOptions(Duration.ofSeconds(5))).join().objectLength();
            Position finalLac = reopened.terminate();
            assertThat(finalLac).isEqualTo(retained);
            assertThat(reopened.terminate()).isEqualTo(finalLac);
            assertThatThrownBy(() -> reopened.addEntry(new byte[] {9}))
                    .isInstanceOf(ManagedLedgerException.ManagedLedgerTerminatedException.class);

            reopened.delete();
            assertThat(objectStore.headObject(
                    retainedTarget.objectKey(), new HeadObjectOptions(Duration.ofSeconds(5))).join().objectLength())
                    .isEqualTo(retainedObjectLength);
            reopened.close();
            bindingGeneration.incrementAndGet();

            NereusManagedLedger recreated = (NereusManagedLedger) factory.open(NAME, config(true));
            assertThat(recreated.projection().incarnation()).isEqualTo(firstIncarnation + 1);
            assertThat(recreated.projection().streamId()).isNotEqualTo(firstStreamId);
            assertThat(recreated.projection().virtualLedgerId()).isNotEqualTo(firstLedgerId);
            assertReadFails(recreated, retained);
            Position newPosition = recreated.addEntry("new-incarnation-0".getBytes(StandardCharsets.UTF_8));
            assertThat(newPosition.getEntryId()).isZero();
            assertThat(newPosition.getLedgerId()).isEqualTo(recreated.projection().virtualLedgerId());
            assertThat(newPosition.getLedgerId()).isNotEqualTo(retained.getLedgerId());
            assertThat(objectStore.headObject(
                    retainedTarget.objectKey(), new HeadObjectOptions(Duration.ofSeconds(5))).join().objectLength())
                    .isEqualTo(retainedObjectLength);
            recreated.close();
            factory.shutdown();
        } finally {
            metadata.close();
            objectStore.close();
        }
    }

    @Test
    void remoteAppendWakesWaitingCursorThroughPollingWithMetadataWatchDisabled() throws Exception {
        Clock clock = Clock.systemUTC();
        LocalFileObjectStore objectStore = new LocalFileObjectStore(root.resolve("remote-poll"));
        FakeOxiaMetadataStore metadata = new FakeOxiaMetadataStore(clock::millis);
        FakeManagedLedgerProjectionMetadataStore.DurableState projectionState =
                new FakeManagedLedgerProjectionMetadataStore.DurableState();
        FakeManagedLedgerProjectionMetadataStore readerProjections =
                new FakeManagedLedgerProjectionMetadataStore(
                        projectionState, ProjectionMetadataStoreConfig.defaults(), clock);
        FakeManagedLedgerProjectionMetadataStore writerProjections =
                new FakeManagedLedgerProjectionMetadataStore(
                        projectionState, ProjectionMetadataStoreConfig.defaults(), clock);
        StreamStorageConfig readerConfig = StreamStorageConfig.defaults("cluster/a", "remote-poll-reader");
        StreamStorageConfig writerConfig = StreamStorageConfig.defaults("cluster/a", "remote-poll-writer");
        assertThat(readerConfig.enableMetadataWatch()).isFalse();
        assertThat(writerConfig.enableMetadataWatch()).isFalse();
        DefaultStreamStorage readerStorage = new DefaultStreamStorage(
                readerConfig,
                metadata,
                new DefaultWalObjectWriter(objectStore, "remote-poll-reader", clock),
                new DefaultWalObjectReader(objectStore),
                clock,
                Runnable::run);
        DefaultStreamStorage writerStorage = new DefaultStreamStorage(
                writerConfig,
                metadata,
                new DefaultWalObjectWriter(objectStore, "remote-poll-writer", clock),
                new DefaultWalObjectReader(objectStore),
                clock,
                Runnable::run);
        try (NereusManagedLedgerRuntime readerRuntime =
                        ManagedLedgerRuntimeTestSupport.runtime(readerStorage, readerProjections);
                NereusManagedLedgerRuntime writerRuntime =
                        ManagedLedgerRuntimeTestSupport.runtime(writerStorage, writerProjections)) {
            NereusManagedLedgerFactory readerFactory = new NereusManagedLedgerFactory(
                    readerRuntime, fixedGuard(7), config(true), new ManagedLedgerFactoryConfig(), false);
            NereusManagedLedgerFactory writerFactory = new NereusManagedLedgerFactory(
                    writerRuntime, fixedGuard(7), config(true), new ManagedLedgerFactoryConfig(), false);
            NereusManagedLedger reader = (NereusManagedLedger) readerFactory.open(NAME, config(true));
            NereusManagedLedger writer = (NereusManagedLedger) writerFactory.open(NAME, config(true));
            assertThat(reader.projection()).isEqualTo(writer.projection());
            ManagedCursor cursor = reader.newNonDurableCursor(PositionFactory.LATEST, "remote-reader");
            CompletableFuture<List<Entry>> waiting = readOrWait(cursor, 1);
            assertThat(waiting).isNotDone();
            byte[] payload = "remote-append-visible-by-poll".getBytes(StandardCharsets.UTF_8);

            Position remotePosition = writer.addEntry(payload);
            List<Entry> awakened = waiting.get(5, TimeUnit.SECONDS);

            assertThat(awakened).singleElement().satisfies(entry -> {
                assertThat(entry.getPosition()).isEqualTo(remotePosition);
                assertThat(entry.getData()).isEqualTo(payload);
            });
            awakened.forEach(Entry::release);
            assertThat(reader.currentMetadata().committedEndOffset()).isEqualTo(1);
            cursor.close();
            reader.close();
            writer.close();
            readerFactory.shutdown();
            writerFactory.shutdown();
        } finally {
            metadata.close();
            objectStore.close();
        }
    }

    @Test
    void objectUploadFailureReleasesFacadeAdmissionAndNextAppendStartsAtZero() throws Exception {
        Clock clock = Clock.systemUTC();
        LocalFileObjectStore objectStore = new LocalFileObjectStore(root.resolve("upload-recovery"));
        FakeOxiaMetadataStore metadata = new FakeOxiaMetadataStore(clock::millis);
        FakeManagedLedgerProjectionMetadataStore projections =
                new FakeManagedLedgerProjectionMetadataStore();
        FailFirstUploadWalWriter writer = new FailFirstUploadWalWriter(
                new DefaultWalObjectWriter(objectStore, "upload-recovery", clock));
        DefaultStreamStorage storage = new DefaultStreamStorage(
                StreamStorageConfig.defaults("cluster/a", "pulsar-upload-recovery"),
                metadata,
                writer,
                new DefaultWalObjectReader(objectStore),
                clock,
                Runnable::run);
        try (NereusManagedLedgerRuntime runtime =
                ManagedLedgerRuntimeTestSupport.runtime(storage, projections)) {
            NereusManagedLedgerFactory factory = new NereusManagedLedgerFactory(
                    runtime, fixedGuard(7), config(true), new ManagedLedgerFactoryConfig(), false);
            NereusManagedLedger ledger = (NereusManagedLedger) factory.open(NAME, config(true));
            AtomicInteger successes = new AtomicInteger();
            AtomicInteger failures = new AtomicInteger();
            CompletableFuture<ManagedLedgerException> failedCallback = new CompletableFuture<>();

            ledger.asyncAddEntry(new byte[] {1, 2, 3}, new AddEntryCallback() {
                @Override
                public void addComplete(Position position, ByteBuf entryData, Object ctx) {
                    successes.incrementAndGet();
                    failedCallback.completeExceptionally(new AssertionError(
                            "failed upload must not produce a Position"));
                }

                @Override
                public void addFailed(ManagedLedgerException exception, Object ctx) {
                    failures.incrementAndGet();
                    failedCallback.complete(exception);
                }
            }, null);

            assertThat(failedCallback.join()).isNotNull();
            assertThat(successes).hasValue(0);
            assertThat(failures).hasValue(1);
            assertThat(writer.uploadAttempts).hasValue(1);
            assertThat(ledger.currentWriteFence()).isEmpty();
            assertThat(ledger.getNumberOfEntries()).isZero();

            byte[] recoveredPayload = "append-after-upload-failure".getBytes(StandardCharsets.UTF_8);
            Position recovered = ledger.addEntry(recoveredPayload);
            assertThat(recovered.getEntryId()).isZero();
            assertThat(writer.uploadAttempts).hasValue(2);
            Entry entry = read(ledger, recovered).join();
            assertThat(entry.getData()).isEqualTo(recoveredPayload);
            entry.release();
            ledger.close();
            factory.shutdown();
        } finally {
            metadata.close();
            objectStore.close();
        }
    }

    @Test
    void appendCallbacksRemainInAdmissionOrderWhenL0CompletesOutOfOrder() throws Exception {
        OrderedAppendStorage storage = new OrderedAppendStorage();
        FakeManagedLedgerProjectionMetadataStore projections =
                new FakeManagedLedgerProjectionMetadataStore();
        try (NereusManagedLedgerRuntime runtime =
                ManagedLedgerRuntimeTestSupport.runtime(storage, projections)) {
            NereusManagedLedger ledger = new NereusManagedLedger(
                    runtime, opened(), config(true), () -> { });
            List<Long> callbackOrder = new CopyOnWriteArrayList<>();
            CompletableFuture<Void> callbacks = new CompletableFuture<>();
            AddEntryCallback callback = new AddEntryCallback() {
                @Override
                public void addComplete(Position position, ByteBuf entryData, Object ctx) {
                    assertThat(entryData.isReadOnly()).isTrue();
                    assertThat(entryData.readableBytes()).isEqualTo(3);
                    callbackOrder.add(position.getEntryId());
                    if (callbackOrder.size() == 2) {
                        callbacks.complete(null);
                    }
                }

                @Override
                public void addFailed(ManagedLedgerException exception, Object ctx) {
                    callbacks.completeExceptionally(exception);
                }
            };

            ledger.asyncAddEntry(new byte[] {1, 2, 3}, callback, null);
            ledger.asyncAddEntry(new byte[] {4, 5, 6}, callback, null);
            storage.second.complete(appendResult(1, 2, 6, 2));
            assertThat(callbackOrder).isEmpty();
            storage.first.complete(appendResult(0, 1, 3, 1));

            callbacks.join();
            assertThat(callbackOrder).containsExactly(0L, 1L);
            assertThat(ledger.getStats().getAddEntrySucceedTotal()).isEqualTo(2);
            ledger.close();
        }
    }

    private static CompletableFuture<Entry> read(ManagedLedger ledger, Position position) {
        CompletableFuture<Entry> result = new CompletableFuture<>();
        ledger.asyncReadEntry(position, new ReadEntryCallback() {
            @Override
            public void readEntryComplete(Entry entry, Object ctx) {
                result.complete(entry);
            }

            @Override
            public void readEntryFailed(ManagedLedgerException exception, Object ctx) {
                result.completeExceptionally(exception);
            }
        }, null);
        return result;
    }

    private static void assertCursorBoundary(
            ManagedCursor cursor, long expectedReadEntryId, long expectedMarkDeleteEntryId) {
        assertThat(cursor.getReadPosition().getEntryId()).isEqualTo(expectedReadEntryId);
        assertThat(cursor.getMarkDeletedPosition().getEntryId()).isEqualTo(expectedMarkDeleteEntryId);
    }

    private static CompletableFuture<Position> resetCursorAsync(
            ManagedCursor cursor, Position position, boolean force) {
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

    private static CompletableFuture<List<Entry>> readOrWait(ManagedCursor cursor, int count) {
        CompletableFuture<List<Entry>> result = new CompletableFuture<>();
        cursor.asyncReadEntriesOrWait(count, new org.apache.bookkeeper.mledger.AsyncCallbacks.ReadEntriesCallback() {
            @Override public void readEntriesComplete(List<Entry> entries, Object ctx) { result.complete(entries); }
            @Override public void readEntriesFailed(ManagedLedgerException exception, Object ctx) { result.completeExceptionally(exception); }
        }, null, null);
        return result;
    }

    private static CompletableFuture<Entry> read(ReadOnlyManagedLedger ledger, Position position) {
        CompletableFuture<Entry> result = new CompletableFuture<>();
        ledger.asyncReadEntry(position, new ReadEntryCallback() {
            @Override public void readEntryComplete(Entry entry, Object ctx) { result.complete(entry); }
            @Override public void readEntryFailed(ManagedLedgerException exception, Object ctx) {
                result.completeExceptionally(exception);
            }
        }, null);
        return result;
    }

    private static CompletableFuture<ReadOnlyManagedLedger> openReadOnly(
            NereusManagedLedgerFactory factory, String name, ManagedLedgerConfig config) {
        CompletableFuture<ReadOnlyManagedLedger> result = new CompletableFuture<>();
        factory.asyncOpenReadOnlyManagedLedger(name, new OpenReadOnlyManagedLedgerCallback() {
            @Override
            public void openReadOnlyManagedLedgerComplete(ReadOnlyManagedLedger managedLedger, Object ctx) {
                result.complete(managedLedger);
            }

            @Override
            public void openReadOnlyManagedLedgerFailed(ManagedLedgerException exception, Object ctx) {
                result.completeExceptionally(exception);
            }
        }, config, null);
        return result;
    }

    private static ManagedLedgerConfig config(boolean createIfMissing) {
        ManagedLedgerConfig config = new ManagedLedgerConfig();
        config.setStorageClassName("nereus");
        config.setCreateIfMissing(createIfMissing);
        config.setProperties(Map.of("source", "test"));
        return config;
    }

    private static NereusCreationGuard fixedGuard(long generation) {
        return name -> CompletableFuture.completedFuture(new NereusCreationPermit() {
            @Override public String persistenceName() { return name; }
            @Override public long bindingGeneration() { return generation; }
            @Override public CompletableFuture<Void> validateBeforeProjectionPublish() {
                return CompletableFuture.completedFuture(null);
            }
        });
    }

    private static NereusCreationGuard mutableGuard(AtomicLong generation) {
        return name -> {
            long acquiredGeneration = generation.get();
            return CompletableFuture.completedFuture(new NereusCreationPermit() {
                @Override public String persistenceName() { return name; }
                @Override public long bindingGeneration() { return acquiredGeneration; }
                @Override public CompletableFuture<Void> validateBeforeProjectionPublish() {
                    return generation.get() == acquiredGeneration
                            ? CompletableFuture.completedFuture(null)
                            : CompletableFuture.failedFuture(new IllegalStateException(
                                    "binding generation changed before projection publish"));
                }
            });
        };
    }

    private static void assertReadFails(ManagedLedger ledger, Position position) {
        assertThatThrownBy(() -> read(ledger, position).join())
                .isInstanceOf(CompletionException.class)
                .hasCauseInstanceOf(ManagedLedgerException.class);
    }

    private static NereusLedgerOpenResult opened() {
        StreamMetadata metadata = new StreamMetadata(
                ManagedLedgerProjectionNames.streamId(NAME, 1),
                ManagedLedgerProjectionNames.streamName(NAME, 1),
                StreamState.ACTIVE,
                StorageProfile.OBJECT_WAL_SYNC_OBJECT,
                Map.of(ManagedLedgerProjectionNames.PAYLOAD_MAPPING_ATTRIBUTE,
                        ManagedLedgerProjectionNames.PAYLOAD_MAPPING_V1),
                1,
                1,
                0,
                0,
                0);
        TopicProjectionRecord topic = new TopicProjectionRecord(
                NAME,
                ManagedLedgerProjectionNames.managedLedgerNameHash(NAME),
                7,
                1,
                metadata.streamName().value(),
                metadata.streamId().value(),
                ManagedLedgerProjectionNames.STORAGE_CLASS,
                StorageProfile.OBJECT_WAL_SYNC_OBJECT.name(),
                ManagedLedgerProjectionNames.MIN_VIRTUAL_LEDGER_ID + 123,
                ManagedLedgerProjectionNames.POSITION_MAPPING_VERSION,
                ManagedLedgerProjectionNames.PAYLOAD_MAPPING_V1,
                ManagedLedgerFacadeState.OPEN.name(),
                Map.of(),
                1,
                0,
                0);
        return new NereusLedgerOpenResult(
                topic,
                new com.nereusstream.managedledger.projection.VirtualLedgerProjection(
                        metadata.streamId(), NAME, 7, 1, topic.virtualLedgerId(),
                        topic.positionMappingVersion(), topic.payloadMapping(), 1, 0),
                metadata);
    }

    private static AppendResult appendResult(
            long start, long end, long cumulativeSize, long commitVersion) {
        EntryIndexRef index = new EntryIndexRef(
                EntryIndexLocation.OBJECT_FOOTER,
                Optional.empty(), Optional.empty(), Optional.empty(),
                0, 1, new Checksum(ChecksumType.CRC32C, "11111111"));
        return new AppendResult(
                ManagedLedgerProjectionNames.streamId(NAME, 1),
                new OffsetRange(start, end),
                end,
                cumulativeSize,
                0,
                new ObjectSliceReadTarget(
                        1,
                        new ObjectId("object-" + start),
                        new ObjectKey("key-" + start),
                        ObjectType.MULTI_STREAM_WAL_OBJECT,
                        "WAL_OBJECT_V1",
                        "OPAQUE_SLICE",
                        "slice",
                        0,
                        1,
                        new Checksum(ChecksumType.CRC32C, "22222222"),
                        index),
                PayloadFormat.OPAQUE_RECORD_BATCH,
                1,
                1,
                3,
                List.of(),
                Optional.empty(),
                commitVersion);
    }

    private static final class FenceStreamStorage implements StreamStorage {
        private static final AppendAttemptId ATTEMPT = new AppendAttemptId("fence-attempt");
        private final AtomicInteger recoveries = new AtomicInteger();
        private final CompletableFuture<AppendResult> terminalRecovery = new CompletableFuture<>();

        @Override
        public CompletableFuture<AppendResult> append(
                StreamId streamId, AppendBatch batch, AppendOptions options) {
            return CompletableFuture.failedFuture(new NereusException(
                    ErrorCode.TIMEOUT,
                    true,
                    "uncertain append",
                    null,
                    AppendOutcome.MAY_HAVE_COMMITTED,
                    ATTEMPT));
        }

        @Override
        public CompletableFuture<AppendResult> recoverAppend(
                StreamId streamId, AppendAttemptId attemptId, AppendRecoveryOptions options) {
            if (recoveries.getAndIncrement() == 0) {
                return CompletableFuture.failedFuture(new NereusException(
                        ErrorCode.METADATA_UNAVAILABLE,
                        true,
                        "callback recovery remains uncertain",
                        null,
                        AppendOutcome.MAY_HAVE_COMMITTED,
                        ATTEMPT));
            }
            return terminalRecovery;
        }

        @Override public CompletableFuture<StreamMetadata> createOrGetStream(StreamName name, StreamCreateOptions options) { return unsupported(); }
        @Override public CompletableFuture<AppendSession> acquireAppendSession(StreamId id, AppendSessionOptions options) { return unsupported(); }
        @Override public CompletableFuture<ReadResult> read(StreamId id, long offset, ReadOptions options) { return unsupported(); }
        @Override public CompletableFuture<ResolveResult> resolve(StreamId id, long offset, ResolveOptions options) { return unsupported(); }
        @Override public CompletableFuture<Void> trim(StreamId id, long offset, TrimOptions options) { return unsupported(); }
        @Override public CompletableFuture<StreamMetadata> getStreamMetadata(StreamId id) { return unsupported(); }
        @Override public CompletableFuture<StreamMetadata> seal(StreamId id, SealOptions options) { return unsupported(); }
        @Override public CompletableFuture<StreamMetadata> delete(StreamId id, DeleteOptions options) { return unsupported(); }
        @Override public void close() { }

        private static <T> CompletableFuture<T> unsupported() {
            return CompletableFuture.failedFuture(new UnsupportedOperationException());
        }
    }

    private static final class OrderedAppendStorage implements StreamStorage {
        private final AtomicInteger appends = new AtomicInteger();
        private final CompletableFuture<AppendResult> first = new CompletableFuture<>();
        private final CompletableFuture<AppendResult> second = new CompletableFuture<>();

        @Override
        public CompletableFuture<AppendResult> append(
                StreamId streamId, AppendBatch batch, AppendOptions options) {
            return appends.getAndIncrement() == 0 ? first : second;
        }

        @Override public CompletableFuture<AppendResult> recoverAppend(StreamId id, AppendAttemptId attempt, AppendRecoveryOptions options) { return unsupported(); }
        @Override public CompletableFuture<StreamMetadata> createOrGetStream(StreamName name, StreamCreateOptions options) { return unsupported(); }
        @Override public CompletableFuture<AppendSession> acquireAppendSession(StreamId id, AppendSessionOptions options) { return unsupported(); }
        @Override public CompletableFuture<ReadResult> read(StreamId id, long offset, ReadOptions options) { return unsupported(); }
        @Override public CompletableFuture<ResolveResult> resolve(StreamId id, long offset, ResolveOptions options) { return unsupported(); }
        @Override public CompletableFuture<Void> trim(StreamId id, long offset, TrimOptions options) { return unsupported(); }
        @Override public CompletableFuture<StreamMetadata> getStreamMetadata(StreamId id) { return unsupported(); }
        @Override public CompletableFuture<StreamMetadata> seal(StreamId id, SealOptions options) { return unsupported(); }
        @Override public CompletableFuture<StreamMetadata> delete(StreamId id, DeleteOptions options) { return unsupported(); }
        @Override public void close() { }

        private static <T> CompletableFuture<T> unsupported() {
            return CompletableFuture.failedFuture(new UnsupportedOperationException());
        }
    }

    private static final class FailFirstUploadWalWriter implements WalObjectWriter {
        private final WalObjectWriter delegate;
        private final AtomicBoolean fail = new AtomicBoolean(true);
        private final AtomicInteger uploadAttempts = new AtomicInteger();

        private FailFirstUploadWalWriter(WalObjectWriter delegate) {
            this.delegate = delegate;
        }

        @Override
        public PreparedWalObject prepare(WalWriteRequest request) {
            return delegate.prepare(request);
        }

        @Override
        public CompletableFuture<WalWriteResult> upload(PreparedWalObject preparedObject) {
            uploadAttempts.incrementAndGet();
            if (fail.compareAndSet(true, false)) {
                return CompletableFuture.failedFuture(new NereusException(
                        ErrorCode.OBJECT_UPLOAD_FAILED, true, "injected first Object WAL upload failure"));
            }
            return delegate.upload(preparedObject);
        }
    }
}
