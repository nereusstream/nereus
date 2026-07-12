/* Licensed under the Apache License, Version 2.0 */
package com.nereusstream.managedledger;

import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.assertThatThrownBy;

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
import com.nereusstream.metadata.oxia.testing.FakeOxiaMetadataStore;
import com.nereusstream.metadata.oxia.records.TopicProjectionRecord;
import com.nereusstream.objectstore.testing.LocalFileObjectStore;
import com.nereusstream.objectstore.wal.DefaultWalObjectReader;
import com.nereusstream.objectstore.wal.DefaultWalObjectWriter;
import java.nio.charset.StandardCharsets;
import java.nio.file.Path;
import java.time.Clock;
import java.util.List;
import java.util.Map;
import java.util.Optional;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.CompletionException;
import java.util.concurrent.atomic.AtomicInteger;
import io.netty.buffer.ByteBuf;
import org.apache.bookkeeper.mledger.AsyncCallbacks.AddEntryCallback;
import org.apache.bookkeeper.mledger.AsyncCallbacks.ReadEntryCallback;
import org.apache.bookkeeper.mledger.Entry;
import org.apache.bookkeeper.mledger.ManagedLedger;
import org.apache.bookkeeper.mledger.ManagedLedgerConfig;
import org.apache.bookkeeper.mledger.ManagedLedgerException;
import org.apache.bookkeeper.mledger.ManagedLedgerFactoryConfig;
import org.apache.bookkeeper.mledger.Position;
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

            NereusManagedLedger ledger = (NereusManagedLedger) factory.open(NAME, config(true));
            assertThat(factory.open(NAME, config(true))).isSameAs(ledger);
            byte[] payload = "pulsar-entry".getBytes(StandardCharsets.UTF_8);
            Position position = ledger.addEntry(payload, 3);

            assertThat(position.getLedgerId()).isEqualTo(ledger.projection().virtualLedgerId());
            assertThat(position.getEntryId()).isZero();
            assertThat(ledger.getNumberOfEntries()).isEqualTo(1);
            assertThat(ledger.getTotalSize()).isEqualTo(payload.length);
            assertThat(ledger.getLastConfirmedEntry()).isEqualTo(position);
            assertThat(ledger.getLedgersInfo().get(position.getLedgerId()).getEntries()).isEqualTo(1);
            assertThat(factory.getCacheStats().getCacheUsedSize()).isZero();
            assertThat(factory.getEntryCacheManager().getSize()).isZero();

            Entry entry = read(ledger, position).join();
            assertThat(entry.getData()).isEqualTo(payload);
            assertThat(entry.release()).isTrue();

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
            ledger.close();
            assertThat(factory.getManagedLedgers()).isEmpty();
            factory.shutdown();
        } finally {
            metadata.close();
            objectStore.close();
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
                    callbackFailure.complete(exception);
                }
            }, null);

            assertThat(callbackFailure.join()).isNotNull();
            NereusWriteFenceSnapshot fence = ledger.currentWriteFence().orElseThrow();
            CompletableFuture<NereusWriteFenceResolution> terminal =
                    ledger.awaitWriteFence(fence.generation());
            assertThat(terminal).isNotDone();

            storage.terminalRecovery.complete(appendResult());

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

    private static AppendResult appendResult() {
        EntryIndexRef index = new EntryIndexRef(
                EntryIndexLocation.OBJECT_FOOTER,
                Optional.empty(), Optional.empty(), Optional.empty(),
                0, 1, new Checksum(ChecksumType.CRC32C, "11111111"));
        return new AppendResult(
                ManagedLedgerProjectionNames.streamId(NAME, 1),
                new OffsetRange(0, 1),
                1,
                3,
                0,
                new ObjectSliceReadTarget(
                        1,
                        new ObjectId("object"),
                        new ObjectKey("key"),
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
                1);
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
}
