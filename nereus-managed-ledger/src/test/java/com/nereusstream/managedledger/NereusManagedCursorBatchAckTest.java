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
import io.netty.buffer.ByteBuf;
import io.netty.buffer.Unpooled;
import java.nio.file.Path;
import java.time.Clock;
import java.util.ArrayList;
import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.concurrent.CompletableFuture;
import org.apache.bookkeeper.mledger.AsyncCallbacks;
import org.apache.bookkeeper.mledger.Entry;
import org.apache.bookkeeper.mledger.ManagedCursor;
import org.apache.bookkeeper.mledger.ManagedLedger;
import org.apache.bookkeeper.mledger.ManagedLedgerConfig;
import org.apache.bookkeeper.mledger.ManagedLedgerException;
import org.apache.bookkeeper.mledger.ManagedLedgerFactoryConfig;
import org.apache.bookkeeper.mledger.Position;
import org.apache.bookkeeper.mledger.impl.AckSetStateUtil;
import org.apache.pulsar.common.api.proto.MessageMetadata;
import org.apache.pulsar.common.protocol.Commands;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.io.TempDir;

class NereusManagedCursorBatchAckTest {
    private static final String NAME = "tenant/ns/persistent/cursor-batch-ack";

    @TempDir
    Path root;

    @Test
    void partialBatchAckPersistsMergesAndExposesDefensiveRemainingWords() throws Exception {
        Clock clock = Clock.systemUTC();
        LocalFileObjectStore objectStore = new LocalFileObjectStore(root);
        FakeOxiaMetadataStore metadata = new FakeOxiaMetadataStore(clock::millis);
        FakeManagedLedgerProjectionMetadataStore projections =
                new FakeManagedLedgerProjectionMetadataStore();
        DefaultStreamStorage streamStorage = new DefaultStreamStorage(
                StreamStorageConfig.defaults("cluster/a", "cursor-batch-ack-test"),
                metadata,
                new DefaultWalObjectWriter(objectStore, "cursor-batch-ack-test", clock),
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
            Position batch = ledger.addEntry(batchEntry(4), 4);
            Position ordinary = ledger.addEntry(new byte[] {9});

            ManagedCursor individual = ledger.openCursor("individual");
            individual.delete(batchPosition(batch, 0b1110));
            assertThat(individual.isMessageDeleted(batch)).isFalse();
            long[] exposed = individual.getBatchPositionAckSet(batch);
            assertThat(exposed).containsExactly(0b1110L);
            exposed[0] = 0;
            assertThat(individual.getDeletedBatchIndexesAsLongArray(batch))
                    .containsExactly(0b1110L);

            individual.close();
            individual = ledger.openCursor("individual");
            assertThat(individual.getBatchPositionAckSet(batch)).containsExactly(0b1110L);
            individual.delete(batchPosition(batch, 0b1101));
            assertThat(individual.getBatchPositionAckSet(batch)).containsExactly(0b1100L);

            List<Entry> liveRead = individual.readEntries(1);
            assertThat(liveRead).singleElement()
                    .satisfies(entry -> assertThat(entry.getPosition()).isEqualTo(batch));
            liveRead.forEach(Entry::release);
            List<Entry> replayed = individual.replayEntries(Set.of(batch));
            assertThat(replayed).singleElement()
                    .satisfies(entry -> assertThat(entry.getPosition()).isEqualTo(batch));
            replayed.forEach(Entry::release);

            individual.delete(batchPosition(batch, 0));
            assertThat(individual.isMessageDeleted(batch)).isTrue();
            assertThat(individual.getBatchPositionAckSet(batch)).isNull();
            assertThat(individual.readEntries(1)).singleElement()
                    .satisfies(entry -> {
                        assertThat(entry.getPosition()).isEqualTo(ordinary);
                        entry.release();
                    });
            assertWholeEntryReplayIsSkipped(individual, batch);

            Entry direct = read(ledger, batch).join();
            List<Entry> candidates = new ArrayList<>(List.of(direct));
            individual.trimDeletedEntries(candidates);
            assertThat(candidates).isEmpty();
            assertThat(direct.release()).isFalse();

            ManagedCursor cumulative = ledger.openCursor("cumulative");
            cumulative.markDelete(batchPosition(batch, 0b1100));
            assertThat(cumulative.getMarkDeletedPosition().getEntryId()).isEqualTo(-1);
            assertThat(cumulative.getBatchPositionAckSet(batch)).containsExactly(0b1100L);
            cumulative.markDelete(batchPosition(batch, 0));
            assertThat(cumulative.getMarkDeletedPosition()).isEqualTo(batch);
            assertThat(cumulative.getBatchPositionAckSet(batch)).isNull();

            assertThatThrownBy(() -> cumulative.delete(batchPosition(ordinary, 1)))
                    .isInstanceOf(ManagedLedgerException.InvalidCursorPositionException.class);

            ledger.close();
            factory.shutdown();
        } finally {
            metadata.close();
            objectStore.close();
        }
    }

    private static void assertWholeEntryReplayIsSkipped(
            ManagedCursor cursor,
            Position position) {
        CompletableFuture<List<Entry>> callback = new CompletableFuture<>();
        Set<? extends Position> skipped = cursor.asyncReplayEntries(
                Set.of(position),
                new AsyncCallbacks.ReadEntriesCallback() {
                    @Override
                    public void readEntriesComplete(List<Entry> entries, Object ctx) {
                        callback.complete(entries);
                    }

                    @Override
                    public void readEntriesFailed(ManagedLedgerException exception, Object ctx) {
                        callback.completeExceptionally(exception);
                    }
                },
                null);
        assertThat(skipped).isEqualTo(Set.of(position));
        assertThat(callback.join()).isEmpty();
    }

    private static CompletableFuture<Entry> read(ManagedLedger ledger, Position position) {
        CompletableFuture<Entry> result = new CompletableFuture<>();
        ledger.asyncReadEntry(position, new AsyncCallbacks.ReadEntryCallback() {
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

    private static Position batchPosition(Position position, long remainingWords) {
        return AckSetStateUtil.createPositionWithAckSet(
                position.getLedgerId(), position.getEntryId(), new long[] {remainingWords});
    }

    private static byte[] batchEntry(int batchSize) {
        MessageMetadata metadata = new MessageMetadata()
                .setProducerName("batch-producer")
                .setSequenceId(1)
                .setPublishTime(1)
                .setNumMessagesInBatch(batchSize);
        ByteBuf payload = Unpooled.wrappedBuffer(new byte[] {1, 2, 3, 4});
        ByteBuf serialized = Commands.serializeMetadataAndPayload(
                Commands.ChecksumType.Crc32c, metadata, payload);
        try {
            byte[] bytes = new byte[serialized.readableBytes()];
            serialized.getBytes(serialized.readerIndex(), bytes);
            return bytes;
        } finally {
            serialized.release();
            payload.release();
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
