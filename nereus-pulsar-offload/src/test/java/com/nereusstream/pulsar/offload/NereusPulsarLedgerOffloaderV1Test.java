/*
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 *   https://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

package com.nereusstream.pulsar.offload;

import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.assertThatThrownBy;
import com.nereusstream.pulsar.offload.NereusPulsarLedgerOffloaderV1.RuntimePolicy;
import com.nereusstream.pulsar.offload.PulsarOffloadObjectStoreV1.Body;
import com.nereusstream.pulsar.offload.PulsarOffloadObjectStoreV1.Capabilities;
import com.nereusstream.pulsar.offload.PulsarOffloadObjectStoreV1.FailureKind;
import com.nereusstream.pulsar.offload.PulsarOffloadObjectStoreV1.ImmutableObject;
import com.nereusstream.pulsar.offload.PulsarOffloadObjectStoreV1.ObjectStoreException;
import com.nereusstream.pulsar.offload.PulsarSealedLedgerAttemptV1.RetentionClass;
import com.nereusstream.pulsar.offload.npd1.Npd1CodecV1.CompressionFamily;
import io.netty.buffer.Unpooled;
import java.io.IOException;
import java.nio.file.Files;
import java.nio.file.Path;
import java.security.MessageDigest;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.HashMap;
import java.util.HexFormat;
import java.util.LinkedHashMap;
import java.util.List;
import java.util.Map;
import java.util.UUID;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.CompletionStage;
import java.util.concurrent.atomic.AtomicInteger;
import javax.crypto.SecretKey;
import javax.crypto.spec.SecretKeySpec;
import org.apache.bookkeeper.client.LedgerMetadataBuilder;
import org.apache.bookkeeper.client.api.DigestType;
import org.apache.bookkeeper.client.api.LastConfirmedAndEntry;
import org.apache.bookkeeper.client.api.LedgerEntries;
import org.apache.bookkeeper.client.api.LedgerEntry;
import org.apache.bookkeeper.client.api.LedgerMetadata;
import org.apache.bookkeeper.client.api.ReadHandle;
import org.apache.bookkeeper.client.impl.LedgerEntriesImpl;
import org.apache.bookkeeper.client.impl.LedgerEntryImpl;
import org.apache.bookkeeper.mledger.SourceSafeLedgerOffloader;
import org.apache.bookkeeper.net.BookieId;
import org.apache.pulsar.common.policies.data.OffloadPoliciesImpl;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.io.TempDir;

class NereusPulsarLedgerOffloaderV1Test {
    private static final long LEDGER_ID = 42;
    private static final UUID ATTEMPT = UUID.fromString("123e4567-e89b-12d3-a456-426614174000");
    private static final SecretKey KEY = new SecretKeySpec(new byte[32], "AES");
    private static final List<byte[]> PAYLOADS = List.of(
            new byte[] {0, 1, 2}, new byte[] {3, 4, 5}, new byte[] {6, 7, 8}, new byte[] {9, 10, 11}, new byte[] {
                12, 13, 14
            });
    private static final PulsarOffloadLimitCandidateV1 LIMITS = new PulsarOffloadLimitCandidateV1(
            PulsarOffloadLimitCandidateV1.FOUR_GIB,
            1_024,
            16L * PulsarOffloadLimitCandidateV1.MIB,
            16L * PulsarOffloadLimitCandidateV1.MIB,
            2,
            List.of(
                    PulsarOffloadLimitCandidateV1.MIB,
                    4 * PulsarOffloadLimitCandidateV1.MIB,
                    8 * PulsarOffloadLimitCandidateV1.MIB,
                    16 * PulsarOffloadLimitCandidateV1.MIB));

    @TempDir
    Path temporaryDirectory;

    @Test
    void offloadsThroughBoundedNativeBatchesThenReadsRevalidatesAndDeletes() throws Exception {
        InMemoryStore store = new InMemoryStore();
        AtomicInteger integritySignals = new AtomicInteger();
        NereusPulsarLedgerOffloaderV1 offloader = offloader(store, integritySignals);
        FakeReadHandle source = new FakeReadHandle(PAYLOADS);

        offloader
                .offload(source, ATTEMPT, Map.of("ManagedLedgerName", "tenant/ns/topic"))
                .join();

        assertThat(source.batchRequests).hasValue(3);
        assertThat(source.maximumRequestedEntries).hasValue(LIMITS.maxEntriesPerBlock());
        assertThat(source.maximumRequestedBytes).hasValue(Math.toIntExact(LIMITS.maxDecodedBlockBytes()));
        assertThat(store.rangeReads).isPositive();
        try (var staged = Files.list(temporaryDirectory)) {
            assertThat(staged).isEmpty();
        }

        Map<String, String> persisted = offloader.getOffloadDriverMetadata();
        assertThat(persisted)
                .containsEntry(NereusPulsarLedgerOffloaderV1.METADATA_KEY_DERIVATION_VERSION, "1")
                .containsEntry(NereusPulsarLedgerOffloaderV1.METADATA_BLOCK_TARGET, "1048576")
                .containsEntry(NereusPulsarLedgerOffloaderV1.METADATA_COMPRESSION, "NONE");
        ReadHandle object =
                offloader.readOffloaded(LEDGER_ID, ATTEMPT, persisted).join();
        assertThat(object.getId()).isEqualTo(LEDGER_ID);
        assertThat(object.getLastAddConfirmed()).isEqualTo(4);
        assertThat(object.getLength()).isEqualTo(15);
        assertThat(object.getLedgerMetadata().getCToken()).isEqualTo(7);
        List<byte[]> actualPayloads = readPayloads(object, 0, 4);
        assertThat(actualPayloads).hasSameSizeAs(PAYLOADS);
        for (int index = 0; index < PAYLOADS.size(); index++) {
            assertThat(actualPayloads.get(index)).containsExactly(PAYLOADS.get(index));
        }
        object.closeAsync().join();

        offloader
                .revalidateOffloadedForSourceDeletion(LEDGER_ID, ATTEMPT, persisted, 4, 5, 15)
                .join();
        assertThatThrownBy(() -> offloader
                        .revalidateOffloadedForSourceDeletion(LEDGER_ID, ATTEMPT, persisted, 4, 5, 16)
                        .join())
                .hasRootCauseMessage("native deletion facts differ from NPO1");

        store.calls.clear();
        offloader.deleteOffloaded(LEDGER_ID, ATTEMPT, persisted).join();
        PulsarOffloadKeysV1 keys = PulsarOffloadKeysV1.derive("cells/pulsar-a", LEDGER_ID, ATTEMPT);
        assertThat(store.calls)
                .containsExactly(
                        "delete:" + keys.rootKey(), "delete:" + keys.dataKey(), "cleanup:" + keys.attemptPrefix());
        assertThat(store.objects).doesNotContainKeys(keys.rootKey(), keys.dataKey());
        assertThat(integritySignals).hasValue(0);
    }

    @Test
    void rejectsPersistedPolicyDriftBeforeProviderIoAndFailsClosedAfterClose() {
        InMemoryStore store = new InMemoryStore();
        NereusPulsarLedgerOffloaderV1 offloader = offloader(store, new AtomicInteger());
        offloader
                .offload(new FakeReadHandle(PAYLOADS), ATTEMPT, Map.of("ManagedLedgerName", "tenant/ns/topic"))
                .join();
        int callsBefore = store.calls.size();
        Map<String, String> changed = new HashMap<>(offloader.getOffloadDriverMetadata());
        changed.put(NereusPulsarLedgerOffloaderV1.METADATA_KEY_DERIVATION_VERSION, "2");

        assertThatThrownBy(() ->
                        offloader.readOffloaded(LEDGER_ID, ATTEMPT, changed).join())
                .hasRootCauseMessage("persisted Nereus offload policy is not supported by this provider");
        assertThat(store.calls).hasSize(callsBefore);

        offloader.close();
        assertThatThrownBy(() -> offloader
                        .readOffloaded(LEDGER_ID, ATTEMPT, offloader.getOffloadDriverMetadata())
                        .join())
                .hasRootCauseMessage("Nereus Pulsar offloader is closed");
        assertThatThrownBy(() -> offloader
                        .deleteOffloaded(LEDGER_ID, ATTEMPT, offloader.getOffloadDriverMetadata())
                        .join())
                .hasRootCauseMessage("Nereus Pulsar offloader is closed");
    }

    @Test
    void classifiesCorruptNativeRootAndExposesQuarantineObserver() {
        InMemoryStore store = new InMemoryStore();
        AtomicInteger integritySignals = new AtomicInteger();
        NereusPulsarLedgerOffloaderV1 offloader = offloader(store, integritySignals);
        offloader
                .offload(new FakeReadHandle(PAYLOADS), ATTEMPT, Map.of("ManagedLedgerName", "tenant/ns/topic"))
                .join();
        PulsarOffloadKeysV1 keys = PulsarOffloadKeysV1.derive("cells/pulsar-a", LEDGER_ID, ATTEMPT);
        store.flipByteWithoutChangingProof(keys.rootKey());

        Throwable failure =
                failureOf(offloader.readOffloaded(LEDGER_ID, ATTEMPT, offloader.getOffloadDriverMetadata()));
        assertThat(offloader.classifyOffloadedReadFailure(failure))
                .isEqualTo(SourceSafeLedgerOffloader.ReadFailureKind.INTEGRITY);
        offloader.recordOffloadedReadIntegrityFailure(LEDGER_ID, ATTEMPT, failure);
        assertThat(integritySignals).hasValue(1);
    }

    private NereusPulsarLedgerOffloaderV1 offloader(InMemoryStore store, AtomicInteger integritySignals) {
        RuntimePolicy policy = new RuntimePolicy(
                "m2-pulsar-v1",
                "cells/pulsar-a",
                RetentionClass.DELETE_AFTER_VERIFIED,
                PulsarOffloadLimitCandidateV1.MIB,
                CompressionFamily.NONE,
                LIMITS);
        return new NereusPulsarLedgerOffloaderV1(
                store,
                policy,
                (ledgerId, attemptUuid, metadata) -> KEY,
                (ledgerId, attemptUuid, failure) -> integritySignals.incrementAndGet(),
                new OffloadPoliciesImpl(),
                Runnable::run,
                temporaryDirectory);
    }

    private static List<byte[]> readPayloads(ReadHandle handle, long first, long last) throws Exception {
        List<byte[]> payloads = new ArrayList<>();
        try (LedgerEntries entries = handle.readAsync(first, last).join()) {
            entries.forEach(entry -> payloads.add(entry.getEntryBytes()));
        }
        return payloads;
    }

    private static Throwable failureOf(CompletionStage<?> stage) {
        try {
            stage.toCompletableFuture().join();
            throw new AssertionError("expected native offload failure");
        } catch (java.util.concurrent.CompletionException failure) {
            return failure.getCause();
        }
    }

    private static final class FakeReadHandle implements ReadHandle {
        private final List<byte[]> payloads;
        private final LedgerMetadata metadata;
        private final AtomicInteger batchRequests = new AtomicInteger();
        private final AtomicInteger maximumRequestedEntries = new AtomicInteger();
        private final AtomicInteger maximumRequestedBytes = new AtomicInteger();

        private FakeReadHandle(List<byte[]> payloads) {
            this.payloads = payloads.stream().map(byte[]::clone).toList();
            long length =
                    this.payloads.stream().mapToLong(payload -> payload.length).sum();
            Map<String, byte[]> customMetadata = new LinkedHashMap<>();
            customMetadata.put("binary", new byte[] {0, (byte) 0xff});
            metadata = LedgerMetadataBuilder.create()
                    .withId(LEDGER_ID)
                    .withMetadataFormatVersion(2)
                    .withEnsembleSize(3)
                    .withWriteQuorumSize(3)
                    .withAckQuorumSize(2)
                    .withDigestType(DigestType.CRC32C)
                    .withPassword(new byte[] {1, 2, 3})
                    .withClosedState()
                    .withLastEntryId(payloads.size() - 1L)
                    .withLength(length)
                    .withCustomMetadata(customMetadata)
                    .withCreationTime(100)
                    .storingCreationTime(true)
                    .withCToken(7)
                    .newEnsembleEntry(
                            0,
                            List.of(
                                    BookieId.parse("127.0.0.1:3181"),
                                    BookieId.parse("127.0.0.2:3181"),
                                    BookieId.parse("127.0.0.3:3181")))
                    .build();
        }

        @Override
        public CompletableFuture<LedgerEntries> batchReadAsync(long startEntryId, int maxCount, long maxSize) {
            batchRequests.incrementAndGet();
            maximumRequestedEntries.accumulateAndGet(maxCount, Math::max);
            maximumRequestedBytes.accumulateAndGet(Math.toIntExact(maxSize), Math::max);
            List<LedgerEntry> entries = new ArrayList<>();
            long bytes = 0;
            for (long entryId = startEntryId; entryId < payloads.size() && entries.size() < maxCount; entryId++) {
                byte[] payload = payloads.get(Math.toIntExact(entryId));
                if (!entries.isEmpty() && bytes + payload.length > maxSize) {
                    break;
                }
                bytes += payload.length;
                entries.add(LedgerEntryImpl.create(
                        LEDGER_ID, entryId, payload.length, Unpooled.wrappedBuffer(payload.clone())));
            }
            return CompletableFuture.completedFuture(LedgerEntriesImpl.create(entries));
        }

        @Override
        public CompletableFuture<LedgerEntries> readAsync(long firstEntry, long lastEntry) {
            List<LedgerEntry> entries = new ArrayList<>();
            for (long entryId = firstEntry; entryId <= lastEntry; entryId++) {
                byte[] payload = payloads.get(Math.toIntExact(entryId));
                entries.add(LedgerEntryImpl.create(
                        LEDGER_ID, entryId, payload.length, Unpooled.wrappedBuffer(payload.clone())));
            }
            return CompletableFuture.completedFuture(LedgerEntriesImpl.create(entries));
        }

        @Override
        public CompletableFuture<LedgerEntries> readUnconfirmedAsync(long firstEntry, long lastEntry) {
            return readAsync(firstEntry, lastEntry);
        }

        @Override
        public CompletableFuture<Long> readLastAddConfirmedAsync() {
            return CompletableFuture.completedFuture(getLastAddConfirmed());
        }

        @Override
        public CompletableFuture<Long> tryReadLastAddConfirmedAsync() {
            return readLastAddConfirmedAsync();
        }

        @Override
        public long getLastAddConfirmed() {
            return payloads.size() - 1L;
        }

        @Override
        public long getLength() {
            return metadata.getLength();
        }

        @Override
        public boolean isClosed() {
            return true;
        }

        @Override
        public CompletableFuture<LastConfirmedAndEntry> readLastAddConfirmedAndEntryAsync(
                long entryId, long timeOutInMillis, boolean parallel) {
            return CompletableFuture.failedFuture(new UnsupportedOperationException("sealed test handle"));
        }

        @Override
        public long getId() {
            return LEDGER_ID;
        }

        @Override
        public CompletableFuture<Void> closeAsync() {
            return CompletableFuture.completedFuture(null);
        }

        @Override
        public LedgerMetadata getLedgerMetadata() {
            return metadata;
        }
    }

    private static final class InMemoryStore implements PulsarOffloadObjectStoreV1 {
        private final Map<String, StoredObject> objects = new HashMap<>();
        private final List<String> calls = new ArrayList<>();
        private int rangeReads;

        @Override
        public Capabilities capabilities() {
            return new Capabilities(
                    LIMITS.maxDataObjectBytes(),
                    1,
                    64L * PulsarOffloadLimitCandidateV1.MIB,
                    1_024,
                    true,
                    true,
                    true,
                    true,
                    true);
        }

        @Override
        public CompletionStage<ImmutableObject> createImmutable(String key, Body body) {
            calls.add("create:" + key);
            try {
                byte[] bytes;
                try (var input = body.inputStreamFactory().open()) {
                    bytes = input.readAllBytes();
                }
                if (bytes.length != body.bytes() || !sha256(bytes).equals(body.sha256())) {
                    return CompletableFuture.failedFuture(new IOException("request body differs"));
                }
                StoredObject stored = new StoredObject("version-" + objects.size(), bytes, body.sha256());
                StoredObject previous = objects.putIfAbsent(key, stored);
                if (previous != null && !previous.sha256().equals(stored.sha256())) {
                    return CompletableFuture.failedFuture(
                            new ObjectStoreException(FailureKind.CONFLICT, "conditional conflict"));
                }
                return CompletableFuture.completedFuture((previous == null ? stored : previous).proof());
            } catch (IOException failure) {
                return CompletableFuture.failedFuture(failure);
            }
        }

        @Override
        public CompletionStage<ImmutableObject> head(String key) {
            calls.add("head:" + key);
            StoredObject stored = objects.get(key);
            return stored == null
                    ? CompletableFuture.failedFuture(new ObjectStoreException(FailureKind.NOT_FOUND, "absent"))
                    : CompletableFuture.completedFuture(stored.proof());
        }

        @Override
        public CompletionStage<byte[]> readRange(String key, long offset, int length) {
            calls.add("read:" + key);
            rangeReads++;
            StoredObject stored = objects.get(key);
            if (stored == null || offset < 0 || offset + length > stored.bytes().length) {
                return CompletableFuture.failedFuture(new ObjectStoreException(FailureKind.SHORT_READ, "outside"));
            }
            return CompletableFuture.completedFuture(
                    Arrays.copyOfRange(stored.bytes(), Math.toIntExact(offset), Math.toIntExact(offset + length)));
        }

        @Override
        public CompletionStage<Void> deleteAndProveAbsent(String key) {
            calls.add("delete:" + key);
            objects.remove(key);
            return CompletableFuture.completedFuture(null);
        }

        @Override
        public CompletionStage<Void> cleanupAttemptMultipartResidue(String attemptPrefix) {
            calls.add("cleanup:" + attemptPrefix);
            return CompletableFuture.completedFuture(null);
        }

        @Override
        public CompletionStage<Void> close() {
            return CompletableFuture.completedFuture(null);
        }

        private void flipByteWithoutChangingProof(String key) {
            StoredObject stored = objects.get(key);
            byte[] changed = stored.bytes().clone();
            changed[changed.length / 2] ^= 1;
            objects.put(key, new StoredObject(stored.version(), changed, stored.sha256()));
        }
    }

    private record StoredObject(String version, byte[] bytes, String sha256) {
        private StoredObject {
            bytes = bytes.clone();
        }

        @Override
        public byte[] bytes() {
            return bytes.clone();
        }

        private ImmutableObject proof() {
            return new ImmutableObject(version, bytes.length, sha256);
        }
    }

    private static String sha256(byte[] bytes) {
        try {
            return HexFormat.of().formatHex(MessageDigest.getInstance("SHA-256").digest(bytes));
        } catch (Exception failure) {
            throw new AssertionError(failure);
        }
    }
}
