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

import com.nereusstream.pulsar.offload.npd1.Npd1CodecV1.EntryPayload;
import com.nereusstream.pulsar.offload.npo1.Npo1CodecV1.CustomMetadataValue;
import com.nereusstream.pulsar.offload.npo1.Npo1CodecV1.Root;
import io.netty.buffer.Unpooled;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.concurrent.CompletableFuture;
import java.util.function.BooleanSupplier;
import org.apache.bookkeeper.client.LedgerMetadataBuilder;
import org.apache.bookkeeper.client.api.DigestType;
import org.apache.bookkeeper.client.api.LastConfirmedAndEntry;
import org.apache.bookkeeper.client.api.LedgerEntries;
import org.apache.bookkeeper.client.api.LedgerEntry;
import org.apache.bookkeeper.client.api.LedgerMetadata;
import org.apache.bookkeeper.client.api.ReadHandle;
import org.apache.bookkeeper.client.impl.LedgerEntriesImpl;
import org.apache.bookkeeper.client.impl.LedgerEntryImpl;
import org.apache.bookkeeper.mledger.SourceSafeLedgerOffloader.OffloadedObjectReadIdentity;
import org.apache.bookkeeper.mledger.SourceSafeLedgerOffloader.OffloadedReadMemoryBounds;
import org.apache.bookkeeper.mledger.SourceSafeLedgerOffloader.OffloadedReadSourceIdentity;
import org.apache.bookkeeper.net.BookieId;

/** BookKeeper ReadHandle adapter backed wholly by one verified NPO1/NPD1 attempt. */
public final class NereusPulsarReadHandleV1 implements ReadHandle {
    private final PulsarObjectReadHandleV1 objectHandle;
    private final LedgerMetadata metadata;
    private final CompletableFuture<Void> readsDrained = new CompletableFuture<>();
    private boolean closed;
    private long activeReads;
    private CompletableFuture<Void> closeFuture;

    public NereusPulsarReadHandleV1(PulsarObjectReadHandleV1 objectHandle) {
        this.objectHandle = java.util.Objects.requireNonNull(objectHandle, "objectHandle");
        this.metadata = metadata(objectHandle.root());
    }

    OffloadedReadSourceIdentity describeSource(PulsarOffloadObjectStoreV1 expectedStore) {
        if (!objectHandle.usesObjectStore(expectedStore)) {
            throw new IllegalArgumentException("Object read handle belongs to another offload provider");
        }
        var attempt = objectHandle.attempt();
        var root = objectHandle.rootObject();
        var data = objectHandle.dataObject();
        return new OffloadedReadSourceIdentity(
                attempt.ledgerId(),
                attempt.attemptUuid(),
                new OffloadedObjectReadIdentity(
                        attempt.keys().rootKey(), root.immutableVersion(), root.bytes(), root.sha256()),
                new OffloadedObjectReadIdentity(
                        attempt.keys().dataKey(), data.immutableVersion(), data.bytes(), data.sha256()));
    }

    @Override
    public CompletableFuture<LedgerEntries> readAsync(long firstEntry, long lastEntry) {
        synchronized (this) {
            if (closed) {
                return CompletableFuture.failedFuture(new IllegalStateException("Nereus Object ReadHandle is closed"));
            }
            activeReads++;
        }
        CompletableFuture<LedgerEntries> result = new CompletableFuture<>();
        try {
            objectHandle.read(firstEntry, lastEntry).whenComplete((entries, failure) -> {
                try {
                    if (failure != null) {
                        result.completeExceptionally(failure);
                    } else if (!result.isCancelled()) {
                        LedgerEntries nativeEntries = nativeEntries(entries);
                        if (!result.complete(nativeEntries)) {
                            nativeEntries.close();
                        }
                    }
                } catch (Throwable deliveryFailure) {
                    result.completeExceptionally(deliveryFailure);
                } finally {
                    readFinished();
                }
            });
        } catch (Throwable failure) {
            try {
                result.completeExceptionally(failure);
            } finally {
                readFinished();
            }
        }
        return result;
    }

    OffloadedReadMemoryBounds memoryBounds(PulsarOffloadObjectStoreV1 expectedStore) {
        if (!objectHandle.usesObjectStore(expectedStore)) {
            throw new IllegalArgumentException("Object memory bounds belong to another provider");
        }
        return objectHandle.memoryBounds();
    }

    private LedgerEntries nativeEntries(List<EntryPayload> entries) {
        List<LedgerEntry> nativeEntries = new ArrayList<>(entries.size());
        try {
            for (EntryPayload entry : entries) {
                byte[] payload = entry.payload();
                nativeEntries.add(LedgerEntryImpl.create(
                        getId(), entry.entryId(), payload.length, Unpooled.wrappedBuffer(payload)));
            }
            return LedgerEntriesImpl.create(nativeEntries);
        } catch (Throwable failure) {
            nativeEntries.forEach(LedgerEntry::close);
            throw failure;
        }
    }

    private void readFinished() {
        boolean complete;
        synchronized (this) {
            activeReads--;
            complete = closed && activeReads == 0;
        }
        if (complete) {
            readsDrained.complete(null);
        }
    }

    CompletableFuture<Void> verifyOpenedSource(
            long lastEntry, long entryCount, long logicalLength, BooleanSupplier sourceUseOpen) {
        synchronized (this) {
            if (closed
                    || lastEntry != getLastAddConfirmed()
                    || logicalLength != getLength()
                    || entryCount != lastEntry + 1) {
                return CompletableFuture.failedFuture(new IllegalStateException("opened verification facts differ"));
            }
            activeReads++;
        }
        var result = new CompletableFuture<Void>();
        try {
            objectHandle.verifyCompleteLedger(sourceUseOpen).whenComplete((report, failure) -> {
                readFinished();
                if (failure == null) {
                    result.complete(null);
                } else {
                    result.completeExceptionally(failure);
                }
            });
        } catch (Throwable failure) {
            readFinished();
            result.completeExceptionally(failure);
        }
        return result.copy();
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
        return metadata.getLastEntryId();
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
        return CompletableFuture.failedFuture(
                new UnsupportedOperationException("sealed Nereus Object handle does not long-poll"));
    }

    @Override
    public long getId() {
        return metadata.getLedgerId();
    }

    @Override
    public synchronized CompletableFuture<Void> closeAsync() {
        if (closeFuture == null) {
            closed = true;
            if (activeReads == 0) {
                readsDrained.complete(null);
            }
            closeFuture = CompletableFuture.allOf(objectHandle.close().toCompletableFuture(), readsDrained);
        }
        return closeFuture.copy();
    }

    @Override
    public LedgerMetadata getLedgerMetadata() {
        return metadata;
    }

    private static LedgerMetadata metadata(Root root) {
        Map<String, byte[]> customMetadata = new HashMap<>();
        for (Map.Entry<String, CustomMetadataValue> entry :
                root.sealedLedger().customMetadata().entrySet()) {
            customMetadata.put(entry.getKey(), entry.getValue().bytes());
        }
        LedgerMetadataBuilder builder = LedgerMetadataBuilder.create()
                .withId(root.attempt().ledgerId())
                .withMetadataFormatVersion(2)
                .withEnsembleSize(root.sealedLedger().ensembleSize())
                .withWriteQuorumSize(root.sealedLedger().writeQuorum())
                .withAckQuorumSize(root.sealedLedger().ackQuorum())
                .withDigestType(digestType(root))
                .withPassword(new byte[0])
                .withClosedState()
                .withLastEntryId(root.sealedLedger().lastAddConfirmed())
                .withLength(root.sealedLedger().logicalLength())
                .withCustomMetadata(customMetadata)
                .withCreationTime(root.sealedLedger().creationTimestampMillis())
                .storingCreationTime(true)
                .withCToken(root.sealedLedger().fencedOwnerEpoch());
        root.sealedLedger()
                .ensembles()
                .forEach(ensemble -> builder.newEnsembleEntry(
                        ensemble.firstEntryId(),
                        ensemble.bookieIds().stream().map(BookieId::parse).toList()));
        return builder.build();
    }

    private static DigestType digestType(Root root) {
        return switch (root.sealedLedger().digestType()) {
            case CRC32C -> DigestType.CRC32C;
            case MAC -> DigestType.MAC;
            case CRC32 -> DigestType.CRC32;
            case DUMMY -> DigestType.DUMMY;
        };
    }
}
