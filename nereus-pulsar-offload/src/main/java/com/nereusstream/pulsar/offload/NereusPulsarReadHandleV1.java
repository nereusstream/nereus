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
import java.util.concurrent.atomic.AtomicBoolean;
import org.apache.bookkeeper.client.LedgerMetadataBuilder;
import org.apache.bookkeeper.client.api.DigestType;
import org.apache.bookkeeper.client.api.LastConfirmedAndEntry;
import org.apache.bookkeeper.client.api.LedgerEntries;
import org.apache.bookkeeper.client.api.LedgerEntry;
import org.apache.bookkeeper.client.api.LedgerMetadata;
import org.apache.bookkeeper.client.api.ReadHandle;
import org.apache.bookkeeper.client.impl.LedgerEntriesImpl;
import org.apache.bookkeeper.client.impl.LedgerEntryImpl;
import org.apache.bookkeeper.net.BookieId;

/** BookKeeper ReadHandle adapter backed wholly by one verified NPO1/NPD1 attempt. */
public final class NereusPulsarReadHandleV1 implements ReadHandle {
    private final PulsarObjectReadHandleV1 objectHandle;
    private final LedgerMetadata metadata;
    private final AtomicBoolean closed = new AtomicBoolean();
    private volatile CompletableFuture<Void> closeFuture;

    public NereusPulsarReadHandleV1(PulsarObjectReadHandleV1 objectHandle) {
        this.objectHandle = java.util.Objects.requireNonNull(objectHandle, "objectHandle");
        this.metadata = metadata(objectHandle.root());
    }

    @Override
    public CompletableFuture<LedgerEntries> readAsync(long firstEntry, long lastEntry) {
        if (closed.get()) {
            return CompletableFuture.failedFuture(new IllegalStateException("Nereus Object ReadHandle is closed"));
        }
        return objectHandle
                .read(firstEntry, lastEntry)
                .thenApply(entries -> {
                    List<LedgerEntry> nativeEntries = new ArrayList<>(entries.size());
                    for (EntryPayload entry : entries) {
                        byte[] payload = entry.payload();
                        nativeEntries.add(LedgerEntryImpl.create(
                                getId(), entry.entryId(), payload.length, Unpooled.wrappedBuffer(payload)));
                    }
                    return (LedgerEntries) LedgerEntriesImpl.create(nativeEntries);
                })
                .toCompletableFuture();
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
            closed.set(true);
            closeFuture = objectHandle.close().toCompletableFuture();
        }
        return closeFuture;
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
