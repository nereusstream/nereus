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

package com.nereusstream.storage.bookkeeper;

import io.netty.buffer.ByteBuf;
import io.netty.util.ReferenceCounted;
import java.util.EnumSet;
import java.util.HashSet;
import java.util.List;
import java.util.Set;
import java.util.concurrent.CompletableFuture;
import org.apache.bookkeeper.client.BKException;
import org.apache.bookkeeper.client.BookKeeper;
import org.apache.bookkeeper.client.ClientContext;
import org.apache.bookkeeper.client.api.WriteFlag;
import org.apache.bookkeeper.conf.ClientConfiguration;
import org.apache.bookkeeper.net.BookieId;
import org.apache.bookkeeper.proto.BookieClient;
import org.apache.bookkeeper.proto.BookkeeperInternalCallbacks.BatchedReadEntryCallback;
import org.apache.bookkeeper.proto.BookkeeperInternalCallbacks.ForceLedgerCallback;
import org.apache.bookkeeper.proto.BookkeeperInternalCallbacks.GetBookieInfoCallback;
import org.apache.bookkeeper.proto.BookkeeperInternalCallbacks.ReadEntryCallback;
import org.apache.bookkeeper.proto.BookkeeperInternalCallbacks.ReadLacCallback;
import org.apache.bookkeeper.proto.BookkeeperInternalCallbacks.WriteCallback;
import org.apache.bookkeeper.proto.BookkeeperInternalCallbacks.WriteLacCallback;
import org.apache.bookkeeper.util.AvailabilityOfEntriesOfLedger;
import org.apache.bookkeeper.util.ByteBufList;

/** Original factory SDK data RPCs, including cursor writes and explicit LAC; no payload copies or second client. */
final class M5PulsarNativeDataClientV2 extends BookKeeper {
    private final ClientContext originalContext;
    private final ClientContext trackedContext;
    private final Set<Attempt> writes = new HashSet<>();
    private long pendingBytes;
    private long dispatchedAdds;
    private long dispatchedLacs;
    private long dispatchedFences;
    private long dispatchedForces;
    private long completedWrites;
    private long fencedWrites;
    private long harmlessForces;
    private CompletableFuture<Void> reconciling;
    private boolean admissionClosed;

    M5PulsarNativeDataClientV2(ClientConfiguration configuration) throws Exception {
        super(configuration);
        originalContext = super.getClientCtx();
        var port = new DataPort(originalContext.getBookieClient());
        trackedContext = new org.apache.bookkeeper.client.NereusM5ClientContextV2(originalContext, port);
    }

    @Override
    public ClientContext getClientCtx() {
        return trackedContext == null ? super.getClientCtx() : trackedContext;
    }

    private static final class Attempt {
        private final int bytes;
        private final long ledgerId;
        private final BookieId address;
        private final int kind;
        private final boolean recoveryAdd;
        private final CompletableFuture<Void> ended = new CompletableFuture<>();

        private Attempt(long ledgerId, BookieId address, int bytes, int kind, boolean recoveryAdd) {
            this.bytes = bytes;
            this.ledgerId = ledgerId;
            this.address = address;
            this.kind = kind;
            this.recoveryAdd = recoveryAdd;
        }
    }

    private Attempt accept(long ledgerId, BookieId address, int bytes, int kind) throws BKException {
        return accept(ledgerId, address, bytes, kind, false);
    }

    private Attempt accept(long ledgerId, BookieId address, int bytes, int kind, boolean recoveryAdd)
            throws BKException {
        if (!((M5PulsarNativeMetadataDriverV1) getMetadataClientDriver()).isDataLedgerAdmitted(ledgerId)) {
            throw new BKException.BKUnauthorizedAccessException();
        }
        // Successful original ledger admissions are monotonic; do not nest the SDK driver and data locks.
        synchronized (this) {
            if (admissionClosed) {
                throw new BKException.BKClientClosedException();
            }
            if (bytes <= 0
                    || writes.size() == M5PulsarNativeCreateSpecV1.MAX_PENDING_SDK_DATA_RPCS
                    || bytes > M5PulsarNativeCreateSpecV1.MAX_PENDING_SDK_DATA_BYTES - pendingBytes) {
                throw new BKException.BKTooManyRequestsException();
            }
            var accepted = new Attempt(ledgerId, address, bytes, kind, recoveryAdd);
            writes.add(accepted);
            pendingBytes += bytes;
            switch (kind) {
                case 0 -> dispatchedAdds++;
                case 1 -> dispatchedLacs++;
                case 2 -> dispatchedFences++;
                case 3 -> dispatchedForces++;
                default -> throw new IllegalArgumentException("unknown SDK data operation");
            }
            return accepted;
        }
    }

    private void complete(Attempt original, int rc) {
        synchronized (this) {
            if (rc == BKException.Code.OK) {
                writes.remove(original);
                pendingBytes -= original.bytes;
                completedWrites++;
            }
        }
        if (rc == BKException.Code.OK) {
            original.ended.complete(null);
        } else {
            original.ended.completeExceptionally(new IllegalStateException("original SDK data write remains unknown"));
        }
    }

    /** Fences this exact port before taking its original operation cut; cancellation never changes that cut. */
    synchronized CompletableFuture<Void> drainDataWrites() {
        admissionClosed = true;
        return CompletableFuture.allOf(writes.stream().map(value -> value.ended).toArray(CompletableFuture[]::new))
                .copy();
    }

    /** One pass over original failed packets; native fence recovery never replays an append or changes its result. */
    CompletableFuture<Void> reconcileDataWrites(java.util.function.Supplier<CompletableFuture<Void>> namespaceCheck) {
        final List<Attempt> originals;
        final CompletableFuture<Void> privateResult;
        synchronized (this) {
            if (!admissionClosed) {
                return CompletableFuture.failedFuture(new IllegalStateException("original SDK data admission is live"));
            }
            if (reconciling != null && !reconciling.isDone()) {
                return reconciling.copy();
            }
            originals = writes.stream()
                    .sorted(java.util.Comparator.comparingInt(value -> value.kind))
                    .toList();
            privateResult = new CompletableFuture<>();
            reconciling = privateResult;
        }
        CompletableFuture<Void> pass = CompletableFuture.completedFuture(null);
        for (var original : originals) {
            pass = pass.thenCompose(unused -> original.kind == 3
                    ? proveOriginalForceHarmless(original, namespaceCheck)
                    : fenceOriginalAppend(original, namespaceCheck));
        }
        pass.whenComplete((unused, failure) -> {
            if (failure == null) {
                privateResult.complete(null);
            } else {
                privateResult.completeExceptionally(failure);
            }
        });
        return privateResult.copy();
    }

    /** BK4.18 force queues a null journal marker; it neither opens a ledger handle nor writes ledger content. */
    private CompletableFuture<Void> proveOriginalForceHarmless(
            Attempt original, java.util.function.Supplier<CompletableFuture<Void>> namespaceCheck) {
        synchronized (this) {
            if (!admissionClosed
                    || !original.ended.isCompletedExceptionally()
                    || writes.stream().anyMatch(value -> value.kind != 3)) {
                return CompletableFuture.failedFuture(new IllegalStateException("original SDK force remains unknown"));
            }
        }
        var spec = ((M5PulsarNativeMetadataDriverV1) getMetadataClientDriver()).createSpec();
        return originalContext
                .getLedgerManager()
                .readLedgerMetadata(original.ledgerId)
                .thenCompose(before -> {
                    var metadata = before.getValue();
                    if (!metadata.isClosed()
                            || spec.bindingFor(metadata, true).isEmpty()
                            || metadata.getAllEnsembles().values().stream()
                                    .noneMatch(value -> value.contains(original.address))) {
                        return CompletableFuture.failedFuture(
                                new IllegalStateException("original SDK force header is not sealed or bound"));
                    }
                    return namespaceCheck
                            .get()
                            .thenCompose(
                                    unused -> originalContext.getLedgerManager().readLedgerMetadata(original.ledgerId))
                            .thenCompose(after -> {
                                if (!before.getVersion().equals(after.getVersion())
                                        || !M5PulsarNativeCreateSpecV1.sameMetadata(metadata, after.getValue())) {
                                    return CompletableFuture.failedFuture(
                                            new IllegalStateException("original SDK force header changed"));
                                }
                                return namespaceCheck.get();
                            })
                            .thenRun(() -> {
                                synchronized (this) {
                                    if (writes.remove(original)) {
                                        pendingBytes -= original.bytes;
                                        harmlessForces++;
                                    }
                                }
                            });
                });
    }

    private CompletableFuture<Void> fenceOriginalAppend(
            Attempt original, java.util.function.Supplier<CompletableFuture<Void>> namespaceCheck) {
        // BK4.18 recoveryAddEntry bypasses the fenced check; a later fence cannot exclude that original packet.
        if (!original.ended.isCompletedExceptionally()
                || original.recoveryAdd
                || original.kind != 0 && original.kind != 2) {
            return CompletableFuture.failedFuture(new IllegalStateException("original SDK data write remains unknown"));
        }
        var driver = (M5PulsarNativeMetadataDriverV1) getMetadataClientDriver();
        var spec = driver.createSpec();
        return originalContext
                .getLedgerManager()
                .readLedgerMetadata(original.ledgerId)
                .thenCompose(before -> {
                    var metadata = before.getValue();
                    if (!metadata.isClosed()
                            || spec.bindingFor(metadata, true).isEmpty()
                            || metadata.getAllEnsembles().values().stream()
                                    .noneMatch(value -> value.contains(original.address))) {
                        return CompletableFuture.failedFuture(
                                new IllegalStateException("original SDK append header is not sealed or bound"));
                    }
                    final byte[] masterKey;
                    try {
                        masterKey = org.apache.bookkeeper.proto.checksum.DigestManager.generateMasterKey(
                                metadata.getPassword());
                    } catch (Exception invalid) {
                        return CompletableFuture.failedFuture(invalid);
                    }
                    var fenced = new CompletableFuture<Void>();
                    try {
                        originalContext
                                .getBookieClient()
                                .readEntry(
                                        original.address,
                                        original.ledgerId,
                                        org.apache.bookkeeper.proto.BookieProtocol.LAST_ADD_CONFIRMED,
                                        (rc, ledgerId, entryId, buffer, ctx) -> {
                                            // ReadCompletion owns this borrowed slice and releases it after callback.
                                            if (rc != BKException.Code.OK
                                                    || ledgerId != original.ledgerId
                                                    || buffer == null
                                                    || !buffer.isReadable()) {
                                                fenced.completeExceptionally(new IllegalStateException(
                                                        "original SDK append fence remains unknown"));
                                            } else {
                                                fenced.complete(null);
                                            }
                                        },
                                        null,
                                        org.apache.bookkeeper.proto.BookieProtocol.FLAG_DO_FENCING,
                                        masterKey,
                                        false);
                    } catch (RuntimeException failure) {
                        fenced.completeExceptionally(failure);
                    }
                    return fenced.thenCompose(
                                    unused -> originalContext.getLedgerManager().readLedgerMetadata(original.ledgerId))
                            .thenCompose(after -> {
                                if (!before.getVersion().equals(after.getVersion())
                                        || !M5PulsarNativeCreateSpecV1.sameMetadata(metadata, after.getValue())) {
                                    return CompletableFuture.failedFuture(new IllegalStateException(
                                            "original SDK append header changed during fencing"));
                                }
                                return namespaceCheck.get();
                            })
                            .thenRun(() -> {
                                synchronized (this) {
                                    if (writes.remove(original)) {
                                        pendingBytes -= original.bytes;
                                        fencedWrites++;
                                    }
                                }
                            });
                });
    }

    synchronized M5BookKeeperNamespaceAuthorityV2.SdkDataWriteSnapshot dataWriteSnapshot() {
        return new M5BookKeeperNamespaceAuthorityV2.SdkDataWriteSnapshot(
                dispatchedAdds,
                dispatchedLacs,
                dispatchedFences,
                dispatchedForces,
                completedWrites,
                fencedWrites,
                harmlessForces,
                writes.size(),
                pendingBytes,
                admissionClosed);
    }

    private static int bytes(ReferenceCounted value) {
        if (value instanceof ByteBufList list) {
            return list.readableBytes();
        }
        if (value instanceof ByteBuf buffer) {
            return buffer.readableBytes();
        }
        return -1;
    }

    private final class DataPort implements BookieClient {
        private final BookieClient delegate;

        private DataPort(BookieClient delegate) {
            this.delegate = delegate;
        }

        public List<BookieId> getFaultyBookies() {
            return delegate.getFaultyBookies();
        }

        public boolean isWritable(BookieId address, long ledgerId) {
            return delegate.isWritable(address, ledgerId);
        }

        public long getNumPendingRequests(BookieId address, long ledgerId) {
            return delegate.getNumPendingRequests(address, ledgerId);
        }

        public void forceLedger(BookieId address, long ledgerId, ForceLedgerCallback cb, Object ctx) {
            final Attempt original;
            try {
                original = accept(ledgerId, address, 64, 3);
            } catch (BKException rejected) {
                cb.forceLedgerComplete(rejected.getCode(), ledgerId, address, ctx);
                return;
            }
            try {
                delegate.forceLedger(
                        address,
                        ledgerId,
                        (rc, actualLedger, actualAddress, actualCtx) -> {
                            complete(original, rc);
                            cb.forceLedgerComplete(rc, actualLedger, actualAddress, actualCtx);
                        },
                        ctx);
            } catch (RuntimeException failure) {
                complete(original, BKException.Code.UnexpectedConditionException);
                throw failure;
            }
        }

        public void readLac(BookieId address, long ledgerId, ReadLacCallback cb, Object ctx) {
            delegate.readLac(address, ledgerId, cb, ctx);
        }

        public void writeLac(
                BookieId address,
                long ledgerId,
                byte[] masterKey,
                long lac,
                ByteBufList toSend,
                WriteLacCallback cb,
                Object ctx) {
            final Attempt original;
            try {
                original = accept(ledgerId, address, bytes(toSend), 1);
            } catch (BKException rejected) {
                cb.writeLacComplete(rejected.getCode(), ledgerId, address, ctx);
                return;
            }
            try {
                delegate.writeLac(
                        address,
                        ledgerId,
                        masterKey,
                        lac,
                        toSend,
                        (rc, actualLedger, actualAddress, actualCtx) -> {
                            complete(original, rc);
                            cb.writeLacComplete(rc, actualLedger, actualAddress, actualCtx);
                        },
                        ctx);
            } catch (RuntimeException failure) {
                complete(original, BKException.Code.UnexpectedConditionException);
                throw failure;
            }
        }

        public void addEntry(
                BookieId address,
                long ledgerId,
                byte[] masterKey,
                long entryId,
                ReferenceCounted toSend,
                WriteCallback cb,
                Object ctx,
                int options,
                boolean allowFastFail,
                EnumSet<WriteFlag> writeFlags) {
            final Attempt original;
            try {
                if (writeFlags.contains(WriteFlag.DEFERRED_SYNC)) {
                    throw new BKException.BKIllegalOpException();
                }
                original = accept(
                        ledgerId,
                        address,
                        bytes(toSend),
                        0,
                        (options & org.apache.bookkeeper.proto.BookieProtocol.FLAG_RECOVERY_ADD) != 0);
            } catch (BKException rejected) {
                cb.writeComplete(rejected.getCode(), ledgerId, entryId, address, ctx);
                return;
            }
            try {
                delegate.addEntry(
                        address,
                        ledgerId,
                        masterKey,
                        entryId,
                        toSend,
                        (rc, actualLedger, actualEntry, actualAddress, actualCtx) -> {
                            complete(original, rc);
                            cb.writeComplete(rc, actualLedger, actualEntry, actualAddress, actualCtx);
                        },
                        ctx,
                        options,
                        allowFastFail,
                        writeFlags);
            } catch (RuntimeException failure) {
                complete(original, BKException.Code.UnexpectedConditionException);
                throw failure;
            }
        }

        public void readEntry(
                BookieId address,
                long ledgerId,
                long entryId,
                ReadEntryCallback cb,
                Object ctx,
                int flags,
                byte[] masterKey,
                boolean allowFastFail) {
            if ((flags & org.apache.bookkeeper.proto.BookieProtocol.FLAG_DO_FENCING) == 0) {
                delegate.readEntry(address, ledgerId, entryId, cb, ctx, flags, masterKey, allowFastFail);
                return;
            }
            final Attempt original;
            try {
                original = accept(ledgerId, address, 64, 2);
            } catch (BKException rejected) {
                cb.readEntryComplete(rejected.getCode(), ledgerId, entryId, null, ctx);
                return;
            }
            try {
                delegate.readEntry(
                        address,
                        ledgerId,
                        entryId,
                        (rc, actualLedger, actualEntry, buffer, actualCtx) -> {
                            complete(original, rc);
                            cb.readEntryComplete(rc, actualLedger, actualEntry, buffer, actualCtx);
                        },
                        ctx,
                        flags,
                        masterKey,
                        allowFastFail);
            } catch (RuntimeException failure) {
                complete(original, BKException.Code.UnexpectedConditionException);
                throw failure;
            }
        }

        public void batchReadEntries(
                BookieId address,
                long ledgerId,
                long startEntryId,
                int maxCount,
                long maxSize,
                BatchedReadEntryCallback cb,
                Object ctx,
                int flags,
                byte[] masterKey,
                boolean allowFastFail) {
            if ((flags & org.apache.bookkeeper.proto.BookieProtocol.FLAG_DO_FENCING) != 0) {
                cb.readEntriesComplete(BKException.Code.IllegalOpException, ledgerId, startEntryId, null, ctx);
                return; // The admitted v3 profile has no batch-fencing RPC.
            }
            delegate.batchReadEntries(
                    address, ledgerId, startEntryId, maxCount, maxSize, cb, ctx, flags, masterKey, allowFastFail);
        }

        public void readEntryWaitForLACUpdate(
                BookieId address,
                long ledgerId,
                long entryId,
                long previousLAC,
                long timeOutInMillis,
                boolean piggyBackEntry,
                ReadEntryCallback cb,
                Object ctx) {
            delegate.readEntryWaitForLACUpdate(
                    address, ledgerId, entryId, previousLAC, timeOutInMillis, piggyBackEntry, cb, ctx);
        }

        public void getBookieInfo(BookieId address, long requested, GetBookieInfoCallback cb, Object ctx) {
            delegate.getBookieInfo(address, requested, cb, ctx);
        }

        public CompletableFuture<AvailabilityOfEntriesOfLedger> getListOfEntriesOfLedger(
                BookieId address, long ledgerId) {
            return delegate.getListOfEntriesOfLedger(address, ledgerId);
        }

        public boolean isClosed() {
            return delegate.isClosed();
        }

        public void close() {
            delegate.close();
        }
    }
}
