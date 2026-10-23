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

import com.nereusstream.domain.bytes.CanonicalBytes;
import com.nereusstream.domain.bytes.CanonicalUtf8;
import com.nereusstream.domain.bytes.Sha256Digest;
import com.nereusstream.storage.object.materialization.M5PulsarCoverageV2.Position;
import com.nereusstream.storage.object.materialization.Nms1CodecV1;
import com.nereusstream.storage.object.provider.C1ObjectProviderSession;
import java.io.ByteArrayInputStream;
import java.io.ByteArrayOutputStream;
import java.io.DataInputStream;
import java.io.DataOutputStream;
import java.io.IOException;
import java.nio.ByteBuffer;
import java.util.ArrayList;
import java.util.List;
import java.util.Optional;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.Executor;
import java.util.function.BooleanSupplier;
import org.apache.bookkeeper.mledger.Entry;
import org.apache.bookkeeper.mledger.SourceSafeLedgerOffloader.NativeReadRange;
import org.apache.bookkeeper.mledger.SourceSafeLedgerOffloader.PreferredReadOperation;
import org.apache.bookkeeper.mledger.impl.EntryImpl;

/** Actual configured SDK loan and original response cleanup for ordinary selected NMS1 reads. */
final class PulsarNativeMaterializedReadV2 {
    private static final int MAGIC = 0x4e504d56; // NPMV, wrapped by the unchanged NPSM immutable-view envelope.
    private final PulsarNativeBindingReadViewV1 view;
    private final PulsarNativeM4ControlV1.SelectedMaterialization selected;
    private final PulsarNativeReadCellBudgetV1 budget;
    private final NereusPulsarLedgerOffloaderV1 offloader;
    private final Executor worker;
    private final List<Read> reads = new ArrayList<>();
    private S3PulsarOffloadObjectStoreV1.MaterializationConnection connection;
    private CompletableFuture<Void> opening;
    private CompletableFuture<Void> stopping;
    private volatile boolean closed;
    private boolean loanReleased;

    PulsarNativeMaterializedReadV2(
            PulsarNativeBindingReadViewV1 view,
            PulsarNativeM4ControlV1.SelectedMaterialization selected,
            PulsarNativeReadCellBudgetV1 budget,
            NereusPulsarLedgerOffloaderV1 offloader,
            Executor worker) {
        this.view = view;
        this.selected = selected;
        this.budget = budget;
        this.offloader = offloader;
        this.worker = worker;
    }

    synchronized CompletableFuture<Void> open() {
        if (opening == null) {
            opening = CompletableFuture.runAsync(
                    () -> {
                        synchronized (this) {
                            if (closed) {
                                throw new IllegalStateException("Native mapped preparation is closed");
                            }
                            connection = offloader.borrowConfiguredMaterializationConnection(
                                    view.binding(),
                                    view.objectNamespace(),
                                    selected.providerProfile().maximumObjectBytes());
                        }
                        connection.admitProfile(view.binding()).join();
                        connection.requireProfileCurrent().join();
                        if (!connection.profile().equals(selected.providerProfile()) || !view.isCurrent()) {
                            throw new IllegalStateException("Native mapped actual SDK profile/source differs");
                        }
                    },
                    worker);
        }
        return opening.copy();
    }

    synchronized S3PulsarOffloadObjectStoreV1.MaterializationConnection connectionForControl() {
        if (closed || loanReleased || opening == null || !opening.isDone() || opening.isCompletedExceptionally()) {
            throw new IllegalStateException("Original Native preferred SDK connection is unavailable");
        }
        return connection;
    }

    /** The already-admitted low-frequency Source worker performs SDK work on its original thread. */
    SourceRead readInline(long ledgerId, long entry, BooleanSupplier sourceUseOpen) {
        final Read original;
        synchronized (this) {
            if (closed || opening == null || !opening.isDone() || opening.isCompletedExceptionally()) {
                return new SourceRead(
                        new PreferredReadOperation(
                                CompletableFuture.failedFuture(
                                        new IllegalStateException("Native mapped SDK is not open")),
                                CompletableFuture.completedFuture(null)),
                        () -> {});
            }
            original = new Read(
                    ledgerId,
                    entry,
                    entry,
                    (first, last) -> CompletableFuture.failedFuture(
                            new IllegalStateException("Native materialization expected an actual mapped payload")),
                    sourceUseOpen);
            reads.add(original);
            original.drained.whenComplete((ignored, failure) -> {
                synchronized (PulsarNativeMaterializedReadV2.this) {
                    reads.remove(original);
                }
            });
        }
        original.run();
        return new SourceRead(new PreferredReadOperation(original.result, original.drained), original::confirmCleanup);
    }

    record SourceRead(PreferredReadOperation observation, Runnable reconcile) {}

    synchronized PreferredReadOperation read(
            long ledgerId, long first, long last, NativeReadRange retained, BooleanSupplier sourceUseOpen) {
        final Read original;
        synchronized (this) {
            if (closed || opening == null || !opening.isDone() || opening.isCompletedExceptionally()) {
                return new PreferredReadOperation(
                        CompletableFuture.failedFuture(new IllegalStateException("Native mapped SDK is not open")),
                        CompletableFuture.completedFuture(null));
            }
            original = new Read(ledgerId, first, last, retained, sourceUseOpen);
            reads.add(original); // Before any accepted dispatch; the observer cannot remove the SDK/response owner.
            original.drained.whenComplete((ignored, failure) -> {
                synchronized (PulsarNativeMaterializedReadV2.this) {
                    reads.remove(original);
                }
            });
        }
        try {
            CompletableFuture.runAsync(original::run, worker);
        } catch (Throwable rejected) {
            original.finished.complete(null);
            original.drained.complete(null);
            original.result.completeExceptionally(rejected);
        }
        return new PreferredReadOperation(original.result, original.drained);
    }

    /** Stop admission and reconcile only original responses. The connection loan remains through Entry alias drain. */
    synchronized CompletableFuture<Void> close() {
        closed = true;
        if (stopping == null || stopping.isCompletedExceptionally()) {
            var original = List.copyOf(reads);
            var openEnded = opening == null
                    ? CompletableFuture.completedFuture(null)
                    : opening.handle((ignored, failure) -> null);
            stopping = openEnded
                    .thenCompose(ignored -> CompletableFuture.allOf(
                            original.stream().map(read -> read.finished).toArray(CompletableFuture[]::new)))
                    .thenRunAsync(
                            () -> {
                                for (var read : original) {
                                    read.confirmCleanup();
                                }
                            },
                            worker);
        }
        return stopping.copy();
    }

    CompletableFuture<Void> releaseLoan() {
        return close().thenCompose(ignored -> {
            var loan = connection;
            return loan == null
                    ? CompletableFuture.completedFuture(null)
                    : loan.reconcileScopeForClose().thenRun(() -> {
                        synchronized (this) {
                            if (!loanReleased) {
                                loan.close();
                                loanReleased = true;
                            }
                        }
                    });
        });
    }

    private final class Read {
        private final long ledger;
        private final long first;
        private final long last;
        private final NativeReadRange retained;
        private final BooleanSupplier sourceUseOpen;
        private final CompletableFuture<List<Entry>> result = new CompletableFuture<>();
        private final CompletableFuture<Void> finished = new CompletableFuture<>();
        private final CompletableFuture<Void> drained = new CompletableFuture<>();
        private C1ObjectProviderSession.VerifiedReadCleanupException uncertainResponse;

        private Read(long ledger, long first, long last, NativeReadRange retained, BooleanSupplier sourceUseOpen) {
            this.ledger = ledger;
            this.first = first;
            this.last = last;
            this.retained = retained;
            this.sourceUseOpen = sourceUseOpen;
        }

        private void run() {
            var entries = new ArrayList<Entry>();
            Throwable failure = null;
            try {
                view.requireReadRange(ledger, first, last);
                if (last - first + 1 > budget.readLimits().maximumEntries()) {
                    throw new IllegalArgumentException("Native mapped range exceeds its original admission");
                }
                long bytes = 0;
                for (long id = first; id <= last; id++) {
                    requireOpen();
                    var position = new Position(ledger, id);
                    if (selected.readMap().rewrites(position)) {
                        connection.requireProfileCurrent().join();
                        var resource = selected.readMap()
                                .locate(position)
                                .rewrittenPayload()
                                .orElseThrow();
                        var credit = selected.readMap().readableOutputs().stream()
                                .filter(value -> value.resource().equals(Optional.of(resource)))
                                .findFirst()
                                .orElseThrow();
                        var consumed = connection
                                .qualifiedProvider()
                                .consumeVerifiedObject(
                                        credit.output().orElseThrow(),
                                        Optional.of(CanonicalUtf8.fromString(resource.immutableIdentity()
                                                        .value())
                                                .bytes()),
                                        input -> Nms1CodecV1.readPulsarEntry(
                                                selected.readMap().planForOutput(credit),
                                                credit.slot().ordinal(),
                                                input,
                                                position,
                                                budget.readLimits().maximumOwnedEntryBytes()));
                        entries.add(EntryImpl.create(
                                ledger, id, consumed.value().entry().payload().toByteArray()));
                    } else {
                        var nativeEntries =
                                retained.read(id, id).toCompletableFuture().join();
                        if (nativeEntries.size() != 1
                                || nativeEntries.get(0).getLedgerId() != ledger
                                || nativeEntries.get(0).getEntryId() != id) {
                            nativeEntries.forEach(Entry::release);
                            throw new IllegalStateException("Native retained range changed its position");
                        }
                        entries.addAll(nativeEntries);
                    }
                    Entry entry = entries.get(entries.size() - 1);
                    bytes = Math.addExact(bytes, entry.getLength());
                    if (entry.getLength() > budget.readLimits().maximumOwnedEntryBytes()
                            || bytes > budget.readLimits().maximumOwnedBatchBytes()) {
                        throw new IllegalArgumentException("Native mapped delivered bytes exceed the reserved batch");
                    }
                }
                requireOpen();
            } catch (C1ObjectProviderSession.VerifiedReadCleanupException unknown) {
                uncertainResponse = unknown;
                failure = unknown;
            } catch (Throwable rejected) {
                failure = rejected;
            }
            try {
                confirmCleanup();
            } catch (Throwable unresolved) {
                if (failure == null) {
                    failure = unresolved;
                } else {
                    failure.addSuppressed(unresolved);
                }
            } finally {
                finished.complete(null);
            }
            if (failure == null) {
                result.complete(List.copyOf(entries));
            } else {
                entries.forEach(Entry::release);
                result.completeExceptionally(failure);
            }
        }

        private void requireOpen() {
            if (closed || !sourceUseOpen.getAsBoolean() || !view.isCurrent()) {
                throw new IllegalStateException("Native mapped owner stopped during accepted read");
            }
        }

        private synchronized void confirmCleanup() {
            if (uncertainResponse != null) {
                try {
                    uncertainResponse.reconcileCleanup();
                } catch (IOException unresolved) {
                    throw new IllegalStateException(
                            "Native original mapped response cleanup remains unknown", unresolved);
                }
                uncertainResponse = null;
            }
            drained.complete(null);
        }
    }

    record Recipe(CanonicalBytes nativeView, Sha256Digest manifestSha256, PulsarNativeMaterializationReadMapV2 map) {}

    static boolean isRecipe(CanonicalBytes bytes) {
        return bytes.length() >= 8 && ByteBuffer.wrap(bytes.toByteArray()).getInt() == MAGIC;
    }

    static CanonicalBytes encodeRecipe(
            CanonicalBytes nativeView, Sha256Digest manifest, PulsarNativeMaterializationReadMapV2 map) {
        try {
            var bytes = new ByteArrayOutputStream();
            try (var out = new DataOutputStream(bytes)) {
                out.writeInt(MAGIC);
                out.writeInt(2);
                out.write(manifest.bytes().toByteArray());
                blob(out, nativeView);
                blob(out, map.encode());
            }
            var result = CanonicalBytes.copyOf(bytes.toByteArray());
            if (result.length() > PulsarNativeM4SourceMapV1.MAX_BYTES - 44) {
                throw new IllegalArgumentException("Native materialized read recipe exceeds its immutable map bound");
            }
            return result;
        } catch (IOException impossible) {
            throw new IllegalStateException(impossible);
        }
    }

    static Recipe decodeRecipe(CanonicalBytes bytes) {
        return decodeRecipe(bytes, 0);
    }

    static Recipe decodeRecipe(CanonicalBytes bytes, int depth) {
        if (!isRecipe(bytes) || bytes.length() > PulsarNativeM4SourceMapV1.MAX_BYTES - 44) {
            throw new IllegalArgumentException("Native materialized read recipe type/size differs");
        }
        try (var in = new DataInputStream(new ByteArrayInputStream(bytes.toByteArray()))) {
            in.readInt();
            if (in.readInt() != 2) {
                throw new IllegalArgumentException("Native materialized read recipe version differs");
            }
            var manifest = Sha256Digest.copyOf(in.readNBytes(32));
            var nativeView = blob(in);
            var map = PulsarNativeMaterializationReadMapV2.decode(blob(in), depth);
            if (in.read() != -1 || !encodeRecipe(nativeView, manifest, map).equals(bytes)) {
                throw new IllegalArgumentException("Native materialized read recipe is non-canonical");
            }
            return new Recipe(nativeView, manifest, map);
        } catch (IOException failure) {
            throw new IllegalArgumentException("Native materialized read recipe is truncated", failure);
        }
    }

    private static void blob(DataOutputStream out, CanonicalBytes value) throws IOException {
        out.writeInt(value.length());
        out.write(value.toByteArray());
    }

    private static CanonicalBytes blob(DataInputStream in) throws IOException {
        int length = in.readInt();
        if (length <= 0 || length > in.available() || length > PulsarNativeM4SourceMapV1.MAX_BYTES) {
            throw new IllegalArgumentException("Native materialized read recipe field length differs");
        }
        return CanonicalBytes.copyOf(in.readNBytes(length));
    }
}
