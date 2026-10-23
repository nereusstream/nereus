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
import com.nereusstream.storage.api.lifecycle.PhysicalResourceIdCodecV2;
import com.nereusstream.storage.bookkeeper.M5BookKeeperNamespaceAuthorityV2;
import com.nereusstream.storage.object.materialization.M5BytePreservingMaterializerV1;
import com.nereusstream.storage.object.materialization.M5MaterializationRecordsV1.ProtocolCoverage;
import com.nereusstream.storage.object.materialization.M5MaterializationRecordsV1.SourceExtent;
import com.nereusstream.storage.object.materialization.M5MaterializationRecordsV1.SourceKind;
import com.nereusstream.storage.object.materialization.M5PulsarCoverageV2;
import com.nereusstream.storage.object.materialization.M5PulsarEntriesV2;
import io.netty.buffer.Unpooled;
import java.io.IOException;
import java.nio.ByteBuffer;
import java.security.MessageDigest;
import java.util.ArrayList;
import java.util.HexFormat;
import java.util.List;
import java.util.Optional;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.Executor;
import org.apache.bookkeeper.mledger.impl.ManagedLedgerImpl.CapturedReadLimits;
import org.apache.bookkeeper.mledger.impl.ManagedLedgerImpl.CapturedReadOperation;
import org.apache.pulsar.common.protocol.Commands;

/** Actual owner-backed sealed source and complete-plan preparation. Output/lifecycle admission remains separate. */
public final class PulsarNativeMaterializationSourceV2
        implements M5BytePreservingMaterializerV1.SourceReader, AutoCloseable {
    private final PulsarNativeM4ReadOwnerV1 owner;
    private final PulsarNativeBindingReadViewV1 view;
    private final PulsarNativeM4ControlV1 control;
    private final M5BookKeeperNamespaceAuthorityV2 bookKeeper;
    private final NereusPulsarLedgerOffloaderV1 offloader;
    private final PulsarNativeReadCellBudgetV1.Reservation workspace;
    private final CapturedReadLimits limits;
    private final Executor executor;
    private final CompletableFuture<Void> preparationSettled = new CompletableFuture<>();
    private M5PulsarCoverageV2 coverage;
    private List<SourceExtent> sources = List.of();
    private volatile com.nereusstream.metadata.oxia.v2.retention.OxiaPulsarMaterializationInputV2 nativeInput;
    private volatile boolean inputConfirmed;
    private volatile boolean closed;
    private boolean workspaceStarted;
    private final java.util.Set<CompletableFuture<Void>> operations = new java.util.HashSet<>();
    private Cursor activeCursor;
    private CompletableFuture<Void> closing;
    private S3PulsarOffloadObjectStoreV1.MaterializationConnection providerConnection;
    private boolean providerStarting;
    private PulsarNativeMaterializationTaskV2 task;

    private PulsarNativeMaterializationSourceV2(
            PulsarNativeM4ReadOwnerV1 owner,
            M5BookKeeperNamespaceAuthorityV2 bookKeeper,
            NereusPulsarLedgerOffloaderV1 offloader,
            Executor executor) {
        this.executor = executor;
        this.owner = owner;
        view = owner.materializationView();
        control = owner.materializationControl();
        this.bookKeeper = bookKeeper;
        this.offloader = offloader;
        var budget = owner.cellBudget();
        limits = new CapturedReadLimits(
                1,
                budget.readLimits().maximumOwnedEntryBytes(),
                budget.readLimits().maximumOwnedEntryBytes());
        workspace = budget.reserveMaterializationSource(
                view.binding(), view.canonicalBytes().length());
    }

    public static CompletableFuture<PulsarNativeMaterializationSourceV2> capture(
            PulsarNativeM4ReadOwnerV1 owner,
            M5BookKeeperNamespaceAuthorityV2 bookKeeper,
            NereusPulsarLedgerOffloaderV1 offloader,
            List<M5PulsarCoverageV2.EntryRange> ranges,
            Executor executor) {
        var observer = new CompletableFuture<PulsarNativeMaterializationSourceV2>();
        final PulsarNativeMaterializationSourceV2 original;
        final List<M5PulsarCoverageV2.EntryRange> selected;
        try {
            selected = List.copyOf(ranges);
            if (selected.isEmpty() || selected.size() > M5PulsarCoverageV2.MAX_RANGES) {
                throw new IllegalArgumentException("Native materialization range count exceeds its cap");
            }
            original = new PulsarNativeMaterializationSourceV2(owner, bookKeeper, offloader, executor);
        } catch (Throwable failure) {
            return CompletableFuture.failedFuture(failure);
        }
        original.workspace
                .ready()
                .thenRunAsync(
                        () -> {
                            try {
                                if (observer.isDone()) {
                                    throw new java.util.concurrent.CancellationException(
                                            "Native source observer ended before dispatch");
                                }
                                original.workspace.sourceStarting();
                                original.workspaceStarted = true;
                                original.prepare(selected);
                                // A caller may synchronously close from delivery; all preparation SDK work is already
                                // settled.
                                original.preparationSettled.complete(null);
                                if (!observer.complete(original)) {
                                    original.cleanupUndelivered();
                                }
                            } catch (Throwable failure) {
                                original.cleanupUndelivered();
                                observer.completeExceptionally(failure);
                            } finally {
                                original.preparationSettled.complete(null);
                            }
                        },
                        executor)
                .whenComplete((ignored, failure) -> {
                    if (failure != null) {
                        original.preparationSettled.complete(null);
                        original.cleanupUndelivered();
                        observer.completeExceptionally(failure);
                    }
                });
        observer.whenComplete((ignored, failure) -> {
            if (observer.isCancelled()) {
                original.cleanupUndelivered();
            }
        });
        return observer;
    }

    private void prepare(List<M5PulsarCoverageV2.EntryRange> ranges) throws Exception {
        verifyNative();
        var directory = view.materializationChain();
        coverage = M5PulsarCoverageV2.of(view.bindingIdentity(), view.sha256(), directory, ranges);
        var groups = new java.util.ArrayList<SourceGroup>();
        for (var slice : view.materializationSlices(coverage.ranges())) {
            var mapped = view.materializedOutput(
                            new M5PulsarCoverageV2.Position(slice.ledgerId(), slice.startEntryInclusive()))
                    .orElse(null);
            var slot = mapped == null
                    ? view.materializationDirectory().stream()
                            .filter(value -> value.ledgerId() == slice.ledgerId())
                            .findFirst()
                            .orElseThrow()
                    : null;
            var base = Sha256Digest.hash(mapped == null ? slot.encode() : mapped.encode());
            SourceGroup group;
            if (!groups.isEmpty() && groups.get(groups.size() - 1).base.equals(base)) {
                group = groups.get(groups.size() - 1);
                var last = group.ranges.get(group.ranges.size() - 1);
                if (last.ledgerId() == slice.ledgerId()) {
                    group.ranges.set(
                            group.ranges.size() - 1,
                            new M5PulsarCoverageV2.EntryRange(
                                    slice.ledgerId(), last.startEntryInclusive(), slice.endEntryExclusive()));
                } else {
                    group.ranges.add(slice);
                }
            } else {
                group = new SourceGroup(base, slot, mapped);
                group.ranges.add(slice);
                groups.add(group);
            }
            if (groups.size()
                    > com.nereusstream.storage.object.materialization.M5MaterializationRecordsV1.MAX_SOURCES) {
                throw new IllegalArgumentException("Native materialization source count exceeds its declared cap");
            }
        }
        List<SourceExtent> actual = new ArrayList<>();
        for (var group : groups) {
            var slot = group.nativeSource;
            var mapped = group.mapped;
            var selected = coverage.withRanges(group.ranges);
            // Repeated physical objects separated by another route are distinct exact source extents.
            var identity = groups.stream()
                                    .filter(value -> value.base.equals(group.base))
                                    .count()
                            == 1
                    ? group.base
                    : Sha256Digest.hash(CanonicalBytes.copyOf(ByteBuffer.allocate(64)
                            .put(group.base.bytes().toByteArray())
                            .put(selected.sha256().bytes().toByteArray())
                            .array()));
            var commitment = new M5PulsarEntriesV2.Commitment(selected);
            MessageDigest canonical = MessageDigest.getInstance("SHA-256");
            long bytes = 0;
            long minimum = Long.MAX_VALUE;
            long maximum = -1;
            try (var entries = open(selected)) {
                for (var entry = entries.next(); entry.isPresent(); entry = entries.next()) {
                    var value = entry.orElseThrow();
                    commitment.accept(value);
                    byte[] header = ByteBuffer.allocate(20)
                            .putLong(value.position().ledgerId())
                            .putLong(value.position().entryId())
                            .putInt(value.payload().length())
                            .array();
                    canonical.update(header);
                    canonical.update(value.payload().toByteArray());
                    bytes = Math.addExact(
                            bytes, Math.addExact(20, value.payload().length()));
                    minimum = Math.min(minimum, value.timestamp());
                    maximum = Math.max(maximum, value.timestamp());
                }
            }
            var entryRoot = commitment.finish();
            var policy = view.binding().authority().aggregate().canonicalStoredDigest();
            if (mapped != null) {
                var data = mapped.output().orElseThrow();
                actual.add(new SourceExtent(
                        SourceKind.PULSAR_NMS1_DATA_V2,
                        identity,
                        ProtocolCoverage.pulsar(selected),
                        data.key(),
                        data.bodyLength(),
                        Math.toIntExact(selected.entryCount()),
                        minimum,
                        maximum,
                        data.bodySha256(),
                        Optional.of(CanonicalUtf8.fromString(mapped.resource()
                                        .orElseThrow()
                                        .immutableIdentity()
                                        .value())
                                .bytes()),
                        Optional.empty(),
                        identity,
                        policy,
                        true,
                        false,
                        List.of(view.bindingIdentity().bindingId().digest()),
                        Optional.of(entryRoot)));
            } else if (slot.offloaded() == null) {
                actual.add(new SourceExtent(
                        SourceKind.BOOKKEEPER_LEDGER,
                        identity,
                        ProtocolCoverage.pulsar(selected),
                        "bookkeeper-ledger:" + slot.ledgerId(),
                        bytes,
                        Math.toIntExact(selected.entryCount()),
                        minimum,
                        maximum,
                        Sha256Digest.copyOf(canonical.digest()),
                        Optional.empty(),
                        Optional.of(Sha256Digest.hash(PhysicalResourceIdCodecV2.encode(
                                slot.bookKeeperMember().orElseThrow()))),
                        identity,
                        policy,
                        false,
                        false,
                        List.of(view.bindingIdentity().bindingId().digest()),
                        Optional.of(entryRoot)));
            } else {
                var data = slot.offloaded().objects().data();
                actual.add(new SourceExtent(
                        SourceKind.PULSAR_NPD1_DATA,
                        identity,
                        ProtocolCoverage.pulsar(selected),
                        data.key(),
                        data.length(),
                        Math.toIntExact(selected.entryCount()),
                        minimum,
                        maximum,
                        Sha256Digest.copyOf(HexFormat.of().parseHex(data.sha256())),
                        Optional.of(CanonicalUtf8.fromString(data.version().substring(8))
                                .bytes()),
                        Optional.empty(),
                        identity,
                        policy,
                        true,
                        false,
                        List.of(view.bindingIdentity().bindingId().digest()),
                        Optional.of(entryRoot)));
            }
        }
        verifyNative();
        sources = List.copyOf(actual);
        var original = beginOperation();
        try {
            var persistence = control.persistMaterializationInput(view, coverage, value -> nativeInput = value);
            persistence.whenComplete((ignored, failure) -> original.complete(null));
            nativeInput = persistence.get();
            inputConfirmed = true;
            owner.requireMaterializationCurrent();
            requireOpen();
        } catch (Exception failure) {
            throw new IOException("Native original input persistence remains unresolved", failure);
        }
    }

    private static final class SourceGroup {
        private final Sha256Digest base;
        private final PulsarNativeBindingReadViewV1.Slot nativeSource;
        private final com.nereusstream.storage.object.gc.M5ObjectBirthCreditV3 mapped;
        private final List<M5PulsarCoverageV2.EntryRange> ranges = new java.util.ArrayList<>();

        private SourceGroup(
                Sha256Digest base,
                PulsarNativeBindingReadViewV1.Slot nativeSource,
                com.nereusstream.storage.object.gc.M5ObjectBirthCreditV3 mapped) {
            this.base = base;
            this.nativeSource = nativeSource;
            this.mapped = mapped;
        }
    }

    public M5PulsarCoverageV2 coverage() {
        return coverage;
    }

    public List<SourceExtent> sources() {
        return sources;
    }

    M5BookKeeperNamespaceAuthorityV2 bookKeeper() {
        return bookKeeper;
    }

    public com.nereusstream.metadata.oxia.v2.retention.OxiaPulsarMaterializationInputV2 nativeInput() {
        requireOpen();
        return java.util.Objects.requireNonNull(nativeInput, "original persisted Native input");
    }

    /** Original source/Cell workspace covers configuration, SDK loan and exact scope admission before any output. */
    CompletableFuture<S3PulsarOffloadObjectStoreV1.MaterializationConnection> qualifyProvider(
            long maximumObjectBytes, Executor executor) {
        var observer = new CompletableFuture<S3PulsarOffloadObjectStoreV1.MaterializationConnection>();
        final CompletableFuture<Void> original;
        final CanonicalBytes cellOwner;
        synchronized (this) {
            requireOpen();
            if (providerStarting || providerConnection != null) {
                throw new IllegalStateException("Original Native source already owns its Provider attempt");
            }
            cellOwner = owner.cellBudget().nativeOwnerIdentity(view.binding());
            original = beginOperation();
            providerStarting = true;
        }
        try {
            CompletableFuture.runAsync(
                    () -> {
                        CompletableFuture<?> metadata = CompletableFuture.completedFuture(null);
                        var localSettled = new CompletableFuture<Void>();
                        S3PulsarOffloadObjectStoreV1.MaterializationConnection delivered = null;
                        Throwable rejected = null;
                        try {
                            if (observer.isDone()) {
                                throw new java.util.concurrent.CancellationException(
                                        "Provider observer ended before dispatch");
                            }
                            owner.requireMaterializationCurrent();
                            metadata = owner.cellBudget()
                                    .requireNativeOwner(view.binding(), cellOwner)
                                    .thenCompose(ignored -> view.binding().revalidate())
                                    .toCompletableFuture();
                            metadata.get();
                            requireOpen();
                            if (observer.isDone()) {
                                throw new java.util.concurrent.CancellationException(
                                        "Provider observer ended before SDK loan");
                            }
                            var connection = offloader.borrowConfiguredMaterializationConnection(
                                    view.binding(), view.objectNamespace(), maximumObjectBytes);
                            synchronized (this) {
                                providerConnection = connection; // Retain before any original catalogue mutation.
                            }
                            metadata = connection.admitProfile(view.binding());
                            metadata.get();
                            owner.requireMaterializationCurrent();
                            requireOpen();
                            delivered = connection;
                        } catch (Throwable failure) {
                            rejected = new IOException("Original Native Provider admission is unresolved", failure);
                        } finally {
                            metadata.handle((ignored, failure) -> null)
                                    .thenCompose(ignored -> localSettled)
                                    .thenRun(() -> original.complete(null));
                            localSettled.complete(
                                    null); // Also precedes failure delivery; callback-close cannot wait on us.
                        }
                        if (rejected == null) {
                            observer.complete(delivered);
                        } else {
                            observer.completeExceptionally(rejected);
                        }
                    },
                    executor);
        } catch (Throwable rejected) {
            synchronized (this) {
                providerStarting = false;
            }
            original.complete(null); // Executor rejected before dispatch.
            observer.completeExceptionally(rejected);
        }
        return observer;
    }

    /** Original actual SDK metadata only. This is not a worker grant, cursor checkpoint or reference transfer. */
    public com.nereusstream.storage.object.materialization.M5PulsarRecoveryInputV2 recoveryInput() {
        requireOpen();
        return view.materializationRecoveryInput();
    }

    /** Prepares one original Task under this Source's already qualified Provider and charged Cell workspace. */
    public CompletableFuture<PulsarNativeMaterializationTaskV2> prepareTask(
            boolean forceRewrite,
            List<com.nereusstream.storage.object.materialization.M5MaterializationRecordsV1.IndexKind> indexes,
            com.nereusstream.storage.object.materialization.M5MaterializationAdmissionV1.Caps caps,
            Executor executor) {
        var observer = new CompletableFuture<PulsarNativeMaterializationTaskV2>();
        final PulsarNativeMaterializationTaskV2 original;
        synchronized (this) {
            requireOpen();
            view.requireSuccessorMapCapacity();
            if (task != null || providerConnection == null) {
                throw new IllegalStateException("Original Native Task requires its sole qualified Provider attempt");
            }
            original =
                    new PulsarNativeMaterializationTaskV2(this, owner, providerConnection, forceRewrite, indexes, caps);
            task = original; // Own before any shared Cell, full-plan or grant mutation.
        }
        try {
            CompletableFuture.runAsync(
                    () -> {
                        Throwable rejected = null;
                        try {
                            if (observer.isDone()) {
                                throw new java.util.concurrent.CancellationException(
                                        "Task observer ended before dispatch");
                            }
                            original.prepare();
                        } catch (Throwable failure) {
                            rejected = failure;
                        } finally {
                            original.preparationSettled();
                        }
                        if (rejected == null) {
                            if (!observer.complete(original)) {
                                closeAsync();
                            }
                        } else {
                            if (!observer.completeExceptionally(rejected)) {
                                closeAsync();
                            }
                        }
                    },
                    executor);
        } catch (Throwable rejected) {
            original.preparationSettled();
            observer.completeExceptionally(rejected);
        }
        observer.whenComplete((ignored, failure) -> {
            if (observer.isCancelled()) {
                closeAsync();
            }
        });
        return observer;
    }

    void requireTaskSourceOpen() {
        requireOpen();
    }

    @Override
    public CanonicalBytes readExact(SourceExtent source) throws IOException {
        throw new IOException("Actual Native source must be read by original ledger/entry");
    }

    @Override
    public void verifyPulsarSource(SourceExtent source) throws IOException {
        requireOriginal(source);
        verifyNative();
    }

    @Override
    public M5PulsarEntriesV2.Cursor openPulsarEntries(SourceExtent source) throws IOException {
        requireOriginal(source);
        verifyNative();
        return open(source.coverage().pulsarCoverage().orElseThrow());
    }

    private void requireOriginal(SourceExtent source) throws IOException {
        if (!sources.contains(source)) {
            throw new IOException("Native source extent is not this original captured source");
        }
    }

    private void verifyNative() throws IOException {
        final CompletableFuture<Void> qualification;
        try {
            qualification = beginOperation(); // Register before durable selector/P1/native/provider I/O.
        } catch (Exception rejected) {
            throw new IOException("Native materialization source qualification remains unresolved", rejected);
        }
        CompletableFuture<Void> metadata = CompletableFuture.completedFuture(null);
        var localSettled = new CompletableFuture<Void>();
        try {
            owner.requireMaterializationCurrent();
            metadata = view.verifyMaterializationSources(bookKeeper, offloader);
            metadata.get();
            for (var slot : view.materializationDirectory()) {
                if (slot.offloaded() != null && !slot.activeTail()) {
                    var original = beginOperation();
                    var verification = view.verifyOwnedObjectSource(slot.ledgerId());
                    verification.drained().whenComplete((unused, failure) -> {
                        if (failure == null) {
                            original.complete(null);
                        } else {
                            original.completeExceptionally(failure);
                        }
                    });
                    verification.result().get();
                    original.get();
                }
            }
            owner.requireMaterializationCurrent();
            requireOpen();
        } catch (Exception failure) {
            throw new IOException("Native materialization source qualification remains unresolved", failure);
        } finally {
            // Interruption/observer failure cannot refund before the original metadata callback ends.
            metadata.handle((ignored, failure) -> null)
                    .thenCompose(ignored -> localSettled)
                    .thenRun(() -> qualification.complete(null));
            localSettled.complete(null);
        }
    }

    private synchronized void requireOpen() {
        if (closed) {
            throw new IllegalStateException("Native materialization source is closed");
        }
        owner.requireMaterializationLocal();
    }

    private synchronized Cursor open(M5PulsarCoverageV2 selected) throws IOException {
        requireOpen();
        if (activeCursor != null) {
            throw new IOException("Native materialization source already has an owned cursor");
        }
        activeCursor = new Cursor(selected);
        return activeCursor;
    }

    private void cleanupUndelivered() {
        workspace.retainUndeliveredSdk(this::closeAsync, false);
        closeAsync();
    }

    private synchronized CompletableFuture<Void> beginOperation() {
        requireOpen();
        if (operations.size() >= 2) {
            throw new IllegalStateException("Native source cleanup slots remain occupied");
        }
        var original = new CompletableFuture<Void>();
        operations.add(original);
        original.whenComplete((ignored, failure) -> {
            if (failure == null) {
                synchronized (PulsarNativeMaterializationSourceV2.this) {
                    operations.remove(original);
                }
            }
        });
        return original;
    }

    public synchronized CompletableFuture<Void> closeAsync() {
        closed = true;
        if (task != null) {
            task.stop();
        }
        if (activeCursor != null) {
            activeCursor.cursorClosed = true;
        }
        if (closing == null || closing.isCompletedExceptionally()) {
            closing = preparationSettled
                    .thenCompose(ignored -> {
                        final Cursor cursor;
                        synchronized (PulsarNativeMaterializationSourceV2.this) {
                            cursor = activeCursor;
                        }
                        return cursor != null && cursor.hasPendingMappedRead()
                                ? CompletableFuture.runAsync(cursor::reconcileMappedRead, executor)
                                : CompletableFuture.completedFuture(null);
                    })
                    .thenCompose(ignored -> {
                        synchronized (PulsarNativeMaterializationSourceV2.this) {
                            return CompletableFuture.allOf(operations.toArray(CompletableFuture[]::new));
                        }
                    })
                    .thenCompose(ignored -> confirmOriginalInput())
                    .thenCompose(
                            ignored -> task == null ? CompletableFuture.completedFuture(null) : task.closeUnstarted())
                    .thenCompose(ignored -> closeOriginalProvider())
                    .thenCompose(ignored ->
                            workspaceStarted ? workspace.releaseAfterDrain() : workspace.releaseBeforeActor());
        }
        return closing.copy();
    }

    private CompletableFuture<Void> confirmOriginalInput() {
        var candidate = nativeInput;
        if (candidate == null || inputConfirmed) {
            return CompletableFuture.completedFuture(null);
        }
        // UNKNOWN keeps the original candidate and Cell workspace. Never recapture or allocate another identity.
        return control.confirmMaterializationInput(candidate).thenRun(() -> inputConfirmed = true);
    }

    private CompletableFuture<Void> closeOriginalProvider() {
        final S3PulsarOffloadObjectStoreV1.MaterializationConnection connection;
        synchronized (this) {
            connection = providerConnection;
        }
        return connection == null
                ? CompletableFuture.completedFuture(null)
                : connection.reconcileScopeForClose().thenRun(connection::close);
    }

    @Override
    public void close() throws Exception {
        closeAsync().get();
    }

    private final class Cursor implements M5PulsarEntriesV2.Cursor {
        private final M5PulsarCoverageV2 selected;
        private Optional<M5PulsarCoverageV2.Position> next;
        private CapturedReadOperation operation;
        private PulsarNativeMaterializedReadV2.SourceRead mappedRead;
        private boolean cursorClosed;
        private CompletableFuture<Void> originalWork;
        private boolean reading;

        private Cursor(M5PulsarCoverageV2 selected) {
            this.selected = selected;
            var range = selected.ranges().get(0);
            next = Optional.of(new M5PulsarCoverageV2.Position(range.ledgerId(), range.startEntryInclusive()));
        }

        @Override
        public Optional<M5PulsarEntriesV2.Entry> next() throws IOException {
            final M5PulsarCoverageV2.Position position;
            final CompletableFuture<Void> work;
            final CapturedReadOperation accepted;
            var delivery = new CompletableFuture<Void>();
            synchronized (PulsarNativeMaterializationSourceV2.this) {
                requireOpen();
                if (cursorClosed || reading) {
                    throw new IOException("Native source cursor is closed or has an original read pending");
                }
                if (next.isEmpty()) {
                    return Optional.empty();
                }
                position = next.orElseThrow();
                work = beginOperation();
                originalWork = work;
                reading = true;
                if (view.materializedOutput(position).isEmpty()) {
                    operation = view.readOwnedOperation(
                            position.ledgerId(),
                            position.entryId(),
                            position.entryId(),
                            limits,
                            () -> !closed && owner.sourceUseOpen());
                    accepted = operation;
                    accepted.drained().thenCompose(ignored -> delivery).whenComplete((ignored, failure) -> {
                        if (failure == null) {
                            work.complete(null);
                        } else {
                            work.completeExceptionally(failure);
                        }
                    });
                } else {
                    accepted = null;
                }
            }
            try {
                final CanonicalBytes payload;
                if (accepted == null) {
                    payload = readMapped(position, work, delivery);
                } else {
                    var batch = accepted.result().get();
                    accepted.drained().get();
                    if (batch.ledgerId() != position.ledgerId()
                            || batch.entries().size() != 1
                            || batch.entries().get(0).entryId() != position.entryId()) {
                        throw new IOException("Native SDK returned another original entry");
                    }
                    payload = CanonicalBytes.copyOf(batch.entries().get(0).payload());
                }
                var buffer = Unpooled.wrappedBuffer(payload.toByteArray());
                final long timestamp;
                try {
                    var metadata = Commands.parseMessageMetadata(buffer);
                    if (!metadata.hasPublishTime()) {
                        throw new IOException("Native Pulsar publish timestamp is unavailable");
                    }
                    timestamp = metadata.getPublishTime();
                } finally {
                    buffer.release();
                }
                var result = new M5PulsarEntriesV2.Entry(position, payload, timestamp);
                synchronized (PulsarNativeMaterializationSourceV2.this) {
                    next = selected.firstAtOrAfter(
                            new M5PulsarCoverageV2.Position(position.ledgerId(), Math.addExact(position.entryId(), 1)));
                }
                return Optional.of(result);
            } catch (Exception failure) {
                throw new IOException("Native original entry read remains unresolved", failure);
            } finally {
                synchronized (PulsarNativeMaterializationSourceV2.this) {
                    reading = false;
                }
                delivery.complete(null);
            }
        }

        private CanonicalBytes readMapped(
                M5PulsarCoverageV2.Position position, CompletableFuture<Void> work, CompletableFuture<Void> delivery)
                throws Exception {
            var backend = new CompletableFuture<Void>();
            var cleanup = backend.thenCompose(ignored -> delivery);
            PulsarNativeMaterializedReadV2.SourceRead accepted = null;
            cleanup.whenComplete((ignored, failure) -> {
                if (failure == null) {
                    work.complete(null);
                } else {
                    work.completeExceptionally(failure);
                }
            });
            try {
                owner.admitRead(position.ledgerId(), position.entryId(), position.entryId(), cleanup)
                        .toCompletableFuture()
                        .get();
                var original = view.readMaterializationEntry(
                        position.ledgerId(), position.entryId(), () -> !closed && owner.sourceUseOpen());
                accepted = original;
                synchronized (PulsarNativeMaterializationSourceV2.this) {
                    mappedRead = original;
                }
                original.observation().drained().whenComplete((ignored, failure) -> {
                    if (failure == null) {
                        backend.complete(null);
                    } else {
                        backend.completeExceptionally(failure);
                    }
                });
                var entries =
                        original.observation().result().toCompletableFuture().get();
                try {
                    if (entries.size() != 1
                            || entries.get(0).getLedgerId() != position.ledgerId()
                            || entries.get(0).getEntryId() != position.entryId()
                            || entries.get(0).getLength() > limits.maximumEntryBytes()) {
                        throw new IOException("Actual mapped Source entry differs from its original position/bound");
                    }
                    return CanonicalBytes.copyOf(entries.get(0).getData());
                } finally {
                    entries.forEach(org.apache.bookkeeper.mledger.Entry::release);
                }
            } finally {
                if (accepted == null) {
                    backend.complete(null); // No accepted SDK read.
                }
            }
        }

        private boolean hasPendingMappedRead() {
            synchronized (PulsarNativeMaterializationSourceV2.this) {
                return mappedRead != null
                        && !mappedRead
                                .observation()
                                .drained()
                                .toCompletableFuture()
                                .isDone();
            }
        }

        private void reconcileMappedRead() {
            final PulsarNativeMaterializedReadV2.SourceRead original;
            synchronized (PulsarNativeMaterializationSourceV2.this) {
                original = mappedRead;
            }
            if (original != null) {
                original.reconcile().run();
            }
        }

        @Override
        public void close() throws IOException {
            try {
                reconcileMappedRead();
                final CompletableFuture<Void> drain;
                synchronized (PulsarNativeMaterializationSourceV2.this) {
                    cursorClosed = true;
                    drain = originalWork == null ? CompletableFuture.completedFuture(null) : originalWork;
                }
                drain.get();
                synchronized (PulsarNativeMaterializationSourceV2.this) {
                    if (activeCursor == this) {
                        activeCursor = null;
                    }
                }
            } catch (Exception failure) {
                throw new IOException("Native source cursor cleanup remains unresolved", failure);
            }
        }
    }
}
