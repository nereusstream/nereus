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

import com.nereusstream.domain.identity.StorageEpochId;
import com.nereusstream.storage.object.read.BindingReadAsyncExecutorV1;
import com.nereusstream.storage.object.read.BindingReadAuthorityV1;
import com.nereusstream.storage.object.read.BindingReadHazardPoolV1;
import com.nereusstream.storage.object.read.BindingReadProtocolV1;
import com.nereusstream.storage.object.read.BindingReadPublicationCellV1;
import com.nereusstream.storage.object.read.BindingReadRouteTableV1;
import com.nereusstream.storage.object.read.control.M4ReadControlRecordsV1.BindingReadSelector;
import com.nereusstream.storage.object.read.control.M4ReadControlRecordsV1.SelectorMode;
import com.nereusstream.storage.object.read.control.M4ReadControlRecordsV1.SourceProtectionIdentity;
import java.util.Comparator;
import java.util.List;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.CompletionStage;
import java.util.concurrent.Executor;
import java.util.concurrent.atomic.AtomicReference;
import org.apache.bookkeeper.mledger.SourceSafeLedgerOffloader.NativeReadAdmission;

/** Actual Native admission/hazard lifetime. A failed SDK cleanup keeps its lease and cannot establish RELEASED. */
public final class PulsarNativeM4ReadOwnerV1 implements NativeReadAdmission {
    private final PulsarNativeBindingReadViewV1 view;
    private final BindingReadSelector selector;
    private final AtomicReference<BindingReadAuthorityV1> current;
    private final BindingReadHazardPoolV1 hazards;
    private final BindingReadAsyncExecutorV1 reads;
    private final CompletableFuture<Void> cleanup = new CompletableFuture<>();
    private CompletableFuture<Void> fence;
    private CompletableFuture<Void> nativeDrain;
    private volatile boolean undeliveredGrant;
    private Executor undeliveredExecutor;
    private Executor logicalTrimDrainExecutor;
    private int inFlight;
    private boolean closed;
    private final int capacity;
    private final PulsarNativeM4ControlV1 control;
    private final PulsarNativeM4DrainAdmissionV1 admission;
    private final Executor ownerEventLoop;
    private PulsarNativeM4FallbackRetirementV1 retirement;
    private CompletableFuture<Void> durableDrain;
    private PulsarNativeControlWriterV2.ReadHold readHold;
    private final PulsarNativeReadCellBudgetV1 cellBudget;
    private final PulsarNativeReadCellBudgetV1.Reservation cellReservation;

    PulsarNativeM4ReadOwnerV1(
            PulsarNativeBindingReadViewV1 view,
            BindingReadSelector selector,
            Executor ownerEventLoop,
            int capacity,
            PulsarNativeM4ControlV1 control,
            PulsarNativeM4DrainAdmissionV1 admission,
            PulsarNativeReadCellBudgetV1 cellBudget,
            PulsarNativeReadCellBudgetV1.Reservation cellReservation) {
        this.cellBudget = cellBudget;
        view.requireSelectedRoot(selector);
        this.cellReservation = cellReservation;
        this.view = view;
        this.selector = selector;
        this.capacity = capacity;
        this.control = control;
        this.admission = admission;
        this.ownerEventLoop = ownerEventLoop;
        var binding = view.bindingIdentity();
        // Native positions remain in the typed graph, including physical ledger zero; no virtual-P4 ID conversion.
        var publication = new BindingReadPublicationCellV1(
                selector.sourceGeneration(), 0, 0, new BindingReadRouteTableV1(List.of()), view);
        current = new AtomicReference<>(new BindingReadAuthorityV1(
                binding.bindingId(),
                binding.incarnationSha256(),
                new StorageEpochId(binding.storageEpochSha256()),
                BindingReadProtocolV1.PULSAR_LEDGER_ENTRY,
                selector.selectedViewSha256(),
                selector.ownerEpoch(),
                selector.readAdmissionEpoch(),
                true,
                selector.capability().generation(),
                selector.capability().evidenceSha256(),
                publication));
        hazards = new BindingReadHazardPoolV1(capacity, 4);
        reads = new BindingReadAsyncExecutorV1(ownerEventLoop, capacity);
        cellReservation.actorStarting();
    }

    void attachReadHold(PulsarNativeControlWriterV2.ReadHold original) {
        if (readHold != null) {
            throw new IllegalStateException("Native read hold is already attached");
        }
        original.requireTrimDirectory(view);
        readHold = original;
        original.actorStarted();
    }

    @Override
    public CompletionStage<Void> admitRead(
            long ledgerId, long firstEntry, long lastEntry, CompletionStage<Void> actualCleanup) {
        try {
            long count = Math.addExact(Math.subtractExact(lastEntry, firstEntry), 1);
            if (firstEntry < 0
                    || lastEntry < firstEntry
                    || count > cellBudget.readLimits().maximumEntries()) {
                throw new IllegalArgumentException("Native M4 range exceeds its reserved memory profile");
            }
        } catch (Throwable failure) {
            return CompletableFuture.failedFuture(failure);
        }
        return admitOperation(
                () -> {
                    view.requireReadRange(ledgerId, firstEntry, lastEntry);
                    if (readHold != null) {
                        readHold.requireReadAtOrAfterTrim(view, ledgerId, firstEntry);
                    }
                },
                actualCleanup);
    }

    @Override
    public int maximumReadEntries() {
        return cellBudget.readLimits().maximumEntries();
    }

    @Override
    public boolean hasPreferredReadSource(long ledgerId) {
        return view.hasPreferredReadSource(ledgerId);
    }

    @Override
    public org.apache.bookkeeper.mledger.SourceSafeLedgerOffloader.PreferredReadOperation readPreferred(
            long ledgerId,
            long firstEntry,
            long lastEntry,
            org.apache.bookkeeper.mledger.SourceSafeLedgerOffloader.NativeReadRange retained) {
        return view.readPreferred(ledgerId, firstEntry, lastEntry, retained, this::sourceUseOpen);
    }

    @Override
    public CompletionStage<Void> admitVerification(long ledgerId, CompletionStage<Void> actualCleanup) {
        return admitOperation(() -> view.requireVerificationSource(ledgerId), actualCleanup);
    }

    CompletionStage<Void> admitMetadataObservation(CompletionStage<Void> actualCleanup) {
        return admitOperation(() -> {}, actualCleanup);
    }

    private CompletionStage<Void> admitOperation(Runnable validate, CompletionStage<Void> actualCleanup) {
        var permission = new CompletableFuture<Void>();
        synchronized (this) {
            if (closed || inFlight >= capacity) {
                return CompletableFuture.failedFuture(new IllegalStateException("Native M4 owner is closed or full"));
            }
            inFlight++;
        }
        try {
            var lifetime = reads.execute(current, hazards, authority -> {
                if (!authority.admitting() || !view.isCurrent()) {
                    throw new IllegalStateException("Native M4 view/owner is no longer current");
                }
                validate.run();
                var confirmedCleanup = new CompletableFuture<Void>();
                actualCleanup.whenComplete((ignored, failure) -> {
                    if (failure == null) {
                        confirmedCleanup.complete(null);
                    } else {
                        cleanup.completeExceptionally(failure);
                        fence();
                        // Keep confirmedCleanup unresolved: a failed cleanup cannot clear the exact hazard lease.
                    }
                });
                permission.complete(null);
                return confirmedCleanup;
            });
            lifetime.whenComplete((ignored, failure) -> {
                synchronized (this) {
                    inFlight--;
                    if (closed && inFlight == 0) {
                        cleanup.complete(null);
                    }
                }
                if (failure != null) {
                    permission.completeExceptionally(failure);
                }
            });
        } catch (Throwable failure) {
            synchronized (this) {
                inFlight--;
            }
            permission.completeExceptionally(failure);
        }
        return permission.copy();
    }

    @Override
    public CompletionStage<Void> admitOwnedRead(
            long ledgerId,
            long firstEntry,
            long lastEntry,
            long maximumEntryBytes,
            long maximumBatchBytes,
            CompletionStage<Void> actualCleanup) {
        if (maximumEntryBytes > cellBudget.readLimits().maximumOwnedEntryBytes()
                || maximumBatchBytes > cellBudget.readLimits().maximumOwnedBatchBytes()) {
            return CompletableFuture.failedFuture(
                    new IllegalArgumentException("Native owned copy exceeds its reserved memory profile"));
        }
        return admitRead(ledgerId, firstEntry, lastEntry, actualCleanup);
    }

    public BindingReadAuthorityV1 authority() {
        return current.get();
    }

    BindingReadSelector selector() {
        return selector;
    }

    PulsarNativeReadCellBudgetV1 cellBudget() {
        return cellBudget;
    }

    PulsarNativeM4ControlV1 materializationControl() {
        requireMaterializationLocal();
        return control;
    }

    PulsarNativeBindingReadViewV1 materializationView() {
        requireMaterializationLocal();
        return view;
    }

    void requireMaterializationLocal() {
        if (!materializationLocalCurrent()) {
            throw new IllegalStateException("Native materialization local owner/source is no longer current");
        }
    }

    boolean materializationLocalCurrent() {
        return sourceUseOpen() && view.isCurrent();
    }

    void requireMaterializationCurrent() {
        requireMaterializationLocal();
        if (!control.coordinator().readSelector().filter(selector::equals).isPresent()) {
            throw new IllegalStateException("Native materialization owner/selector is no longer current");
        }
    }

    /** Persist this exact actor's actual read drain for a later independently constructed cold controller. */
    public synchronized CompletableFuture<Void> drainForRecovery(Executor controlExecutor) {
        try {
            java.util.Objects.requireNonNull(controlExecutor, "control executor");
            if (selector.mode() != SelectorMode.PREFERRED_ONLY && !view.materialized()) {
                throw new IllegalStateException("fallback-bearing Native owner requires fused retirement");
            }
            if (durableDrain == null || durableDrain.isCompletedExceptionally()) {
                durableDrain =
                        fence().thenCompose(ignored -> control.persistOwnerDrain(this, admission, controlExecutor));
            }
            return durableDrain.copy();
        } catch (Throwable failure) {
            return CompletableFuture.failedFuture(failure);
        }
    }

    /** Durable original read drain for a logical-floor scan; fallback/protection authority remains retained. */
    public synchronized CompletableFuture<Void> drainForLogicalTrim(Executor controlExecutor) {
        try {
            java.util.Objects.requireNonNull(controlExecutor, "control executor");
            if (logicalTrimDrainExecutor == null) {
                if (fence != null) {
                    throw new IllegalStateException("Logical trim drain must start on the original unfenced owner");
                }
                logicalTrimDrainExecutor = controlExecutor;
            }
            return fence();
        } catch (Throwable failure) {
            return CompletableFuture.failedFuture(failure);
        }
    }

    synchronized void requireConfirmedDrain(PulsarNativeM4ControlV1 expectedControl) {
        if (control != expectedControl
                || nativeDrain == null
                || !nativeDrain.isDone()
                || nativeDrain.isCompletedExceptionally()
                || nativeDrain.isCancelled()
                || current.get().admitting()
                || hazardState() != BindingReadHazardPoolV1.ScanOutcome.CLEAN) {
            throw new IllegalStateException("Native owner lacks its exact confirmed private read drain");
        }
    }

    /** Original materialization control worker only; this records actual drain, not fallback retirement/RELEASED. */
    void persistMaterializationDrain() {
        control.persistMaterializationOwnerDrain(this, admission);
    }

    synchronized PulsarNativeM4FallbackRetirementV1 prepareRetirement(
            PulsarNativeM4ControlV1 expectedControl, PulsarNativeBindingReadViewV1 preferred) {
        if (control != expectedControl) {
            throw new IllegalArgumentException("Native retirement belongs to another actual control owner");
        }
        preferred.requirePreparationBudget(cellBudget);
        if (retirement == null) {
            preferred.requirePreferredReplacementOf(view);
            retirement = new PulsarNativeM4FallbackRetirementV1(control, this, view, preferred, admission);
        } else if (!retirement.preferredView().sha256().equals(preferred.sha256())) {
            throw new IllegalArgumentException("Native retirement retry changed its exact successor");
        }
        return retirement;
    }

    List<SourceProtectionIdentity> fallbackSources() {
        if (view.materialized()) {
            return selector.mode() == SelectorMode.PREFERRED_ONLY ? List.of() : view.materializedFallbackSources();
        }
        return view.fallbackSourceIdentities().stream()
                .map(source -> new SourceProtectionIdentity(
                        source, 1, selector.readAdmissionEpoch(), selector.sourceGeneration(), selector.capability()))
                .sorted(Comparator.comparing(
                        source -> source.sourceIdentitySha256().toHex()))
                .toList();
    }

    BindingReadHazardPoolV1 hazards() {
        return hazards;
    }

    CompletableFuture<PulsarNativeM4ReadOwnerV1> registerSuccessor(
            PulsarNativeBindingReadViewV1 preferred, BindingReadSelector successor, Executor controlExecutor) {
        var reservation = control.reserveOwnerBudget(preferred, cellBudget, capacity);
        var started = new java.util.concurrent.atomic.AtomicReference<PulsarNativeM4ReadOwnerV1>();
        return reservation
                .ready()
                .thenCompose(unused -> control.openSuccessorReadHold(
                        preferred,
                        successor,
                        cellBudget,
                        retirement.bookKeeper(),
                        hold -> {
                            var actor = new PulsarNativeM4ReadOwnerV1(
                                    preferred,
                                    successor,
                                    ownerEventLoop,
                                    capacity,
                                    control,
                                    admission,
                                    cellBudget,
                                    reservation);
                            started.set(actor);
                            if (hold != null) {
                                actor.attachReadHold(hold);
                            }
                            preferred.installReadAdmission(actor);
                            return actor;
                        },
                        controlExecutor))
                .exceptionallyCompose(
                        failure -> control.cleanupFailedOwner(failure, reservation, started.get(), controlExecutor));
    }

    @Override
    public boolean sourceUseOpen() {
        return current.get().admitting() && view.isCurrent();
    }

    public BindingReadHazardPoolV1.ScanOutcome hazardState() {
        var authority = current.get();
        return hazards.scan(authority.bindingId(), authority.sourceGeneration());
    }

    /** One-way local close waits for SDK cleanup, callbacks and downstream Entry aliases. Emits no proof. */
    public CompletableFuture<Void> fence() {
        synchronized (this) {
            if (fence != null && !fence.isCompletedExceptionally()) {
                return fence.copy();
            }
            fence = new CompletableFuture<>();
            closed = true;
            current.updateAndGet(authority -> new BindingReadAuthorityV1(
                    authority.bindingId(),
                    authority.topicIncarnationIdentity(),
                    authority.storageEpochId(),
                    authority.protocol(),
                    authority.selectedViewSha256(),
                    authority.ownerEpoch(),
                    authority.readAdmissionEpoch(),
                    false,
                    authority.capabilityGeneration(),
                    authority.capabilityEvidenceSha256(),
                    authority.publicationCell()));
            hazards.closeAdmission();
            if (inFlight == 0) {
                cleanup.complete(null);
            }
        }
        var nativeCleanup = view.closePreferredReads().thenCompose(ignored -> view.fenceOwnerReadAdmission());
        final CompletableFuture<Void> attempt;
        final CompletableFuture<Void> drained;
        synchronized (this) {
            // The registered owner also owns mapped SDK reads outside ManagedLedger's native handle tracker.
            // Keep the original source/preparation charge through those exact hazards and downstream aliases.
            nativeDrain =
                    CompletableFuture.allOf(nativeCleanup, cleanup).thenCompose(ignored -> view.closePreparation());
            drained = nativeDrain;
            attempt = fence;
        }
        drained.thenCompose(ignored ->
                        readHold == null ? CompletableFuture.completedFuture(null) : readHold.closeAfterNativeDrain())
                .thenCompose(ignored -> undeliveredGrant || logicalTrimDrainExecutor != null
                        ? control.persistOwnerDrain(
                                this, admission, undeliveredGrant ? undeliveredExecutor : logicalTrimDrainExecutor)
                        : CompletableFuture.completedFuture(null))
                .thenCompose(ignored -> cellReservation.releaseAfterDrain())
                .whenComplete((ignored, failure) -> {
                    if (failure == null) {
                        attempt.complete(null);
                    } else {
                        attempt.completeExceptionally(failure);
                    }
                });
        return attempt.copy();
    }

    /** Original undelivered grant cleanup also persists this exact actor's confirmed read drain. */
    synchronized CompletableFuture<Void> fenceUndelivered(Executor controlExecutor) {
        boolean alreadyFencing = fence != null && !fence.isCompletedExceptionally();
        if (undeliveredExecutor == null) {
            undeliveredExecutor = java.util.Objects.requireNonNull(controlExecutor, "original cleanup executor");
        }
        undeliveredGrant = true;
        var original = fence();
        // Native reads may have fenced this exact actor before its opening observer could receive it.
        return alreadyFencing
                ? original.thenCompose(ignored -> control.persistOwnerDrain(this, admission, undeliveredExecutor))
                : original;
    }
}
