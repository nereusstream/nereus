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

package com.nereusstream.kafka.bookkeeper.compaction;

import com.nereusstream.domain.bytes.CanonicalBytes;
import com.nereusstream.domain.bytes.Sha256Digest;
import com.nereusstream.storage.api.bookkeeper.BookKeeperCellSession;
import com.nereusstream.storage.api.lifecycle.PhysicalResourceIdV2;
import com.nereusstream.storage.object.gc.M5TargetDeleteAuthorityRecordsV1.ProofBoundWriterClassV1;
import com.nereusstream.storage.object.gc.M5TargetDeleteMultiWriterGuardV2;
import com.nereusstream.storage.object.gc.M5TargetDeleteMultiWriterGuardV2.Completion;
import com.nereusstream.storage.object.gc.M5TargetDeleteMultiWriterGuardV2.Context;
import com.nereusstream.storage.object.read.BindingReadAuthorityV1;
import com.nereusstream.storage.object.read.BindingReadHazardPoolV1;
import com.nereusstream.storage.object.read.control.BindingReadSelectorRuntimeV1;
import com.nereusstream.storage.object.read.control.M4ReadControlCodecV1;
import com.nereusstream.storage.object.read.control.M4ReadControlCoordinatorV1;
import com.nereusstream.storage.object.read.control.M4ReadControlCoordinatorV1.Outcome;
import com.nereusstream.storage.object.read.control.M4ReadControlRecordsV1.AdmissionState;
import com.nereusstream.storage.object.read.control.M4ReadControlRecordsV1.BindingReadSelector;
import com.nereusstream.storage.object.read.control.M4ReadControlRecordsV1.ClosureAnchor;
import com.nereusstream.storage.object.read.control.M4ReadControlRecordsV1.SelectorMode;
import com.nereusstream.storage.object.read.control.M4ReadControlRecordsV1.SourceProtectionIdentity;
import java.nio.ByteBuffer;
import java.util.List;
import java.util.Objects;
import java.util.Optional;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.CompletionStage;
import java.util.concurrent.Executor;
import java.util.function.Function;
import java.util.function.Supplier;

/**
 * Structured lifetime for one selected BK read owner. All physical tickets precede session creation and outlive
 * actual IO/session termination. The caller supplies the admitted native Binding route and bounded owner executor.
 * This local drain is not a global protocol-owner proof, an M4 terminal, a RELEASED record or delete authority.
 */
public final class KafkaBookKeeperReadOwnerV2 {
    /** Constructor is private: only exact local closure plus actual session termination can produce this value. */
    public static final class DrainEvidence {
        private final BindingReadSelector predecessor;
        private final BindingReadSelector successor;
        private final ClosureAnchor anchor;

        private DrainEvidence(BindingReadSelector predecessor, BindingReadSelector successor, ClosureAnchor anchor) {
            this.predecessor = predecessor;
            this.successor = successor;
            this.anchor = anchor;
        }

        public BindingReadSelector predecessor() {
            return predecessor;
        }

        public BindingReadSelector successor() {
            return successor;
        }

        public ClosureAnchor anchor() {
            return anchor;
        }
    }

    /** Metadata-only retry handle, created only after this invocation's IO and owned session have terminated. */
    public static final class TicketCleanupException extends IllegalStateException {
        private static final long serialVersionUID = 1L;
        private final M5TargetDeleteMultiWriterGuardV2 guard;
        private final List<PhysicalResourceIdV2> resources;
        private final Context context;
        private final Sha256Digest operationId;
        private final Sha256Digest terminalProof;

        private TicketCleanupException(
                M5TargetDeleteMultiWriterGuardV2 guard,
                List<PhysicalResourceIdV2> resources,
                Context context,
                Sha256Digest operationId,
                Sha256Digest terminalProof,
                Throwable workFailure) {
            super("BK read owner terminated but physical ticket cleanup is unresolved", workFailure);
            this.guard = guard;
            this.resources = List.copyOf(resources);
            this.context = context;
            this.operationId = operationId;
            this.terminalProof = terminalProof;
        }

        public Sha256Digest operationId() {
            return operationId;
        }

        /** Returns targets still unresolved after one bounded pass; cancellation cannot cancel internal cleanup. */
        public CompletionStage<List<PhysicalResourceIdV2>> reconcileTickets() {
            return guard.reconcileOperation(resources, context, operationId, terminalProof)
                    .thenApply(left -> left);
        }
    }

    private record WorkResult<T>(T value, Throwable failure) {}

    private final KafkaSealedBookKeeperDescriptorV2 descriptor;
    private final M4ReadControlCoordinatorV1 coordinator;
    private final BindingReadSelector predecessor;
    private final BindingReadAuthorityV1 authority;
    private final BindingReadSelectorRuntimeV1 runtime;
    private final BindingReadHazardPoolV1 hazards;
    private final KafkaBookKeeperM4RecoveryV2 recovery;
    private final BookKeeperCellSession session;
    private final Executor owner;
    private final CompletableFuture<Void> terminated = new CompletableFuture<>();
    private boolean admissionClosed;
    private boolean closingSession;
    private boolean scopeEnded;
    private int active;
    private DrainEvidence closure;

    private KafkaBookKeeperReadOwnerV2(
            KafkaSealedBookKeeperDescriptorV2 descriptor,
            M4ReadControlCoordinatorV1 coordinator,
            BindingReadSelector predecessor,
            BookKeeperCellSession session,
            KafkaBookKeeperCompactionWriterV2.SealedMetadataReader metadataReader,
            Executor owner,
            KafkaBookKeeperSelectedSourceV2.Bounds bounds,
            int capacity) {
        this.descriptor = descriptor;
        this.coordinator = coordinator;
        this.predecessor = predecessor;
        this.session = session;
        this.owner = owner;
        authority = KafkaBookKeeperM4RecoveryV2.project(descriptor, predecessor);
        runtime = new BindingReadSelectorRuntimeV1(predecessor.binding(), coordinator, predecessor, authority);
        runtime.installExactDurable(authority);
        hazards = new BindingReadHazardPoolV1(capacity, 4);
        recovery = new KafkaBookKeeperM4RecoveryV2(
                runtime.currentAuthority(),
                hazards,
                owner,
                new KafkaSealedBookKeeperReaderV2(
                        session,
                        metadataReader,
                        bounds.encodedBytes(),
                        new KafkaSealedBookKeeperReaderV2.DecodingBounds(bounds.records(), bounds.decodedBytes()),
                        owner));
    }

    /**
     * Fixed Cell/Binding shares precede all admission work and remain charged through actual IO/session termination.
     * Completion/cancellation/failure of work closes admission and waits for accepted IO and the owned session.
     * Cancelling the returned observer never cancels internal cleanup or releases its Cell reservation.
     */
    public static <T> CompletionStage<T> run(
            KafkaBookKeeperReadCellBudgetV2 budget,
            KafkaSealedBookKeeperDescriptorV2 descriptor,
            M4ReadControlCoordinatorV1 coordinator,
            M5TargetDeleteMultiWriterGuardV2 guard,
            Supplier<? extends BookKeeperCellSession> sessions,
            KafkaBookKeeperCompactionWriterV2.SealedMetadataReader metadataReader,
            Executor owner,
            KafkaBookKeeperSelectedSourceV2.Bounds bounds,
            int capacity,
            Function<KafkaBookKeeperReadOwnerV2, ? extends CompletionStage<T>> work) {
        final KafkaBookKeeperReadCellBudgetV2.Reservation reservation;
        try {
            reservation = Objects.requireNonNull(budget, "budget").reserve(descriptor, bounds, capacity);
        } catch (Throwable failure) {
            return CompletableFuture.failedFuture(failure);
        }
        CompletionStage<T> operation;
        try {
            operation = runInternal(
                    descriptor,
                    coordinator,
                    guard,
                    sessions,
                    metadataReader,
                    owner,
                    bounds,
                    capacity,
                    work,
                    reservation);
        } catch (Throwable failure) {
            reservation.releaseBeforeSession();
            return CompletableFuture.failedFuture(failure);
        }
        return operation
                .whenComplete((value, failure) -> reservation.releaseBeforeSession())
                .thenApply(value -> value);
    }

    private static <T> CompletionStage<T> runInternal(
            KafkaSealedBookKeeperDescriptorV2 descriptor,
            M4ReadControlCoordinatorV1 coordinator,
            M5TargetDeleteMultiWriterGuardV2 guard,
            Supplier<? extends BookKeeperCellSession> sessions,
            KafkaBookKeeperCompactionWriterV2.SealedMetadataReader metadataReader,
            Executor owner,
            KafkaBookKeeperSelectedSourceV2.Bounds bounds,
            int capacity,
            Function<KafkaBookKeeperReadOwnerV2, ? extends CompletionStage<T>> work,
            KafkaBookKeeperReadCellBudgetV2.Reservation reservation) {
        Objects.requireNonNull(descriptor, "descriptor");
        Objects.requireNonNull(coordinator, "coordinator");
        Objects.requireNonNull(guard, "guard");
        Objects.requireNonNull(sessions, "sessions");
        Objects.requireNonNull(metadataReader, "metadataReader");
        Objects.requireNonNull(owner, "owner");
        Objects.requireNonNull(bounds, "bounds");
        Objects.requireNonNull(work, "work");
        if (capacity <= 0
                || capacity > 64
                || descriptor.batchCount() > bounds.batches()
                || descriptor.task().parts().stream()
                                .mapToLong(part -> part.entryCount())
                                .sum()
                        > bounds.entries()
                || descriptor.task().parts().stream()
                                .mapToLong(part -> part.length())
                                .sum()
                        > bounds.encodedBytes()) {
            throw new IllegalArgumentException("BK read owner exceeds its admitted capacity");
        }
        var started = CompletableFuture.supplyAsync(
                        () -> {
                            var selector = coordinator.readSelector().orElseThrow();
                            KafkaBookKeeperM4RecoveryV2.project(descriptor, selector);
                            if (selector.admissionState() != AdmissionState.ADMITTING) {
                                throw new IllegalStateException("BK read owner requires admitting durable selection");
                            }
                            return selector;
                        },
                        owner)
                .thenCompose(selector -> {
                    var resources = descriptor.sealedParts().stream()
                            .map(part -> (PhysicalResourceIdV2) new PhysicalResourceIdV2.BookKeeperLedger(
                                    descriptor.task().namespace(),
                                    part.handle().ledgerIdentity().ledgerId()))
                            .toList();
                    var context = new Context(
                            ProofBoundWriterClassV1.OWNER_WORKER_LEASE_HANDLE_PIN_V1,
                            descriptor.task().capability().configurationDigest(),
                            Sha256Digest.hash(M4ReadControlCodecV1.encodeSelector(selector)),
                            descriptor.descriptorSha256());
                    var terminalProof = Sha256Digest.hash(CanonicalBytes.copyOf(ByteBuffer.allocate(36)
                            .putInt(0x4d354f44)
                            .put(descriptor.descriptorSha256().bytes().toByteArray())
                            .array()));
                    var guarded = guard.execute(resources, context, () -> CompletableFuture.supplyAsync(
                                    () -> {
                                        reservation.sessionStarting();
                                        var session = Objects.requireNonNull(sessions.get(), "native session");
                                        KafkaBookKeeperReadOwnerV2 controller;
                                        try {
                                            if (!session.capabilitySnapshot()
                                                            .equals(descriptor
                                                                    .task()
                                                                    .capability())
                                                    || !coordinator
                                                            .readSelector()
                                                            .equals(Optional.of(selector))) {
                                                throw new IllegalStateException(
                                                        "BK read owner selection or capability changed");
                                            }
                                            controller = new KafkaBookKeeperReadOwnerV2(
                                                    descriptor,
                                                    coordinator,
                                                    selector,
                                                    session,
                                                    metadataReader,
                                                    owner,
                                                    bounds,
                                                    capacity);
                                        } catch (Throwable failure) {
                                            return session.closeAsync().thenApply(ignored -> {
                                                reservation.releaseAfterDrain();
                                                return new Completion<>(
                                                        new WorkResult<T>(null, failure), Optional.of(terminalProof));
                                            });
                                        }
                                        CompletionStage<T> lifetime;
                                        try {
                                            lifetime = Objects.requireNonNull(work.apply(controller), "owner lifetime");
                                        } catch (Throwable failure) {
                                            lifetime = CompletableFuture.failedFuture(failure);
                                        }
                                        return lifetime.handle(WorkResult<T>::new)
                                                .thenCompose(result -> controller
                                                        .stopAndDrain()
                                                        .thenApply(ignored -> {
                                                            reservation.releaseAfterDrain();
                                                            return new Completion<>(result, Optional.of(terminalProof));
                                                        }));
                                    },
                                    owner)
                            .thenCompose(stage -> stage));
                    return guarded.thenCompose(result -> {
                        if (!result.mutationInvoked()) {
                            return CompletableFuture.<T>failedFuture(
                                    new IllegalStateException("BK read owner physical admission failed"));
                        }
                        if (result.value().isEmpty()) {
                            return CompletableFuture.<T>failedFuture(
                                    new IllegalStateException("BK read owner terminal or tickets unresolved"));
                        }
                        var value = result.value().orElseThrow();
                        // This callback returns a WorkResult only after confirmed IO/session termination.
                        if (!result.unresolvedTargets().isEmpty()) {
                            return CompletableFuture.<T>failedFuture(new TicketCleanupException(
                                    guard, resources, context, result.operationId(), terminalProof, value.failure()));
                        }
                        return value.failure() == null
                                ? CompletableFuture.completedFuture(value.value())
                                : CompletableFuture.<T>failedFuture(value.failure());
                    });
                });
        return started.thenApply(value -> value);
    }

    /** The observer is detached so cancellation cannot turn a pending native read into a completed drain. */
    public CompletableFuture<KafkaBookKeeperM4RecoveryV2.RecoveryResult> recover() {
        var observer = new CompletableFuture<KafkaBookKeeperM4RecoveryV2.RecoveryResult>();
        dispatch(
                () -> {
                    if (admissionClosed) {
                        observer.completeExceptionally(new IllegalStateException("BK read owner admission is closed"));
                        return;
                    }
                    active++;
                    try {
                        recovery.recover(descriptor)
                                .whenComplete((value, failure) -> dispatch(
                                        () -> {
                                            active--;
                                            closeSessionIfDrained();
                                            if (failure == null) {
                                                observer.complete(value);
                                            } else {
                                                observer.completeExceptionally(failure);
                                            }
                                        },
                                        observer));
                    } catch (Throwable failure) {
                        active--;
                        closeSessionIfDrained();
                        observer.completeExceptionally(failure);
                    }
                },
                observer);
        return observer;
    }

    /** Exact retries remain possible after an unknown CAS; neither unknown nor conflicting closure returns evidence. */
    public CompletionStage<DrainEvidence> closeFallbackAndDrain(List<SourceProtectionIdentity> sources) {
        var exact = List.copyOf(sources);
        var observer = new CompletableFuture<DrainEvidence>();
        dispatch(
                () -> {
                    try {
                        if (scopeEnded) {
                            throw new IllegalStateException("BK read owner lifetime has ended");
                        }
                        if (predecessor.mode() != SelectorMode.PREFERRED_WITH_FALLBACK
                                || !predecessor
                                        .fallbackSetSha256()
                                        .equals(Optional.of(M4ReadControlCodecV1.calculateFallbackSetSha256(exact)))) {
                            throw new IllegalArgumentException(
                                    "BK read owner closure differs from its admitted fallback set");
                        }
                        closeAdmission();
                        if (closure == null) {
                            var core = new BindingReadSelector(
                                    predecessor.binding(),
                                    predecessor.selectedViewSha256(),
                                    predecessor.ownerEpoch(),
                                    Math.addExact(predecessor.readAdmissionEpoch(), 1),
                                    Math.addExact(predecessor.sourceGeneration(), 1),
                                    SelectorMode.PREFERRED_ONLY,
                                    AdmissionState.ADMITTING,
                                    Optional.empty(),
                                    predecessor.capability(),
                                    List.of(),
                                    List.of());
                            var next = KafkaBookKeeperM4RecoveryV2.project(descriptor, core);
                            var outcome = runtime.closeFallback(
                                    predecessor,
                                    authority,
                                    next,
                                    next.selectedViewSha256(),
                                    next.sourceGeneration(),
                                    exact);
                            if (outcome != Outcome.APPLIED && outcome != Outcome.EXISTING_EXACT) {
                                throw new IllegalStateException("BK read owner closure is unresolved: " + outcome);
                            }
                            var durable = coordinator.readSelector().orElseThrow();
                            var anchor = durable.pendingAnchors().stream()
                                    .filter(value ->
                                            value.closedReadAdmissionEpoch() == predecessor.readAdmissionEpoch())
                                    .findFirst()
                                    .orElseThrow();
                            if (!anchor.predecessorSelectorCoreSha256()
                                            .equals(M4ReadControlCodecV1.selectorCoreSha256(predecessor))
                                    || !anchor.successorSelectorCoreSha256()
                                            .equals(M4ReadControlCodecV1.selectorCoreSha256(durable))) {
                                throw new IllegalStateException(
                                        "BK read owner closure anchor differs from exact transition");
                            }
                            closure = new DrainEvidence(predecessor, durable, anchor);
                        }
                        closeSessionIfDrained();
                        terminated.whenComplete((ignored, failure) -> {
                            if (failure == null) {
                                observer.complete(closure);
                            } else {
                                observer.completeExceptionally(failure);
                            }
                        });
                    } catch (Throwable failure) {
                        observer.completeExceptionally(failure);
                    }
                },
                observer);
        return observer;
    }

    private CompletionStage<Void> stopAndDrain() {
        var observer = new CompletableFuture<Void>();
        dispatch(
                () -> {
                    scopeEnded = true;
                    closeAdmission();
                    closeSessionIfDrained();
                    terminated.whenComplete((ignored, failure) -> {
                        if (failure == null) {
                            observer.complete(null);
                        } else {
                            observer.completeExceptionally(failure);
                        }
                    });
                },
                observer);
        return observer;
    }

    private void closeAdmission() {
        admissionClosed = true;
        hazards.closeAdmission();
    }

    private void closeSessionIfDrained() {
        if (!admissionClosed || active != 0 || closingSession) {
            return;
        }
        if (hazards.scanBinding(predecessor.binding().bindingId()) != BindingReadHazardPoolV1.ScanOutcome.CLEAN) {
            terminated.completeExceptionally(new IllegalStateException("BK read owner still has unresolved leases"));
            return;
        }
        closingSession = true;
        try {
            session.closeAsync().whenComplete((ignored, failure) -> {
                if (failure == null) {
                    terminated.complete(null);
                } else {
                    terminated.completeExceptionally(failure);
                }
            });
        } catch (Throwable failure) {
            terminated.completeExceptionally(failure);
        }
    }

    private void dispatch(Runnable operation, CompletableFuture<?> observer) {
        try {
            owner.execute(operation);
        } catch (Throwable rejected) {
            observer.completeExceptionally(rejected);
        }
    }
}
