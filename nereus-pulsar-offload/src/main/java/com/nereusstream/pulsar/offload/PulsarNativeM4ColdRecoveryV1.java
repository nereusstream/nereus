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
import com.nereusstream.storage.object.read.control.M4ReadControlCodecV1;
import com.nereusstream.storage.object.read.control.M4ReadControlCoordinatorV1;
import com.nereusstream.storage.object.read.control.M4ReadControlRecordsV1.BindingReadSelector;
import java.nio.ByteBuffer;
import java.util.Objects;
import java.util.UUID;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.Executor;

/** One private cold grant attempt after the original actor's durable confirmed read drain. */
public final class PulsarNativeM4ColdRecoveryV1 {
    private final PulsarNativeM4ControlV1 control;
    private com.nereusstream.storage.bookkeeper.M5BookKeeperNamespaceAuthorityV2 bookKeeper;
    private final PulsarNativeBindingReadViewV1 view;
    private final BindingReadSelector predecessor;
    private final BindingReadSelector successor;
    private final PulsarNativeM4DrainAdmissionV1 admission;
    private final CanonicalBytes claim;
    private CompletableFuture<PulsarNativeM4ReadOwnerV1> attempt;
    private PulsarNativeM4ReadOwnerV1 owner;
    private Throwable registrationFailure;
    private Executor ownerEventLoop;
    private int capacity;
    private PulsarNativeReadCellBudgetV1 cellBudget;
    private PulsarNativeReadCellBudgetV1.Reservation cellReservation;
    private boolean selectorAttempted;
    private final boolean publishedInitialGrant;
    private PulsarNativeM4ControlV1.DrainedOwnerProof recoveredProof;

    PulsarNativeM4ColdRecoveryV1(
            PulsarNativeM4ControlV1 control,
            PulsarNativeBindingReadViewV1 view,
            BindingReadSelector predecessor,
            PulsarNativeM4DrainAdmissionV1 admission) {
        this(control, view, predecessor, admission, false);
    }

    PulsarNativeM4ColdRecoveryV1(
            PulsarNativeM4ControlV1 control,
            PulsarNativeBindingReadViewV1 view,
            BindingReadSelector predecessor,
            PulsarNativeM4DrainAdmissionV1 admission,
            boolean publishedInitialGrant) {
        this.control = control;
        this.view = view;
        this.predecessor = predecessor;
        this.admission = admission;
        this.publishedInitialGrant = publishedInitialGrant;
        successor = publishedInitialGrant
                ? predecessor
                : control.coordinator()
                        .takeoverCandidate(
                                predecessor,
                                view.sha256(),
                                Math.addExact(predecessor.ownerEpoch(), 1),
                                Math.addExact(predecessor.sourceGeneration(), 1));
        var nonce = UUID.randomUUID();
        byte[] selected = M4ReadControlCodecV1.encodeSelector(predecessor).toByteArray();
        claim = CanonicalBytes.copyOf(ByteBuffer.allocate(28 + selected.length + 32)
                .putInt(0x4e504f47)
                .putInt(1)
                .putInt(selected.length)
                .putLong(nonce.getMostSignificantBits())
                .putLong(nonce.getLeastSignificantBits())
                .put(selected)
                .put(view.sha256().bytes().toByteArray())
                .array());
    }

    void requireBookKeeper(com.nereusstream.storage.bookkeeper.M5BookKeeperNamespaceAuthorityV2 actual) {
        bookKeeper = actual;
    }

    /** Cancelling/completing the observer cannot grant another actor or cancel the private exact attempt. */
    public synchronized CompletableFuture<PulsarNativeM4ReadOwnerV1> recover(
            Executor controlExecutor, Executor ownerExecutor, int readCapacity, PulsarNativeReadCellBudgetV1 budget) {
        try {
            Objects.requireNonNull(controlExecutor, "control executor");
            if (readCapacity <= 0 || readCapacity > 65_536) {
                throw new IllegalArgumentException("Native cold capacity is outside its hard bound");
            }
            if (ownerEventLoop == null) {
                ownerEventLoop = Objects.requireNonNull(ownerExecutor, "owner executor");
                capacity = readCapacity;
                cellBudget = Objects.requireNonNull(budget, "admitted Cell budget");
            } else if (ownerEventLoop != ownerExecutor || capacity != readCapacity || cellBudget != budget) {
                throw new IllegalArgumentException("Native cold retry changed its actual owner configuration");
            }
            if (attempt != null && (!attempt.isDone() || !attempt.isCompletedExceptionally())) {
                return attempt.copy();
            }
            if (cellReservation == null) {
                cellReservation = control.reserveOwnerBudget(view, cellBudget, capacity);
            }
            attempt = new CompletableFuture<>();
            var currentAttempt = attempt;
            cellReservation
                    .ready()
                    .thenCompose(unused -> control.revalidateBinding())
                    .thenCompose(ignored -> control.openSuccessorReadHold(
                            view,
                            successor,
                            cellBudget,
                            bookKeeper,
                            hold -> {
                                grant();
                                requireGrantedFacts();
                                return register(hold);
                            },
                            controlExecutor))
                    .whenComplete((value, failure) -> {
                        if (failure == null) {
                            currentAttempt.complete(value);
                        } else {
                            CompletableFuture<Void> unusedRelease = CompletableFuture.completedFuture(null);
                            synchronized (this) {
                                if (!selectorAttempted && owner == null) {
                                    unusedRelease = cellReservation
                                            .releaseBeforeActor()
                                            .thenRun(() -> {
                                                synchronized (this) {
                                                    cellReservation = null;
                                                }
                                            });
                                }
                            }
                            unusedRelease.whenComplete((nothing, cleanupFailure) -> {
                                if (cleanupFailure != null && cleanupFailure != failure) {
                                    failure.addSuppressed(cleanupFailure);
                                }
                                currentAttempt.completeExceptionally(failure);
                            });
                        }
                    });
            return currentAttempt.copy();
        } catch (Throwable failure) {
            return CompletableFuture.failedFuture(failure);
        }
    }

    private void requireGrantedFacts() {
        if (control.readFacts(view)
                .join()
                .filter(value -> value.selector().equals(successor))
                .isEmpty()) {
            throw new IllegalStateException("Native cold grant changed before registration");
        }
    }

    private void grant() {
        if (!view.isCurrent()) {
            throw new IllegalStateException("Native cold source cut is no longer current");
        }
        control.requireCurrentCapability(predecessor, admission);
        var coordinator = control.coordinator();
        var observed = coordinator.readSelector().orElseThrow();
        if (!observed.equals(predecessor) && !observed.equals(successor)) {
            throw new IllegalStateException("Native cold selector changed from its exact attempt");
        }
        // Unique persistent nonce prevents two independently prepared handles from sharing one grant.
        if (publishedInitialGrant) {
            control.publishMaterializedInitialClaim(predecessor, claim);
        } else {
            control.publishColdClaim(predecessor, claim);
        }
        control.publishSourceMap(view);
        if (publishedInitialGrant) {
            // Materialization already granted this epoch by its sole selector CAS. The unique immutable claim
            // installs its first real SDK actor; it must not create an unowned extra read epoch.
            if (!coordinator.readSelector().filter(predecessor::equals).isPresent()) {
                throw new IllegalStateException("Native initial materialized grant changed before registration");
            }
            selectorAttempted = true;
            return;
        }
        if (view.materialized()
                && predecessor.mode()
                        == com.nereusstream.storage.object.read.control.M4ReadControlRecordsV1.SelectorMode
                                .PREFERRED_WITH_FALLBACK
                && recoveredProof == null) {
            recoveredProof = control.captureRecoveredOwnerProof(predecessor, successor, admission);
        }
        selectorAttempted = true;
        var result = coordinator.grantTakeover(
                predecessor, successor.selectedViewSha256(), successor.ownerEpoch(), successor.sourceGeneration());
        if ((result != M4ReadControlCoordinatorV1.Outcome.APPLIED
                        && result != M4ReadControlCoordinatorV1.Outcome.EXISTING_EXACT)
                || !coordinator.readSelector().filter(successor::equals).isPresent()) {
            throw new IllegalStateException("Native cold grant remains unresolved: " + result);
        }
        if (view.materialized()
                && predecessor.mode()
                        == com.nereusstream.storage.object.read.control.M4ReadControlRecordsV1.SelectorMode
                                .PREFERRED_WITH_FALLBACK) {
            control.publishRecoveredOwnerProof(recoveredProof);
        }
    }

    private PulsarNativeM4ReadOwnerV1 register(PulsarNativeControlWriterV2.ReadHold hold) {
        if (registrationFailure != null) {
            throw new IllegalStateException("Native cold registration remains unresolved", registrationFailure);
        }
        if (owner == null) {
            owner = new PulsarNativeM4ReadOwnerV1(
                    view, successor, ownerEventLoop, capacity, control, admission, cellBudget, cellReservation);
            if (hold != null) {
                owner.attachReadHold(hold);
            }
            try {
                view.installReadAdmission(owner);
            } catch (Throwable failure) {
                registrationFailure = failure;
                owner.fence();
                throw failure;
            }
        }
        return owner;
    }
}
