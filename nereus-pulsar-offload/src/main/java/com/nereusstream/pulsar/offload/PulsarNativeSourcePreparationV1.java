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

import com.nereusstream.storage.api.lifecycle.PhysicalResourceIdV2.Namespace;
import com.nereusstream.storage.bookkeeper.M5BookKeeperNamespaceAuthorityV2;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.Executor;
import org.apache.bookkeeper.mledger.impl.ManagedLedgerImpl;
import org.apache.bookkeeper.mledger.impl.ManagedLedgerImpl.CapturedNativeReadView;

/** Owns actual Native source preparation. Observing a view never releases its accepted SDK-open reservation. */
public final class PulsarNativeSourcePreparationV1 {
    private final CapturedNativeReadView nativeScope;
    private final PulsarNativeReadCellBudgetV1 budget;
    private final PulsarNativeReadCellBudgetV1.Reservation reservation;
    private final CompletableFuture<PulsarNativeBindingReadViewV1> view = new CompletableFuture<>();
    private CompletableFuture<Void> closing;
    private CompletableFuture<Void> unregisteredClosing;
    private final PulsarNativeM4SourceMapV1 published;
    private boolean installed;
    private boolean closeRequested;
    private PulsarNativeMaterializedReadV2 preferredReads;

    PulsarNativeSourcePreparationV1(
            PulsarNativeBindingV1 binding,
            CapturedNativeReadView nativeScope,
            ManagedLedgerImpl ledger,
            M5BookKeeperNamespaceAuthorityV2 bookKeeper,
            NereusPulsarLedgerOffloaderV1 offloader,
            Namespace objectNamespace,
            PulsarNativeReadCellBudgetV1 budget,
            boolean preferredOnly,
            PulsarNativeM4SourceMapV1 published) {
        this(
                binding,
                nativeScope,
                ledger,
                bookKeeper,
                offloader,
                objectNamespace,
                budget,
                preferredOnly,
                published,
                null,
                null);
    }

    PulsarNativeSourcePreparationV1(
            PulsarNativeBindingV1 binding,
            CapturedNativeReadView nativeScope,
            ManagedLedgerImpl ledger,
            M5BookKeeperNamespaceAuthorityV2 bookKeeper,
            NereusPulsarLedgerOffloaderV1 offloader,
            Namespace objectNamespace,
            PulsarNativeReadCellBudgetV1 budget,
            boolean preferredOnly,
            PulsarNativeM4SourceMapV1 published,
            PulsarNativeM4ControlV1.SelectedMaterialization selected,
            Executor controlWorker) {
        this.nativeScope = nativeScope;
        this.budget = budget;
        this.published = published;
        int objects = Math.toIntExact(nativeScope.ledgerIds().stream()
                .filter(nativeScope::offloadComplete)
                .count());
        reservation = budget.reservePreparation(binding, nativeScope.ledgerIds().size(), objects);
        reservation.ready().whenComplete((ignored, reserveFailure) -> {
            synchronized (this) {
                if (reserveFailure != null || closeRequested) {
                    view.completeExceptionally(
                            reserveFailure != null
                                    ? reserveFailure
                                    : new IllegalStateException("Native source preparation is closing"));
                    close();
                    return;
                }
                try {
                    nativeScope.installReadPreparation();
                    installed = true;
                    reservation.sourceStarting();
                } catch (Throwable failure) {
                    view.completeExceptionally(failure);
                    close();
                    return;
                }
                var actual = selected != null
                        ? binding.captureMappedPreferredView(
                                ledger, bookKeeper, offloader, objectNamespace, published, selected)
                        : published != null
                                ? binding.capturePublishedPreferredView(
                                        ledger, bookKeeper, offloader, objectNamespace, published)
                                : preferredOnly
                                        ? binding.capturePreferredOnlyReadView(
                                                ledger, bookKeeper, offloader, objectNamespace)
                                        : binding.captureReadView(ledger, bookKeeper, offloader, objectNamespace);
                actual.whenComplete((captured, failure) -> {
                    synchronized (this) {
                        if (closeRequested) {
                            view.completeExceptionally(
                                    new IllegalStateException("Native source preparation is closing"));
                            close();
                            return;
                        }
                        if (failure == null) {
                            captured.attachPreparation(this);
                            if (selected == null) {
                                view.complete(captured);
                            } else {
                                preferredReads = new PulsarNativeMaterializedReadV2(
                                        captured, selected, budget, offloader, controlWorker);
                                captured.attachPreferredReads(preferredReads);
                                preferredReads.open().whenComplete((nothing, openFailure) -> {
                                    if (openFailure == null) {
                                        view.complete(captured);
                                    } else {
                                        view.completeExceptionally(openFailure);
                                        close();
                                    }
                                });
                            }
                        } else {
                            view.completeExceptionally(failure);
                            close();
                        }
                    }
                });
            }
        });
    }

    public CompletableFuture<PulsarNativeBindingReadViewV1> view() {
        return view.copy();
    }

    PulsarNativeReadCellBudgetV1 budget() {
        return budget;
    }

    void requirePublished(PulsarNativeM4SourceMapV1 current) {
        if (published == null || !published.encode().equals(current.encode())) {
            throw new IllegalStateException("Native cold preparation differs from its selected published sources");
        }
    }

    synchronized CompletableFuture<Void> closeUnregisteredOwner() {
        closeRequested = true;
        if (unregisteredClosing == null || unregisteredClosing.isCompletedExceptionally()) {
            unregisteredClosing = reservation.ready().thenCompose(ignored -> {
                if (!installed) {
                    return CompletableFuture.failedFuture(
                            new IllegalStateException("Native initial grant has no installed source preparation"));
                }
                var sdk = preferredReads == null ? CompletableFuture.completedFuture(null) : preferredReads.close();
                return sdk.thenCompose(nothing -> nativeScope
                        .fenceUnregisteredReadPreparation()
                        .thenRun(nativeScope::requireUnregisteredReadPreparationDrain)
                        .thenCompose(done -> preferredReads == null
                                ? CompletableFuture.completedFuture(null)
                                : preferredReads.releaseLoan())
                        .thenCompose(done -> reservation.releaseAfterDrain()));
            });
        }
        return unregisteredClosing.copy();
    }

    void requireUnregisteredDrain() {
        nativeScope.requireUnregisteredReadPreparationDrain();
    }

    /** Actual source fence waits for cached opens/handle disposal, callbacks and aliases; timeout cannot free it. */
    public synchronized CompletableFuture<Void> close() {
        closeRequested = true;
        if (closing == null || closing.isCompletedExceptionally()) {
            closing = reservation
                    .ready()
                    .handle((ignored, reserveFailure) -> null)
                    .thenCompose(ignored -> {
                        synchronized (this) {
                            var sdk = preferredReads == null
                                    ? CompletableFuture.completedFuture(null)
                                    : preferredReads.close();
                            return sdk.thenCompose(nothing -> installed
                                    ? nativeScope
                                            .fenceReadPreparation()
                                            .thenCompose(done -> preferredReads == null
                                                    ? CompletableFuture.completedFuture(null)
                                                    : preferredReads.releaseLoan())
                                            .thenCompose(done -> reservation.releaseAfterDrain())
                                    : reservation.releaseBeforeActor());
                        }
                    });
        }
        return closing.copy();
    }
}
