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

import com.nereusstream.metadata.spi.retention.ExactMetadataTransactionStoreV1.VersionedValue;
import com.nereusstream.storage.api.lifecycle.PhysicalResourceIdV2.BookKeeperLedger;
import com.nereusstream.storage.api.lifecycle.PhysicalResourceIdV2.Namespace;
import com.nereusstream.storage.bookkeeper.M5PulsarNativeCreateSpecV1;
import com.nereusstream.storage.object.gc.M5TargetDeleteStoredValueV2;
import com.nereusstream.storage.object.materialization.M5MaterializationAdmissionV1.Caps;
import com.nereusstream.storage.object.materialization.M5MaterializationRecordsV1.IndexKind;
import com.nereusstream.storage.object.materialization.M5PulsarCoverageV2.EntryRange;
import com.nereusstream.storage.object.retention.M5ClosedWriterRegistryV1;
import com.nereusstream.storage.object.retention.M5PulsarAggregateRetirementCoordinatorV1;
import java.nio.file.Path;
import java.util.List;
import java.util.Objects;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.CompletionStage;
import java.util.concurrent.Executor;
import java.util.function.Supplier;
import org.apache.bookkeeper.mledger.impl.ManagedLedgerImpl;

/** Explicit quiescent replacement of one originally offloaded BK source; independent of ordinary offload. */
public final class PulsarNativeBookKeeperReplacementV2 {
    /** Each opening owns the real SDK ledger/factory and its original Object provider until close. */
    public record Scope(
            PulsarNativeManagedLedgerFactoryV2 factory,
            ManagedLedgerImpl ledger,
            NereusPulsarLedgerOffloaderV1 offloader,
            Namespace objects,
            Supplier<CompletableFuture<Void>> close) {
        public Scope {
            Objects.requireNonNull(close, "original native factory close");
            factory.requireOwnedLedger(ledger);
            if (!offloader.preservesIncompleteAttempt()) {
                throw new IllegalArgumentException("Native replacement requires an enrolled offloader");
            }
        }
    }

    public record ReleasedSource(
            BookKeeperLedger resource,
            M5PulsarNativeCreateSpecV1 originalScope,
            PulsarNativeM4FallbackRetirementV1.Result release) {}

    private final PulsarNativeBindingV1 binding;
    private final PulsarNativeM4DrainAdmissionV1 admission;
    private final PulsarNativeReadCellBudgetV1 budget;
    private final M5ClosedWriterRegistryV1 writers;
    private final M5PulsarAggregateRetirementCoordinatorV1 aggregate;
    private final String key;
    private final Executor worker;
    private final Executor readEventLoop;
    private final int readSlots;
    private final Supplier<CompletableFuture<Scope>> openScope;
    private final long target;
    private PulsarNativeM4ControlV1 control;
    private Scope scope;
    private PulsarNativeM4ReadOwnerV1 owner;
    private PulsarNativeSourcePreparationV1 preparation;
    private PulsarNativeMaterializationSourceV2 materialization;
    private BookKeeperLedger resource;
    private M5PulsarNativeCreateSpecV1 originalScope;
    private PulsarNativeM4FallbackRetirementV1.Result released;
    private CompletableFuture<ReleasedSource> original;
    private CompletableFuture<Void> nativeDone;

    public PulsarNativeBookKeeperReplacementV2(
            PulsarNativeBindingV1 binding,
            PulsarNativeM4DrainAdmissionV1 admission,
            PulsarNativeReadCellBudgetV1 budget,
            M5ClosedWriterRegistryV1 writers,
            M5PulsarAggregateRetirementCoordinatorV1 aggregate,
            String key,
            Executor worker,
            Executor readEventLoop,
            int readSlots,
            Supplier<CompletableFuture<Scope>> openScope,
            long target) {
        this.binding = binding;
        this.admission = admission;
        this.budget = budget;
        this.writers = writers;
        this.aggregate = aggregate;
        this.key = key;
        this.worker = worker;
        this.readEventLoop = readEventLoop;
        this.readSlots = readSlots;
        this.openScope = openScope;
        if (target < 0 || readSlots <= 0 || readSlots > 65_536) {
            throw new IllegalArgumentException("Native replacement target or read slots are outside their domain");
        }
        this.target = target;
    }

    /** Runs under one exclusively owned, quiescent Cell. The same actor owns all original replies. */
    public synchronized CompletableFuture<ReleasedSource> prepare(Path staging, long maximumObjectBytes, Caps caps) {
        if (original != null) {
            return original.copy();
        }
        control = binding.m4Control();
        original = open().thenCompose(unused -> captureOriginalSource())
                .thenCompose(unused -> control.installControlWriter(writers, aggregate, key, budget))
                .thenCompose(unused -> preparePlain(false))
                .thenCompose(view -> control.createReadOwner(
                        view,
                        admission,
                        scope.factory().namespace(),
                        scope.offloader(),
                        worker,
                        readEventLoop,
                        readSlots,
                        budget))
                .thenCompose(admitted -> {
                    owner = admitted;
                    preparation = null; // The read owner now owns and drains its preparation.
                    return owner.drainForLogicalTrim(worker);
                })
                .thenCompose(unused -> closeScope())
                .thenCompose(unused -> open())
                .thenCompose(unused -> preparePlain(true))
                .thenCompose(preferred -> control.retireFallback(
                        owner, preferred, scope.factory().namespace(), scope.offloader(), worker))
                .thenCompose(retired -> {
                    released = retired;
                    owner = retired.successorOwner();
                    preparation = null;
                    var ranges = owner.materializationView().materializationChain().stream()
                            .filter(row -> !row.active() && row.entryEndExclusive() > 0)
                            .map(row -> new EntryRange(row.ledgerId(), 0, row.entryEndExclusive()))
                            .toList();
                    return PulsarNativeMaterializationSourceV2.capture(
                            owner, scope.factory().namespace(), scope.offloader(), ranges, worker);
                })
                .thenCompose(source -> {
                    materialization = source;
                    return source.qualifyProvider(maximumObjectBytes, worker);
                })
                .thenCompose(unused -> materialization.prepareTask(
                        true, List.of(IndexKind.OFFSET_OR_POSITION, IndexKind.CHECKSUM_COVERAGE), caps, worker))
                .thenCompose(task -> task.writeAndVerify(staging, maximumObjectBytes, worker)
                        .thenCompose(unused -> task.publish(worker)))
                // The original Task performs synchronous exact quota reconciliation during close.
                .thenComposeAsync(unused -> materialization.closeAsync(), worker)
                .thenCompose(unused -> closeScope())
                .thenCompose(unused -> control.closeControlWriter())
                .thenCompose(unused -> {
                    owner = null;
                    materialization = null;
                    control = binding.m4Control();
                    return control.installControlWriter(writers, aggregate, key, budget);
                })
                .thenCompose(unused -> open())
                .thenCompose(unused -> control.prepareMaterializedReadSources(
                        scope.ledger(),
                        scope.factory().namespace(),
                        scope.offloader(),
                        scope.objects(),
                        budget,
                        worker))
                .thenCompose(prepared -> {
                    preparation = prepared;
                    return control.prepareMaterializedReadOwner(
                            prepared, admission, scope.factory().namespace(), scope.offloader(), worker);
                })
                .thenCompose(recovery -> recovery.recover(worker, readEventLoop, readSlots, budget))
                .thenCompose(current -> {
                    owner = current;
                    preparation = null;
                    return released.prepareBookKeeperDeleteIntent(resource, owner, worker);
                })
                .thenCompose(unused -> closeScope())
                .thenCompose(unused -> control.closeControlWriter())
                .thenApply(unused -> new ReleasedSource(resource, originalScope, released))
                .toCompletableFuture();
        return original.copy();
    }

    /** Finishes the native SDK marker after physical GC and its original reference scan have closed. */
    public synchronized CompletableFuture<Void> completeNativeDone(VersionedValue exactPhysicalDone) {
        if (original == null || !original.isDone() || original.isCompletedExceptionally()) {
            return CompletableFuture.failedFuture(new IllegalStateException("Native replacement has no exact INTENT"));
        }
        var physical = M5TargetDeleteStoredValueV2.decode(exactPhysicalDone);
        if (!resource.equals(physical.resource()) || physical.compactDone().isEmpty()) {
            return CompletableFuture.failedFuture(
                    new IllegalStateException("Native replacement has no exact physical DONE"));
        }
        if (nativeDone == null) {
            nativeDone = open().thenCompose(unused -> scope.ledger().captureNativeReadView())
                    .thenCompose(view -> {
                        var transition = view.captureBookKeeperDeletion(target);
                        return view.fenceOwnerReadAdmission().thenCompose(unused -> transition.completeDone());
                    })
                    .thenCompose(unused -> closeScope());
        }
        return nativeDone.copy();
    }

    private CompletableFuture<Void> open() {
        return openScope.get().thenAccept(actual -> {
            scope = actual;
            if (!binding.bindingIdentity().equals(actual.factory().bindingIdentity())) {
                throw new IllegalStateException("Native replacement opened a different Binding");
            }
        });
    }

    private CompletionStage<Void> captureOriginalSource() {
        var actual = scope.factory().namespace();
        return scope.ledger()
                .captureNativeReadView()
                .thenCompose(view -> {
                    admission.requireConfiguration(view, actual, scope.offloader(), binding.metadataNamespace());
                    view.captureBookKeeperDeletion(target); // Validate exact sealed DELETE_AFTER_VERIFIED metadata.
                    return actual.borrowClient().getLedgerManager().readLedgerMetadata(target);
                })
                .thenCompose(metadata -> actual.readPriorPulsarSealedBirth(target, metadata.getValue()))
                .thenAccept(birth -> {
                    if (birth.scope().bookKeeperGcGrace().isEmpty()) {
                        throw new IllegalStateException("Original BookKeeper birth has no admitted GC grace");
                    }
                    originalScope = birth.scope();
                    resource = new BookKeeperLedger(actual.namespace(), target);
                });
    }

    private CompletionStage<PulsarNativeBindingReadViewV1> preparePlain(boolean preferredOnly) {
        return binding.prepareReadSources(
                        scope.ledger(),
                        scope.factory().namespace(),
                        scope.offloader(),
                        scope.objects(),
                        budget,
                        preferredOnly)
                .thenCompose(prepared -> {
                    preparation = prepared;
                    return prepared.view();
                });
    }

    private CompletableFuture<Void> closeScope() {
        Scope opened = scope;
        return opened.close().get().thenRun(() -> scope = null);
    }
}
