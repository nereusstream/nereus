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
import com.nereusstream.domain.bytes.Sha256Digest;
import com.nereusstream.metadata.spi.retention.ExactMetadataTransactionStoreV1.VersionedValue;
import com.nereusstream.storage.bookkeeper.M5BookKeeperNamespaceAuthorityV2;
import com.nereusstream.storage.object.read.BindingReadHazardPoolV1.ScanOutcome;
import com.nereusstream.storage.object.read.control.M4ReadControlCodecV1;
import com.nereusstream.storage.object.read.control.M4ReadControlCoordinatorV1;
import com.nereusstream.storage.object.read.control.M4ReadControlCoordinatorV1.Outcome;
import com.nereusstream.storage.object.read.control.M4ReadControlRecordsV1.BindingReadSelector;
import com.nereusstream.storage.object.read.control.M4ReadControlRecordsV1.ClosureAnchor;
import com.nereusstream.storage.object.read.control.M4ReadControlRecordsV1.ReadAdmissionEpochTerminalCut;
import com.nereusstream.storage.object.read.control.M4ReadControlRecordsV1.ReadQuiescenceProof;
import com.nereusstream.storage.object.read.control.M4ReadControlRecordsV1.SelectorMode;
import com.nereusstream.storage.object.read.control.M4ReadControlRecordsV1.SourceRetirementBatch;
import com.nereusstream.storage.object.read.control.M4ReadControlRecordsV1.TerminalKind;
import java.nio.ByteBuffer;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.Executor;

/** One actual Native actor's planned drain. No expiry, writer/reference or physical-delete authority. */
public final class PulsarNativeM4FallbackRetirementV1 {
    public static final class Result {
        private final BindingReadSelector selector;
        private final SourceRetirementBatch batch;
        private final ReadAdmissionEpochTerminalCut terminal;
        private final ReadQuiescenceProof proof;
        private final PulsarNativeM4ReadOwnerV1 successorOwner;
        private final BindingReadSelector predecessor;
        private final VersionedValue originalSourceMap;
        private final VersionedValue releaseHead;
        private final Executor controlExecutor;
        private com.nereusstream.storage.api.lifecycle.PhysicalResourceIdV2.BookKeeperLedger nativeDeleteResource;
        private org.apache.bookkeeper.mledger.impl.ManagedLedgerImpl.NativeBookKeeperSourceDeletion nativeDeletion;
        private PulsarNativeReadCellBudgetV1.Reservation nativeDeleteWorkspace;
        private boolean nativeDeleteStarted;
        private PulsarNativeM4ReadOwnerV1 nativeDeleteOwner;
        private PulsarNativeM4ControlV1 nativeDeleteControl;
        private Executor nativeDeleteExecutor;
        private CompletableFuture<Void> nativeIntent;

        private Result(
                BindingReadSelector selector,
                SourceRetirementBatch batch,
                ReadAdmissionEpochTerminalCut terminal,
                ReadQuiescenceProof proof,
                PulsarNativeM4ReadOwnerV1 successorOwner,
                BindingReadSelector predecessor,
                VersionedValue originalSourceMap,
                VersionedValue releaseHead,
                Executor controlExecutor) {
            this.selector = selector;
            this.batch = batch;
            this.terminal = terminal;
            this.proof = proof;
            this.successorOwner = successorOwner;
            this.predecessor = predecessor;
            this.originalSourceMap = originalSourceMap;
            this.releaseHead = releaseHead;
            this.controlExecutor = controlExecutor;
        }

        BindingReadSelector predecessor() {
            return predecessor;
        }

        VersionedValue originalSourceMap() {
            return originalSourceMap;
        }

        VersionedValue releaseHead() {
            return releaseHead;
        }

        public BindingReadSelector selector() {
            return selector;
        }

        public SourceRetirementBatch batch() {
            return batch;
        }

        public ReadAdmissionEpochTerminalCut terminal() {
            return terminal;
        }

        public ReadQuiescenceProof proof() {
            return proof;
        }

        public PulsarNativeM4ReadOwnerV1 successorOwner() {
            return successorOwner;
        }

        /**
         * Records one original Native compatibility INTENT after this real M4 release and SDK pin drain.
         * It grants no physical delete; the subsequent GC still needs complete current eligibility and grace.
         */
        public synchronized CompletableFuture<Void> prepareBookKeeperDeleteIntent(
                com.nereusstream.storage.api.lifecycle.PhysicalResourceIdV2.BookKeeperLedger resource) {
            return prepareBookKeeperDeleteIntent(resource, successorOwner, controlExecutor);
        }

        /** Uses the actual current owner while retaining the original source-map and exact release identity. */
        public synchronized CompletableFuture<Void> prepareBookKeeperDeleteIntent(
                com.nereusstream.storage.api.lifecycle.PhysicalResourceIdV2.BookKeeperLedger resource,
                PulsarNativeM4ReadOwnerV1 currentOwner,
                Executor currentControlExecutor) {
            try {
                java.util.Objects.requireNonNull(currentControlExecutor, "original Native delete control executor");
                if (nativeIntent != null && !nativeIntent.isCompletedExceptionally()) {
                    if (!resource.equals(nativeDeleteResource) || currentOwner != nativeDeleteOwner) {
                        throw new IllegalArgumentException("Native release already owns another BK deletion cut");
                    }
                    return nativeIntent.copy();
                }
                if (nativeDeletion == null) {
                    var view = currentOwner.materializationView();
                    var map = PulsarNativeM4SourceMapV1.decode(
                            originalSourceMap.canonicalStoredBytes(), predecessor.selectedViewSha256());
                    if (!resource.namespace()
                                    .equals(view.bookKeeperNamespaceBinding().physicalNamespace())
                            || batch.sources().stream().noneMatch(source -> map.protectedPhysicalReferences(
                                            view.binding(),
                                            view.bookKeeperNamespaceBinding(),
                                            view.objectBinding(),
                                            source.sourceIdentitySha256())
                                    .contains(resource))) {
                        throw new IllegalArgumentException("Native M4 release does not contain this exact BK source");
                    }
                    // Capture the actual current SDK cut before fencing, without reopening the replaced source.
                    nativeDeletion = view.captureBookKeeperDeletion(resource.ledgerId());
                    nativeDeleteResource = resource;
                    nativeDeleteOwner = currentOwner;
                    nativeDeleteControl = currentOwner.materializationControl();
                    nativeDeleteExecutor = currentControlExecutor;
                    nativeDeleteWorkspace = currentOwner
                            .cellBudget()
                            .reserveCursorObservation(view.binding(), PulsarNativeCursorMetadataV2.MAX_BYTES);
                } else if (!resource.equals(nativeDeleteResource) || currentOwner != nativeDeleteOwner) {
                    throw new IllegalArgumentException("Original Native BK intent resource changed");
                }
                nativeIntent = nativeDeleteWorkspace
                        .ready()
                        .thenCompose(
                                ignored -> nativeDeleteControl.requireCurrentSelector(nativeDeleteOwner.selector()))
                        .thenCompose(ignored -> {
                            if (!nativeDeleteStarted) {
                                nativeDeleteWorkspace.sourceStarting();
                                nativeDeleteStarted = true;
                            }
                            return nativeDeleteOwner.drainForRecovery(nativeDeleteExecutor);
                        })
                        .thenCompose(ignored -> nativeDeletion.prepareIntent())
                        .thenCompose(ignored -> nativeDeleteWorkspace.releaseAfterDrain());
                return nativeIntent.copy();
            } catch (Throwable failure) {
                return CompletableFuture.failedFuture(failure);
            }
        }
    }

    private final PulsarNativeM4ControlV1 control;
    private final PulsarNativeM4ReadOwnerV1 owner;
    private final PulsarNativeBindingReadViewV1 original;
    private final PulsarNativeBindingReadViewV1 preferred;
    private final PulsarNativeM4DrainAdmissionV1 admission;
    private final BindingReadSelector predecessor;
    private M4ReadControlCoordinatorV1 coordinator;
    private M5BookKeeperNamespaceAuthorityV2 bookKeeper;
    private PulsarNativePhysicalReferenceSetV2 physical;
    private CompletableFuture<Void> drain;
    private CompletableFuture<Result> attempt;
    private BindingReadSelector successor;
    private ClosureAnchor anchor;
    private SourceRetirementBatch batch;
    private ReadAdmissionEpochTerminalCut terminal;
    private ReadQuiescenceProof proof;
    private PulsarNativeM4ReadOwnerV1 successorOwner;
    private Throwable registrationFailure;
    private Result result;

    PulsarNativeM4FallbackRetirementV1(
            PulsarNativeM4ControlV1 control,
            PulsarNativeM4ReadOwnerV1 owner,
            PulsarNativeBindingReadViewV1 original,
            PulsarNativeBindingReadViewV1 preferred,
            PulsarNativeM4DrainAdmissionV1 admission) {
        this.control = control;
        this.owner = owner;
        this.original = original;
        this.preferred = preferred;
        this.admission = admission;
        predecessor = owner.selector();
        if (predecessor.mode() != SelectorMode.PREFERRED_WITH_FALLBACK
                || owner.fallbackSources().isEmpty()) {
            throw new IllegalArgumentException("Native retirement requires its actual fallback-bearing owner");
        }
        coordinator = control.coordinator();
    }

    M5BookKeeperNamespaceAuthorityV2 bookKeeper() {
        return bookKeeper;
    }

    PulsarNativeBindingReadViewV1 preferredView() {
        return preferred;
    }

    void requireConfiguration(M5BookKeeperNamespaceAuthorityV2 bookKeeper, NereusPulsarLedgerOffloaderV1 offloader) {
        admission.requireConfiguration(preferred, bookKeeper, offloader);
        if (this.bookKeeper != null && this.bookKeeper != bookKeeper) {
            throw new IllegalArgumentException("Native fallback retry changed its original SDK namespace connection");
        }
        this.bookKeeper = bookKeeper;
    }

    /** The private attempt survives observer mutation; retries retain the exact terminal/proof bytes. */
    synchronized CompletableFuture<Result> retire(Executor executor) {
        if (attempt != null && (!attempt.isDone() || !attempt.isCompletedExceptionally())) {
            return attempt.copy();
        }
        if (drain == null) {
            drain = owner.fence();
        }
        attempt = new CompletableFuture<>();
        var actualAttempt = attempt;
        control.revalidateBinding()
                .thenRunAsync(
                        () -> {
                            if (original.materialized() && physical == null) {
                                physical = new PulsarNativePhysicalReferenceSetV2(
                                        original, preferred, owner.cellBudget(), bookKeeper, predecessor, false);
                                coordinator = control.coordinator(physical);
                            }
                        },
                        executor)
                .thenCompose(ignored -> control.reconcileControlWriterCleanup())
                .thenRunAsync(this::closeFallback, executor)
                .thenCompose(ignored -> drain)
                .thenCompose(ignored -> control.revalidateBinding())
                .thenComposeAsync(ignored -> releaseAndRegister(executor), executor)
                .whenComplete((value, failure) -> {
                    if (failure == null) {
                        actualAttempt.complete(value);
                    } else {
                        actualAttempt.completeExceptionally(failure);
                    }
                });
        return actualAttempt.copy();
    }

    private void closeFallback() {
        requireCurrentPreferred();
        if (successor == null) {
            control.publishSourceMap(preferred, java.util.Optional.ofNullable(physical));
            exact(coordinator.closeFallback(
                    predecessor,
                    preferred.sha256(),
                    Math.addExact(predecessor.sourceGeneration(), 1),
                    owner.fallbackSources()));
            successor = coordinator.readSelector().orElseThrow();
            anchor = successor.pendingAnchors().stream()
                    .filter(value -> value.closedReadAdmissionEpoch() == predecessor.readAdmissionEpoch())
                    .findFirst()
                    .orElseThrow();
            batch = successor.activeBatches().stream()
                    .filter(value ->
                            value.sharedLastFallbackCapableReadAdmissionEpoch() == predecessor.readAdmissionEpoch()
                                    && value.sources().equals(owner.fallbackSources()))
                    .findFirst()
                    .orElseThrow();
            if (!anchor.predecessorSelectorCoreSha256().equals(M4ReadControlCodecV1.selectorCoreSha256(predecessor))
                    || !anchor.successorSelectorCoreSha256().equals(M4ReadControlCodecV1.selectorCoreSha256(successor))
                    || !successor.selectedViewSha256().equals(preferred.sha256())) {
                throw new IllegalStateException("Native fallback closure differs from its exact transition");
            }
        }
        requireSuccessor();
    }

    private CompletableFuture<Result> releaseAndRegister(Executor controlExecutor) {
        if (result != null) {
            return CompletableFuture.completedFuture(result);
        }
        requireSuccessor();
        requireCurrentPreferred();
        if (!drain.isDone()
                || drain.isCompletedExceptionally()
                || drain.isCancelled()
                || owner.authority().admitting()
                || owner.hazardState() != ScanOutcome.CLEAN) {
            throw new IllegalStateException("Native fallback owner lacks confirmed SDK/hazard drain");
        }
        if (terminal == null) {
            terminal = new ReadAdmissionEpochTerminalCut(
                    predecessor.binding(),
                    M4ReadControlCodecV1.anchorSha256(anchor),
                    predecessor.readAdmissionEpoch(),
                    predecessor.ownerEpoch(),
                    predecessor.sourceGeneration(),
                    Math.max(1, System.currentTimeMillis()),
                    predecessor.capability(),
                    TerminalKind.PLANNED_DRAIN,
                    facts(0x4e504646),
                    facts(0x4e504452),
                    0,
                    0,
                    predecessor.ownerEpoch());
            var terminalSha = M4ReadControlCodecV1.terminalSha256(terminal);
            var draft = new ReadQuiescenceProof(
                    predecessor.binding(),
                    predecessor.readAdmissionEpoch(),
                    terminalSha,
                    predecessor.sourceGeneration(),
                    terminal.safeAfterAuthorityTimeMillis(),
                    predecessor.capability(),
                    TerminalKind.PLANNED_DRAIN,
                    terminalSha);
            proof = new ReadQuiescenceProof(
                    draft.binding(),
                    draft.readAdmissionEpoch(),
                    draft.terminalCutSha256(),
                    draft.drainedThroughReadViewGeneration(),
                    draft.safeAfterAuthorityTimeMillis(),
                    draft.capability(),
                    draft.kind(),
                    M4ReadControlCodecV1.calculateProofIdentity(draft));
        }
        exact(coordinator.publishTerminal(terminal));
        exact(coordinator.publishProof(proof));
        for (var source : batch.sources()) {
            exact(coordinator.releaseProtection(batch, source, owner.hazards()));
        }
        requireSuccessor();
        if (registrationFailure != null) {
            throw new IllegalStateException("Native successor registration remains unresolved", registrationFailure);
        }
        var registration = successorOwner == null
                ? owner.registerSuccessor(preferred, successor, controlExecutor)
                : CompletableFuture.completedFuture(successorOwner);
        return registration
                .handle((actor, failure) -> {
                    if (failure != null) {
                        registrationFailure = failure;
                        throw new java.util.concurrent.CompletionException(failure);
                    }
                    successorOwner = actor;
                    return actor;
                })
                .thenCompose(actor -> control.readRetirementFact(control.sourceMapKey(original.sha256()))
                        .thenCompose(sourceMap -> control.readRetirementFact(control.proofHeadKey())
                                .thenApply(head -> {
                                    if (!sourceMap
                                            .canonicalStoredBytes()
                                            .equals(PulsarNativeM4SourceMapV1.capture(original)
                                                    .encode())) {
                                        throw new IllegalStateException(
                                                "Original Native M4 source-map changed during retirement");
                                    }
                                    result = new Result(
                                            successor,
                                            batch,
                                            terminal,
                                            proof,
                                            actor,
                                            predecessor,
                                            sourceMap,
                                            head,
                                            controlExecutor);
                                    return result;
                                })))
                .toCompletableFuture();
    }

    private Sha256Digest facts(int domain) {
        return Sha256Digest.hash(CanonicalBytes.copyOf(ByteBuffer.allocate(196)
                .putInt(domain)
                .put(M4ReadControlCodecV1.selectorCoreSha256(predecessor)
                        .bytes()
                        .toByteArray())
                .put(M4ReadControlCodecV1.selectorCoreSha256(successor).bytes().toByteArray())
                .put(M4ReadControlCodecV1.anchorSha256(anchor).bytes().toByteArray())
                .put(original.sha256().bytes().toByteArray())
                .put(preferred.sha256().bytes().toByteArray())
                .put(admission.receipt().bytes().toByteArray())
                .array()));
    }

    private void requireCurrentPreferred() {
        if (!preferred.isCurrent()) {
            throw new IllegalStateException("Native preferred successor is no longer current");
        }
    }

    private void requireSuccessor() {
        if (!coordinator.readSelector().filter(successor::equals).isPresent()) {
            throw new IllegalStateException("Native fallback successor changed during retirement");
        }
    }

    private static void exact(Outcome outcome) {
        if (outcome != Outcome.APPLIED && outcome != Outcome.EXISTING_EXACT) {
            throw new IllegalStateException("Native fallback metadata is unresolved: " + outcome);
        }
    }
}
