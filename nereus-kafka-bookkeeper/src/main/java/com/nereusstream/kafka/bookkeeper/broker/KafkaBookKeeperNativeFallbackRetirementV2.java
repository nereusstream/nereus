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
package com.nereusstream.kafka.bookkeeper.broker;

import com.nereusstream.domain.bytes.CanonicalBytes;
import com.nereusstream.domain.bytes.Sha256Digest;
import com.nereusstream.storage.object.read.BindingReadAuthorityV1;
import com.nereusstream.storage.object.read.BindingReadPublicationCellV1;
import com.nereusstream.storage.object.read.control.M4ReadControlCodecV1;
import com.nereusstream.storage.object.read.control.M4ReadControlCoordinatorV1;
import com.nereusstream.storage.object.read.control.M4ReadControlCoordinatorV1.Outcome;
import com.nereusstream.storage.object.read.control.M4ReadControlRecordsV1.BindingReadSelector;
import com.nereusstream.storage.object.read.control.M4ReadControlRecordsV1.ClosureAnchor;
import com.nereusstream.storage.object.read.control.M4ReadControlRecordsV1.ReadAdmissionEpochTerminalCut;
import com.nereusstream.storage.object.read.control.M4ReadControlRecordsV1.ReadQuiescenceProof;
import com.nereusstream.storage.object.read.control.M4ReadControlRecordsV1.SelectorMode;
import com.nereusstream.storage.object.read.control.M4ReadControlRecordsV1.SourceProtectionIdentity;
import com.nereusstream.storage.object.read.control.M4ReadControlRecordsV1.SourceRetirementBatch;
import com.nereusstream.storage.object.read.control.M4ReadControlRecordsV1.TerminalKind;
import java.nio.ByteBuffer;
import java.util.List;
import java.util.Objects;
import java.util.Optional;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.CompletionStage;
import java.util.concurrent.Executor;

/** One actual Broker read actor's fused fallback closure and confirmed SDK/hazard drain. */
public final class KafkaBookKeeperNativeFallbackRetirementV2 {
    /** Only this owned retirement can construct a successor after all exact releases are authoritative. */
    public static final class Result {
        private final KafkaBookKeeperM4BrokerReadOwnerV2 readOwner;
        private final SourceRetirementBatch batch;
        private final ReadAdmissionEpochTerminalCut terminal;
        private final ReadQuiescenceProof proof;
        private final Optional<KafkaBookKeeperPartitionV1.NativePreferredReadSources> originalSources;
        private final BindingReadSelector selector;
        private SourceTransfer transfer;

        private Result(
                KafkaBookKeeperM4BrokerReadOwnerV2 readOwner,
                SourceRetirementBatch batch,
                ReadAdmissionEpochTerminalCut terminal,
                ReadQuiescenceProof proof,
                Optional<KafkaBookKeeperPartitionV1.NativePreferredReadSources> originalSources,
                BindingReadSelector selector) {
            this.readOwner = readOwner;
            this.batch = batch;
            this.terminal = terminal;
            this.proof = proof;
            this.originalSources = originalSources;
            this.selector = selector;
        }

        public KafkaBookKeeperM4BrokerReadOwnerV2 readOwner() {
            return readOwner;
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

        public BindingReadSelector selector() {
            return selector;
        }

        /** Available only after this result's actual hot source/checkpoint transfer. */
        public SourceTransfer transfer() {
            if (transfer == null) {
                throw new IllegalStateException("native fallback source transfer has not occurred");
            }
            return transfer;
        }

        void recordTransfer(
                KafkaBookKeeperPartitionV1.NativeInputSources before,
                KafkaBookKeeperPartitionV1.NativeInputSources after) {
            if (transfer != null) {
                throw new IllegalStateException("native fallback source transfer already captured");
            }
            transfer = new SourceTransfer(originalSources.orElseThrow(), before, after);
        }
    }

    /** Immutable observations of an actual owned transfer; callers cannot construct this qualification. */
    public static final class SourceTransfer {
        private final KafkaBookKeeperPartitionV1.NativePreferredReadSources original;
        private final KafkaBookKeeperPartitionV1.NativeInputSources before;
        private final KafkaBookKeeperPartitionV1.NativeInputSources after;

        private SourceTransfer(
                KafkaBookKeeperPartitionV1.NativePreferredReadSources original,
                KafkaBookKeeperPartitionV1.NativeInputSources before,
                KafkaBookKeeperPartitionV1.NativeInputSources after) {
            this.original = original;
            this.before = before;
            this.after = after;
            if (!original.rawSources().equals(before.sources())
                    || !original.retiredTail().rawSources().equals(after.sources())) {
                throw new IllegalStateException("native transferred sources differ from the actual read actor");
            }
            var old = before.protocol();
            var current = after.protocol();
            if (!old.root().fence().equals(current.root().fence())
                    || !old.root().frontiers().equals(current.root().frontiers())
                    || current.root().stateVersion()
                            != Math.incrementExact(old.root().stateVersion())
                    || !old.activeTail().equals(current.activeTail())
                    || !old.committedProducerState().equals(current.committedProducerState())
                    || !old.speculativeQueue().equals(current.speculativeQueue())
                    || !old.transactionState().equals(current.transactionState())
                    || !old.leaderEpochIndex().equals(current.leaderEpochIndex())) {
                throw new IllegalStateException("native transferred protocol state changed a logical obligation");
            }
            var removed = before.sources().sealedRoots().stream()
                    .filter(root -> root.kafkaStartOffset()
                            < original.descriptor().sourceCut().coverage().exclusiveEnd())
                    .map(root -> root.runId())
                    .toList();
            var checkpoint = after.recoveryCheckpoint().orElseThrow();
            if (!before.recoveryCheckpoint().orElseThrow().components().equals(checkpoint.components())
                    || removed.contains(checkpoint.vector().runBinding().runId())
                    || checkpoint.readIndexes().stream().anyMatch(index -> removed.contains(index.runId()))) {
                throw new IllegalStateException("native transferred checkpoint retains a covered physical source");
            }
        }

        public KafkaBookKeeperPartitionV1.NativePreferredReadSources original() {
            return original;
        }

        public KafkaBookKeeperPartitionV1.NativeInputSources before() {
            return before;
        }

        public KafkaBookKeeperPartitionV1.NativeInputSources after() {
            return after;
        }
    }

    private record FallbackView(
            Object preferredOnly,
            Sha256Digest selectedView,
            Sha256Digest rawView,
            List<SourceProtectionIdentity> protections,
            Optional<KafkaBookKeeperPartitionV1.NativePreferredReadSources> compactedBookKeeper) {
        private static FallbackView capture(Object actual) {
            if (actual instanceof KafkaBookKeeperPartitionV1.NativePreferredReadSources preferred) {
                return new FallbackView(
                        preferred.retiredTail(),
                        preferred.descriptor().descriptorSha256(),
                        preferred.rawSources().selectedViewSha256(),
                        preferred.fallbackSources(),
                        Optional.of(preferred));
            }
            if (actual instanceof KafkaBookKeeperNativeObjectReadSourcesV1 selected && selected.fallback()) {
                return new FallbackView(
                        selected.withoutFallback(),
                        selected.prefix().manifestSha256(),
                        selected.rawSources().selectedViewSha256(),
                        selected.fallbackSources(),
                        Optional.empty());
            }
            throw new IllegalStateException("native fallback retirement lacks its actual selected reader");
        }
    }

    private final KafkaBookKeeperM4BrokerReadOwnerV2 owner;
    private final M4ReadControlCoordinatorV1 coordinator;
    private final BindingReadAuthorityV1 authority;
    private final FallbackView sources;
    private BindingReadSelector predecessor;
    private BindingReadSelector successor;
    private BindingReadAuthorityV1 successorAuthority;
    private ClosureAnchor anchor;
    private SourceRetirementBatch batch;
    private ReadAdmissionEpochTerminalCut terminal;
    private ReadQuiescenceProof proof;
    private KafkaBookKeeperM4BrokerReadOwnerV2 preferredObjectOwner;
    private Result result;

    KafkaBookKeeperNativeFallbackRetirementV2(
            KafkaBookKeeperM4BrokerReadOwnerV2 owner,
            M4ReadControlCoordinatorV1 coordinator,
            BindingReadAuthorityV1 authority) {
        this.owner = Objects.requireNonNull(owner, "owner");
        this.coordinator = Objects.requireNonNull(coordinator, "coordinator");
        this.authority = Objects.requireNonNull(authority, "authority");
        if (!authority.admitting()) {
            throw new IllegalStateException("native fallback retirement lacks its actual admitted selected reader");
        }
        sources = FallbackView.capture(authority.publicationCell().protocolStateReference());
        owner.closeAdmission();
        owner.selectorRuntime().stopLocalAdmission();
    }

    /** The existing control executor serializes exact metadata attempts; cancellation affects only the observer. */
    public CompletionStage<Result> retire(Executor control) {
        if (sources.compactedBookKeeper().isEmpty()) {
            throw new IllegalStateException("Object fallback retirement must open its preferred-only read actor first");
        }
        return CompletableFuture.runAsync(this::closeFallback, control)
                .thenCompose(ignored -> owner.drain())
                .thenApplyAsync(ignored -> releaseAfterConfirmedDrain(), control)
                .thenApply(value -> value);
    }

    CompletionStage<KafkaBookKeeperM4BrokerReadOwnerV2> closeObjectAdmission(Executor control) {
        if (sources.compactedBookKeeper().isPresent()) {
            throw new IllegalStateException(
                    "native BK compaction still requires a drained predecessor before activation");
        }
        return CompletableFuture.supplyAsync(
                        () -> {
                            closeFallback();
                            if (preferredObjectOwner == null) {
                                preferredObjectOwner = owner.successorAfterFence(successor, successorAuthority);
                            }
                            return preferredObjectOwner;
                        },
                        control)
                .thenApply(value -> value);
    }

    CompletionStage<Result> releaseObjectAfterDrain(Executor control, KafkaBookKeeperM4BrokerReadOwnerV2 installed) {
        if (installed == null || installed != preferredObjectOwner) {
            throw new IllegalStateException("Object fallback retirement lacks its active preferred-only read actor");
        }
        return owner.drain()
                .thenApplyAsync(ignored -> releaseAfterConfirmedDrain(), control)
                .thenApply(value -> value);
    }

    public KafkaBookKeeperM4BrokerReadOwnerV2 predecessorOwner() {
        return owner;
    }

    public Sha256Digest descriptorSha256() {
        return sources.selectedView();
    }

    boolean owns(Result candidate) {
        return result == candidate;
    }

    private void closeFallback() {
        if (result != null) {
            return;
        }
        if (predecessor == null) {
            var observed = coordinator.readSelector().orElseThrow();
            if (observed.mode() != SelectorMode.PREFERRED_WITH_FALLBACK
                    || !observed.selectedViewSha256().equals(descriptorSha256())) {
                throw new IllegalStateException("native fallback closure differs from its selected descriptor");
            }
            predecessor = observed;
            var cell = authority.publicationCell();
            var next = new BindingReadPublicationCellV1(
                    Math.addExact(predecessor.sourceGeneration(), 1),
                    cell.readableUpperBound(),
                    cell.activeTailViewVersion(),
                    cell.routes(),
                    sources.preferredOnly());
            successorAuthority = new BindingReadAuthorityV1(
                    authority.bindingId(),
                    authority.topicIncarnationIdentity(),
                    authority.storageEpochId(),
                    authority.protocol(),
                    authority.selectedViewSha256(),
                    authority.ownerEpoch(),
                    Math.addExact(authority.readAdmissionEpoch(), 1),
                    true,
                    authority.capabilityGeneration(),
                    authority.capabilityEvidenceSha256(),
                    next);
        }
        if (successor == null) {
            exact(owner.selectorRuntime()
                    .closeFallback(
                            predecessor,
                            authority,
                            successorAuthority,
                            descriptorSha256(),
                            successorAuthority.sourceGeneration(),
                            sources.protections()));
            successor = coordinator.readSelector().orElseThrow();
            anchor = successor.pendingAnchors().stream()
                    .filter(value -> value.closedReadAdmissionEpoch() == predecessor.readAdmissionEpoch())
                    .findFirst()
                    .orElseThrow();
            batch = successor.activeBatches().stream()
                    .filter(value ->
                            value.sharedLastFallbackCapableReadAdmissionEpoch() == predecessor.readAdmissionEpoch()
                                    && value.sources().equals(sources.protections()))
                    .findFirst()
                    .orElseThrow();
            if (!anchor.predecessorSelectorCoreSha256().equals(M4ReadControlCodecV1.selectorCoreSha256(predecessor))
                    || !anchor.successorSelectorCoreSha256()
                            .equals(M4ReadControlCodecV1.selectorCoreSha256(successor))) {
                throw new IllegalStateException("native fallback closure anchor differs from its actual transition");
            }
        }
        requireSuccessor();
    }

    private Result releaseAfterConfirmedDrain() {
        if (result != null) {
            return result;
        }
        requireSuccessor();
        if (terminal == null) {
            // This code runs only after the actual actor's accepted IO and exact hazard leases have drained.
            var fence = facts(0x4d424346);
            var evidence = facts(0x4d424452);
            terminal = new ReadAdmissionEpochTerminalCut(
                    predecessor.binding(),
                    M4ReadControlCodecV1.anchorSha256(anchor),
                    predecessor.readAdmissionEpoch(),
                    predecessor.ownerEpoch(),
                    predecessor.sourceGeneration(),
                    Math.max(1, System.currentTimeMillis()),
                    predecessor.capability(),
                    TerminalKind.PLANNED_DRAIN,
                    fence,
                    evidence,
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
        var selected = requireSuccessor();
        result = new Result(
                preferredObjectOwner == null
                        ? owner.successorAfterDrain(selected, successorAuthority)
                        : preferredObjectOwner,
                batch,
                terminal,
                proof,
                sources.compactedBookKeeper(),
                selected);
        return result;
    }

    private Sha256Digest facts(int domain) {
        return Sha256Digest.hash(CanonicalBytes.copyOf(ByteBuffer.allocate(132)
                .putInt(domain)
                .put(M4ReadControlCodecV1.selectorCoreSha256(predecessor)
                        .bytes()
                        .toByteArray())
                .put(M4ReadControlCodecV1.selectorCoreSha256(successor).bytes().toByteArray())
                .put(M4ReadControlCodecV1.anchorSha256(anchor).bytes().toByteArray())
                .put(sources.rawView().bytes().toByteArray())
                .array()));
    }

    private BindingReadSelector requireSuccessor() {
        var current = coordinator.readSelector().orElseThrow();
        if (current.equals(successor)) {
            return current;
        }
        // A preferred-only Object actor may keep rolling BK tail sources while the old fallback reads drain.
        // This advances only the source generation; the closed epoch, selected manifest and proof batch remain exact.
        if (preferredObjectOwner != null
                && current.binding().equals(successor.binding())
                && current.selectedViewSha256().equals(successor.selectedViewSha256())
                && current.ownerEpoch() == successor.ownerEpoch()
                && current.readAdmissionEpoch() == successor.readAdmissionEpoch()
                && current.sourceGeneration() > successor.sourceGeneration()
                && current.mode() == successor.mode()
                && current.admissionState() == successor.admissionState()
                && current.capability().equals(successor.capability())
                && current.fallbackSetSha256().equals(successor.fallbackSetSha256())
                && current.pendingAnchors().contains(anchor)
                && current.activeBatches().contains(batch)) {
            return current;
        }
        throw new IllegalStateException("native fallback retirement selector changed");
    }

    private static void exact(Outcome outcome) {
        if (outcome != Outcome.APPLIED && outcome != Outcome.EXISTING_EXACT) {
            throw new IllegalStateException("native fallback retirement is unresolved: " + outcome);
        }
    }
}
