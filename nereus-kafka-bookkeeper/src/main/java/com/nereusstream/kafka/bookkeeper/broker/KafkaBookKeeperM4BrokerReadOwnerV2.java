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

import com.nereusstream.domain.bytes.Sha256Digest;
import com.nereusstream.domain.codec.TopicIncarnationIdentityCodecV1;
import com.nereusstream.storage.object.read.BindingReadAsyncExecutorV1;
import com.nereusstream.storage.object.read.BindingReadAuthorityV1;
import com.nereusstream.storage.object.read.BindingReadHazardPoolV1;
import com.nereusstream.storage.object.read.BindingReadPublicationCellV1;
import com.nereusstream.storage.object.read.BindingReadRouteTableV1;
import com.nereusstream.storage.object.read.control.BindingReadSelectorRuntimeV1;
import com.nereusstream.storage.object.read.control.M4ReadControlCoordinatorV1;
import com.nereusstream.storage.object.read.control.M4ReadControlCoordinatorV1.Outcome;
import com.nereusstream.storage.object.read.control.M4ReadControlRecordsV1.BindingReadSelector;
import com.nereusstream.storage.object.read.control.M4ReadControlRecordsV1.SelectorMode;
import java.util.List;
import java.util.Objects;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.CompletionStage;
import java.util.concurrent.Executor;
import java.util.concurrent.Semaphore;

/** One Broker partition's long-lived M4 admission and hazard lifetime over the native BK session. */
public final class KafkaBookKeeperM4BrokerReadOwnerV2 {
    /** Returned data/failure is separate from confirmed physical drain and an uncertain-close signal. */
    public record ReadLifetime<T>(
            CompletionStage<T> result, CompletionStage<Void> drained, CompletionStage<Void> unknown) {
        public ReadLifetime {
            Objects.requireNonNull(result, "result");
            Objects.requireNonNull(drained, "drained");
            Objects.requireNonNull(unknown, "unknown");
        }
    }

    @FunctionalInterface
    public interface OwnedRead<T> {
        ReadLifetime<T> start(BindingReadAuthorityV1 authority);
    }

    private final M4ReadControlCoordinatorV1 coordinator;
    private final BindingReadSelectorRuntimeV1 selector;
    private final BindingReadHazardPoolV1 hazards;
    private final BindingReadAsyncExecutorV1 executor;
    private final CompletableFuture<Void> drained = new CompletableFuture<>();
    private final int maximumInFlightReads;
    private final Executor ownerEventLoop;
    private final Semaphore cellReadSlots;
    private final Semaphore bindingReadSlots;
    private int inFlight;
    private boolean closed;

    public KafkaBookKeeperM4BrokerReadOwnerV2(
            M4ReadControlCoordinatorV1 coordinator,
            BindingReadSelector durableSelector,
            BindingReadAuthorityV1 authority,
            Executor ownerEventLoop,
            int maximumInFlightReads) {
        this(
                coordinator,
                durableSelector,
                authority,
                ownerEventLoop,
                maximumInFlightReads,
                new Semaphore(maximumInFlightReads));
    }

    public KafkaBookKeeperM4BrokerReadOwnerV2(
            M4ReadControlCoordinatorV1 coordinator,
            BindingReadSelector durableSelector,
            BindingReadAuthorityV1 authority,
            Executor ownerEventLoop,
            int maximumInFlightReads,
            Semaphore cellReadSlots) {
        this(
                coordinator,
                durableSelector,
                authority,
                ownerEventLoop,
                maximumInFlightReads,
                cellReadSlots,
                new Semaphore(maximumInFlightReads));
    }

    public KafkaBookKeeperM4BrokerReadOwnerV2(
            M4ReadControlCoordinatorV1 coordinator,
            BindingReadSelector durableSelector,
            BindingReadAuthorityV1 authority,
            Executor ownerEventLoop,
            int maximumInFlightReads,
            Semaphore cellReadSlots,
            Semaphore bindingReadSlots) {
        Objects.requireNonNull(ownerEventLoop, "ownerEventLoop");
        this.ownerEventLoop = ownerEventLoop;
        this.cellReadSlots = Objects.requireNonNull(cellReadSlots, "cellReadSlots");
        this.bindingReadSlots = Objects.requireNonNull(bindingReadSlots, "bindingReadSlots");
        this.coordinator = Objects.requireNonNull(coordinator, "coordinator");
        selector = new BindingReadSelectorRuntimeV1(durableSelector.binding(), coordinator, durableSelector, authority);
        selector.installExactDurable(authority);
        this.maximumInFlightReads = maximumInFlightReads;
        hazards = new BindingReadHazardPoolV1(maximumInFlightReads, 4);
        executor = new BindingReadAsyncExecutorV1(ownerEventLoop, maximumInFlightReads);
    }

    /** The returned observer is detached from the internal source callback and exact hazard clear. */
    public <T> CompletionStage<T> read(BindingReadAsyncExecutorV1.CapturedOperation<T> operation) {
        return readOwned(
                authority -> {
                    var result = operation.start(authority);
                    return new ReadLifetime<>(
                            result, result.handle((ignored, failure) -> null), new CompletableFuture<>());
                },
                false);
    }

    /** One existing Cell/Binding reservation covers serial owned historical handles until actual drain. */
    public <T> CompletionStage<T> readOwned(OwnedRead<T> operation) {
        return readOwned(operation, true);
    }

    private <T> CompletionStage<T> readOwned(OwnedRead<T> operation, boolean publishResultBeforeDrain) {
        if (!cellReadSlots.tryAcquire()) {
            return CompletableFuture.failedFuture(new IllegalStateException("native M4 Cell read capacity is full"));
        }
        if (!bindingReadSlots.tryAcquire()) {
            cellReadSlots.release();
            return CompletableFuture.failedFuture(new IllegalStateException("native M4 Binding read capacity is full"));
        }
        synchronized (this) {
            if (closed) {
                cellReadSlots.release();
                bindingReadSlots.release();
                return CompletableFuture.failedFuture(new IllegalStateException("native M4 read owner is fenced"));
            }
            if (inFlight >= maximumInFlightReads) {
                cellReadSlots.release();
                bindingReadSlots.release();
                return CompletableFuture.failedFuture(
                        new IllegalStateException("native M4 read owner capacity is full"));
            }
            inFlight++;
        }
        final CompletableFuture<T> internal;
        var observer = new CompletableFuture<T>();
        try {
            internal = executor.execute(selector.currentAuthority(), hazards, authority -> {
                var lifetime = Objects.requireNonNull(operation.start(authority), "native read lifetime");
                var confirmedDrain = new CompletableFuture<Void>();
                lifetime.drained().whenComplete((ignored, failure) -> {
                    if (failure == null) {
                        confirmedDrain.complete(null);
                    } else {
                        drained.completeExceptionally(failure);
                        observer.completeExceptionally(failure);
                    }
                });
                lifetime.unknown().whenComplete((ignored, failure) -> {
                    if (failure != null) {
                        drained.completeExceptionally(failure);
                    }
                });
                if (publishResultBeforeDrain) {
                    lifetime.result().whenComplete((result, failure) -> {
                        if (failure == null) {
                            observer.complete(result);
                        } else {
                            observer.completeExceptionally(failure);
                        }
                    });
                }
                return lifetime.result()
                        .handle((result, failure) -> confirmedDrain.thenCompose(ignored -> failure == null
                                ? CompletableFuture.completedFuture(result)
                                : CompletableFuture.<T>failedFuture(failure)))
                        .thenCompose(terminal -> terminal);
            });
        } catch (RuntimeException | Error failure) {
            completeRead();
            throw failure;
        }
        internal.whenComplete((result, failure) -> {
            completeRead();
            if (failure == null) {
                observer.complete(result);
            } else {
                observer.completeExceptionally(failure);
            }
        });
        return observer.thenApply(value -> value);
    }

    public BindingReadSelectorRuntimeV1 selectorRuntime() {
        return selector;
    }

    public BindingReadHazardPoolV1 hazards() {
        return hazards;
    }

    public synchronized KafkaBookKeeperNativeFallbackRetirementV2 beginNativeFallbackRetirement() {
        if (closed) {
            throw new IllegalStateException("native fallback reader is already fenced");
        }
        return new KafkaBookKeeperNativeFallbackRetirementV2(
                this, coordinator, selector.currentAuthority().get());
    }

    synchronized KafkaBookKeeperM4BrokerReadOwnerV2 successorAfterDrain(
            BindingReadSelector durable, BindingReadAuthorityV1 authority) {
        requireConfirmedDrain();
        return successorAfterFence(durable, authority);
    }

    synchronized KafkaBookKeeperM4BrokerReadOwnerV2 successorAfterFence(
            BindingReadSelector durable, BindingReadAuthorityV1 authority) {
        if (!closed) {
            throw new IllegalStateException("native predecessor read admission is still open");
        }
        return new KafkaBookKeeperM4BrokerReadOwnerV2(
                coordinator, durable, authority, ownerEventLoop, maximumInFlightReads, cellReadSlots, bindingReadSlots);
    }

    synchronized void requireConfirmedDrain() {
        if (!closed || inFlight != 0 || !drained.isDone() || drained.isCompletedExceptionally()) {
            throw new IllegalStateException("native fallback actor has not confirmed its actual drain");
        }
    }

    /** Reconcile a recovered source map against the actual selector; caller retains its cache/source capacity. */
    public KafkaBookKeeperM4BrokerReadOwnerV2 installNativePreferred(
            KafkaBookKeeperPartitionV1.NativePreferredReadSources sources, long readableUpperBound) {
        BindingReadSelector durable = coordinator
                .readSelector()
                .orElseThrow(() -> new IllegalStateException("native preferred selector is absent"));
        BindingReadAuthorityV1 previous = selector.currentAuthority().get();
        var predecessor = sources.descriptor().sourceCut().predecessorSelector();
        if (previous.admitting()
                || previous.ownerEpoch() != durable.ownerEpoch()
                || previous.ownerEpoch() != predecessor.ownerEpoch()
                || previous.readAdmissionEpoch() != predecessor.readAdmissionEpoch()
                || !previous.selectedViewSha256().equals(predecessor.selectedViewSha256())
                || durable.readAdmissionEpoch() != Math.addExact(predecessor.readAdmissionEpoch(), 1)
                || durable.mode() != SelectorMode.PREFERRED_WITH_FALLBACK
                || durable.sourceGeneration() != sources.descriptor().sourceGeneration()
                || !durable.selectedViewSha256().equals(sources.descriptor().descriptorSha256())) {
            throw new IllegalStateException("native preferred activation lacks its stopped exact owner/selector");
        }
        var cell = new BindingReadPublicationCellV1(
                durable.sourceGeneration(), readableUpperBound, 0, new BindingReadRouteTableV1(List.of()), sources);
        var authority = new BindingReadAuthorityV1(
                previous.bindingId(),
                previous.topicIncarnationIdentity(),
                previous.storageEpochId(),
                previous.protocol(),
                durable.selectedViewSha256(),
                durable.ownerEpoch(),
                durable.readAdmissionEpoch(),
                true,
                durable.capability().generation(),
                durable.capability().evidenceSha256(),
                cell);
        synchronized (this) {
            if (closed) {
                return successorAfterDrain(durable, authority);
            }
            selector.installExactDurable(authority);
            return this;
        }
    }

    /** Reopen only the raw source map still named by the actual durable selector after a known non-selection. */
    public KafkaBookKeeperM4BrokerReadOwnerV2 resumeNativeRaw(
            KafkaBookKeeperPartitionV1.NativeReadSources sources, long upperBound) {
        return resumeNativeSources(sources, sources.selectedViewSha256(), upperBound);
    }

    public KafkaBookKeeperM4BrokerReadOwnerV2 resumeNativePreferred(
            KafkaBookKeeperPartitionV1.NativePreferredReadSources sources, long upperBound) {
        return resumeNativeSources(sources, sources.descriptor().descriptorSha256(), upperBound);
    }

    void installNativeObjectPreferred(KafkaBookKeeperNativeObjectReadSourcesV1 sources, long upperBound) {
        var durable = coordinator.readSelector().orElseThrow();
        sources.requireSelector(durable);
        var previous = selector.currentAuthority().get();
        var predecessor = sources.prefix().sourceCut().predecessorSelector();
        if (previous.admitting()
                || previous.ownerEpoch() != durable.ownerEpoch()
                || previous.ownerEpoch() != predecessor.ownerEpoch()
                || previous.readAdmissionEpoch() != predecessor.readAdmissionEpoch()
                || !previous.selectedViewSha256().equals(predecessor.selectedViewSha256())
                || durable.readAdmissionEpoch() != Math.addExact(predecessor.readAdmissionEpoch(), 1)
                || durable.sourceGeneration() != sources.prefix().sourceGeneration()) {
            throw new IllegalStateException(
                    "native Object activation does not follow the stopped exact BK predecessor");
        }
        var cell = new BindingReadPublicationCellV1(
                durable.sourceGeneration(), upperBound, 0, new BindingReadRouteTableV1(List.of()), sources);
        var authority = new BindingReadAuthorityV1(
                previous.bindingId(),
                previous.topicIncarnationIdentity(),
                previous.storageEpochId(),
                previous.protocol(),
                durable.selectedViewSha256(),
                durable.ownerEpoch(),
                durable.readAdmissionEpoch(),
                true,
                durable.capability().generation(),
                durable.capability().evidenceSha256(),
                cell);
        synchronized (this) {
            if (closed) {
                throw new IllegalStateException("native Object activation was fenced");
            }
            selector.installExactDurable(authority);
        }
    }

    /** Replace only unused BK recovery references; the selected Object/BK Fetch plan and generation are unchanged. */
    boolean transferRetainedObjectTail(BindingReadAuthorityV1 before, KafkaBookKeeperNativeObjectReadSourcesV1 tail) {
        var cell = before.publicationCell();
        var next = new BindingReadAuthorityV1(
                before.bindingId(),
                before.topicIncarnationIdentity(),
                before.storageEpochId(),
                before.protocol(),
                before.selectedViewSha256(),
                before.ownerEpoch(),
                before.readAdmissionEpoch(),
                before.admitting(),
                before.capabilityGeneration(),
                before.capabilityEvidenceSha256(),
                new BindingReadPublicationCellV1(
                        cell.sourceGeneration(),
                        cell.readableUpperBound(),
                        cell.activeTailViewVersion(),
                        cell.routes(),
                        tail));
        return selector.currentAuthority().compareAndSet(before, next);
    }

    private KafkaBookKeeperM4BrokerReadOwnerV2 resumeNativeSources(Object sources, Sha256Digest view, long upperBound) {
        var durable = coordinator.readSelector().orElseThrow();
        var previous = selector.currentAuthority().get();
        if (durable.mode() != SelectorMode.PREFERRED_ONLY
                || durable.ownerEpoch() != previous.ownerEpoch()
                || !durable.selectedViewSha256().equals(view)) {
            throw new IllegalStateException("native raw resume differs from its exact current selector");
        }
        var cell = new BindingReadPublicationCellV1(
                durable.sourceGeneration(), upperBound, 0, new BindingReadRouteTableV1(List.of()), sources);
        var authority = new BindingReadAuthorityV1(
                previous.bindingId(),
                previous.topicIncarnationIdentity(),
                previous.storageEpochId(),
                previous.protocol(),
                durable.selectedViewSha256(),
                durable.ownerEpoch(),
                durable.readAdmissionEpoch(),
                true,
                durable.capability().generation(),
                durable.capability().evidenceSha256(),
                cell);
        synchronized (this) {
            if (closed) {
                return successorAfterDrain(durable, authority);
            }
            selector.installExactDurable(authority);
            return this;
        }
    }

    /** Close new captures under the partition lock before any remote selector read or CAS. */
    public BindingReadAuthorityV1 stopNativeRolloverAdmission() {
        return selector.stopLocalAdmission();
    }

    /** Publish one rollover source map after the partition freezes the exact predecessor locally. */
    public void publishNativeRollover(
            BindingReadAuthorityV1 predecessor,
            KafkaBookKeeperPartitionV1.NativeReadSources successorSources,
            long readableUpperBound) {
        Objects.requireNonNull(predecessor, "predecessor");
        Objects.requireNonNull(successorSources, "successorSources");
        try {
            BindingReadSelector durable = coordinator
                    .readSelector()
                    .orElseThrow(() -> new IllegalStateException("native M4 selector disappeared during rollover"));
            var payload = predecessor.publicationCell().protocolStateReference();
            var preferred =
                    payload instanceof KafkaBookKeeperPartitionV1.NativePreferredReadSources selected ? selected : null;
            var object = payload instanceof KafkaBookKeeperNativeObjectReadSourcesV1 selected ? selected : null;
            if (object != null) {
                object.requireSelector(durable);
            } else if (preferred == null) {
                if (durable.mode() != SelectorMode.PREFERRED_ONLY
                        || durable.fallbackSetSha256().isPresent()) {
                    throw new IllegalStateException("raw native BK rollover requires its preferred-only M4 selector");
                }
            } else if (!durable.selectedViewSha256()
                            .equals(preferred.descriptor().descriptorSha256())
                    || durable.sourceGeneration() < preferred.descriptor().sourceGeneration()) {
                throw new IllegalStateException("native BK rollover lost its exact preferred descriptor");
            }
            var active = successorSources.activeRoot();
            if (!active.bindingId().equals(predecessor.bindingId())
                    || !Sha256Digest.hash(TopicIncarnationIdentityCodecV1.encode(active.topicIncarnation()))
                            .equals(predecessor.topicIncarnationIdentity())
                    || !active.storageEpochId().equals(predecessor.storageEpochId())) {
                throw new IllegalArgumentException("native BK rollover source map belongs to another M4 Binding");
            }
            long successorGeneration = Math.addExact(durable.sourceGeneration(), 1);
            Object successorPayload = object != null
                    ? object.withRawSources(successorSources)
                    : preferred == null ? successorSources : preferred.withRawSources(successorSources);
            Sha256Digest successorView = object != null
                    ? object.prefix().manifestSha256()
                    : preferred == null
                            ? successorSources.selectedViewSha256()
                            : preferred.descriptor().descriptorSha256();
            var fallbackSources = object != null
                    ? object.fallbackSources()
                    : preferred != null && durable.fallbackSetSha256().isPresent()
                            ? preferred.fallbackSources()
                            : List
                                    .<com.nereusstream.storage.object.read.control.M4ReadControlRecordsV1
                                                    .SourceProtectionIdentity>
                                            of();
            var cell = new BindingReadPublicationCellV1(
                    successorGeneration,
                    readableUpperBound,
                    0,
                    new BindingReadRouteTableV1(List.of()),
                    successorPayload);
            var successor = new BindingReadAuthorityV1(
                    predecessor.bindingId(),
                    predecessor.topicIncarnationIdentity(),
                    predecessor.storageEpochId(),
                    predecessor.protocol(),
                    successorView,
                    predecessor.ownerEpoch(),
                    predecessor.readAdmissionEpoch(),
                    true,
                    predecessor.capabilityGeneration(),
                    predecessor.capabilityEvidenceSha256(),
                    cell);
            Outcome outcome = selector.updateMembershipNeutralView(
                    durable, predecessor, successor, successorView, successorGeneration, fallbackSources);
            if (outcome != Outcome.APPLIED && outcome != Outcome.EXISTING_EXACT) {
                throw new IllegalStateException("native BK rollover M4 selector update is unresolved: " + outcome);
            }
        } catch (RuntimeException | Error failure) {
            closeAdmission();
            throw failure;
        }
    }

    /** Fences new captures; accepted Provider work and its exact hazard lease remain live. */
    public synchronized void closeAdmission() {
        if (!closed) {
            closed = true;
            hazards.closeAdmission();
            if (inFlight == 0) {
                drained.complete(null);
            }
        }
    }

    /** The owning partition closes its native session only after this and its local reads drain. */
    public CompletionStage<Void> drain() {
        closeAdmission();
        return drained.thenApply(ignored -> null);
    }

    private synchronized void completeRead() {
        inFlight--;
        cellReadSlots.release();
        bindingReadSlots.release();
        if (closed && inFlight == 0) {
            drained.complete(null);
        }
    }
}
