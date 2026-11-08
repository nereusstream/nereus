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

package com.nereusstream.pulsar.broker;

import com.nereusstream.domain.bytes.Sha256Digest;
import com.nereusstream.pulsar.offload.PulsarNativeBindingV1;
import com.nereusstream.pulsar.offload.objectwal.PulsarObjectBindingReadAdapterV1;
import com.nereusstream.pulsar.offload.objectwal.PulsarObjectWalBridgeV1;
import com.nereusstream.pulsar.offload.objectwal.PulsarObjectWalBridgeV1.AppendInput;
import com.nereusstream.pulsar.offload.objectwal.PulsarObjectWalBridgeV1.BridgeException;
import com.nereusstream.pulsar.offload.objectwal.PulsarObjectWalBridgeV1.BridgeRejectionCode;
import com.nereusstream.pulsar.offload.objectwal.PulsarObjectWalBridgeV1.ExtentIdentity;
import com.nereusstream.pulsar.offload.objectwal.PulsarObjectWalBridgeV1.ManifestHandoffRequest;
import com.nereusstream.pulsar.offload.objectwal.PulsarObjectWalBridgeV1.ManifestSource;
import com.nereusstream.pulsar.offload.objectwal.PulsarObjectWalBridgeV1.MemberAppendResult;
import com.nereusstream.pulsar.offload.objectwal.PulsarObjectWalBridgeV1.ObjectWalManifestOperations;
import com.nereusstream.pulsar.offload.objectwal.PulsarObjectWalBridgeV1.ObjectWalSuccessorAuthority;
import com.nereusstream.pulsar.offload.objectwal.PulsarObjectWalBridgeV1.PlannedEntry;
import com.nereusstream.pulsar.offload.objectwal.PulsarObjectWalBridgeV1.ProductionObjectWalContext;
import com.nereusstream.pulsar.offload.objectwal.PulsarObjectWalBridgeV1.ProductionObjectWalExtentStore;
import com.nereusstream.pulsar.offload.objectwal.PulsarObjectWalBridgeV1.ProductionWalRunClosure;
import com.nereusstream.pulsar.offload.objectwal.PulsarObjectWalBridgeV1.PublishResult;
import com.nereusstream.pulsar.offload.objectwal.PulsarObjectWalBridgeV1.PulsarPosition;
import com.nereusstream.pulsar.offload.objectwal.PulsarObjectWalBridgeV1.ReadEntry;
import com.nereusstream.pulsar.offload.objectwal.PulsarObjectWalBridgeV1.SealedExtentPlan;
import com.nereusstream.pulsar.offload.objectwal.PulsarObjectWalBridgeV1.VerifiedAppend;
import com.nereusstream.pulsar.offload.objectwal.PulsarObjectWalBridgeV1.VerifiedManifestHandoff;
import com.nereusstream.pulsar.offload.objectwal.PulsarObjectWalM4ReaderV1;
import com.nereusstream.pulsar.offload.objectwal.PulsarObjectWalReadViewV1;
import com.nereusstream.pulsar.offload.objectwal.PulsarVirtualLedgerChainControllerV1.FixedSlice;
import com.nereusstream.pulsar.offload.objectwal.PulsarVirtualLedgerChainControllerV1.PulsarBindingKey;
import com.nereusstream.storage.object.control.WalLaneId;
import com.nereusstream.storage.object.control.WalRunControlKeys;
import com.nereusstream.storage.object.control.WalRunLifecycleManager;
import com.nereusstream.storage.object.control.WalRunPredecessor;
import com.nereusstream.storage.object.kms.KmsCellSession;
import com.nereusstream.storage.object.read.BindingReadHazardPoolV1;
import com.nereusstream.storage.object.read.control.M4ReadControlRecordsV1.CapabilityBinding;
import io.oxia.client.api.AsyncOxiaClient;
import java.time.Duration;
import java.util.List;
import java.util.Map;
import java.util.Optional;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.CompletionException;
import java.util.concurrent.CompletionStage;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.Executor;
import java.util.concurrent.TimeUnit;
import java.util.function.Supplier;
import org.apache.pulsar.broker.storage.nereus.v2.NereusPulsarTopicAuthority;

/** The native topic's admitted Object WAL writer, captured read source, and original terminal drain. */
final class PulsarM6ObjectWalTopic {
    private static final int RECOVERY_LOCATORS = 8192;
    private static final int PACKING_POLICY = 1;
    private static final WalLaneId LANE = WalLaneId.OBJECT_LATENCY;
    private final PulsarM6ObjectWalAllocator.BoundChain chain;
    private final PulsarM6ObjectWalRuns runs;
    private final PulsarM6ObjectWalProvider provider;
    private final PulsarNativeBindingV1 binding;
    private final FixedSlice geometry;
    private final Limits limits;
    private final Duration timeout;
    private final Executor io;
    private final Executor ownerLoop;
    private final Map<PulsarPosition, Integer> decodedSizes;
    private volatile Run run;
    private CompletableFuture<Run> runRollover;
    private CompletableFuture<Void> appendTail = CompletableFuture.completedFuture(null);
    private CompletableFuture<Void> closing;
    private Throwable appendFailure;
    private int pendingAppends;
    private long pendingBytes;

    record Limits(
            int maximumEntryBytes,
            long entriesPerLedger,
            int maximumPendingAppends,
            long pendingBytes,
            int maximumReadEntries,
            int readSlots) {
        Limits {
            if (maximumEntryBytes <= 0
                    || entriesPerLedger <= 0
                    || entriesPerLedger > RECOVERY_LOCATORS
                    || maximumPendingAppends <= 0
                    || pendingBytes < maximumEntryBytes
                    || maximumReadEntries <= 0
                    || readSlots <= 0
                    || readSlots > 65_536) {
                throw new IllegalArgumentException("Pulsar Object WAL limits are outside their admitted bounds");
            }
        }
    }

    static CompletionStage<PulsarM6ObjectWalTopic> open(
            PulsarM6ObjectWalAllocator allocator,
            PulsarM6ObjectWalProvider provider,
            AsyncOxiaClient oxia,
            String root,
            PulsarNativeBindingV1 binding,
            NereusPulsarTopicAuthority authority,
            Limits limits,
            Duration timeout,
            Executor io,
            Executor ownerLoop) {
        return allocator.open().thenCompose(scope -> {
            var key = PulsarM6ObjectWalAllocator.bindingKey(binding);
            var incarnation = PulsarM6ObjectWalAllocator.incarnation(binding);
            var runs =
                    new PulsarM6ObjectWalRuns(oxia, root, incarnation, provider, limits.maximumEntryBytes(), timeout);
            var history = new PulsarM6ObjectWalHistory(
                    runs, provider, binding, key, scope.geometry(), timeout, RECOVERY_LOCATORS);
            var predecessor = new java.util.concurrent.atomic.AtomicReference<Optional<WalRunPredecessor>>();
            return allocator
                    .openBinding(
                            binding,
                            authority,
                            (owner, head) -> async(
                                    () -> {
                                        predecessor.set(history.fencePredecessor(owner, head));
                                        return null;
                                    },
                                    io))
                    .thenCompose(chain -> openRun(
                                    chain,
                                    runs,
                                    provider,
                                    binding,
                                    scope.geometry(),
                                    history,
                                    predecessor.get(),
                                    limits,
                                    timeout,
                                    io,
                                    ownerLoop)
                            .thenApply(opened -> new PulsarM6ObjectWalTopic(
                                    chain,
                                    runs,
                                    provider,
                                    binding,
                                    scope.geometry(),
                                    limits,
                                    timeout,
                                    io,
                                    ownerLoop,
                                    opened)))
                    .whenComplete((unused, failure) -> {
                        if (failure != null) {
                            closeAfterFailure(history, unwrap(failure));
                        }
                    });
        });
    }

    private static CompletionStage<Run> openRun(
            PulsarM6ObjectWalAllocator.BoundChain chain,
            PulsarM6ObjectWalRuns runs,
            PulsarM6ObjectWalProvider provider,
            PulsarNativeBindingV1 binding,
            FixedSlice geometry,
            PulsarM6ObjectWalHistory history,
            Optional<WalRunPredecessor> predecessor,
            Limits limits,
            Duration timeout,
            Executor io,
            Executor ownerLoop) {
        return async(
                        () -> {
                            var writerKms = provider.openKms();
                            try {
                                var fresh = history.openFresh(predecessor, chain.owner(), writerKms);
                                try {
                                    var projection = new PulsarM6ObjectWalProjection(
                                            fresh.selected().root(),
                                            binding,
                                            chain.binding(),
                                            geometry,
                                            chain.owner().witness());
                                    return new Resources(
                                            fresh,
                                            history,
                                            writerKms,
                                            makeActive(chain, runs, fresh, projection, timeout, io));
                                } catch (Exception | Error failure) {
                                    runs.abandonFresh(fresh, failure);
                                    throw failure;
                                }
                            } catch (Exception | Error failure) {
                                closeAfterFailure(writerKms, failure);
                                throw failure;
                            }
                        },
                        io)
                .thenCompose(resources -> {
                    var root = resources.fresh().selected().root();
                    var bridge = new PulsarObjectWalBridgeV1(
                            new PulsarObjectWalBridgeV1.Configuration(
                                    limits.maximumEntryBytes(),
                                    limits.entriesPerLedger(),
                                    1,
                                    RECOVERY_LOCATORS,
                                    RECOVERY_LOCATORS,
                                    1,
                                    root.nwg1AdmissionCaps().maxCanonicalBodyBytes()),
                            chain.controller(),
                            new PulsarM6ObjectWalExtentRoute(resources.fresh(), resources.active(), history, io));
                    var capability = new CapabilityBinding(
                            1, Sha256Digest.hash(provider.profile().encode()));
                    return bridge.activateRecoveredChain(
                                    chain.binding(),
                                    chain.owner().epoch(),
                                    (node, terminal) ->
                                            CompletableFuture.completedFuture(history.recover(node.virtualLedgerId())))
                            .thenCompose(unused -> chain.owner().requireCurrent())
                            .thenApply(unused -> {
                                history.requireNoForeignLedgers();
                                var reader = new PulsarObjectWalM4ReaderV1(
                                        bridge,
                                        chain.binding(),
                                        selector(bridge, chain.binding(), root.shardRunEpoch(), capability),
                                        new BindingReadHazardPoolV1(limits.readSlots(), 3),
                                        ownerLoop,
                                        limits.maximumReadEntries());
                                return new Run(resources, bridge, reader, capability);
                            })
                            .exceptionallyCompose(failure -> async(
                                    () -> {
                                        var original = unwrap(failure);
                                        runs.abandonFresh(resources.fresh(), original);
                                        closeAfterFailure(resources.writerKms(), original);
                                        throw new CompletionException(original);
                                    },
                                    io));
                });
    }

    private record Resources(
            PulsarM6ObjectWalRuns.Fresh fresh,
            PulsarM6ObjectWalHistory history,
            KmsCellSession writerKms,
            ProductionObjectWalExtentStore active) {}

    private static final class Run {
        private final Resources resources;
        private final PulsarObjectWalBridgeV1 bridge;
        private final PulsarObjectWalM4ReaderV1 reader;
        private final CapabilityBinding capability;
        private CompletableFuture<Void> drained;

        private Run(
                Resources resources,
                PulsarObjectWalBridgeV1 bridge,
                PulsarObjectWalM4ReaderV1 reader,
                CapabilityBinding capability) {
            this.resources = resources;
            this.bridge = bridge;
            this.reader = reader;
            this.capability = capability;
        }

        private boolean needsSuccessor() {
            var root = resources.fresh().selected().root();
            var runtime = resources.fresh().session().runtimeRecoveryState();
            return runtime.reservedExtentCount() >= root.bounds().maxExtentCount()
                    || root.nwg1AdmissionCaps().maxCanonicalBodyBytes()
                            > root.bounds().maxCanonicalBodyBytes() - runtime.reservedCanonicalBodyBytes()
                    || System.currentTimeMillis() - root.openedAtMillis()
                            >= root.bounds().maxRunAgeMillis();
        }

        private synchronized CompletableFuture<Void> drain(PulsarM6ObjectWalRuns runs, Executor io) {
            if (drained == null) {
                drained = reader.stopAndDrain()
                        .thenCompose(unused -> resources.active().stopAndDrainProviderTasks())
                        .thenCompose(unused -> async(
                                () -> {
                                    runs.closeAndRequireDrained(
                                            resources.fresh().selected());
                                    resources.active().close();
                                    resources.history().close();
                                    resources.writerKms().close();
                                    return null;
                                },
                                io));
            }
            return drained;
        }
    }

    private PulsarM6ObjectWalTopic(
            PulsarM6ObjectWalAllocator.BoundChain chain,
            PulsarM6ObjectWalRuns runs,
            PulsarM6ObjectWalProvider provider,
            PulsarNativeBindingV1 binding,
            FixedSlice geometry,
            Limits limits,
            Duration timeout,
            Executor io,
            Executor ownerLoop,
            Run run) {
        this.chain = chain;
        this.runs = runs;
        this.provider = provider;
        this.binding = binding;
        this.geometry = geometry;
        this.limits = limits;
        this.timeout = timeout;
        this.io = io;
        this.ownerLoop = ownerLoop;
        this.run = run;
        decodedSizes = new ConcurrentHashMap<>(run.resources.history().exactSizes());
    }

    private synchronized CompletionStage<Run> writableRun() {
        var current = run;
        if (!current.needsSuccessor()) {
            return CompletableFuture.completedFuture(current);
        }
        if (runRollover != null) {
            return runRollover;
        }
        var barrier = new CompletableFuture<Run>();
        runRollover = barrier;
        current.drain(runs, io)
                .thenCompose(unused -> async(
                        () -> runs.sealedPredecessor(current.resources.fresh().selected()), io))
                .thenCompose(predecessor -> {
                    var history = new PulsarM6ObjectWalHistory(
                            runs, provider, binding, chain.binding(), geometry, timeout, RECOVERY_LOCATORS);
                    return openRun(
                                    chain,
                                    runs,
                                    provider,
                                    binding,
                                    geometry,
                                    history,
                                    Optional.of(predecessor),
                                    limits,
                                    timeout,
                                    io,
                                    ownerLoop)
                            .whenComplete((unused, failure) -> {
                                if (failure != null) {
                                    closeAfterFailure(history, unwrap(failure));
                                }
                            });
                })
                .thenCompose(next -> {
                    if (next.resources.history().exactSizes().equals(decodedSizes)) {
                        return CompletableFuture.completedFuture(next);
                    }
                    var error =
                            new IllegalStateException("Pulsar Object WAL successor lost its exact retained positions");
                    return next.drain(runs, io).handle((unused, failure) -> {
                        if (failure != null) {
                            error.addSuppressed(unwrap(failure));
                        }
                        throw error;
                    });
                })
                .whenComplete((next, failure) -> {
                    if (failure != null) {
                        barrier.completeExceptionally(failure);
                        return;
                    }
                    synchronized (this) {
                        run = next;
                        runRollover = null;
                    }
                    barrier.complete(next);
                });
        return barrier;
    }

    private static ProductionObjectWalExtentStore makeActive(
            PulsarM6ObjectWalAllocator.BoundChain chain,
            PulsarM6ObjectWalRuns runs,
            PulsarM6ObjectWalRuns.Fresh fresh,
            PulsarM6ObjectWalProjection projection,
            Duration timeout,
            Executor io) {
        var root = fresh.selected().root();
        return new ProductionObjectWalExtentStore(
                fresh.session(),
                io,
                projection::seal,
                (member, locator) -> {
                    try {
                        chain.owner()
                                .requireSelectedEpoch()
                                .toCompletableFuture()
                                .get(timeout.toNanos(), TimeUnit.NANOSECONDS);
                        fresh.dispatch().publish(locator);
                        return Optional.empty();
                    } catch (Exception failure) {
                        if (failure instanceof InterruptedException) {
                            Thread.currentThread().interrupt();
                        }
                        throw new CompletionException(failure);
                    }
                },
                projection::requireBinding,
                new ObjectWalManifestOperations() {
                    @Override
                    public CompletionStage<VerifiedManifestHandoff> verify(ManifestHandoffRequest request) {
                        return CompletableFuture.failedFuture(
                                new IllegalStateException("No Pulsar manifest is selected"));
                    }

                    @Override
                    public CompletionStage<ReadEntry> read(
                            PulsarBindingKey binding, PulsarPosition position, long generation, ManifestSource source) {
                        return CompletableFuture.failedFuture(
                                new IllegalStateException("No Pulsar manifest is selected"));
                    }
                },
                new ObjectWalSuccessorAuthority() {
                    @Override
                    public CompletionStage<PublishResult> resumeInSuccessor(
                            SealedExtentPlan plan, Optional<ExtentIdentity> absent, List<PlannedEntry> unresolved) {
                        return unavailable();
                    }

                    @Override
                    public CompletionStage<PublishResult> publishSuccessor(
                            SealedExtentPlan previous, SealedExtentPlan successor) {
                        return unavailable();
                    }

                    private CompletionStage<PublishResult> unavailable() {
                        return CompletableFuture.failedFuture(new IllegalStateException(
                                "Pulsar WAL Root cannot change while the exact entry is unresolved"));
                    }
                },
                new ProductionObjectWalContext(projection.verification(), System::currentTimeMillis),
                new ProductionWalRunClosure(
                        fresh.publisher(),
                        new WalRunLifecycleManager(runs.metadata()),
                        fresh.selected().reference(),
                        WalRunControlKeys.checkpointHeadKey(root.shardId(), root.shardRunEpoch()),
                        WalRunControlKeys.sealKey(root.shardId(), root.shardRunEpoch())));
    }

    synchronized CompletionStage<PulsarPosition> append(byte[] payload, Object authorityWord) {
        if (closing != null
                || payload.length > limits.maximumEntryBytes()
                || pendingAppends >= limits.maximumPendingAppends()
                || payload.length > limits.pendingBytes() - pendingBytes
                || appendFailure != null) {
            return CompletableFuture.failedFuture(
                    new IllegalStateException("Pulsar Object WAL append admission closed"));
        }
        pendingAppends++;
        pendingBytes += payload.length;
        var original = appendTail
                .thenComposeAsync(
                        unused -> {
                            if (appendFailure != null) {
                                return CompletableFuture.<PulsarPosition>failedFuture(appendFailure);
                            }
                            var owner = chain.owner();
                            if (owner.capture() != authorityWord) {
                                return CompletableFuture.<PulsarPosition>failedFuture(new IllegalStateException(
                                        "Pulsar Object WAL publish started under stale ownership"));
                            }
                            return writableRun().thenCompose(next -> appendInRun(next, payload, authorityWord));
                        },
                        ownerLoop)
                .toCompletableFuture();
        appendTail = original.handle((position, failure) -> {
            synchronized (this) {
                pendingAppends--;
                pendingBytes -= payload.length;
                if (failure != null) {
                    appendFailure = unwrap(failure);
                }
            }
            return null;
        });
        return original.copy();
    }

    private CompletionStage<PulsarPosition> appendInRun(Run selected, byte[] payload, Object authorityWord) {
        var owner = chain.owner();
        if (owner.capture() != authorityWord) {
            return CompletableFuture.failedFuture(
                    new IllegalStateException("Pulsar Object WAL publish crossed native ownership"));
        }
        var input = new AppendInput(
                chain.binding(), owner.epoch(), LANE, PACKING_POLICY, PulsarM6ObjectWalProjection.newId(), payload);
        var bridge = selected.bridge;
        CompletionStage<?> space =
                bridge.frontiers(chain.binding()).bindingDurableThrough() + 1 >= limits.entriesPerLedger()
                        ? bridge.rollover(chain.binding(), owner.epoch())
                        : CompletableFuture.completedFuture(null);
        return space.thenCompose(available -> bridge.appendShared(List.of(input)))
                .exceptionallyCompose(failure -> {
                    var exact = unwrap(failure);
                    if (exact instanceof BridgeException bridgeFailure
                            && bridgeFailure.code() == BridgeRejectionCode.PROVIDER_OUTCOME_UNKNOWN) {
                        return bridge.resumePendingEntry(chain.binding(), owner.epoch());
                    }
                    return CompletableFuture.failedFuture(exact);
                })
                .thenApply(accepted -> {
                    MemberAppendResult result = accepted.get(0);
                    if (!(result instanceof VerifiedAppend exact)) {
                        throw new IllegalStateException("Pulsar Object WAL append was not durably published");
                    }
                    if (decodedSizes.putIfAbsent(exact.position(), payload.length) != null) {
                        throw new IllegalStateException("Native Object WAL published a repeated position");
                    }
                    selected.reader.refresh(selector(
                            bridge,
                            chain.binding(),
                            selected.resources.fresh().selected().root().shardRunEpoch(),
                            selected.capability));
                    return exact.position();
                });
    }

    synchronized CompletionStage<List<ReadEntry>> read(List<PulsarPosition> positions) {
        if (closing != null) {
            return CompletableFuture.failedFuture(new IllegalStateException("Pulsar Object WAL read admission closed"));
        }
        if (runRollover != null) {
            return runRollover.thenCompose(unused -> read(positions));
        }
        final var before = chain.owner().capture();
        return run.reader.readBatch(positions).thenApply(values -> {
            if (!chain.owner().isCurrent(before)) {
                throw new IllegalStateException("Pulsar Object WAL read completed under stale ownership");
            }
            return values;
        });
    }

    int maximumEntryBytes() {
        return limits.maximumEntryBytes();
    }

    PulsarM6ObjectWalOwner owner() {
        return chain.owner();
    }

    PulsarM6ObjectWalAllocator.BoundChain chain() {
        return chain;
    }

    PulsarObjectWalReadViewV1 view() {
        return run.bridge.captureReadView(chain.binding());
    }

    PulsarObjectWalReadViewV1 publishedView() {
        var state = run.reader.currentAuthority().get().publicationCell().protocolStateReference();
        return ((PulsarObjectBindingReadAdapterV1.ReadCell) state).view();
    }

    int size(PulsarPosition position) {
        var size = decodedSizes.get(position);
        if (size == null) {
            throw new IllegalArgumentException("Object WAL position has no exact decoded size");
        }
        return size;
    }

    int pendingAppends() {
        synchronized (this) {
            return pendingAppends;
        }
    }

    synchronized CompletableFuture<Void> close() {
        if (closing == null) {
            closing = appendTail
                    .thenCompose(unused -> {
                        var rollover = runRollover;
                        return rollover == null
                                ? run.drain(runs, io)
                                : rollover.thenCompose(opened -> opened.drain(runs, io));
                    })
                    .exceptionallyCompose(failure -> {
                        var cause = unwrap(failure);
                        if (appendFailure == null || appendFailure == cause) {
                            return CompletableFuture.failedFuture(cause);
                        }
                        var unsettled =
                                new IllegalStateException("Pulsar Object WAL drain left an unsettled append", cause);
                        unsettled.addSuppressed(appendFailure);
                        return CompletableFuture.failedFuture(unsettled);
                    });
        }
        return closing.copy();
    }

    private static com.nereusstream.storage.object.read.control.M4ReadControlRecordsV1.BindingReadSelector selector(
            PulsarObjectWalBridgeV1 bridge, PulsarBindingKey binding, long epoch, CapabilityBinding capability) {
        return PulsarObjectBindingReadAdapterV1.preferredSelector(bridge.captureReadView(binding), epoch, capability);
    }

    private static void closeAfterFailure(AutoCloseable resource, Throwable failure) {
        try {
            resource.close();
        } catch (Exception cleanup) {
            failure.addSuppressed(cleanup);
        }
    }

    private static Throwable unwrap(Throwable failure) {
        while (failure instanceof CompletionException wrapped && wrapped.getCause() != null) {
            failure = wrapped.getCause();
        }
        return failure;
    }

    @FunctionalInterface
    private interface Operation<T> {
        T run() throws Exception;
    }

    private static <T> CompletableFuture<T> async(Operation<T> task, Executor io) {
        Supplier<T> action = () -> {
            try {
                return task.run();
            } catch (Exception failure) {
                if (failure instanceof InterruptedException) {
                    Thread.currentThread().interrupt();
                }
                throw new CompletionException(failure);
            }
        };
        return CompletableFuture.supplyAsync(action, io);
    }
}
