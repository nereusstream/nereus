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
import com.nereusstream.pulsar.offload.objectwal.PulsarVirtualLedgerChainControllerV1.PulsarBindingKey;
import com.nereusstream.storage.object.control.WalLaneId;
import com.nereusstream.storage.object.control.WalRunControlKeys;
import com.nereusstream.storage.object.control.WalRunLifecycleManager;
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
    private final PulsarM6ObjectWalRuns.Fresh fresh;
    private final PulsarM6ObjectWalHistory history;
    private final KmsCellSession writerKms;
    private final ProductionObjectWalExtentStore active;
    private final PulsarObjectWalBridgeV1 bridge;
    private final PulsarObjectWalM4ReaderV1 reader;
    private final Executor io;
    private final Executor ownerLoop;
    private final int maximumEntryBytes;
    private final long entriesPerLedger;
    private final int maximumPendingAppends;
    private final long maximumPendingBytes;
    private final CapabilityBinding capability;
    private final Map<PulsarPosition, Integer> decodedSizes;
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
            var predecessor = new java.util.concurrent.atomic.AtomicReference<
                    Optional<com.nereusstream.storage.object.control.WalRunPredecessor>>();
            var opened = allocator
                    .openBinding(
                            binding,
                            authority,
                            (owner, head) -> async(
                                    () -> {
                                        predecessor.set(history.fencePredecessor(owner, head));
                                        return null;
                                    },
                                    io))
                    .thenCompose(chain -> async(
                            () -> {
                                var writerKms = provider.openKms();
                                try {
                                    var fresh = history.openFresh(predecessor.get(), chain.owner(), writerKms);
                                    try {
                                        var projection = new PulsarM6ObjectWalProjection(
                                                fresh.selected().root(),
                                                binding,
                                                key,
                                                scope.geometry(),
                                                chain.owner().witness());
                                        return new Pending(
                                                chain,
                                                runs,
                                                fresh,
                                                history,
                                                writerKms,
                                                makeActive(chain, runs, fresh, projection, timeout, io),
                                                new CapabilityBinding(
                                                        1,
                                                        Sha256Digest.hash(provider.profile()
                                                                .encode())));
                                    } catch (Exception | Error failure) {
                                        runs.abandonFresh(fresh, failure);
                                        throw failure;
                                    }
                                } catch (Exception | Error failure) {
                                    closeAfterFailure(writerKms, failure);
                                    throw failure;
                                }
                            },
                            io))
                    .thenCompose(pending -> {
                        var bridge = new PulsarObjectWalBridgeV1(
                                new PulsarObjectWalBridgeV1.Configuration(
                                        limits.maximumEntryBytes(),
                                        limits.entriesPerLedger(),
                                        1,
                                        RECOVERY_LOCATORS,
                                        RECOVERY_LOCATORS,
                                        1,
                                        pending.fresh()
                                                .selected()
                                                .root()
                                                .nwg1AdmissionCaps()
                                                .maxCanonicalBodyBytes()),
                                pending.chain().controller(),
                                new PulsarM6ObjectWalExtentRoute(pending.fresh(), pending.active(), history, io));
                        return bridge.activateRecoveredChain(
                                        key,
                                        pending.chain().owner().epoch(),
                                        (node, terminal) -> CompletableFuture.completedFuture(
                                                history.recover(node.virtualLedgerId())))
                                .thenCompose(unused -> pending.chain().owner().requireCurrent())
                                .thenApply(unused -> {
                                    history.requireNoForeignLedgers();
                                    return new PulsarM6ObjectWalTopic(pending, bridge, limits, io, ownerLoop);
                                })
                                .exceptionallyCompose(failure -> async(
                                        () -> {
                                            var original = unwrap(failure);
                                            runs.abandonFresh(pending.fresh(), original);
                                            closeAfterFailure(pending.writerKms(), original);
                                            throw new CompletionException(original);
                                        },
                                        io));
                    });
            return opened.whenComplete((unused, failure) -> {
                if (failure != null) {
                    closeAfterFailure(history, unwrap(failure));
                }
            });
        });
    }

    private record Pending(
            PulsarM6ObjectWalAllocator.BoundChain chain,
            PulsarM6ObjectWalRuns runs,
            PulsarM6ObjectWalRuns.Fresh fresh,
            PulsarM6ObjectWalHistory history,
            KmsCellSession writerKms,
            ProductionObjectWalExtentStore active,
            CapabilityBinding capability) {}

    private PulsarM6ObjectWalTopic(
            Pending pending, PulsarObjectWalBridgeV1 bridge, Limits limits, Executor io, Executor ownerLoop) {
        chain = pending.chain();
        runs = pending.runs();
        fresh = pending.fresh();
        history = pending.history();
        writerKms = pending.writerKms();
        active = pending.active();
        capability = pending.capability();
        decodedSizes = new ConcurrentHashMap<>(history.exactSizes());
        this.bridge = bridge;
        this.io = io;
        this.ownerLoop = ownerLoop;
        maximumEntryBytes = limits.maximumEntryBytes();
        entriesPerLedger = limits.entriesPerLedger();
        maximumPendingAppends = limits.maximumPendingAppends();
        maximumPendingBytes = limits.pendingBytes();
        reader = new PulsarObjectWalM4ReaderV1(
                bridge,
                chain.binding(),
                selector(),
                new BindingReadHazardPoolV1(limits.readSlots(), 3),
                ownerLoop,
                limits.maximumReadEntries());
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
                || payload.length > maximumEntryBytes
                || pendingAppends >= maximumPendingAppends
                || payload.length > maximumPendingBytes - pendingBytes
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
                            var input = new AppendInput(
                                    chain.binding(),
                                    owner.epoch(),
                                    LANE,
                                    PACKING_POLICY,
                                    PulsarM6ObjectWalProjection.newId(),
                                    payload);
                            CompletionStage<?> space =
                                    bridge.frontiers(chain.binding()).bindingDurableThrough() + 1 >= entriesPerLedger
                                            ? bridge.rollover(chain.binding(), owner.epoch())
                                            : CompletableFuture.completedFuture(null);
                            return space.thenCompose(available -> bridge.appendShared(List.of(input)))
                                    .exceptionallyCompose(failure -> {
                                        var exact = unwrap(failure);
                                        if (exact instanceof BridgeException bridgeFailure
                                                && bridgeFailure.code()
                                                        == BridgeRejectionCode.PROVIDER_OUTCOME_UNKNOWN) {
                                            return bridge.resumePendingEntry(chain.binding(), owner.epoch());
                                        }
                                        return CompletableFuture.failedFuture(exact);
                                    })
                                    .thenApply(accepted -> {
                                        MemberAppendResult result = accepted.get(0);
                                        if (!(result instanceof VerifiedAppend exact)) {
                                            throw new IllegalStateException(
                                                    "Pulsar Object WAL append was not durably published");
                                        }
                                        if (decodedSizes.putIfAbsent(exact.position(), payload.length) != null) {
                                            throw new IllegalStateException(
                                                    "Native Object WAL published a repeated position");
                                        }
                                        reader.refresh(selector());
                                        return exact.position();
                                    });
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

    CompletionStage<List<ReadEntry>> read(List<PulsarPosition> positions) {
        final var before = chain.owner().capture();
        return reader.readBatch(positions).thenApply(values -> {
            if (!chain.owner().isCurrent(before)) {
                throw new IllegalStateException("Pulsar Object WAL read completed under stale ownership");
            }
            return values;
        });
    }

    int maximumEntryBytes() {
        return maximumEntryBytes;
    }

    PulsarM6ObjectWalOwner owner() {
        return chain.owner();
    }

    PulsarM6ObjectWalAllocator.BoundChain chain() {
        return chain;
    }

    PulsarObjectWalReadViewV1 view() {
        return bridge.captureReadView(chain.binding());
    }

    PulsarObjectWalReadViewV1 publishedView() {
        var state = reader.currentAuthority().get().publicationCell().protocolStateReference();
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
            var reads = reader.stopAndDrain();
            closing = CompletableFuture.allOf(appendTail, reads)
                    .thenCompose(unused -> active.stopAndDrainProviderTasks())
                    .thenCompose(unused -> async(
                            () -> {
                                runs.closeAndRequireDrained(fresh.selected());
                                active.close();
                                history.close();
                                writerKms.close();
                                return null;
                            },
                            io));
        }
        return closing.copy();
    }

    private com.nereusstream.storage.object.read.control.M4ReadControlRecordsV1.BindingReadSelector selector() {
        return PulsarObjectBindingReadAdapterV1.preferredSelector(
                view(), fresh.selected().root().shardRunEpoch(), capability);
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
