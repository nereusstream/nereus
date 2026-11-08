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
import com.nereusstream.domain.protocol.PulsarProtocolCellIdentity;
import com.nereusstream.metadata.spi.allocator.VersionedManagedLedgerAllocatorHeadV1;
import com.nereusstream.pulsar.offload.PulsarNativeBindingV1;
import com.nereusstream.pulsar.offload.objectwal.PulsarObjectWalBridgeV1.ExtentIdentity;
import com.nereusstream.pulsar.offload.objectwal.PulsarObjectWalBridgeV1.ExtentLocator;
import com.nereusstream.pulsar.offload.objectwal.PulsarObjectWalBridgeV1.ObservedPulsarOwnerFence;
import com.nereusstream.pulsar.offload.objectwal.PulsarObjectWalBridgeV1.PulsarPosition;
import com.nereusstream.pulsar.offload.objectwal.PulsarObjectWalBridgeV1.PulsarProtocolOwnerFenceExecutor;
import com.nereusstream.pulsar.offload.objectwal.PulsarObjectWalBridgeV1.ReadEntry;
import com.nereusstream.pulsar.offload.objectwal.PulsarObjectWalBridgeV1.ReadFailureScope;
import com.nereusstream.pulsar.offload.objectwal.PulsarObjectWalBridgeV1.ReadSource;
import com.nereusstream.pulsar.offload.objectwal.PulsarObjectWalBridgeV1.RecoverySeed;
import com.nereusstream.pulsar.offload.objectwal.PulsarVirtualLedgerChainControllerV1.FixedSlice;
import com.nereusstream.pulsar.offload.objectwal.PulsarVirtualLedgerChainControllerV1.PulsarBindingKey;
import com.nereusstream.storage.object.control.ControlMutationOutcome;
import com.nereusstream.storage.object.control.ObjectWalLeafKeyV1;
import com.nereusstream.storage.object.control.ProviderResolvedExtentRowV1;
import com.nereusstream.storage.object.control.WalCheckpointHeadV1;
import com.nereusstream.storage.object.control.WalLaneId;
import com.nereusstream.storage.object.control.WalRunControlCodec;
import com.nereusstream.storage.object.control.WalRunControlKeys;
import com.nereusstream.storage.object.control.WalRunHistoricalObjectReaderV1;
import com.nereusstream.storage.object.control.WalRunLifecycleManager;
import com.nereusstream.storage.object.control.WalRunObjectSession;
import com.nereusstream.storage.object.control.WalRunPredecessor;
import com.nereusstream.storage.object.control.WalRunReference;
import com.nereusstream.storage.object.kms.KmsCellSession;
import com.nereusstream.storage.object.nwg1.Nwg1ObjectReaderV1;
import com.nereusstream.storage.object.provider.ObjectIdentity;
import com.nereusstream.storage.object.recovery.BoundedObjectTailRecovery;
import com.nereusstream.storage.object.recovery.OwnerOpenRecoveryCoordinator;
import com.nereusstream.storage.object.recovery.WalRunLineageRecovery;
import java.io.IOException;
import java.time.Duration;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.Optional;
import java.util.TreeMap;
import java.util.concurrent.CompletionStage;
import java.util.concurrent.TimeUnit;

/** Reconstructs exact native positions from retained authenticated physical WAL runs. */
final class PulsarM6ObjectWalHistory implements AutoCloseable {
    private final PulsarM6ObjectWalRuns runs;
    private final PulsarM6ObjectWalProvider provider;
    private final PulsarNativeBindingV1 nativeBinding;
    private final PulsarBindingKey binding;
    private final FixedSlice geometry;
    private final Duration timeout;
    private final int locatorLimit;
    private final WalRunLifecycleManager lifecycle;
    private final Map<Sha256Digest, HistoricalRun> sources = new HashMap<>();
    private final Map<Long, TreeMap<Long, ExtentLocator>> positions = new HashMap<>();
    private final Map<Long, RecoverySeed> selectedRecovery = new HashMap<>();
    private final Map<PulsarPosition, Integer> knownSizes = new HashMap<>();
    private int locatorCount;

    PulsarM6ObjectWalHistory(
            PulsarM6ObjectWalRuns runs,
            PulsarM6ObjectWalProvider provider,
            PulsarNativeBindingV1 nativeBinding,
            PulsarBindingKey binding,
            FixedSlice geometry,
            Duration timeout,
            int locatorLimit) {
        this.runs = runs;
        this.provider = provider;
        this.nativeBinding = nativeBinding;
        this.binding = binding;
        this.geometry = geometry;
        this.timeout = timeout;
        this.locatorLimit = locatorLimit;
        lifecycle = new WalRunLifecycleManager(runs.metadata());
    }

    Optional<WalRunPredecessor> fencePredecessor(
            PulsarM6ObjectWalOwner owner, VersionedManagedLedgerAllocatorHeadV1 head) throws Exception {
        var current = runs.current();
        if (current.isEmpty()) {
            current = runs.recoverUnselected(Optional.empty(), lifecycle);
        }
        if (current.isEmpty()) {
            if (!head.value().visibleChainHead().isAbsent()) {
                throw new IllegalStateException("Published virtual ledger chain has no physical WAL Root");
            }
            return Optional.empty();
        }
        var prior = closeCurrent(owner, current.orElseThrow());
        var retained = runs.recoverUnselected(Optional.of(prior), lifecycle);
        if (retained.isPresent()) {
            prior = closeCurrent(owner, retained.orElseThrow());
        }
        return Optional.of(prior);
    }

    private WalRunPredecessor closeCurrent(PulsarM6ObjectWalOwner owner, PulsarM6ObjectWalRuns.Selected selected)
            throws Exception {
        await(owner.requireCurrent());
        await(provider.requireCurrent());
        var closed = runs.closeAndRequireDrained(selected);
        var root = selected.root();
        var projection = projection(selected);
        String physicalHead = WalRunControlKeys.checkpointHeadKey(root.shardId(), root.shardRunEpoch());
        if (runs.metadata().get(physicalHead).isEmpty()) {
            if (closed.admitted()) {
                throw new IllegalStateException("Previously admitted Pulsar WAL has no physical checkpoint Head");
            }
            var original = WalRunControlCodec.encodeCheckpointHead(
                    WalCheckpointHeadV1.empty(selected.reference().rootSha256(), root.shardRunEpoch(), owner.epoch()));
            if (runs.metadata().putIfAbsent(physicalHead, original) != ControlMutationOutcome.APPLIED
                    && !runs.metadata().get(physicalHead).equals(Optional.of(original))) {
                throw new IllegalStateException("Original empty Pulsar physical Head did not converge");
            }
        }
        var lineage = new WalRunLineageRecovery(runs.metadata())
                .recover(
                        WalRunControlKeys.pointerKey(root.shardId()),
                        root.protocolCellIdentity(),
                        root.providerScopeId(),
                        Optional.empty(),
                        System::nanoTime);
        var fence = new PulsarProtocolOwnerFenceExecutor(
                (cell, reference, action) -> {
                    try {
                        await(owner.requireCurrent());
                        if (!cell.equals(root.protocolCellIdentity()) || !reference.equals(selected.reference())) {
                            throw new IllegalStateException("Native Pulsar recovery selected a different Root or Cell");
                        }
                        var recovered = action.recover(new ObservedPulsarOwnerFence(
                                (PulsarProtocolCellIdentity) root.protocolCellIdentity(),
                                reference,
                                owner.epoch(),
                                owner.commitment()));
                        try {
                            await(owner.requireCurrent());
                            return recovered;
                        } catch (Exception failure) {
                            recovered.close();
                            throw failure;
                        }
                    } catch (Exception failure) {
                        throw new IOException("Pulsar Object WAL durable owner fence rejected recovery", failure);
                    }
                },
                projection.verification());
        var staged = new CurrentCut(selected, projection, closed);
        var io = runs.readProvider(selected);
        try (var kms = provider.openKms()) {
            try (var session = OwnerOpenRecoveryCoordinator.recoverUnderDurableFence(
                    fence,
                    root.protocolCellIdentity(),
                    selected.reference(),
                    root,
                    runs.metadata(),
                    io,
                    kms,
                    lineage,
                    staged)) {
                await(owner.requireCurrent());
                var checkpointed = session.checkpointRecoveredPhysicalRows(runs.metadata(), owner.epoch());
                await(owner.requireCurrent());
                var seal = session.sealRecovered(lifecycle, checkpointed);
                return new WalRunPredecessor(seal.root(), seal.sealKey(), seal.sealSha256(), Optional.empty());
            }
        } finally {
            if (io.state() != com.nereusstream.storage.object.provider.C1ObjectProviderSession.State.CLOSED) {
                io.close();
            }
        }
    }

    PulsarM6ObjectWalRuns.Fresh openFresh(
            Optional<WalRunPredecessor> prior, PulsarM6ObjectWalOwner owner, KmsCellSession writerKms)
            throws Exception {
        var fresh = runs.create(prior, owner, writerKms, lifecycle);
        try {
            loadSources(fresh.selected(), owner);
            return fresh;
        } catch (Exception | Error failure) {
            runs.abandonFresh(fresh, failure);
            throw failure;
        }
    }

    private void loadSources(PulsarM6ObjectWalRuns.Selected fresh, PulsarM6ObjectWalOwner owner) throws Exception {
        var stagedRows = new HashMap<WalRunReference, List<ProviderResolvedExtentRowV1>>();
        int[] staged = {0};
        var root = fresh.root();
        var lineage = new WalRunLineageRecovery(runs.metadata())
                .recover(
                        WalRunControlKeys.pointerKey(root.shardId()),
                        root.protocolCellIdentity(),
                        root.providerScopeId(),
                        Optional.empty(),
                        System::nanoTime,
                        (ref, row) -> {
                            if (++staged[0] > locatorLimit) {
                                throw new IllegalStateException("Pulsar Object WAL retained locator bound exceeded");
                            }
                            stagedRows
                                    .computeIfAbsent(ref, key -> new ArrayList<>())
                                    .add(row);
                        });
        if (!lineage.runs().get(0).reference().equals(fresh.reference())) {
            throw new IllegalStateException("Pulsar Object WAL current Root changed during lineage recovery");
        }
        for (var run : lineage.runs().subList(1, lineage.runs().size())) {
            await(owner.requireCurrent());
            var selected = new PulsarM6ObjectWalRuns.Selected(run.reference(), run.root());
            var source = openHistorical(lineage, selected);
            if (sources.putIfAbsent(run.reference().rootSha256(), source) != null) {
                source.close();
                throw new IllegalStateException("Pulsar Object WAL lineage repeated a Root");
            }
            for (var row : stagedRows.getOrDefault(run.reference(), List.of())) {
                lineage.chargeRetainedPulsarFrame(run.reference(), row.directoryPrefixEnd(), row.bodyLength());
                var identity = extentIdentity(selected, row);
                var exactFrame = source.readVerified(identity, null);
                var locator = exactFrame.locator();
                if (knownSizes.putIfAbsent(locator.position(), exactFrame.decodedBytes()) != null) {
                    throw new IllegalStateException("Pulsar Object WAL repeated a native position");
                }
                var entries = positions.computeIfAbsent(locator.position().virtualLedgerId(), id -> new TreeMap<>());
                if (entries.putIfAbsent(locator.position().entryId(), locator) != null
                        || ++locatorCount > locatorLimit) {
                    throw new IllegalStateException(
                            "Pulsar Object WAL retained locator repeated or exceeded its bound");
                }
            }
        }
        await(owner.requireCurrent());
    }

    private HistoricalRun openHistorical(
            WalRunLineageRecovery.RecoveredLineage lineage, PulsarM6ObjectWalRuns.Selected selected) throws Exception {
        var exact = runs.read(selected.reference());
        var projection = projection(exact);
        var providerSession = runs.readProvider(exact);
        var kms = provider.openKms();
        try {
            var read = new WalRunHistoricalObjectReaderV1(lineage, exact.reference(), providerSession, kms);
            return new HistoricalRun(read, kms, projection);
        } catch (RuntimeException | Error failure) {
            providerSession.close();
            kms.close();
            throw failure;
        }
    }

    RecoverySeed recover(long ledgerId) {
        var entries = positions.remove(ledgerId);
        var seed = entries == null
                ? RecoverySeed.empty(ledgerId)
                : new RecoverySeed(
                        ledgerId, -1, entries.lastKey(), entries.lastKey(), 0, List.copyOf(entries.values()));
        if (selectedRecovery.putIfAbsent(ledgerId, seed) != null) {
            throw new IllegalStateException("Pulsar virtual ledger was recovered twice");
        }
        return seed;
    }

    void requireRecovered(
            com.nereusstream.pulsar.offload.objectwal.PulsarObjectWalBridgeV1.ActiveTailRecoveryRequest request) {
        var seed = selectedRecovery.remove(request.virtualLedgerId());
        if (seed == null
                || !request.binding().equals(binding)
                || request.manifestThrough() != -1
                || request.durableThrough() != seed.durableThrough()
                || !request.activeTail().equals(seed.activeTail())) {
            throw new IllegalStateException("Pulsar Object WAL recovery request differs from verified physical rows");
        }
    }

    void requireNoForeignLedgers() {
        if (!positions.isEmpty() || !selectedRecovery.isEmpty()) {
            throw new IllegalStateException("Pulsar Object WAL retained frame has no typed virtual ledger");
        }
    }

    Map<PulsarPosition, Integer> exactSizes() {
        return Map.copyOf(knownSizes);
    }

    ReadEntry read(ExtentLocator locator) throws IOException {
        var source = sources.get(locator.identity().walRunRootSha256());
        if (source == null || !locator.binding().equals(binding)) {
            throw new IllegalStateException("Pulsar read selected an unretained Object WAL Root");
        }
        var exact = source.readVerified(locator.identity(), locator);
        return new ReadEntry(
                binding, locator.position(), exact.payload(), ReadSource.ACTIVE_TAIL, ReadFailureScope.NONE);
    }

    private PulsarM6ObjectWalProjection projection(PulsarM6ObjectWalRuns.Selected selected) throws Exception {
        return new PulsarM6ObjectWalProjection(
                selected.root(), nativeBinding, binding, geometry, runs.creator(selected));
    }

    private static ExtentIdentity extentIdentity(PulsarM6ObjectWalRuns.Selected run, ProviderResolvedExtentRowV1 row) {
        if (row.laneId() != WalLaneId.OBJECT_LATENCY) {
            throw new IllegalStateException("Native Pulsar WAL Root has another lane");
        }
        var leaf = ObjectWalLeafKeyV1.fromRow(row);
        return new ExtentIdentity(
                run.reference().rootSha256(),
                row.laneId(),
                row.laneSequence(),
                new ObjectIdentity(
                        leaf.fullKey(run.root().providerConfiguration()), row.bodyLength(), row.objectSha256()));
    }

    private ExtentLocator locator(ExtentIdentity identity, Nwg1ObjectReaderV1.VerifiedFrame frame) {
        var stored = frame.storedPulsarFrame()
                .orElseThrow(() ->
                        new IllegalStateException("Verified native Pulsar range lacks its original stored bytes"));
        return new ExtentLocator(
                binding,
                new PulsarPosition(frame.coverage0(), frame.coverage1()),
                identity,
                Math.toIntExact(frame.absoluteFrameOrdinal()),
                stored.offset(),
                stored.length(),
                stored.digest().toHex());
    }

    private <T> T await(CompletionStage<T> stage) throws Exception {
        return stage.toCompletableFuture().get(timeout.toNanos(), TimeUnit.NANOSECONDS);
    }

    @Override
    public void close() {
        RuntimeException failure = null;
        for (var source : sources.values()) {
            try {
                source.close();
            } catch (RuntimeException cleanup) {
                if (failure == null) {
                    failure = cleanup;
                } else {
                    failure.addSuppressed(cleanup);
                }
            }
        }
        if (failure != null) {
            throw failure;
        }
        sources.clear();
    }

    private final class CurrentCut implements OwnerOpenRecoveryCoordinator.ProtocolRecoveryHandler {
        private final PulsarM6ObjectWalRuns.Selected current;
        private final PulsarM6ObjectWalProjection projection;
        private final PulsarM6ObjectWalDispatchFence.Closure closure;
        private long lastPhysical = -1;
        private Sha256Digest lastAuthorized;

        private CurrentCut(
                PulsarM6ObjectWalRuns.Selected current,
                PulsarM6ObjectWalProjection projection,
                PulsarM6ObjectWalDispatchFence.Closure closure) {
            this.current = current;
            this.projection = projection;
            this.closure = closure;
        }

        @Override
        public void stage(
                ProviderResolvedExtentRowV1 row,
                Nwg1ObjectReaderV1.AuthenticatedPrefix prefix,
                BoundedObjectTailRecovery.SelectedAppendUnitReader selected)
                throws IOException {
            var identity = extentIdentity(current, row);
            if (prefix.directory().bindings().size() != 1
                    || prefix.directory().frames().size() != 1
                    || prefix.directory().appendUnits().size() != 1) {
                throw new IllegalStateException("Pulsar Object WAL recovered extent has the wrong projection");
            }
            projection.requireBinding(binding, prefix.directory().bindings().get(0));
            var verified = selected.read(0, (frame, payload) -> {
                if (row.laneSequence() == closure.publishedThrough()) {
                    lastAuthorized = PulsarM6ObjectWalDispatchFence.locatorDigest(locator(identity, frame));
                }
            });
            if (verified.protocolKind() != 2) {
                throw new IllegalStateException("Pulsar Object WAL recovered an entry from another protocol");
            }
            lastPhysical = Math.max(lastPhysical, row.laneSequence());
        }

        @Override
        public void install(WalRunObjectSession session) {
            if (!closure.admitted() && lastPhysical >= 0
                    || lastPhysical < closure.publishedThrough()
                    || lastPhysical > Math.incrementExact(closure.publishedThrough())
                    || closure.publishedThrough() >= 0
                            && !closure.lastPublished().equals(lastAuthorized)) {
                throw new IllegalStateException("Pulsar Object WAL physical recovery differs from durable publication");
            }
        }

        @Override
        public void abort() {
            lastAuthorized = null;
        }
    }

    private record Verified(ExtentLocator locator, int decodedBytes, byte[] payload) {}

    private final class HistoricalRun implements AutoCloseable {
        private final WalRunHistoricalObjectReaderV1 reader;
        private final KmsCellSession kms;
        private final PulsarM6ObjectWalProjection projection;

        private HistoricalRun(
                WalRunHistoricalObjectReaderV1 reader, KmsCellSession kms, PulsarM6ObjectWalProjection projection) {
            this.reader = reader;
            this.kms = kms;
            this.projection = projection;
        }

        Verified readVerified(ExtentIdentity identity, ExtentLocator expected) throws IOException {
            ExtentLocator[] exact = {null};
            byte[][] owned = {null};
            var verified =
                    reader.readAppendUnit(identity.objectIdentity(), projection.verification(), 0, (frame, payload) -> {
                        exact[0] = locator(identity, frame);
                        if (expected != null) {
                            owned[0] = new byte[payload.remaining()];
                            payload.get(owned[0]);
                        }
                    });
            if (exact[0] == null || verified.protocolKind() != 2 || expected != null && !expected.equals(exact[0])) {
                throw new IllegalStateException("Historical Pulsar Object WAL frame differs from the exact locator");
            }
            return new Verified(exact[0], Math.toIntExact(verified.decodedPayloadBytes()), owned[0]);
        }

        @Override
        public void close() {
            reader.close();
            kms.close();
        }
    }
}
