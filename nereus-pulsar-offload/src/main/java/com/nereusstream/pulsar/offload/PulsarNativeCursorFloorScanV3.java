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
import com.nereusstream.domain.bytes.CanonicalUtf8;
import com.nereusstream.domain.bytes.Sha256Digest;
import com.nereusstream.metadata.spi.retention.ExactMetadataTransactionStoreV1.VersionedValue;
import com.nereusstream.storage.api.lifecycle.PhysicalResourceIdCodecV2;
import com.nereusstream.storage.api.lifecycle.PhysicalResourceIdV2;
import com.nereusstream.storage.bookkeeper.M5BookKeeperNamespaceAuthorityV2;
import com.nereusstream.storage.object.materialization.M5MaterializationRecordsV1.PositionDomain;
import com.nereusstream.storage.object.materialization.M5PulsarCoverageV2;
import com.nereusstream.storage.object.retention.M5ClosedWriterRegistryV1;
import com.nereusstream.storage.object.retention.M5PulsarAggregateAuthorityCodecV1;
import com.nereusstream.storage.object.retention.M5RetentionEvidenceAssemblerV1.ReferenceAdapterResultV1;
import com.nereusstream.storage.object.retention.M5RetentionRecordsV1;
import java.io.ByteArrayOutputStream;
import java.io.DataOutputStream;
import java.nio.charset.StandardCharsets;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.List;
import java.util.Optional;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.Executor;
import org.apache.bookkeeper.client.api.ReadHandle;
import org.apache.bookkeeper.mledger.impl.MetaStoreImpl;
import org.apache.bookkeeper.mledger.proto.PositionInfo;
import org.apache.pulsar.metadata.api.extended.MetadataStoreExtended;

/** Actual cursor and installed native retention floors inside an exact P1 source-scan fence. */
public final class PulsarNativeCursorFloorScanV3 implements AutoCloseable {
    private static final int MAX_BYTES = PulsarNativeCursorMetadataV2.MAX_BYTES;
    private final PulsarNativeBindingV1 binding;
    private final MetadataStoreExtended metadata;
    private final MetaStoreImpl parser = new MetaStoreImpl(null, null);
    private final M5BookKeeperNamespaceAuthorityV2 observer;
    private final VersionedValue scan;
    private final org.apache.bookkeeper.client.api.DigestType digestType;
    private final byte[] password;
    private volatile ReadHandle nativeHandle;
    private final PulsarNativeReadCellBudgetV1.Reservation workspace;
    private final CompletableFuture<Void> ended = new CompletableFuture<>();
    private final CompletableFuture<PulsarNativeCursorFloorScanV3> result = new CompletableFuture<>();
    private CompletableFuture<Void> closing;
    private boolean started;
    private volatile boolean closed;
    private M5PulsarCoverageV2 coverage;
    private M5PulsarCoverageV2.Position minimum;
    private M5PulsarCoverageV2.Position retentionMinimum;
    private CanonicalBytes witness;
    private int cursorCount;
    private List<PhysicalResourceIdV2> subscriptionReferences;
    private List<PhysicalResourceIdV2> recoveryReferences;
    private List<PhysicalResourceIdV2.Namespace> referenceNamespaces;
    private boolean unresolvedCheckpointProperties;
    private java.util.Map<Long, BookKeeperIntent> bookKeeperIntents = java.util.Map.of();
    private final java.util.Set<CompletableFuture<Void>> referenceOperations = new java.util.HashSet<>();
    private ByteArrayOutputStream audit = new ByteArrayOutputStream();
    private DataOutputStream out = new DataOutputStream(audit);

    private PulsarNativeCursorFloorScanV3(
            PulsarNativeBindingV1 binding,
            MetadataStoreExtended metadata,
            M5BookKeeperNamespaceAuthorityV2 observer,
            VersionedValue scan,
            PulsarNativeReadCellBudgetV1 budget,
            org.apache.bookkeeper.client.api.DigestType digestType,
            byte[] password) {
        this.binding = binding;
        this.metadata = metadata;
        this.observer = observer;
        this.scan = scan;
        this.digestType = digestType;
        this.password = password.clone();
        workspace = budget.reserveCursorObservation(binding, MAX_BYTES);
    }

    /** Caller retains the actual metadata/observer clients through close; this method creates no client or thread. */
    public static CompletableFuture<PulsarNativeCursorFloorScanV3> capture(
            PulsarNativeBindingV1 binding,
            MetadataStoreExtended metadata,
            M5BookKeeperNamespaceAuthorityV2 observer,
            VersionedValue sourceScan,
            PulsarNativeReadCellBudgetV1 budget,
            org.apache.bookkeeper.mledger.ManagedLedgerConfig config,
            Executor executor) {
        try {
            return new PulsarNativeCursorFloorScanV3(
                            binding,
                            metadata,
                            observer,
                            sourceScan,
                            budget,
                            org.apache.bookkeeper.client.api.DigestType.valueOf(
                                    config.getDigestType().name()),
                            config.getPassword())
                    .start(executor);
        } catch (Throwable failure) {
            return CompletableFuture.failedFuture(failure);
        }
    }

    private CompletableFuture<PulsarNativeCursorFloorScanV3> start(Executor executor) {
        workspace
                .ready()
                .thenRunAsync(
                        () -> {
                            try {
                                if (result.isDone()) {
                                    throw new java.util.concurrent.CancellationException(
                                            "floor observer ended before dispatch");
                                }
                                started = true;
                                workspace.sourceStarting();
                                capture();
                                ended.complete(null);
                                if (closed) {
                                    result.completeExceptionally(
                                            new IllegalStateException("Native floor source closed before delivery"));
                                    closeAsync();
                                } else if (!result.complete(this)) {
                                    closeAsync();
                                }
                            } catch (Throwable failure) {
                                fail(failure);
                            }
                        },
                        executor)
                .whenComplete((unused, failure) -> {
                    if (failure != null) {
                        fail(failure);
                    }
                });
        return result;
    }

    private void fail(Throwable failure) {
        reconcileCleanup().whenComplete((unused, cleanupFailure) -> {
            if (cleanupFailure != null) {
                workspace.retainUndeliveredSdk(this::reconcileCleanup, false);
            }
            result.completeExceptionally(cleanupFailure == null ? failure : cleanupFailure);
        });
    }

    CompletableFuture<Void> reconcileCleanup() {
        var original = nativeHandle;
        return (original == null ? CompletableFuture.<Void>completedFuture(null) : original.closeAsync())
                .thenCompose(unused -> {
                    nativeHandle = null;
                    ended.complete(null);
                    return closeAsync();
                });
    }

    private void requireFence() throws Exception {
        binding.logicalTrimCoordinator()
                .requireSourceScanCurrent(scan)
                .toCompletableFuture()
                .join();
    }

    private void capture() throws Exception {
        requireFence();
        var actualBinding = observer.readBinding().toCompletableFuture().join().orElseThrow();
        if (!actualBinding.metadataNamespace().equals(binding.metadataNamespace())) {
            throw new IllegalArgumentException("Native floor observer belongs to another metadata namespace");
        }
        String parent = "/managed-ledgers/" + binding.managedLedgerName();
        var original = readStored(parent).join().orElseThrow();
        var info = parser.parseManagedLedgerInfo(original.storedBytes(), MAX_BYTES);
        binding.requireStoredMetadataBinding(info);
        if (info.getLedgerInfosList().isEmpty() || info.getLedgerInfosList().size() > M5PulsarCoverageV2.MAX_LEDGERS) {
            throw new IllegalArgumentException("Native floor directory is empty or exceeds its bound");
        }
        var policy = M5PulsarAggregateAuthorityCodecV1.decodeAuthority(scan.canonicalStoredBytes())
                .nativeRetention()
                .filter(value -> value.installedByFactoryScope().isPresent())
                .orElse(null);
        var control = binding.m4Control();
        var selection = control.captureSelectedSourceMap().join();
        var materialized = selection.manifest()
                || selection
                        .map()
                        .filter(map -> PulsarNativeMaterializedReadV2.isRecipe(map.actualViewBytes()))
                        .isPresent();
        var selected = materialized ? control.captureSelectedMaterialization() : null;
        var nativeMap = materialized ? null : selection.map().orElse(null);
        var nativeDirectory = selected != null
                ? selected.selected().readMap().originalNativeDirectory(binding)
                : nativeMap == null ? null : nativeMap.floorDirectory(binding);
        if (nativeDirectory != null && !actualBinding.equals(nativeDirectory.bookKeeper())) {
            throw new IllegalArgumentException("Native floor selected BK namespace differs from actual observer");
        }
        if (selected != null
                && !nativeDirectory
                        .objects()
                        .equals(selected.selected().providerProfile().physicalBinding())) {
            throw new IllegalArgumentException("Native floor original Object namespace differs from selected profile");
        }
        out.writeInt(0x4e504346); // NPCF witness: never represented as an actual stored metadata key/value.
        out.writeInt(5);
        blob(CanonicalUtf8.fromString(scan.key()).bytes().toByteArray());
        blob(scan.metadataVersion().value().toByteArray());
        blob(scan.canonicalStoredSha256().bytes().toByteArray());
        blob(actualBinding.encode().toByteArray());
        stored(original);
        out.writeBoolean(materialized);
        if (selected != null) {
            // Full map bytes are selected by actual metadata, never supplied by the floor caller.
            var fact = selected.selected().selectorFact();
            blob(CanonicalUtf8.fromString(fact.key()).bytes().toByteArray());
            blob(fact.metadataVersion().value().toByteArray());
            blob(fact.canonicalStoredBytes().toByteArray());
            blob(selected.selected().readMap().encode().toByteArray());
        }
        out.writeBoolean(nativeMap != null);
        if (nativeMap != null) {
            authority(selection.selector().orElseThrow());
            authority(selection.source().orElseThrow());
        }
        var chain = new ArrayList<M5PulsarCoverageV2.ChainLedger>();
        var intents = new java.util.HashMap<Long, BookKeeperIntent>();
        var sizes = new long[info.getLedgerInfosCount()];
        var timestamps = new long[sizes.length];
        int sourceIndex = 0;
        for (var source : info.getLedgerInfosList()) {
            if (source.hasOffloadContext()) {
                var offload = source.getOffloadContext();
                if (offload.isComplete()
                        && offload.hasUidMsb()
                        && offload.hasUidLsb()
                        && offload.hasBookkeeperRetentionClass()
                        && offload.hasBookkeeperDeleteState()
                        && offload.getBookkeeperRetentionClass()
                                == org.apache.bookkeeper.mledger.proto.BookKeeperRetentionClass.DELETE_AFTER_VERIFIED
                        && offload.getBookkeeperDeleteState()
                                == org.apache.bookkeeper.mledger.proto.BookKeeperDeleteState.BK_DELETE_INTENT
                        && offload.isBookkeeperDeleted()
                        && source.getEntries() > 0
                        && source.getSize() > 0) {
                    intents.put(
                            source.getLedgerId(),
                            new BookKeeperIntent(
                                    new java.util.UUID(offload.getUidMsb(), offload.getUidLsb()),
                                    source.getEntries(),
                                    source.getSize()));
                }
            }
            int index = sourceIndex++;
            sizes[index] = source.hasSize() ? source.getSize() : -1;
            timestamps[index] = source.hasTimestamp() ? source.getTimestamp() : -1;
            var map = selected == null ? null : selected.selected().readMap();
            var mapped = map == null
                    ? Optional.<M5PulsarCoverageV2.ChainLedger>empty()
                    : map.plan().sourceCut().coverage().pulsarCoverage().orElseThrow().chain().stream()
                            .filter(row -> row.ledgerId() == source.getLedgerId()
                                    && !row.active()
                                    && map.fullyReplaces(row.ledgerId(), row.entryEndExclusive() - 1))
                            .findFirst();
            var objectOnly = nativeDirectory == null
                    ? null
                    : nativeDirectory.objectOnly().get(source.getLedgerId());
            out.writeByte(mapped.isPresent() ? 1 : objectOnly == null ? 0 : 2);
            if (mapped.isPresent()) {
                var row = mapped.orElseThrow();
                if (source.getEntries() != row.entryEndExclusive()) {
                    throw new IllegalStateException("Native mapped floor boundary differs from actual parent metadata");
                }
                var identity = PulsarNativeMaterializationReadMapV2.mappedLedgerRoot(
                        selected.selected().selector().selectedViewSha256(), row.ledgerId());
                out.writeLong(row.ledgerId());
                out.writeLong(row.entryEndExclusive());
                blob(identity.bytes().toByteArray());
                chain.add(new M5PulsarCoverageV2.ChainLedger(row.ledgerId(), false, row.entryEndExclusive(), identity));
                continue;
            }
            if (objectOnly != null) {
                var row = nativeDirectory.chain().stream()
                        .filter(value -> value.ledgerId() == source.getLedgerId())
                        .findFirst()
                        .orElseThrow();
                if (!source.hasOffloadContext()
                        || !source.getOffloadContext().isComplete()
                        || source.getEntries() != row.entryEndExclusive()
                        || source.getSize() != objectOnly.logicalLength()
                        || source.getOffloadContext().getUidMsb()
                                != objectOnly.identity().attemptUuid().getMostSignificantBits()
                        || source.getOffloadContext().getUidLsb()
                                != objectOnly.identity().attemptUuid().getLeastSignificantBits()) {
                    throw new IllegalStateException("Native Object-only floor differs from actual parent metadata");
                }
                out.writeLong(row.ledgerId());
                out.writeLong(row.entryEndExclusive());
                blob(row.sourceIdentitySha256().bytes().toByteArray());
                chain.add(row);
                continue;
            }
            try (var handle = open(source.getLedgerId())) {
                long count = Math.addExact(handle.getLastAddConfirmed(), 1);
                sizes[index] = handle.getLength();
                boolean active = !handle.getLedgerMetadata().isClosed();
                // Native asyncClose closes the writing ledger without persisting its final parent counters.
                // The actual BK header supplies that last ledger's boundary, including after normal close.
                boolean last = source
                        == info.getLedgerInfosList()
                                .get(info.getLedgerInfosList().size() - 1);
                if (active && !last || !active && !last && source.getEntries() != count) {
                    throw new IllegalStateException("Native floor source directory differs from actual BK state");
                }
                var descriptor = CanonicalBytes.copyOf(java.nio.ByteBuffer.allocate(25)
                        .putLong(source.getLedgerId())
                        .put((byte) (active ? 1 : 0))
                        .putLong(count)
                        .putLong(handle.getLength())
                        .array());
                blob(descriptor.toByteArray());
                chain.add(new M5PulsarCoverageV2.ChainLedger(
                        source.getLedgerId(), active, count, Sha256Digest.hash(descriptor)));
            }
            nativeHandle = null;
        }
        var recovery = new java.util.TreeSet<PhysicalResourceIdV2>();
        recovery.add(new PhysicalResourceIdV2.BookKeeperLedger(
                actualBinding.physicalNamespace(),
                info.getLedgerInfosList()
                        .get(info.getLedgerInfosList().size() - 1)
                        .getLedgerId()));
        var names = names();
        if (names.isEmpty()) {
            if (policy == null) {
                throw new IllegalStateException("Native no-cursor floor requires its authoritative policy");
            }
            var last = chain.get(chain.size() - 1);
            minimum = new M5PulsarCoverageV2.Position(last.ledgerId(), last.entryEndExclusive());
        }
        var observed = new ArrayList<MetaStoreImpl.StoredMutationValue>();
        for (String name : names) {
            blob(name.getBytes(StandardCharsets.UTF_8));
            var cursor = readStored(parent + "/" + name).join().orElseThrow();
            observed.add(cursor);
            stored(cursor);
            var cursorInfo = parser.parseManagedCursorInfo(cursor.storedBytes(), MAX_BYTES);
            unresolvedCheckpointProperties |= cursorInfo.getPropertiesCount() != 0;
            long ledgerId;
            long entryId;
            if (cursorInfo.getCursorsLedgerId() == -1) {
                if (!cursorInfo.hasMarkDeleteLedgerId() || !cursorInfo.hasMarkDeleteEntryId()) {
                    throw new IllegalStateException("Native cursor has no durable position");
                }
                ledgerId = cursorInfo.getMarkDeleteLedgerId();
                entryId = cursorInfo.getMarkDeleteEntryId();
            } else {
                recovery.add(new PhysicalResourceIdV2.BookKeeperLedger(
                        actualBinding.physicalNamespace(), cursorInfo.getCursorsLedgerId()));
                try (var handle = open(cursorInfo.getCursorsLedgerId())) {
                    long lac = handle.getLastAddConfirmed();
                    if (lac < 0) {
                        throw new IllegalStateException("Native cursor has no confirmed checkpoint");
                    }
                    out.writeLong(handle.getId());
                    out.writeLong(lac);
                    try (var entries = handle.readAsync(lac, lac).join()) {
                        var entry = entries.getEntry(lac);
                        if (entry.getLength() <= 0 || entry.getLength() > MAX_BYTES - audit.size() - 4) {
                            throw new IllegalArgumentException("Native cursor floor checkpoint exceeds its bound");
                        }
                        byte[] bytes = entry.getEntryBytes();
                        blob(bytes);
                        var position = new PositionInfo();
                        position.parseFrom(bytes);
                        unresolvedCheckpointProperties |= position.getPropertiesCount() != 0;
                        if (!position.hasLedgerId() || !position.hasEntryId()) {
                            throw new IllegalStateException("Native checkpoint has no position");
                        }
                        ledgerId = position.getLedgerId();
                        entryId = position.getEntryId();
                    }
                }
                nativeHandle = null;
            }
            var floor = firstRequired(chain, ledgerId, entryId);
            if (minimum == null || compare(chain, floor, minimum) < 0) {
                minimum = floor;
            }
        }
        out.writeBoolean(policy != null);
        if (policy != null) {
            int consumed = chain.size() - 1;
            for (int index = 0; index < chain.size(); index++) {
                if (chain.get(index).ledgerId() == minimum.ledgerId()) {
                    consumed = Math.min(consumed, index);
                    break;
                }
            }
            long observedNow = java.time.Clock.systemUTC().millis();
            blob(policy.encode().toByteArray());
            out.writeLong(observedNow);
            for (int index = 0; index < sizes.length; index++) {
                out.writeLong(sizes[index]);
                out.writeLong(timestamps[index]);
            }
            int retained = retainedLedgerIndex(sizes, timestamps, consumed, policy, observedNow);
            retentionMinimum =
                    new M5PulsarCoverageV2.Position(chain.get(retained).ledgerId(), 0);
        }
        var subscriptions = new java.util.TreeSet<PhysicalResourceIdV2>();
        var map = selected == null ? null : selected.selected().readMap();
        for (var row : chain) {
            if (row.entryEndExclusive() == 0) {
                continue;
            }
            var whole = new M5PulsarCoverageV2.EntryRange(row.ledgerId(), 0, row.entryEndExclusive());
            recovery.addAll(physicalReferences(whole, actualBinding, nativeDirectory, map, nativeMap));
            long start = row.ledgerId() == minimum.ledgerId()
                    ? minimum.entryId()
                    : compare(chain, new M5PulsarCoverageV2.Position(row.ledgerId(), 0), minimum) < 0
                            ? row.entryEndExclusive()
                            : 0;
            if (start < row.entryEndExclusive()) {
                subscriptions.addAll(physicalReferences(
                        new M5PulsarCoverageV2.EntryRange(row.ledgerId(), start, row.entryEndExclusive()),
                        actualBinding,
                        nativeDirectory,
                        map,
                        nativeMap));
            }
        }
        if (selected != null) {
            map.readableOutputs()
                    .forEach(credit -> recovery.add(credit.resource().orElseThrow()));
            if (selected.selected().selector().mode()
                    == com.nereusstream.storage.object.read.control.M4ReadControlRecordsV1.SelectorMode
                            .PREFERRED_WITH_FALLBACK) {
                recovery.addAll(map.fallbackPhysicalReferences(binding, actualBinding, nativeDirectory.objects()));
            }
        }
        subscriptionReferences = List.copyOf(subscriptions);
        bookKeeperIntents = java.util.Map.copyOf(intents);
        recoveryReferences = List.copyOf(recovery);
        referenceNamespaces = nativeDirectory == null
                ? List.of(actualBinding.physicalNamespace())
                : List.of(
                        actualBinding.physicalNamespace(),
                        nativeDirectory.objects().physicalNamespace());
        for (var references : List.of(subscriptionReferences, recoveryReferences)) {
            out.writeInt(references.size());
            for (var resource : references) {
                blob(PhysicalResourceIdCodecV2.encode(resource).toByteArray());
            }
        }
        var after = readStored(parent).join().orElseThrow();
        if (!same(original, after) || !names.equals(names())) {
            throw new IllegalStateException("Native floor directory changed during its source scan");
        }
        for (int index = 0; index < names.size(); index++) {
            var afterCursor = readStored(parent + "/" + names.get(index)).join().orElseThrow();
            if (!same(observed.get(index), afterCursor)) {
                throw new IllegalStateException("Native cursor changed during its source scan");
            }
        }
        if (selected != null) {
            var current = control.captureSelectedMaterialization();
            if (!current.nativeFacts().equals(selected.nativeFacts())
                    || !current.immutableFacts().equals(selected.immutableFacts())
                    || !current.selected()
                            .readMap()
                            .encode()
                            .equals(selected.selected().readMap().encode())) {
                throw new IllegalStateException("Native mapped floor selection changed during capture");
            }
        }
        control.requireSelectedSourceSnapshot(selection).join();
        requireFence();
        if (!observer.readBinding().toCompletableFuture().join().equals(Optional.of(actualBinding))) {
            throw new IllegalStateException("Native floor observer namespace changed during its scan");
        }
        witness = CanonicalBytes.copyOf(audit.toByteArray());
        var ranges = chain.stream()
                .filter(row -> !row.active() && row.entryEndExclusive() > 0)
                .map(row -> new M5PulsarCoverageV2.EntryRange(row.ledgerId(), 0, row.entryEndExclusive()))
                .toList();
        coverage = M5PulsarCoverageV2.of(
                binding.bindingIdentity(),
                directoryAuthority(actualBinding, parent, original, selection, selected, chain),
                chain,
                ranges);
        cursorCount = names.size();
    }

    /** Stable directory identity; current scan/cursor facts remain in the separate complete NPCF witness. */
    private Sha256Digest directoryAuthority(
            com.nereusstream.storage.api.lifecycle.PhysicalNamespaceAuthorityBindingV2 namespace,
            String parent,
            MetaStoreImpl.StoredMutationValue original,
            PulsarNativeM4ControlV1.SelectedSourceSnapshot selection,
            PulsarNativeM4ControlV1.SelectedMaterializationSnapshot selected,
            List<M5PulsarCoverageV2.ChainLedger> chain)
            throws java.io.IOException {
        var bytes = new ByteArrayOutputStream();
        try (var directory = new DataOutputStream(bytes)) {
            directory.writeInt(0x4e504744); // NPGD
            directory.writeInt(1);
            directoryBlob(
                    directory,
                    CanonicalUtf8.fromString("NEREUS_V2_NATIVE_PULSAR_FLOOR_DIRECTORY_V1")
                            .bytes());
            var identity = binding.bindingIdentity();
            directory.write(identity.bindingId().digest().bytes().toByteArray());
            directory.write(identity.incarnationSha256().bytes().toByteArray());
            directory.write(identity.storageEpochSha256().bytes().toByteArray());
            directoryBlob(directory, namespace.encode());
            directoryBlob(directory, CanonicalUtf8.fromString(parent).bytes());
            directory.writeLong(original.stat().getVersion());
            directory.write(Sha256Digest.hash(CanonicalBytes.copyOf(original.storedBytes()))
                    .bytes()
                    .toByteArray());
            directory.writeByte(selected != null ? 2 : selection.source().isPresent() ? 1 : 0);
            directoryFact(directory, selection.selector());
            directoryFact(directory, selection.source());
            if (selected != null) {
                var manifest = selected.immutableFacts().values().stream()
                        .filter(value -> value.key()
                                .endsWith("/manifests/"
                                        + selected.selected()
                                                .materializedManifestSha256()
                                                .toHex()))
                        .findFirst()
                        .orElseThrow();
                directoryFact(directory, Optional.of(manifest));
                directory.write(selected.selected().readMap().sha256().bytes().toByteArray());
            }
            directory.writeInt(chain.size());
            for (var ledger : chain) {
                directory.writeLong(ledger.ledgerId());
                directory.writeBoolean(ledger.active());
                directory.writeLong(ledger.entryEndExclusive());
                directory.write(ledger.sourceIdentitySha256().bytes().toByteArray());
            }
        }
        return Sha256Digest.hash(CanonicalBytes.copyOf(bytes.toByteArray()));
    }

    private static void directoryFact(DataOutputStream output, Optional<VersionedValue> fact)
            throws java.io.IOException {
        output.writeBoolean(fact.isPresent());
        if (fact.isPresent()) {
            var value = fact.orElseThrow();
            directoryBlob(output, CanonicalUtf8.fromString(value.key()).bytes());
            directoryBlob(output, value.metadataVersion().value());
            output.write(value.canonicalStoredSha256().bytes().toByteArray());
        }
    }

    private static void directoryBlob(DataOutputStream output, CanonicalBytes value) throws java.io.IOException {
        output.writeInt(value.length());
        output.write(value.toByteArray());
    }

    private List<PhysicalResourceIdV2> physicalReferences(
            M5PulsarCoverageV2.EntryRange range,
            com.nereusstream.storage.api.lifecycle.PhysicalNamespaceAuthorityBindingV2 bk,
            PulsarNativeM4SourceMapV1.FloorDirectory directory,
            PulsarNativeMaterializationReadMapV2 map,
            PulsarNativeM4SourceMapV1 nativeMap) {
        if (map != null) {
            var ledger = map.plan().sourceCut().coverage().pulsarCoverage().orElseThrow().chain().stream()
                    .filter(row -> row.ledgerId() == range.ledgerId())
                    .findFirst();
            if (ledger.isPresent()
                    && range.endEntryExclusive() <= ledger.orElseThrow().entryEndExclusive()) {
                var result = new java.util.TreeSet<PhysicalResourceIdV2>();
                for (var slice : map.splitAtSourceBoundaries(range)) {
                    var route =
                            map.locate(new M5PulsarCoverageV2.Position(slice.ledgerId(), slice.startEntryInclusive()));
                    if (route.rewrittenPayload().isPresent()) {
                        result.add(route.rewrittenPayload().orElseThrow());
                    } else {
                        result.addAll(PulsarNativeM4SourceMapV1.nativeLedgerReferences(
                                route.retainedNativeSource().orElseThrow(),
                                binding,
                                bk,
                                directory.objects(),
                                slice.ledgerId(),
                                slice.endEntryExclusive()));
                    }
                }
                return List.copyOf(result);
            }
            return map.physicalLedgerReferences(
                            binding, bk, directory.objects(), range.ledgerId(), range.endEntryExclusive())
                    .orElseGet(() -> List.of(
                            new PhysicalResourceIdV2.BookKeeperLedger(bk.physicalNamespace(), range.ledgerId())));
        }
        return nativeMap == null
                ? List.of(new PhysicalResourceIdV2.BookKeeperLedger(bk.physicalNamespace(), range.ledgerId()))
                : nativeMap.physicalDataReferences(binding, bk, directory.objects(), range);
    }

    /** The original completed metadata-only observation can share a proof's workspace lifetime with Task capture. */
    synchronized java.util.UUID referenceScope(
            VersionedValue expectedScan, PulsarNativeReadCellBudgetV1.Reservation parent) {
        requireOpen();
        if (!scan.equals(expectedScan)
                || !workspace.sameAllocator(parent)
                || nativeHandle != null
                || !ended.isDone()
                || ended.isCompletedExceptionally()
                || ended.isCancelled()) {
            throw new IllegalStateException("Native cursor observation belongs to another or unfinished source scan");
        }
        return workspace.operationId();
    }

    synchronized void requireObservationScope(VersionedValue expectedScan) {
        requireOpen();
        if (!scan.equals(expectedScan)
                || nativeHandle != null
                || !ended.isDone()
                || ended.isCompletedExceptionally()
                || ended.isCancelled()) {
            throw new IllegalStateException("Native observation is not the original completed source scan");
        }
    }

    /** One resource-owning GC observation, including its already accepted child I/O. */
    final class ObservationGroup {
        private int active;
        private CompletableFuture<Void> terminal;

        <T> CompletableFuture<T> retain(
                java.util.function.Supplier<? extends java.util.concurrent.CompletionStage<T>> operation) {
            final CompletableFuture<Void> original;
            synchronized (PulsarNativeCursorFloorScanV3.this) {
                requireOpen();
                if (active == 0) {
                    if (referenceOperations.size() >= 2) {
                        return CompletableFuture.failedFuture(
                                new IllegalStateException("Native observation slots are occupied"));
                    }
                    terminal = new CompletableFuture<>();
                    referenceOperations.add(terminal);
                }
                active++;
                original = terminal;
            }
            var observer = new CompletableFuture<T>();
            java.util.concurrent.CompletionStage<T> work;
            try {
                work = operation.get();
            } catch (Throwable failure) {
                work = CompletableFuture.failedFuture(failure);
            }
            work.whenComplete((value, failure) -> {
                boolean drained;
                synchronized (PulsarNativeCursorFloorScanV3.this) {
                    drained = --active == 0;
                    if (drained) {
                        referenceOperations.remove(original);
                    }
                }
                if (drained) {
                    original.complete(null);
                }
                if (failure == null) {
                    observer.complete(value);
                } else {
                    observer.completeExceptionally(failure);
                }
            });
            return observer;
        }
    }

    ObservationGroup observationGroup() {
        return new ObservationGroup();
    }

    /** Original accepted I/O joins the existing observation drain; cancelling a returned observer cannot end it. */
    <T> CompletableFuture<T> retainObservation(
            java.util.function.Supplier<? extends java.util.concurrent.CompletionStage<T>> operation) {
        var terminal = new CompletableFuture<Void>();
        var observer = new CompletableFuture<T>();
        synchronized (this) {
            requireOpen();
            if (referenceOperations.size() >= 2) {
                return CompletableFuture.failedFuture(
                        new IllegalStateException("Native observation slots are occupied"));
            }
            referenceOperations.add(terminal);
        }
        java.util.concurrent.CompletionStage<T> work;
        try {
            work = operation.get();
        } catch (Throwable failure) {
            work = CompletableFuture.failedFuture(failure);
        }
        work.whenComplete((value, failure) -> {
            synchronized (this) {
                referenceOperations.remove(terminal);
            }
            terminal.complete(null);
            if (failure == null) {
                observer.complete(value);
            } else {
                observer.completeExceptionally(failure);
            }
        });
        return observer;
    }

    /** Complete scoped Native cursor/recovery data; first-admission writer closure is mandatory for absence. */
    public CompletableFuture<ReferenceAdapterResultV1> scanReferences(
            M5ClosedWriterRegistryV1 registry,
            M5RetentionRecordsV1.ReferenceKindV1 kind,
            PhysicalResourceIdV2 target,
            Executor executor) {
        var original = new CompletableFuture<Void>();
        var observer = new CompletableFuture<ReferenceAdapterResultV1>();
        try {
            java.util.Objects.requireNonNull(target, "target");
            java.util.Objects.requireNonNull(executor, "executor");
            synchronized (this) {
                requireOpen();
                if (referenceOperations.size() >= 2) {
                    throw new IllegalStateException("Native cursor reference observation slots are occupied");
                }
                referenceOperations.add(original);
            }
            PulsarNativeWriterRegistryV2.requireNativeOwners(registry);
            var source = M5PulsarAggregateAuthorityCodecV1.decodeAuthority(scan.canonicalStoredBytes());
            if (source.retentionPolicy().isEmpty()
                    || source.firstNativeFactoryScope().isEmpty()
                    || !source.writerEnrollment().equals(Optional.of(registry.enrollment()))) {
                throw new IllegalStateException("Native cursor references lack original closed writer admission");
            }
            if (kind != M5RetentionRecordsV1.ReferenceKindV1.PULSAR_SUBSCRIPTION_OR_REPLICATION_CURSOR
                    && kind != M5RetentionRecordsV1.ReferenceKindV1.RECOVERY_CHECKPOINT_OR_SNAPSHOT) {
                throw new IllegalArgumentException("Native cursor source does not own this reference class");
            }
            if (unresolvedCheckpointProperties) {
                // Native compaction also keeps a BK recovery root in cursor position properties.
                throw new IllegalStateException(
                        "Native cursor checkpoint properties have unresolved recovery references");
            }
            var work = binding.logicalTrimCoordinator()
                    .requireSourceScanCurrent(scan)
                    .thenCompose(ignored -> binding.revalidate())
                    .thenApplyAsync(
                            ignored -> {
                                synchronized (this) {
                                    requireOpen();
                                    if (!referenceNamespaces.contains(target.namespace())) {
                                        throw new IllegalArgumentException(
                                                "Native cursor target belongs to another physical namespace");
                                    }
                                    var refs =
                                            kind == M5RetentionRecordsV1.ReferenceKindV1.RECOVERY_CHECKPOINT_OR_SNAPSHOT
                                                    ? recoveryReferences
                                                    : subscriptionReferences;
                                    var row = new M5RetentionRecordsV1.ReferenceObservationV1(
                                            kind,
                                            new M5RetentionRecordsV1.AuthorityFactV1(
                                                    scan.key(), scan.metadataVersion(), scan.canonicalStoredSha256()),
                                            target.sha256(),
                                            com.nereusstream.storage.object.materialization.M5MaterializationRecordsV1
                                                    .ProtocolCoverage.pulsar(coverage),
                                            refs.contains(target)
                                                    ? M5RetentionRecordsV1.ReferenceDispositionV1.PRESENT
                                                    : M5RetentionRecordsV1.ReferenceDispositionV1.ABSENT,
                                            true);
                                    return new ReferenceAdapterResultV1(
                                            kind,
                                            new M5RetentionRecordsV1.ReferenceScanSummaryV1(
                                                    kind, 1, 1, witness.length(), true),
                                            List.of(row));
                                }
                            },
                            executor)
                    .thenCompose(value -> binding.logicalTrimCoordinator()
                            .requireSourceScanCurrent(scan)
                            .thenApply(ignored -> value))
                    .toCompletableFuture();
            work.whenComplete((value, failure) -> {
                synchronized (this) {
                    referenceOperations.remove(original);
                }
                original.complete(null);
                if (failure != null) {
                    observer.completeExceptionally(failure);
                } else if (closed) {
                    observer.completeExceptionally(
                            new IllegalStateException("Native cursor source closed during reference observation"));
                } else {
                    observer.complete(value);
                }
            });
        } catch (Throwable failure) {
            synchronized (this) {
                referenceOperations.remove(original);
            }
            original.complete(null);
            observer.completeExceptionally(failure);
        }
        return observer;
    }

    private ReadHandle open(long ledgerId) throws Exception {
        var handle = observer.borrowClient()
                .newOpenLedgerOp()
                .withRecovery(false)
                .withLedgerId(ledgerId)
                .withDigestType(digestType)
                .withPassword(password)
                .execute()
                .join();
        nativeHandle = handle;
        if (!Arrays.equals(
                handle.getLedgerMetadata().getCustomMetadata().get("pulsar/managed-ledger"),
                binding.managedLedgerName().getBytes(StandardCharsets.UTF_8))) {
            handle.closeAsync().join();
            nativeHandle = null;
            throw new IllegalArgumentException("Native floor BK source belongs to another managed ledger");
        }
        return handle;
    }

    private List<String> names() throws Exception {
        var names = metadata.getChildrenFromStore("/managed-ledgers/" + binding.managedLedgerName()).join().stream()
                .sorted()
                .toList();
        if (names.size() > PulsarNativeCursorMetadataV2.MAX_CURSORS
                || names.stream().distinct().count() != names.size()
                || names.stream()
                        .anyMatch(name -> name.isEmpty()
                                || name.contains("/")
                                || name.indexOf('\0') >= 0
                                || name.getBytes(StandardCharsets.UTF_8).length > 1024)) {
            throw new IllegalArgumentException("Native floor cursor directory exceeds its scope/bounds");
        }
        return names;
    }

    private CompletableFuture<Optional<MetaStoreImpl.StoredMutationValue>> readStored(String path) {
        return metadata.get(path)
                .thenApply(value -> value.map(actual -> {
                    if (actual.getValue().length > MAX_BYTES
                            || !path.equals(actual.getStat().getPath())) {
                        throw new IllegalArgumentException("Native floor metadata exceeds its scope/bound");
                    }
                    return new MetaStoreImpl.StoredMutationValue(actual.getValue(), actual.getStat());
                }));
    }

    private void authority(VersionedValue value) throws Exception {
        blob(CanonicalUtf8.fromString(value.key()).bytes().toByteArray());
        blob(value.metadataVersion().value().toByteArray());
        blob(value.canonicalStoredBytes().toByteArray());
    }

    private void stored(MetaStoreImpl.StoredMutationValue value) throws Exception {
        out.writeLong(value.stat().getVersion());
        blob(value.storedBytes());
    }

    private void blob(byte[] bytes) throws Exception {
        if (bytes.length > MAX_BYTES - audit.size() - 4) {
            throw new IllegalArgumentException("Native cursor floor witness exceeds its bound");
        }
        out.writeInt(bytes.length);
        out.write(bytes);
    }

    private static boolean same(MetaStoreImpl.StoredMutationValue left, MetaStoreImpl.StoredMutationValue right) {
        return left.stat().equals(right.stat()) && Arrays.equals(left.storedBytes(), right.storedBytes());
    }

    private static M5PulsarCoverageV2.Position firstRequired(
            List<M5PulsarCoverageV2.ChainLedger> chain, long ledgerId, long entryId) {
        if (ledgerId == -1 && entryId == -1) {
            return new M5PulsarCoverageV2.Position(chain.get(0).ledgerId(), 0);
        }
        for (int index = 0; index < chain.size(); index++) {
            var ledger = chain.get(index);
            if (ledger.ledgerId() == ledgerId && entryId >= -1 && entryId < ledger.entryEndExclusive()) {
                long next = entryId + 1;
                if (next == ledger.entryEndExclusive() && index + 1 < chain.size()) {
                    return new M5PulsarCoverageV2.Position(chain.get(index + 1).ledgerId(), 0);
                }
                return new M5PulsarCoverageV2.Position(ledgerId, next);
            }
        }
        throw new IllegalStateException("Native cursor floor position is outside its actual directory");
    }

    private static int compare(
            List<M5PulsarCoverageV2.ChainLedger> chain,
            M5PulsarCoverageV2.Position left,
            M5PulsarCoverageV2.Position right) {
        int a = -1, b = -1;
        for (int index = 0; index < chain.size(); index++) {
            if (chain.get(index).ledgerId() == left.ledgerId()) {
                a = index;
            }
            if (chain.get(index).ledgerId() == right.ledgerId()) {
                b = index;
            }
        }
        return a == b ? Long.compare(left.entryId(), right.entryId()) : Integer.compare(a, b);
    }

    /** Same sealed, consumed prefix and OR boundary as this dependency's ManagedLedgerImpl trimming. */
    static int retainedLedgerIndex(
            long[] sizes,
            long[] timestamps,
            int consumedExclusive,
            com.nereusstream.storage.object.retention.M5PulsarRetentionConfigurationV1 policy,
            long now) {
        if (sizes.length == 0
                || sizes.length != timestamps.length
                || consumedExclusive < 0
                || consumedExclusive >= sizes.length) {
            throw new IllegalArgumentException("Native retention prefix differs from its directory");
        }
        long total = 0;
        for (long size : sizes) {
            if (size < 0) {
                throw new IllegalStateException("Native retention lacks an actual ledger length");
            }
            total = Math.addExact(total, size);
        }
        long removed = 0;
        int retained = 0;
        for (; retained < consumedExclusive; retained++) {
            removed = Math.addExact(removed, sizes[retained]);
            boolean expired = policy.retentionTimeMillis() >= 0
                    && timestamps[retained] > 0
                    && now > timestamps[retained]
                    && now - timestamps[retained] > policy.retentionTimeMillis();
            boolean overSize =
                    policy.retentionSizeInMB() >= 0 && total - removed >= policy.retentionSizeInMB() * 1_048_576L;
            if (!expired && !overSize) {
                break;
            }
        }
        return retained;
    }

    public synchronized M5RetentionRecordsV1.RetentionFloorObservationV1 bindingRetentionFloor() {
        requireOpen();
        if (retentionMinimum == null || unresolvedCheckpointProperties) {
            throw new IllegalStateException(
                    "Native retention policy or compaction/recovery obligations are unresolved");
        }
        return new M5RetentionRecordsV1.RetentionFloorObservationV1(
                M5RetentionRecordsV1.FloorClassV1.BINDING_EPOCH_POLICY,
                new M5RetentionRecordsV1.AuthorityFactV1(
                        scan.key(), scan.metadataVersion(), scan.canonicalStoredSha256()),
                PositionDomain.PULSAR_ENTRY,
                -1,
                true,
                true,
                Optional.of(retentionMinimum));
    }

    public synchronized M5RetentionRecordsV1.RetentionFloorObservationV1 floor() {
        requireOpen();
        return new M5RetentionRecordsV1.RetentionFloorObservationV1(
                M5RetentionRecordsV1.FloorClassV1.PULSAR_CURSOR_SUBSCRIPTION,
                new M5RetentionRecordsV1.AuthorityFactV1(
                        scan.key(), scan.metadataVersion(), scan.canonicalStoredSha256()),
                PositionDomain.PULSAR_ENTRY,
                -1,
                true,
                true,
                Optional.of(minimum));
    }

    record BookKeeperIntent(java.util.UUID offloadAttempt, long entries, long logicalLength) {}

    /** Derived only from the captured actual Native parent inside this same P1 writer fence. */
    synchronized BookKeeperIntent bookKeeperIntent(long ledgerId) {
        requireOpen();
        var intent = bookKeeperIntents.get(ledgerId);
        if (intent == null) {
            throw new IllegalStateException("Native parent has no exact DELETE_AFTER_VERIFIED BK intent");
        }
        return intent;
    }

    public synchronized M5PulsarCoverageV2 coverage() {
        requireOpen();
        return coverage;
    }

    public synchronized CanonicalBytes witness() {
        requireOpen();
        return witness;
    }

    public synchronized int cursorCount() {
        requireOpen();
        return cursorCount;
    }

    private void requireOpen() {
        if (closed || witness == null) {
            throw new IllegalStateException("Native floor source is not open");
        }
    }

    public synchronized CompletableFuture<Void> closeAsync() {
        closed = true;
        if (closing == null || closing.isCompletedExceptionally()) {
            closing = ended.thenCompose(ignored -> {
                        synchronized (this) {
                            return CompletableFuture.allOf(referenceOperations.toArray(CompletableFuture[]::new));
                        }
                    })
                    .thenCompose(unused -> started ? workspace.releaseAfterDrain() : workspace.releaseBeforeActor())
                    .thenRun(() -> {
                        witness = null;
                        coverage = null;
                        minimum = null;
                        retentionMinimum = null;
                        audit = null;
                        out = null;
                        subscriptionReferences = null;
                        recoveryReferences = null;
                        referenceNamespaces = null;
                    });
        }
        return closing.copy();
    }

    @Override
    public void close() throws Exception {
        closeAsync().join();
    }
}
