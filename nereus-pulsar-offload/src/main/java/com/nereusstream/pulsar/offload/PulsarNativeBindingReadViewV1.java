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
import com.nereusstream.pulsar.offload.PulsarNativeBindingV1.CapturedSource;
import com.nereusstream.storage.api.lifecycle.PhysicalNamespaceAuthorityBindingV2;
import com.nereusstream.storage.api.lifecycle.PhysicalResourceIdCodecV2;
import com.nereusstream.storage.api.lifecycle.PhysicalResourceIdV2;
import com.nereusstream.storage.api.lifecycle.PhysicalResourceIdV2.Namespace;
import com.nereusstream.storage.bookkeeper.M5BookKeeperNamespaceAuthorityV2;
import com.nereusstream.storage.object.read.control.M4ReadControlRecordsV1.BindingIdentity;
import java.io.ByteArrayOutputStream;
import java.io.DataOutputStream;
import java.io.IOException;
import java.util.List;
import java.util.Map;
import java.util.Optional;
import java.util.OptionalLong;
import java.util.concurrent.CompletableFuture;
import java.util.function.BooleanSupplier;
import java.util.stream.Collectors;
import org.apache.bookkeeper.mledger.Position;
import org.apache.bookkeeper.mledger.SourceSafeLedgerOffloader.NativeReadAdmission;
import org.apache.bookkeeper.mledger.impl.ManagedLedgerImpl.CapturedBookKeeperReadSource;
import org.apache.bookkeeper.mledger.impl.ManagedLedgerImpl.CapturedNativeReadView;
import org.apache.bookkeeper.mledger.impl.ManagedLedgerImpl.CapturedReadBatch;
import org.apache.bookkeeper.mledger.impl.ManagedLedgerImpl.CapturedReadLimits;
import org.apache.bookkeeper.mledger.impl.ManagedLedgerImpl.CapturedReadOperation;
import org.apache.bookkeeper.mledger.impl.ManagedLedgerImpl.CapturedSourceVerification;

/**
 * One complete Native data-ledger cut, including plain sealed BK and the confirmed active tail. This is no M4
 * capability/selector grant and supplies no cursor/writer/reference/delete qualification.
 */
public final class PulsarNativeBindingReadViewV1 {
    record Slot(
            CapturedSource offloaded,
            CapturedBookKeeperReadSource plain,
            PhysicalNamespaceAuthorityBindingV2 bookKeeperBinding,
            Optional<PhysicalResourceIdV2.BookKeeperLedger> bookKeeperMember,
            OptionalLong bookKeeperVersion) {
        static Slot offloaded(CapturedSource source) {
            return new Slot(
                    source,
                    null,
                    source.bookKeeperBinding(),
                    source.preferredOnly()
                            ? Optional.empty()
                            : Optional.of((PhysicalResourceIdV2.BookKeeperLedger)
                                    source.members().get(0)),
                    source.preferredOnly()
                            ? OptionalLong.empty()
                            : OptionalLong.of(source.bookKeeperMetadataVersion()));
        }

        static Slot bookKeeper(
                CapturedBookKeeperReadSource source,
                PhysicalNamespaceAuthorityBindingV2 binding,
                PhysicalResourceIdV2.BookKeeperLedger member,
                long metadataVersion) {
            return new Slot(null, source, binding, Optional.of(member), OptionalLong.of(metadataVersion));
        }

        long ledgerId() {
            return offloaded == null
                    ? bookKeeperMember.orElseThrow().ledgerId()
                    : offloaded.nativeSource().ledgerId();
        }

        long lastEntry() {
            return offloaded == null
                    ? plain.lastAddConfirmed()
                    : offloaded.nativeSource().lastAddConfirmed();
        }

        boolean activeTail() {
            return plain != null && plain.activeTail();
        }

        long sealedLength() {
            return offloaded == null
                    ? plain.sealedLength()
                    : offloaded.nativeSource().length();
        }

        List<PhysicalResourceIdV2> members() {
            return offloaded == null ? List.of(bookKeeperMember.orElseThrow()) : offloaded.members();
        }

        CanonicalBytes encode() {
            if (offloaded != null) {
                return offloaded.readView().canonicalBytes();
            }
            try {
                var bytes = new ByteArrayOutputStream();
                try (var out = new DataOutputStream(bytes)) {
                    writeBytes(
                            out,
                            CanonicalUtf8.fromString("NEREUS_V2_NATIVE_PULSAR_BK_READ_SOURCE_V1")
                                    .bytes());
                    writeBytes(out, bookKeeperBinding.encode());
                    writeBytes(out, PhysicalResourceIdCodecV2.encode(bookKeeperMember.orElseThrow()));
                    out.writeLong(bookKeeperVersion.orElseThrow());
                    out.writeBoolean(activeTail());
                    out.writeLong(lastEntry());
                    if (!activeTail()) {
                        out.writeLong(sealedLength());
                    }
                }
                return CanonicalBytes.copyOf(bytes.toByteArray());
            } catch (IOException failure) {
                throw new IllegalStateException("cannot encode native BK source", failure);
            }
        }
    }

    private final PulsarNativeBindingV1 binding;
    private final CapturedNativeReadView nativeView;
    private final List<Slot> slots;
    private final Map<Long, Slot> routes;
    private final List<PhysicalResourceIdV2> members;
    private final CanonicalBytes canonicalBytes;
    private final Sha256Digest sha256;
    private final int bookKeeperFrameBytes;
    private PulsarNativeSourcePreparationV1 preparation;
    private final PhysicalNamespaceAuthorityBindingV2 objectBinding;
    private final PhysicalNamespaceAuthorityBindingV2 bookKeeperBinding;
    private final PulsarNativeMaterializationReadMapV2 mapped;
    private final Sha256Digest materializedManifest;
    private PulsarNativeMaterializedReadV2 preferredReads;

    private PulsarNativeBindingReadViewV1(
            PulsarNativeBindingV1 binding,
            CapturedNativeReadView nativeView,
            PhysicalNamespaceAuthorityBindingV2 objects,
            PhysicalNamespaceAuthorityBindingV2 bookKeeper,
            List<Slot> slots,
            int bookKeeperFrameBytes) {
        this(binding, nativeView, objects, bookKeeper, slots, bookKeeperFrameBytes, null, null);
    }

    private PulsarNativeBindingReadViewV1(
            PulsarNativeBindingV1 binding,
            CapturedNativeReadView nativeView,
            PhysicalNamespaceAuthorityBindingV2 objects,
            PhysicalNamespaceAuthorityBindingV2 bookKeeper,
            List<Slot> slots,
            int bookKeeperFrameBytes,
            PulsarNativeMaterializationReadMapV2 mapped,
            Sha256Digest materializedManifest) {
        this.mapped = mapped;
        this.materializedManifest = materializedManifest;
        if (bookKeeperFrameBytes <= 0) {
            throw new IllegalArgumentException("actual BK frame limit is non-positive");
        }
        this.bookKeeperFrameBytes = bookKeeperFrameBytes;
        this.objectBinding = objects;
        this.bookKeeperBinding = java.util.Objects.requireNonNull(bookKeeper, "actual BookKeeper namespace binding");
        if (!bookKeeper.metadataNamespace().equals(binding.metadataNamespace())
                || slots.stream().anyMatch(slot -> !slot.bookKeeperBinding().equals(bookKeeper))) {
            throw new IllegalArgumentException("Native source BookKeeper namespace differs from its SDK capture");
        }
        this.binding = binding;
        this.nativeView = nativeView;
        this.slots = List.copyOf(slots);
        if (!nativeView.isCurrent()
                || !this.slots.stream()
                        .map(Slot::ledgerId)
                        .toList()
                        .equals(nativeView.ledgerIds().stream()
                                .filter(id -> mapped == null || !mapped.fullyReplaces(id, nativeView.lastEntry(id)))
                                .toList())
                || this.slots.stream()
                        .anyMatch(slot ->
                                !slot.bookKeeperBinding().metadataNamespace().equals(objects.metadataNamespace())
                                        || (slot.offloaded() != null
                                                && (!slot.offloaded()
                                                                .binding()
                                                                .bindingIdentity()
                                                                .equals(binding.bindingIdentity())
                                                        || !slot.offloaded()
                                                                .physicalBinding()
                                                                .equals(objects))))) {
            throw new IllegalStateException("native Binding read view has inconsistent coverage or authority");
        }
        routes = Map.copyOf(this.slots.stream().collect(Collectors.toMap(Slot::ledgerId, slot -> slot)));
        members = java.util.stream.Stream.concat(
                        this.slots.stream().flatMap(slot -> slot.members().stream()),
                        mapped == null
                                ? java.util.stream.Stream.empty()
                                : mapped.readableOutputs().stream()
                                        .map(credit -> credit.resource().orElseThrow()))
                .distinct()
                .toList();
        var nativeBytes = encode(objects);
        canonicalBytes = mapped == null
                ? nativeBytes
                : PulsarNativeMaterializedReadV2.encodeRecipe(nativeBytes, materializedManifest, mapped);
        sha256 = Sha256Digest.hash(canonicalBytes);
    }

    static PulsarNativeBindingReadViewV1 fromCaptured(
            PulsarNativeBindingV1 binding,
            CapturedNativeReadView nativeView,
            PhysicalNamespaceAuthorityBindingV2 objects,
            PhysicalNamespaceAuthorityBindingV2 bookKeeper,
            List<Slot> slots,
            int bookKeeperFrameBytes) {
        return new PulsarNativeBindingReadViewV1(binding, nativeView, objects, bookKeeper, slots, bookKeeperFrameBytes);
    }

    static PulsarNativeBindingReadViewV1 fromMapped(
            PulsarNativeBindingV1 binding,
            CapturedNativeReadView nativeView,
            PhysicalNamespaceAuthorityBindingV2 objects,
            PhysicalNamespaceAuthorityBindingV2 bookKeeper,
            List<Slot> slots,
            int bookKeeperFrameBytes,
            PulsarNativeM4ControlV1.SelectedMaterialization selected) {
        return new PulsarNativeBindingReadViewV1(
                binding,
                nativeView,
                objects,
                bookKeeper,
                slots,
                bookKeeperFrameBytes,
                selected.readMap(),
                selected.materializedManifestSha256());
    }

    /** Actual SDK namespace remains available when every data source is served by mapped Object outputs. */
    public PhysicalNamespaceAuthorityBindingV2 bookKeeperNamespaceBinding() {
        return bookKeeperBinding;
    }

    void attachPreferredReads(PulsarNativeMaterializedReadV2 reads) {
        if (mapped == null || preferredReads != null) {
            throw new IllegalStateException("Native preferred pipeline is absent or already attached");
        }
        preferredReads = reads;
    }

    boolean hasPreferredReadSource(long ledgerId) {
        return preferredReads != null && nativeView.ledgerIds().contains(ledgerId);
    }

    PulsarNativeMaterializationReadMapV2 materializedMap() {
        return java.util.Objects.requireNonNull(mapped, "Native selected materialization map");
    }

    S3PulsarOffloadObjectStoreV1.MaterializationConnection materializedConnectionForControl() {
        return java.util.Objects.requireNonNull(preferredReads, "Native preferred SDK reader")
                .connectionForControl();
    }

    boolean materialized() {
        return mapped != null;
    }

    void requireSelectedRoot(
            com.nereusstream.storage.object.read.control.M4ReadControlRecordsV1.BindingReadSelector selector) {
        if (selector.selectedViewSha256().equals(sha256)) {
            return;
        }
        var predecessor = mapped == null ? null : mapped.plan().sourceCut().predecessorSelector();
        if (mapped == null
                || !selector.selectedViewSha256().equals(materializedManifest)
                || selector.ownerEpoch() != predecessor.ownerEpoch()
                || selector.readAdmissionEpoch() != Math.addExact(predecessor.readAdmissionEpoch(), 1)
                || selector.sourceGeneration() != Math.addExact(predecessor.sourceGeneration(), 1)
                || selector.mode()
                        != com.nereusstream.storage.object.read.control.M4ReadControlRecordsV1.SelectorMode
                                .PREFERRED_WITH_FALLBACK) {
            throw new IllegalArgumentException("Native SDK projection does not belong to its selected root");
        }
    }

    List<com.nereusstream.storage.object.read.control.M4ReadControlRecordsV1.SourceProtectionIdentity>
            materializedFallbackSources() {
        var cut = mapped.plan().sourceCut();
        var predecessor = cut.predecessorSelector();
        return cut.sources().stream()
                .map(source ->
                        new com.nereusstream.storage.object.read.control.M4ReadControlRecordsV1
                                .SourceProtectionIdentity(
                                source.sourceIdentitySha256(),
                                cut.identity().workerEpoch(),
                                Math.addExact(predecessor.readAdmissionEpoch(), 1),
                                Math.addExact(predecessor.sourceGeneration(), 1),
                                predecessor.capability()))
                .sorted(java.util.Comparator.comparing(
                        source -> source.sourceIdentitySha256().toHex()))
                .toList();
    }

    org.apache.bookkeeper.mledger.SourceSafeLedgerOffloader.PreferredReadOperation readPreferred(
            long ledgerId,
            long firstEntry,
            long lastEntry,
            org.apache.bookkeeper.mledger.SourceSafeLedgerOffloader.NativeReadRange retained,
            java.util.function.BooleanSupplier sourceUseOpen) {
        return preferredReads.read(ledgerId, firstEntry, lastEntry, retained, sourceUseOpen);
    }

    /** Metadata-only capture; fully replaced BK handles are never reopened to prepare their Native INTENT. */
    public org.apache.bookkeeper.mledger.impl.ManagedLedgerImpl.NativeBookKeeperSourceDeletion
            captureBookKeeperDeletion(long ledgerId) {
        return nativeView.captureBookKeeperDeletion(ledgerId);
    }

    CompletableFuture<Void> closePreferredReads() {
        return preferredReads == null ? CompletableFuture.completedFuture(null) : preferredReads.close();
    }

    /** Conservative temporary read allowance: SDK frames, NPD codec copies, range output and owned copy work. */
    PulsarNativeReadCellBudgetV1.Usage readCharge(PulsarNativeReadCellBudgetV1.ReadLimits limits, int capacity) {
        long frames = Math.multiplyExact((long) limits.maximumEntries(), bookKeeperFrameBytes);
        long encodedWork = 0;
        long decodedWork = 0;
        for (var slot : slots) {
            if (slot.offloaded() == null) {
                continue;
            }
            var bounds = slot.offloaded().memoryBounds();
            long directory = Math.multiplyExact((long) bounds.maximumBlockEntries(), 16);
            // NPD arrays include ciphertext/plaintext, directory/compressed/decoded and EntryPayload copies.
            long work = Math.multiplyExact(
                    8,
                    Math.addExact(
                            directory,
                            Math.addExact(bounds.maximumEncodedBlockBytes(), bounds.maximumDecodedBlockBytes())));
            long output = Math.min(
                    slot.sealedLength(),
                    Math.multiplyExact((long) limits.maximumEntries(), bounds.maximumDecodedBlockBytes()));
            encodedWork = Math.max(encodedWork, Math.multiplyExact(8, bounds.maximumEncodedBlockBytes()));
            decodedWork = Math.max(decodedWork, Math.addExact(work, Math.multiplyExact(4, output)));
        }
        // Sum BK and Object allowances: an unsuccessful Object range can precede full-range BK fallback.
        long encoded = Math.addExact(frames, encodedWork);
        long decoded = Math.addExact(
                Math.addExact(Math.multiplyExact(4, frames), decodedWork),
                Math.multiplyExact(2, limits.maximumOwnedBatchBytes()));
        if (mapped != null) {
            decoded = Math.addExact(decoded, mapped.decodedWorkspaceBytes() + 4L * limits.maximumOwnedBatchBytes());
        }
        return new PulsarNativeReadCellBudgetV1.Usage(
                1, capacity, Math.multiplyExact(capacity, encoded), Math.multiplyExact(capacity, decoded));
    }

    public BindingIdentity bindingIdentity() {
        return binding.bindingIdentity();
    }

    public List<Long> ledgerIds() {
        return nativeView.ledgerIds();
    }

    public Position confirmedPosition() {
        return nativeView.confirmedPosition();
    }

    public List<PhysicalResourceIdV2> members() {
        return members;
    }

    public CanonicalBytes canonicalBytes() {
        return canonicalBytes;
    }

    public Sha256Digest sha256() {
        return sha256;
    }

    public boolean isCurrent() {
        return nativeView.isCurrent()
                && slots.stream()
                        .allMatch(slot -> slot.offloaded() != null
                                ? slot.offloaded().nativeSource().isCurrent()
                                : slot.plain().isCurrent());
    }

    List<Slot> materializationDirectory() {
        if (!isCurrent()) {
            throw new IllegalStateException("Native materialization directory is stale");
        }
        return slots;
    }

    void requireSuccessorMapCapacity() {
        if (mapped != null) {
            mapped.requireSuccessorCapacity();
        }
    }

    List<com.nereusstream.storage.object.materialization.M5PulsarCoverageV2.ChainLedger> materializationChain() {
        materializationDirectory();
        return nativeView.ledgerIds().stream()
                .map(id -> {
                    var nativeSource = routes.get(id);
                    if (nativeSource != null) {
                        return new com.nereusstream.storage.object.materialization.M5PulsarCoverageV2.ChainLedger(
                                id,
                                nativeSource.activeTail(),
                                Math.addExact(nativeSource.lastEntry(), 1),
                                Sha256Digest.hash(nativeSource.encode()));
                    }
                    var old = mapped.plan().sourceCut().coverage().pulsarCoverage().orElseThrow().chain().stream()
                            .filter(ledger -> ledger.ledgerId() == id)
                            .findFirst()
                            .orElseThrow();
                    if (old.active() || !mapped.fullyReplaces(id, old.entryEndExclusive() - 1)) {
                        throw new IllegalStateException("Native materialization omitted a retained source");
                    }
                    return new com.nereusstream.storage.object.materialization.M5PulsarCoverageV2.ChainLedger(
                            id,
                            false,
                            old.entryEndExclusive(),
                            PulsarNativeMaterializationReadMapV2.mappedLedgerRoot(sha256, id));
                })
                .toList();
    }

    java.util.Optional<com.nereusstream.storage.object.gc.M5ObjectBirthCreditV3> materializedOutput(
            com.nereusstream.storage.object.materialization.M5PulsarCoverageV2.Position position) {
        if (mapped == null
                || mapped.plan().sourceCut().coverage().pulsarCoverage().orElseThrow().chain().stream()
                        .noneMatch(ledger -> ledger.ledgerId() == position.ledgerId()
                                && position.entryId() < ledger.entryEndExclusive())) {
            return java.util.Optional.empty();
        }
        var resource = mapped.locate(position).rewrittenPayload();
        return resource.flatMap(value -> mapped.readableOutputs().stream()
                .filter(credit -> credit.resource().equals(java.util.Optional.of(value)))
                .findFirst());
    }

    /** Existing selected-map descriptor; reading a source identity does not decode another recipe copy. */
    java.util.Optional<com.nereusstream.storage.object.gc.M5ObjectBirthCreditV3> materializedOutput(
            PhysicalResourceIdV2 resource) {
        return mapped == null
                ? java.util.Optional.empty()
                : mapped.readableOutputs().stream()
                        .filter(value -> value.resource().equals(java.util.Optional.of(resource)))
                        .findFirst();
    }

    List<com.nereusstream.storage.object.materialization.M5PulsarCoverageV2.EntryRange> materializationSlices(
            List<com.nereusstream.storage.object.materialization.M5PulsarCoverageV2.EntryRange> requested) {
        return requested.stream()
                .flatMap(range -> (mapped == null ? List.of(range) : mapped.splitAtSourceBoundaries(range)).stream())
                .toList();
    }

    /** Physical data read dependencies for one slice already split at the selected map's source boundaries. */
    List<PhysicalResourceIdV2> physicalDataReferences(
            com.nereusstream.storage.object.materialization.M5PulsarCoverageV2.EntryRange range) {
        var ledger = materializationChain().stream()
                .filter(value -> value.ledgerId() == range.ledgerId())
                .findFirst()
                .orElseThrow();
        if (range.endEntryExclusive() > ledger.entryEndExclusive()
                || materializationSlices(List.of(range)).size() != 1) {
            throw new IllegalArgumentException("physical reference range crosses its captured source boundary");
        }
        return PulsarNativeM4SourceMapV1.capture(this)
                .physicalDataReferences(binding, bookKeeperBinding, objectBinding, range);
    }

    PulsarNativeMaterializedReadV2.SourceRead readMaterializationEntry(
            long ledger, long entry, java.util.function.BooleanSupplier gate) {
        return preferredReads.readInline(ledger, entry, gate);
    }

    CompletableFuture<Void> verifyMaterializationSources(
            M5BookKeeperNamespaceAuthorityV2 bookKeeper, NereusPulsarLedgerOffloaderV1 offloader) {
        return binding.verifyMaterializationView(this, bookKeeper, offloader)
                .thenCompose(ignored -> nativeView.verifyStoredMetadata());
    }

    CompletableFuture<org.apache.bookkeeper.mledger.impl.ManagedLedgerImpl.CapturedCursorCatalogue>
            captureStoredCursorCatalogue(int maximumCursors, long maximumBytes) {
        return nativeView.captureStoredCursorCatalogue(maximumCursors, maximumBytes);
    }

    org.apache.bookkeeper.mledger.impl.ManagedLedgerImpl.CapturedCursorCheckpointOperation captureCursorCheckpoints(
            int maximumCursors, int maximumBytes) {
        return nativeView.captureCursorCheckpoints(maximumCursors, maximumBytes);
    }

    boolean usesBookKeeper(M5BookKeeperNamespaceAuthorityV2 bookKeeper) {
        return nativeView.usesBookKeeper(bookKeeper.borrowClient());
    }

    PulsarNativeBindingV1 binding() {
        return binding;
    }

    PhysicalNamespaceAuthorityBindingV2 objectBinding() {
        return objectBinding;
    }

    com.nereusstream.storage.object.materialization.M5PulsarRecoveryInputV2 materializationRecoveryInput() {
        if (!isCurrent()) {
            throw new IllegalStateException("Native recovery input belongs to a stale captured view");
        }
        String name = nativeView.managedLedgerName();
        return new com.nereusstream.storage.object.materialization.M5PulsarRecoveryInputV2(
                binding.metadataNamespace(),
                binding.bindingIdentity(),
                name,
                "/managed-ledgers/" + name,
                nativeView.metadataVersion(),
                CanonicalBytes.copyOf(nativeView.storedMetadataBytes()));
    }

    Sha256Digest readAdmissionConfiguration(
            M5BookKeeperNamespaceAuthorityV2 bookKeeper, NereusPulsarLedgerOffloaderV1 offloader) {
        return readAdmissionConfiguration(nativeView, bookKeeper, offloader, bookKeeperBinding.metadataNamespace());
    }

    static Sha256Digest readAdmissionConfiguration(
            CapturedNativeReadView nativeView,
            M5BookKeeperNamespaceAuthorityV2 bookKeeper,
            NereusPulsarLedgerOffloaderV1 offloader,
            com.nereusstream.storage.api.lifecycle.MetadataNamespaceIdentityV2 metadataNamespace) {
        if (!nativeView.usesBookKeeper(bookKeeper.borrowClient()) || !nativeView.usesOffloader(offloader)) {
            throw new IllegalArgumentException("Native M4 configuration belongs to another owned SDK");
        }
        var values = new java.util.TreeMap<String, String>(nativeView.readAdmissionConfiguration());
        offloader.readAdmissionConfiguration().forEach((key, value) -> values.put("object." + key, value));
        values.put("bookKeeper", bookKeeper.capability().toString());
        values.put("bookKeeper.namespace", bookKeeper.namespace().toString());
        values.put("metadataNamespace", metadataNamespace.toString());
        return PulsarNativeM4DrainAdmissionV1.digest(values);
    }

    List<Sha256Digest> fallbackSourceIdentities() {
        return slots.stream()
                .filter(slot -> slot.offloaded() != null && !slot.offloaded().preferredOnly())
                .map(slot -> slot.offloaded().readView().sha256())
                .toList();
    }

    /** Exact existing physical source continuity; this is no general compaction semantic validator. */
    void requirePreferredReplacementOf(PulsarNativeBindingReadViewV1 predecessor) {
        if ((mapped == null) != (predecessor.mapped == null)
                || mapped != null && !mapped.encode().equals(predecessor.mapped.encode())) {
            throw new IllegalArgumentException("Native preferred successor changes its selected materialization");
        }
        if (!bindingIdentity().equals(predecessor.bindingIdentity())
                || !fallbackSourceIdentities().isEmpty()
                || !nativeView.managedLedgerProperties().equals(predecessor.nativeView.managedLedgerProperties())
                || confirmedPosition().compareTo(predecessor.confirmedPosition()) < 0) {
            throw new IllegalArgumentException("Native preferred successor differs from its Binding/read cut");
        }
        for (var old : predecessor.slots) {
            var next = routes.get(old.ledgerId());
            if (next == null && old.activeTail() && old.lastEntry() < 0) {
                // A real reopened ledger can discard the old empty tail; it contained no admitted entry.
                continue;
            }
            if (next == null
                    || next.lastEntry() < old.lastEntry()
                    || !next.bookKeeperBinding().equals(old.bookKeeperBinding())) {
                throw new IllegalArgumentException("Native preferred successor drops an admitted source extent");
            }
            if (old.offloaded() != null) {
                var before = old.offloaded();
                var after = next.offloaded();
                if (after == null
                        || !after.preferredOnly()
                        || !before.objects().equals(after.objects())
                        || !before.physicalBinding().equals(after.physicalBinding())
                        || old.lastEntry() != next.lastEntry()
                        || old.sealedLength() != next.sealedLength()
                        || old.bookKeeperMember().filter(members::contains).isPresent()) {
                    throw new IllegalArgumentException(
                            "Native preferred successor changes the immutable Object source");
                }
            } else if (next.plain() == null
                    || !old.bookKeeperMember().equals(next.bookKeeperMember())
                    || (!old.activeTail()
                            && (old.lastEntry() != next.lastEntry() || old.sealedLength() != next.sealedLength()))) {
                throw new IllegalArgumentException("Native preferred successor changes a retained BK source");
            }
        }
    }

    void requireReadRange(long ledgerId, long firstEntry, long lastEntry) {
        Slot slot = routes.get(ledgerId);
        if (slot == null && mapped != null) {
            mapped.locate(new com.nereusstream.storage.object.materialization.M5PulsarCoverageV2.Position(
                    ledgerId, firstEntry));
            mapped.locate(new com.nereusstream.storage.object.materialization.M5PulsarCoverageV2.Position(
                    ledgerId, lastEntry));
            if (firstEntry < 0 || lastEntry < firstEntry) {
                throw new IllegalArgumentException("Native mapped read range is invalid");
            }
            return;
        }
        if (slot == null || firstEntry < 0 || lastEntry < firstEntry || lastEntry > slot.lastEntry()) {
            throw new IllegalArgumentException("Native M4 read is outside its exact captured frontier");
        }
    }

    void requireVerificationSource(long ledgerId) {
        var slot = routes.get(ledgerId);
        if (slot == null || slot.offloaded() == null) {
            throw new IllegalArgumentException("Native verification requires a captured Object source");
        }
    }

    public CapturedSourceVerification verifyOwnedObjectSource(long ledgerId) {
        try {
            requireVerificationSource(ledgerId);
            return routes.get(ledgerId).offloaded().nativeSource().verifyOwnedObjectSource();
        } catch (Throwable failure) {
            return CapturedSourceVerification.rejected(failure);
        }
    }

    void requirePublishedPlainCoverage(PulsarNativeM4SourceMapV1.PublishedSources published) {
        for (var source : published.plain().values()) {
            if (mapped != null && mapped.fullyReplaces(source.member().ledgerId(), source.lastEntry())) {
                continue;
            }
            if (source.lastEntry() < 0) {
                continue; // No entry range was admitted on a published empty writer tail.
            }
            var current = routes.get(source.member().ledgerId());
            if (current == null
                    || current.plain() == null
                    || !current.bookKeeperMember().orElseThrow().equals(source.member())
                    || current.lastEntry() < source.lastEntry()
                    || (source.sealedLength().isPresent()
                            && (current.activeTail()
                                    || current.lastEntry() != source.lastEntry()
                                    || current.sealedLength()
                                            != source.sealedLength().orElseThrow()))) {
                throw new IllegalStateException("Native cold cut dropped or changed a published BK data extent");
            }
        }
    }

    public CompletableFuture<CapturedReadBatch> readOwnedAsync(
            long ledgerId, long firstEntry, long lastEntry, CapturedReadLimits limits, BooleanSupplier sourceUseOpen) {
        return readOwnedOperation(ledgerId, firstEntry, lastEntry, limits, sourceUseOpen)
                .result();
    }

    public CapturedReadOperation readOwnedOperation(
            long ledgerId, long firstEntry, long lastEntry, CapturedReadLimits limits, BooleanSupplier sourceUseOpen) {
        Slot slot = routes.get(ledgerId);
        if (slot == null) {
            return CapturedReadOperation.rejected(
                    new IllegalArgumentException("ledger is outside native Binding view"));
        }
        return slot.offloaded() != null
                ? slot.offloaded().readView().readOwnedOperation(ledgerId, firstEntry, lastEntry, limits, sourceUseOpen)
                : slot.plain().readOwnedOperation(firstEntry, lastEntry, limits, sourceUseOpen);
    }

    /** Actual Native read fencing still needs its durable M4 owner terminal/proof before RELEASED. */
    public CompletableFuture<Void> fenceOwnerReadAdmission() {
        return nativeView.fenceOwnerReadAdmission();
    }

    public void installReadAdmission(NativeReadAdmission admission) {
        nativeView.installReadAdmission(admission);
    }

    void attachPreparation(PulsarNativeSourcePreparationV1 value) {
        if (preparation != null) {
            throw new IllegalStateException("Native view already belongs to a source preparation");
        }
        preparation = value;
    }

    void requirePreparationBudget(PulsarNativeReadCellBudgetV1 budget) {
        if (preparation == null) {
            throw new IllegalStateException("Native M4 owner requires its actual source preparation");
        }
        if (preparation.budget() != budget) {
            throw new IllegalArgumentException("Native owner differs from its exact source preparation budget");
        }
    }

    Namespace objectNamespace() {
        return objectBinding.physicalNamespace();
    }

    CompletableFuture<Void> closePreparation() {
        return closePreferredReads()
                .thenCompose(
                        ignored -> preparation == null ? CompletableFuture.completedFuture(null) : preparation.close());
    }

    CompletableFuture<Void> closeUnregisteredPreparation() {
        return java.util.Objects.requireNonNull(preparation, "original source preparation")
                .closeUnregisteredOwner();
    }

    void requireUnregisteredPreparationDrain() {
        java.util.Objects.requireNonNull(preparation, "original source preparation")
                .requireUnregisteredDrain();
    }

    private CanonicalBytes encode(PhysicalNamespaceAuthorityBindingV2 objects) {
        try {
            var bytes = new ByteArrayOutputStream();
            try (var out = new DataOutputStream(bytes)) {
                out.writeInt(0x4e504256); // NPBV
                out.writeInt(1);
                writeBytes(
                        out,
                        CanonicalUtf8.fromString("NEREUS_V2_NATIVE_PULSAR_BINDING_READ_VIEW_V1")
                                .bytes());
                var identity = binding.bindingIdentity();
                out.write(identity.bindingId().digest().bytes().toByteArray());
                out.write(identity.incarnationSha256().bytes().toByteArray());
                out.write(identity.storageEpochSha256().bytes().toByteArray());
                var p1 = binding.authority();
                out.write(p1.aggregate().canonicalStoredDigest().bytes().toByteArray());
                writeBytes(out, p1.aggregate().aggregateSourceVersion().value());
                writeBytes(out, p1.selector().metadataVersion().value());
                writeBytes(out, objects.encode());
                out.writeLong(nativeView.metadataVersion());
                writeBytes(out, CanonicalBytes.copyOf(nativeView.storedMetadataBytes()));
                out.writeLong(confirmedPosition().getLedgerId());
                out.writeLong(confirmedPosition().getEntryId());
                out.writeInt(slots.size());
                for (var slot : slots) {
                    out.writeLong(slot.ledgerId());
                    out.writeByte(slot.offloaded() == null ? 1 : 2);
                    writeBytes(out, slot.encode());
                }
            }
            return CanonicalBytes.copyOf(bytes.toByteArray());
        } catch (IOException failure) {
            throw new IllegalStateException("cannot encode native Binding read view", failure);
        }
    }

    private static void writeBytes(DataOutputStream out, CanonicalBytes bytes) throws IOException {
        out.writeInt(bytes.length());
        out.write(bytes.toByteArray());
    }
}
