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
import com.nereusstream.storage.api.lifecycle.MetadataNamespaceIdentityV2;
import com.nereusstream.storage.api.lifecycle.PhysicalResourceIdCodecV2;
import com.nereusstream.storage.api.lifecycle.PhysicalResourceIdV2;
import com.nereusstream.storage.object.materialization.M5PulsarCoverageV2;
import com.nereusstream.storage.object.read.control.M4ReadControlRecordsV1.BindingIdentity;
import com.nereusstream.storage.object.retention.M5RetentionEvidenceAssemblerV1.ReferenceAdapterV1;
import com.nereusstream.storage.object.retention.M5RetentionRecordsV1.ReferenceKindV1;
import java.io.ByteArrayOutputStream;
import java.io.DataOutputStream;
import java.io.IOException;
import java.util.ArrayList;
import java.util.List;
import java.util.Optional;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.Executor;
import org.apache.bookkeeper.mledger.impl.ManagedLedgerImpl.CapturedCursorCatalogue;
import org.apache.bookkeeper.mledger.impl.ManagedLedgerImpl.CapturedCursorCheckpoint;
import org.apache.bookkeeper.mledger.impl.ManagedLedgerImpl.CapturedCursorCheckpointOperation;
import org.apache.bookkeeper.mledger.impl.ManagedLedgerImpl.CapturedStoredCursor;
import org.apache.bookkeeper.mledger.proto.ManagedCursorInfo;
import org.apache.bookkeeper.mledger.proto.PositionInfo;

/** Actual versioned Native cursor metadata. Stable rereads alone are not a complete reference-free proof. */
public final class PulsarNativeCursorMetadataV2 implements AutoCloseable {
    public static final int MAX_CURSORS = 4096;
    public static final int MAX_BYTES = 8_388_608;
    private final PulsarNativeM4ReadOwnerV1 owner;
    private final PulsarNativeBindingReadViewV1 view;
    private final PulsarNativeReadCellBudgetV1.Reservation workspace;
    private final boolean ownsWorkspace;
    private final CompletableFuture<Void> ended = new CompletableFuture<>();
    private CapturedCursorCatalogue catalogue;
    private List<CapturedCursorCheckpoint> checkpoints;
    private List<M5PulsarCoverageV2.ChainLedger> chain;
    private List<CursorFloor> cursorFloors;
    private List<CheckpointReference> checkpointReferences;
    private List<DataReference> dataReferences;
    private volatile CapturedCursorCheckpointOperation nativeOperation;
    private CanonicalBytes bytes;
    private boolean started;
    private volatile boolean closed;
    private CompletableFuture<Void> closing;
    private PulsarNativeCursorMetadataV2 validationSnapshot;
    private CompletableFuture<Void> validation;

    private PulsarNativeCursorMetadataV2(PulsarNativeM4ReadOwnerV1 owner) {
        this.owner = owner;
        view = owner.materializationView();
        workspace = owner.cellBudget().reserveCursorObservation(view.binding(), MAX_BYTES);
        ownsWorkspace = true;
    }

    private PulsarNativeCursorMetadataV2(PulsarNativeCursorMetadataV2 retained) {
        owner = retained.owner;
        view = retained.view;
        workspace = retained.workspace;
        ownsWorkspace = false;
    }

    public static CompletableFuture<PulsarNativeCursorMetadataV2> capture(
            PulsarNativeM4ReadOwnerV1 owner, Executor control) {
        final PulsarNativeCursorMetadataV2 original;
        try {
            original = new PulsarNativeCursorMetadataV2(owner);
        } catch (Throwable failure) {
            return CompletableFuture.failedFuture(failure);
        }
        return captureOriginal(original, control);
    }

    private static CompletableFuture<PulsarNativeCursorMetadataV2> captureOriginal(
            PulsarNativeCursorMetadataV2 original, Executor control) {
        var owner = original.owner;
        var observer = new CompletableFuture<PulsarNativeCursorMetadataV2>();
        original.workspace
                .ready()
                .thenRunAsync(
                        () -> {
                            CompletableFuture<Void> backend = CompletableFuture.completedFuture(null);
                            try {
                                if (observer.isDone()) {
                                    throw new java.util.concurrent.CancellationException(
                                            "cursor observer ended before dispatch");
                                }
                                if (original.ownsWorkspace) {
                                    original.workspace.sourceStarting();
                                }
                                original.started = true;
                                var cleanup = new CompletableFuture<Void>();
                                owner.admitMetadataObservation(cleanup)
                                        .toCompletableFuture()
                                        .get();
                                try {
                                    owner.requireMaterializationCurrent();
                                    original.nativeOperation =
                                            original.view.captureCursorCheckpoints(MAX_CURSORS, MAX_BYTES);
                                    backend = original.nativeOperation.drained();
                                    var captured =
                                            original.nativeOperation.result().get();
                                    original.catalogue = captured.metadata();
                                    original.checkpoints = captured.checkpoints();
                                    owner.requireMaterializationCurrent();
                                    original.chain = original.view.materializationChain();
                                    original.cursorFloors = original.deriveCursorFloors();
                                    original.checkpointReferences = original.deriveCheckpointReferences();
                                    original.dataReferences = original.deriveDataReferences();
                                    if (original.closed) {
                                        throw new IllegalStateException("original cursor metadata capture is closed");
                                    }
                                    original.bytes = original.encode();
                                } finally {
                                    backend.thenRun(() -> cleanup.complete(null));
                                }
                                backend.get();
                                original.ended.complete(
                                        null); // Precedes callbacks that synchronously close this source.
                                if (!observer.complete(original)) {
                                    original.retainUndelivered();
                                }
                            } catch (Throwable failure) {
                                backend.thenRun(() -> original.ended.complete(null));
                                original.retainUndelivered();
                                observer.completeExceptionally(failure);
                            }
                        },
                        control)
                .whenComplete((unused, failure) -> {
                    if (failure != null) {
                        original.ended.complete(null);
                        original.retainUndelivered();
                        observer.completeExceptionally(failure);
                    }
                });
        observer.whenComplete((value, failure) -> {
            if (observer.isCancelled()) {
                original.retainUndelivered();
            }
        });
        return observer;
    }

    /** One fresh reread within this retained workspace; no cached observation grants an absence or delete fact. */
    public synchronized CompletableFuture<Void> revalidate(Executor control) {
        try {
            requireOpen();
            java.util.Objects.requireNonNull(control, "control executor");
            if (validation != null
                    && (!validation.isDone()
                            || validationSnapshot.closing == null
                            || !validationSnapshot.closing.isDone()
                            || validationSnapshot.closing.isCompletedExceptionally())) {
                return validation.copy();
            }
            var fresh = new PulsarNativeCursorMetadataV2(this);
            validationSnapshot = fresh;
            validation = captureOriginal(fresh, control)
                    .handle((observed, captureFailure) -> {
                        Throwable failure = captureFailure;
                        if (failure == null) {
                            try {
                                requireOpen();
                                owner.requireMaterializationCurrent();
                                if (!bytes.equals(observed.bytes)) {
                                    throw new IllegalStateException("actual Native cursor observation changed");
                                }
                            } catch (Throwable mismatch) {
                                failure = mismatch;
                            }
                        }
                        return failure;
                    })
                    .thenCompose(failure -> fresh.closeAsync().handle((unused, cleanupFailure) -> {
                        if (cleanupFailure != null) {
                            throw new java.util.concurrent.CompletionException(cleanupFailure);
                        }
                        if (failure != null) {
                            throw new java.util.concurrent.CompletionException(failure);
                        }
                        return (Void) null;
                    }));
            return validation.copy();
        } catch (Throwable failure) {
            return CompletableFuture.failedFuture(failure);
        }
    }

    public BindingIdentity binding() {
        return view.bindingIdentity();
    }

    public MetadataNamespaceIdentityV2 metadataNamespace() {
        return view.binding().metadataNamespace();
    }

    public List<CapturedStoredCursor> cursors() {
        requireOpen();
        return catalogue.cursors();
    }

    public ManagedCursorInfo cursorInfo(CapturedStoredCursor cursor) throws IOException {
        requireOpen();
        if (!catalogue.cursors().contains(cursor)) {
            throw new IllegalArgumentException("cursor row belongs to another actual catalogue");
        }
        var info = new ManagedCursorInfo();
        info.parseFrom(cursor.cursorInfoBytes());
        return info;
    }

    public List<CapturedCursorCheckpoint> checkpoints() {
        requireOpen();
        return checkpoints;
    }

    /** Actual cursor checkpoint files only; logical data floors and the other reference classes remain separate. */
    public record CheckpointReference(
            String cursorName,
            long cursorMetadataVersion,
            Sha256Digest cursorStoredBytesSha256,
            PhysicalResourceIdV2.BookKeeperLedger resource,
            long checkpointEntryId,
            Sha256Digest positionInfoSha256) {}

    public List<CheckpointReference> checkpointReferences() {
        requireOpen();
        return checkpointReferences;
    }

    /** Union of captured cursor data obligations; selected manifest/index references are independent classes. */
    public record DataReference(M5PulsarCoverageV2.EntryRange range, List<PhysicalResourceIdV2> resources) {
        public DataReference {
            java.util.Objects.requireNonNull(range, "retained data range");
            resources = List.copyOf(resources);
            if (resources.isEmpty()
                    || !resources.equals(resources.stream().sorted().distinct().toList())) {
                throw new IllegalArgumentException("retained data resources are not canonical");
            }
        }
    }

    public List<DataReference> dataReferences() {
        requireOpen();
        return dataReferences;
    }

    /** Actual bounded chain context; this supplies no policy floor or complete reference absence. */
    public M5PulsarCoverageV2 referenceCoverage() {
        requireOpen();
        var ranges = chain.stream()
                .filter(ledger -> ledger.entryEndExclusive() > 0)
                .map(ledger -> new M5PulsarCoverageV2.EntryRange(ledger.ledgerId(), 0, ledger.entryEndExclusive()))
                .toList();
        return M5PulsarCoverageV2.of(binding(), view.sha256(), chain, ranges);
    }

    /**
     * Native physical-reference veto producer. Every call rereads the original catalogue/checkpoints and keeps
     * their existing SDK workspace. A missing reference is not absence without closed writers/directory policy.
     */
    public ReferenceAdapterV1 referenceAdapter(ReferenceKindV1 kind, Executor control) {
        requireOpen();
        if (kind != ReferenceKindV1.PULSAR_SUBSCRIPTION_OR_REPLICATION_CURSOR
                && kind != ReferenceKindV1.RECOVERY_CHECKPOINT_OR_SNAPSHOT) {
            throw new IllegalArgumentException("Native cursor adapter does not own this reference kind");
        }
        java.util.Objects.requireNonNull(control, "control executor");
        return request -> {
            try {
                request.coverage().requireCurrentPulsar();
                if (!request.identity().binding().equals(binding())) {
                    throw new IllegalArgumentException("Native cursor reference request belongs to another Binding");
                }
                return scanReferences(
                        kind,
                        request.targetIdentitySha256(),
                        request.coverage().pulsarCoverage().orElseThrow(),
                        control);
            } catch (Throwable failure) {
                return CompletableFuture.failedFuture(failure);
            }
        };
    }

    /** Direct Native scan query, without inventing floor/trim/grace facts needed by a full proof request. */
    public CompletableFuture<
                    com.nereusstream.storage.object.retention.M5RetentionEvidenceAssemblerV1.ReferenceAdapterResultV1>
            scanReferences(ReferenceKindV1 kind, Sha256Digest target, M5PulsarCoverageV2 coverage, Executor control) {
        try {
            requireOpen();
            java.util.Objects.requireNonNull(target, "target");
            java.util.Objects.requireNonNull(control, "control executor");
            if (kind != ReferenceKindV1.PULSAR_SUBSCRIPTION_OR_REPLICATION_CURSOR
                    && kind != ReferenceKindV1.RECOVERY_CHECKPOINT_OR_SNAPSHOT) {
                throw new IllegalArgumentException("Native cursor adapter does not own this reference kind");
            }
            if (!referenceCoverage().contains(coverage)) {
                throw new IllegalArgumentException(
                        "Native cursor reference request differs from its actual Binding/chain");
            }
            return revalidate(control).thenApply(unused -> {
                requireOpen();
                List<PhysicalResourceIdV2> present;
                if (kind == ReferenceKindV1.RECOVERY_CHECKPOINT_OR_SNAPSHOT) {
                    present = checkpointReferences.stream()
                            .map(CheckpointReference::resource)
                            .filter(resource -> resource.sha256().equals(target))
                            .map(PhysicalResourceIdV2.class::cast)
                            .distinct()
                            .sorted()
                            .toList();
                } else {
                    if (cursorFloors.isEmpty() || cursorFloors.stream().anyMatch(floor -> !floor.positionResolved())) {
                        throw new IllegalStateException("Native cursor data reference position/policy is unresolved");
                    }
                    present = dataReferences.stream()
                            .filter(reference -> coverage.ranges().stream()
                                    .anyMatch(range -> range.ledgerId()
                                                    == reference.range().ledgerId()
                                            && range.startEntryInclusive()
                                                    < reference.range().endEntryExclusive()
                                            && reference.range().startEntryInclusive() < range.endEntryExclusive()))
                            .flatMap(reference -> reference.resources().stream())
                            .filter(resource -> resource.sha256().equals(target))
                            .distinct()
                            .sorted()
                            .toList();
                }
                if (!present.isEmpty()) {
                    throw new NativeReferencePresentException(kind, target, coverage, canonicalBytes(), present);
                }
                throw new IllegalStateException(
                        "Native cursor reference absence requires closed writer/directory evidence");
            });
        } catch (Throwable failure) {
            return CompletableFuture.failedFuture(failure);
        }
    }

    /** Actual Native veto bytes/resource witnesses, never a caller-constructed absence or deletion grant. */
    public static final class NativeReferencePresentException extends IllegalStateException {
        private final ReferenceKindV1 kind;
        private final Sha256Digest target;
        private final M5PulsarCoverageV2 coverage;
        private final CanonicalBytes nativeSnapshot;
        private final List<PhysicalResourceIdV2> resources;

        private NativeReferencePresentException(
                ReferenceKindV1 kind,
                Sha256Digest target,
                M5PulsarCoverageV2 coverage,
                CanonicalBytes nativeSnapshot,
                List<PhysicalResourceIdV2> resources) {
            super("actual Native cursor/checkpoint reference is present");
            this.kind = kind;
            this.target = target;
            this.coverage = coverage;
            this.nativeSnapshot = nativeSnapshot;
            this.resources = List.copyOf(resources);
        }

        public ReferenceKindV1 kind() {
            return kind;
        }

        public Sha256Digest target() {
            return target;
        }

        public M5PulsarCoverageV2 coverage() {
            return coverage;
        }

        public CanonicalBytes nativeSnapshot() {
            return nativeSnapshot;
        }

        public List<PhysicalResourceIdV2> resources() {
            return resources;
        }
    }

    private List<DataReference> deriveDataReferences() {
        Optional<M5PulsarCoverageV2.Position> minimum =
                cursorFloors.isEmpty() || cursorFloors.stream().anyMatch(floor -> !floor.positionResolved())
                        ? Optional.empty()
                        : cursorFloors.stream().map(CursorFloor::firstRequired).min(this::compareInCapturedChain);
        var result = new ArrayList<DataReference>();
        long encodedBytes = 4;
        for (var ledger : chain) {
            long start = 0;
            if (minimum.isPresent()) {
                var floor = minimum.orElseThrow();
                if (floor.ledgerId() == ledger.ledgerId()) {
                    start = floor.entryId();
                } else if (compareInCapturedChain(floor, new M5PulsarCoverageV2.Position(ledger.ledgerId(), 0)) > 0) {
                    continue;
                }
            }
            if (start >= ledger.entryEndExclusive()) {
                continue;
            }
            var retained = new M5PulsarCoverageV2.EntryRange(ledger.ledgerId(), start, ledger.entryEndExclusive());
            for (var slice : view.materializationSlices(List.of(retained))) {
                var resources = view.physicalDataReferences(slice);
                encodedBytes = Math.addExact(encodedBytes, 28);
                for (var resource : resources) {
                    encodedBytes = Math.addExact(
                            encodedBytes,
                            4L + PhysicalResourceIdCodecV2.encode(resource).length());
                }
                if (encodedBytes > MAX_BYTES) {
                    throw new IllegalArgumentException("cursor physical reference data exceeds its admitted cap");
                }
                result.add(new DataReference(slice, resources));
            }
        }
        return List.copyOf(result);
    }

    private List<CheckpointReference> deriveCheckpointReferences() throws IOException {
        var references = new ArrayList<CheckpointReference>();
        for (int i = 0; i < checkpoints.size(); i++) {
            var checkpoint = checkpoints.get(i);
            var row = catalogue.cursors().get(i);
            var info = new ManagedCursorInfo();
            info.parseFrom(row.cursorInfoBytes());
            if (!row.cursorName().equals(checkpoint.cursorName())
                    || info.getCursorsLedgerId() != checkpoint.cursorLedgerId()) {
                throw new IllegalStateException("cursor checkpoint differs from its actual stored row");
            }
            if (checkpoint.cursorLedgerId() >= 0) {
                if (checkpoint.checkpointEntryId() < 0 || checkpoint.positionInfoBytes().length == 0) {
                    throw new IllegalStateException("cursor checkpoint has no actual durable entry");
                }
                references.add(new CheckpointReference(
                        row.cursorName(),
                        row.metadataVersion(),
                        Sha256Digest.hash(CanonicalBytes.copyOf(row.storedBytes())),
                        new PhysicalResourceIdV2.BookKeeperLedger(
                                view.bookKeeperNamespaceBinding().physicalNamespace(), checkpoint.cursorLedgerId()),
                        checkpoint.checkpointEntryId(),
                        Sha256Digest.hash(CanonicalBytes.copyOf(checkpoint.positionInfoBytes()))));
            }
        }
        return List.copyOf(references);
    }

    /** Original BK bytes when cursorLedgerId is nonnegative; metadata-only cursors use ManagedCursorInfo. */
    public PositionInfo positionInfo(CapturedCursorCheckpoint checkpoint) throws IOException {
        requireOpen();
        if (!checkpoints.contains(checkpoint) || checkpoint.cursorLedgerId() < 0) {
            throw new IllegalArgumentException("checkpoint does not carry an actual BK PositionInfo");
        }
        var info = new PositionInfo();
        info.parseFrom(checkpoint.positionInfoBytes());
        return info;
    }

    /** First entry still required by a captured durable cursor; unresolved positions conservatively retain all. */
    public record CursorFloor(String cursorName, M5PulsarCoverageV2.Position firstRequired, boolean positionResolved) {}

    public List<CursorFloor> cursorFloors() {
        requireOpen();
        return cursorFloors;
    }

    /** Empty requires a separately authoritative no-cursor policy; it never implies unconstrained retention. */
    public Optional<M5PulsarCoverageV2.Position> minimumCursorFloor() {
        requireOpen();
        if (cursorFloors.isEmpty() || cursorFloors.stream().anyMatch(floor -> !floor.positionResolved())) {
            return Optional.empty();
        }
        return cursorFloors.stream().map(CursorFloor::firstRequired).min(this::compareInCapturedChain);
    }

    /** A conservative veto observation only, not a complete reference-free or logical-trim grant. */
    public boolean cursorRetains(M5PulsarCoverageV2.EntryRange range) {
        requireOpen();
        var ledger = chain.stream()
                .filter(item -> item.ledgerId() == range.ledgerId())
                .findFirst()
                .orElseThrow();
        if (range.endEntryExclusive() > ledger.entryEndExclusive()) {
            throw new IllegalArgumentException("cursor range is outside the captured Native chain");
        }
        if (cursorFloors.isEmpty() || cursorFloors.stream().anyMatch(floor -> !floor.positionResolved())) {
            return true;
        }
        var end = new M5PulsarCoverageV2.Position(range.ledgerId(), range.endEntryExclusive());
        return cursorFloors.stream().anyMatch(floor -> compareInCapturedChain(floor.firstRequired(), end) < 0);
    }

    private List<CursorFloor> deriveCursorFloors() throws IOException {
        if (chain.isEmpty() || checkpoints.size() != catalogue.cursors().size()) {
            throw new IllegalStateException("actual cursor checkpoints differ from the Native chain/catalogue");
        }
        var rows = new ArrayList<CursorFloor>();
        var initial = new M5PulsarCoverageV2.Position(chain.get(0).ledgerId(), 0);
        for (int i = 0; i < checkpoints.size(); i++) {
            var checkpoint = checkpoints.get(i);
            var cursor = catalogue.cursors().get(i);
            if (!cursor.cursorName().equals(checkpoint.cursorName())) {
                throw new IllegalStateException("actual cursor checkpoint order differs");
            }
            long ledgerId;
            long entryId;
            if (checkpoint.cursorLedgerId() < 0) {
                var info = new ManagedCursorInfo();
                info.parseFrom(cursor.cursorInfoBytes());
                ledgerId = info.getMarkDeleteLedgerId();
                entryId = info.getMarkDeleteEntryId();
            } else {
                var info = new PositionInfo();
                info.parseFrom(checkpoint.positionInfoBytes());
                ledgerId = info.getLedgerId();
                entryId = info.getEntryId();
            }
            int ordinal = -1;
            for (int j = 0; j < chain.size(); j++) {
                if (chain.get(j).ledgerId() == ledgerId) {
                    ordinal = j;
                    break;
                }
            }
            if (ledgerId == -1 && entryId == -1) {
                rows.add(new CursorFloor(cursor.cursorName(), initial, true));
            } else if (ordinal < 0
                    || entryId < -1
                    || entryId >= chain.get(ordinal).entryEndExclusive()) {
                rows.add(new CursorFloor(cursor.cursorName(), initial, false));
            } else {
                long next = Math.addExact(entryId, 1);
                if (next == chain.get(ordinal).entryEndExclusive() && ordinal + 1 < chain.size()) {
                    ordinal++;
                    next = 0;
                }
                rows.add(new CursorFloor(
                        cursor.cursorName(),
                        new M5PulsarCoverageV2.Position(chain.get(ordinal).ledgerId(), next),
                        true));
            }
        }
        return List.copyOf(rows);
    }

    private int compareInCapturedChain(M5PulsarCoverageV2.Position left, M5PulsarCoverageV2.Position right) {
        int leftOrdinal = -1;
        int rightOrdinal = -1;
        for (int i = 0; i < chain.size(); i++) {
            if (chain.get(i).ledgerId() == left.ledgerId()) {
                leftOrdinal = i;
            }
            if (chain.get(i).ledgerId() == right.ledgerId()) {
                rightOrdinal = i;
            }
        }
        if (leftOrdinal < 0 || rightOrdinal < 0) {
            throw new IllegalArgumentException("cursor position is outside its captured Native chain");
        }
        return leftOrdinal == rightOrdinal
                ? Long.compare(left.entryId(), right.entryId())
                : Integer.compare(leftOrdinal, rightOrdinal);
    }

    public CanonicalBytes canonicalBytes() {
        requireOpen();
        return bytes;
    }

    public Sha256Digest sha256() {
        return Sha256Digest.hash(canonicalBytes());
    }

    private CanonicalBytes encode() throws IOException {
        var raw = new ByteArrayOutputStream();
        try (var out = new DataOutputStream(raw)) {
            out.writeInt(0x4e504353); // NPCS
            out.writeInt(4);
            blob(out, metadataNamespace().encode());
            blob(out, view.bookKeeperNamespaceBinding().encode());
            out.write(binding().bindingId().digest().bytes().toByteArray());
            out.write(binding().incarnationSha256().bytes().toByteArray());
            out.write(binding().storageEpochSha256().bytes().toByteArray());
            out.write(view.sha256().bytes().toByteArray());
            blob(out, CanonicalUtf8.fromString(catalogue.managedLedgerName()).bytes());
            out.writeLong(catalogue.metadataVersion());
            blob(out, CanonicalBytes.copyOf(catalogue.storedMetadataBytes()));
            out.writeInt(catalogue.cursors().size());
            for (var cursor : catalogue.cursors()) {
                blob(out, CanonicalUtf8.fromString(cursor.cursorName()).bytes());
                out.writeLong(cursor.metadataVersion());
                blob(out, CanonicalBytes.copyOf(cursor.storedBytes()));
                blob(out, CanonicalBytes.copyOf(cursor.cursorInfoBytes()));
                if (raw.size() > MAX_BYTES) {
                    throw new IllegalArgumentException("actual cursor metadata encoding exceeds cap");
                }
            }
            out.writeInt(checkpoints.size());
            for (var checkpoint : checkpoints) {
                blob(out, CanonicalUtf8.fromString(checkpoint.cursorName()).bytes());
                out.writeLong(checkpoint.cursorLedgerId());
                out.writeLong(checkpoint.checkpointEntryId());
                blob(out, CanonicalBytes.copyOf(checkpoint.positionInfoBytes()));
                if (raw.size() > MAX_BYTES) {
                    throw new IllegalArgumentException("actual cursor checkpoint encoding exceeds cap");
                }
            }
            out.writeInt(chain.size());
            for (var ledger : chain) {
                out.writeLong(ledger.ledgerId());
                out.writeBoolean(ledger.active());
                out.writeLong(ledger.entryEndExclusive());
                out.write(ledger.sourceIdentitySha256().bytes().toByteArray());
            }
            out.writeInt(cursorFloors.size());
            for (var floor : cursorFloors) {
                blob(out, CanonicalUtf8.fromString(floor.cursorName()).bytes());
                out.writeLong(floor.firstRequired().ledgerId());
                out.writeLong(floor.firstRequired().entryId());
                out.writeBoolean(floor.positionResolved());
            }
            out.writeInt(dataReferences.size());
            for (var reference : dataReferences) {
                out.writeLong(reference.range().ledgerId());
                out.writeLong(reference.range().startEntryInclusive());
                out.writeLong(reference.range().endEntryExclusive());
                out.writeInt(reference.resources().size());
                for (var resource : reference.resources()) {
                    blob(out, PhysicalResourceIdCodecV2.encode(resource));
                }
                if (raw.size() > MAX_BYTES) {
                    throw new IllegalArgumentException("actual cursor physical reference encoding exceeds cap");
                }
            }
            if (raw.size() > MAX_BYTES) {
                throw new IllegalArgumentException("actual cursor floor encoding exceeds cap");
            }
        }
        return CanonicalBytes.copyOf(raw.toByteArray());
    }

    private static void blob(DataOutputStream out, CanonicalBytes bytes) throws IOException {
        out.writeInt(bytes.length());
        out.write(bytes.toByteArray());
    }

    private synchronized void requireOpen() {
        if (closed || bytes == null) {
            throw new IllegalStateException("actual cursor catalogue is closed or unresolved");
        }
    }

    private void retainUndelivered() {
        if (ownsWorkspace) {
            workspace.retainUndeliveredSdk(this::closeAsync, false);
        }
        closeAsync();
    }

    public synchronized CompletableFuture<Void> closeAsync() {
        closed = true;
        if (closing == null || closing.isCompletedExceptionally()) {
            var operation = nativeOperation;
            var cleanup =
                    operation == null ? CompletableFuture.<Void>completedFuture(null) : operation.reconcileCleanup();
            closing = cleanup.thenCompose(unused -> ended)
                    .thenCompose(unused -> validationSnapshot == null
                            ? CompletableFuture.completedFuture(null)
                            : validationSnapshot.closeAsync())
                    .thenCompose(unused -> validation == null
                            ? CompletableFuture.completedFuture(null)
                            : validation.handle((value, failure) -> null))
                    .thenCompose(unused -> !ownsWorkspace
                            ? CompletableFuture.completedFuture(null)
                            : started ? workspace.releaseAfterDrain() : workspace.releaseBeforeActor());
        }
        return closing.copy();
    }

    @Override
    public void close() throws Exception {
        closeAsync().get();
    }
}
