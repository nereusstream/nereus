/* Licensed under the Apache License, Version 2.0 */
package com.nereusstream.managedledger.cursor;

import com.nereusstream.api.Checksum;
import com.nereusstream.api.ChecksumType;
import com.nereusstream.api.ObjectKey;
import com.nereusstream.api.keys.KeyComponentCodec;
import com.nereusstream.metadata.oxia.OxiaKeyspace;
import com.nereusstream.metadata.oxia.VersionedCursorState;
import com.nereusstream.metadata.oxia.codec.F3MetadataCodecs;
import com.nereusstream.metadata.oxia.codec.MetadataValueTooLargeException;
import com.nereusstream.metadata.oxia.records.CursorAckRangeRecord;
import com.nereusstream.metadata.oxia.records.CursorPartialBatchAckRecord;
import com.nereusstream.metadata.oxia.records.CursorRecordLifecycle;
import com.nereusstream.metadata.oxia.records.CursorSnapshotReferenceRecord;
import com.nereusstream.metadata.oxia.records.CursorStateRecord;
import java.util.ArrayList;
import java.util.List;
import java.util.Map;
import java.util.NavigableMap;
import java.util.Objects;
import java.util.Optional;
import java.util.OptionalLong;
import java.util.TreeMap;

/** Canonical root hydration plus inline/snapshot persistence selection. */
public final class CursorStatePersistencePlanner {
    private static final String PREFLIGHT_SNAPSHOT_ID = "00000000000000000000000000000000";

    private final String cluster;
    private final CursorStorageConfig config;

    public CursorStatePersistencePlanner(String cluster, CursorStorageConfig config) {
        this.cluster = new OxiaKeyspace(cluster).cluster();
        this.config = Objects.requireNonNull(config, "config");
    }

    public HydratedState hydrate(
            VersionedCursorState versioned,
            CursorLedgerIdentity expectedLedger,
            Optional<CursorAckState> snapshotBase) {
        Objects.requireNonNull(versioned, "versioned");
        Objects.requireNonNull(expectedLedger, "expectedLedger");
        snapshotBase = Objects.requireNonNull(snapshotBase, "snapshotBase");
        CursorStateRecord root = versioned.value();
        if (!root.projection().equals(expectedLedger.projection())) {
            throw corruption("cursor root projection does not match the ledger");
        }
        boolean referenced = root.snapshotReference().isPresent();
        if (referenced != snapshotBase.isPresent()) {
            throw corruption("cursor snapshot bytes do not match root reference presence");
        }
        CursorIdentity identity = new CursorIdentity(
                expectedLedger,
                root.cursorName(),
                root.cursorNameHash(),
                root.cursorGeneration());
        Optional<CursorSnapshotReference> reference =
                root.snapshotReference().map(CursorStatePersistencePlanner::fromRecord);
        CursorAckState effective;
        if (root.lifecycle() == CursorRecordLifecycle.DELETED) {
            effective = CursorAckState.empty(root.markDeleteOffset());
        } else {
            CursorAckState base = snapshotBase.orElseGet(
                    () -> CursorAckState.empty(root.markDeleteOffset()));
            effective = applyRootDelta(root, base);
        }
        return new HydratedState(
                versioned,
                new CursorState(
                        identity,
                        root.ownerSessionId(),
                        root.lifecycle() == CursorRecordLifecycle.ACTIVE
                                ? CursorLifecycle.ACTIVE
                                : CursorLifecycle.DELETED,
                        root.mutationSequence(),
                        root.ackStateEpoch(),
                        root.lastProtectionAttemptId(),
                        effective,
                        root.positionProperties(),
                        root.cursorProperties(),
                        reference,
                        root.createdAtMillis(),
                        root.updatedAtMillis(),
                        versioned.metadataVersion()),
                snapshotBase);
    }

    public PersistencePlan plan(HydratedState current, CursorState candidate) {
        Objects.requireNonNull(current, "current");
        Objects.requireNonNull(candidate, "candidate");
        requireSuccessor(current.state(), candidate);

        RootDelta delta = selectInlineDelta(current, candidate);
        CursorStateRecord inline = toRecord(
                candidate,
                candidate.snapshotReference(),
                delta.ranges(),
                delta.partials());
        int inlineEncodedBytes = encodedBytes(inline);
        int plannedRootMax = config.cursorMetadataValueMaxBytes()
                - config.cursorMetadataSafetyMarginBytes();
        if (delta.compatible() && delta.ranges().isEmpty() && delta.partials().isEmpty()) {
            if (inlineEncodedBytes > plannedRootMax) {
                throw new IllegalArgumentException(
                        "cursor root cannot fit the margin-adjusted metadata bound without ack state to spill");
            }
            return new InlinePlan(inline);
        }
        boolean thresholdExceeded = !delta.compatible()
                || inlineAckBytes(delta.ranges(), delta.partials())
                        > config.cursorInlineAckMaxBytes()
                || delta.ranges().size() + delta.partials().size()
                        > config.cursorInlineDeltaMaxCount()
                || inlineEncodedBytes > plannedRootMax;
        if (!thresholdExceeded) {
            return new InlinePlan(inline);
        }
        if (candidate.lifecycle() == CursorLifecycle.DELETED) {
            throw new IllegalArgumentException("deleted cursor tombstone exceeds the metadata bound");
        }

        CursorSnapshotWriteRequest request = new CursorSnapshotWriteRequest(
                candidate.identity(),
                candidate.mutationSequence(),
                candidate.acknowledgements(),
                candidate.updatedAtMillis());
        CursorSnapshotCodecV1.EncodedSnapshot preflight = CursorSnapshotCodecV1.encode(
                request, PREFLIGHT_SNAPSHOT_ID, config);
        CursorSnapshotReference placeholder = new CursorSnapshotReference(
                snapshotObjectKey(candidate.identity(), PREFLIGHT_SNAPSHOT_ID),
                PREFLIGHT_SNAPSHOT_ID,
                candidate.identity().cursorGeneration(),
                candidate.mutationSequence(),
                candidate.acknowledgements().markDeleteOffset(),
                preflight.objectLength(),
                preflight.storageChecksum(),
                preflight.formatCrc32c(),
                1,
                candidate.updatedAtMillis());
        CursorStateRecord referenced = toRecord(
                candidate, Optional.of(placeholder), List.of(), List.of());
        int referencedBytes = encodedBytes(referenced);
        if (referencedBytes > plannedRootMax) {
            throw new IllegalArgumentException(
                    "cursor root cannot fit the margin-adjusted metadata bound after snapshot spill");
        }
        return new SnapshotPlan(request);
    }

    public CursorStateRecord afterSnapshot(
            CursorState candidate, CursorSnapshotReference reference) {
        Objects.requireNonNull(candidate, "candidate");
        Objects.requireNonNull(reference, "reference");
        CursorStateRecord record = toRecord(
                candidate, Optional.of(reference), List.of(), List.of());
        if (encodedBytes(record) > config.cursorMetadataValueMaxBytes()) {
            throw new IllegalArgumentException("cursor snapshot root exceeds the metadata hard bound");
        }
        return record;
    }

    public CursorState persisted(
            CursorState candidate,
            Optional<CursorSnapshotReference> reference,
            long metadataVersion) {
        Objects.requireNonNull(candidate, "candidate");
        return new CursorState(
                candidate.identity(),
                candidate.ownerSessionId(),
                candidate.lifecycle(),
                candidate.mutationSequence(),
                candidate.ackStateEpoch(),
                candidate.lastProtectionAttemptId(),
                candidate.acknowledgements(),
                candidate.positionProperties(),
                candidate.cursorProperties(),
                Objects.requireNonNull(reference, "reference"),
                candidate.createdAtMillis(),
                candidate.updatedAtMillis(),
                metadataVersion);
    }

    public CursorStateRecord recordWithoutSnapshot(CursorState state) {
        Objects.requireNonNull(state, "state");
        return toRecord(
                state,
                Optional.empty(),
                toRangeRecords(state.acknowledgements().wholeAckRanges()),
                toPartialRecords(state.acknowledgements().partialBatchAcks()));
    }

    CursorSnapshotReference hydrateReference(CursorSnapshotReferenceRecord reference) {
        return fromRecord(Objects.requireNonNull(reference, "reference"));
    }

    private RootDelta selectInlineDelta(HydratedState current, CursorState candidate) {
        Optional<CursorSnapshotReference> currentReference = current.state().snapshotReference();
        if (currentReference.isEmpty()
                || candidate.snapshotReference().isEmpty()
                || !candidate.snapshotReference().equals(currentReference)) {
            return new RootDelta(
                    toRangeRecords(candidate.acknowledgements().wholeAckRanges()),
                    toPartialRecords(candidate.acknowledgements().partialBatchAcks()),
                    true);
        }
        CursorAckState base = current.snapshotBase().orElseThrow(
                () -> corruption("referenced cursor lacks hydrated snapshot base"));
        return deltaFromSnapshot(base, candidate.acknowledgements());
    }

    private static RootDelta deltaFromSnapshot(
            CursorAckState snapshot, CursorAckState candidate) {
        if (candidate.markDeleteOffset() < snapshot.markDeleteOffset()) {
            return RootDelta.incompatible();
        }
        CursorAckState clipped = clipAt(snapshot, candidate.markDeleteOffset());
        if (!baseIsSubsumed(clipped, candidate)) {
            return RootDelta.incompatible();
        }
        List<OffsetRange> addedRanges = subtractRanges(
                candidate.wholeAckRanges(), clipped.wholeAckRanges());
        List<CursorPartialBatchAckRecord> overrides = new ArrayList<>();
        for (Map.Entry<Long, BatchAckState> entry :
                candidate.partialBatchAcks().entrySet()) {
            BatchAckState base = clipped.partialBatchAcks().get(entry.getKey());
            if (!entry.getValue().equals(base)) {
                overrides.add(toPartialRecord(entry.getKey(), entry.getValue()));
            }
        }
        return new RootDelta(toRangeRecords(addedRanges), List.copyOf(overrides), true);
    }

    private static boolean baseIsSubsumed(CursorAckState base, CursorAckState candidate) {
        for (OffsetRange range : base.wholeAckRanges()) {
            if (!rangeCovered(candidate, range)) {
                return false;
            }
        }
        for (Map.Entry<Long, BatchAckState> entry : base.partialBatchAcks().entrySet()) {
            long offset = entry.getKey();
            if (candidate.isWholeEntryAcknowledged(offset)) {
                continue;
            }
            BatchAckState next = candidate.partialBatchAcks().get(offset);
            if (next == null
                    || next.batchSize() != entry.getValue().batchSize()
                    || !next.and(entry.getValue()).equals(next)) {
                return false;
            }
        }
        return true;
    }

    private static boolean rangeCovered(CursorAckState state, OffsetRange expected) {
        long cursor = expected.startOffset();
        if (cursor < state.markDeleteOffset()) {
            cursor = Math.min(expected.endOffset(), state.markDeleteOffset());
        }
        for (OffsetRange range : state.wholeAckRanges()) {
            if (range.endOffset() <= cursor) {
                continue;
            }
            if (range.startOffset() > cursor) {
                return false;
            }
            cursor = Math.max(cursor, range.endOffset());
            if (cursor >= expected.endOffset()) {
                return true;
            }
        }
        return cursor >= expected.endOffset();
    }

    private static List<OffsetRange> subtractRanges(
            List<OffsetRange> candidate, List<OffsetRange> base) {
        List<OffsetRange> result = new ArrayList<>();
        int baseIndex = 0;
        for (OffsetRange range : candidate) {
            long cursor = range.startOffset();
            while (baseIndex < base.size()
                    && base.get(baseIndex).endOffset() <= cursor) {
                baseIndex++;
            }
            int scan = baseIndex;
            while (scan < base.size()
                    && base.get(scan).startOffset() < range.endOffset()) {
                OffsetRange covered = base.get(scan);
                if (covered.startOffset() > cursor) {
                    result.add(new OffsetRange(
                            cursor, Math.min(covered.startOffset(), range.endOffset())));
                }
                cursor = Math.max(cursor, covered.endOffset());
                if (cursor >= range.endOffset()) {
                    break;
                }
                scan++;
            }
            if (cursor < range.endOffset()) {
                result.add(new OffsetRange(cursor, range.endOffset()));
            }
        }
        return List.copyOf(result);
    }

    private static CursorAckState applyRootDelta(
            CursorStateRecord root, CursorAckState snapshot) {
        if (snapshot.markDeleteOffset() > root.markDeleteOffset()) {
            throw corruption("snapshot mark-delete is ahead of the cursor root");
        }
        CursorAckState clipped = clipAt(snapshot, root.markDeleteOffset());
        if (clipped.markDeleteOffset() != root.markDeleteOffset()) {
            throw corruption("snapshot range should have been folded into the cursor root mark-delete");
        }
        List<OffsetRange> ranges = new ArrayList<>(clipped.wholeAckRanges());
        root.inlineWholeAckDeltas().forEach(range ->
                ranges.add(new OffsetRange(range.startOffset(), range.endOffset())));
        NavigableMap<Long, BatchAckState> partials =
                new TreeMap<>(clipped.partialBatchAcks());
        root.inlinePartialAckOverrides().forEach(partial -> partials.put(
                partial.entryOffset(),
                new BatchAckState(partial.batchSize(), partial.remainingWords())));
        CursorAckState effective = new CursorAckState(
                root.markDeleteOffset(), ranges, partials);
        if (effective.markDeleteOffset() != root.markDeleteOffset()) {
            throw corruption("cursor root mark-delete is not canonical with its snapshot delta");
        }
        return effective;
    }

    private static CursorAckState clipAt(CursorAckState state, long markDeleteOffset) {
        if (markDeleteOffset < state.markDeleteOffset()) {
            throw corruption("cannot clip snapshot behind its base mark-delete");
        }
        List<OffsetRange> ranges = new ArrayList<>();
        for (OffsetRange range : state.wholeAckRanges()) {
            if (range.endOffset() <= markDeleteOffset) {
                continue;
            }
            ranges.add(new OffsetRange(
                    Math.max(markDeleteOffset, range.startOffset()), range.endOffset()));
        }
        NavigableMap<Long, BatchAckState> partials =
                new TreeMap<>(state.partialBatchAcks().tailMap(markDeleteOffset, true));
        return new CursorAckState(markDeleteOffset, ranges, partials);
    }

    private CursorStateRecord toRecord(
            CursorState state,
            Optional<CursorSnapshotReference> reference,
            List<CursorAckRangeRecord> ranges,
            List<CursorPartialBatchAckRecord> partials) {
        boolean deleted = state.lifecycle() == CursorLifecycle.DELETED;
        return new CursorStateRecord(
                0,
                state.identity().ledger().projection(),
                state.ownerSessionId(),
                state.identity().cursorName(),
                state.identity().cursorNameHash(),
                state.identity().cursorGeneration(),
                deleted ? CursorRecordLifecycle.DELETED : CursorRecordLifecycle.ACTIVE,
                state.mutationSequence(),
                state.ackStateEpoch(),
                state.lastProtectionAttemptId(),
                state.acknowledgements().markDeleteOffset(),
                deleted ? Optional.empty() : reference.map(CursorStatePersistencePlanner::toRecord),
                deleted ? List.of() : ranges,
                deleted ? List.of() : partials,
                deleted ? Map.of() : state.positionProperties(),
                deleted ? Map.of() : state.cursorProperties(),
                state.createdAtMillis(),
                state.updatedAtMillis(),
                deleted ? OptionalLong.of(state.updatedAtMillis()) : OptionalLong.empty());
    }

    private int encodedBytes(CursorStateRecord record) {
        try {
            return F3MetadataCodecs.encodeEnvelope(record, CursorStateRecord.class).length;
        } catch (MetadataValueTooLargeException oversize) {
            return Math.addExact(config.cursorMetadataValueMaxBytes(), 1);
        }
    }

    private static long inlineAckBytes(
            List<CursorAckRangeRecord> ranges,
            List<CursorPartialBatchAckRecord> partials) {
        long bytes = Integer.BYTES * 2L + ranges.size() * (Long.BYTES * 2L);
        for (CursorPartialBatchAckRecord partial : partials) {
            bytes = Math.addExact(
                    bytes,
                    Long.BYTES + Integer.BYTES * 2L
                            + Math.multiplyExact((long) partial.remainingWords().length, Long.BYTES));
        }
        return bytes;
    }

    private ObjectKey snapshotObjectKey(CursorIdentity identity, String snapshotId) {
        return new ObjectKey(
                KeyComponentCodec.encodeComponent(cluster)
                        + "/cursor-snapshots/v1/"
                        + KeyComponentCodec.encodeComponent(identity.ledger().projection().streamId())
                        + "/"
                        + identity.cursorNameHash()
                        + "/"
                        + KeyComponentCodec.encodeNonNegativeLong(identity.cursorGeneration())
                        + "/"
                        + snapshotId
                        + ".ncs");
    }

    private static CursorSnapshotReference fromRecord(CursorSnapshotReferenceRecord reference) {
        return new CursorSnapshotReference(
                new ObjectKey(reference.objectKey()),
                reference.snapshotId(),
                reference.cursorGeneration(),
                reference.sourceMutationSequence(),
                reference.baseMarkDeleteOffset(),
                reference.objectLength(),
                new Checksum(
                        ChecksumType.valueOf(reference.storageChecksumType()),
                        reference.storageChecksumValue()),
                reference.formatCrc32c(),
                reference.formatVersion(),
                reference.createdAtMillis());
    }

    private static CursorSnapshotReferenceRecord toRecord(CursorSnapshotReference reference) {
        return reference.toMetadataRecord();
    }

    private static List<CursorAckRangeRecord> toRangeRecords(List<OffsetRange> ranges) {
        return ranges.stream()
                .map(range -> new CursorAckRangeRecord(range.startOffset(), range.endOffset()))
                .toList();
    }

    private static List<CursorPartialBatchAckRecord> toPartialRecords(
            NavigableMap<Long, BatchAckState> partials) {
        return partials.entrySet().stream()
                .map(entry -> toPartialRecord(entry.getKey(), entry.getValue()))
                .toList();
    }

    private static CursorPartialBatchAckRecord toPartialRecord(
            long offset, BatchAckState state) {
        return new CursorPartialBatchAckRecord(
                offset, state.batchSize(), state.remainingWords());
    }

    private static void requireSuccessor(CursorState current, CursorState candidate) {
        if (!candidate.identity().equals(current.identity())
                || !candidate.ownerSessionId().equals(current.ownerSessionId())
                || candidate.mutationSequence() != Math.addExact(current.mutationSequence(), 1)
                || candidate.metadataVersion() != current.metadataVersion()) {
            throw new IllegalArgumentException("cursor persistence candidate is not the exact next root");
        }
    }

    private static CursorSnapshotCodecV1.CursorSnapshotCorruptionException corruption(
            String message) {
        return new CursorSnapshotCodecV1.CursorSnapshotCorruptionException(message);
    }

    public record HydratedState(
            VersionedCursorState root,
            CursorState state,
            Optional<CursorAckState> snapshotBase) {
        public HydratedState {
            Objects.requireNonNull(root, "root");
            Objects.requireNonNull(state, "state");
            snapshotBase = Objects.requireNonNull(snapshotBase, "snapshotBase");
        }
    }

    public sealed interface PersistencePlan permits InlinePlan, SnapshotPlan {
    }

    public record InlinePlan(CursorStateRecord record) implements PersistencePlan {
        public InlinePlan {
            Objects.requireNonNull(record, "record");
        }
    }

    public record SnapshotPlan(CursorSnapshotWriteRequest request) implements PersistencePlan {
        public SnapshotPlan {
            Objects.requireNonNull(request, "request");
        }
    }

    private record RootDelta(
            List<CursorAckRangeRecord> ranges,
            List<CursorPartialBatchAckRecord> partials,
            boolean compatible) {
        private RootDelta {
            ranges = List.copyOf(ranges);
            partials = List.copyOf(partials);
        }

        private static RootDelta incompatible() {
            return new RootDelta(List.of(), List.of(), false);
        }
    }
}
