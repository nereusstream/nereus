/* Licensed under the Apache License, Version 2.0 */
package com.nereusstream.managedledger.cursor;

import com.nereusstream.metadata.oxia.CursorNames;
import java.util.ArrayList;
import java.util.LinkedHashMap;
import java.util.List;
import java.util.Map;
import java.util.NavigableMap;
import java.util.Objects;
import java.util.Optional;
import java.util.TreeMap;
import org.apache.bookkeeper.mledger.ManagedLedgerException;

/** Pure deterministic durable cursor transitions; this class performs no IO or callback work. */
public final class CursorStateMachine {
    private static final String INTERNAL_PROPERTY_PREFIX = "#pulsar.internal.";

    private final CursorStorageConfig config;

    public CursorStateMachine(CursorStorageConfig config) {
        this.config = Objects.requireNonNull(config, "config");
    }

    public CursorState create(
            CursorOwnerSession owner,
            String cursorName,
            long generation,
            long mutationSequence,
            String protectionAttemptId,
            long markDeleteOffset,
            Map<String, Long> positionProperties,
            Map<String, String> cursorProperties,
            long nowMillis) {
        Objects.requireNonNull(owner, "owner");
        String exactName = CursorNames.requireCursorName(cursorName);
        if (generation < 1 || mutationSequence < 1 || nowMillis < 0) {
            throw new IllegalArgumentException("cursor create generation, sequence, or time is invalid");
        }
        return new CursorState(
                new CursorIdentity(
                        owner.ledger(),
                        exactName,
                        CursorNames.cursorNameHash(exactName),
                        generation),
                owner.ownerSessionId(),
                CursorLifecycle.ACTIVE,
                mutationSequence,
                1,
                protectionAttemptId,
                CursorAckState.empty(markDeleteOffset),
                positionProperties,
                cursorProperties,
                Optional.empty(),
                nowMillis,
                nowMillis,
                0);
    }

    public CursorState claim(CursorState current, String ownerSessionId, long nowMillis) {
        requireActive(current);
        Objects.requireNonNull(ownerSessionId, "ownerSessionId");
        if (current.ownerSessionId().equals(ownerSessionId)) {
            return current;
        }
        return copy(
                current,
                ownerSessionId,
                current.lifecycle(),
                Math.addExact(current.mutationSequence(), 1),
                current.ackStateEpoch(),
                current.lastProtectionAttemptId(),
                current.acknowledgements(),
                current.positionProperties(),
                current.cursorProperties(),
                current.snapshotReference(),
                Math.max(current.updatedAtMillis(), requireNonNegative(nowMillis, "nowMillis")));
    }

    public CursorMutationResult cumulativeAck(
            CursorState current,
            CursorAckRequest request,
            long trimOffset,
            long committedEndOffset,
            long nowMillis) throws ManagedLedgerException {
        requireMutableBounds(current, trimOffset, committedEndOffset);
        Objects.requireNonNull(request, "request");
        requireAckOffset(request.entryOffset(), trimOffset, committedEndOffset);
        requireBatchWithinLimit(request);
        if (request.entryOffset() < current.acknowledgements().markDeleteOffset()) {
            return alreadyApplied(current);
        }

        CursorAckState nextAck = request.wholeEntry()
                ? wholeCumulative(current.acknowledgements(), request.entryOffset())
                : partialCumulative(
                        current.acknowledgements(),
                        request.entryOffset(),
                        request.batchAck().orElseThrow());
        if (nextAck.equals(current.acknowledgements())
                && request.positionProperties().equals(current.positionProperties())) {
            return alreadyApplied(current);
        }
        return applied(copy(
                current,
                current.ownerSessionId(),
                current.lifecycle(),
                Math.addExact(current.mutationSequence(), 1),
                current.ackStateEpoch(),
                current.lastProtectionAttemptId(),
                nextAck,
                request.positionProperties(),
                current.cursorProperties(),
                current.snapshotReference(),
                nextUpdatedAt(current, nowMillis)));
    }

    public CursorMutationResult individualAck(
            CursorState current,
            List<CursorAckRequest> requests,
            long trimOffset,
            long committedEndOffset,
            long nowMillis) throws ManagedLedgerException {
        requireMutableBounds(current, trimOffset, committedEndOffset);
        List<CursorAckRequest> canonical = canonicalIndividualRequests(requests);
        List<OffsetRange> ranges = new ArrayList<>(current.acknowledgements().wholeAckRanges());
        NavigableMap<Long, BatchAckState> partials =
                new TreeMap<>(current.acknowledgements().partialBatchAcks());
        for (CursorAckRequest request : canonical) {
            requireAckOffset(request.entryOffset(), trimOffset, committedEndOffset);
            long offset = request.entryOffset();
            if (current.acknowledgements().isWholeEntryAcknowledged(offset)
                    || covered(ranges, offset)) {
                continue;
            }
            if (request.wholeEntry()) {
                partials.remove(offset);
                ranges.add(new OffsetRange(offset, Math.addExact(offset, 1)));
                continue;
            }
            BatchAckState requested = request.batchAck().orElseThrow();
            BatchAckState existing = partials.get(offset);
            BatchAckState merged = existing == null
                    ? allRemaining(requested.batchSize()).and(requested)
                    : mergeSameBatch(existing, requested);
            if (merged.isWholeEntryAcknowledged()) {
                partials.remove(offset);
                ranges.add(new OffsetRange(offset, Math.addExact(offset, 1)));
            } else if (merged.isAllRemaining()) {
                partials.remove(offset);
            } else {
                partials.put(offset, merged);
            }
        }
        CursorAckState nextAck = new CursorAckState(
                current.acknowledgements().markDeleteOffset(), ranges, partials);
        if (nextAck.equals(current.acknowledgements())) {
            return alreadyApplied(current);
        }
        return applied(copy(
                current,
                current.ownerSessionId(),
                current.lifecycle(),
                Math.addExact(current.mutationSequence(), 1),
                current.ackStateEpoch(),
                current.lastProtectionAttemptId(),
                nextAck,
                current.positionProperties(),
                current.cursorProperties(),
                current.snapshotReference(),
                nextUpdatedAt(current, nowMillis)));
    }

    public CursorMutationResult reset(
            CursorState current,
            CursorResetRequest request,
            String protectionAttemptId,
            long nowMillis) throws ManagedLedgerException {
        requireActive(current);
        Objects.requireNonNull(request, "request");
        requireResetBounds(request);
        CursorAckState resetAck = resetAck(request);
        return applied(copy(
                current,
                current.ownerSessionId(),
                current.lifecycle(),
                Math.addExact(current.mutationSequence(), 1),
                Math.addExact(current.ackStateEpoch(), 1),
                Objects.requireNonNull(protectionAttemptId, "protectionAttemptId"),
                resetAck,
                Map.of(),
                current.cursorProperties(),
                Optional.empty(),
                nextUpdatedAt(current, nowMillis)));
    }

    public CursorMutationResult clearBacklog(
            CursorState current,
            long committedEndOffset,
            long nowMillis) throws ManagedLedgerException {
        requireActive(current);
        if (committedEndOffset < current.acknowledgements().markDeleteOffset()) {
            throw new ManagedLedgerException.InvalidCursorPositionException(
                    "clear-backlog end is behind the durable cursor");
        }
        CursorAckState cleared = CursorAckState.empty(committedEndOffset);
        return applied(copy(
                current,
                current.ownerSessionId(),
                current.lifecycle(),
                Math.addExact(current.mutationSequence(), 1),
                Math.addExact(current.ackStateEpoch(), 1),
                current.lastProtectionAttemptId(),
                cleared,
                Map.of(),
                current.cursorProperties(),
                Optional.empty(),
                nextUpdatedAt(current, nowMillis)));
    }

    public CursorMutationResult mutateCursorProperties(
            CursorState current,
            CursorPropertyMutation mutation,
            long nowMillis) throws ManagedLedgerException {
        requireActive(current);
        Objects.requireNonNull(mutation, "mutation");
        LinkedHashMap<String, String> next = new LinkedHashMap<>(current.cursorProperties());
        if (mutation instanceof CursorPropertyMutation.Put put) {
            next.put(put.key(), put.value());
        } else if (mutation instanceof CursorPropertyMutation.Remove remove) {
            next.remove(remove.key());
        } else {
            CursorPropertyMutation.ReplaceExternal replace =
                    (CursorPropertyMutation.ReplaceExternal) mutation;
            next.entrySet().removeIf(entry -> !entry.getKey().startsWith(INTERNAL_PROPERTY_PREFIX));
            next.putAll(replace.properties());
        }
        if (next.equals(current.cursorProperties())) {
            return alreadyApplied(current);
        }
        return applied(copy(
                current,
                current.ownerSessionId(),
                current.lifecycle(),
                Math.addExact(current.mutationSequence(), 1),
                current.ackStateEpoch(),
                current.lastProtectionAttemptId(),
                current.acknowledgements(),
                current.positionProperties(),
                next,
                current.snapshotReference(),
                nextUpdatedAt(current, nowMillis)));
    }

    public CursorMutationResult flushPositionProperties(
            CursorState current,
            Map<String, Long> fullProperties,
            long nowMillis) throws ManagedLedgerException {
        requireActive(current);
        Objects.requireNonNull(fullProperties, "fullProperties");
        if (fullProperties.equals(current.positionProperties())) {
            return alreadyApplied(current);
        }
        return applied(copy(
                current,
                current.ownerSessionId(),
                current.lifecycle(),
                Math.addExact(current.mutationSequence(), 1),
                current.ackStateEpoch(),
                current.lastProtectionAttemptId(),
                current.acknowledgements(),
                fullProperties,
                current.cursorProperties(),
                current.snapshotReference(),
                nextUpdatedAt(current, nowMillis)));
    }

    public CursorMutationResult delete(CursorState current, long nowMillis)
            throws ManagedLedgerException {
        Objects.requireNonNull(current, "current");
        if (current.lifecycle() == CursorLifecycle.DELETED) {
            return alreadyApplied(current);
        }
        return applied(copy(
                current,
                current.ownerSessionId(),
                CursorLifecycle.DELETED,
                Math.addExact(current.mutationSequence(), 1),
                current.ackStateEpoch(),
                current.lastProtectionAttemptId(),
                CursorAckState.empty(current.acknowledgements().markDeleteOffset()),
                Map.of(),
                Map.of(),
                Optional.empty(),
                nextUpdatedAt(current, nowMillis)));
    }

    public boolean isCumulativeAckSubsumed(CursorState current, CursorAckRequest request) {
        Objects.requireNonNull(current, "current");
        Objects.requireNonNull(request, "request");
        if (!current.positionProperties().equals(request.positionProperties())) {
            return false;
        }
        long offset = request.entryOffset();
        CursorAckState ack = current.acknowledgements();
        if (request.wholeEntry()) {
            return ack.isWholeEntryAcknowledged(offset);
        }
        if (ack.markDeleteOffset() < offset) {
            return false;
        }
        if (ack.isWholeEntryAcknowledged(offset)) {
            return true;
        }
        if (ack.markDeleteOffset() > offset) {
            return true;
        }
        return partialSubsumes(
                ack.partialBatchAcks().get(offset), request.batchAck().orElseThrow());
    }

    public boolean isIndividualAckSubsumed(
            CursorState current, List<CursorAckRequest> requests) {
        Objects.requireNonNull(current, "current");
        for (CursorAckRequest request : canonicalIndividualRequests(requests)) {
            if (current.acknowledgements().isWholeEntryAcknowledged(request.entryOffset())) {
                continue;
            }
            if (request.wholeEntry()
                    || !partialSubsumes(
                            current.acknowledgements().partialBatchAcks().get(request.entryOffset()),
                            request.batchAck().orElseThrow())) {
                return false;
            }
        }
        return true;
    }

    public boolean isExactResetResult(
            CursorState state, CursorResetRequest request, long capturedEpoch) {
        Objects.requireNonNull(state, "state");
        Objects.requireNonNull(request, "request");
        return state.ackStateEpoch() == Math.addExact(capturedEpoch, 1)
                && state.acknowledgements().equals(resetAck(request))
                && state.snapshotReference().isEmpty()
                && state.positionProperties().isEmpty();
    }

    public boolean isExactClearResult(
            CursorState state, long committedEndOffset, long capturedEpoch) {
        Objects.requireNonNull(state, "state");
        return state.ackStateEpoch() == Math.addExact(capturedEpoch, 1)
                && state.acknowledgements().equals(CursorAckState.empty(committedEndOffset))
                && state.snapshotReference().isEmpty()
                && state.positionProperties().isEmpty();
    }

    public List<CursorAckRequest> canonicalIndividualRequests(List<CursorAckRequest> requests) {
        Objects.requireNonNull(requests, "requests");
        if (requests.isEmpty() || requests.size() > config.cursorAckPositionsPerRequestMax()) {
            throw new IllegalArgumentException("individual ack request count is outside the configured bound");
        }
        TreeMap<Long, CursorAckRequest> canonical = new TreeMap<>();
        for (CursorAckRequest request : List.copyOf(requests)) {
            Objects.requireNonNull(request, "requests contains null");
            if (!request.positionProperties().isEmpty()) {
                throw new IllegalArgumentException("individual ack cannot carry position properties");
            }
            if (request.batchAck().isPresent()
                    && request.batchAck().orElseThrow().batchSize() > config.cursorBatchIndexesMax()) {
                throw new IllegalArgumentException("batch ack exceeds the configured batch-index bound");
            }
            canonical.merge(request.entryOffset(), request, CursorStateMachine::mergeRequests);
        }
        return List.copyOf(canonical.values());
    }

    private static CursorAckRequest mergeRequests(CursorAckRequest left, CursorAckRequest right) {
        if (left.wholeEntry() || right.wholeEntry()) {
            return new CursorAckRequest(left.entryOffset(), Optional.empty(), Map.of());
        }
        BatchAckState merged = mergeSameBatch(
                left.batchAck().orElseThrow(), right.batchAck().orElseThrow());
        return new CursorAckRequest(left.entryOffset(), Optional.of(merged), Map.of());
    }

    private void requireBatchWithinLimit(CursorAckRequest request) {
        if (request.batchAck().isPresent()
                && request.batchAck().orElseThrow().batchSize() > config.cursorBatchIndexesMax()) {
            throw new IllegalArgumentException("batch ack exceeds the configured batch-index bound");
        }
    }

    private static CursorAckState wholeCumulative(CursorAckState current, long offset) {
        long markDelete = Math.addExact(offset, 1);
        return clipAt(current, markDelete);
    }

    private static CursorAckState partialCumulative(
            CursorAckState current, long offset, BatchAckState request) {
        CursorAckState base = clipAt(current, offset);
        if (base.isWholeEntryAcknowledged(offset)) {
            return base;
        }
        List<OffsetRange> ranges = new ArrayList<>(base.wholeAckRanges());
        NavigableMap<Long, BatchAckState> partials = new TreeMap<>(base.partialBatchAcks());
        BatchAckState existing = partials.get(offset);
        BatchAckState merged = existing == null
                ? allRemaining(request.batchSize()).and(request)
                : mergeSameBatch(existing, request);
        if (merged.isWholeEntryAcknowledged()) {
            partials.remove(offset);
            ranges.add(new OffsetRange(offset, Math.addExact(offset, 1)));
        } else if (merged.isAllRemaining()) {
            partials.remove(offset);
        } else {
            partials.put(offset, merged);
        }
        return new CursorAckState(offset, ranges, partials);
    }

    private static CursorAckState clipAt(CursorAckState current, long markDelete) {
        if (markDelete <= current.markDeleteOffset()) {
            return current;
        }
        List<OffsetRange> ranges = new ArrayList<>();
        for (OffsetRange range : current.wholeAckRanges()) {
            if (range.endOffset() <= markDelete) {
                continue;
            }
            ranges.add(new OffsetRange(Math.max(range.startOffset(), markDelete), range.endOffset()));
        }
        NavigableMap<Long, BatchAckState> partials =
                new TreeMap<>(current.partialBatchAcks().tailMap(markDelete, true));
        return new CursorAckState(markDelete, ranges, partials);
    }

    private static CursorAckState resetAck(CursorResetRequest request) {
        NavigableMap<Long, BatchAckState> partials = new TreeMap<>();
        request.targetBatchAck().ifPresent(state -> partials.put(request.nextReadOffset(), state));
        return new CursorAckState(request.nextReadOffset(), List.of(), partials);
    }

    private static boolean partialSubsumes(BatchAckState current, BatchAckState requested) {
        if (current == null) {
            return requested.isAllRemaining();
        }
        if (current.batchSize() != requested.batchSize()) {
            return false;
        }
        return current.and(requested).equals(current);
    }

    private static BatchAckState mergeSameBatch(BatchAckState left, BatchAckState right) {
        if (left.batchSize() != right.batchSize()) {
            throw new IllegalArgumentException("duplicate partial ack batch sizes do not match");
        }
        return left.and(right);
    }

    private static BatchAckState allRemaining(int batchSize) {
        int wordCount = Math.addExact(batchSize, Long.SIZE - 1) / Long.SIZE;
        long[] words = new long[wordCount];
        java.util.Arrays.fill(words, -1L);
        int remainder = batchSize & (Long.SIZE - 1);
        if (remainder != 0) {
            words[wordCount - 1] = (1L << remainder) - 1;
        }
        return new BatchAckState(batchSize, words);
    }

    private static boolean covered(List<OffsetRange> ranges, long offset) {
        for (OffsetRange range : ranges) {
            if (range.contains(offset)) {
                return true;
            }
        }
        return false;
    }

    private static CursorMutationResult alreadyApplied(CursorState current) {
        return new CursorMutationResult(CursorMutationOutcome.ALREADY_APPLIED, current);
    }

    private static CursorMutationResult applied(CursorState candidate) {
        return new CursorMutationResult(CursorMutationOutcome.APPLIED, candidate);
    }

    private static CursorState copy(
            CursorState current,
            String ownerSessionId,
            CursorLifecycle lifecycle,
            long mutationSequence,
            long ackStateEpoch,
            String lastProtectionAttemptId,
            CursorAckState acknowledgements,
            Map<String, Long> positionProperties,
            Map<String, String> cursorProperties,
            Optional<CursorSnapshotReference> snapshotReference,
            long updatedAtMillis) {
        return new CursorState(
                current.identity(),
                ownerSessionId,
                lifecycle,
                mutationSequence,
                ackStateEpoch,
                lastProtectionAttemptId,
                acknowledgements,
                positionProperties,
                cursorProperties,
                snapshotReference,
                current.createdAtMillis(),
                updatedAtMillis,
                current.metadataVersion());
    }

    private static long nextUpdatedAt(CursorState current, long nowMillis) {
        return Math.max(current.updatedAtMillis(), requireNonNegative(nowMillis, "nowMillis"));
    }

    private static long requireNonNegative(long value, String fieldName) {
        if (value < 0) {
            throw new IllegalArgumentException(fieldName + " must be non-negative");
        }
        return value;
    }

    private static void requireActive(CursorState current) {
        Objects.requireNonNull(current, "current");
        if (current.lifecycle() != CursorLifecycle.ACTIVE) {
            throw new IllegalStateException("durable cursor is deleted");
        }
    }

    private static void requireMutableBounds(
            CursorState current, long trimOffset, long committedEndOffset)
            throws ManagedLedgerException {
        requireActive(current);
        if (trimOffset < 0
                || committedEndOffset < trimOffset
                || current.acknowledgements().markDeleteOffset() < trimOffset
                || current.acknowledgements().markDeleteOffset() > committedEndOffset) {
            throw new ManagedLedgerException("authoritative cursor/L0 bounds are inconsistent");
        }
    }

    private static void requireAckOffset(long offset, long trimOffset, long committedEndOffset)
            throws ManagedLedgerException.InvalidCursorPositionException {
        if (offset < trimOffset || offset >= committedEndOffset) {
            throw new ManagedLedgerException.InvalidCursorPositionException(
                    "ack offset is outside the retained committed range");
        }
    }

    private static void requireResetBounds(CursorResetRequest request)
            throws ManagedLedgerException.InvalidCursorPositionException {
        if (request.nextReadOffset() < request.observedTrimOffset()
                || request.nextReadOffset() > request.observedCommittedEndOffset()
                || (request.force()
                        && request.nextReadOffset() < request.observedTrimOffset())) {
            throw new ManagedLedgerException.InvalidCursorPositionException(
                    "reset target is outside the retained committed range");
        }
    }
}
