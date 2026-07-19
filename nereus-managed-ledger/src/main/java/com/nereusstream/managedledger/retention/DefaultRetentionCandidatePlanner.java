/* Licensed under the Apache License, Version 2.0 */
package com.nereusstream.managedledger.retention;

import com.nereusstream.api.ErrorCode;
import com.nereusstream.api.NereusException;
import com.nereusstream.api.ReadView;
import com.nereusstream.api.StreamId;
import com.nereusstream.managedledger.cursor.CursorOwnerSession;
import com.nereusstream.managedledger.cursor.CursorRetentionView;
import com.nereusstream.managedledger.cursor.CursorStorage;
import com.nereusstream.metadata.oxia.F4ScanToken;
import com.nereusstream.metadata.oxia.GenerationMetadataStore;
import com.nereusstream.metadata.oxia.OxiaMetadataStore;
import com.nereusstream.metadata.oxia.RangeRetentionStatsScanPage;
import com.nereusstream.metadata.oxia.StreamMetadataSnapshot;
import com.nereusstream.metadata.oxia.VersionedGenerationCandidate;
import com.nereusstream.metadata.oxia.VersionedGenerationIndex;
import com.nereusstream.metadata.oxia.VersionedGenerationZeroIndex;
import com.nereusstream.metadata.oxia.VersionedRangeRetentionStats;
import com.nereusstream.metadata.oxia.records.GenerationIndexRecord;
import com.nereusstream.metadata.oxia.records.GenerationLifecycle;
import com.nereusstream.metadata.oxia.records.RangeRetentionStatsRecord;
import java.time.Clock;
import java.util.ArrayList;
import java.util.Comparator;
import java.util.List;
import java.util.Objects;
import java.util.Optional;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.CompletionException;
import java.util.concurrent.ExecutionException;
import java.util.concurrent.TimeUnit;

/** Stable, source-index-verified implementation of the Phase 4 logical-retention formula. */
public final class DefaultRetentionCandidatePlanner
        implements RetentionCandidatePlanner {
    static final int MAX_STABLE_ATTEMPTS = 4;

    private final PlanningAuthority authority;
    private final CursorOwnerSession owner;
    private final NereusRetentionConfig config;
    private final Clock clock;

    public DefaultRetentionCandidatePlanner(
            String cluster,
            OxiaMetadataStore l0,
            GenerationMetadataStore generations,
            CursorStorage cursors,
            CursorOwnerSession owner,
            RetentionPolicySnapshotProvider policies,
            NereusRetentionConfig config,
            Clock clock) {
        this(
                new StoreBackedPlanningAuthority(
                        requireText(cluster, "cluster"),
                        Objects.requireNonNull(l0, "l0"),
                        Objects.requireNonNull(generations, "generations"),
                        Objects.requireNonNull(cursors, "cursors"),
                        Objects.requireNonNull(policies, "policies")),
                owner,
                config,
                clock);
    }

    DefaultRetentionCandidatePlanner(
            PlanningAuthority authority,
            CursorOwnerSession owner,
            NereusRetentionConfig config,
            Clock clock) {
        this.authority = Objects.requireNonNull(authority, "authority");
        this.owner = Objects.requireNonNull(owner, "owner");
        this.config = Objects.requireNonNull(config, "config");
        this.clock = Objects.requireNonNull(clock, "clock");
    }

    @Override
    public CompletableFuture<Optional<RetentionCandidate>> plan(
            StreamId streamId,
            RetentionPolicySnapshot policy) {
        final StreamId exactStream;
        final RetentionPolicySnapshot exactPolicy;
        try {
            exactStream = requireOwnerStream(
                    Objects.requireNonNull(streamId, "streamId"));
            exactPolicy = Objects.requireNonNull(policy, "policy");
        } catch (Throwable failure) {
            return CompletableFuture.failedFuture(failure);
        }
        return planAttempt(exactStream, exactPolicy, 0)
                .orTimeout(
                        config.operationTimeout().toMillis(),
                        TimeUnit.MILLISECONDS);
    }

    @Override
    public CompletableFuture<Void> revalidate(
            RetentionCandidate candidate,
            RetentionPolicySnapshot policy) {
        final RetentionCandidate exactCandidate;
        final RetentionPolicySnapshot exactPolicy;
        try {
            exactCandidate = Objects.requireNonNull(candidate, "candidate");
            exactPolicy = Objects.requireNonNull(policy, "policy");
            requireOwnerStream(exactCandidate.streamId());
            if (exactCandidate.policyVersion() != exactPolicy.policyVersion()) {
                throw condition("retention policy version changed before trim");
            }
        } catch (Throwable failure) {
            return CompletableFuture.failedFuture(failure);
        }
        return revalidateAttempt(exactCandidate, exactPolicy, 0)
                .orTimeout(
                        config.operationTimeout().toMillis(),
                        TimeUnit.MILLISECONDS);
    }

    private CompletableFuture<Optional<RetentionCandidate>> planAttempt(
            StreamId streamId,
            RetentionPolicySnapshot policy,
            int attempt) {
        if (attempt >= MAX_STABLE_ATTEMPTS) {
            return CompletableFuture.failedFuture(condition(
                    "retention authorities did not stabilize after four attempts"));
        }
        long plannedAtMillis = nonNegativeNow();
        return capture(streamId, policy, plannedAtMillis)
                .thenCompose(first -> {
                    if (first.isEmpty()) {
                        return CompletableFuture.completedFuture(
                                Optional.<RetentionCandidate>empty());
                    }
                    RetentionCandidate expected = first.orElseThrow();
                    return capture(streamId, policy, plannedAtMillis)
                            .thenApply(second -> {
                                if (second.isEmpty()
                                        || !expected.equals(second.orElseThrow())) {
                                    throw new AuthorityDriftException();
                                }
                                return Optional.of(expected);
                            });
                })
                .handle((value, failure) -> {
                    Throwable cause = failure == null ? null : unwrap(failure);
                    if (cause instanceof AuthorityDriftException) {
                        return planAttempt(streamId, policy, attempt + 1);
                    }
                    if (cause != null) {
                        return CompletableFuture
                                .<Optional<RetentionCandidate>>failedFuture(cause);
                    }
                    return CompletableFuture.completedFuture(value);
                })
                .thenCompose(value -> value);
    }

    private CompletableFuture<Void> revalidateAttempt(
            RetentionCandidate candidate,
            RetentionPolicySnapshot policy,
            int attempt) {
        if (attempt >= MAX_STABLE_ATTEMPTS) {
            return CompletableFuture.failedFuture(condition(
                    "retention authorities did not stabilize during final revalidation"));
        }
        return capture(candidate.streamId(), policy, candidate.plannedAtMillis())
                .thenCompose(first -> {
                    if (first.isEmpty()
                            || !candidate.equals(first.orElseThrow())) {
                        return CompletableFuture.failedFuture(condition(
                                "retention candidate authority changed before trim"));
                    }
                    return capture(
                                    candidate.streamId(),
                                    policy,
                                    candidate.plannedAtMillis())
                            .thenAccept(second -> {
                                if (second.isEmpty()
                                        || !candidate.equals(second.orElseThrow())) {
                                    throw new AuthorityDriftException();
                                }
                            });
                })
                .handle((ignored, failure) -> {
                    Throwable cause = failure == null ? null : unwrap(failure);
                    if (cause instanceof AuthorityDriftException) {
                        return revalidateAttempt(candidate, policy, attempt + 1);
                    }
                    if (cause != null) {
                        return CompletableFuture.<Void>failedFuture(cause);
                    }
                    return CompletableFuture.<Void>completedFuture(null);
                })
                .thenCompose(value -> value);
    }

    private CompletableFuture<Optional<RetentionCandidate>> capture(
            StreamId streamId,
            RetentionPolicySnapshot suppliedPolicy,
            long plannedAtMillis) {
        return authority.policy(streamId)
                .thenCompose(policy -> {
                    if (!policy.equals(suppliedPolicy)) {
                        return CompletableFuture.failedFuture(condition(
                                "retention policy snapshot changed during planning"));
                    }
                    return authority.head(streamId)
                            .thenCompose(head -> authority.cursor(owner)
                                    .thenCompose(cursor -> captureStats(
                                            streamId,
                                            head,
                                            cursor,
                                            policy,
                                            plannedAtMillis)));
                });
    }

    private CompletableFuture<Optional<RetentionCandidate>> captureStats(
            StreamId streamId,
            StreamMetadataSnapshot head,
            CursorRetentionView cursor,
            RetentionPolicySnapshot policy,
            long plannedAtMillis) {
        validateAuthority(streamId, head, cursor);
        long currentTrim = head.trim().trimOffset();
        long committedEnd = head.committedEnd().committedEndOffset();
        long cursorCut = Math.min(
                cursor.protectedFloorOffset(), committedEnd);
        if (cursor.lifecycle() != CursorRetentionView.Lifecycle.ACTIVE
                || cursor.lastCompletedTrimOffset() != currentTrim
                || cursorCut <= currentTrim
                || policy.retainsIndefinitely()) {
            return CompletableFuture.completedFuture(Optional.empty());
        }
        if (policy.disablesPostConsumeRetention()) {
            return finishCapture(
                    streamId,
                    head,
                    cursor,
                    policy,
                    plannedAtMillis,
                    cursorCut,
                    List.of());
        }
        long scanStart = Math.addExact(currentTrim, 1);
        return scanAllStats(
                        streamId,
                        scanStart,
                        cursorCut,
                        Optional.empty(),
                        new ArrayList<>(),
                        null)
                .thenCompose(scanned -> {
                    if (!scanned.complete()) {
                        return CompletableFuture.completedFuture(Optional.empty());
                    }
                    return verifySources(
                                    streamId,
                                    scanned.values(),
                                    0,
                                    new ArrayList<>())
                            .thenCompose(verified -> finishCapture(
                                    streamId,
                                    head,
                                    cursor,
                                    policy,
                                    plannedAtMillis,
                                    cursorCut,
                                    verified));
                });
    }

    private CompletableFuture<Optional<RetentionCandidate>> finishCapture(
            StreamId streamId,
            StreamMetadataSnapshot initialHead,
            CursorRetentionView initialCursor,
            RetentionPolicySnapshot initialPolicy,
            long plannedAtMillis,
            long cursorCut,
            List<VersionedRangeRetentionStats> verified) {
        CandidateCuts cuts = cuts(
                initialHead,
                initialPolicy,
                cursorCut,
                plannedAtMillis,
                verified);
        if (cuts.candidateTrimOffset()
                <= initialHead.trim().trimOffset()) {
            return CompletableFuture.completedFuture(Optional.empty());
        }
        List<RetentionStatsToken> tokens = verified.stream()
                .map(DefaultRetentionCandidatePlanner::token)
                .sorted()
                .toList();
        return authority.policy(streamId)
                .thenCompose(policy -> authority.head(streamId)
                        .thenCompose(head -> authority.cursor(owner)
                                .thenApply(cursor -> {
                                    if (!initialPolicy.equals(policy)
                                            || !initialHead
                                                    .sameVersionedAuthority(head)
                                            || !initialCursor.equals(cursor)) {
                                        throw new AuthorityDriftException();
                                    }
                                    var evidence = RetentionEvidenceDigests.candidate(
                                            streamId,
                                            head,
                                            cursor,
                                            policy,
                                            cursorCut,
                                            cuts.timeCut(),
                                            cuts.sizeCut(),
                                            cuts.candidateTrimOffset(),
                                            tokens,
                                            plannedAtMillis);
                                    return Optional.of(new RetentionCandidate(
                                            streamId,
                                            head.trim().trimOffset(),
                                            head.committedEnd().committedEndOffset(),
                                            cursorCut,
                                            cuts.timeCut(),
                                            cuts.sizeCut(),
                                            cuts.candidateTrimOffset(),
                                            head.metadataVersion(),
                                            cursor.metadataVersion(),
                                            policy.policyVersion(),
                                            tokens,
                                            evidence,
                                            plannedAtMillis));
                                })));
    }

    private CompletableFuture<StatsScan> scanAllStats(
            StreamId streamId,
            long minOffsetEndInclusive,
            long maxOffsetEndInclusive,
            Optional<F4ScanToken> continuation,
            List<VersionedRangeRetentionStats> values,
            String previousKey) {
        int remaining = RetentionCandidate.MAX_STATS_TOKENS - values.size();
        if (remaining <= 0 && continuation.isPresent()) {
            return CompletableFuture.completedFuture(
                    new StatsScan(List.copyOf(values), false));
        }
        int limit = Math.min(config.statsScanPageSize(), Math.max(1, remaining));
        return authority.scanStats(
                        streamId,
                        minOffsetEndInclusive,
                        maxOffsetEndInclusive,
                        continuation,
                        limit)
                .thenCompose(page -> {
                    String key = previousKey;
                    for (VersionedRangeRetentionStats value : page.values()) {
                        if (key != null && key.compareTo(value.key()) >= 0) {
                            return CompletableFuture.failedFuture(invariant(
                                    "retention stats scan is not strictly ordered across pages"));
                        }
                        key = value.key();
                        if (values.size()
                                == RetentionCandidate.MAX_STATS_TOKENS) {
                            return CompletableFuture.completedFuture(
                                    new StatsScan(List.copyOf(values), false));
                        }
                        values.add(value);
                    }
                    if (page.continuation().isEmpty()) {
                        return CompletableFuture.completedFuture(
                                new StatsScan(List.copyOf(values), true));
                    }
                    return scanAllStats(
                            streamId,
                            minOffsetEndInclusive,
                            maxOffsetEndInclusive,
                            page.continuation(),
                            values,
                            key);
                });
    }

    private CompletableFuture<List<VersionedRangeRetentionStats>> verifySources(
            StreamId streamId,
            List<VersionedRangeRetentionStats> values,
            int index,
            List<VersionedRangeRetentionStats> verified) {
        if (index >= values.size()) {
            return CompletableFuture.completedFuture(List.copyOf(verified));
        }
        VersionedRangeRetentionStats stats = values.get(index);
        RangeRetentionStatsRecord value = stats.value();
        if (!value.streamId().equals(streamId.value())) {
            return CompletableFuture.failedFuture(invariant(
                    "retention stats scan returned another stream"));
        }
        return authority.sourceIndex(streamId, value.sourceIndexKey())
                .thenCompose(source -> {
                    if (source.isPresent()
                            && sourceMatches(stats, source.orElseThrow())) {
                        verified.add(stats);
                    }
                    return verifySources(
                            streamId,
                            values,
                            index + 1,
                            verified);
                });
    }

    private static CandidateCuts cuts(
            StreamMetadataSnapshot head,
            RetentionPolicySnapshot policy,
            long cursorCut,
            long plannedAtMillis,
            List<VersionedRangeRetentionStats> verified) {
        long trim = head.trim().trimOffset();
        long timeCut;
        if (policy.retentionTimeMillis() == -1) {
            timeCut = trim;
        } else if (policy.retentionTimeMillis() == 0) {
            timeCut = cursorCut;
        } else {
            timeCut = trim;
            long contiguous = trim;
            List<VersionedRangeRetentionStats> byBoundary = verified.stream()
                    .sorted(Comparator
                            .comparingLong((VersionedRangeRetentionStats value) ->
                                    value.value().offsetEnd())
                            .thenComparing(VersionedRangeRetentionStats::key))
                    .toList();
            for (VersionedRangeRetentionStats wrapper : byBoundary) {
                RangeRetentionStatsRecord value = wrapper.value();
                if (value.offsetStart() != contiguous
                        || value.offsetEnd() > cursorCut
                        || !expired(
                                plannedAtMillis,
                                value.maxPublishTimeMillis(),
                                policy.retentionTimeMillis())) {
                    break;
                }
                contiguous = value.offsetEnd();
                timeCut = contiguous;
            }
        }

        long sizeCut;
        if (policy.retentionSizeBytes() == -1) {
            sizeCut = trim;
        } else if (policy.retentionSizeBytes() == 0) {
            sizeCut = cursorCut;
        } else {
            sizeCut = trim;
            long total = head.committedEnd().cumulativeSize();
            for (VersionedRangeRetentionStats wrapper : verified) {
                RangeRetentionStatsRecord value = wrapper.value();
                if (value.offsetEnd() <= cursorCut
                        && value.cumulativeSizeAtEnd() <= total
                        && total - value.cumulativeSizeAtEnd()
                                >= policy.retentionSizeBytes()) {
                    sizeCut = Math.max(sizeCut, value.offsetEnd());
                }
            }
        }
        long candidate = Math.min(cursorCut, Math.max(timeCut, sizeCut));
        return new CandidateCuts(timeCut, sizeCut, candidate);
    }

    private static boolean expired(
            long nowMillis,
            long maxPublishTimeMillis,
            long retentionTimeMillis) {
        long age = nowMillis >= maxPublishTimeMillis
                ? nowMillis - maxPublishTimeMillis
                : 0;
        return age > retentionTimeMillis;
    }

    private static boolean sourceMatches(
            VersionedRangeRetentionStats stats,
            VersionedGenerationCandidate source) {
        RangeRetentionStatsRecord expected = stats.value();
        if (!source.key().equals(expected.sourceIndexKey())
                || source.metadataVersion()
                        != expected.sourceIndexMetadataVersion()
                || !source.durableValueSha256().value()
                        .equals(expected.sourceIndexIdentitySha256())) {
            return false;
        }
        if (source instanceof VersionedGenerationZeroIndex zero) {
            var value = zero.value();
            long cumulativeStart;
            try {
                cumulativeStart = Math.subtractExact(
                        value.cumulativeSize(), value.logicalBytes());
            } catch (ArithmeticException failure) {
                return false;
            }
            return !value.tombstoned()
                    && value.streamId().value().equals(expected.streamId())
                    && value.offsetStart() == expected.offsetStart()
                    && value.offsetEnd() == expected.offsetEnd()
                    && value.commitVersion() == expected.commitVersion()
                    && cumulativeStart == expected.cumulativeSizeAtStart()
                    && value.cumulativeSize() == expected.cumulativeSizeAtEnd();
        }
        VersionedGenerationIndex higher = (VersionedGenerationIndex) source;
        GenerationIndexRecord value = higher.value();
        return value.lifecycle() == GenerationLifecycle.COMMITTED
                && value.readViewId() == ReadView.COMMITTED.wireId()
                && value.streamId().equals(expected.streamId())
                && value.offsetStart() <= expected.offsetStart()
                && value.offsetEnd() >= expected.offsetEnd()
                && value.firstCommitVersion() <= expected.commitVersion()
                && value.lastCommitVersion() >= expected.commitVersion()
                && value.cumulativeSizeAtStart()
                        <= expected.cumulativeSizeAtStart()
                && value.cumulativeSizeAtEnd()
                        >= expected.cumulativeSizeAtEnd();
    }

    private void validateAuthority(
            StreamId streamId,
            StreamMetadataSnapshot head,
            CursorRetentionView cursor) {
        Objects.requireNonNull(head, "head");
        Objects.requireNonNull(cursor, "cursor");
        if (!head.metadata().streamId().equals(streamId.value())
                || !cursor.ledger().equals(owner.ledger())
                || !cursor.ownerSessionId().equals(owner.ownerSessionId())
                || !owner.ledger().projection().streamId()
                        .equals(streamId.value())) {
            throw invariant(
                    "retention head, owner session, and projection do not identify one stream");
        }
        String state = head.metadata().state();
        if (!state.equals("ACTIVE") && !state.equals("SEALED")) {
            throw condition("logical retention requires a live stream");
        }
    }

    private StreamId requireOwnerStream(StreamId streamId) {
        if (!owner.ledger().projection().streamId()
                .equals(streamId.value())) {
            throw new IllegalArgumentException(
                    "retention planner stream does not match its writable owner projection");
        }
        return streamId;
    }

    private long nonNegativeNow() {
        return Math.max(0, clock.millis());
    }

    private static RetentionStatsToken token(
            VersionedRangeRetentionStats value) {
        return new RetentionStatsToken(
                value.key(),
                value.metadataVersion(),
                value.durableValueSha256());
    }

    private static NereusException condition(String message) {
        return new NereusException(
                ErrorCode.METADATA_CONDITION_FAILED,
                true,
                message);
    }

    private static NereusException invariant(String message) {
        return new NereusException(
                ErrorCode.METADATA_INVARIANT_VIOLATION,
                false,
                message);
    }

    private static Throwable unwrap(Throwable failure) {
        Throwable current = Objects.requireNonNull(failure, "failure");
        while ((current instanceof CompletionException
                        || current instanceof ExecutionException)
                && current.getCause() != null) {
            current = current.getCause();
        }
        return current;
    }

    private static String requireText(String value, String field) {
        Objects.requireNonNull(value, field);
        if (value.isBlank()) {
            throw new IllegalArgumentException(field + " cannot be blank");
        }
        return value;
    }

    interface PlanningAuthority {
        CompletableFuture<RetentionPolicySnapshot> policy(StreamId streamId);

        CompletableFuture<StreamMetadataSnapshot> head(StreamId streamId);

        CompletableFuture<CursorRetentionView> cursor(CursorOwnerSession owner);

        CompletableFuture<RangeRetentionStatsScanPage> scanStats(
                StreamId streamId,
                long minOffsetEndInclusive,
                long maxOffsetEndInclusive,
                Optional<F4ScanToken> continuation,
                int limit);

        CompletableFuture<Optional<VersionedGenerationCandidate>> sourceIndex(
                StreamId streamId,
                String key);
    }

    private record StoreBackedPlanningAuthority(
            String cluster,
            OxiaMetadataStore l0,
            GenerationMetadataStore generations,
            CursorStorage cursors,
            RetentionPolicySnapshotProvider policies)
            implements PlanningAuthority {
        @Override
        public CompletableFuture<RetentionPolicySnapshot> policy(
                StreamId streamId) {
            return Objects.requireNonNull(
                    policies.snapshot(streamId),
                    "retention policy provider returned null future");
        }

        @Override
        public CompletableFuture<StreamMetadataSnapshot> head(
                StreamId streamId) {
            return l0.getStreamSnapshot(cluster, streamId);
        }

        @Override
        public CompletableFuture<CursorRetentionView> cursor(
                CursorOwnerSession owner) {
            return cursors.retentionView(owner);
        }

        @Override
        public CompletableFuture<RangeRetentionStatsScanPage> scanStats(
                StreamId streamId,
                long minOffsetEndInclusive,
                long maxOffsetEndInclusive,
                Optional<F4ScanToken> continuation,
                int limit) {
            return generations.scanRangeRetentionStats(
                    cluster,
                    streamId,
                    minOffsetEndInclusive,
                    maxOffsetEndInclusive,
                    continuation,
                    limit);
        }

        @Override
        public CompletableFuture<Optional<VersionedGenerationCandidate>>
                sourceIndex(StreamId streamId, String key) {
            return generations.getCandidateByKey(
                    cluster, streamId, ReadView.COMMITTED, key);
        }
    }

    private record StatsScan(
            List<VersionedRangeRetentionStats> values,
            boolean complete) {
        private StatsScan {
            values = List.copyOf(values);
        }
    }

    private record CandidateCuts(
            long timeCut,
            long sizeCut,
            long candidateTrimOffset) {
    }

    private static final class AuthorityDriftException
            extends RuntimeException {
        private AuthorityDriftException() {
            super("retention authority changed during one planning attempt");
        }
    }
}
