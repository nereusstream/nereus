/* Licensed under the Apache License, Version 2.0 */
package com.nereusstream.materialization;

import com.nereusstream.api.ErrorCode;
import com.nereusstream.api.NereusException;
import com.nereusstream.api.ObjectType;
import com.nereusstream.api.ReadView;
import com.nereusstream.api.StorageProfile;
import com.nereusstream.api.StreamId;
import com.nereusstream.api.StreamState;
import com.nereusstream.api.target.ObjectSliceReadTarget;
import com.nereusstream.core.backpressure.MaterializationLagSnapshot;
import com.nereusstream.core.backpressure.MaterializationLagSnapshotReader;
import com.nereusstream.core.recovery.AnchorAwareCommitWalk;
import com.nereusstream.core.recovery.AnchorAwareCommitWalker;
import com.nereusstream.metadata.oxia.AppendRecoveryCommit;
import com.nereusstream.metadata.oxia.F4ScanToken;
import com.nereusstream.metadata.oxia.GenerationMetadataStore;
import com.nereusstream.metadata.oxia.GenerationScanPage;
import com.nereusstream.metadata.oxia.OxiaMetadataStore;
import com.nereusstream.metadata.oxia.RangeRetentionStatsScanPage;
import com.nereusstream.metadata.oxia.StreamMetadataSnapshot;
import com.nereusstream.metadata.oxia.VersionedGenerationCandidate;
import com.nereusstream.metadata.oxia.VersionedGenerationIndex;
import com.nereusstream.metadata.oxia.VersionedGenerationZeroIndex;
import com.nereusstream.metadata.oxia.VersionedMaterializationCheckpoint;
import com.nereusstream.metadata.oxia.VersionedMaterializationStreamRegistration;
import com.nereusstream.metadata.oxia.VersionedRangeRetentionStats;
import com.nereusstream.metadata.oxia.codec.ReadTargetCodecRegistry;
import com.nereusstream.metadata.oxia.records.GenerationIndexRecord;
import com.nereusstream.metadata.oxia.records.GenerationLifecycle;
import com.nereusstream.metadata.oxia.records.MaterializationCheckpointRecord;
import com.nereusstream.metadata.oxia.records.RangeRetentionStatsRecord;
import java.nio.charset.StandardCharsets;
import java.time.Clock;
import java.time.Duration;
import java.util.ArrayList;
import java.util.Comparator;
import java.util.List;
import java.util.Objects;
import java.util.Optional;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.ScheduledExecutorService;

/**
 * Rebuilds current-policy committed coverage and derives exact unmaterialized source records, bytes, and oldest age.
 *
 * <p>Task count and stored checkpoint progress are never treated as lag truth. A stored checkpoint is accepted only
 * when it does not lead the independently rebuilt committed cover.
 */
public final class DefaultMaterializationLagSnapshotReader
        implements MaterializationLagSnapshotReader {
    public static final int MAX_CANDIDATES = 4_096;
    public static final int MAX_RETENTION_STATS = 4_096;
    private static final int MAX_STABILITY_ATTEMPTS = 4;
    private static final Comparator<VersionedGenerationIndex>
            COVER_ORDER = Comparator
                    .comparingLong(
                            (VersionedGenerationIndex value) ->
                                    value.value().offsetEnd())
                    .reversed()
                    .thenComparing(
                            Comparator.comparingLong(
                                            (VersionedGenerationIndex value) ->
                                                    value.value()
                                                            .generation())
                                    .reversed())
                    .thenComparing(
                            VersionedGenerationIndex::key,
                            DefaultMaterializationLagSnapshotReader
                                    ::compareUtf8);

    private final String cluster;
    private final OxiaMetadataStore l0;
    private final GenerationMetadataStore generations;
    private final MaterializationPolicy policy;
    private final int pageSize;
    private final int maxLiveCommits;
    private final AnchorAwareCommitWalker walker;
    private final ScheduledExecutorService scheduler;
    private final Clock clock;
    private final ReadTargetCodecRegistry targetCodecs =
            ReadTargetCodecRegistry.phase15();

    public DefaultMaterializationLagSnapshotReader(
            String cluster,
            OxiaMetadataStore l0,
            GenerationMetadataStore generations,
            MaterializationPolicy policy,
            int pageSize,
            int maxLiveCommits,
            ScheduledExecutorService scheduler,
            Clock clock) {
        this.cluster = requireText(cluster, "cluster");
        this.l0 = Objects.requireNonNull(l0, "l0");
        this.generations = Objects.requireNonNull(
                generations, "generations");
        this.policy = requirePolicy(policy);
        if (pageSize <= 0
                || pageSize > MaterializationConfig.MAX_PAGE_SIZE) {
            throw new IllegalArgumentException(
                    "pageSize must be in [1, 1000]");
        }
        if (maxLiveCommits <= 0) {
            throw new IllegalArgumentException(
                    "maxLiveCommits must be positive");
        }
        this.pageSize = pageSize;
        this.maxLiveCommits = maxLiveCommits;
        this.walker = new AnchorAwareCommitWalker(
                this.cluster, l0, generations);
        this.scheduler = Objects.requireNonNull(
                scheduler, "scheduler");
        this.clock = Objects.requireNonNull(clock, "clock");
    }

    @Override
    public CompletableFuture<MaterializationLagSnapshot> measure(
            StreamId streamId,
            Duration timeout) {
        final Operation operation;
        try {
            operation = new Operation(
                    Objects.requireNonNull(streamId, "streamId"),
                    timeout);
        } catch (Throwable failure) {
            return CompletableFuture.failedFuture(failure);
        }
        return operation.run()
                .whenComplete((ignored, failure) ->
                        operation.close());
    }

    private final class Operation implements AutoCloseable {
        private final StreamId streamId;
        private final MaterializationDeadline deadline;

        private Operation(
                StreamId streamId,
                Duration timeout) {
            this.streamId = streamId;
            this.deadline = new MaterializationDeadline(
                    timeout, scheduler);
        }

        private CompletableFuture<MaterializationLagSnapshot> run() {
            return attempt(0, "initial authority was not measured");
        }

        private CompletableFuture<MaterializationLagSnapshot> attempt(
                int attempt,
                String lastInstability) {
            if (attempt >= MAX_STABILITY_ATTEMPTS) {
                return CompletableFuture.failedFuture(condition(
                        "materialization lag authority changed throughout measurement: "
                                + lastInstability));
            }
            return loadAuthority()
                    .thenCompose(authority -> scanCoverage(
                                    authority.bounds(),
                                    authority.registration()
                                            .value()
                                            .projectionRef())
                            .thenCompose(coverage -> {
                                requireCheckpointNotAhead(
                                        authority.checkpoint(),
                                        coverage);
                                return deadline.bound(
                                                () -> walker.walk(
                                                        streamId,
                                                        maxLiveCommits,
                                                        pageSize),
                                                "walk stable append tail for materialization lag")
                                        .thenCompose(walk ->
                                                measureStable(
                                                        authority,
                                                        coverage,
                                                        walk))
                                        .thenCompose(measured ->
                                                finalRevalidate(
                                                        authority,
                                                        coverage,
                                                        measured.walk())
                                                        .thenCompose(
                                                                instability ->
                                                                        instability
                                                                                        .isEmpty()
                                                                                ? CompletableFuture
                                                                                        .completedFuture(
                                                                                                measured
                                                                                                        .snapshot())
                                                                                : attempt(
                                                                                        attempt
                                                                                                + 1,
                                                                                        instability
                                                                                                .orElseThrow())));
                            }));
        }

        private CompletableFuture<Authority> loadAuthority() {
            return deadline.bound(
                            () -> l0.getStreamSnapshot(
                                    cluster, streamId),
                            "load stream snapshot for materialization lag")
                    .thenCompose(snapshot -> deadline.bound(
                                    () -> generations
                                            .getStreamRegistration(
                                                    cluster,
                                                    streamId),
                                    "load registration for materialization lag")
                            .thenCompose(registration -> deadline.bound(
                                            () -> generations
                                                    .getMaterializationCheckpoint(
                                                            cluster,
                                                            streamId,
                                                            policy.policyId(),
                                                            policy.policyVersion()),
                                            "load advisory checkpoint for materialization lag")
                                    .thenApply(checkpoint ->
                                            authority(
                                                    snapshot,
                                                    registration
                                                            .orElseThrow(
                                                                    () ->
                                                                            condition(
                                                                                    "materialization registration is absent")),
                                                    checkpoint))));
        }

        private Authority authority(
                StreamMetadataSnapshot snapshot,
                VersionedMaterializationStreamRegistration registration,
                Optional<VersionedMaterializationCheckpoint> checkpoint) {
            if (!snapshot.metadata().streamId()
                            .equals(streamId.value())
                    || !registration.value().streamId()
                            .equals(streamId.value())) {
                throw invariant(
                        "materialization lag authority belongs to another stream",
                        null);
            }
            final StreamState state;
            final StorageProfile profile;
            final StorageProfile registeredProfile;
            try {
                state = StreamState.valueOf(
                        snapshot.metadata().state());
                profile = StorageProfile.valueOf(
                                snapshot.metadata().profile())
                        .canonical();
                registeredProfile = StorageProfile.valueOf(
                                registration.value().storageProfile())
                        .canonical();
            } catch (RuntimeException failure) {
                throw invariant(
                        "materialization lag authority has an unknown state or profile",
                        failure);
            }
            if ((state != StreamState.ACTIVE
                            && state != StreamState.SEALED)
                    || profile != registeredProfile
                    || (profile
                                    != StorageProfile
                                            .OBJECT_WAL_SYNC_OBJECT
                            && profile
                                    != StorageProfile
                                            .OBJECT_WAL_ASYNC_OBJECT)) {
                throw condition(
                        "stream no longer admits materialization lag measurement");
            }
            long trim = snapshot.trim().trimOffset();
            long head = snapshot.committedEnd()
                    .committedEndOffset();
            if (trim > head) {
                throw invariant(
                        "stream trim is ahead of committed end",
                        null);
            }
            checkpoint.ifPresent(value ->
                    requireCheckpointIdentity(value.value()));
            return new Authority(
                    snapshot,
                    registration,
                    checkpoint,
                    new Bounds(trim, head));
        }

        private void requireCheckpointIdentity(
                MaterializationCheckpointRecord value) {
            if (!value.streamId().equals(streamId.value())
                    || !value.policyId()
                            .equals(policy.policyId())
                    || value.policyVersion()
                            != policy.policyVersion()
                    || !value.policySha256()
                            .equals(policy.digestSha256().value())) {
                throw invariant(
                        "materialization lag checkpoint identity mismatch",
                        null);
            }
        }

        private CompletableFuture<Coverage> scanCoverage(
                Bounds bounds,
                String projectionRef) {
            if (bounds.trimOffset() >= bounds.headOffset()) {
                return CompletableFuture.completedFuture(
                        Coverage.at(bounds.trimOffset()));
            }
            return scanCandidates(
                            bounds,
                            Optional.empty(),
                            new ArrayList<>())
                    .thenApply(candidates -> coverage(
                            bounds,
                            projectionRef,
                            candidates));
        }

        private CompletableFuture<List<VersionedGenerationCandidate>>
                scanCandidates(
                        Bounds bounds,
                        Optional<F4ScanToken> continuation,
                        ArrayList<VersionedGenerationCandidate>
                                values) {
            long minimumOffsetEnd = Math.addExact(
                    bounds.trimOffset(), 1);
            return deadline.bound(
                            () -> generations.scanIndex(
                                    cluster,
                                    streamId,
                                    policy.view(),
                                    minimumOffsetEnd,
                                    bounds.headOffset(),
                                    continuation,
                                    pageSize),
                            "scan current-policy generations for materialization lag")
                    .thenCompose(page -> appendCandidatePage(
                            bounds, values, page));
        }

        private CompletableFuture<List<VersionedGenerationCandidate>>
                appendCandidatePage(
                        Bounds bounds,
                        ArrayList<VersionedGenerationCandidate>
                                values,
                        GenerationScanPage page) {
            values.addAll(page.values());
            if (values.size() > MAX_CANDIDATES) {
                return CompletableFuture.failedFuture(
                        new NereusException(
                                ErrorCode.METADATA_LIMIT_EXCEEDED,
                                false,
                                "materialization lag candidate count exceeds 4096"));
            }
            if (page.continuation().isPresent()) {
                return scanCandidates(
                        bounds,
                        page.continuation(),
                        values);
            }
            return CompletableFuture.completedFuture(
                    List.copyOf(values));
        }

        private Coverage coverage(
                Bounds bounds,
                String projectionRef,
                List<VersionedGenerationCandidate> candidates) {
            List<VersionedGenerationIndex> healthy =
                    candidates.stream()
                            .filter(
                                    VersionedGenerationIndex.class
                                            ::isInstance)
                            .map(
                                    VersionedGenerationIndex.class
                                            ::cast)
                            .filter(index -> satisfiesPolicy(
                                    index,
                                    projectionRef,
                                    bounds))
                            .toList();
            long cursor = bounds.trimOffset();
            ArrayList<VersionedGenerationIndex> selected =
                    new ArrayList<>();
            int steps = 0;
            while (cursor < bounds.headOffset()) {
                final long current = cursor;
                VersionedGenerationIndex next =
                        healthy.stream()
                                .filter(index ->
                                        index.value()
                                                        .offsetStart()
                                                <= current
                                                && index.value()
                                                                .offsetEnd()
                                                        > current)
                                .sorted(COVER_ORDER)
                                .findFirst()
                                .orElse(null);
                if (next == null) {
                    break;
                }
                if (++steps > healthy.size()) {
                    throw invariant(
                            "materialization lag coverage did not converge",
                            null);
                }
                selected.add(next);
                cursor = next.value().offsetEnd();
            }
            return new Coverage(cursor, selected);
        }

        private boolean satisfiesPolicy(
                VersionedGenerationIndex candidate,
                String projectionRef,
                Bounds bounds) {
            GenerationIndexRecord value = candidate.value();
            if (!value.streamId().equals(streamId.value())
                    || value.readViewId()
                            != policy.view().wireId()
                    || value.offsetEnd()
                            > bounds.headOffset()) {
                throw invariant(
                        "generation scan returned an out-of-scope lag candidate",
                        null);
            }
            if (value.lifecycle()
                            != GenerationLifecycle.COMMITTED
                    || !value.policySha256().equals(
                            policy.digestSha256().value())
                    || !value.materializationPolicySha256()
                            .equals(policy.digestSha256().value())
                    || !value.projectionRef()
                            .equals(projectionRef)) {
                return false;
            }
            try {
                if (!(targetCodecs.decode(value.readTarget())
                                instanceof ObjectSliceReadTarget target)
                        || target.objectType()
                                != ObjectType
                                        .STREAM_COMPACTED_OBJECT
                        || !target.physicalFormat().equals(
                                policy.targetPhysicalFormat())) {
                    throw invariant(
                            "current-policy lag target is incompatible",
                            null);
                }
            } catch (NereusException failure) {
                throw failure;
            } catch (RuntimeException failure) {
                throw invariant(
                        "current-policy lag target cannot be decoded",
                        failure);
            }
            return true;
        }

        private void requireCheckpointNotAhead(
                Optional<VersionedMaterializationCheckpoint>
                        checkpoint,
                Coverage coverage) {
            checkpoint.ifPresent(value -> {
                if (value.value().contiguousCoveredOffset()
                        > coverage.coveredOffset()) {
                    throw invariant(
                            "materialization checkpoint is ahead of verified committed coverage",
                            null);
                }
            });
        }

        private CompletableFuture<Measured> measureStable(
                Authority authority,
                Coverage coverage,
                AnchorAwareCommitWalk walk) {
            requireWalkMatchesHead(
                    authority.snapshot(), walk);
            long head = authority.bounds().headOffset();
            long covered = coverage.coveredOffset();
            if (covered == head) {
                return CompletableFuture.completedFuture(
                        new Measured(
                                new MaterializationLagSnapshot(
                                        streamId,
                                        covered,
                                        head,
                                        0,
                                        0,
                                        0,
                                        authority.snapshot()
                                                .metadataVersion(),
                                        Math.max(0, clock.millis())),
                                walk));
            }
            long anchor = walk.anchor().offsetEnd();
            if (anchor > head) {
                return CompletableFuture.failedFuture(invariant(
                        "recovery anchor is ahead of committed end",
                        null));
            }
            SourceLag live = liveLag(
                    walk, Math.max(covered, anchor));
            CompletableFuture<SourceLag> source;
            if (covered >= anchor) {
                source = CompletableFuture.completedFuture(live);
            } else {
                source = scanAndVerifyStats(
                                covered,
                                anchor,
                                live.cumulativeSizeAtStart())
                        .thenApply(stats -> combine(
                                stats, live));
            }
            return source.thenApply(lag -> {
                long lagRecords = Math.subtractExact(
                        head, covered);
                long lagBytes = Math.subtractExact(
                        authority.snapshot()
                                .committedEnd()
                                .cumulativeSize(),
                        lag.cumulativeSizeAtStart());
                if (lagRecords <= 0 || lagBytes < 0) {
                    throw invariant(
                            "non-empty materialization lag has invalid record or byte accounting",
                            null);
                }
                long now = Math.max(0, clock.millis());
                long age = now <= lag.oldestPublishTimeMillis()
                        ? 0
                        : Math.subtractExact(
                                now,
                                lag.oldestPublishTimeMillis());
                return new Measured(
                        new MaterializationLagSnapshot(
                                streamId,
                                covered,
                                head,
                                lagRecords,
                                lagBytes,
                                age,
                                authority.snapshot()
                                        .metadataVersion(),
                                now),
                        walk);
            });
        }

        private SourceLag liveLag(
                AnchorAwareCommitWalk walk,
                long covered) {
            AppendRecoveryCommit oldest = null;
            for (AppendRecoveryCommit commit :
                    walk.commitsNewestFirst()) {
                if (commit.canonicalCommit().offsetEnd()
                        > covered) {
                    oldest = commit;
                }
            }
            if (oldest == null) {
                if (covered == walk.observedHead()
                        .offsetEnd()) {
                    return SourceLag.empty(
                            walk.observedHead()
                                    .cumulativeSize());
                }
                throw condition(
                        "stable live tail does not cover materialization lag start");
            }
            var value = oldest.canonicalCommit();
            if (value.offsetStart() != covered) {
                throw condition(
                        "materialization lag start is not a complete live commit boundary");
            }
            return new SourceLag(
                    Math.subtractExact(
                            value.cumulativeSize(),
                            value.logicalBytes()),
                    value.preparedAtMillis(),
                    false);
        }

        private CompletableFuture<SourceLag> scanAndVerifyStats(
                long covered,
                long anchor,
                long cumulativeSizeAtAnchor) {
            return scanStats(
                            covered,
                            anchor,
                            Optional.empty(),
                            new ArrayList<>())
                    .thenCompose(stats -> verifyStats(
                                    stats,
                                    0)
                            .thenApply(ignored -> statsLag(
                                    covered,
                                    anchor,
                                    cumulativeSizeAtAnchor,
                                    stats)));
        }

        private CompletableFuture<List<VersionedRangeRetentionStats>>
                scanStats(
                        long covered,
                        long anchor,
                        Optional<F4ScanToken> continuation,
                        ArrayList<VersionedRangeRetentionStats>
                                values) {
            long minimum = Math.addExact(covered, 1);
            return deadline.bound(
                            () -> generations
                                    .scanRangeRetentionStats(
                                            cluster,
                                            streamId,
                                            minimum,
                                            anchor,
                                            continuation,
                                            pageSize),
                            "scan retention stats for checkpointed materialization lag")
                    .thenCompose(page -> appendStatsPage(
                            covered, anchor, values, page));
        }

        private CompletableFuture<List<VersionedRangeRetentionStats>>
                appendStatsPage(
                        long covered,
                        long anchor,
                        ArrayList<VersionedRangeRetentionStats>
                                values,
                        RangeRetentionStatsScanPage page) {
            values.addAll(page.values());
            if (values.size() > MAX_RETENTION_STATS) {
                return CompletableFuture.failedFuture(
                        new NereusException(
                                ErrorCode.METADATA_LIMIT_EXCEEDED,
                                false,
                                "materialization lag retention stats exceed 4096"));
            }
            if (page.continuation().isPresent()) {
                return scanStats(
                        covered,
                        anchor,
                        page.continuation(),
                        values);
            }
            return CompletableFuture.completedFuture(
                    values.stream()
                            .sorted(
                                    Comparator.comparingLong(
                                                    (VersionedRangeRetentionStats value) ->
                                                            value.value()
                                                                    .offsetStart())
                                            .thenComparing(
                                                    VersionedRangeRetentionStats
                                                            ::key))
                            .toList());
        }

        private CompletableFuture<Void> verifyStats(
                List<VersionedRangeRetentionStats> stats,
                int index) {
            if (index >= stats.size()) {
                return CompletableFuture.completedFuture(null);
            }
            VersionedRangeRetentionStats expected =
                    stats.get(index);
            RangeRetentionStatsRecord value =
                    expected.value();
            return deadline.bound(
                            () -> generations.getCandidateByKey(
                                    cluster,
                                    streamId,
                                    ReadView.COMMITTED,
                                    value.sourceIndexKey()),
                            "revalidate retention stats source index")
                    .thenAccept(optional -> requireStatsSource(
                            expected,
                            optional.orElseThrow(() ->
                                    condition(
                                            "materialization lag stats source index is absent"))))
                    .thenCompose(ignored -> verifyStats(
                            stats,
                            index + 1));
        }

        private SourceLag statsLag(
                long covered,
                long anchor,
                long cumulativeSizeAtAnchor,
                List<VersionedRangeRetentionStats> stats) {
            if (stats.isEmpty()) {
                throw condition(
                        "checkpointed materialization lag has no retention stats");
            }
            long offset = covered;
            long cumulative = -1;
            long oldest = Long.MAX_VALUE;
            for (VersionedRangeRetentionStats wrapper : stats) {
                RangeRetentionStatsRecord value =
                        wrapper.value();
                if (!value.streamId().equals(streamId.value())
                        || value.offsetStart() != offset
                        || value.offsetEnd() > anchor
                        || (cumulative >= 0
                                && value.cumulativeSizeAtStart()
                                        != cumulative)) {
                    throw condition(
                            "retention stats do not form exact materialization lag coverage");
                }
                offset = value.offsetEnd();
                cumulative = value.cumulativeSizeAtEnd();
                oldest = Math.min(
                        oldest,
                        value.minPublishTimeMillis());
            }
            if (offset != anchor
                    || cumulative != cumulativeSizeAtAnchor) {
                throw condition(
                        "retention stats do not reach the recovery anchor");
            }
            RangeRetentionStatsRecord first =
                    stats.get(0).value();
            return new SourceLag(
                    first.cumulativeSizeAtStart(),
                    oldest,
                    false);
        }

        private void requireStatsSource(
                VersionedRangeRetentionStats stats,
                VersionedGenerationCandidate candidate) {
            RangeRetentionStatsRecord expected =
                    stats.value();
            if (!candidate.key()
                            .equals(expected.sourceIndexKey())
                    || candidate.metadataVersion()
                            != expected.sourceIndexMetadataVersion()
                    || !candidate.durableValueSha256()
                            .value()
                            .equals(expected
                                    .sourceIndexIdentitySha256())) {
                throw condition(
                        "retention stats source identity changed");
            }
            if (candidate
                    instanceof VersionedGenerationZeroIndex zero) {
                var value = zero.value();
                long cumulativeStart = Math.subtractExact(
                        value.cumulativeSize(),
                        value.logicalBytes());
                if (value.tombstoned()
                        || value.offsetStart()
                                != expected.offsetStart()
                        || value.offsetEnd()
                                != expected.offsetEnd()
                        || value.commitVersion()
                                != expected.commitVersion()
                        || cumulativeStart
                                != expected
                                        .cumulativeSizeAtStart()
                        || value.cumulativeSize()
                                != expected
                                        .cumulativeSizeAtEnd()) {
                    throw condition(
                            "generation-zero retention stats source changed");
                }
                return;
            }
            VersionedGenerationIndex higher =
                    (VersionedGenerationIndex) candidate;
            GenerationIndexRecord value = higher.value();
            if (value.lifecycle()
                            != GenerationLifecycle.COMMITTED
                    || value.readViewId()
                            != ReadView.COMMITTED.wireId()
                    || value.offsetStart()
                            > expected.offsetStart()
                    || value.offsetEnd()
                            < expected.offsetEnd()
                    || value.firstCommitVersion()
                            > expected.commitVersion()
                    || value.lastCommitVersion()
                            < expected.commitVersion()
                    || value.cumulativeSizeAtStart()
                            > expected.cumulativeSizeAtStart()
                    || value.cumulativeSizeAtEnd()
                            < expected.cumulativeSizeAtEnd()) {
                throw condition(
                        "higher-generation retention stats source changed");
            }
        }

        private SourceLag combine(
                SourceLag checkpointed,
                SourceLag live) {
            if (live.empty()) {
                return checkpointed;
            }
            return new SourceLag(
                    checkpointed.cumulativeSizeAtStart(),
                    Math.min(
                            checkpointed
                                    .oldestPublishTimeMillis(),
                            live.oldestPublishTimeMillis()),
                    false);
        }

        private CompletableFuture<Optional<String>> finalRevalidate(
                Authority authority,
                Coverage coverage,
                AnchorAwareCommitWalk walk) {
            return deadline.bound(
                            () -> l0.getStreamSnapshot(
                                    cluster, streamId),
                            "revalidate stream snapshot after materialization lag")
                    .thenCompose(snapshot -> deadline.bound(
                                    () -> generations
                                            .getStreamRegistration(
                                                    cluster,
                                                    streamId),
                                    "revalidate registration after materialization lag")
                            .thenCompose(registration -> deadline.bound(
                                            () -> generations
                                                    .getRecoveryRoot(
                                                            cluster,
                                                            streamId),
                                            "revalidate recovery root after materialization lag")
                                    .thenCompose(root -> {
                                        if (!sameStreamAuthority(
                                                snapshot,
                                                authority.snapshot())) {
                                            return CompletableFuture
                                                    .completedFuture(
                                                            Optional.of(
                                                                    "stream snapshot changed"));
                                        }
                                        if (!registration.equals(
                                                Optional.of(
                                                        authority
                                                                .registration()))) {
                                            return CompletableFuture
                                                    .completedFuture(
                                                            Optional.of(
                                                                    "stream registration changed"));
                                        }
                                        if (!root.equals(
                                                walk.recoveryRoot())) {
                                            return CompletableFuture
                                                    .completedFuture(
                                                            Optional.of(
                                                                    "recovery root changed"));
                                        }
                                        return scanCoverage(
                                                        authority.bounds(),
                                                        authority
                                                                .registration()
                                                                .value()
                                                                .projectionRef())
                                                .thenApply(
                                                        current ->
                                                                coverage.equals(
                                                                                current)
                                                                        ? Optional
                                                                                .<String>empty()
                                                                        : Optional.of(
                                                                                "committed generation coverage changed"));
                                    })));
        }

        private void requireWalkMatchesHead(
                StreamMetadataSnapshot snapshot,
                AnchorAwareCommitWalk walk) {
            var head = walk.observedHead();
            if (!head.streamId().equals(streamId)
                    || head.offsetEnd()
                            != snapshot.committedEnd()
                                    .committedEndOffset()
                    || head.cumulativeSize()
                            != snapshot.committedEnd()
                                    .cumulativeSize()
                    || head.commitVersion()
                            != snapshot.committedEnd()
                                    .commitVersion()
                    || head.metadataVersion()
                            != snapshot.metadataVersion()
                    || !walk.anchorReached()) {
                throw condition(
                        "append recovery tail changed or exceeded the materialization lag bound");
            }
        }

        @Override
        public void close() {
            deadline.close();
        }
    }

    private static MaterializationPolicy requirePolicy(
            MaterializationPolicy policy) {
        Objects.requireNonNull(policy, "policy");
        if (policy.view() != ReadView.COMMITTED
                || policy.taskKind()
                        != TaskKind.LOSSLESS_REWRITE
                || !policy.targetPhysicalFormat().equals(
                        MaterializationPolicy.COMMITTED_FORMAT)
                || policy.topicCompaction().isPresent()) {
            throw new IllegalArgumentException(
                    "lag reader requires the lossless COMMITTED NCP1 policy");
        }
        return policy;
    }

    private static int compareUtf8(
            String left,
            String right) {
        byte[] leftBytes =
                left.getBytes(StandardCharsets.UTF_8);
        byte[] rightBytes =
                right.getBytes(StandardCharsets.UTF_8);
        int length = Math.min(
                leftBytes.length, rightBytes.length);
        for (int index = 0; index < length; index++) {
            int compared = Integer.compare(
                    leftBytes[index] & 0xff,
                    rightBytes[index] & 0xff);
            if (compared != 0) {
                return compared;
            }
        }
        return Integer.compare(
                leftBytes.length, rightBytes.length);
    }

    private static boolean sameStreamAuthority(
            StreamMetadataSnapshot current,
            StreamMetadataSnapshot expected) {
        return current.sameVersionedAuthority(expected);
    }

    private static NereusException condition(
            String message) {
        return new NereusException(
                ErrorCode.METADATA_CONDITION_FAILED,
                true,
                message);
    }

    private static NereusException invariant(
            String message,
            Throwable cause) {
        return new NereusException(
                ErrorCode.METADATA_INVARIANT_VIOLATION,
                false,
                message,
                cause);
    }

    private static String requireText(
            String value,
            String field) {
        Objects.requireNonNull(value, field);
        if (value.isBlank()) {
            throw new IllegalArgumentException(
                    field + " cannot be blank");
        }
        return value;
    }

    private record Bounds(
            long trimOffset,
            long headOffset) {
    }

    private record Authority(
            StreamMetadataSnapshot snapshot,
            VersionedMaterializationStreamRegistration registration,
            Optional<VersionedMaterializationCheckpoint> checkpoint,
            Bounds bounds) {
    }

    private record Coverage(
            long coveredOffset,
            List<VersionedGenerationIndex> selected) {
        private Coverage {
            if (coveredOffset < 0) {
                throw new IllegalArgumentException(
                        "coveredOffset must be non-negative");
            }
            selected = List.copyOf(
                    Objects.requireNonNull(selected, "selected"));
        }

        private static Coverage at(long offset) {
            return new Coverage(offset, List.of());
        }
    }

    private record SourceLag(
            long cumulativeSizeAtStart,
            long oldestPublishTimeMillis,
            boolean empty) {
        private SourceLag {
            if (cumulativeSizeAtStart < 0
                    || oldestPublishTimeMillis < 0) {
                throw new IllegalArgumentException(
                        "source lag facts must be non-negative");
            }
        }

        private static SourceLag empty(
                long cumulativeSize) {
            return new SourceLag(
                    cumulativeSize, 0, true);
        }
    }

    private record Measured(
            MaterializationLagSnapshot snapshot,
            AnchorAwareCommitWalk walk) {
    }
}
