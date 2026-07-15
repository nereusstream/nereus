/* Licensed under the Apache License, Version 2.0 */
package com.nereusstream.materialization;

import com.nereusstream.api.Checksum;
import com.nereusstream.api.ErrorCode;
import com.nereusstream.api.NereusException;
import com.nereusstream.api.ObjectType;
import com.nereusstream.api.OffsetRange;
import com.nereusstream.api.PayloadFormat;
import com.nereusstream.api.ProjectionRef;
import com.nereusstream.api.ReadView;
import com.nereusstream.api.SchemaRef;
import com.nereusstream.api.StorageProfile;
import com.nereusstream.api.StreamId;
import com.nereusstream.api.StreamState;
import com.nereusstream.api.target.BookKeeperEntryRangeReadTarget;
import com.nereusstream.api.target.ObjectSliceReadTarget;
import com.nereusstream.api.target.ReadTarget;
import com.nereusstream.metadata.oxia.F4ScanToken;
import com.nereusstream.metadata.oxia.GenerationMetadataStore;
import com.nereusstream.metadata.oxia.GenerationScanPage;
import com.nereusstream.metadata.oxia.OxiaMetadataStore;
import com.nereusstream.metadata.oxia.ProjectionIdentity;
import com.nereusstream.metadata.oxia.StreamMetadataSnapshot;
import com.nereusstream.metadata.oxia.TaskScanPage;
import com.nereusstream.metadata.oxia.VersionedGenerationCandidate;
import com.nereusstream.metadata.oxia.VersionedGenerationIndex;
import com.nereusstream.metadata.oxia.VersionedMaterializationStreamRegistration;
import com.nereusstream.metadata.oxia.VersionedMaterializationTask;
import com.nereusstream.metadata.oxia.records.GenerationLifecycle;
import com.nereusstream.metadata.oxia.records.MaterializationTaskRecord;
import com.nereusstream.metadata.oxia.records.TaskLifecycle;
import java.nio.charset.StandardCharsets;
import java.util.ArrayList;
import java.util.Comparator;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.Objects;
import java.util.Optional;
import java.util.concurrent.CompletableFuture;

/** Bounded whole-index DAG planner with deterministic overlap tiling and fixed-point eligibility. */
public final class DefaultMaterializationPlanner implements MaterializationPlanner {
    public static final int MAX_CANDIDATE_EDGES = 4_096;
    public static final int MAX_SCANNED_TASKS = 4_096;

    private static final Comparator<SourceGeneration> EDGE_ORDER = Comparator
            .comparingLong((SourceGeneration source) -> source.range().startOffset())
            .thenComparingLong(source -> source.range().endOffset())
            .thenComparing(Comparator.comparingLong(SourceGeneration::generation).reversed())
            .thenComparing(SourceGeneration::indexKey, DefaultMaterializationPlanner::compareUtf8);

    private final String cluster;
    private final OxiaMetadataStore l0Metadata;
    private final GenerationMetadataStore generations;
    private final int scanPageSize;

    public DefaultMaterializationPlanner(
            String cluster,
            OxiaMetadataStore l0Metadata,
            GenerationMetadataStore generations,
            int scanPageSize) {
        this.cluster = requireText(cluster, "cluster");
        this.l0Metadata = Objects.requireNonNull(l0Metadata, "l0Metadata");
        this.generations = Objects.requireNonNull(generations, "generations");
        if (scanPageSize <= 0 || scanPageSize > 1_000) {
            throw new IllegalArgumentException("scanPageSize must be in [1, 1000]");
        }
        this.scanPageSize = scanPageSize;
    }

    @Override
    public CompletableFuture<List<MaterializationTask>> plan(
            StreamId streamId,
            OffsetRange requestedRange,
            MaterializationPolicy policy,
            int maxTasks) {
        try {
            Objects.requireNonNull(streamId, "streamId");
            Objects.requireNonNull(requestedRange, "requestedRange");
            Objects.requireNonNull(policy, "policy");
            if (requestedRange.isEmpty()) {
                return CompletableFuture.completedFuture(List.of());
            }
            if (maxTasks <= 0 || maxTasks > 1_000) {
                throw new IllegalArgumentException("maxTasks must be in [1, 1000]");
            }
            if (policy.view() != ReadView.COMMITTED || policy.taskKind() != TaskKind.LOSSLESS_REWRITE) {
                return CompletableFuture.failedFuture(new NereusException(
                        ErrorCode.UNSUPPORTED_FORMAT,
                        false,
                        "F4-M3 scheduling admits only lossless COMMITTED materialization"));
            }
            return l0Metadata.getStreamSnapshot(cluster, streamId).thenCompose(snapshot ->
                    generations.getStreamRegistration(cluster, streamId).thenCompose(registration ->
                            planSnapshot(streamId, requestedRange, policy, maxTasks, snapshot, registration)));
        } catch (RuntimeException failure) {
            return CompletableFuture.failedFuture(failure);
        }
    }

    private CompletableFuture<List<MaterializationTask>> planSnapshot(
            StreamId streamId,
            OffsetRange requestedRange,
            MaterializationPolicy policy,
            int maxTasks,
            StreamMetadataSnapshot snapshot,
            Optional<VersionedMaterializationStreamRegistration> registration) {
        PlannerBounds bounds = requireBounds(streamId, requestedRange, snapshot, registration);
        if (bounds.startOffset() >= bounds.endOffset()) {
            return CompletableFuture.completedFuture(List.of());
        }
        long minimumOffsetEnd;
        try {
            minimumOffsetEnd = Math.addExact(bounds.startOffset(), 1);
        } catch (ArithmeticException overflow) {
            return CompletableFuture.completedFuture(List.of());
        }
        CompletableFuture<List<VersionedGenerationCandidate>> candidates = scanCandidates(
                streamId,
                policy.view(),
                minimumOffsetEnd,
                bounds.endOffset(),
                Optional.empty(),
                new ArrayList<>());
        CompletableFuture<List<VersionedMaterializationTask>> tasks = scanTasks(
                streamId, Optional.empty(), new ArrayList<>());
        return candidates.thenCombine(tasks, (allCandidates, allTasks) -> buildPlan(
                streamId,
                policy,
                maxTasks,
                bounds,
                allCandidates,
                allTasks));
    }

    private PlannerBounds requireBounds(
            StreamId streamId,
            OffsetRange requestedRange,
            StreamMetadataSnapshot snapshot,
            Optional<VersionedMaterializationStreamRegistration> registration) {
        Objects.requireNonNull(snapshot, "snapshot");
        if (!snapshot.metadata().streamId().equals(streamId.value())) {
            throw invariant("planner stream snapshot belongs to another stream", null);
        }
        StreamState state;
        StorageProfile profile;
        try {
            state = StreamState.valueOf(snapshot.metadata().state());
            profile = StorageProfile.valueOf(snapshot.metadata().profile()).canonical();
        } catch (IllegalArgumentException failure) {
            throw invariant("planner snapshot contains an unknown stream state/profile", failure);
        }
        if (state != StreamState.ACTIVE && state != StreamState.SEALED) {
            throw new NereusException(
                    state == StreamState.DELETED ? ErrorCode.STREAM_NOT_FOUND : ErrorCode.STREAM_NOT_ACTIVE,
                    false,
                    "stream state does not admit materialization planning");
        }
        if (!profile.objectMaterializationEnabled()) {
            throw new NereusException(
                    ErrorCode.UNSUPPORTED_STORAGE_PROFILE,
                    false,
                    "stream profile disables object materialization");
        }
        VersionedMaterializationStreamRegistration exactRegistration = registration.orElseThrow(() ->
                new NereusException(
                        ErrorCode.METADATA_CONDITION_FAILED,
                        true,
                        "materialization stream registration is absent"));
        if (!exactRegistration.value().streamId().equals(streamId.value())) {
            throw invariant("materialization registration belongs to another stream", null);
        }
        StorageProfile registeredProfile;
        Optional<ProjectionRef> effectiveProjection;
        try {
            registeredProfile = StorageProfile.valueOf(
                    exactRegistration.value().storageProfile()).canonical();
            effectiveProjection = ProjectionIdentity.decode(exactRegistration.value().projectionRef());
        } catch (RuntimeException failure) {
            throw invariant("materialization registration contains an unsupported identity", failure);
        }
        if (registeredProfile != profile || effectiveProjection.isEmpty()) {
            throw new NereusException(
                    ErrorCode.METADATA_CONDITION_FAILED,
                    true,
                    "materialization registration no longer matches the stream profile/projection");
        }
        long start = Math.max(requestedRange.startOffset(), snapshot.trim().trimOffset());
        long end = Math.min(requestedRange.endOffset(), snapshot.committedEnd().committedEndOffset());
        return new PlannerBounds(
                start,
                end,
                snapshot.committedEnd().committedEndOffset(),
                snapshot.committedEnd().commitVersion(),
                effectiveProjection);
    }

    private CompletableFuture<List<VersionedGenerationCandidate>> scanCandidates(
            StreamId streamId,
            ReadView view,
            long minimumOffsetEnd,
            long maximumOffsetEnd,
            Optional<F4ScanToken> continuation,
            ArrayList<VersionedGenerationCandidate> values) {
        return generations.scanIndex(
                        cluster,
                        streamId,
                        view,
                        minimumOffsetEnd,
                        maximumOffsetEnd,
                        continuation,
                        scanPageSize)
                .thenCompose(page -> appendCandidatePage(
                        streamId, view, minimumOffsetEnd, maximumOffsetEnd, values, page));
    }

    private CompletableFuture<List<VersionedGenerationCandidate>> appendCandidatePage(
            StreamId streamId,
            ReadView view,
            long minimumOffsetEnd,
            long maximumOffsetEnd,
            ArrayList<VersionedGenerationCandidate> values,
            GenerationScanPage page) {
        values.addAll(page.values());
        if (values.size() > MAX_CANDIDATE_EDGES) {
            return CompletableFuture.failedFuture(limit("generation candidate edge count exceeds 4096"));
        }
        if (page.continuation().isPresent()) {
            return scanCandidates(
                    streamId,
                    view,
                    minimumOffsetEnd,
                    maximumOffsetEnd,
                    page.continuation(),
                    values);
        }
        return CompletableFuture.completedFuture(List.copyOf(values));
    }

    private CompletableFuture<List<VersionedMaterializationTask>> scanTasks(
            StreamId streamId,
            Optional<F4ScanToken> continuation,
            ArrayList<VersionedMaterializationTask> values) {
        return generations.scanTasks(cluster, streamId, continuation, scanPageSize).thenCompose(page -> {
            values.addAll(page.values());
            if (values.size() > MAX_SCANNED_TASKS) {
                return CompletableFuture.failedFuture(limit("materialization task scan exceeds 4096"));
            }
            if (page.continuation().isPresent()) {
                return scanTasks(streamId, page.continuation(), values);
            }
            return CompletableFuture.completedFuture(List.copyOf(values));
        });
    }

    private List<MaterializationTask> buildPlan(
            StreamId streamId,
            MaterializationPolicy policy,
            int maxTasks,
            PlannerBounds bounds,
            List<VersionedGenerationCandidate> candidates,
            List<VersionedMaterializationTask> durableTasks) {
        List<SourceGeneration> edges = candidates.stream()
                .map(candidate -> MaterializationSourceMapper.committedSource(
                        candidate,
                        streamId,
                        policy.view(),
                        bounds.committedEndOffset(),
                        bounds.headCommitVersion(),
                        bounds.effectiveProjection()))
                .flatMap(Optional::stream)
                .filter(source -> source.range().endOffset() > bounds.startOffset())
                .filter(source -> source.range().endOffset() <= bounds.endOffset())
                .sorted(EDGE_ORDER)
                .toList();
        List<MaterializationTaskRecord> existingTasks = durableTasks.stream()
                .map(VersionedMaterializationTask::value)
                .toList();
        List<MaterializationTask> result = new ArrayList<>(Math.min(maxTasks, 16));
        long cursor = bounds.startOffset();
        int iterations = 0;
        while (cursor < bounds.endOffset() && result.size() < maxTasks && iterations++ <= edges.size()) {
            Optional<Path> selected = selectPath(cursor, bounds.endOffset(), edges, policy);
            if (selected.isEmpty()) {
                break;
            }
            Path path = selected.orElseThrow();
            if (path.endOffset() <= cursor) {
                throw invariant("planner path did not advance the source cursor", null);
            }
            MaterializationTask task = MaterializationTask.create(
                    streamId,
                    new OffsetRange(path.startOffset(), path.endOffset()),
                    path.sources(),
                    policy);
            if (!eligible(task, policy)
                    || alreadyPublished(task, candidates)
                    || exactTaskExists(task, existingTasks)) {
                cursor = path.endOffset();
                continue;
            }
            if (overlapsActiveTask(task, existingTasks)) {
                break;
            }
            result.add(task);
            cursor = path.endOffset();
        }
        return List.copyOf(result);
    }

    private static Optional<Path> selectPath(
            long cursor,
            long maximumEndOffset,
            List<SourceGeneration> edges,
            MaterializationPolicy policy) {
        Map<Long, Map<PathState, Path>> pathsByEnd = new HashMap<>();
        Path best = null;
        for (SourceGeneration edge : edges) {
            if (edge.range().endOffset() > maximumEndOffset || edge.range().endOffset() <= cursor) {
                continue;
            }
            List<Path> candidates = new ArrayList<>();
            if (edge.range().startOffset() <= cursor && cursor < edge.range().endOffset()) {
                candidates.add(Path.first(edge));
            }
            Map<PathState, Path> predecessors = pathsByEnd.get(edge.range().startOffset());
            if (predecessors != null) {
                for (Path predecessor : predecessors.values()) {
                    if (predecessor.canAppend(edge)) {
                        candidates.add(predecessor.append(edge));
                    }
                }
            }
            for (Path candidate : candidates) {
                if (!candidate.fits(policy)) {
                    continue;
                }
                PathState state = candidate.state();
                Map<PathState, Path> atEnd = pathsByEnd.computeIfAbsent(
                        candidate.endOffset(), ignored -> new HashMap<>());
                Path current = atEnd.get(state);
                if (current == null || compareSameEnd(candidate, current) < 0) {
                    atEnd.put(state, candidate);
                }
                if (best == null || compareSelected(candidate, best) < 0) {
                    best = candidate;
                }
            }
        }
        return Optional.ofNullable(best);
    }

    private static boolean eligible(MaterializationTask task, MaterializationPolicy policy) {
        Checksum digest = policy.digestSha256();
        boolean everySourceAlreadyCurrent = task.sources().stream().allMatch(source ->
                source.generation() > 0
                        && source.materializationPolicySha256().filter(digest::equals).isPresent()
                        && source.readTarget() instanceof ObjectSliceReadTarget target
                        && target.objectType() == ObjectType.STREAM_COMPACTED_OBJECT
                        && target.physicalFormat().equals(policy.targetPhysicalFormat()));
        return !everySourceAlreadyCurrent || task.sources().size() >= policy.minMergeSourceRanges();
    }

    private static boolean alreadyPublished(
            MaterializationTask task,
            List<VersionedGenerationCandidate> candidates) {
        return candidates.stream()
                .filter(VersionedGenerationIndex.class::isInstance)
                .map(VersionedGenerationIndex.class::cast)
                .map(VersionedGenerationIndex::value)
                .anyMatch(index -> index.lifecycle() == GenerationLifecycle.COMMITTED
                        && index.taskId().equals(task.taskId())
                        && index.offsetStart() == task.coverage().startOffset()
                        && index.offsetEnd() == task.coverage().endOffset()
                        && index.sourceSetSha256().equals(task.sourceSetSha256().value())
                        && index.policySha256().equals(task.policyDigestSha256().value()));
    }

    private static boolean exactTaskExists(
            MaterializationTask task,
            List<MaterializationTaskRecord> existing) {
        return existing.stream().anyMatch(record -> record.taskId().equals(task.taskId()));
    }

    private static boolean overlapsActiveTask(
            MaterializationTask task,
            List<MaterializationTaskRecord> existing) {
        return existing.stream().anyMatch(record ->
                record.readViewId() == task.view().wireId()
                        && record.policyId().equals(task.policy().policyId())
                        && record.policyVersion() == task.policy().policyVersion()
                        && active(record.lifecycle())
                        && new OffsetRange(record.offsetStart(), record.offsetEnd()).overlaps(task.coverage()));
    }

    private static boolean active(TaskLifecycle lifecycle) {
        return lifecycle == TaskLifecycle.PLANNED
                || lifecycle == TaskLifecycle.CLAIMED
                || lifecycle == TaskLifecycle.OUTPUT_READY
                || lifecycle == TaskLifecycle.PUBLISHING
                || lifecycle == TaskLifecycle.RETRY_WAIT;
    }

    private static int compareSelected(Path left, Path right) {
        int end = Long.compare(right.endOffset(), left.endOffset());
        return end != 0 ? end : compareSameEnd(left, right);
    }

    private static int compareSameEnd(Path left, Path right) {
        int count = Integer.compare(left.sources().size(), right.sources().size());
        if (count != 0) {
            return count;
        }
        for (int index = 0; index < left.sources().size(); index++) {
            SourceGeneration leftEdge = left.sources().get(index);
            SourceGeneration rightEdge = right.sources().get(index);
            int generation = Long.compare(rightEdge.generation(), leftEdge.generation());
            if (generation != 0) {
                return generation;
            }
            int edgeEnd = Long.compare(
                    rightEdge.range().endOffset(), leftEdge.range().endOffset());
            if (edgeEnd != 0) {
                return edgeEnd;
            }
            int key = compareUtf8(leftEdge.indexKey(), rightEdge.indexKey());
            if (key != 0) {
                return key;
            }
        }
        return Long.compare(right.startOffset(), left.startOffset());
    }

    private static int compareUtf8(String left, String right) {
        byte[] leftBytes = left.getBytes(StandardCharsets.UTF_8);
        byte[] rightBytes = right.getBytes(StandardCharsets.UTF_8);
        int length = Math.min(leftBytes.length, rightBytes.length);
        for (int index = 0; index < length; index++) {
            int compared = Integer.compare(leftBytes[index] & 0xff, rightBytes[index] & 0xff);
            if (compared != 0) {
                return compared;
            }
        }
        return Integer.compare(leftBytes.length, rightBytes.length);
    }

    private static String logicalFormat(ReadTarget target, PayloadFormat payloadFormat) {
        if (target instanceof ObjectSliceReadTarget object) {
            return object.logicalFormat();
        }
        if (target instanceof BookKeeperEntryRangeReadTarget) {
            return payloadFormat.name();
        }
        throw invariant("planner source has an unknown read target", null);
    }

    private static NereusException limit(String message) {
        return new NereusException(ErrorCode.METADATA_LIMIT_EXCEEDED, false, message);
    }

    private static NereusException invariant(String message, Throwable cause) {
        return new NereusException(ErrorCode.METADATA_INVARIANT_VIOLATION, false, message, cause);
    }

    private static String requireText(String value, String field) {
        Objects.requireNonNull(value, field);
        if (value.isBlank()) {
            throw new IllegalArgumentException(field + " cannot be blank");
        }
        return value;
    }

    private record PlannerBounds(
            long startOffset,
            long endOffset,
            long committedEndOffset,
            long headCommitVersion,
            Optional<ProjectionRef> effectiveProjection) {
    }

    private record Compatibility(
            ReadView view,
            PayloadFormat payloadFormat,
            Optional<ProjectionRef> projection,
            List<SchemaRef> schemas,
            String logicalFormat) {
        private static Compatibility from(SourceGeneration source) {
            return new Compatibility(
                    source.view(),
                    source.payloadFormat(),
                    source.projectionRef(),
                    source.schemaRefs(),
                    DefaultMaterializationPlanner.logicalFormat(
                            source.readTarget(), source.payloadFormat()));
        }
    }

    private record PathState(
            long startOffset,
            long cumulativeSizeAtStart,
            Compatibility compatibility) {
    }

    private record Path(
            long startOffset,
            long endOffset,
            long cumulativeSizeAtStart,
            long cumulativeSizeAtEnd,
            Compatibility compatibility,
            List<SourceGeneration> sources) {
        private static Path first(SourceGeneration source) {
            return new Path(
                    source.range().startOffset(),
                    source.range().endOffset(),
                    source.cumulativeSizeAtStart(),
                    source.cumulativeSizeAtEnd(),
                    Compatibility.from(source),
                    List.of(source));
        }

        private boolean canAppend(SourceGeneration source) {
            return endOffset == source.range().startOffset()
                    && cumulativeSizeAtEnd == source.cumulativeSizeAtStart()
                    && compatibility.equals(Compatibility.from(source))
                    && source.commitVersion() >= sources.get(sources.size() - 1).commitVersion();
        }

        private Path append(SourceGeneration source) {
            ArrayList<SourceGeneration> combined = new ArrayList<>(sources.size() + 1);
            combined.addAll(sources);
            combined.add(source);
            return new Path(
                    startOffset,
                    source.range().endOffset(),
                    cumulativeSizeAtStart,
                    source.cumulativeSizeAtEnd(),
                    compatibility,
                    List.copyOf(combined));
        }

        private boolean fits(MaterializationPolicy policy) {
            long records;
            long bytes;
            try {
                records = Math.subtractExact(endOffset, startOffset);
                bytes = Math.subtractExact(cumulativeSizeAtEnd, cumulativeSizeAtStart);
            } catch (ArithmeticException failure) {
                return false;
            }
            return records > 0
                    && records <= policy.maxRangeRecords()
                    && bytes >= 0
                    && bytes <= policy.targetObjectBytes()
                    && sources.size() <= policy.maxSourceRanges();
        }

        private PathState state() {
            return new PathState(startOffset, cumulativeSizeAtStart, compatibility);
        }
    }
}
