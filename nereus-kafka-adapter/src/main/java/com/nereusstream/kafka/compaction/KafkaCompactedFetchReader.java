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

package com.nereusstream.kafka.compaction;

import com.nereusstream.api.ErrorCode;
import com.nereusstream.api.FirstEntryPolicy;
import com.nereusstream.api.NereusException;
import com.nereusstream.api.ReadBatch;
import com.nereusstream.api.ReadBoundaryMode;
import com.nereusstream.api.ReadIsolation;
import com.nereusstream.api.ReadOptions;
import com.nereusstream.api.ReadRequest;
import com.nereusstream.api.ReadResult;
import com.nereusstream.api.ReadView;
import com.nereusstream.api.SemanticReadResult;
import com.nereusstream.api.StreamId;
import com.nereusstream.api.StreamStorage;
import com.nereusstream.core.read.ConstrainedSemanticStreamReader;
import com.nereusstream.core.read.GenerationReadConstraint;
import com.nereusstream.kafka.partition.KafkaPartitionIdentity;
import com.nereusstream.kafka.partition.KafkaStableSnapshot;
import com.nereusstream.kafka.partition.KafkaStorageReadRequest;
import com.nereusstream.metadata.oxia.KafkaPartitionMetadataStore;
import java.time.Duration;
import java.util.ArrayList;
import java.util.List;
import java.util.Objects;
import java.util.Optional;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.CompletionException;
import java.util.concurrent.ExecutionException;

/**
 * Executes a binding-rooted Fetch plan and composes its sparse compacted prefix with its lossless
 * tail.
 *
 * <p>Every mandatory call is issued explicitly in {@link ReadView#TOPIC_COMPACTED}. Failures from
 * that view propagate directly; this class has no code path that retries a mandatory offset in
 * COMMITTED.
 */
public final class KafkaCompactedFetchReader {
    private static final int MAX_VIEW_READS = 512;

    private final KafkaPartitionIdentity identity;
    private final StreamId streamId;
    private final StreamStorage streams;
    private final Optional<KafkaPartitionMetadataStore> bindings;
    private final Optional<KafkaActivatedGenerationAuthority> activatedGenerations;
    private final KafkaCompactedFetchPlanner planner;

    public KafkaCompactedFetchReader(
            KafkaPartitionIdentity identity,
            StreamId streamId,
            StreamStorage streams,
            KafkaPartitionMetadataStore bindings) {
        this(identity, streamId, streams, bindings, KafkaActivatedGenerationAuthority.unavailable());
    }

    public KafkaCompactedFetchReader(
            KafkaPartitionIdentity identity,
            StreamId streamId,
            StreamStorage streams,
            KafkaPartitionMetadataStore bindings,
            KafkaActivatedGenerationAuthority activatedGenerations) {
        this(
                identity,
                streamId,
                streams,
                Optional.of(Objects.requireNonNull(bindings, "bindings")),
                Optional.of(Objects.requireNonNull(activatedGenerations, "activatedGenerations")),
                new KafkaCompactedFetchPlanner());
    }

    public static KafkaCompactedFetchReader committedOnly(
            KafkaPartitionIdentity identity, StreamId streamId, StreamStorage streams) {
        return new KafkaCompactedFetchReader(
                identity, streamId, streams, Optional.empty(), Optional.empty(), new KafkaCompactedFetchPlanner());
    }

    KafkaCompactedFetchReader(
            KafkaPartitionIdentity identity,
            StreamId streamId,
            StreamStorage streams,
            Optional<KafkaPartitionMetadataStore> bindings,
            Optional<KafkaActivatedGenerationAuthority> activatedGenerations,
            KafkaCompactedFetchPlanner planner) {
        this.identity = Objects.requireNonNull(identity, "identity");
        this.streamId = Objects.requireNonNull(streamId, "streamId");
        this.streams = Objects.requireNonNull(streams, "streams");
        this.bindings = Objects.requireNonNull(bindings, "bindings");
        this.activatedGenerations = Objects.requireNonNull(activatedGenerations, "activatedGenerations");
        this.planner = Objects.requireNonNull(planner, "planner");
    }

    public CompletableFuture<Result> read(KafkaStorageReadRequest request, KafkaStableSnapshot stableSnapshot) {
        Objects.requireNonNull(request, "request");
        Objects.requireNonNull(stableSnapshot, "stableSnapshot");
        long startedNanos = System.nanoTime();
        CompletableFuture<KafkaCompactedFetchPlanner.Plan> planned = bindings.map(store -> store.get(
                                identity.durableId())
                        .thenApply(optional -> planner.plan(
                                identity,
                                streamId,
                                stableSnapshot,
                                request,
                                optional.orElseThrow(() -> new NereusException(
                                        ErrorCode.STREAM_NOT_FOUND,
                                        false,
                                        "Kafka partition " + "binding " + "disappeared " + "before" + " Fetch")))))
                .orElseGet(() -> CompletableFuture.completedFuture(planner.committedOnly(stableSnapshot, request)));
        return planned.thenCompose(plan -> execute(request, plan, startedNanos));
    }

    /**
     * Performs one bounded, binding-rooted read against every untrimmed activated generation.
     *
     * <p>This is an availability probe for internal-topic coordinator election, not a second
     * coordinator replay. Each request is explicitly constrained to the activated generation set
     * and TOPIC_COMPACTED, so it cannot emit or retry a COMMITTED segment.
     */
    public CompletableFuture<Void> probeMandatoryCompactedRead(KafkaStableSnapshot stableSnapshot, Duration timeout) {
        Objects.requireNonNull(stableSnapshot, "stableSnapshot");
        Objects.requireNonNull(timeout, "timeout");
        if (timeout.isZero() || timeout.isNegative() || timeout.toMillis() <= 0) {
            throw new IllegalArgumentException(
                    "mandatory compacted-read probe timeout must be positive and millisecond-representable");
        }
        KafkaPartitionMetadataStore store = bindings.orElseThrow(() -> new NereusException(
                ErrorCode.UNSUPPORTED_READ_SEMANTICS,
                false,
                "mandatory compacted-read probe requires Kafka partition metadata"));
        long startedNanos = System.nanoTime();
        return store.get(identity.durableId()).thenCompose(optional -> {
            var binding = optional.orElseThrow(() -> new NereusException(
                    ErrorCode.STREAM_NOT_FOUND,
                    false,
                    "Kafka partition binding disappeared before mandatory " + "compacted-read" + " probe"));
            KafkaActivatedGenerationAuthority authority = activatedGenerations.orElseThrow(() -> new NereusException(
                    ErrorCode.UNSUPPORTED_READ_SEMANTICS,
                    false,
                    "mandatory compacted-read probe requires activated " + "generation" + " discovery"));
            return authority
                    .repairIfQuarantined(identity.durableId(), streamId, binding, remaining(timeout, startedNanos))
                    .thenCompose(
                            repaired -> probeMandatoryCompactedRead(stableSnapshot, timeout, startedNanos, repaired));
        });
    }

    private CompletableFuture<Void> probeMandatoryCompactedRead(
            KafkaStableSnapshot stableSnapshot,
            Duration timeout,
            long startedNanos,
            com.nereusstream.metadata.oxia.VersionedKafkaPartitionBinding binding) {
        var root = binding.value();
        long startOffset = Math.max(stableSnapshot.logStartOffset(), root.observedLogStartOffset());
        var coverage = root.compactionCoverage();
        long maxOffsetExclusive =
                coverage.coverageVersion() == 0 ? startOffset : Math.max(startOffset, coverage.endOffset());
        KafkaStorageReadRequest request = new KafkaStorageReadRequest(
                startOffset,
                maxOffsetExclusive,
                1,
                Integer.MAX_VALUE,
                Integer.MAX_VALUE,
                true,
                startOffset,
                0,
                remaining(timeout, startedNanos));
        KafkaCompactedFetchPlanner.Plan plan = planner.plan(identity, streamId, stableSnapshot, request, binding);
        if (!plan.hasMandatoryCompactedPrefix()) {
            if (coverage.coverageVersion() != 0 && startOffset < coverage.endOffset()) {
                throw invariant("activated mandatory compacted coverage produced no compacted probe segment");
            }
            return CompletableFuture.completedFuture(null);
        }
        if (plan.segments().size() != 1 || plan.segments().get(0).view() != ReadView.TOPIC_COMPACTED) {
            throw invariant("mandatory compacted-read probe exposed another semantic view");
        }
        return constraintFor(plan)
                .thenCompose(constrained -> probeActivatedGenerations(
                        constrained.constraint().orElseThrow(), startOffset, timeout, startedNanos));
    }

    private CompletableFuture<Void> probeActivatedGenerations(
            GenerationReadConstraint constraint, long authoritativeLogStart, Duration timeout, long startedNanos) {
        if (!(streams instanceof ConstrainedSemanticStreamReader constrained)) {
            return failed(
                    ErrorCode.UNSUPPORTED_READ_SEMANTICS,
                    false,
                    "mandatory compacted-read probe requires a constrained semantic reader");
        }
        ArrayList<CompletableFuture<?>> probes = new ArrayList<>();
        try {
            for (GenerationReadConstraint.Identity generation : constraint.identities()) {
                if (generation.coverage().endOffset() <= authoritativeLogStart) {
                    continue;
                }
                long probeOffset =
                        Math.max(authoritativeLogStart, generation.coverage().startOffset());
                ReadRequest sourceRequest = new ReadRequest(
                        probeOffset,
                        ReadView.TOPIC_COMPACTED,
                        ReadBoundaryMode.CONTAINING_ENTRY,
                        FirstEntryPolicy.ALLOW_FIRST_ENTRY_OVERFLOW,
                        new ReadOptions(1, 1, ReadIsolation.COMMITTED, remaining(timeout, startedNanos)));
                probes.add(constrained
                        .read(streamId, sourceRequest, constraint)
                        .thenAccept(source -> validateProbe(source, probeOffset, constraint)));
            }
        } catch (Throwable failure) {
            return CompletableFuture.failedFuture(failure);
        }
        if (probes.isEmpty()) {
            return CompletableFuture.failedFuture(
                    invariant("mandatory compacted-read probe found no untrimmed activated generation"));
        }
        return CompletableFuture.allOf(probes.toArray(CompletableFuture[]::new));
    }

    private void validateProbe(SemanticReadResult source, long probeOffset, GenerationReadConstraint constraint) {
        SemanticReadResult exact = Objects.requireNonNull(source, "mandatory compacted-read probe result");
        if (!exact.result().streamId().equals(streamId)
                || exact.result().requestedOffset() != probeOffset
                || exact.view() != ReadView.TOPIC_COMPACTED
                || exact.sourceCoverageEndOffset() <= probeOffset
                || exact.sourceCoverageEndOffset() > constraint.coverage().endOffset()) {
            throw invariant("mandatory compacted-read probe returned invalid source coverage");
        }
    }

    private CompletableFuture<Result> execute(
            KafkaStorageReadRequest request, KafkaCompactedFetchPlanner.Plan plan, long startedNanos) {
        return constraintFor(plan).thenCompose(constrained -> {
            KafkaCompactedFetchPlanner.Plan exactPlan = constrained.plan();
            Accumulator accumulator = new Accumulator(request.startOffset());
            return readSegment(
                            request,
                            exactPlan,
                            constrained.constraint(),
                            0,
                            request.startOffset(),
                            startedNanos,
                            accumulator,
                            0)
                    .thenApply(ignored -> finish(request, exactPlan, accumulator));
        });
    }

    private CompletableFuture<ConstrainedPlan> constraintFor(KafkaCompactedFetchPlanner.Plan plan) {
        if (!plan.hasMandatoryCompactedPrefix()) {
            return CompletableFuture.completedFuture(new ConstrainedPlan(plan, Optional.empty()));
        }
        KafkaActivatedGenerationAuthority authority = activatedGenerations.orElseThrow(() -> new NereusException(
                ErrorCode.UNSUPPORTED_READ_SEMANTICS,
                false,
                "mandatory compacted Fetch requires activated generation discovery"));
        var coverage = plan.binding().value().compactionCoverage();
        return authority.resolve(streamId, coverage).thenApply(constraint -> {
            if (!constraint.streamId().equals(streamId)
                    || constraint.view() != ReadView.TOPIC_COMPACTED
                    || constraint.coverage().startOffset() != coverage.startOffset()
                    || constraint.coverage().endOffset() != coverage.endOffset()) {
                throw invariant("activated generation authority returned another stream/view/coverage");
            }
            return new ConstrainedPlan(plan, Optional.of(constraint));
        });
    }

    private CompletableFuture<Void> readSegment(
            KafkaStorageReadRequest request,
            KafkaCompactedFetchPlanner.Plan plan,
            Optional<GenerationReadConstraint> constraint,
            int segmentIndex,
            long cursor,
            long startedNanos,
            Accumulator accumulator,
            int readCalls) {
        if (segmentIndex >= plan.segments().size() || accumulator.limitReached(request)) {
            return CompletableFuture.completedFuture(null);
        }
        if (readCalls >= MAX_VIEW_READS) {
            return failed(
                    ErrorCode.METADATA_LIMIT_EXCEEDED,
                    false,
                    "Kafka Fetch exceeded the bounded semantic-view read count");
        }
        KafkaCompactedFetchPlanner.Segment segment = plan.segments().get(segmentIndex);
        if (cursor >= segment.range().endOffset()) {
            return readSegment(
                    request, plan, constraint, segmentIndex + 1, cursor, startedNanos, accumulator, readCalls);
        }

        Duration remaining;
        try {
            remaining = remaining(request.timeout(), startedNanos);
        } catch (Throwable failure) {
            return CompletableFuture.failedFuture(failure);
        }
        int remainingRecords = Math.toIntExact(Math.max(1L, request.maxRecords() - accumulator.records));
        int remainingBytes = Math.max(1, request.maxPartitionBytes() - accumulator.bytes);
        boolean allowFirstOverflow = accumulator.batches.isEmpty() && request.minOneMessage();
        ReadRequest sourceRequest = new ReadRequest(
                cursor,
                segment.view(),
                ReadBoundaryMode.CONTAINING_ENTRY,
                allowFirstOverflow ? FirstEntryPolicy.ALLOW_FIRST_ENTRY_OVERFLOW : FirstEntryPolicy.LEGACY_STRICT_LIMIT,
                new ReadOptions(remainingRecords, remainingBytes, ReadIsolation.COMMITTED, remaining));
        CompletableFuture<SemanticReadResult> sourceRead;
        if (segment.view() == ReadView.TOPIC_COMPACTED) {
            if (!(streams instanceof ConstrainedSemanticStreamReader constrained)) {
                return failed(
                        ErrorCode.UNSUPPORTED_READ_SEMANTICS,
                        false,
                        "mandatory compacted Fetch requires a constrained semantic reader");
            }
            sourceRead = constrained.read(
                    streamId,
                    sourceRequest,
                    constraint.orElseThrow(
                            () -> invariant("mandatory compacted Fetch lost its generation constraint")));
        } else {
            sourceRead = streams.read(streamId, sourceRequest);
        }
        return sourceRead
                .handle((source, failure) -> {
                    if (failure == null) {
                        return Optional.of(Objects.requireNonNull(source, "Kafka semantic-view read result"));
                    }
                    Throwable cause = unwrap(failure);
                    if (!accumulator.batches.isEmpty()
                            && cause instanceof NereusException nereus
                            && nereus.code() == ErrorCode.READ_LIMIT_TOO_SMALL) {
                        accumulator.stoppedAtReadLimit = true;
                        return Optional.<SemanticReadResult>empty();
                    }
                    throw new CompletionException(cause);
                })
                .thenCompose(optionalSource -> {
                    if (optionalSource.isEmpty()) {
                        return CompletableFuture.completedFuture(null);
                    }
                    SemanticReadResult source = optionalSource.orElseThrow();
                    long nextCursor;
                    try {
                        nextCursor = accept(request, segment, cursor, source, accumulator);
                    } catch (Throwable failure) {
                        return CompletableFuture.failedFuture(failure);
                    }
                    if (accumulator.limitReached(request)) {
                        return CompletableFuture.completedFuture(null);
                    }
                    if (nextCursor >= segment.range().endOffset()) {
                        return readSegment(
                                request,
                                plan,
                                constraint,
                                segmentIndex + 1,
                                nextCursor,
                                startedNanos,
                                accumulator,
                                readCalls + 1);
                    }
                    if (nextCursor <= cursor) {
                        return failed(
                                ErrorCode.READ_RESOLUTION_FAILED,
                                true,
                                "Kafka semantic-view read made no source-coverage progress");
                    }
                    return readSegment(
                            request,
                            plan,
                            constraint,
                            segmentIndex,
                            nextCursor,
                            startedNanos,
                            accumulator,
                            readCalls + 1);
                });
    }

    private static Throwable unwrap(Throwable failure) {
        Throwable current = failure;
        while ((current instanceof CompletionException || current instanceof ExecutionException)
                && current.getCause() != null) {
            current = current.getCause();
        }
        return current;
    }

    private long accept(
            KafkaStorageReadRequest request,
            KafkaCompactedFetchPlanner.Segment segment,
            long cursor,
            SemanticReadResult source,
            Accumulator accumulator) {
        if (!source.result().streamId().equals(streamId)
                || source.result().requestedOffset() != cursor
                || source.view() != segment.view()) {
            throw invariant("Nereus semantic read result does not match the planned Kafka Fetch segment");
        }
        List<ReadBatch> returned = source.result().batches();
        int acceptedBefore = accumulator.batches.size();
        for (ReadBatch batch : returned) {
            if (segment.range().startOffset() > request.startOffset()
                    && batch.range().startOffset() < segment.range().startOffset()) {
                throw invariant("Kafka batch crosses the mandatory compacted/committed view boundary");
            }
            if (batch.range().startOffset() >= segment.range().endOffset()) {
                break;
            }
            if (batch.range().endOffset() > segment.range().endOffset()) {
                if (segment.view() == ReadView.TOPIC_COMPACTED) {
                    throw invariant("mandatory compacted batch crosses the activated coverage boundary");
                }
                accumulator.stoppedAtUpperBound = true;
                break;
            }
            accumulator.add(request, batch);
            if (accumulator.limitReached(request)) {
                break;
            }
        }

        long nextCursor;
        if (segment.view() == ReadView.TOPIC_COMPACTED) {
            nextCursor =
                    Math.min(source.sourceCoverageEndOffset(), segment.range().endOffset());
            if (nextCursor < cursor || (!accumulator.batches.isEmpty() && accumulator.lastBatchEnd() > nextCursor)) {
                throw invariant("compacted Fetch source coverage does not cover returned Kafka batches");
            }
        } else {
            if (returned.isEmpty()) {
                if (accumulator.batches.isEmpty() && cursor < segment.range().endOffset()) {
                    throw new NereusException(
                            ErrorCode.READ_RESOLUTION_FAILED,
                            true,
                            "committed Kafka Fetch source produced no readable batch");
                }
                accumulator.stoppedAtReadLimit = true;
                nextCursor = cursor;
            } else if (accumulator.batches.size() == acceptedBefore) {
                if (!accumulator.stoppedAtUpperBound) {
                    throw invariant("committed Kafka Fetch source returned no admissible batch");
                }
                nextCursor = cursor;
            } else {
                nextCursor = accumulator.lastBatchEnd();
            }
        }
        accumulator.coverageEnd = Math.max(accumulator.coverageEnd, nextCursor);
        if (accumulator.stoppedAtUpperBound) {
            return segment.range().endOffset();
        }
        return nextCursor;
    }

    private Result finish(
            KafkaStorageReadRequest request, KafkaCompactedFetchPlanner.Plan plan, Accumulator accumulator) {
        long nextOffset = accumulator.batches.isEmpty() ? request.startOffset() : accumulator.lastBatchEnd();
        ReadView resultView = plan.hasMandatoryCompactedPrefix() ? ReadView.TOPIC_COMPACTED : ReadView.COMMITTED;
        long coverageEnd = resultView == ReadView.COMMITTED ? nextOffset : accumulator.coverageEnd;
        ReadResult result = new ReadResult(
                streamId,
                request.startOffset(),
                nextOffset,
                accumulator.batches,
                coverageEnd >= plan.requestRange().endOffset());
        SemanticReadResult semantic = new SemanticReadResult(resultView, result, coverageEnd);
        return new Result(semantic, accumulator.firstEntryOverflow, plan);
    }

    public record Result(
            SemanticReadResult semanticRead, boolean firstEntryOverflow, KafkaCompactedFetchPlanner.Plan plan) {
        public Result {
            Objects.requireNonNull(semanticRead, "semanticRead");
            Objects.requireNonNull(plan, "plan");
            if (firstEntryOverflow && semanticRead.result().batches().size() != 1) {
                throw new IllegalArgumentException("Kafka first-entry overflow requires exactly one returned batch");
            }
        }
    }

    private record ConstrainedPlan(
            KafkaCompactedFetchPlanner.Plan plan, Optional<GenerationReadConstraint> constraint) {
        private ConstrainedPlan {
            Objects.requireNonNull(plan, "plan");
            constraint = Objects.requireNonNull(constraint, "constraint");
            if (plan.hasMandatoryCompactedPrefix() != constraint.isPresent()) {
                throw new IllegalArgumentException("Kafka compacted Fetch plan and generation constraint disagree");
            }
        }
    }

    private static Duration remaining(Duration timeout, long startedNanos) {
        long elapsed = Math.max(0, System.nanoTime() - startedNanos);
        long remaining;
        try {
            remaining = Math.subtractExact(timeout.toNanos(), elapsed);
        } catch (ArithmeticException failure) {
            remaining = Long.MAX_VALUE;
        }
        if (remaining <= 0) {
            throw new NereusException(ErrorCode.TIMEOUT, true, "Kafka compacted Fetch deadline expired");
        }
        return Duration.ofNanos(remaining);
    }

    private static <T> CompletableFuture<T> failed(ErrorCode code, boolean retriable, String message) {
        return CompletableFuture.failedFuture(new NereusException(code, retriable, message));
    }

    private static NereusException invariant(String message) {
        return new NereusException(ErrorCode.METADATA_INVARIANT_VIOLATION, false, message);
    }

    private static final class Accumulator {
        private final List<ReadBatch> batches = new ArrayList<>();
        private int bytes;
        private long records;
        private long coverageEnd;
        private boolean firstEntryOverflow;
        private boolean stoppedAtUpperBound;
        private boolean stoppedAtReadLimit;

        private Accumulator(long startOffset) {
            coverageEnd = startOffset;
        }

        private void add(KafkaStorageReadRequest request, ReadBatch batch) {
            Objects.requireNonNull(batch, "batch");
            int nextBytes = Math.addExact(bytes, batch.payload().length);
            long nextRecords = Math.addExact(records, batch.range().recordCount());
            boolean overflow = nextBytes > request.maxPartitionBytes() || nextRecords > request.maxRecords();
            if (overflow && (!request.minOneMessage() || !batches.isEmpty())) {
                throw invariant("Nereus semantic read exceeded Kafka Fetch partition limits");
            }
            batches.add(batch);
            bytes = nextBytes;
            records = nextRecords;
            firstEntryOverflow = overflow && batches.size() == 1;
        }

        private boolean limitReached(KafkaStorageReadRequest request) {
            return firstEntryOverflow
                    || bytes >= request.maxPartitionBytes()
                    || records >= request.maxRecords()
                    || stoppedAtUpperBound
                    || stoppedAtReadLimit;
        }

        private long lastBatchEnd() {
            return batches.get(batches.size() - 1).range().endOffset();
        }
    }
}
