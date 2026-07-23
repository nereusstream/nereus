/* Licensed under the Apache License, Version 2.0 */
package com.nereusstream.core.read;
import com.nereusstream.api.ReadBatch;
import com.nereusstream.api.ReadOptions;
import com.nereusstream.api.FirstEntryPolicy;
import com.nereusstream.api.PhysicalReadResult;
import com.nereusstream.api.PhysicalReadStats;
import com.nereusstream.api.ResolvedRange;
import com.nereusstream.api.ReadRequest;
import com.nereusstream.api.StreamId;
import com.nereusstream.core.wal.PrimaryWalRegistry;
import java.util.ArrayList;
import java.util.List;
import java.util.Objects;
import java.util.OptionalLong;
import java.util.concurrent.CompletableFuture;

/** Validates and dispatches maximal adjacent exact-reader-key runs without provider fall-through. */
public final class ReadTargetDispatcher {
    private final ReadTargetReaderRegistry registry;

    public ReadTargetDispatcher(ReadTargetReaderRegistry registry) {
        this.registry = Objects.requireNonNull(registry);
    }

    public ReadTargetDispatcher(PrimaryWalRegistry registry) {
        this(Objects.requireNonNull(registry, "registry").readerRegistry());
    }

    public long reservationBytes(List<ResolvedRange> ranges) {
        validateAdapters(ranges);
        return ranges.stream().mapToLong(range ->
                registry.require(range.readTarget()).reservationBytes(range)).max().orElseThrow();
    }

    public CompletableFuture<PhysicalReadResult> read(
            StreamId streamId,
            long startOffset,
            List<ResolvedRange> ranges,
            ReadOptions options) {
        return read(
                streamId,
                new ReadRequest(
                        startOffset,
                        com.nereusstream.api.ReadView.COMMITTED,
                        com.nereusstream.api.ReadBoundaryMode.EXACT_START,
                        FirstEntryPolicy.LEGACY_STRICT_LIMIT,
                        options),
                ranges);
    }

    public CompletableFuture<PhysicalReadResult> read(
            StreamId streamId,
            ReadRequest request,
            List<ResolvedRange> ranges) {
        Objects.requireNonNull(streamId, "streamId");
        Objects.requireNonNull(request, "request");
        validateAdapters(ranges);
        List<Run> runs = runs(ranges);
        return readRun(
                streamId,
                runs,
                0,
                request,
                new ArrayList<>(),
                new ArrayList<>(),
                0,
                0,
                OptionalLong.empty());
    }

    private CompletableFuture<PhysicalReadResult> readRun(
            StreamId streamId,
            List<Run> runs, int index, ReadRequest request,
            List<ReadBatch> batches,
            List<PhysicalReadStats> stats,
            long records,
            long bytes,
            OptionalLong sourceCoverageEndOffset) {
        ReadOptions options = request.options();
        if (index >= runs.size() || records >= options.maxRecords() || bytes >= options.maxBytes()) {
            return CompletableFuture.completedFuture(
                    new PhysicalReadResult(batches, stats, sourceCoverageEndOffset));
        }
        Run run = runs.get(index);
        ReadOptions remaining = new ReadOptions(
                Math.toIntExact(options.maxRecords() - records), Math.toIntExact(options.maxBytes() - bytes),
                options.isolation(), options.timeout());
        ReadRequest remainingRequest = new ReadRequest(
                request.startOffset(),
                request.view(),
                request.boundaryMode(),
                batches.isEmpty() ? request.firstEntryPolicy() : FirstEntryPolicy.LEGACY_STRICT_LIMIT,
                remaining);
        ReadTargetReader reader = registry.require(run.key());
        return reader.readPhysicalWithStats(streamId, remainingRequest, run.ranges()).thenCompose(result -> {
            batches.addAll(result.batches()); stats.addAll(result.rangeStats());
            long next = request.startOffset();
            long newRecords = records;
            long newBytes = bytes;
            for (ReadBatch batch : result.batches()) {
                next = batch.range().endOffset();
                newRecords = Math.addExact(newRecords, batch.range().recordCount());
                newBytes = Math.addExact(newBytes, batch.payload().length);
            }
            long runProgress = result.sourceCoverageEndOffset().orElse(next);
            OptionalLong coverage = OptionalLong.of(sourceCoverageEndOffset.isPresent()
                    ? Math.max(sourceCoverageEndOffset.getAsLong(), runProgress)
                    : runProgress);
            if (runProgress < run.ranges().get(run.ranges().size() - 1).offsetRange().endOffset()) {
                return CompletableFuture.completedFuture(new PhysicalReadResult(batches, stats, coverage));
            }
            return readRun(
                    streamId,
                    runs,
                    index + 1,
                    new ReadRequest(
                            runProgress,
                            request.view(),
                            request.boundaryMode(),
                            FirstEntryPolicy.LEGACY_STRICT_LIMIT,
                            options),
                    batches,
                    stats,
                    newRecords,
                    newBytes,
                    coverage);
        });
    }

    private void validateAdapters(List<ResolvedRange> ranges) {
        ranges.forEach(range -> registry.require(range.readTarget()));
    }

    private static List<Run> runs(List<ResolvedRange> ranges) {
        List<Run> result = new ArrayList<>();
        for (ResolvedRange range : ranges) {
            ReadTargetReaderKey key = ReadTargetReaderKey.from(range.readTarget());
            if (result.isEmpty() || !result.get(result.size() - 1).key().equals(key)) {
                result.add(new Run(key, new ArrayList<>()));
            }
            result.get(result.size() - 1).ranges().add(range);
        }
        return result;
    }

    private record Run(ReadTargetReaderKey key, List<ResolvedRange> ranges) { }
}
