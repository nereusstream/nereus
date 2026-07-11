/* Licensed under the Apache License, Version 2.0 */
package com.nereusstream.core.read;
import com.nereusstream.api.ReadBatch;
import com.nereusstream.api.ReadOptions;
import com.nereusstream.api.ResolvedRange;
import com.nereusstream.api.target.ReadTargetType;
import com.nereusstream.core.wal.PrimaryWalReader;
import com.nereusstream.core.wal.PrimaryWalRegistry;
import com.nereusstream.objectstore.wal.WalReadResult;
import com.nereusstream.objectstore.wal.WalSliceReadStats;
import java.util.ArrayList;
import java.util.List;
import java.util.Objects;
import java.util.concurrent.CompletableFuture;

/** Validates and dispatches maximal adjacent target-type runs without provider fall-through. */
public final class ReadTargetDispatcher {
    private final PrimaryWalRegistry registry;
    public ReadTargetDispatcher(PrimaryWalRegistry registry) { this.registry = Objects.requireNonNull(registry); }

    public long reservationBytes(List<ResolvedRange> ranges) {
        validateAdapters(ranges);
        return ranges.stream().mapToLong(range ->
                registry.requireReader(range.readTarget().type()).reservationBytes(range)).max().orElseThrow();
    }

    public CompletableFuture<WalReadResult> read(
            long startOffset, List<ResolvedRange> ranges, ReadOptions options) {
        validateAdapters(ranges);
        List<Run> runs = runs(ranges);
        return readRun(runs, 0, startOffset, options, new ArrayList<>(), new ArrayList<>(), 0, 0);
    }

    private CompletableFuture<WalReadResult> readRun(
            List<Run> runs, int index, long startOffset, ReadOptions options,
            List<ReadBatch> batches, List<WalSliceReadStats> stats, long records, long bytes) {
        if (index >= runs.size() || records >= options.maxRecords() || bytes >= options.maxBytes()) {
            return CompletableFuture.completedFuture(new WalReadResult(batches, stats));
        }
        Run run = runs.get(index);
        ReadOptions remaining = new ReadOptions(
                Math.toIntExact(options.maxRecords() - records), Math.toIntExact(options.maxBytes() - bytes),
                options.isolation(), options.timeout());
        PrimaryWalReader reader = registry.requireReader(run.type());
        return reader.readWithStats(startOffset, run.ranges(), remaining).thenCompose(result -> {
            batches.addAll(result.batches()); stats.addAll(result.sliceStats());
            long next = startOffset;
            long newRecords = records;
            long newBytes = bytes;
            for (ReadBatch batch : result.batches()) {
                next = batch.range().endOffset();
                newRecords = Math.addExact(newRecords, batch.range().recordCount());
                newBytes = Math.addExact(newBytes, batch.payload().length);
            }
            if (result.batches().isEmpty() || next < run.ranges().getLast().offsetRange().endOffset()) {
                return CompletableFuture.completedFuture(new WalReadResult(batches, stats));
            }
            return readRun(runs, index + 1, next, options, batches, stats, newRecords, newBytes);
        });
    }

    private void validateAdapters(List<ResolvedRange> ranges) {
        ranges.forEach(range -> registry.requireReader(range.readTarget().type()));
    }

    private static List<Run> runs(List<ResolvedRange> ranges) {
        List<Run> result = new ArrayList<>();
        for (ResolvedRange range : ranges) {
            ReadTargetType type = range.readTarget().type();
            if (result.isEmpty() || result.getLast().type() != type) result.add(new Run(type, new ArrayList<>()));
            result.getLast().ranges().add(range);
        }
        return result;
    }

    private record Run(ReadTargetType type, List<ResolvedRange> ranges) { }
}
