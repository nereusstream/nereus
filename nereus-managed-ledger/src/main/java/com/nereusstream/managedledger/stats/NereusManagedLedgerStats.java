/* Licensed under the Apache License, Version 2.0 */
package com.nereusstream.managedledger.stats;

import java.util.Objects;
import java.util.concurrent.atomic.LongAdder;
import java.util.function.LongSupplier;
import org.apache.bookkeeper.mledger.ManagedLedgerMXBean;
import org.apache.bookkeeper.mledger.proto.PendingBookieOpsStats;
import org.apache.bookkeeper.mledger.util.StatsBuckets;

/** Exact cumulative facade counters; rate windows and BookKeeper-only counters remain zero. */
public final class NereusManagedLedgerStats implements ManagedLedgerMXBean {
    private static final long[] ENTRY_BUCKETS = {128, 512, 1024, 4096, 16384, 65536, 262144, 1048576};
    private static final long[] LATENCY_BUCKETS_USEC = {100, 500, 1000, 5000, 10000, 50000, 100000, 1000000};

    private final String name;
    private final LongSupplier storedSize;
    private final StatsBuckets entrySizes = new StatsBuckets(ENTRY_BUCKETS);
    private final StatsBuckets addLatencies = new StatsBuckets(LATENCY_BUCKETS_USEC);
    private final StatsBuckets emptyLedgerSwitchLatencies = new StatsBuckets(LATENCY_BUCKETS_USEC);
    private final LongAdder addBytes = new LongAdder();
    private final LongAdder addSucceeded = new LongAdder();
    private final LongAdder addErrors = new LongAdder();
    private final LongAdder readBytes = new LongAdder();
    private final LongAdder entriesRead = new LongAdder();
    private final LongAdder readSucceeded = new LongAdder();
    private final LongAdder readErrors = new LongAdder();
    private final LongAdder entrySizeTotal = new LongAdder();
    private final LongAdder entrySizeCount = new LongAdder();
    private final LongAdder addLatencyTotalUsec = new LongAdder();
    private final LongAdder addLatencyCount = new LongAdder();

    public NereusManagedLedgerStats(String name, LongSupplier storedSize) {
        this.name = Objects.requireNonNull(name, "name");
        this.storedSize = Objects.requireNonNull(storedSize, "storedSize");
    }

    public void recordAddSuccess(long bytes, long startedNanos) {
        long latencyUsec = elapsedUsec(startedNanos);
        addBytes.add(bytes);
        addSucceeded.increment();
        entrySizeTotal.add(bytes);
        entrySizeCount.increment();
        addLatencyTotalUsec.add(latencyUsec);
        addLatencyCount.increment();
        entrySizes.addValue(bytes);
        addLatencies.addValue(latencyUsec);
    }

    public void recordAddFailure() {
        addErrors.increment();
    }

    public void recordReadSuccess(long bytes) {
        readBytes.add(bytes);
        entriesRead.increment();
        readSucceeded.increment();
    }

    public void recordReadFailure() {
        readErrors.increment();
    }

    @Override public String getName() { return name; }
    @Override public long getStoredMessagesSize() { return storedSize.getAsLong(); }
    @Override public long getStoredMessagesLogicalSize() { return storedSize.getAsLong(); }
    @Override public long getNumberOfMessagesInBacklog() { return 0; }
    @Override public double getAddEntryMessagesRate() { return 0; }
    @Override public double getAddEntryBytesRate() { return 0; }
    @Override public long getAddEntryBytesTotal() { return addBytes.sum(); }
    @Override public double getAddEntryWithReplicasBytesRate() { return 0; }
    @Override public long getAddEntryWithReplicasBytesTotal() { return addBytes.sum(); }
    @Override public double getReadEntriesRate() { return 0; }
    @Override public double getReadEntriesBytesRate() { return 0; }
    @Override public long getReadEntriesBytesTotal() { return readBytes.sum(); }
    @Override public double getMarkDeleteRate() { return 0; }
    @Override public long getMarkDeleteTotal() { return 0; }
    @Override public long getAddEntrySucceed() { return addSucceeded.sum(); }
    @Override public long getAddEntrySucceedTotal() { return addSucceeded.sum(); }
    @Override public long getAddEntryErrors() { return addErrors.sum(); }
    @Override public long getAddEntryErrorsTotal() { return addErrors.sum(); }
    @Override public long getEntriesReadTotalCount() { return entriesRead.sum(); }
    @Override public long getReadEntriesSucceeded() { return readSucceeded.sum(); }
    @Override public long getReadEntriesSucceededTotal() { return readSucceeded.sum(); }
    @Override public long getReadEntriesErrors() { return readErrors.sum(); }
    @Override public long getReadEntriesErrorsTotal() { return readErrors.sum(); }
    @Override public double getReadEntriesOpsCacheMissesRate() { return 0; }
    @Override public long getReadEntriesOpsCacheMissesTotal() { return readSucceeded.sum(); }
    @Override public double getEntrySizeAverage() { return average(entrySizeTotal, entrySizeCount); }
    @Override public long[] getEntrySizeBuckets() { return snapshot(entrySizes); }
    @Override public double getAddEntryLatencyAverageUsec() { return average(addLatencyTotalUsec, addLatencyCount); }
    @Override public long[] getAddEntryLatencyBuckets() { return snapshot(addLatencies); }
    @Override public long[] getLedgerSwitchLatencyBuckets() { return new long[LATENCY_BUCKETS_USEC.length + 1]; }
    @Override public double getLedgerSwitchLatencyAverageUsec() { return 0; }
    @Override public StatsBuckets getInternalAddEntryLatencyBuckets() { return addLatencies; }
    @Override public StatsBuckets getInternalEntrySizeBuckets() { return entrySizes; }
    @Override public PendingBookieOpsStats getPendingBookieOpsStats() { return new PendingBookieOpsStats(); }
    @Override public double getLedgerAddEntryLatencyAverageUsec() { return 0; }
    @Override public long[] getLedgerAddEntryLatencyBuckets() { return new long[LATENCY_BUCKETS_USEC.length + 1]; }
    @Override public StatsBuckets getInternalLedgerAddEntryLatencyBuckets() { return emptyLedgerSwitchLatencies; }

    private static long elapsedUsec(long startedNanos) {
        return Math.max(0, (System.nanoTime() - startedNanos) / 1_000);
    }

    private static double average(LongAdder total, LongAdder count) {
        long samples = count.sum();
        return samples == 0 ? 0 : total.sum() / (double) samples;
    }

    private static synchronized long[] snapshot(StatsBuckets buckets) {
        buckets.refresh();
        return buckets.getBuckets().clone();
    }
}
