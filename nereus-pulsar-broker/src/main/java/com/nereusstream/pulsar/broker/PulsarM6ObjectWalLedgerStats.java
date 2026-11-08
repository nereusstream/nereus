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

package com.nereusstream.pulsar.broker;

import java.util.concurrent.atomic.LongAdder;
import org.apache.bookkeeper.mledger.ManagedLedgerMXBean;
import org.apache.bookkeeper.mledger.impl.ManagedLedgerMBeanImpl;
import org.apache.bookkeeper.mledger.proto.PendingBookieOpsStats;
import org.apache.bookkeeper.mledger.util.StatsBuckets;

/** Native topic counters for the Object carrier; no BookKeeper operation is attributed to this ledger. */
final class PulsarM6ObjectWalLedgerStats implements ManagedLedgerMXBean {
    private final PulsarM6ObjectWalManagedLedger ledger;
    private final long openedAt = System.nanoTime();
    private final LongAdder added = new LongAdder();
    private final LongAdder addedBytes = new LongAdder();
    private final LongAdder addFailures = new LongAdder();
    private final LongAdder readCalls = new LongAdder();
    private final LongAdder readEntries = new LongAdder();
    private final LongAdder readBytes = new LongAdder();
    private final LongAdder readFailures = new LongAdder();
    private final LongAdder markDeletes = new LongAdder();
    private final StatsBuckets sizes = new StatsBuckets(ManagedLedgerMBeanImpl.ENTRY_SIZE_BUCKETS_BYTES);
    private final StatsBuckets latency = new StatsBuckets(ManagedLedgerMBeanImpl.ENTRY_LATENCY_BUCKETS_USEC);

    PulsarM6ObjectWalLedgerStats(PulsarM6ObjectWalManagedLedger ledger) {
        this.ledger = ledger;
    }

    void added(long bytes, long micros) {
        added.increment();
        addedBytes.add(bytes);
        sizes.addValue(bytes);
        latency.addValue(micros);
    }

    void addFailed() {
        addFailures.increment();
    }

    void read(long count, long bytes) {
        readCalls.increment();
        readEntries.add(count);
        readBytes.add(bytes);
    }

    void readFailed() {
        readFailures.increment();
    }

    void marked() {
        markDeletes.increment();
    }

    private double rate(long total) {
        return total / Math.max(1e-9, (System.nanoTime() - openedAt) / 1e9);
    }

    private static StatsBuckets refresh(StatsBuckets buckets) {
        synchronized (buckets) {
            buckets.refresh();
            return buckets;
        }
    }

    @Override
    public String getName() {
        return ledger.getName();
    }

    @Override
    public long getStoredMessagesSize() {
        return ledger.getTotalSize();
    }

    @Override
    public long getStoredMessagesLogicalSize() {
        return ledger.getTotalSize();
    }

    @Override
    public long getNumberOfMessagesInBacklog() {
        return ledger.getNumberOfActiveEntries();
    }

    @Override
    public double getAddEntryMessagesRate() {
        return rate(added.sum());
    }

    @Override
    public double getAddEntryBytesRate() {
        return rate(addedBytes.sum());
    }

    @Override
    public long getAddEntryBytesTotal() {
        return addedBytes.sum();
    }

    @Override
    public double getAddEntryWithReplicasBytesRate() {
        return 0;
    }

    @Override
    public long getAddEntryWithReplicasBytesTotal() {
        return 0;
    }

    @Override
    public double getReadEntriesRate() {
        return rate(readCalls.sum());
    }

    @Override
    public double getReadEntriesBytesRate() {
        return rate(readBytes.sum());
    }

    @Override
    public long getReadEntriesBytesTotal() {
        return readBytes.sum();
    }

    @Override
    public double getMarkDeleteRate() {
        return rate(markDeletes.sum());
    }

    @Override
    public long getMarkDeleteTotal() {
        return markDeletes.sum();
    }

    @Override
    public long getAddEntrySucceed() {
        return added.sum();
    }

    @Override
    public long getAddEntrySucceedTotal() {
        return added.sum();
    }

    @Override
    public long getAddEntryErrors() {
        return addFailures.sum();
    }

    @Override
    public long getAddEntryErrorsTotal() {
        return addFailures.sum();
    }

    @Override
    public long getEntriesReadTotalCount() {
        return readEntries.sum();
    }

    @Override
    public long getReadEntriesSucceeded() {
        return readCalls.sum();
    }

    @Override
    public long getReadEntriesSucceededTotal() {
        return readCalls.sum();
    }

    @Override
    public long getReadEntriesErrors() {
        return readFailures.sum();
    }

    @Override
    public long getReadEntriesErrorsTotal() {
        return readFailures.sum();
    }

    @Override
    public double getReadEntriesOpsCacheMissesRate() {
        return rate(readCalls.sum());
    }

    @Override
    public long getReadEntriesOpsCacheMissesTotal() {
        return readCalls.sum();
    }

    @Override
    public double getEntrySizeAverage() {
        return refresh(sizes).getAvg();
    }

    @Override
    public long[] getEntrySizeBuckets() {
        return refresh(sizes).getBuckets();
    }

    @Override
    public double getAddEntryLatencyAverageUsec() {
        return refresh(latency).getAvg();
    }

    @Override
    public long[] getAddEntryLatencyBuckets() {
        return refresh(latency).getBuckets();
    }

    @Override
    public long[] getLedgerSwitchLatencyBuckets() {
        return new long[0];
    }

    @Override
    public double getLedgerSwitchLatencyAverageUsec() {
        return 0;
    }

    @Override
    public StatsBuckets getInternalAddEntryLatencyBuckets() {
        return refresh(latency);
    }

    @Override
    public StatsBuckets getInternalEntrySizeBuckets() {
        return refresh(sizes);
    }

    @Override
    public PendingBookieOpsStats getPendingBookieOpsStats() {
        return new PendingBookieOpsStats();
    }

    @Override
    public double getLedgerAddEntryLatencyAverageUsec() {
        return refresh(latency).getAvg();
    }

    @Override
    public long[] getLedgerAddEntryLatencyBuckets() {
        return refresh(latency).getBuckets();
    }

    @Override
    public StatsBuckets getInternalLedgerAddEntryLatencyBuckets() {
        return refresh(latency);
    }
}
