/* Licensed under the Apache License, Version 2.0 */
package com.nereusstream.kafka.runtime;

import java.util.Optional;
import java.util.concurrent.atomic.AtomicBoolean;

/** Checked global byte budget for owned Kafka request snapshots. */
public final class KafkaByteBudget {
    private final long maxBytes;
    private long usedBytes;

    public KafkaByteBudget(long maxBytes) {
        if (maxBytes <= 0) throw new IllegalArgumentException("maxBytes must be positive");
        this.maxBytes = maxBytes;
    }

    public synchronized Optional<Lease> tryAcquire(int bytes) {
        if (bytes <= 0) throw new IllegalArgumentException("bytes must be positive");
        if (bytes > maxBytes - usedBytes) return Optional.empty();
        usedBytes = Math.addExact(usedBytes, bytes);
        return Optional.of(new Lease(this, bytes));
    }

    public long maxBytes() {
        return maxBytes;
    }

    public synchronized long usedBytes() {
        return usedBytes;
    }

    private synchronized void release(int bytes) {
        if (bytes > usedBytes) throw new IllegalStateException("Kafka byte budget release underflow");
        usedBytes -= bytes;
    }

    /** Idempotent ownership token. */
    public static final class Lease implements AutoCloseable {
        private final KafkaByteBudget owner;
        private final int bytes;
        private final AtomicBoolean closed = new AtomicBoolean();

        private Lease(KafkaByteBudget owner, int bytes) {
            this.owner = owner;
            this.bytes = bytes;
        }

        public int bytes() {
            return bytes;
        }

        @Override
        public void close() {
            if (closed.compareAndSet(false, true)) owner.release(bytes);
        }
    }
}
