/* Licensed under the Apache License, Version 2.0 */
package com.nereusstream.kafka.runtime;

import com.nereusstream.api.AppendOutcome;
import com.nereusstream.api.ErrorCode;
import com.nereusstream.api.NereusException;
import java.nio.ByteBuffer;
import java.util.Objects;
import java.util.concurrent.atomic.AtomicBoolean;

/** Owned immutable Produce bytes safe to retain after the Kafka request thread returns. */
public final class KafkaProduceBufferSnapshot implements AutoCloseable {
    private final byte[] bytes;
    private final KafkaByteBudget.Lease lease;
    private final AtomicBoolean closed = new AtomicBoolean();

    private KafkaProduceBufferSnapshot(byte[] bytes, KafkaByteBudget.Lease lease) {
        this.bytes = bytes;
        this.lease = lease;
    }

    public static KafkaProduceBufferSnapshot capture(ByteBuffer requestBytes, KafkaByteBudget budget) {
        Objects.requireNonNull(requestBytes, "requestBytes");
        Objects.requireNonNull(budget, "budget");
        ByteBuffer source = requestBytes.duplicate();
        if (!source.hasRemaining()) throw new IllegalArgumentException("Kafka Produce buffer cannot be empty");
        int bytes = source.remaining();
        KafkaByteBudget.Lease lease = budget.tryAcquire(bytes).orElseThrow(() -> new NereusException(
                ErrorCode.BACKPRESSURE_REJECTED,
                true,
                "Kafka Produce owned-buffer byte budget is exhausted",
                AppendOutcome.KNOWN_NOT_COMMITTED));
        try {
            byte[] copy = new byte[bytes];
            source.get(copy);
            return new KafkaProduceBufferSnapshot(copy, lease);
        } catch (Throwable failure) {
            lease.close();
            throw failure;
        }
    }

    public int sizeInBytes() {
        return bytes.length;
    }

    public ByteBuffer buffer() {
        if (closed.get()) throw new IllegalStateException("Kafka Produce buffer snapshot is closed");
        return ByteBuffer.wrap(bytes).asReadOnlyBuffer();
    }

    @Override
    public void close() {
        if (closed.compareAndSet(false, true)) lease.close();
    }
}
