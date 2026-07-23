/* Licensed under the Apache License, Version 2.0 */
package com.nereusstream.materialization;

import com.nereusstream.api.ErrorCode;
import com.nereusstream.api.NereusException;
import com.nereusstream.api.PayloadFormat;
import com.nereusstream.api.ReadBatch;
import com.nereusstream.api.ReadOptions;
import com.nereusstream.objectstore.Crc32cChecksums;
import com.nereusstream.objectstore.compacted.RangedCompactedObjectRow;
import java.nio.ByteBuffer;
import java.util.Objects;
import java.util.OptionalLong;
import java.util.concurrent.Executor;
import java.util.concurrent.Flow;
import java.util.concurrent.atomic.AtomicBoolean;

/** One-to-one streaming map from exact ranged source entries to dense NCP2 rows. */
public final class RangedLosslessMaterializationRowPublisher
        implements Flow.Publisher<RangedCompactedObjectRow>, AutoCloseable {
    private final ExactSourceBatchPublisher batches;
    private final AtomicBoolean subscribed = new AtomicBoolean();

    public RangedLosslessMaterializationRowPublisher(
            MaterializationTask task,
            ExactSourceRangeReader reader,
            ReadOptions readOptions,
            Executor callbackExecutor) {
        MaterializationTask exactTask = Objects.requireNonNull(task, "task");
        if (exactTask.taskKind() != TaskKind.LOSSLESS_REWRITE
                || exactTask.sources().stream().anyMatch(
                        source -> source.payloadFormat() != PayloadFormat.KAFKA_RECORD_BATCH)) {
            throw new IllegalArgumentException(
                    "ranged lossless publisher requires a Kafka LOSSLESS_REWRITE task");
        }
        batches = new ExactSourceBatchPublisher(
                exactTask,
                Objects.requireNonNull(reader, "reader"),
                Objects.requireNonNull(readOptions, "readOptions"),
                Objects.requireNonNull(callbackExecutor, "callbackExecutor"),
                true);
    }

    @Override
    public void subscribe(Flow.Subscriber<? super RangedCompactedObjectRow> subscriber) {
        Objects.requireNonNull(subscriber, "subscriber");
        if (!subscribed.compareAndSet(false, true)) {
            subscriber.onSubscribe(NoopSubscription.INSTANCE);
            subscriber.onError(new IllegalStateException(
                    "ranged lossless row publisher permits exactly one subscriber"));
            return;
        }
        batches.subscribe(new MappingSubscriber(subscriber));
    }

    @Override
    public void close() {
        batches.close();
    }

    static RangedCompactedObjectRow row(ReadBatch batch, int ordinal) {
        ReadBatch exact = Objects.requireNonNull(batch, "batch");
        if (exact.payloadFormat() != PayloadFormat.KAFKA_RECORD_BATCH) {
            throw new NereusException(
                    ErrorCode.UNSUPPORTED_FORMAT,
                    false,
                    "NCP2 publisher accepts only exact Kafka record batches");
        }
        byte[] payload = exact.payload();
        return new RangedCompactedObjectRow(
                exact.range().startOffset(),
                Math.toIntExact(exact.range().recordCount()),
                ordinal,
                ByteBuffer.wrap(payload),
                Crc32cChecksums.intValue(Crc32cChecksums.checksum(payload)),
                OptionalLong.empty());
    }

    private enum NoopSubscription implements Flow.Subscription {
        INSTANCE;

        @Override
        public void request(long count) {
        }

        @Override
        public void cancel() {
        }
    }

    private static final class MappingSubscriber implements Flow.Subscriber<ReadBatch> {
        private final Flow.Subscriber<? super RangedCompactedObjectRow> downstream;
        private int ordinal;
        private Flow.Subscription subscription;
        private boolean terminal;

        private MappingSubscriber(Flow.Subscriber<? super RangedCompactedObjectRow> downstream) {
            this.downstream = downstream;
        }

        @Override
        public void onSubscribe(Flow.Subscription subscription) {
            this.subscription = subscription;
            downstream.onSubscribe(subscription);
        }

        @Override
        public void onNext(ReadBatch item) {
            if (terminal) {
                return;
            }
            try {
                downstream.onNext(row(item, ordinal));
                ordinal = Math.addExact(ordinal, 1);
            } catch (Throwable failure) {
                terminal = true;
                subscription.cancel();
                downstream.onError(failure);
            }
        }

        @Override
        public void onError(Throwable failure) {
            if (!terminal) {
                terminal = true;
                downstream.onError(failure);
            }
        }

        @Override
        public void onComplete() {
            if (!terminal) {
                terminal = true;
                downstream.onComplete();
            }
        }
    }
}
