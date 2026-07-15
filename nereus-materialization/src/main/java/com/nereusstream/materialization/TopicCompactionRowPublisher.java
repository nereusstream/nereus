/* Licensed under the Apache License, Version 2.0 */
package com.nereusstream.materialization;

import com.nereusstream.api.Checksum;
import com.nereusstream.api.ErrorCode;
import com.nereusstream.api.NereusException;
import com.nereusstream.api.ReadBatch;
import com.nereusstream.api.ReadOptions;
import com.nereusstream.metadata.oxia.records.TaskFailureClass;
import com.nereusstream.objectstore.Crc32cChecksums;
import com.nereusstream.objectstore.compacted.CompactedObjectRow;
import com.nereusstream.objectstore.compacted.TopicCompactionKeyEncodingV1;
import java.nio.ByteBuffer;
import java.util.ArrayDeque;
import java.util.BitSet;
import java.util.Objects;
import java.util.Optional;
import java.util.OptionalInt;
import java.util.OptionalLong;
import java.util.concurrent.Executor;
import java.util.concurrent.Flow;
import java.util.concurrent.RejectedExecutionException;
import java.util.concurrent.atomic.AtomicBoolean;

/** Pass-two source replay that emits only pass-one survivors and re-proves every decoded fact. */
final class TopicCompactionRowPublisher
        implements Flow.Publisher<CompactedObjectRow>, AutoCloseable {
    private final MaterializationTask task;
    private final TopicCompactionRegistry.Binding binding;
    private final BitSet survivors;
    private final int expectedOutputRecords;
    private final Checksum expectedFactSha256;
    private final int maxKeyBytes;
    private final ExactSourceBatchPublisher batches;
    private final SerialExecutor serial;
    private final DefaultTopicCompactionEngine.FactDigest facts =
            new DefaultTopicCompactionEngine.FactDigest();
    private final AtomicBoolean subscribed = new AtomicBoolean();
    private final AtomicBoolean closeRequested = new AtomicBoolean();

    private Flow.Subscriber<? super CompactedObjectRow> downstream;
    private Flow.Subscription upstream;
    private long demand;
    private boolean upstreamRequested;
    private boolean terminal;
    private int emitted;

    TopicCompactionRowPublisher(
            MaterializationTask task,
            ExactSourceRangeReader reader,
            ReadOptions options,
            TopicCompactionRegistry.Binding binding,
            BitSet survivors,
            int expectedOutputRecords,
            Checksum expectedFactSha256,
            int maxKeyBytes,
            Executor executor) {
        this.task = Objects.requireNonNull(task, "task");
        this.binding = Objects.requireNonNull(binding, "binding");
        this.survivors = (BitSet) Objects.requireNonNull(survivors, "survivors").clone();
        this.expectedFactSha256 = Objects.requireNonNull(expectedFactSha256, "expectedFactSha256");
        if (expectedOutputRecords < 0
                || expectedOutputRecords != this.survivors.cardinality()) {
            throw new IllegalArgumentException("topic-compaction output count is inconsistent");
        }
        if (maxKeyBytes <= 0 || maxKeyBytes > DefaultTopicCompactionEngine.MAX_KEY_BYTES) {
            throw new IllegalArgumentException("maxKeyBytes is outside the NTC1 limit");
        }
        this.expectedOutputRecords = expectedOutputRecords;
        this.maxKeyBytes = maxKeyBytes;
        Executor exactExecutor = Objects.requireNonNull(executor, "executor");
        this.serial = new SerialExecutor(exactExecutor);
        this.batches = new ExactSourceBatchPublisher(task, reader, options, exactExecutor);
    }

    @Override
    public void subscribe(Flow.Subscriber<? super CompactedObjectRow> subscriber) {
        Objects.requireNonNull(subscriber, "subscriber");
        if (!subscribed.compareAndSet(false, true)) {
            reject(subscriber);
            return;
        }
        submit(() -> {
            if (terminal || closeRequested.get()) {
                reject(subscriber);
                return;
            }
            downstream = subscriber;
            try {
                subscriber.onSubscribe(new Flow.Subscription() {
                    @Override
                    public void request(long count) {
                        submit(() -> requestOnSerial(count));
                    }

                    @Override
                    public void cancel() {
                        closeRequested.set(true);
                        submit(() -> fail(new NereusException(
                                ErrorCode.CANCELLED,
                                false,
                                "topic-compaction row subscriber cancelled"), false));
                    }
                });
                batches.subscribe(new BatchSubscriber());
            } catch (Throwable failure) {
                fail(failure, true);
            }
        });
    }

    @Override
    public void close() {
        closeRequested.set(true);
        submit(() -> fail(new NereusException(
                ErrorCode.CANCELLED, false, "topic-compaction row publisher closed"), false));
    }

    private void requestOnSerial(long count) {
        if (terminal) {
            return;
        }
        if (count <= 0) {
            fail(new IllegalArgumentException("Flow request count must be positive"), true);
            return;
        }
        demand = addDemand(demand, count);
        advance();
    }

    private void advance() {
        if (closeRequested.get() && !terminal) {
            fail(new NereusException(
                    ErrorCode.CANCELLED, false, "topic-compaction row publisher closed"), false);
            return;
        }
        if (terminal || demand == 0 || upstream == null || upstreamRequested) {
            return;
        }
        upstreamRequested = true;
        try {
            upstream.request(1);
        } catch (Throwable failure) {
            upstreamRequested = false;
            fail(failure, true);
        }
    }

    private void batchNext(ReadBatch batch) {
        if (terminal) {
            return;
        }
        if (!upstreamRequested || demand == 0) {
            fail(invariant("topic-compaction pass two emitted outside demand"), true);
            return;
        }
        upstreamRequested = false;
        try {
            long offset = batch.range().startOffset();
            byte[] exactPayload = batch.payload();
            Optional<CompactionRecord> decoded = DefaultTopicCompactionEngine.decode(
                    binding.decoder(), offset, exactPayload);
            facts.add(offset, decoded);
            int relative = DefaultTopicCompactionEngine.relativeOffset(task, offset);
            if (survivors.get(relative)) {
                CompactedObjectRow row = row(offset, exactPayload, decoded);
                emitted = Math.addExact(emitted, 1);
                if (emitted > expectedOutputRecords) {
                    throw invariant("topic-compaction pass two exceeded its survivor count");
                }
                demand--;
                downstream.onNext(row);
            }
        } catch (Throwable failure) {
            fail(failure, true);
            return;
        }
        advance();
    }

    private CompactedObjectRow row(
            long offset,
            byte[] exactPayload,
            Optional<CompactionRecord> decoded) {
        ByteBuffer encodedKey;
        CompactionDisposition disposition;
        OptionalLong publishTime;
        OptionalLong eventTime;
        if (decoded.isEmpty()) {
            encodedKey = TopicCompactionKeyEncodingV1.unkeyed(offset);
            disposition = CompactionDisposition.VALUE;
            publishTime = OptionalLong.empty();
            eventTime = OptionalLong.empty();
        } else {
            CompactionRecord record = DefaultTopicCompactionEngine.validateDecoded(
                    decoded.orElseThrow(), offset);
            byte[] rawKey = DefaultTopicCompactionEngine.bytes(record.compactionKey());
            if (rawKey.length > maxKeyBytes) {
                throw execution(
                        TaskFailureClass.UNSUPPORTED_MAPPING,
                        ErrorCode.UNSUPPORTED_FORMAT,
                        false,
                        "decoded topic-compaction key exceeds the configured byte cap",
                        null);
            }
            encodedKey = TopicCompactionKeyEncodingV1.keyed(ByteBuffer.wrap(rawKey));
            disposition = record.disposition();
            publishTime = record.publishTimeMillis();
            eventTime = record.eventTimeMillis();
        }
        byte[] payload = disposition == CompactionDisposition.VALUE
                ? exactPayload
                : new byte[0];
        return new CompactedObjectRow(
                offset,
                ByteBuffer.wrap(payload),
                Crc32cChecksums.intValue(Crc32cChecksums.checksum(payload)),
                publishTime,
                eventTime,
                Optional.empty(),
                Optional.empty(),
                Optional.empty(),
                OptionalLong.empty(),
                OptionalLong.empty(),
                OptionalInt.empty(),
                OptionalInt.of(disposition.wireId()),
                Optional.of(encodedKey));
    }

    private void batchComplete() {
        if (terminal) {
            return;
        }
        try {
            Checksum actual = facts.finish();
            if (!actual.equals(expectedFactSha256) || emitted != expectedOutputRecords) {
                throw execution(
                        TaskFailureClass.OUTPUT_INVARIANT,
                        ErrorCode.METADATA_INVARIANT_VIOLATION,
                        false,
                        "topic-compaction pass two differs from the pass-one survivor proof",
                        null);
            }
            terminal = true;
            downstream.onComplete();
        } catch (Throwable failure) {
            fail(failure, true);
        }
    }

    private void fail(Throwable failure, boolean signal) {
        if (terminal) {
            return;
        }
        terminal = true;
        Flow.Subscription subscription = upstream;
        upstream = null;
        if (subscription != null) {
            subscription.cancel();
        }
        batches.close();
        if (signal && downstream != null) {
            try {
                downstream.onError(failure);
            } catch (Throwable ignored) {
            }
        }
    }

    private void submit(Runnable action) {
        try {
            serial.execute(action);
        } catch (RejectedExecutionException failure) {
            fail(new NereusException(
                    ErrorCode.STORAGE_CLOSED,
                    false,
                    "topic-compaction callback executor rejected admitted work",
                    failure), true);
        }
    }

    private static void reject(Flow.Subscriber<?> subscriber) {
        try {
            subscriber.onSubscribe(new Flow.Subscription() {
                @Override public void request(long count) { }
                @Override public void cancel() { }
            });
            subscriber.onError(new IllegalStateException(
                    "topic-compaction row publisher permits exactly one subscriber"));
        } catch (Throwable ignored) {
        }
    }

    private static long addDemand(long current, long requested) {
        long result = current + requested;
        return result < 0 ? Long.MAX_VALUE : result;
    }

    private static NereusException invariant(String message) {
        return new NereusException(
                ErrorCode.METADATA_INVARIANT_VIOLATION, false, message);
    }

    private static MaterializationExecutionException execution(
            TaskFailureClass failureClass,
            ErrorCode code,
            boolean retriable,
            String message,
            Throwable cause) {
        return new MaterializationExecutionException(
                failureClass, code, retriable, message, cause);
    }

    private final class BatchSubscriber implements Flow.Subscriber<ReadBatch> {
        @Override
        public void onSubscribe(Flow.Subscription subscription) {
            submit(() -> {
                if (terminal || upstream != null) {
                    subscription.cancel();
                    if (!terminal) {
                        fail(new IllegalStateException(
                                "topic-compaction pass two subscribed more than once"), true);
                    }
                    return;
                }
                upstream = subscription;
                advance();
            });
        }

        @Override
        public void onNext(ReadBatch item) {
            submit(() -> batchNext(item));
        }

        @Override
        public void onError(Throwable failure) {
            submit(() -> fail(failure, true));
        }

        @Override
        public void onComplete() {
            submit(TopicCompactionRowPublisher.this::batchComplete);
        }
    }

    private static final class SerialExecutor implements Executor {
        private final Executor delegate;
        private final ArrayDeque<Runnable> queue = new ArrayDeque<>();
        private Runnable active;

        private SerialExecutor(Executor delegate) {
            this.delegate = delegate;
        }

        @Override
        public synchronized void execute(Runnable command) {
            queue.addLast(() -> {
                try {
                    command.run();
                } finally {
                    scheduleNext();
                }
            });
            if (active == null) {
                scheduleNext();
            }
        }

        private synchronized void scheduleNext() {
            active = queue.pollFirst();
            if (active != null) {
                delegate.execute(active);
            }
        }
    }
}
