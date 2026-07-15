/* Licensed under the Apache License, Version 2.0 */
package com.nereusstream.objectstore.compacted;

import static org.assertj.core.api.Assertions.assertThat;

import com.nereusstream.objectstore.staging.StagingFileManager;
import java.nio.charset.StandardCharsets;
import java.nio.file.Path;
import java.util.List;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.Flow;
import java.util.concurrent.atomic.AtomicBoolean;
import java.util.concurrent.atomic.AtomicInteger;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.io.TempDir;

class CompactedObjectStreamingUploadTest {
    @TempDir
    Path temporaryDirectory;

    @Test
    void requestsOneRowAtATimeAndReturnsReplayableSealedBytes() throws Exception {
        List<CompactedObjectRow> rows = List.of(
                row(10, "first"),
                row(11, "second"),
                row(12, "third"));
        TrackingPublisher publisher = new TrackingPublisher(rows);
        long logicalBytes = rows.stream().mapToLong(row -> row.exactPayload().remaining()).sum();
        CompactedObjectWriteRequest request =
                CompactedParquetTestSupport.committedRequest(3, logicalBytes, 2, "ZSTD");

        try (StagingFileManager staging =
                        CompactedParquetTestSupport.staging(temporaryDirectory, 32L << 20);
                CompactedObjectWriteResult result = new ParquetCompactedObjectWriter(staging, Runnable::run)
                        .write(request, publisher)
                        .join()) {
            assertThat(publisher.maximumRequest()).isEqualTo(1);
            assertThat(publisher.requestCalls()).isEqualTo(rows.size());
            assertThat(publisher.cancelled()).isFalse();

            byte[] firstReplay = CompactedParquetTestSupport.collect(result.stagingFile());
            byte[] secondReplay = CompactedParquetTestSupport.collect(result.stagingFile());
            assertThat(firstReplay).containsExactly(secondReplay);
            assertThat(firstReplay).hasSize(Math.toIntExact(result.objectLength()));
            assertThat(staging.reservedBytes()).isEqualTo(result.objectLength());
        }
    }

    @Test
    void cancellingAnAdmittedWriterCancelsThePublisherAndReleasesStaging() throws Exception {
        NeverPublisher publisher = new NeverPublisher();
        CompactedObjectWriteRequest request =
                CompactedParquetTestSupport.committedRequest(1, 1, 1, "UNCOMPRESSED");
        try (StagingFileManager staging =
                CompactedParquetTestSupport.staging(temporaryDirectory, 32L << 20)) {
            CompletableFuture<CompactedObjectWriteResult> future =
                    new ParquetCompactedObjectWriter(staging, Runnable::run).write(request, publisher);
            assertThat(publisher.subscribed()).isTrue();
            assertThat(future.cancel(true)).isTrue();
            assertThat(publisher.cancelled()).isTrue();
            assertThat(staging.reservedBytes()).isZero();
        }
    }

    private static CompactedObjectRow row(long offset, String value) {
        return CompactedParquetTestSupport.denseRow(
                offset,
                value.getBytes(StandardCharsets.UTF_8));
    }

    private static final class TrackingPublisher implements Flow.Publisher<CompactedObjectRow> {
        private final List<CompactedObjectRow> rows;
        private final AtomicInteger maximumRequest = new AtomicInteger();
        private final AtomicInteger requestCalls = new AtomicInteger();
        private final AtomicBoolean cancelled = new AtomicBoolean();

        private TrackingPublisher(List<CompactedObjectRow> rows) {
            this.rows = List.copyOf(rows);
        }

        @Override
        public void subscribe(Flow.Subscriber<? super CompactedObjectRow> subscriber) {
            subscriber.onSubscribe(new Flow.Subscription() {
                private int index;
                private boolean complete;

                @Override
                public void request(long count) {
                    if (complete) {
                        return;
                    }
                    requestCalls.incrementAndGet();
                    maximumRequest.accumulateAndGet(Math.toIntExact(count), Math::max);
                    if (count != 1) {
                        complete = true;
                        subscriber.onError(new AssertionError("writer demand must be exactly one row"));
                        return;
                    }
                    subscriber.onNext(rows.get(index++));
                    if (index == rows.size()) {
                        complete = true;
                        subscriber.onComplete();
                    }
                }

                @Override
                public void cancel() {
                    cancelled.set(true);
                    complete = true;
                }
            });
        }

        private int maximumRequest() {
            return maximumRequest.get();
        }

        private int requestCalls() {
            return requestCalls.get();
        }

        private boolean cancelled() {
            return cancelled.get();
        }
    }

    private static final class NeverPublisher implements Flow.Publisher<CompactedObjectRow> {
        private final AtomicBoolean subscribed = new AtomicBoolean();
        private final AtomicBoolean cancelled = new AtomicBoolean();

        @Override
        public void subscribe(Flow.Subscriber<? super CompactedObjectRow> subscriber) {
            subscribed.set(true);
            subscriber.onSubscribe(new Flow.Subscription() {
                @Override
                public void request(long count) {
                    // Keep the admitted operation pending until the caller cancels it.
                }

                @Override
                public void cancel() {
                    cancelled.set(true);
                }
            });
        }

        private boolean subscribed() {
            return subscribed.get();
        }

        private boolean cancelled() {
            return cancelled.get();
        }
    }
}
