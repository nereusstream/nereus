/* Licensed under the Apache License, Version 2.0 */
package com.nereusstream.materialization;

import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.assertThatThrownBy;

import com.nereusstream.api.Checksum;
import com.nereusstream.api.ChecksumType;
import com.nereusstream.api.ReadBatch;
import com.nereusstream.api.ReadIsolation;
import com.nereusstream.api.ReadOptions;
import com.nereusstream.api.ReadSourceRef;
import java.time.Duration;
import java.util.ArrayList;
import java.util.List;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.CompletionException;
import java.util.concurrent.Flow;
import java.util.concurrent.atomic.AtomicInteger;
import org.junit.jupiter.api.Test;

class ExactSourceSetBatchPublisherTest {
    @Test
    void streamsEveryExactSourceAndCompletesWhenTheLastItemConsumesDemand() {
        ExactSourceSet sourceSet = ExactSourceSetCodecV1Test.sourceSet();
        AtomicInteger closes = new AtomicInteger();
        ExactSourceRangeReader reader =
                (source, options) -> CompletableFuture.completedFuture(
                        exactRead(source, false, closes));
        CollectingSubscriber subscriber =
                new CollectingSubscriber(sourceSet.sources().size());

        try (ExactSourceSetBatchPublisher publisher =
                new ExactSourceSetBatchPublisher(
                        sourceSet, reader, options(), Runnable::run, true)) {
            publisher.subscribe(subscriber);

            assertThat(subscriber.completion().join())
                    .extracting(ReadBatch::range)
                    .containsExactly(
                            sourceSet.sources().get(0).range(),
                            sourceSet.sources().get(1).range());
            assertThat(closes).hasValue(sourceSet.sources().size());
        }
    }

    @Test
    void rejectsAReaderThatSubstitutesAnotherGenerationIdentity() {
        ExactSourceSet sourceSet = ExactSourceSetCodecV1Test.sourceSet();
        ExactSourceRangeReader reader =
                (source, options) -> CompletableFuture.completedFuture(
                        exactRead(source, true, new AtomicInteger()));
        CollectingSubscriber subscriber = new CollectingSubscriber(1);

        try (ExactSourceSetBatchPublisher publisher =
                new ExactSourceSetBatchPublisher(
                        sourceSet, reader, options(), Runnable::run, true)) {
            publisher.subscribe(subscriber);

            assertThatThrownBy(() -> subscriber.completion().join())
                    .isInstanceOf(CompletionException.class)
                    .hasRootCauseMessage(
                            "exact source batch is not a dense task entry");
        }
    }

    private static ExactSourceRead exactRead(
            SourceGeneration source,
            boolean substituteGeneration,
            AtomicInteger closes) {
        byte[] payload = new byte[Math.toIntExact(source.logicalBytes())];
        long generation =
                substituteGeneration
                        ? Math.addExact(source.generation(), 1)
                        : source.generation();
        ReadBatch batch =
                new ReadBatch(
                        source.range(),
                        source.payloadFormat(),
                        payload,
                        source.schemaRefs(),
                        source.projectionRef(),
                        new ReadSourceRef(
                                source.range(),
                                generation,
                                source.commitVersion(),
                                source.readTarget(),
                                source.targetIdentitySha256()));
        ExactSourceReadSummary summary =
                new ExactSourceReadSummary(
                        source.range(),
                        source.recordCount(),
                        source.entryCount(),
                        source.logicalBytes(),
                        new Checksum(ChecksumType.SHA256, "f".repeat(64)));
        return new ExactSourceRead() {
            @Override
            public SourceGeneration source() {
                return source;
            }

            @Override
            public Flow.Publisher<ReadBatch> batches() {
                return subscriber ->
                        subscriber.onSubscribe(
                                new Flow.Subscription() {
                                    private boolean emitted;

                                    @Override
                                    public void request(long count) {
                                        if (emitted) {
                                            return;
                                        }
                                        emitted = true;
                                        subscriber.onNext(batch);
                                        subscriber.onComplete();
                                    }

                                    @Override
                                    public void cancel() {
                                        emitted = true;
                                    }
                                });
            }

            @Override
            public CompletableFuture<ExactSourceReadSummary> completion() {
                return CompletableFuture.completedFuture(summary);
            }

            @Override
            public void close() {
                closes.incrementAndGet();
            }
        };
    }

    private static ReadOptions options() {
        return new ReadOptions(
                64,
                1 << 20,
                ReadIsolation.COMMITTED,
                Duration.ofSeconds(10));
    }

    private static final class CollectingSubscriber
            implements Flow.Subscriber<ReadBatch> {
        private final long initialDemand;
        private final ArrayList<ReadBatch> batches = new ArrayList<>();
        private final CompletableFuture<List<ReadBatch>> completion =
                new CompletableFuture<>();

        private CollectingSubscriber(long initialDemand) {
            this.initialDemand = initialDemand;
        }

        @Override
        public void onSubscribe(Flow.Subscription subscription) {
            subscription.request(initialDemand);
        }

        @Override
        public void onNext(ReadBatch item) {
            batches.add(item);
        }

        @Override
        public void onError(Throwable throwable) {
            completion.completeExceptionally(throwable);
        }

        @Override
        public void onComplete() {
            completion.complete(List.copyOf(batches));
        }

        private CompletableFuture<List<ReadBatch>> completion() {
            return completion;
        }
    }
}
